package handlers

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/converged-computing/fluence/pkg/placement"
	"github.com/converged-computing/fluence/pkg/webhook"
	"github.com/converged-computing/fluence/pkg/webhook/spec"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func init() {
	webhook.Register(&quantumHandler{})
}

// Quantum-specific policy.
const (
	// QuantumResource is the Fluxion resource a pod requests to ask Fluence to
	// schedule quantum work. Requesting it is the sole trigger for this handler.
	QuantumResource = "fluxion.flux-framework.org/qpu"

	// QuantumGate holds a worker pod unscheduled until the scout's task is
	// ready (the scout's sidecar removes it).
	QuantumGate = "quantum.braket/ready"

	// ObserveLabel opts a STANDALONE quantum pod (a group of one) into
	// observe-only. The sidecar is injected and polls queue position
	// does not ungate.
	ObserveLabel = "fluence.flux-framework.org/observe"

	// DependencyKindQuantumSubmit is the readiness Kind for the quantum resource
	// type: worker pods wait for a quantum submission to reach the device queue.
	// First concrete instance of the general Dependency primitive (dependency.go).
	DependencyKindQuantumSubmit = "quantum-submit"

	// CoordinationAnnotation selects how a quantum gang is coordinated. It is an
	// open enum so future designs (e.g. index-paired "scatter") add a mode
	// without changing the mechanism.
	CoordinationAnnotation = "fluence.flux-framework.org/coordination"

	// CoordinationShared. One real task. The scout (index 0) submits and the
	// other members are gated workers that fetch the scout's result. Each
	// member is told its role via FLUENCE_COORDINATION_ROLE. A role-aware workload
	// branches on it (scout submits, worker fetches by FLUENCE_QUANTUM_JOB_ID).
	CoordinationShared = "shared"

	// CoordinationIndependent (default). Every member does its own quantum work.
	// no coordination, no gating. Never invent coordination the user did not ask
	// for, and never dedup tasks meant to be distinct.
	CoordinationIndependent = "independent"

	// ScoutGroupSuffix names the scout's own group-of-one. <group>-scout
	// (minCount 1) so it schedules alone and never deadlocks against the gated
	// worker gang.
	ScoutGroupSuffix = "-scout"

	// CompletionIndexAnnotation is the indexed-Job completion index the Job
	// controller stamps on each pod. Index "0" is the scout (deterministic
	// election with no recorded state).
	CompletionIndexAnnotation = "batch.kubernetes.io/job-completion-index"

	// ScoutIndex is the completion index promoted to scout.
	ScoutIndex = "0"

	// GangGroupEnv tells the scout's sidecar which worker group label to list
	// and ungate when the task is ready.
	GangGroupEnv = "FLUENCE_GANG_GROUP"
)

// quantumHandler splits a shared quantum gang into a single scout (real
// submit + sidecar) and N-1 gated workers.
type quantumHandler struct{}

func (h *quantumHandler) Name() string { return "quantum" }

// Applies to any pod requesting the quantum resource.
func (h *quantumHandler) Applies(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod) bool {
	return spec.PodRequestsResource(pod, QuantumResource)
}

var _ webhook.Admitter = (*quantumHandler)(nil)

// Admit is admission control, see pkg/webhook/admission.go.
func (h *quantumHandler) Admit(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod) error {
	if spec.Annotation(pod, webhook.ProtectedAnnotation) != "" {
		return fmt.Errorf("%s is set by fluence and must not be set on a submission. It marks a pod as "+
			"not preemptible, which is not yours to decide", webhook.ProtectedAnnotation)
	}
	g := resolveGroup(pod)
	n := resolveGangSize(ctx, m, pod, g)
	cores := placement.PodCores(pod)
	key := pod.Namespace + "/" + pod.Name
	want := cores
	if coordinationMode(pod) == CoordinationShared && g != "" && n > 1 {
		if groupAdmitted(ctx, m, pod.Namespace, g) {
			return nil
		}
		key = pod.Namespace + "/" + g
		want = cores * int(n)
	}
	return m.Admit(ctx, pod, key, want)
}

// groupAdmitted reports whether the gang PodGroup already has an admission record.
func groupAdmitted(ctx context.Context, m webhook.MutatorAPI, namespace, group string) bool {
	c := m.Client()
	if c == nil {
		return false
	}
	pg, err := c.SchedulingV1alpha2().PodGroups(namespace).Get(ctx, group, metav1.GetOptions{})
	if err != nil {
		return false
	}
	return pg.Annotations[webhook.AdmittedCoresAnnotation] != ""
}

// admissionOps marks a quantum pod protected and names its admission record.
// A standalone pod is its own record and has the total.
func admissionOps(pod *corev1.Pod, record string, standaloneCores int) []spec.Op {
	ann := map[string]string{
		webhook.ProtectedAnnotation:     webhook.ProtectedValue,
		webhook.AdmittedGroupAnnotation: record,
	}
	if standaloneCores > 0 {
		ann[webhook.AdmittedCoresAnnotation] = strconv.Itoa(standaloneCores)
	}
	return annotateOps(pod, ann)
}

// annotateOps adds annotations with one patch when the pod has none yet,
// since repeated adds of the whole map would replace each other, and per key
// otherwise. The pod is updated in place so later helpers see the keys.
func annotateOps(pod *corev1.Pod, ann map[string]string) []spec.Op {
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
		for k, v := range ann {
			pod.Annotations[k] = v
		}
		return []spec.Op{{Op: "add", Path: "/metadata/annotations", Value: ann}}
	}
	var ops []spec.Op
	for k, v := range ann {
		ops = append(ops, spec.Op{Op: "add", Path: "/metadata/annotations/" + escapeJSONPointer(k), Value: v})
		pod.Annotations[k] = v
	}
	return ops
}

func (h *quantumHandler) Mutate(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod) []spec.Op {
	g := resolveGroup(pod)
	n := resolveGangSize(ctx, m, pod, g)
	mode := coordinationMode(pod)
	observe := spec.Label(pod, ObserveLabel) == "true"
	cores := placement.PodCores(pod)

	// No coordination. A standalone quantum pod, or an explicitly independent
	// member. The REAL submit happens in THIS pod. The sidecar is added only for
	// observe-only telemetry. (independent mode routes every member here -> N
	// standalone scouts, each owning its task and its own queue wait.)
	if mode != CoordinationShared || g == "" || n <= 1 {
		ops := admissionOps(pod, pod.Namespace+"/"+pod.Name, cores)
		ops = append(ops, interceptorOps(pod)...)
		ops = append(ops, handoffOps(pod)...)
		if observe {
			sc := sidecarFor(m)
			sc.EnsureRBAC(ctx, pod.Namespace)
			ops = append(ops, sc.ContainerOps(pod, true, nil)...)
		}
		log.Printf("[fluence-webhook] quantum %s/%s mode=%s (standalone/independent, observe=%v)",
			pod.Namespace, pod.Name, mode, observe)
		return ops
	}

	// shared mode. Promote one member to scout. The rest are gated workers.
	// the gang PodGroup is the admission record for scout and workers
	// alike, so make sure it exists and stamp the total on it once
	m.EnsurePodGroup(ctx, pod.Namespace, g, pod.Name, n-1)
	m.RecordAdmission(ctx, pod.Namespace, g, cores*int(n))
	ops := admissionOps(pod, pod.Namespace+"/"+g, 0)
	if isScout(ctx, m, pod, g) {
		return append(ops, h.mutateScout(ctx, m, pod, g)...)
	}
	return append(ops, h.mutateWorker(ctx, m, pod, g, n)...)
}

// mutateScout wires the single scout member (indexed-Job completion index
// 0). Its own group-of-one <group>-scout (minCount 1) so it schedules alone
// and runs the REAL submit, the interceptor in tag mode, RBAC, and the sidecar
// told which worker group to ungate (FLUENCE_GANG_GROUP). The scout is one
// of the N members, so the application is NOT run an extra time. Never gated.
func (h *quantumHandler) mutateScout(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod, group string) []spec.Op {
	pg := group + ScoutGroupSuffix
	m.EnsurePodGroup(ctx, pod.Namespace, pg, pod.Name, 1)
	ops := linkGroupOps(pod, pg)
	ops = append(ops, interceptorOps(pod)...)           // tag mode: the scout submits for real
	ops = append(ops, handoffOps(pod)...)               // where the interceptor writes the task id and session
	ops = append(ops, roleEnvOps(pod, RoleScout)...) // FLUENCE_COORDINATION_ROLE=scout
	sc := sidecarFor(m)
	sc.EnsureRBAC(ctx, pod.Namespace)
	extra := []corev1.EnvVar{{Name: GangGroupEnv, Value: group}}
	ops = append(ops, sc.ContainerOps(pod, false, extra)...)
	log.Printf("[fluence-webhook] quantum scout %s/%s, group %s (ungates workers %q)",
		pod.Namespace, pod.Name, pg, group)
	return ops
}

// mutateWorker wires a non-scout member. It joins the <group> worker gang
// (minCount N-1) and is gated until the scout's task is ready. It is told its
// role (FLUENCE_COORDINATION_ROLE=worker) and handed the scout's task id
// (FLUENCE_QUANTUM_JOB_ID, stamped on the pod by the sidecar at ungate). A
// role-aware worker reads those and fetches the shared result instead of
// submitting, so the worker never calls the vendor submit, and needs neither
// the interceptor nor a faux flag.
func (h *quantumHandler) mutateWorker(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod, group string, n int32) []spec.Op {
	m.EnsurePodGroup(ctx, pod.Namespace, group, pod.Name, n-1)
	ops := linkGroupOps(pod, group)
	// the worker waits on the scout's submission, held by the quantum gate
	dep := Dependency{Kind: DependencyKindQuantumSubmit, Scout: group + ScoutGroupSuffix, Gate: QuantumGate}
	ops = append(ops, dep.applyOps(pod)...)
	ops = append(ops, workerEnvOps(pod)...)
	// A worker does not hold a qpu of its own. It uses the scout session, so
	// its quantum resource request is dropped. Otherwise Fluxion would allocate
	// a qpu per worker and a backend with one qpu could never seat the gang.
	ops = append(ops, dropQuantumResourceOps(pod)...)
	log.Printf("[fluence-webhook] quantum worker %s/%s, group %s minCount=%d, gated (role=worker, qpu stripped)",
		pod.Namespace, pod.Name, group, n-1)
	return ops
}

// dropQuantumResourceOps removes the Fluxion quantum resource from a worker's
// containers (requests and limits), returning the patch ops and mutating pod in
// place. Only entries that are present are removed (a JSON-patch remove on a
// missing path would fail). The sidecar container is never a worker concern.
func dropQuantumResourceOps(pod *corev1.Pod) []spec.Op {
	rn := corev1.ResourceName(QuantumResource)
	// JSON Pointer escaping for the resource key: '~' -> '~0', '/' -> '~1'.
	key := strings.ReplaceAll(strings.ReplaceAll(QuantumResource, "~", "~0"), "/", "~1")
	var ops []spec.Op
	for i, c := range pod.Spec.Containers {
		if c.Name == SidecarContainerName {
			continue
		}
		if _, ok := c.Resources.Requests[rn]; ok {
			ops = append(ops, spec.Op{Op: "remove",
				Path: fmt.Sprintf("/spec/containers/%d/resources/requests/%s", i, key)})
			delete(pod.Spec.Containers[i].Resources.Requests, rn)
		}
		if _, ok := c.Resources.Limits[rn]; ok {
			ops = append(ops, spec.Op{Op: "remove",
				Path: fmt.Sprintf("/spec/containers/%d/resources/limits/%s", i, key)})
			delete(pod.Spec.Containers[i].Resources.Limits, rn)
		}
	}
	return ops
}

// coordinationMode reads the coordination annotation. Default independent.
func coordinationMode(pod *corev1.Pod) string {
	if v := spec.Annotation(pod, CoordinationAnnotation); v != "" {
		return v
	}
	return CoordinationIndependent
}

// isScout decides whether THIS pod is the gang's single scout. Indexed Job
// (recommended). Completion index 0 is the scout, deterministic, race-free,
// no recorded state. Otherwise. First arrival claims the scout slot by the
// absence of the scout PodGroup (best-effort under concurrent admission.
// prefer an indexed Job for determinism). Indexing a nil annotations map yields
// ok=false, so the indexed branch is nil-safe.
func isScout(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod, group string) bool {
	if idx, ok := pod.Annotations[CompletionIndexAnnotation]; ok {
		return idx == ScoutIndex
	}
	c := m.Client()
	if c == nil {
		return true // tests / no client: treat as scout
	}
	pg := group + ScoutGroupSuffix
	if _, err := c.SchedulingV1alpha2().PodGroups(pod.Namespace).Get(ctx, pg, metav1.GetOptions{}); err == nil {
		return false // already claimed by an earlier arrival
	}
	return true
}

// resolveGroup returns the gang group identity. The explicit group label, else
// the owning controller's name (Job/ReplicaSet/StatefulSet, a Deployment's pods
// are owned by a ReplicaSet), else "" (a loose quantum pod with no group, which
// is treated as a standalone group of one).
func resolveGroup(pod *corev1.Pod) string {
	if g := webhook.GroupName(pod); g != "" {
		return g
	}
	for _, ref := range pod.OwnerReferences {
		switch ref.Kind {
		case "Job", "ReplicaSet", "StatefulSet":
			return ref.Name
		}
	}
	return ""
}

// resolveGangSize returns the full gang size N. The explicit group-size
// annotation (authoritative override), else the owner's replica count (Job
// parallelism/completions, ReplicaSet replicas), else a count of pods already
// with the group label (best-effort for loose grouped pods. Admission-order
// dependent, which is why the annotation is preferred), else 1.
func resolveGangSize(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod, group string) int32 {
	if pod.Annotations != nil {
		if v, err := strconv.Atoi(pod.Annotations[webhook.GroupSizeAnnotation]); err == nil && v > 0 {
			return int32(v)
		}
	}
	if n := ownerJobN(ctx, m, pod); n > 0 {
		return n
	}
	if n := ownerReplicaSetN(ctx, m, pod); n > 0 {
		return n
	}
	if group != "" {
		if n := countGroupPods(ctx, m, pod.Namespace, group); n > 0 {
			return n
		}
	}
	return 1
}

// ownerReplicaSetN returns the replica count of the ReplicaSet that owns the pod
// (the Deployment case: Deployment -> ReplicaSet -> Pod), or 0 if none.
func ownerReplicaSetN(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod) int32 {
	c := m.Client()
	if c == nil {
		return 0
	}
	for _, ref := range pod.OwnerReferences {
		if ref.Kind != "ReplicaSet" {
			continue
		}
		rs, err := c.AppsV1().ReplicaSets(pod.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return 0
		}
		if rs.Spec.Replicas != nil && *rs.Spec.Replicas > 0 {
			return *rs.Spec.Replicas
		}
	}
	return 0
}

// countGroupPods counts pods that already have the group label (best-effort gang
// size for loose grouped pods that have neither a group-size annotation nor an
// owning controller). Admission-order dependent, prefer the group-size
// annotation when the exact size must be guaranteed.
func countGroupPods(ctx context.Context, m webhook.MutatorAPI, namespace, group string) int32 {
	c := m.Client()
	if c == nil {
		return 0
	}
	list, err := c.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: webhook.GroupLabel + "=" + group,
	})
	if err != nil {
		return 0
	}
	return int32(len(list.Items))
}

// linkGroupOps ensures the gang pod has the group label (so the scout's
// sidecar can list it) and is linked to the gang PodGroup via
// spec.schedulingGroup.podGroupName. Idempotent.
func linkGroupOps(pod *corev1.Pod, group string) []spec.Op {
	var ops []spec.Op
	if webhook.GroupName(pod) != group {
		if pod.Labels == nil {
			ops = append(ops, spec.Op{Op: "add", Path: "/metadata/labels",
				Value: map[string]string{webhook.GroupLabel: group}})
		} else {
			ops = append(ops, spec.Op{Op: "add",
				Path:  "/metadata/labels/" + escapeJSONPointer(webhook.GroupLabel),
				Value: group})
		}
	}
	if pod.Spec.SchedulingGroup == nil || pod.Spec.SchedulingGroup.PodGroupName == nil ||
		*pod.Spec.SchedulingGroup.PodGroupName != group {
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/schedulingGroup",
			Value: map[string]string{"podGroupName": group}})
	}
	return ops
}

// escapeJSONPointer escapes "~" and "/" for use in a JSON Pointer path segment.
func escapeJSONPointer(s string) string {
	s = strings.ReplaceAll(s, "~", "~0")
	s = strings.ReplaceAll(s, "/", "~1")
	return s
}

const QuantumClassicalPriorityClass = "fluence-quantum-classical"

// ── coordination role (scout / worker) ─────────────────────────────────────
//
// In a shared gang each member is told its role positively, so the application
// branches on it instead of relying on any submit-interception magic.
//   scout  submits the one real task (and is tagged so the sidecar finds it).
//   worker  does NOT submit, it reads the scout's task id and fetches the
//             shared result (e.g. via the vendor's S3-backed result API).
// The role is decided at admission by isScout (completion index 0, else the
// scout-group claim) and surfaced as FLUENCE_COORDINATION_ROLE. Because the
// election is the webhook's, this env is the single source of truth, the
// container never re-derives its role from the Job index (which loose, non-Job
// pods don't even have).

const (
	// CoordinationRoleEnv has the pod's role in a shared gang. A role-aware
	// workload branches on it. RoleScout submits, RoleWorker fetches the
	// shared result by id. Unset for standalone/independent pods (they all submit).
	CoordinationRoleEnv = "FLUENCE_COORDINATION_ROLE"
	RoleScout        = "scout"
	RoleWorker        = "worker"

	// QuantumJobIDAnnotation is the vendor-neutral task id the ungating sidecar
	// stamps on each worker (mirrors python/fluence/ungate.py JOB_ID_ANNOTATION),
	// BEFORE removing the gate. Surfaced into FLUENCE_QUANTUM_JOB_ID via the
	// downward API so a worker can fetch the scout's result by id.
	QuantumJobIDAnnotation = "fluence.flux-framework.org/quantum-job-id"

	// QuantumJobIDEnv is the env a worker reads for the scout's task id.
	QuantumJobIDEnv = "FLUENCE_QUANTUM_JOB_ID"

	// QuantumSessionIDAnnotation is the session the scout acquired, stamped
	// on each worker at ungate when there is one, the same way as the job id
	// (python/fluence/ungate.py SESSION_ID_ANNOTATION). A worker can submit
	// into the same session with it.
	QuantumSessionIDAnnotation = "fluence.flux-framework.org/quantum-session-id"
	QuantumSessionIDEnv        = "FLUENCE_QUANTUM_SESSION_ID"
)

// roleEnvOps sets FLUENCE_COORDINATION_ROLE=<role> on each non-sidecar container.
func roleEnvOps(pod *corev1.Pod, role string) []spec.Op {
	return setContainerEnvOps(pod, corev1.EnvVar{Name: CoordinationRoleEnv, Value: role})
}

// workerEnvOps tells a worker its role and hands it the scout's task id and
// session, read from the annotations the sidecar stamps at ungate. A worker
// may fetch the scout result by id or submit into the scout session.
func workerEnvOps(pod *corev1.Pod) []spec.Op {
	ops := roleEnvOps(pod, RoleWorker)
	ops = append(ops, setContainerEnvOps(pod, spec.AnnotationEnv(QuantumJobIDEnv, QuantumJobIDAnnotation))...)
	ops = append(ops, setContainerEnvOps(pod, spec.AnnotationEnv(QuantumSessionIDEnv, QuantumSessionIDAnnotation))...)
	return ops
}

// setContainerEnvOps appends env var e to every non-sidecar container that does
// not already define it, returning the patch ops and mutating pod in place.
func setContainerEnvOps(pod *corev1.Pod, e corev1.EnvVar) []spec.Op {
	var ops []spec.Op
	for i, c := range pod.Spec.Containers {
		if c.Name == SidecarContainerName || spec.HasEnv(c, e.Name) {
			continue
		}
		if len(c.Env) == 0 {
			ops = append(ops, spec.Op{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/env", i), Value: []corev1.EnvVar{e}})
			pod.Spec.Containers[i].Env = []corev1.EnvVar{e}
		} else {
			ops = append(ops, spec.Op{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/env/-", i), Value: e})
			pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, e)
		}
	}
	return ops
}

// Sidecar implementation, quantum-owned, NOT core.
//
// The fluence coordination sidecar (its container, name, RBAC, image, and the
// Python interceptor staging) is specific to the quantum integration. It polls a
// vendor queue and ungates workers. None of this belongs on the webhook core,
// which stays domain-agnostic and only exposes generic primitives (Client,
// InjectedEnv, EnsurePodGroup). The core invokes each handler's generic Mutate.
// a handler does its own create/edit side-effects (here. RBAC, ConfigMaps,
// container injection) through the generic client.
//
// These are package-level functions (not methods on the core *Mutator) operating
// on the generic webhook.MutatorAPI. coreSidecar (see sidecar.go) delegates to
// them. A future non-quantum handler that needs a different sidecar supplies its
// own Sidecar implementation and its own container name/image.

const (
	// SidecarContainerName is the injected sidecar container's name. Owned here
	// (not a global core const) because the container is quantum-specific.
	SidecarContainerName = "fluence-sidecar"

	// SidecarServiceAccount is the ServiceAccount (and Role/RoleBinding) name the
	// sidecar uses to patch pods and read PodGroups.
	SidecarServiceAccount = "fluence-sidecar"

	// defaultSidecarImage is used when FLUENCE_SIDECAR_IMAGE is not set. Owned by
	// the quantum integration. The deployment may override it via the env var.
	defaultSidecarImage = "vanessa/fluence-sidecar:latest"

	// StageVolumeName / StageMountPath. The shared emptyDir the init container
	// stages the fluence Python package into, mounted into workload containers
	// and prepended to PYTHONPATH (Model C delivery).
	StageVolumeName = "fluence-pkg"
	StageMountPath  = "/opt/fluence-staged"

	// HandoffVolumeName / HandoffMountPath / HandoffDirEnv. The shared emptyDir
	// a submitting pod's interceptor writes its task id and session to, and
	// its sidecar reads, then writes the workers done marker to
	// (python/fluence/handoff.py). Workers never get it.
	HandoffVolumeName = "fluence-handoff"
	HandoffMountPath  = "/opt/fluence-handoff"
	HandoffDirEnv     = "FLUENCE_HANDOFF_DIR"
)

// sidecarImage resolves the sidecar image. The FLUENCE_SIDECAR_IMAGE override
// (deployment config) or the quantum default. Read here so image config is owned
// by the integration that uses it, not the core.
func sidecarImage() string {
	if v := os.Getenv("FLUENCE_SIDECAR_IMAGE"); v != "" {
		return v
	}
	return defaultSidecarImage
}

// ensureSidecarRBAC provisions the per-namespace ServiceAccount/Role/RoleBinding
// the sidecar uses to patch pods and read PodGroups. Idempotent (create-if-absent).
func ensureSidecarRBAC(ctx context.Context, m webhook.MutatorAPI, namespace string) {
	c := m.Client()
	if c == nil {
		return
	}
	lbl := map[string]string{"app": SidecarServiceAccount}

	if _, err := c.CoreV1().ServiceAccounts(namespace).Get(ctx, SidecarServiceAccount, metav1.GetOptions{}); err != nil {
		sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: SidecarServiceAccount, Namespace: namespace, Labels: lbl}}
		if _, err := c.CoreV1().ServiceAccounts(namespace).Create(ctx, sa, metav1.CreateOptions{}); err != nil {
			log.Printf("[fluence-webhook] could not create ServiceAccount %s/%s: %v", namespace, SidecarServiceAccount, err)
		}
	}
	if _, err := c.RbacV1().Roles(namespace).Get(ctx, SidecarServiceAccount, metav1.GetOptions{}); err != nil {
		role := &rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: SidecarServiceAccount, Namespace: namespace, Labels: lbl},
			Rules: []rbacv1.PolicyRule{
				{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "list", "patch", "update"}},
				{APIGroups: []string{"scheduling.k8s.io"}, Resources: []string{"podgroups"}, Verbs: []string{"get", "list"}},
			},
		}
		if _, err := c.RbacV1().Roles(namespace).Create(ctx, role, metav1.CreateOptions{}); err != nil {
			log.Printf("[fluence-webhook] could not create Role %s/%s: %v", namespace, SidecarServiceAccount, err)
		}
	}
	if _, err := c.RbacV1().RoleBindings(namespace).Get(ctx, SidecarServiceAccount, metav1.GetOptions{}); err != nil {
		rb := &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: SidecarServiceAccount, Namespace: namespace, Labels: lbl},
			Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: SidecarServiceAccount, Namespace: namespace}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: SidecarServiceAccount},
		}
		if _, err := c.RbacV1().RoleBindings(namespace).Create(ctx, rb, metav1.CreateOptions{}); err != nil {
			log.Printf("[fluence-webhook] could not create RoleBinding %s/%s: %v", namespace, SidecarServiceAccount, err)
		}
	}
}

// interceptorOps stages the fluence Python package (Model C). An init container
// copies it into a shared emptyDir, mounted into every workload container
// (skipping the sidecar) with PYTHONPATH + FLUENCE_POD_UID, so Python auto-imports
// the interceptor via sitecustomize, which tags the vendor submit so the sidecar
// can find the task. Added to scouts and standalone/independent pods (the ones
// that actually submit). Workers don't submit, so they don't get it.
func interceptorOps(pod *corev1.Pod) []spec.Op {
	var ops []spec.Op

	vol := corev1.Volume{Name: StageVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}
	if len(pod.Spec.Volumes) == 0 {
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/volumes", Value: []corev1.Volume{vol}})
	} else {
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/volumes/-", Value: vol})
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, vol)

	initc := corev1.Container{
		Name:            "fluence-stage",
		Image:           sidecarImage(),
		ImagePullPolicy: corev1.PullAlways,
		Command: []string{"sh", "-c",
			fmt.Sprintf("python3 -m fluence.stage %s || echo '[fluence] staging skipped (interceptor unavailable)'", StageMountPath)},
		VolumeMounts: []corev1.VolumeMount{{Name: StageVolumeName, MountPath: StageMountPath}},
	}
	if len(pod.Spec.InitContainers) == 0 {
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/initContainers", Value: []corev1.Container{initc}})
	} else {
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/initContainers/-", Value: initc})
	}

	mount := corev1.VolumeMount{Name: StageVolumeName, MountPath: StageMountPath, ReadOnly: true}
	pythonpath := corev1.EnvVar{Name: "PYTHONPATH", Value: StageMountPath}
	uid := spec.FieldEnv("FLUENCE_POD_UID", "metadata.uid")
	for i, c := range pod.Spec.Containers {
		if c.Name == SidecarContainerName {
			continue
		}
		if len(c.VolumeMounts) == 0 {
			ops = append(ops, spec.Op{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/volumeMounts", i), Value: []corev1.VolumeMount{mount}})
		} else {
			ops = append(ops, spec.Op{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/volumeMounts/-", i), Value: mount})
		}
		pod.Spec.Containers[i].VolumeMounts = append(pod.Spec.Containers[i].VolumeMounts, mount)
		if !spec.HasEnv(c, "PYTHONPATH") {
			if len(c.Env) == 0 {
				ops = append(ops, spec.Op{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/env", i), Value: []corev1.EnvVar{pythonpath}})
				pod.Spec.Containers[i].Env = []corev1.EnvVar{pythonpath}
			} else {
				ops = append(ops, spec.Op{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/env/-", i), Value: pythonpath})
				pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, pythonpath)
			}
		}
		if !spec.HasEnv(c, "FLUENCE_POD_UID") {
			ops = append(ops, spec.Op{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/env/-", i), Value: uid})
			pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, uid)
		}
	}
	return ops
}

// handoffOps gives a submitting pod the handoff volume. Workload containers
// mount it writable and get FLUENCE_HANDOFF_DIR. The sidecar mounts it read
// only in sidecarContainerOps. Idempotent.
func handoffOps(pod *corev1.Pod) []spec.Op {
	for _, v := range pod.Spec.Volumes {
		if v.Name == HandoffVolumeName {
			return nil
		}
	}
	var ops []spec.Op
	vol := corev1.Volume{Name: HandoffVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}
	if len(pod.Spec.Volumes) == 0 {
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/volumes", Value: []corev1.Volume{vol}})
	} else {
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/volumes/-", Value: vol})
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, vol)

	mount := corev1.VolumeMount{Name: HandoffVolumeName, MountPath: HandoffMountPath}
	for i, c := range pod.Spec.Containers {
		if c.Name == SidecarContainerName {
			continue
		}
		if len(c.VolumeMounts) == 0 {
			ops = append(ops, spec.Op{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/volumeMounts", i), Value: []corev1.VolumeMount{mount}})
		} else {
			ops = append(ops, spec.Op{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/volumeMounts/-", i), Value: mount})
		}
		pod.Spec.Containers[i].VolumeMounts = append(pod.Spec.Containers[i].VolumeMounts, mount)
	}
	ops = append(ops, setContainerEnvOps(pod, corev1.EnvVar{Name: HandoffDirEnv, Value: HandoffMountPath})...)
	return ops
}

// sidecarContainerOps adds the fluence sidecar container (pod identity env, the
// generic FLUXION_* contract from InjectedEnv, the observe flag, handler-supplied
// extraEnv, and the workload's secret/configMap-sourced credentials) and sets the
// sidecar ServiceAccount. observe=true selects observe-only telemetry mode.
func sidecarContainerOps(m webhook.MutatorAPI, pod *corev1.Pod, observe bool, extraEnv []corev1.EnvVar) []spec.Op {
	var ops []spec.Op
	env := []corev1.EnvVar{
		spec.FieldEnv("FLUENCE_POD_UID", "metadata.uid"),
		spec.FieldEnv("FLUENCE_POD_NAME", "metadata.name"),
		spec.FieldEnv("FLUENCE_NAMESPACE", "metadata.namespace"),
		spec.FieldEnv("FLUENCE_GROUP", "metadata.labels['"+webhook.GroupLabel+"']"),
	}
	env = append(env, m.InjectedEnv()...)
	if observe {
		env = append(env, corev1.EnvVar{Name: "FLUENCE_OBSERVE", Value: "true"})
	}
	env = append(env, extraEnv...)
	// Copy the workload container's secret/configMap-sourced env onto the sidecar
	// so it can talk to the same backend (domain-agnostic. We propagate whatever
	// the workload pulls from a secret/configMap. Existing FLUENCE_/FLUXION_ names
	// are not overwritten).
	if len(pod.Spec.Containers) > 0 {
		have := map[string]bool{}
		for _, e := range env {
			have[e.Name] = true
		}
		for _, e := range pod.Spec.Containers[0].Env {
			if have[e.Name] || e.ValueFrom == nil {
				continue
			}
			if e.ValueFrom.SecretKeyRef != nil || e.ValueFrom.ConfigMapKeyRef != nil {
				env = append(env, e)
			}
		}
	}
	var mounts []corev1.VolumeMount
	for _, v := range pod.Spec.Volumes {
		if v.Name == HandoffVolumeName {
			mounts = append(mounts, corev1.VolumeMount{Name: HandoffVolumeName, MountPath: HandoffMountPath})
		}
	}
	sidecar := corev1.Container{
		Name: SidecarContainerName, Image: sidecarImage(), ImagePullPolicy: corev1.PullAlways,
		Env: env, VolumeMounts: mounts,
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("256Mi"),
		}},
	}
	if len(pod.Spec.Containers) == 0 {
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/containers", Value: []corev1.Container{sidecar}})
	} else {
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/containers/-", Value: sidecar})
	}
	if pod.Spec.ServiceAccountName == "" || pod.Spec.ServiceAccountName == "default" {
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/serviceAccountName", Value: SidecarServiceAccount})
	}
	return ops
}
