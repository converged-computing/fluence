package handlers

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

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

// Quantum-specific policy. The webhook core knows NONE of these — they live only
// here, in the quantum handler.
//
// Model (no leader/worker): a workload requesting the quantum resource (Job,
// Deployment, or loose pods — the trigger is the resource, not the kind) becomes
// a GANG of full size N: one PodGroup, every pod fully gated and raised to a
// preempting priority, each staged with the interceptor in FAUX mode (the submit
// is a no-op). Fluence ALSO creates a separate one-off SUBMITTER pod — a
// group-of-one running the SAME application container plus the real sidecar —
// which submits the quantum task for real, tags it, stamps the resulting job-id
// onto the gang, and ungates the gang. There is no leader among the user's pods;
// the submitter is the only submitting pod and Fluence owns it.
const (
	// QuantumResource is the Fluxion resource a pod requests to ask Fluence to
	// schedule quantum work. Requesting it is the sole trigger for this handler.
	QuantumResource = "fluxion.flux-framework.org/qpu"

	// QuantumGate holds a gang pod unscheduled until the submitter's task is
	// ready (the submitter's sidecar removes it).
	QuantumGate = "quantum.braket/ready"

	// ObserveLabel opts a STANDALONE quantum pod (a group of one) into
	// observe-only telemetry: the sidecar is injected and polls queue position
	// but ungates nothing.
	ObserveLabel = "fluence.flux-framework.org/observe"

	// DependencyKindQuantumSubmit is the readiness Kind for the quantum resource
	// type: gang pods wait for a quantum submission to reach the device queue.
	// First concrete instance of the general Dependency primitive (dependency.go).
	DependencyKindQuantumSubmit = "quantum-submit"

	// SubmitterAnnotation marks the Fluence-created submitter pod so its own
	// admission is recognized (real sidecar, real submit, not gated) instead of
	// being treated as another gang member.
	SubmitterAnnotation = "fluence.flux-framework.org/submitter"

	// GangGroupAnnotation, set on the submitter at creation, names the gang group
	// the submitter must ungate. Surfaced to its sidecar as FLUENCE_GANG_GROUP.
	GangGroupAnnotation = "fluence.flux-framework.org/gang-group"

	// SubmitterGroupSuffix: the submitter is its own group-of-one named
	// <group>-submitter (a distinct PodGroup, minCount 1, so it schedules alone
	// and never deadlocks against the gated gang).
	SubmitterGroupSuffix = "-submitter"

	// GangGroupEnv tells the submitter's sidecar which gang group label to list
	// and ungate when the task is ready.
	GangGroupEnv = "FLUENCE_GANG_GROUP"
)

// quantumHandler creates, for a quantum workload, a fully-gated faux-submitting
// gang plus a one-off real submitter (see the package-level model comment). It
// is the only place in the webhook that knows about quantum resources, gates,
// submitters, or observe semantics.
type quantumHandler struct{}

func (h *quantumHandler) Name() string { return "quantum" }

// Applies to any pod requesting the quantum resource. Gang members run the same
// image as the submitter and request it; the submitter (a copy) requests it; a
// standalone quantum pod requests it. Nothing without the resource needs quantum
// handling, so this is the single, unambiguous trigger.
func (h *quantumHandler) Applies(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod) bool {
	return spec.PodRequestsResource(pod, QuantumResource)
}

func (h *quantumHandler) Mutate(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod) []spec.Op {
	// The Fluence-created submitter: real interceptor + real sidecar, its own
	// group-of-one, NOT gated. Recognized by the marker set at creation.
	if spec.Annotation(pod, SubmitterAnnotation) == "true" {
		return h.mutateSubmitter(ctx, m, pod)
	}

	g := resolveGroup(pod)
	observe := spec.Label(pod, ObserveLabel) == "true"
	n := resolveGangSize(ctx, m, pod, g)

	// Standalone quantum pod (a group of one): it performs its own real submit.
	// No gang, no gating, no faux, no separate submitter. The sidecar is added
	// only for observe-only telemetry.
	if g == "" || n <= 1 {
		ops := interceptorOps(pod)
		if observe {
			sc := sidecarFor(m)
			sc.EnsureRBAC(ctx, pod.Namespace)
			ops = append(ops, sc.ContainerOps(pod, true, nil)...)
		}
		log.Printf("[fluence-webhook] quantum standalone %s/%s (observe=%v)", pod.Namespace, pod.Name, observe)
		return ops
	}

	// Gang member: full gang of N in one PodGroup, fully gated + preempting
	// priority + faux interceptor. Fluence also ensures the one-off submitter
	// (idempotent) that does the real submit and ungates this gang.
	m.EnsurePodGroup(ctx, pod.Namespace, g, pod.Name, n)
	ensureSubmitterPod(ctx, m, pod, g)

	ops := linkGroupOps(pod, g)
	// Express the wait as the GENERAL dependency primitive: this gang pod depends
	// on the quantum submission produced by <group>-submitter, held by the quantum
	// gate. applyOps gates the pod, raises priority, and stamps depends-on-*.
	dep := Dependency{Kind: DependencyKindQuantumSubmit, Producer: g + SubmitterGroupSuffix, Gate: QuantumGate}
	ops = append(ops, dep.applyOps(pod)...)
	// Same interceptor as the submitter, but FAUX mode so the gang pod never
	// resubmits; it receives the real task id via FLUENCE_QUANTUM_JOB_ID.
	ops = append(ops, interceptorOps(pod)...)
	ops = append(ops, fauxSubmitEnvOps(pod)...)
	log.Printf("[fluence-webhook] quantum gang member %s/%s — group %s minCount=%d, gated+faux",
		pod.Namespace, pod.Name, g, n)
	return ops
}

// mutateSubmitter wires the Fluence-created submitter pod: its own PodGroup of
// one, the real interceptor (tag mode), RBAC, and the sidecar container told
// which gang group to ungate (FLUENCE_GANG_GROUP). The submitter is never gated.
func (h *quantumHandler) mutateSubmitter(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod) []spec.Op {
	sg := webhook.GroupName(pod) // the submitter's own group: <gang>-submitter
	gang := spec.Annotation(pod, GangGroupAnnotation)
	if sg != "" {
		m.EnsurePodGroup(ctx, pod.Namespace, sg, pod.Name, 1)
	}
	sc := sidecarFor(m)
	ops := sc.InterceptorOps(pod)
	sc.EnsureRBAC(ctx, pod.Namespace)
	extra := []corev1.EnvVar{{Name: GangGroupEnv, Value: gang}}
	ops = append(ops, sc.ContainerOps(pod, false, extra)...)
	log.Printf("[fluence-webhook] quantum submitter %s/%s — group %s (ungates gang %q)",
		pod.Namespace, pod.Name, sg, gang)
	return ops
}

// resolveGroup returns the gang group identity: the explicit group label, else
// the owning controller's name (Job/ReplicaSet/StatefulSet — a Deployment's pods
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

// resolveGangSize returns the full gang size N: the explicit group-size
// annotation (authoritative override), else the owner's replica count (Job
// parallelism/completions, ReplicaSet replicas), else a count of pods already
// carrying the group label (best-effort for loose grouped pods; admission-order
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

// countGroupPods counts pods already carrying the group label (best-effort gang
// size for loose grouped pods that have neither a group-size annotation nor an
// owning controller). Admission-order dependent — prefer the group-size
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

// SubmitterPodSuffix names the Fluence-created submitter for a group:
// <group>-submitter. It also serves as the submitter's own PodGroup name.
const SubmitterPodSuffix = SubmitterGroupSuffix

// ensureSubmitterPod creates the one-off quantum submitter pod for a group
// (idempotent create-if-absent — a client side-effect of admission, like
// EnsurePodGroup/EnsureSidecarRBAC; NOT a separate controller). It is built from
// the admitted gang pod so it runs the SAME application + credentials, is its own
// group-of-one (<group>-submitter), is marked the submitter (so its admission
// gets the real sidecar and is not gated), and records the gang group it must
// ungate. An ownerReference to the gang's PodGroup cascades GC: when the gang
// PodGroup is deleted (gang completed/deleted), the submitter is collected too.
func ensureSubmitterPod(ctx context.Context, m webhook.MutatorAPI, gangPod *corev1.Pod, group string) {
	c := m.Client()
	if c == nil {
		return
	}
	name := group + SubmitterGroupSuffix
	if _, err := c.CoreV1().Pods(gangPod.Namespace).Get(ctx, name, metav1.GetOptions{}); err == nil {
		return // already created (idempotent)
	}
	// Clean copy of the user's application: same containers (image, env, creds,
	// the quantum resource request) and app volumes — none of the gang's gating
	// or faux wiring.
	src := gangPod.DeepCopy()
	submitter := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: gangPod.Namespace,
			Labels:    map[string]string{webhook.GroupLabel: name},
			Annotations: map[string]string{
				SubmitterAnnotation: "true",
				GangGroupAnnotation: group,
			},
		},
		Spec: corev1.PodSpec{
			SchedulerName: webhook.SchedulerName,
			RestartPolicy: corev1.RestartPolicyNever,
			Containers:    src.Spec.Containers,
			Volumes:       src.Spec.Volumes,
		},
	}
	// Cascade GC: own the submitter by the gang's PodGroup (created moments ago by
	// the caller). Best-effort — only set when the PodGroup UID is known (it is on
	// a real cluster; the fake client in tests may leave it empty, in which case
	// we skip the ref rather than emit an invalid one).
	if pg, err := c.SchedulingV1alpha2().PodGroups(gangPod.Namespace).Get(ctx, group, metav1.GetOptions{}); err == nil && pg.UID != "" {
		submitter.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: "scheduling.k8s.io/v1alpha2",
			Kind:       "PodGroup",
			Name:       group,
			UID:        pg.UID,
		}}
	}
	if _, err := c.CoreV1().Pods(gangPod.Namespace).Create(ctx, submitter, metav1.CreateOptions{}); err != nil {
		log.Printf("[fluence-webhook] submitter pod %s/%s: %v", gangPod.Namespace, name, err)
	} else {
		log.Printf("[fluence-webhook] created submitter pod %s/%s for gang %s", gangPod.Namespace, name, group)
	}
}

// linkGroupOps ensures the gang pod carries the group label (so the submitter's
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

// ── faux-submit (worker submit dedup) ───────────────────────────────────────────
//
// Quantum-specific, and delivered through the SAME Python interceptor as the
// submitter — not a second mechanism. The submitter's interceptor tags the
// submit; the worker's interceptor (same staged code) no-ops the submit. Which
// behavior runs is selected at runtime by FLUENCE_FAUX_SUBMIT, set here on the
// worker. Workers run the submitter's image and may call submit, but by ungate
// time the task already exists, so resubmitting would duplicate it N times.

const (
	// FauxSubmitEnv selects the interceptor's no-op (faux) mode on workers.
	// install_interceptor (see python/fluence/providers/braket.py) reads it and
	// patches the vendor submit to return the existing task instead of submitting.
	FauxSubmitEnv = "FLUENCE_FAUX_SUBMIT"

	// QuantumJobIDAnnotation is the vendor-neutral task id the ungating sidecar
	// stamps on each worker (mirrors python/fluence/ungate.py JOB_ID_ANNOTATION),
	// BEFORE removing the gate. Surfaced into FLUENCE_QUANTUM_JOB_ID via the
	// downward API so the faux interceptor can return a handle to that task.
	QuantumJobIDAnnotation = "fluence.flux-framework.org/quantum-job-id"

	// QuantumJobIDEnv is the env the faux interceptor reads for the existing
	// task's id.
	QuantumJobIDEnv = "FLUENCE_QUANTUM_JOB_ID"
)

// fauxSubmitEnvOps sets, on each non-sidecar worker container, the faux-mode
// marker (FLUENCE_FAUX_SUBMIT=true) and the existing task's id
// (FLUENCE_QUANTUM_JOB_ID, downward API from the annotation the ungating sidecar
// stamps). The interceptor is staged separately via the shared sidecar
// InterceptorOps path — these env vars only switch its mode and hand it the id.
func fauxSubmitEnvOps(pod *corev1.Pod) []spec.Op {
	faux := corev1.EnvVar{Name: FauxSubmitEnv, Value: "true"}
	jobID := spec.AnnotationEnv(QuantumJobIDEnv, QuantumJobIDAnnotation)
	var ops []spec.Op
	for i, c := range pod.Spec.Containers {
		if c.Name == SidecarContainerName {
			continue
		}
		if !spec.HasEnv(c, FauxSubmitEnv) {
			if len(c.Env) == 0 {
				ops = append(ops, spec.Op{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/env", i), Value: []corev1.EnvVar{faux}})
				pod.Spec.Containers[i].Env = []corev1.EnvVar{faux}
			} else {
				ops = append(ops, spec.Op{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/env/-", i), Value: faux})
				pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, faux)
			}
		}
		if !spec.HasEnv(c, QuantumJobIDEnv) {
			ops = append(ops, spec.Op{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/env/-", i), Value: jobID})
			pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, jobID)
		}
	}
	return ops
}

// Sidecar implementation — quantum-owned, NOT core.
//
// The fluence coordination sidecar (its container, name, RBAC, image, and the
// Python interceptor staging) is specific to the quantum integration: it polls a
// vendor queue and ungates workers. None of this belongs on the webhook core,
// which stays domain-agnostic and only exposes generic primitives (Client,
// InjectedEnv, EnsurePodGroup). The core invokes each handler's generic Mutate;
// a handler does its own create/edit side-effects (here: RBAC, ConfigMaps,
// container injection) through the generic client.
//
// These are package-level functions (not methods on the core *Mutator) operating
// on the generic webhook.MutatorAPI. coreSidecar (see sidecar.go) delegates to
// them; a future non-quantum handler that needs a different sidecar supplies its
// own Sidecar implementation and its own container name/image.

const (
	// SidecarContainerName is the injected sidecar container's name. Owned here
	// (not a global core const) because the container is quantum-specific.
	SidecarContainerName = "fluence-sidecar"

	// SidecarServiceAccount is the ServiceAccount (and Role/RoleBinding) name the
	// sidecar uses to patch pods and read PodGroups.
	SidecarServiceAccount = "fluence-sidecar"

	// defaultSidecarImage is used when FLUENCE_SIDECAR_IMAGE is not set. Owned by
	// the quantum integration; the deployment may override it via the env var.
	defaultSidecarImage = "ghcr.io/converged-computing/fluence-sidecar:latest"

	// StageVolumeName / StageMountPath: the shared emptyDir the init container
	// stages the fluence Python package into, mounted into workload containers
	// and prepended to PYTHONPATH (Model C delivery).
	StageVolumeName = "fluence-pkg"
	StageMountPath  = "/opt/fluence-staged"
)

// sidecarImage resolves the sidecar image: the FLUENCE_SIDECAR_IMAGE override
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

// interceptorOps stages the fluence Python package (Model C): an init container
// copies it into a shared emptyDir, mounted into every workload container
// (skipping the sidecar) with PYTHONPATH + FLUENCE_POD_UID, so Python auto-imports
// the interceptor via sitecustomize. Broad mounting is safe (fail-soft when the
// vendor SDK is absent) and is required so a quantum WORKER — which runs the same
// image but does not request the resource — also gets the (faux-mode) interceptor.
func interceptorOps(pod *corev1.Pod) []spec.Op {
	var ops []spec.Op

	vol := corev1.Volume{Name: StageVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}
	if len(pod.Spec.Volumes) == 0 {
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/volumes", Value: []corev1.Volume{vol}})
	} else {
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/volumes/-", Value: vol})
	}

	initc := corev1.Container{
		Name:            "fluence-stage",
		Image:           sidecarImage(),
		ImagePullPolicy: corev1.PullAlways,
		Command: []string{"sh", "-c",
			fmt.Sprintf("python -m fluence.stage %s || echo '[fluence] staging skipped (interceptor unavailable)'", StageMountPath)},
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
	// so it can talk to the same backend (domain-agnostic: we propagate whatever
	// the workload pulls from a secret/configMap; existing FLUENCE_/FLUXION_ names
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
	sidecar := corev1.Container{
		Name: SidecarContainerName, Image: sidecarImage(), ImagePullPolicy: corev1.PullAlways,
		Env: env,
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
