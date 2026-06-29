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
// Model (producer/consumer split, no separate submitter pod). A quantum task's
// circuit comes from user code, so the pod that defines a task must RUN to submit
// it — submit and gate are mutually exclusive per pod. Gating therefore only
// helps pods that do NOT submit. A quantum gang in CoordinationShared mode is
// split, per pod, into two roles decided at admission:
//
//   - PRODUCER (one member, the indexed-Job completion index 0): its own
//     group-of-one <group>-producer (minCount 1) so it schedules alone and runs
//     the SINGLE real submit; staged with the interceptor in REAL (tag) mode and
//     given the sidecar, which polls the task and ungates the consumers at
//     position==1. NOT gated. The producer is one of the N members, so the
//     application is run exactly N times — never N+1.
//   - CONSUMERS (the other N-1 members): the <group> gang (minCount N-1), each
//     gated on quantum.braket/ready + preempting priority, told its role via
//     FLUENCE_COORDINATION_ROLE=consumer and handed the producer's task id via
//     FLUENCE_QUANTUM_JOB_ID. A consumer does NOT submit; it fetches the shared
//     result by that id. Ungated together when the producer's task is ready.
//
// In CoordinationIndependent mode (the default) there is no shared result to
// coordinate: every member is its own standalone producer (real submit, no gate),
// each owning its task and its own queue wait. A lone quantum pod (no group) is
// always standalone.
const (
	// QuantumResource is the Fluxion resource a pod requests to ask Fluence to
	// schedule quantum work. Requesting it is the sole trigger for this handler.
	QuantumResource = "fluxion.flux-framework.org/qpu"

	// QuantumGate holds a consumer pod unscheduled until the producer's task is
	// ready (the producer's sidecar removes it).
	QuantumGate = "quantum.braket/ready"

	// ObserveLabel opts a STANDALONE quantum pod (a group of one) into
	// observe-only telemetry: the sidecar is injected and polls queue position
	// but ungates nothing.
	ObserveLabel = "fluence.flux-framework.org/observe"

	// DependencyKindQuantumSubmit is the readiness Kind for the quantum resource
	// type: consumer pods wait for a quantum submission to reach the device queue.
	// First concrete instance of the general Dependency primitive (dependency.go).
	DependencyKindQuantumSubmit = "quantum-submit"

	// CoordinationAnnotation selects how a quantum gang is coordinated. It is an
	// open enum so future designs (e.g. index-paired "scatter") add a mode
	// without changing the mechanism.
	CoordinationAnnotation = "fluence.flux-framework.org/coordination"

	// CoordinationShared: one real task; the producer (index 0) submits and the
	// other members are gated consumers that fetch the producer's result. Each
	// member is told its role via FLUENCE_COORDINATION_ROLE; a role-aware workload
	// branches on it (producer submits, consumer fetches by FLUENCE_QUANTUM_JOB_ID).
	CoordinationShared = "shared"

	// CoordinationIndependent (default): every member does its own quantum work;
	// no coordination, no gating. Never invent coordination the user did not ask
	// for, and never dedup tasks meant to be distinct.
	CoordinationIndependent = "independent"

	// CoordinationBatch (brokered): one broker (index 0) submits ALL N tasks to
	// the same vendor queue, recording slot i -> task id_i. Each worker is gated
	// and ungated with ITS OWN id_i the moment task_i COMPLETES (per-result, not
	// broadcast). The broker holds the only node during the long vendor wait;
	// workers materialize just-in-time to post-process. This is the shared
	// machinery generalized from one shared result to N per-slot results -- it
	// trades a little worker-startup latency for a large node-time saving, since
	// N-1 workers no longer squat nodes through the queue wait. Submitting from
	// one pod (vs N) also lets the broker throttle to the vendor concurrency cap;
	// results returning out of order is a non-issue (each keyed to its slot id).
	CoordinationBatch = "batch"

	// ProducerGroupSuffix names the producer's own group-of-one: <group>-producer
	// (minCount 1) so it schedules alone and never deadlocks against the gated
	// consumer gang.
	ProducerGroupSuffix = "-producer"

	// CompletionIndexAnnotation is the indexed-Job completion index the Job
	// controller stamps on each pod; index "0" is the producer (deterministic
	// election with no recorded state).
	CompletionIndexAnnotation = "batch.kubernetes.io/job-completion-index"

	// ProducerIndex is the completion index promoted to producer.
	ProducerIndex = "0"

	// GangGroupEnv tells the producer's sidecar which consumer group label to list
	// and ungate when the task is ready.
	GangGroupEnv = "FLUENCE_GANG_GROUP"
)

// quantumHandler splits a shared quantum gang into a single producer (real
// submit + sidecar) and N-1 gated, role-aware consumers, or runs every member
// standalone in independent mode (see the package-level model comment). It is the
// only place in the webhook that knows about quantum resources, gates,
// coordination, or observe semantics.
type quantumHandler struct{}

func (h *quantumHandler) Name() string { return "quantum" }

// Applies to any pod requesting the quantum resource. Producers, consumers, and
// standalone quantum pods all request it; nothing without the resource needs
// quantum handling, so this is the single, unambiguous trigger.
func (h *quantumHandler) Applies(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod) bool {
	return spec.PodRequestsResource(pod, QuantumResource)
}

func (h *quantumHandler) Mutate(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod) []spec.Op {
	g := resolveGroup(pod)
	n := resolveGangSize(ctx, m, pod, g)
	mode := coordinationMode(pod)
	observe := spec.Label(pod, ObserveLabel) == "true"

	// No coordination: a standalone quantum pod, or an explicitly independent
	// member. The REAL submit happens in THIS pod; the sidecar is added only for
	// observe-only telemetry. (independent mode routes every member here -> N
	// standalone producers, each owning its task and its own queue wait.)
	if (mode != CoordinationShared && mode != CoordinationBatch) || g == "" || n <= 1 {
		ops := interceptorOps(pod)
		if observe {
			sc := sidecarFor(m)
			sc.EnsureRBAC(ctx, pod.Namespace)
			ops = append(ops, sc.ContainerOps(pod, true, nil)...)
		}
		log.Printf("[fluence-webhook] quantum %s/%s mode=%s (standalone/independent, observe=%v)",
			pod.Namespace, pod.Name, mode, observe)
		return ops
	}

	// shared / batch: same two roles. Promote one member to producer (index 0),
	// the rest are gated consumers. The mode (shared|batch) rides along and
	// decides the producer's submit fan-out and the sidecar's ungate strategy --
	// it does NOT create new roles.
	if isProducer(ctx, m, pod, g) {
		return h.mutateProducer(ctx, m, pod, g, mode, n)
	}
	return h.mutateConsumer(ctx, m, pod, g, n, mode)
}

// mutateProducer wires the single producer member (indexed-Job completion index
// 0): its own group-of-one <group>-producer (minCount 1) so it schedules alone
// and runs the REAL submit, the interceptor in tag mode, RBAC, and the sidecar
// told which consumer group to ungate (FLUENCE_GANG_GROUP). The producer is one
// of the N members, so the application is NOT run an extra time. Never gated.
func (h *quantumHandler) mutateProducer(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod, group, mode string, n int32) []spec.Op {
	pg := group + ProducerGroupSuffix
	m.EnsurePodGroup(ctx, pod.Namespace, pg, pod.Name, 1)
	ops := linkGroupOps(pod, pg)
	ops = append(ops, interceptorOps(pod)...)           // tag mode: the producer submits for real
	ops = append(ops, roleEnvOps(pod, RoleProducer)...) // FLUENCE_COORDINATION_ROLE=producer
	ops = append(ops, modeEnvOps(pod, mode)...)          // shared|batch: the producer's submit fan-out
	// the producer's workload needs N (in batch it submits N tasks).
	ops = append(ops, setContainerEnvOps(pod, corev1.EnvVar{Name: GangSizeEnv, Value: strconv.Itoa(int(n))})...)
	sc := sidecarFor(m)
	sc.EnsureRBAC(ctx, pod.Namespace)
	// The sidecar needs the gang group (whom to ungate), the mode (broadcast one
	// id vs release each consumer by its own per-slot result), and N (in batch,
	// the number of tasks/slots to map).
	extra := []corev1.EnvVar{
		{Name: GangGroupEnv, Value: group},
		{Name: CoordinationModeEnv, Value: mode},
		{Name: GangSizeEnv, Value: strconv.Itoa(int(n))},
	}
	ops = append(ops, sc.ContainerOps(pod, false, extra)...)
	log.Printf("[fluence-webhook] quantum producer %s/%s — group %s, mode=%s, N=%d (ungates consumers %q)",
		pod.Namespace, pod.Name, pg, mode, n, group)
	return ops
}

// mutateConsumer wires a non-producer member: it joins the <group> consumer gang
// (minCount N-1) and is gated until the producer's task is ready. It is told its
// role (FLUENCE_COORDINATION_ROLE=consumer) and handed the producer's task id
// (FLUENCE_QUANTUM_JOB_ID, stamped on the pod by the sidecar at ungate). A
// role-aware consumer reads those and fetches the shared result instead of
// submitting — so the consumer never calls the vendor submit, and needs neither
// the interceptor nor a faux flag.
func (h *quantumHandler) mutateConsumer(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod, group string, n int32, mode string) []spec.Op {
	m.EnsurePodGroup(ctx, pod.Namespace, group, pod.Name, n-1)
	ops := linkGroupOps(pod, group)
	// Express the wait as the GENERAL dependency primitive: this consumer depends
	// on the quantum submission produced by <group>-producer, held by the quantum
	// gate. applyOps gates the pod, raises priority, and stamps depends-on-*.
	dep := Dependency{Kind: DependencyKindQuantumSubmit, Producer: group + ProducerGroupSuffix, Gate: QuantumGate}
	ops = append(ops, dep.applyOps(pod)...)
	ops = append(ops, consumerEnvOps(pod)...)
	ops = append(ops, modeEnvOps(pod, mode)...) // shared|batch: how the sidecar releases this consumer
	// A gated consumer never runs the QPU task — it only fetches the producer's
	// shared result — so it must not hold the Fluxion quantum resource. Leaving it
	// would make Fluxion allocate a qpu per consumer, capping the gang at the
	// backend's graph qpu count and, on a single-slot real QPU, leaving the
	// consumers unschedulable. Applies() already routed this pod on the request, so
	// stripping it here is safe.
	ops = append(ops, dropQuantumResourceOps(pod)...)
	log.Printf("[fluence-webhook] quantum consumer %s/%s — group %s minCount=%d, gated (role=consumer, qpu stripped)",
		pod.Namespace, pod.Name, group, n-1)
	return ops
}

// dropQuantumResourceOps removes the Fluxion quantum resource from a consumer's
// containers (requests and limits), returning the patch ops and mutating pod in
// place. Only entries that are present are removed (a JSON-patch remove on a
// missing path would fail). The sidecar container is never a consumer concern.
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

// coordinationMode reads the coordination annotation; default independent.
func coordinationMode(pod *corev1.Pod) string {
	if v := spec.Annotation(pod, CoordinationAnnotation); v != "" {
		return v
	}
	return CoordinationIndependent
}

// isProducer decides whether THIS pod is the gang's single producer. Indexed Job
// (recommended): completion index 0 is the producer — deterministic, race-free,
// no recorded state. Otherwise: first arrival claims the producer slot by the
// absence of the producer PodGroup (best-effort under concurrent admission;
// prefer an indexed Job for determinism). Indexing a nil annotations map yields
// ok=false, so the indexed branch is nil-safe.
func isProducer(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod, group string) bool {
	if idx, ok := pod.Annotations[CompletionIndexAnnotation]; ok {
		return idx == ProducerIndex
	}
	c := m.Client()
	if c == nil {
		return true // tests / no client: treat as producer
	}
	pg := group + ProducerGroupSuffix
	if _, err := c.SchedulingV1alpha2().PodGroups(pod.Namespace).Get(ctx, pg, metav1.GetOptions{}); err == nil {
		return false // already claimed by an earlier arrival
	}
	return true
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

// linkGroupOps ensures the gang pod carries the group label (so the producer's
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

// ── coordination role (producer / consumer) ─────────────────────────────────────
//
// In a shared gang each member is told its role positively, so the application
// branches on it instead of relying on any submit-interception magic:
//   producer  submits the one real task (and is tagged so the sidecar finds it);
//   consumer  does NOT submit — it reads the producer's task id and fetches the
//             shared result (e.g. via the vendor's S3-backed result API).
// The role is decided at admission by isProducer (completion index 0, else the
// producer-group claim) and surfaced as FLUENCE_COORDINATION_ROLE. Because the
// election is the webhook's, this env is the single source of truth — the
// container never re-derives its role from the Job index (which loose, non-Job
// pods don't even have).

const (
	// CoordinationRoleEnv carries the pod's role in a shared gang. A role-aware
	// workload branches on it: RoleProducer submits, RoleConsumer fetches the
	// shared result by id. Unset for standalone/independent pods (they all submit).
	CoordinationRoleEnv = "FLUENCE_COORDINATION_ROLE"
	RoleProducer        = "producer"
	RoleConsumer        = "consumer"

	// CoordinationModeEnv carries the coordination mode (shared|batch) to the
	// workload and the sidecar. Roles are unchanged across modes -- the producer
	// (index 0) always submits and runs the sidecar; consumers are always gated.
	// Only WHAT the producer submits (one task vs N) and HOW the sidecar ungates
	// (one shared id broadcast vs each consumer released by its own per-slot
	// result) differ, and that is exactly what this mode selects.
	CoordinationModeEnv = "FLUENCE_COORDINATION_MODE"
	// GangSizeEnv tells the producer the gang size N (in batch it submits N).
	GangSizeEnv = "FLUENCE_GANG_SIZE"

	// QuantumJobIDAnnotation is the vendor-neutral task id the ungating sidecar
	// stamps on each consumer (mirrors python/fluence/ungate.py JOB_ID_ANNOTATION),
	// BEFORE removing the gate. Surfaced into FLUENCE_QUANTUM_JOB_ID via the
	// downward API so a consumer can fetch the producer's result by id.
	QuantumJobIDAnnotation = "fluence.flux-framework.org/quantum-job-id"

	// QuantumJobIDEnv is the env a consumer reads for the producer's task id.
	QuantumJobIDEnv = "FLUENCE_QUANTUM_JOB_ID"
)

// roleEnvOps sets FLUENCE_COORDINATION_ROLE=<role> on each non-sidecar container.
func roleEnvOps(pod *corev1.Pod, role string) []spec.Op {
	return setContainerEnvOps(pod, corev1.EnvVar{Name: CoordinationRoleEnv, Value: role})
}

// consumerEnvOps tells a consumer its role and hands it the producer's task id
// (FLUENCE_QUANTUM_JOB_ID, downward API from the annotation the ungating sidecar
// stamps). A consumer never submits, so it gets neither the interceptor nor any
// faux flag — just its role and the id to fetch the shared result with.
func consumerEnvOps(pod *corev1.Pod) []spec.Op {
	ops := roleEnvOps(pod, RoleConsumer)
	ops = append(ops, setContainerEnvOps(pod, spec.AnnotationEnv(QuantumJobIDEnv, QuantumJobIDAnnotation))...)
	return ops
}

// modeEnvOps sets FLUENCE_COORDINATION_MODE=<mode> on each non-sidecar container.
func modeEnvOps(pod *corev1.Pod, mode string) []spec.Op {
	return setContainerEnvOps(pod, corev1.EnvVar{Name: CoordinationModeEnv, Value: mode})
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
	defaultSidecarImage = "vanessa/fluence-sidecar:latest"

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
// the interceptor via sitecustomize, which tags the vendor submit so the sidecar
// can find the task. Added to producers and standalone/independent pods (the ones
// that actually submit); consumers don't submit, so they don't get it.
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
