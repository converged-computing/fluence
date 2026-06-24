package handlers

import (
	"context"
	"fmt"
	"log"

	"github.com/converged-computing/fluence/pkg/webhook"
	"github.com/converged-computing/fluence/pkg/webhook/spec"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func init() {
	webhook.Register(&quantumHandler{})
}

// Quantum-specific policy. The webhook core knows NONE of these — they live
// only here, in the quantum handler.
const (
	// QuantumResource is the Fluxion resource a pod requests when it wants
	// Fluence to schedule quantum work. Requesting it is the trigger for sidecar
	// + interceptor injection.
	QuantumResource = "fluxion.flux-framework.org/qpu"

	// QuantumGate holds a classical worker until the leader's quantum task is
	// ready (the sidecar removes it).
	QuantumGate = "quantum.braket/ready"

	// ObserveLabel opts a standalone quantum pod into observe-only telemetry:
	// the sidecar is injected and polls queue position but ungates nothing.
	ObserveLabel = "fluence.flux-framework.org/observe"

	// Role values for webhook.RoleAnnotation.
	RoleLeader = "leader"
	RoleWorker = "worker"
)

// quantumHandler coordinates quantum-classical workflows. It applies to a pod
// in either role:
//   - the quantum submitter (requests QuantumResource): inject the interceptor,
//     plus the sidecar when there is coordination to do (group leader, or
//     observe-only telemetry requested);
//   - a classical worker (a non-leader member of a group whose leader is a
//     quantum pod): gate it until the leader's task is ready.
//
// This is the only place in the webhook that knows about quantum resources,
// gates, or observe semantics.
type quantumHandler struct{}

func (h *quantumHandler) Name() string { return "quantum" }

func (h *quantumHandler) Applies(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod) bool {
	if spec.PodRequestsResource(pod, QuantumResource) {
		return true
	}
	// An explicitly-declared worker applies (so it gets gated) even if it
	// doesn't request the quantum resource and the leader isn't recorded yet —
	// this removes the admission-order race for explicitly-roled gangs.
	if webhook.Role(pod) == RoleWorker && webhook.GroupName(pod) != "" {
		return true
	}
	return h.isWorkerOfQuantumGroup(ctx, m, pod)
}

// isWorkerOfQuantumGroup reports whether pod is a non-leader member of a group
// whose recorded leader is a quantum (QuantumResource-requesting) pod. Workers
// are classical and do not request the resource themselves, so their role is a
// property of group membership, resolved against cluster state.
func (h *quantumHandler) isWorkerOfQuantumGroup(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod) bool {
	g := webhook.GroupName(pod)
	if g == "" || m.Client() == nil {
		return false
	}
	leader := m.PodGroupLeader(ctx, pod.Namespace, g)
	if leader == "" || leader == pod.Name {
		return false
	}
	lp, err := m.Client().CoreV1().Pods(pod.Namespace).Get(ctx, leader, metav1.GetOptions{})
	if err != nil {
		return false
	}
	return spec.PodRequestsResource(lp, QuantumResource)
}

func (h *quantumHandler) Mutate(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod) []spec.Op {
	g := webhook.GroupName(pod)

	// Determine role. An explicit role annotation is AUTHORITATIVE: the workload
	// declares which pod leads and which wait, and Fluence honors it directly —
	// no admission-order race, and the same value is echoed to the app as
	// FLUENCE_ROLE so the webhook's notion of leader and the application's notion
	// cannot disagree. When the annotation is absent, fall back to the legacy
	// behavior: role is decided by admission order (the first pod admitted in the
	// group, recorded on the PodGroup by the gang handler). The admission-order
	// path suits a homogeneous pod-template gang where every pod is identical;
	// the explicit annotation suits a heterogeneous leader/worker gang.
	role := webhook.Role(pod)
	var isWorker bool
	switch role {
	case RoleWorker:
		isWorker = true
	case RoleLeader:
		isWorker = false
	default:
		if g != "" {
			leader := m.PodGroupLeader(ctx, pod.Namespace, g)
			isWorker = leader != "" && leader != pod.Name
		}
	}

	if g != "" && isWorker {
		log.Printf("[fluence-webhook] quantum worker %s/%s (role=%q) — gating",
			pod.Namespace, pod.Name, role)
		ops := gateOps(pod)
		ops = append(ops, roleEnvOps(pod, RoleWorker)...)
		return ops
	}

	// Submitter/leader role: recorded or declared group leader, or a standalone
	// quantum pod. Always gets the interceptor (so its task is tagged). It gets
	// the SIDECAR only when there is coordination to do: it is a group leader
	// (workers to ungate), or observe-only telemetry is requested.
	isLeader := g != ""
	observe := spec.Label(pod, ObserveLabel) == "true"

	log.Printf("[fluence-webhook] quantum pod %s/%s — interceptor (leader=%v role=%q observe=%v)",
		pod.Namespace, pod.Name, isLeader, role, observe)

	ops := m.InterceptorOps(pod)
	ops = append(ops, roleEnvOps(pod, RoleLeader)...)
	if isLeader || observe {
		m.EnsureSidecarRBAC(ctx, pod.Namespace)
		ops = append(ops, m.SidecarContainerOps(pod, observe)...)
	}
	return ops
}

// roleEnvOps injects FLUENCE_ROLE into every (non-sidecar) container so the
// application reads its gang role from the same source of truth the webhook
// used. effectiveRole is what the webhook decided (leader/worker), used only
// when the pod carries no explicit role annotation; when the annotation is
// present we source the value from it via the downward API so the two always
// agree. Unlike InterceptorOps, this is NOT limited to Fluxion-resource
// containers — worker containers do not request the quantum resource but still
// need to know they are workers.
func roleEnvOps(pod *corev1.Pod, effectiveRole string) []spec.Op {
	var value corev1.EnvVar
	if webhook.Role(pod) != "" {
		value = spec.AnnotationEnv("FLUENCE_ROLE", webhook.RoleAnnotation)
	} else {
		value = corev1.EnvVar{Name: "FLUENCE_ROLE", Value: effectiveRole}
	}
	var ops []spec.Op
	for i, c := range pod.Spec.Containers {
		if c.Name == "fluence-sidecar" || spec.HasEnv(c, "FLUENCE_ROLE") {
			continue
		}
		if len(c.Env) == 0 {
			ops = append(ops, spec.Op{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/env", i), Value: []corev1.EnvVar{value}})
		} else {
			ops = append(ops, spec.Op{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/env/-", i), Value: value})
		}
		pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, value)
	}
	return ops
}

// gateOps adds the quantum scheduling gate (idempotent).
const QuantumClassicalPriorityClass = "fluence-quantum-classical"

func gateOps(pod *corev1.Pod) []spec.Op {
	for _, g := range pod.Spec.SchedulingGates {
		if g.Name == QuantumGate {
			return nil
		}
	}
	var ops []spec.Op
	gate := corev1.PodSchedulingGate{Name: QuantumGate}
	if len(pod.Spec.SchedulingGates) == 0 {
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/schedulingGates", Value: []corev1.PodSchedulingGate{gate}})
	} else {
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/schedulingGates/-", Value: gate})
	}
	// Give gated classical workers a raised priority so they schedule reliably
	// once ungated. priorityClassName is immutable post-creation, so it MUST be
	// set here at admission, not at ungate time. Only set it if the pod doesn't
	// already declare one (don't overwrite a user's class).
	if pod.Spec.PriorityClassName == "" {
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/priorityClassName", Value: QuantumClassicalPriorityClass})
		// Clear spec.priority so the priority admission controller recomputes it
		// from the class. The controller errors only when spec.priority is
		// non-nil AND differs from the class value; setting it to null avoids
		// that in every case. We use add-with-null (not remove): a JSON Patch
		// "remove" of an absent path is a hard error, and whether the API has
		// already defaulted spec.priority differs across clusters/k8s versions
		// (it broke in CI but not on GKE, or vice versa). add-null is valid
		// whether the field is absent, 0, or set.
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/priority", EmitNull: true})
	}
	return ops
}
