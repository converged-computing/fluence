package handlers

import (
	"context"
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

	// Role is decided by ADMISSION ORDER, not resource request. In a pod-template
	// gang (Deployment/Job/StatefulSet) every pod has an identical spec — same
	// group label, every pod requests the quantum resource. The leader is simply
	// the first pod admitted (recorded on the PodGroup by the gang handler);
	// every other pod is a worker, regardless of its own resource request.
	if g != "" {
		leader := m.PodGroupLeader(ctx, pod.Namespace, g)
		if leader != "" && leader != pod.Name {
			log.Printf("[fluence-webhook] quantum worker %s/%s (leader=%s) — gating",
				pod.Namespace, pod.Name, leader)
			return gateOps(pod)
		}
	}

	// Submitter role: recorded group leader, or a standalone quantum pod. Always
	// gets the interceptor (so its task is tagged). It gets the SIDECAR only when
	// there is coordination to do: it is a group leader (workers to ungate), or
	// observe-only telemetry is requested. A standalone quantum pod with neither
	// has nothing to coordinate, so no sidecar is injected.
	isLeader := g != ""
	observe := spec.Label(pod, ObserveLabel) == "true"

	log.Printf("[fluence-webhook] quantum pod %s/%s — interceptor (leader=%v observe=%v)",
		pod.Namespace, pod.Name, isLeader, observe)

	ops := m.InterceptorOps(pod)
	if isLeader || observe {
		m.EnsureSidecarRBAC(ctx, pod.Namespace)
		ops = append(ops, m.SidecarContainerOps(pod, observe)...)
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
