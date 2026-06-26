package webhook

import (
	"context"

	"github.com/converged-computing/fluence/pkg/webhook/spec"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

// MutatorAPI is the capability surface the webhook core exposes to handlers.
// Handlers live in a subpackage and depend only on this interface, never on the
// concrete Mutator — so the core never has to import the handlers package, and
// handlers cannot reach into core internals. The concrete *Mutator implements
// this.
//
// The core knows nothing about what any handler does with these capabilities;
// quantum-specific policy (resource names, gate names, observe label) lives
// entirely in the quantum handler.
type MutatorAPI interface {
	// Client returns the Kubernetes client (may be nil in unit tests).
	Client() kubernetes.Interface

	// InjectedEnv is the FLUXION_* env contract the scheduler/webhook supplies.
	InjectedEnv() []corev1.EnvVar

	// PodGroup operations (gang scheduling). Group identity is the value of the
	// group label, which the core treats as an opaque string.
	PodGroupLeader(ctx context.Context, namespace, group string) string
	EnsurePodGroup(ctx context.Context, namespace, group, leaderPod string, minCount int32)
	RecordLeader(ctx context.Context, namespace, group, leaderPod string)

	// EnsureSidecarRBAC provisions the per-namespace ServiceAccount/Role/Binding
	// the sidecar needs.
	EnsureSidecarRBAC(ctx context.Context, namespace string)

	// InterceptorOps stages the fluence package into the quantum container via an
	// init container + shared volume on PYTHONPATH (Model C). SidecarContainerOps
	// adds the sidecar container (observe=true => observe-only telemetry mode).
	InterceptorOps(pod *corev1.Pod) []spec.Op
	SidecarContainerOps(pod *corev1.Pod, observe bool) []spec.Op
}

// Handler inspects a pod and, when it applies, contributes JSON patch ops. A pod
// flows through every registered handler whose Applies returns true; their ops
// are concatenated. Applies is fully general — it receives the pod and the
// MutatorAPI, so a handler may consult cluster state (e.g. resolve a group's
// leader) in deciding whether it applies.
type Handler interface {
	Name() string
	Applies(ctx context.Context, m MutatorAPI, pod *corev1.Pod) bool
	Mutate(ctx context.Context, m MutatorAPI, pod *corev1.Pod) []spec.Op
}

// ── registration ────────────────────────────────────────────────────────────────
//
// Handlers self-register via Register() from their package's init(). The core
// never names a handler; importing the handlers package (a blank import in the
// webhook server wiring) is what populates the registry. This keeps the core
// domain-agnostic: adding or removing a handler does not touch core code.

var registry []Handler

// Register adds a handler to the global set. Called from handler packages'
// init(). Order of registration is the order handlers run.
func Register(h Handler) {
	registry = append(registry, h)
}

// registered returns the registered handlers (the live registry).
func registered() []Handler {
	return registry
}
