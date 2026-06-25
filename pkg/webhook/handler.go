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

	// EnsurePodGroup creates the group's PodGroup with the given gang minCount if
	// it does not already exist (idempotent). Group identity is the opaque value
	// of the group label. Leader election is NOT here — it is a leader/worker
	// concern owned by the handlers that need it (see handlers/leader.go).
	EnsurePodGroup(ctx context.Context, namespace, group, leaderPod string, minCount int32)

	// Sidecar staging primitives. These remain on the core because the default
	// Sidecar implementation (coreSidecar) delegates to them, but handlers do
	// NOT use them directly — they go through the handlers.Sidecar interface,
	// which is the customization seam. Kept here (not removed) so the concrete
	// *Mutator continues to satisfy both this interface and coreSidecar's needs.
	EnsureSidecarRBAC(ctx context.Context, namespace string)
	InterceptorOps(pod *corev1.Pod) []spec.Op
	SidecarContainerOps(pod *corev1.Pod, observe bool, extraEnv []corev1.EnvVar) []spec.Op
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

// DefaultHandlerOrder is the active set AND the dispatch order when the operator
// passes no --handlers flag. Order matters: specific handlers run before the
// generic gang fallback, so "gang" is LAST — it applies default gang sizing
// (group-size annotation or owner-derived N) only if no earlier handler already
// shaped the gang. To change the order or disable a handler, pass a different
// list (e.g. --handlers=fluxion,gang drops quantum).
var DefaultHandlerOrder = []string{"fluxion", "quantum", "gang"}

// ── registration ────────────────────────────────────────────────────────────────
//
// Handlers self-register via Register() from their package's init(). The core
// never names a handler; importing the handlers package (a blank import in the
// webhook server wiring) is what populates the registry. This keeps the core
// domain-agnostic: adding or removing a handler does not touch core code.

// available maps a handler's Name() to the handler. Populated by Register() from
// each handler package's init(). This is the set of handlers that EXIST; which
// ones actually run, and in what order, is decided by activeOrder.
var available = map[string]Handler{}

// activeOrder is the ordered list of handler names to dispatch. It is BOTH the
// selection (names not present are disabled) and the order (dispatch follows the
// slice). Defaults to DefaultHandlerOrder; overridden by SetActiveHandlers.
var activeOrder = append([]string(nil), DefaultHandlerOrder...)

// Register adds a handler to the available set under its Name(). Called from
// handler packages' init().
func Register(h Handler) {
	available[h.Name()] = h
}

// SetActiveHandlers sets the active, ordered handler list (the --handlers value).
// Empty/nil restores DefaultHandlerOrder. Names with no registered handler are
// dropped and returned as `unknown` so the caller can warn. Order is preserved
// exactly as given — the list is the dispatch order.
func SetActiveHandlers(names []string) (active, unknown []string) {
	if len(names) == 0 {
		activeOrder = append([]string(nil), DefaultHandlerOrder...)
		return activeOrder, nil
	}
	var ordered []string
	for _, n := range names {
		if _, ok := available[n]; ok {
			ordered = append(ordered, n)
		} else {
			unknown = append(unknown, n)
		}
	}
	activeOrder = ordered
	return activeOrder, unknown
}

// ActiveHandlerNames returns the active dispatch order (for logging at startup).
func ActiveHandlerNames() []string {
	return append([]string(nil), activeOrder...)
}

// registered returns the active handlers, resolved from activeOrder, in order.
// Names in the order with no registered handler are skipped (already warned at
// SetActiveHandlers time).
func registered() []Handler {
	out := make([]Handler, 0, len(activeOrder))
	for _, n := range activeOrder {
		if h, ok := available[n]; ok {
			out = append(out, h)
		}
	}
	return out
}
