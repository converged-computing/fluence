package handlers

import (
	"context"

	"github.com/converged-computing/fluence/pkg/webhook"
	"github.com/converged-computing/fluence/pkg/webhook/spec"

	corev1 "k8s.io/api/core/v1"
)

// Sidecar is the capability a handler uses to attach a coordination sidecar to a
// pod. It is NOT part of the webhook core's MutatorAPI: only handlers that need
// a sidecar (today, quantum) depend on it, and a handler may supply its own
// implementation to customize delivery. The default implementation
// (coreSidecar) delegates to the webhook core's interceptor/sidecar ops, which
// remain the staging mechanism shared by any sidecar-using handler.
//
// This is the seam your design calls for: "a general sidecar interface that can
// be used across handlers and customized by the quantum [handler]". A future
// custom-resource handler can implement Sidecar differently (different image,
// env, gating) without touching the core or other handlers.
type Sidecar interface {
	// EnsureRBAC provisions the per-namespace ServiceAccount/Role/Binding the
	// sidecar needs to read/patch pods and podgroups.
	EnsureRBAC(ctx context.Context, namespace string)
	// InterceptorOps stages the in-pod interceptor (Model C) into the workload
	// containers (init container + shared volume on PYTHONPATH).
	InterceptorOps(pod *corev1.Pod) []spec.Op
	// ContainerOps adds the sidecar container. observe=true selects observe-only
	// telemetry mode (no ungating). extraEnv carries handler-computed,
	// domain-specific env (e.g. the quantum handler's FLUENCE_EXPECTED_WORKERS =
	// N-1 and FLUENCE_WORKER_GROUP_BASE) so the core never has to know about
	// leader/worker concepts — the handler that owns the split owns those values.
	ContainerOps(pod *corev1.Pod, observe bool, extraEnv []corev1.EnvVar) []spec.Op
}

// coreSidecar is the default Sidecar. It delegates to the quantum-owned sidecar
// implementation (see sidecar_impl.go), which uses only the generic MutatorAPI
// (Client, InjectedEnv). The webhook core no longer carries any sidecar logic; a
// custom handler could supply its own Sidecar with a different container/image.
type coreSidecar struct{ m webhook.MutatorAPI }

func (s coreSidecar) EnsureRBAC(ctx context.Context, namespace string) {
	ensureSidecarRBAC(ctx, s.m, namespace)
}
func (s coreSidecar) InterceptorOps(pod *corev1.Pod) []spec.Op {
	return interceptorOps(pod)
}
func (s coreSidecar) ContainerOps(pod *corev1.Pod, observe bool, extraEnv []corev1.EnvVar) []spec.Op {
	return sidecarContainerOps(s.m, pod, observe, extraEnv)
}

// sidecarFor returns the Sidecar a handler should use. Centralized so the choice
// of implementation (and any future per-handler customization) lives in one
// place. Today every sidecar-using handler gets the core-backed default.
func sidecarFor(m webhook.MutatorAPI) Sidecar { return coreSidecar{m: m} }
