// Package handlers holds the webhook's resource handlers. Each handler
// self-registers (init -> webhook.Register) so the core never names it; a blank
// import of this package from the webhook server wiring is what activates them.
//
// Each handler owns its own domain policy and trigger condition. The webhook
// core knows none of it.
package handlers

import (
	"context"

	"github.com/converged-computing/fluence/pkg/webhook"
	"github.com/converged-computing/fluence/pkg/webhook/spec"

	corev1 "k8s.io/api/core/v1"
)

func init() {
	webhook.Register(&fluxionHandler{})
}

// fluxionHandler injects the FLUXION_* env contract into any container that
// requests a fluxion.flux-framework.org/* resource. Generic to all Fluxion
// resources — not quantum-specific. (Named for the domain, not "fluxion_env",
// since Fluxion concerns may grow beyond env injection.)
type fluxionHandler struct{}

func (h *fluxionHandler) Name() string { return "fluxion" }

func (h *fluxionHandler) Applies(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod) bool {
	return spec.PodRequestsFluxionResource(pod)
}

func (h *fluxionHandler) Mutate(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod) []spec.Op {
	return spec.InjectEnvOps(pod, m.InjectedEnv())
}
