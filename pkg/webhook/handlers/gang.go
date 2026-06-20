package handlers

import (
	"context"

	"github.com/converged-computing/fluence/pkg/webhook"
	"github.com/converged-computing/fluence/pkg/webhook/spec"

	corev1 "k8s.io/api/core/v1"
)

func init() {
	webhook.Register(&gangHandler{})
}

// gangHandler gang-schedules pods that carry the group label: it creates a
// Fluence-owned PodGroup (first pod admitted becomes the recorded leader) and
// links every pod to it via spec.schedulingGroup.podGroupName, which is the
// field the scheduler gangs by. It knows nothing about quantum — a purely
// classical gang is fully handled here, with no sidecar.
type gangHandler struct{}

func (h *gangHandler) Name() string { return "gang" }

func (h *gangHandler) Applies(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod) bool {
	return webhook.GroupName(pod) != ""
}

func (h *gangHandler) Mutate(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod) []spec.Op {
	g := webhook.GroupName(pod)
	// First pod admitted in the group creates the PodGroup and is recorded as
	// the admission-order leader. All pods are linked to the group.
	if m.PodGroupLeader(ctx, pod.Namespace, g) == "" {
		m.EnsurePodGroup(ctx, pod.Namespace, g, pod.Name)
		m.RecordLeader(ctx, pod.Namespace, g, pod.Name)
	}
	return schedulingGroupOps(pod, g)
}

// schedulingGroupOps links a pod to its PodGroup via the native 1.36 field
// spec.schedulingGroup.podGroupName. Idempotent if already linked.
func schedulingGroupOps(pod *corev1.Pod, group string) []spec.Op {
	if pod.Spec.SchedulingGroup != nil && pod.Spec.SchedulingGroup.PodGroupName != nil &&
		*pod.Spec.SchedulingGroup.PodGroupName == group {
		return nil
	}
	return []spec.Op{{
		Op:    "add",
		Path:  "/spec/schedulingGroup",
		Value: map[string]string{"podGroupName": group},
	}}
}
