package handlers

import (
	"context"
	"fmt"
	"time"

	"github.com/converged-computing/fluence/pkg/webhook"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Leader election is a LEADER/WORKER concern, not a core gang concern, so it
// lives with the handlers that need it (quantum) rather than on the webhook
// core's MutatorAPI. It records/reads the admission-order leader on the group's
// PodGroup via an annotation, used only when a workload does NOT declare an
// explicit role (RoleAnnotation). A purely classical gang never touches this.

// LeaderAnnotation records the admission-order leader on a PodGroup.
const LeaderAnnotation = "fluence.flux-framework.org/leader"

// podGroupLeader returns the recorded admission-order leader for the group, or
// "". Retries briefly to absorb the concurrent leader/worker admission race.
func podGroupLeader(ctx context.Context, m webhook.MutatorAPI, namespace, group string) string {
	c := m.Client()
	if c == nil || group == "" {
		return ""
	}
	for i := 0; i < 3; i++ {
		pg, err := c.SchedulingV1alpha2().PodGroups(namespace).Get(ctx, group, metav1.GetOptions{})
		if err != nil {
			return ""
		}
		if pg.Annotations != nil && pg.Annotations[LeaderAnnotation] != "" {
			return pg.Annotations[LeaderAnnotation]
		}
		if i < 2 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	return ""
}

// recordLeaderIfUnset records leaderPod as the group's admission-order leader if
// none is set yet. Best-effort; safe to call on every quantum admission.
func recordLeaderIfUnset(ctx context.Context, m webhook.MutatorAPI, namespace, group, leaderPod string) {
	c := m.Client()
	if c == nil || group == "" {
		return
	}
	if podGroupLeader(ctx, m, namespace, group) != "" {
		return
	}
	patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, LeaderAnnotation, leaderPod)
	if _, err := c.SchedulingV1alpha2().PodGroups(namespace).Patch(
		ctx, group, types.MergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
		// best-effort; the explicit RoleAnnotation path does not need this
		_ = err
	}
}

// leaderName is a tiny helper so callers read naturally.
func leaderName(pod *corev1.Pod) string { return pod.Name }
