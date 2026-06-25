package handlers

import (
	"context"
	"log"
	"strconv"

	"github.com/converged-computing/fluence/pkg/webhook"
	"github.com/converged-computing/fluence/pkg/webhook/spec"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	// Ensure the group's PodGroup exists with the resolved gang size, and link
	// this pod to it. EnsurePodGroup is idempotent (no-ops if the PodGroup
	// already exists — e.g. created by an earlier, more specific handler), so we
	// call it unconditionally. The gang handler knows nothing about leaders or
	// roles; that is a leader/worker concern handled by the quantum handler.
	// minCount = full gang size N (group-size annotation, else owner-derived);
	// see resolveMinCount.
	m.EnsurePodGroup(ctx, pod.Namespace, g, pod.Name, resolveMinCount(ctx, m, pod))
	return schedulingGroupOps(pod, g)
}

// resolveMinCount determines the gang's atomic-schedule size N:
//  1. explicit group-size annotation -> honor it verbatim. This is the override
//     for when minCount must differ from the parent's replica count (e.g. the
//     quantum leader/worker split, where the gang's N is expressed directly).
//  2. otherwise derive from the OWNING object: a Flux Operator MiniCluster pod
//     is owned by an indexed Job whose parallelism == completions == size == N.
//     (The operator sets Parallelism = Completions = MiniCluster.Spec.Size.)
//  3. otherwise default to 1, logged — never silently size a multi-pod gang to 1.
//
// The leader/worker (quantum) split is orthogonal and unchanged: it is driven by
// RoleAnnotation / QuantumResource in the quantum handler. minCount is always the
// FULL gang N regardless of which pods get gated.
func resolveMinCount(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod) int32 {
	// 1. explicit override
	if pod.Annotations != nil {
		if n := pod.Annotations[webhook.GroupSizeAnnotation]; n != "" {
			if v, err := strconv.Atoi(n); err == nil && v > 0 {
				return int32(v)
			}
		}
	}
	// 2. derive from the owning Job's parallelism
	if n := ownerJobN(ctx, m, pod); n > 0 {
		return n
	}
	// 3. no signal: a single-pod gang. Log so a missing size on a multi-pod
	// workload is visible rather than a silent gang-of-1.
	log.Printf("[fluence-webhook] group %s: no group-size annotation and no owning Job parallelism; defaulting minCount=1", webhook.GroupName(pod))
	return 1
}

// ownerJobN returns the parallelism (== size N) of the indexed Job that owns the
// pod, or 0 if there is no such owner. The Flux Operator sets a MiniCluster's
// Job Parallelism == Completions == size, so this is the full gang size N.
// Shared by the gang handler (classical: minCount = N) and the quantum handler
// (split: leader group = 1, worker group = N-1).
func ownerJobN(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod) int32 {
	c := m.Client()
	if c == nil {
		return 0
	}
	for _, ref := range pod.OwnerReferences {
		if ref.Kind != "Job" {
			continue
		}
		job, err := c.BatchV1().Jobs(pod.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return 0
		}
		if job.Spec.Parallelism != nil && *job.Spec.Parallelism > 0 {
			return *job.Spec.Parallelism
		}
		if job.Spec.Completions != nil && *job.Spec.Completions > 0 {
			return *job.Spec.Completions
		}
	}
	return 0
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
