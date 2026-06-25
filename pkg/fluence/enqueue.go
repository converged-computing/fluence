/*
Copyright 2024 Lawrence Livermore National Security, LLC
 (c.f. AUTHORS, NOTICE.LLNS, COPYING)
SPDX-License-Identifier: Apache-2.0
*/

package fluence

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
)

// Fluence implements EnqueueExtensions so that a gang rejected as Unschedulable
// (because the node pool was full) is RE-ATTEMPTED when capacity frees up.
//
// Without this, a losing gang sits in the unschedulable queue until the
// scheduler's periodic backoff flush — it is NOT woken when another gang
// finishes and releases nodes. For the contention experiment (submit more gang
// demand than the cluster holds, watch gangs drain as others complete) that
// means contended gangs stall instead of draining promptly. The capacity-freeing
// events are: a pod terminating (Succeeded/Failed — batch apps Complete and
// linger before deletion, so Update catches it earlier than Delete), a pod being
// deleted, and node capacity appearing/growing.
var _ fwk.EnqueueExtensions = (*Fluence)(nil)

// EventsToRegister declares the cluster events that may let a previously
// Unschedulable Fluence gang schedule, each with a QueueingHint that filters out
// events which cannot plausibly free capacity (so we do not churn the queue).
func (f *Fluence) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		// A pod going terminal (Succeeded/Failed) frees its node BEFORE deletion;
		// this is the event that actually fires when a batch gang completes.
		{Event: fwk.ClusterEvent{Resource: fwk.Pod, ActionType: fwk.Update},
			QueueingHintFn: f.isPodCapacityChange},
		// A pod being deleted frees its node.
		{Event: fwk.ClusterEvent{Resource: fwk.Pod, ActionType: fwk.Delete},
			QueueingHintFn: f.isPodCapacityChange},
		// New node, or a node's allocatable growing, adds capacity.
		{Event: fwk.ClusterEvent{Resource: fwk.Node,
			ActionType: fwk.Add | fwk.UpdateNodeAllocatable}},
	}, nil
}

// isPodCapacityChange returns Queue when the pod event plausibly frees node
// capacity for a waiting gang — i.e. another pod terminated or was deleted.
// Anything else (a pod being created, an unrelated label change) returns
// QueueSkip so the waiting gang is not retried pointlessly.
//
// We do not try to be clever about which specific nodes freed: any capacity
// release can change a Fluxion match, and PreFilter re-matches the whole graph
// on retry. The hint just suppresses the obviously-irrelevant events.
func (f *Fluence) isPodCapacityChange(
	logger klog.Logger, _ *corev1.Pod, oldObj, newObj interface{},
) (fwk.QueueingHint, error) {
	// Delete event: newObj is nil, oldObj is the deleted pod -> frees capacity.
	if newObj == nil {
		if _, ok := oldObj.(*corev1.Pod); ok {
			return fwk.Queue, nil
		}
		return fwk.QueueSkip, nil
	}
	// Update event: queue only when the pod BECOMES terminal (was running, now
	// Succeeded/Failed) — that is the moment its node frees.
	newPod, ok := newObj.(*corev1.Pod)
	if !ok {
		return fwk.QueueSkip, nil
	}
	if !isTerminalPhase(newPod.Status.Phase) {
		return fwk.QueueSkip, nil
	}
	// If we can see the old object, only fire on the transition INTO terminal
	// (avoid re-queuing on every update of an already-finished pod).
	if oldPod, ok := oldObj.(*corev1.Pod); ok && isTerminalPhase(oldPod.Status.Phase) {
		return fwk.QueueSkip, nil
	}
	return fwk.Queue, nil
}

// isTerminalPhase reports whether a pod phase means its node capacity is released.
func isTerminalPhase(p corev1.PodPhase) bool {
	return p == corev1.PodSucceeded || p == corev1.PodFailed
}