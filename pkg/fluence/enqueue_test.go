//go:build cgo

/*
Copyright 2024 Lawrence Livermore National Security, LLC
 (c.f. AUTHORS, NOTICE.LLNS, COPYING)
SPDX-License-Identifier: Apache-2.0
*/

// Unit tests for the requeue QueueingHint. Tagged cgo because the package links
// the Fluxion matcher; runs in CI (fluence-base) via `make test`.
package fluence

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
)

func pod(phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{Status: corev1.PodStatus{Phase: phase}}
}

func TestQueueingHint(t *testing.T) {
	f := &Fluence{}
	lg := klog.Background()
	waiting := pod(corev1.PodPending) // the rejected gang pod (unused by the hint)

	cases := []struct {
		name   string
		oldObj interface{}
		newObj interface{}
		want   fwk.QueueingHint
	}{
		{"pod deleted frees capacity",
			pod(corev1.PodRunning), nil, fwk.Queue},
		{"pod became Succeeded frees capacity",
			pod(corev1.PodRunning), pod(corev1.PodSucceeded), fwk.Queue},
		{"pod became Failed frees capacity",
			pod(corev1.PodRunning), pod(corev1.PodFailed), fwk.Queue},
		{"already-terminal update does not re-fire",
			pod(corev1.PodSucceeded), pod(corev1.PodSucceeded), fwk.QueueSkip},
		{"pod still running is irrelevant",
			pod(corev1.PodRunning), pod(corev1.PodRunning), fwk.QueueSkip},
		{"pod created (pending) does not free capacity",
			nil, pod(corev1.PodPending), fwk.QueueSkip},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := f.isPodCapacityChange(lg, waiting, c.oldObj, c.newObj)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("hint = %v, want %v", got, c.want)
			}
		})
	}
}

// The plugin must advertise the capacity-freeing events.
func TestEventsToRegister(t *testing.T) {
	f := &Fluence{}
	evts, err := f.EventsToRegister(context.Background())
	if err != nil {
		t.Fatalf("EventsToRegister error: %v", err)
	}
	if len(evts) == 0 {
		t.Fatal("no events registered — unschedulable gangs would never wake on capacity change")
	}
	var podUpdate, podDelete, node bool
	for _, e := range evts {
		switch {
		case e.Event.Resource == fwk.Pod && e.Event.ActionType&fwk.Update != 0:
			podUpdate = true
		case e.Event.Resource == fwk.Pod && e.Event.ActionType&fwk.Delete != 0:
			podDelete = true
		case e.Event.Resource == fwk.Node:
			node = true
		}
	}
	if !podUpdate || !podDelete || !node {
		t.Errorf("missing events: podUpdate=%v podDelete=%v node=%v", podUpdate, podDelete, node)
	}
}
