/*
Copyright 2024 Lawrence Livermore National Security, LLC
 (c.f. AUTHORS, NOTICE.LLNS, COPYING)
SPDX-License-Identifier: Apache-2.0
*/

// Two-group quantum split: a quantum gang of size N becomes a leader PodGroup
// <group> (minCount 1) and a worker PodGroup <group>-workers (minCount N-1).
// minCount is derived from the owning Job's parallelism (N).
package handlers

import (
	"context"
	"testing"

	"github.com/converged-computing/fluence/pkg/webhook"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func qpuLeader(ns, group, name, job string) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: ns,
			Labels:          map[string]string{webhook.GroupLabel: group},
			Annotations:     map[string]string{webhook.RoleAnnotation: RoleLeader},
			OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: job}},
		},
		Spec: corev1.PodSpec{
			SchedulerName: webhook.SchedulerName,
			Containers: []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{QuantumResource: *resource.NewQuantity(1, resource.DecimalSI)}}}},
		},
	}
	return p
}

func qpuWorker(ns, group, name, job string) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: ns,
			Labels:          map[string]string{webhook.GroupLabel: group},
			Annotations:     map[string]string{webhook.RoleAnnotation: RoleWorker},
			OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: job}},
		},
		Spec: corev1.PodSpec{
			SchedulerName: webhook.SchedulerName,
			Containers:    []corev1.Container{{Name: "w"}},
		},
	}
	return p
}

func mincount(t *testing.T, cs *fake.Clientset, ns, group string) (int32, bool) {
	t.Helper()
	pg, err := cs.SchedulingV1alpha2().PodGroups(ns).Get(context.Background(), group, metav1.GetOptions{})
	if err != nil {
		return 0, false
	}
	if pg.Spec.SchedulingPolicy.Gang == nil {
		return 0, false
	}
	return pg.Spec.SchedulingPolicy.Gang.MinCount, true
}

// Quantum gang of size N=4 owned by a Job(parallelism=4): leader group minCount
// 1, worker group <group>-workers minCount 3.
func TestQuantumSplitLeaderOneWorkersNMinus1(t *testing.T) {
	ns, group, job := "default", "qg", "qg-job"
	par := int32(4)
	jobObj := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: job, Namespace: ns},
		Spec:       batchv1.JobSpec{Parallelism: &par, Completions: &par},
	}
	cs := fake.NewSimpleClientset(jobObj)
	m := &webhook.Mutator{Clientset: cs}

	// leader admitted first
	m.Mutate(context.Background(), qpuLeader(ns, group, "qg-0", job))
	// then a worker
	m.Mutate(context.Background(), qpuWorker(ns, group, "qg-1", job))

	if mc, ok := mincount(t, cs, ns, group); !ok || mc != 1 {
		t.Errorf("leader group %q minCount=%d (ok=%v), want 1", group, mc, ok)
	}
	wg := group + WorkerGroupSuffix
	if mc, ok := mincount(t, cs, ns, wg); !ok || mc != 3 {
		t.Errorf("worker group %q minCount=%d (ok=%v), want 3 (N-1)", wg, mc, ok)
	}
}

// The worker is relinked into <group>-workers (label + schedulingGroup op).
func TestQuantumWorkerRelinkedToWorkerGroup(t *testing.T) {
	ns, group, job := "default", "qg2", "qg2-job"
	par := int32(3)
	cs := fake.NewSimpleClientset(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: job, Namespace: ns},
		Spec:       batchv1.JobSpec{Parallelism: &par, Completions: &par}})
	m := &webhook.Mutator{Clientset: cs}
	m.Mutate(context.Background(), qpuLeader(ns, group, "qg2-0", job))

	ops := m.Mutate(context.Background(), qpuWorker(ns, group, "qg2-1", job))
	wg := group + WorkerGroupSuffix
	var relinked bool
	for _, op := range ops {
		if v, ok := op.Value.(map[string]string); ok && v["podGroupName"] == wg {
			relinked = true
		}
	}
	if !relinked {
		t.Errorf("worker not relinked to %q (ops: %+v)", wg, ops)
	}
}
