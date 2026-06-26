/*
Copyright 2024 Lawrence Livermore National Security, LLC
 (c.f. AUTHORS, NOTICE.LLNS, COPYING)
SPDX-License-Identifier: Apache-2.0
*/

// Tests for gang PodGroup minCount: the whole gang (full N) must schedule
// atomically. Regression guard for the bug where every PodGroup was created
// with minCount=1, so a multi-pod gang was "satisfied" by a single pod and the
// rest were stranded (partial placement).
package handlers

import (
	"context"
	"testing"

	"strconv"

	"github.com/converged-computing/fluence/pkg/webhook"

	corev1 "k8s.io/api/core/v1"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

// minCountOf runs the gang handler for the leader pod of a group and returns the
// minCount of the PodGroup the webhook created.
func minCountOf(t *testing.T, pod *corev1.Pod) int32 {
	t.Helper()
	m := &webhook.Mutator{Clientset: fake.NewSimpleClientset()}
	m.Mutate(context.Background(), pod)
	pg, err := m.Clientset.SchedulingV1alpha2().
		PodGroups(pod.Namespace).Get(context.Background(), webhook.GroupName(pod), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("PodGroup not created: %v", err)
	}
	if pg.Spec.SchedulingPolicy.Gang == nil {
		t.Fatal("PodGroup has no gang scheduling policy")
	}
	return pg.Spec.SchedulingPolicy.Gang.MinCount
}

// minCountWithClient runs the gang handler with a pre-seeded clientset (so the
// owning Job exists) and returns the created PodGroup's minCount.
func minCountWithClient(t *testing.T, pod *corev1.Pod, objs ...interface{}) int32 {
	t.Helper()
	cs := fake.NewSimpleClientset(toRuntime(objs)...)
	m := &webhook.Mutator{Clientset: cs}
	m.Mutate(context.Background(), pod)
	pg, err := cs.SchedulingV1alpha2().PodGroups(pod.Namespace).
		Get(context.Background(), webhook.GroupName(pod), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("PodGroup not created: %v", err)
	}
	return pg.Spec.SchedulingPolicy.Gang.MinCount
}

func jobWithParallelism(ns, name string, n int32) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       batchv1.JobSpec{Parallelism: &n, Completions: &n},
	}
}

func ownedBy(pod *corev1.Pod, kind, name string) {
	pod.OwnerReferences = append(pod.OwnerReferences,
		metav1.OwnerReference{Kind: kind, Name: name})
}

// No annotation, but the pod is owned by an indexed Job with parallelism N
// (the Flux Operator MiniCluster case: Parallelism == Completions == size == N).
// minCount must come from the Job.
func TestGangMinCountDerivedFromOwningJob(t *testing.T) {
	pod := cpuPod("fluence")
	pod.Namespace = "default"
	pod.Labels = map[string]string{webhook.GroupLabel: "mc-gang"}
	ownedBy(pod, "Job", "mc-gang-job")
	got := minCountWithClient(t, pod, jobWithParallelism("default", "mc-gang-job", 4))
	if got != 4 {
		t.Errorf("owner-derived: minCount=%d, want 4 (from Job parallelism)", got)
	}
}

// The explicit annotation OVERRIDES the owning Job's parallelism (the override
// exists precisely because minCount may differ from the parent replica count).
func TestGangMinCountAnnotationOverridesOwner(t *testing.T) {
	pod := cpuPod("fluence")
	pod.Namespace = "default"
	pod.Labels = map[string]string{webhook.GroupLabel: "ovr-gang"}
	pod.Annotations = map[string]string{webhook.GroupSizeAnnotation: "2"}
	ownedBy(pod, "Job", "ovr-gang-job")
	got := minCountWithClient(t, pod, jobWithParallelism("default", "ovr-gang-job", 8))
	if got != 2 {
		t.Errorf("annotation override: minCount=%d, want 2 (annotation wins over Job=8)", got)
	}
}

// A classical gang of size N must get minCount = N so the whole group schedules
// atomically (this is the core multi-gang fix).
func atoi32(s string) int32 { v, _ := strconv.Atoi(s); return int32(v) }

func toRuntime(objs []interface{}) []runtime.Object {
	out := make([]runtime.Object, 0, len(objs))
	for _, o := range objs {
		if ro, ok := o.(runtime.Object); ok {
			out = append(out, ro)
		}
	}
	return out
}

func TestGangMinCountEqualsGroupSize(t *testing.T) {
	for _, n := range []string{"2", "4", "8"} {
		pod := cpuPod("fluence")
		pod.Namespace = "default"
		pod.Labels = map[string]string{webhook.GroupLabel: "g-" + n}
		pod.Annotations = map[string]string{webhook.GroupSizeAnnotation: n}
		got := minCountOf(t, pod)
		want := atoi32(n)
		if got != want {
			t.Errorf("group-size=%s: minCount=%d, want %d", n, got, want)
		}
	}
}

// No group-size annotation -> minCount falls back to 1 (single-pod gang).
func TestGangMinCountDefaultsToOne(t *testing.T) {
	pod := cpuPod("fluence")
	pod.Namespace = "default"
	pod.Labels = map[string]string{webhook.GroupLabel: "g-default"}
	if got := minCountOf(t, pod); got != 1 {
		t.Errorf("absent group-size: minCount=%d, want 1", got)
	}
}

// Quantum distinction: a gang of full size N=4 that ALSO carries
// expected-workers=3 (the N-1 workers the sidecar ungates) must still get
// minCount=4 (the whole gang), NOT 3. minCount comes from group-size, not
// expected-workers.
func TestGangMinCountHonorsFullNWithQuantumSplit(t *testing.T) {
	pod := cpuPod("fluence")
	pod.Namespace = "default"
	pod.Labels = map[string]string{webhook.GroupLabel: "q-gang"}
	pod.Annotations = map[string]string{
		webhook.GroupSizeAnnotation:       "4", // full N (leader + workers)
		webhook.ExpectedWorkersAnnotation: "3", // N-1 workers to ungate
	}
	if got := minCountOf(t, pod); got != 4 {
		t.Errorf("quantum gang: minCount=%d, want 4 (full N, not N-1)", got)
	}
}
