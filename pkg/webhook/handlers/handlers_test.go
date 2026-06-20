package handlers

import (
	"context"
	"testing"

	"github.com/converged-computing/fluence/pkg/placement"
	"github.com/converged-computing/fluence/pkg/webhook"
	"github.com/converged-computing/fluence/pkg/webhook/spec"

	corev1 "k8s.io/api/core/v1"
	schedulingv1alpha2 "k8s.io/api/scheduling/v1alpha2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// ── fixtures ────────────────────────────────────────────────────────────────────

func qpuPod(scheduler string) *corev1.Pod {
	return &corev1.Pod{Spec: corev1.PodSpec{
		SchedulerName: scheduler,
		Containers: []corev1.Container{{
			Name: "app",
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				placement.FluxionResourcePrefix + "qpu": *resource.NewQuantity(1, resource.DecimalSI),
			}},
		}},
	}}
}

func cpuPod(scheduler string) *corev1.Pod {
	return &corev1.Pod{Spec: corev1.PodSpec{
		SchedulerName: scheduler,
		Containers: []corev1.Container{{
			Name: "c",
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU: *resource.NewQuantity(1, resource.DecimalSI),
			}},
		}},
	}}
}

func opEnvNames(ops []spec.Op) []string {
	var names []string
	for _, op := range ops {
		switch v := op.Value.(type) {
		case corev1.EnvVar:
			names = append(names, v.Name)
		case []corev1.EnvVar:
			for _, e := range v {
				names = append(names, e.Name)
			}
		}
	}
	return names
}

func contains(names []string, want string) bool {
	for _, n := range names {
		if n == want {
			return true
		}
	}
	return false
}

func hasGateOp(ops []spec.Op) bool {
	for _, op := range ops {
		switch v := op.Value.(type) {
		case corev1.PodSchedulingGate:
			if v.Name == QuantumGate {
				return true
			}
		case []corev1.PodSchedulingGate:
			for _, g := range v {
				if g.Name == QuantumGate {
					return true
				}
			}
		}
	}
	return false
}

func hasSidecarOp(ops []spec.Op) bool {
	for _, op := range ops {
		switch v := op.Value.(type) {
		case corev1.Container:
			if v.Name == "fluence-sidecar" {
				return true
			}
		case []corev1.Container:
			for _, c := range v {
				if c.Name == "fluence-sidecar" {
					return true
				}
			}
		}
	}
	return false
}

// ── fluxion handler ─────────────────────────────────────────────────────────────

func TestMutateInjectsContract(t *testing.T) {
	m := &webhook.Mutator{AttributeKeys: []string{"region"}}
	names := opEnvNames(m.Mutate(context.Background(), qpuPod("fluence")))
	for _, want := range []string{"FLUXION_BACKEND", "FLUXION_REGION"} {
		if !contains(names, want) {
			t.Errorf("want %s injected, got %v", want, names)
		}
	}
}

func TestMutateSkipsOtherScheduler(t *testing.T) {
	m := &webhook.Mutator{}
	if ops := m.Mutate(context.Background(), qpuPod("default-scheduler")); ops != nil {
		t.Errorf("non-fluence pod should be untouched, got %v", ops)
	}
}

func TestMutateSkipsNonFluxion(t *testing.T) {
	m := &webhook.Mutator{}
	if ops := m.Mutate(context.Background(), cpuPod("fluence")); len(ops) != 0 {
		t.Errorf("classical non-group pod should get no ops, got %v", ops)
	}
}

// ── quantum handler: submitter ──────────────────────────────────────────────────

func TestSingleQuantumGetsInterceptorNoSidecar(t *testing.T) {
	m := &webhook.Mutator{AttributeKeys: []string{"region"}}
	ops := m.Mutate(context.Background(), qpuPod("fluence"))
	names := opEnvNames(ops)
	if !contains(names, "FLUXION_BACKEND") {
		t.Errorf("want FLUXION_BACKEND, got %v", names)
	}
	if !contains(names, "PYTHONPATH") || !contains(names, "FLUENCE_POD_UID") {
		t.Errorf("want interceptor env (PYTHONPATH, FLUENCE_POD_UID), got %v", names)
	}
	if hasSidecarOp(ops) {
		t.Error("standalone quantum pod should not get a sidecar")
	}
	if hasGateOp(ops) {
		t.Error("standalone quantum pod should not be gated")
	}
}

func TestObserveLabelInjectsSidecar(t *testing.T) {
	m := &webhook.Mutator{}
	pod := qpuPod("fluence")
	pod.Labels = map[string]string{ObserveLabel: "true"}
	ops := m.Mutate(context.Background(), pod)
	if !hasSidecarOp(ops) {
		t.Error("observe-labeled quantum pod should get the sidecar")
	}
	if hasGateOp(ops) {
		t.Error("observe-only pod should not be gated")
	}
}

// ── quantum handler: worker gating ──────────────────────────────────────────────

func quantumGroupFixture(ns, group, leaderName string) *fake.Clientset {
	pg := &schedulingv1alpha2.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name: group, Namespace: ns,
			Annotations: map[string]string{webhook.LeaderAnnotation: leaderName},
		},
	}
	leaderPod := qpuPod("fluence")
	leaderPod.Name = leaderName
	leaderPod.Namespace = ns
	leaderPod.Labels = map[string]string{webhook.GroupLabel: group}
	return fake.NewSimpleClientset(pg, leaderPod)
}

func TestClassicalWorkerInQuantumGroupIsGated(t *testing.T) {
	ns, group, leader := "default", "qaoa", "qaoa-leader"
	m := &webhook.Mutator{Clientset: quantumGroupFixture(ns, group, leader)}

	worker := cpuPod("fluence")
	worker.Name = "qaoa-worker-0"
	worker.Namespace = ns
	worker.Labels = map[string]string{webhook.GroupLabel: group}

	ops := m.Mutate(context.Background(), worker)
	if !hasGateOp(ops) {
		t.Errorf("classical worker in a quantum group should be gated; ops=%v", ops)
	}
	if hasSidecarOp(ops) {
		t.Error("worker should not get a sidecar")
	}
}

func TestClassicalGangWorkerNotGated(t *testing.T) {
	ns, group, leader := "default", "classical", "classical-leader"
	pg := &schedulingv1alpha2.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: group, Namespace: ns,
			Annotations: map[string]string{webhook.LeaderAnnotation: leader}},
	}
	leaderPod := cpuPod("fluence")
	leaderPod.Name = leader
	leaderPod.Namespace = ns
	leaderPod.Labels = map[string]string{webhook.GroupLabel: group}
	m := &webhook.Mutator{Clientset: fake.NewSimpleClientset(pg, leaderPod)}

	worker := cpuPod("fluence")
	worker.Name = "classical-worker-0"
	worker.Namespace = ns
	worker.Labels = map[string]string{webhook.GroupLabel: group}

	if hasGateOp(m.Mutate(context.Background(), worker)) {
		t.Error("worker in a classical gang must NOT be gated (would deadlock)")
	}
}

// Pod-template gang: every pod requests QPU; only the recorded leader gets the
// sidecar, the rest are gated workers (role by admission order, not request).
func TestPodTemplateGangSecondPodIsWorker(t *testing.T) {
	ns, group, leader := "default", "qaoa", "qaoa-abc123"
	pg := &schedulingv1alpha2.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: group, Namespace: ns,
			Annotations: map[string]string{webhook.LeaderAnnotation: leader}},
	}
	leaderPod := qpuPod("fluence")
	leaderPod.Name = leader
	leaderPod.Namespace = ns
	leaderPod.Labels = map[string]string{webhook.GroupLabel: group}
	m := &webhook.Mutator{Clientset: fake.NewSimpleClientset(pg, leaderPod)}

	second := qpuPod("fluence") // identical spec, requests QPU
	second.Name = "qaoa-def456"
	second.Namespace = ns
	second.Labels = map[string]string{webhook.GroupLabel: group}

	ops := m.Mutate(context.Background(), second)
	if !hasGateOp(ops) {
		t.Error("second pod in a pod-template gang must be gated as a worker")
	}
	if hasSidecarOp(ops) {
		t.Error("second pod must NOT get a sidecar (it is a worker)")
	}
}

// ── gang handler: scheduling group linkage ──────────────────────────────────────

func TestGangStampsSchedulingGroup(t *testing.T) {
	m := &webhook.Mutator{}
	pod := cpuPod("fluence")
	pod.Labels = map[string]string{webhook.GroupLabel: "g1"}
	var found bool
	for _, op := range m.Mutate(context.Background(), pod) {
		if op.Path == "/spec/schedulingGroup" {
			found = true
		}
	}
	if !found {
		t.Error("gang handler should stamp /spec/schedulingGroup")
	}
}
