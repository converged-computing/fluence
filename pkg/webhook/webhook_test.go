package webhook

import (
	"testing"

	"github.com/converged-computing/fluence/pkg/placement"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func qpuPod(scheduler string, withEnv bool) *corev1.Pod {
	c := corev1.Container{
		Name: "app",
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				placement.FluxionResourcePrefix + "qpu": *resource.NewQuantity(1, resource.DecimalSI),
			},
		},
	}
	if withEnv {
		c.Env = []corev1.EnvVar{{Name: "QRMI_BACKEND", Value: "preset"}}
	}
	return &corev1.Pod{Spec: corev1.PodSpec{SchedulerName: scheduler, Containers: []corev1.Container{c}}}
}

func TestMutateInjectsBackendEnv(t *testing.T) {
	ops := Mutate(qpuPod("fluence", false))
	if len(ops) != 1 {
		t.Fatalf("want 1 op, got %d", len(ops))
	}
	if ops[0].Path != "/spec/containers/0/env" {
		t.Errorf("path = %q", ops[0].Path)
	}
}

func TestMutateSkipsOtherScheduler(t *testing.T) {
	if ops := Mutate(qpuPod("default-scheduler", false)); ops != nil {
		t.Fatalf("non-fluence pod should not be mutated, got %v", ops)
	}
}

func TestMutateRespectsExistingEnv(t *testing.T) {
	if ops := Mutate(qpuPod("fluence", true)); ops != nil {
		t.Fatalf("should not override an existing QRMI_BACKEND, got %v", ops)
	}
}

func TestMutateSkipsNonQuantum(t *testing.T) {
	p := &corev1.Pod{Spec: corev1.PodSpec{
		SchedulerName: "fluence",
		Containers:    []corev1.Container{{Name: "c", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: *resource.NewQuantity(1, resource.DecimalSI)}}}},
	}}
	if ops := Mutate(p); ops != nil {
		t.Fatalf("classical pod should not be mutated, got %v", ops)
	}
}
