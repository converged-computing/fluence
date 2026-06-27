package handlers

import (
	"context"
	"strings"
	"testing"

	"github.com/converged-computing/fluence/pkg/placement"
	"github.com/converged-computing/fluence/pkg/webhook"
	"github.com/converged-computing/fluence/pkg/webhook/spec"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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

// hasDropQuantumResourceOp reports whether ops remove the Fluxion quantum
// resource from a container's requests or limits (the consumer qpu strip).
func hasDropQuantumResourceOp(ops []spec.Op) bool {
	for _, op := range ops {
		if op.Op == "remove" && strings.HasSuffix(op.Path, "qpu") &&
			(strings.Contains(op.Path, "/resources/requests/") ||
				strings.Contains(op.Path, "/resources/limits/")) {
			return true
		}
	}
	return false
}

func hasSidecarOp(ops []spec.Op) bool {
	for _, op := range ops {
		switch v := op.Value.(type) {
		case corev1.Container:
			if v.Name == SidecarContainerName {
				return true
			}
		case []corev1.Container:
			for _, c := range v {
				if c.Name == SidecarContainerName {
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
