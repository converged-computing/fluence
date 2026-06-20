package webhook

import (
	"context"
	"testing"

	"github.com/converged-computing/fluence/pkg/placement"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func qpuPod(scheduler, presetEnv string) *corev1.Pod {
	c := corev1.Container{
		Name: "app",
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				placement.FluxionResourcePrefix + "qpu": *resource.NewQuantity(1, resource.DecimalSI),
			},
		},
	}
	if presetEnv != "" {
		c.Env = []corev1.EnvVar{{Name: presetEnv, Value: "preset"}}
	}
	return &corev1.Pod{Spec: corev1.PodSpec{SchedulerName: scheduler, Containers: []corev1.Container{c}}}
}

func cpuPod(scheduler string) *corev1.Pod {
	return &corev1.Pod{Spec: corev1.PodSpec{
		SchedulerName: scheduler,
		Containers: []corev1.Container{{
			Name: "c",
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU: *resource.NewQuantity(1, resource.DecimalSI),
				},
			},
		}},
	}}
}

func opEnvNames(ops []jsonPatchOp) []string {
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

func hasGateOp(ops []jsonPatchOp) bool {
	for _, op := range ops {
		switch v := op.Value.(type) {
		case corev1.PodSchedulingGate:
			if v.Name == QuantumGateName {
				return true
			}
		case []corev1.PodSchedulingGate:
			for _, g := range v {
				if g.Name == QuantumGateName {
					return true
				}
			}
		}
	}
	return false
}

func hasSidecarOp(ops []jsonPatchOp) bool {
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

func TestMutateInjectsContract(t *testing.T) {
	m := &Mutator{AttributeKeys: []string{"region", "qubits"}}
	ops := m.Mutate(context.Background(), qpuPod("fluence", ""))
	names := opEnvNames(ops)
	for _, want := range []string{"FLUXION_BACKEND", "FLUXION_REGION", "FLUXION_QUBITS"} {
		if !contains(names, want) {
			t.Errorf("missing injected env %q; got %v", want, names)
		}
	}
}

func TestMutateBackendOnly(t *testing.T) {
	m := &Mutator{}
	names := opEnvNames(m.Mutate(context.Background(), qpuPod("fluence", "")))
	if len(names) != 1 || names[0] != "FLUXION_BACKEND" {
		t.Fatalf("want [FLUXION_BACKEND], got %v", names)
	}
}

func TestMutateSkipsOtherScheduler(t *testing.T) {
	m := &Mutator{AttributeKeys: []string{"region"}}
	if ops := m.Mutate(context.Background(), qpuPod("default-scheduler", "")); ops != nil {
		t.Fatalf("non-fluence pod should not be mutated, got %v", ops)
	}
}

func TestMutateRespectsExistingEnv(t *testing.T) {
	m := &Mutator{AttributeKeys: []string{"region"}}
	names := opEnvNames(m.Mutate(context.Background(), qpuPod("fluence", "FLUXION_BACKEND")))
	if contains(names, "FLUXION_BACKEND") {
		t.Errorf("should not re-inject existing FLUXION_BACKEND; got %v", names)
	}
	if !contains(names, "FLUXION_REGION") {
		t.Errorf("should still inject FLUXION_REGION; got %v", names)
	}
}

func TestMutateSkipsNonFluxion(t *testing.T) {
	m := &Mutator{AttributeKeys: []string{"region"}}
	if ops := m.Mutate(context.Background(), cpuPod("fluence")); ops != nil {
		t.Fatalf("classical pod should not be mutated, got %v", ops)
	}
}

func TestEnvVarNames(t *testing.T) {
	m := &Mutator{AttributeKeys: []string{"region", "connectivity"}}
	names := m.EnvVarNames()
	if len(names) != 3 || names[0] != "FLUXION_BACKEND" {
		t.Fatalf("EnvVarNames = %v, want FLUXION_BACKEND first then attrs", names)
	}
}

// A QPU pod with no group label gets no gate and no sidecar.
func TestMutateQPUSinglePodNoSidecar(t *testing.T) {
	m := &Mutator{}
	ops := m.Mutate(context.Background(), qpuPod("fluence", ""))
	if hasGateOp(ops) {
		t.Error("single QPU pod should not get a scheduling gate")
	}
	if hasSidecarOp(ops) {
		t.Error("single QPU pod should not get a sidecar injected")
	}
}

// quantumWorkerGateOps adds the gate to a pod with no existing gates.
func TestQuantumWorkerGateOpsEmpty(t *testing.T) {
	pod := qpuPod("fluence", "")
	ops := quantumWorkerGateOps(pod)
	if !hasGateOp(ops) {
		t.Errorf("expected gate op, got %v", ops)
	}
}

// quantumWorkerGateOps is idempotent.
func TestQuantumWorkerGateOpsIdempotent(t *testing.T) {
	pod := qpuPod("fluence", "")
	pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: QuantumGateName}}
	ops := quantumWorkerGateOps(pod)
	if len(ops) != 0 {
		t.Errorf("expected no ops when gate already present, got %v", ops)
	}
}

// groupName returns the quantum group label value.
func TestGroupName(t *testing.T) {
	pod := qpuPod("fluence", "")
	if groupName(pod) != "" {
		t.Error("pod without group label should return empty")
	}
	pod.Labels = map[string]string{QuantumGroupLabel: "my-workflow"}
	if groupName(pod) != "my-workflow" {
		t.Errorf("expected my-workflow, got %q", groupName(pod))
	}
}

// schedulingGroupOps stamps spec.schedulingGroup.podGroupName on a pod with no
// existing scheduling group — this is the field the scheduler gangs by.
func TestSchedulingGroupOpsEmpty(t *testing.T) {
	pod := qpuPod("fluence", "")
	ops := schedulingGroupOps(pod, "my-workflow")
	if len(ops) != 1 {
		t.Fatalf("expected 1 op, got %d: %v", len(ops), ops)
	}
	if ops[0].Path != "/spec/schedulingGroup" {
		t.Errorf("expected path /spec/schedulingGroup, got %q", ops[0].Path)
	}
	val, ok := ops[0].Value.(map[string]string)
	if !ok || val["podGroupName"] != "my-workflow" {
		t.Errorf("expected podGroupName=my-workflow, got %v", ops[0].Value)
	}
}

// schedulingGroupOps is idempotent when the pod is already linked to the group.
func TestSchedulingGroupOpsAlreadyLinked(t *testing.T) {
	pod := qpuPod("fluence", "")
	group := "my-workflow"
	pod.Spec.SchedulingGroup = &corev1.PodSchedulingGroup{PodGroupName: &group}
	if ops := schedulingGroupOps(pod, group); len(ops) != 0 {
		t.Errorf("expected no ops when already linked, got %v", ops)
	}
}

// schedulingGroupOps re-stamps when linked to a DIFFERENT group.
func TestSchedulingGroupOpsDifferentGroup(t *testing.T) {
	pod := qpuPod("fluence", "")
	other := "other-group"
	pod.Spec.SchedulingGroup = &corev1.PodSchedulingGroup{PodGroupName: &other}
	if ops := schedulingGroupOps(pod, "my-workflow"); len(ops) != 1 {
		t.Errorf("expected 1 op when linked to a different group, got %v", ops)
	}
}
