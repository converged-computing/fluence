package webhook

import (
	"testing"

	"github.com/converged-computing/fluence/pkg/placement"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func qpuPod(scheduler string, presetEnv string) *corev1.Pod {
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

// envNames returns the env var names referenced by a list of add-ops.
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

// With a config-derived contract (region, qubits), a fluxion pod gets
// FLUXION_BACKEND plus one FLUXION_<KEY> per attribute key.
func TestMutateInjectsContract(t *testing.T) {
	m := &Mutator{AttributeKeys: []string{"region", "qubits"}}
	ops := m.Mutate(qpuPod("fluence", ""))
	names := opEnvNames(ops)

	for _, want := range []string{"FLUXION_BACKEND", "FLUXION_REGION", "FLUXION_QUBITS"} {
		if !contains(names, want) {
			t.Errorf("missing injected env %q; got %v", want, names)
		}
	}
	if len(names) != 3 {
		t.Errorf("expected exactly 3 env vars, got %v", names)
	}
}

// With no configured attributes, only FLUXION_BACKEND is injected.
func TestMutateBackendOnly(t *testing.T) {
	m := &Mutator{}
	names := opEnvNames(m.Mutate(qpuPod("fluence", "")))
	if len(names) != 1 || names[0] != "FLUXION_BACKEND" {
		t.Fatalf("want [FLUXION_BACKEND], got %v", names)
	}
}

// Non-fluence pods are never mutated.
func TestMutateSkipsOtherScheduler(t *testing.T) {
	m := &Mutator{AttributeKeys: []string{"region"}}
	if ops := m.Mutate(qpuPod("default-scheduler", "")); ops != nil {
		t.Fatalf("non-fluence pod should not be mutated, got %v", ops)
	}
}

// An env var the container already defines is not re-injected (idempotent / no
// override), while the others still are.
func TestMutateRespectsExistingEnv(t *testing.T) {
	m := &Mutator{AttributeKeys: []string{"region"}}
	names := opEnvNames(m.Mutate(qpuPod("fluence", "FLUXION_BACKEND")))
	if contains(names, "FLUXION_BACKEND") {
		t.Errorf("should not re-inject existing FLUXION_BACKEND; got %v", names)
	}
	if !contains(names, "FLUXION_REGION") {
		t.Errorf("should still inject FLUXION_REGION; got %v", names)
	}
}

// Classical pods (no fluxion resource request) are not mutated.
func TestMutateSkipsNonFluxion(t *testing.T) {
	m := &Mutator{AttributeKeys: []string{"region"}}
	p := &corev1.Pod{Spec: corev1.PodSpec{
		SchedulerName: "fluence",
		Containers:    []corev1.Container{{Name: "c", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: *resource.NewQuantity(1, resource.DecimalSI)}}}},
	}}
	if ops := m.Mutate(p); ops != nil {
		t.Fatalf("classical pod should not be mutated, got %v", ops)
	}
}

// EnvVarNames reports the full contract for startup logging.
func TestEnvVarNames(t *testing.T) {
	m := &Mutator{AttributeKeys: []string{"region", "connectivity"}}
	names := m.EnvVarNames()
	if len(names) != 3 || names[0] != "FLUXION_BACKEND" {
		t.Fatalf("EnvVarNames = %v, want FLUXION_BACKEND first then attrs", names)
	}
}
