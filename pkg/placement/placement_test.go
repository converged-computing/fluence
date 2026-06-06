package placement

import (
	"testing"

	"github.com/converged-computing/fluence/pkg/jobspec"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func podWith(name string, req corev1.ResourceList) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Resources: corev1.ResourceRequirements{Requests: req}}}},
	}
}

func qty(n int64) resource.Quantity { return *resource.NewQuantity(n, resource.DecimalSI) }

// withType returns the count for a given Fluxion type in the slot's `with`.
func withType(js *jobspec.Jobspec, t string) (int, bool) {
	for _, w := range js.Resources[0].With {
		if w.Type == t {
			return w.Count, true
		}
	}
	return 0, false
}

func TestClassical(t *testing.T) {
	pods := []corev1.Pod{
		podWith("p0", corev1.ResourceList{corev1.ResourceCPU: qty(4), "nvidia.com/gpu": qty(1)}),
		podWith("p1", corev1.ResourceList{corev1.ResourceCPU: qty(4), "nvidia.com/gpu": qty(1)}),
	}
	js, err := JobspecForGroup("grp", pods)
	if err != nil {
		t.Fatal(err)
	}
	if js.Resources[0].Count != 2 {
		t.Fatalf("slot count = %d, want 2", js.Resources[0].Count)
	}
	if c, _ := withType(js, "core"); c != 4 {
		t.Errorf("core = %d, want 4", c)
	}
	if c, _ := withType(js, "gpu"); c != 1 {
		t.Errorf("gpu = %d, want 1", c)
	}
	if _, ok := withType(js, "qpu"); ok {
		t.Error("classical pod should not request qpu")
	}
}

func TestGenericQuantumCount(t *testing.T) {
	// fluxion.flux-framework.org/qpu: 1 -> a qpu count, with no per-type code.
	p := podWith("q", corev1.ResourceList{FluxionResourcePrefix + "qpu": qty(1)})
	js, err := JobspecForGroup("qgrp", []corev1.Pod{p})
	if err != nil {
		t.Fatal(err)
	}
	if c, ok := withType(js, "qpu"); !ok || c != 1 {
		t.Fatalf("qpu = %d (ok=%v), want 1", c, ok)
	}
	// no classical core forced on an exotic-only request
	if _, ok := withType(js, "core"); ok {
		t.Error("quantum-only pod should not be forced to request a core")
	}
}

func TestGenericQubitCount(t *testing.T) {
	// "at least 156 qubits" expressed as a count (Fluxion count match is >=).
	p := podWith("q", corev1.ResourceList{FluxionResourcePrefix + "qubit": qty(156)})
	js, err := JobspecForGroup("qubits", []corev1.Pod{p})
	if err != nil {
		t.Fatal(err)
	}
	if c, ok := withType(js, "qubit"); !ok || c != 156 {
		t.Fatalf("qubit = %d (ok=%v), want 156", c, ok)
	}
}

func TestHybrid(t *testing.T) {
	// cores AND a qpu in the same pod -> both appear in the slot.
	p := podWith("h", corev1.ResourceList{
		corev1.ResourceCPU:            qty(2),
		FluxionResourcePrefix + "qpu": qty(1),
	})
	js, err := JobspecForGroup("hyb", []corev1.Pod{p})
	if err != nil {
		t.Fatal(err)
	}
	if c, _ := withType(js, "core"); c != 2 {
		t.Errorf("core = %d, want 2", c)
	}
	if c, _ := withType(js, "qpu"); c != 1 {
		t.Errorf("qpu = %d, want 1", c)
	}
}

func TestPlacementFromAllocation(t *testing.T) {
	alloc := `{"graph":{"nodes":[
	  {"metadata":{"type":"node","name":"node-a"}},
	  {"metadata":{"type":"core","name":"core0"}},
	  {"metadata":{"type":"node","name":"node-b"}},
	  {"metadata":{"type":"qpu","name":"ibm_fez"}}]}}`
	p, err := PlacementFromAllocation(alloc)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Nodes) != 2 || p.Nodes[0] != "node-a" || p.Nodes[1] != "node-b" {
		t.Fatalf("nodes = %v", p.Nodes)
	}
	if p.Backend != "ibm_fez" {
		t.Fatalf("backend = %q", p.Backend)
	}
}

func TestPlacementQuantumOnly(t *testing.T) {
	// A pure-quantum allocation has a qpu (under qgateway) but NO node vertex.
	// Nodes must be empty and Backend set — fluence then imposes no node constraint.
	alloc := `{"graph":{"nodes":[
	  {"metadata":{"type":"cluster","name":"kind"}},
	  {"metadata":{"type":"qgateway","name":"qgateway0"}},
	  {"metadata":{"type":"qpu","name":"ibm_marrakesh"}},
	  {"metadata":{"type":"qubit","name":"qubit0"}}]}}`
	p, err := PlacementFromAllocation(alloc)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Nodes) != 0 {
		t.Fatalf("quantum-only allocation should have no nodes, got %v", p.Nodes)
	}
	if p.Backend != "ibm_marrakesh" {
		t.Fatalf("backend = %q, want ibm_marrakesh", p.Backend)
	}
}
