package placement

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func cpuPod(name string, cpu, gpu int64) corev1.Pod {
	req := corev1.ResourceList{corev1.ResourceCPU: *resource.NewQuantity(cpu, resource.DecimalSI)}
	if gpu > 0 {
		req["nvidia.com/gpu"] = *resource.NewQuantity(gpu, resource.DecimalSI)
	}
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Resources: corev1.ResourceRequirements{Requests: req}}}},
	}
}

func TestJobspecForGroupClassical(t *testing.T) {
	pods := []corev1.Pod{cpuPod("p0", 4, 1), cpuPod("p1", 4, 1), cpuPod("p2", 4, 1)}
	js, err := JobspecForGroup("grp", pods)
	if err != nil {
		t.Fatal(err)
	}
	if js.Resources[0].Count != 3 {
		t.Fatalf("slot count = %d, want 3", js.Resources[0].Count)
	}
	y, _ := js.YAML()
	if !strings.Contains(y, "core") || !strings.Contains(y, "gpu") {
		t.Fatalf("missing core/gpu:\n%s", y)
	}
}

func TestJobspecForGroupQuantum(t *testing.T) {
	q := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "q0"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				QuantumResource: *resource.NewQuantity(1, resource.DecimalSI),
			}},
		}}},
	}
	js, err := JobspecForGroup("qgrp", []corev1.Pod{q})
	if err != nil {
		t.Fatal(err)
	}
	if js.Resources[0].With[0].Type != "qpu" {
		t.Fatalf("want qpu, got %+v", js.Resources[0].With)
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
