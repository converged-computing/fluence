package cluster

import (
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// isControlPlane detects control-plane nodes by taint, independent of node name
// or type, and only for the well-known control-plane/master taint keys.
func TestIsControlPlane(t *testing.T) {
	cp := &corev1.Node{}
	cp.Spec.Taints = []corev1.Taint{
		{Key: "node-role.kubernetes.io/control-plane", Effect: corev1.TaintEffectNoSchedule},
	}
	if !isControlPlane(cp) {
		t.Error("expected control-plane node (control-plane taint) to be detected")
	}

	master := &corev1.Node{}
	master.Spec.Taints = []corev1.Taint{
		{Key: "node-role.kubernetes.io/master", Effect: corev1.TaintEffectNoSchedule},
	}
	if !isControlPlane(master) {
		t.Error("expected control-plane node (master taint) to be detected")
	}

	worker := &corev1.Node{}
	worker.Spec.Taints = []corev1.Taint{
		{Key: "some/other-taint", Effect: corev1.TaintEffectNoSchedule},
	}
	if isControlPlane(worker) {
		t.Error("worker with an unrelated taint should not be treated as control-plane")
	}

	plain := &corev1.Node{}
	if isControlPlane(plain) {
		t.Error("node with no taints should not be control-plane")
	}
}

// A real node is built with one vertex per core and per GPU, and memory as one
// chunk per core, each chunk the node's memory split over its cores in whole
// MiB. Fluxion hands a vertex to a slot and never splits it, so a node built
// as one sized vertex per type could host one slot of a gang.
func TestBuildGraphOneVertexPerUnit(t *testing.T) {
	n := corev1.Node{}
	n.Name = "worker"
	n.Status.Allocatable = corev1.ResourceList{
		corev1.ResourceCPU:                      resource.MustParse("4"),
		corev1.ResourceMemory:                   resource.MustParse("8704Mi"), // 8.5 GiB
		corev1.ResourceName(DefaultGPUResource): resource.MustParse("2"),
	}
	out, err := BuildGraph([]corev1.Node{n}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Graph struct {
			Nodes []struct {
				Metadata struct {
					Type string `json:"type"`
					Size int64  `json:"size"`
					Unit string `json:"unit"`
				} `json:"metadata"`
			} `json:"nodes"`
		} `json:"graph"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatal(err)
	}
	count := map[string]int{}
	for _, v := range doc.Graph.Nodes {
		count[v.Metadata.Type]++
		switch v.Metadata.Type {
		case "memory":
			// 8704 MiB over 4 cores
			if v.Metadata.Size != 2176 || v.Metadata.Unit != "MiB" {
				t.Errorf("memory chunk = %d %s, want 2176 MiB", v.Metadata.Size, v.Metadata.Unit)
			}
		case "core", "gpu":
			if v.Metadata.Size != 1 {
				t.Errorf("%s vertex has size %d, want 1", v.Metadata.Type, v.Metadata.Size)
			}
		}
	}
	want := map[string]int{"cluster": 1, "node": 1, "core": 4, "memory": 4, "gpu": 2}
	for typ, w := range want {
		if count[typ] != w {
			t.Errorf("%s vertices = %d, want %d", typ, count[typ], w)
		}
	}
}
