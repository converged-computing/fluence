package graph

import "testing"

// jgf match format: vertices at top-level graph.
const jgfAlloc = `{"graph":{"nodes":[
  {"metadata":{"type":"node","name":"kind-worker"}},
  {"metadata":{"type":"core","name":"core0"}},
  {"metadata":{"type":"qpu","name":"ibm_marrakesh"}}]}}`

// rv1 match format: execution view + scheduling key holding the same vertices.
const rv1Alloc = `{"version":1,
  "execution":{"R_lite":[{"rank":"0","children":{"core":"0"}}],"nodelist":["kind-worker"]},
  "scheduling":{"graph":{"nodes":[
    {"metadata":{"type":"node","name":"kind-worker"}},
    {"metadata":{"type":"core","name":"core0"}},
    {"metadata":{"type":"qpu","name":"ibm_marrakesh"}}]}}}`

func TestNamesFromAllocationJGF(t *testing.T) {
	nodes, err := NamesFromAllocation(jgfAlloc, "node")
	if err != nil || len(nodes) != 1 || nodes[0] != "kind-worker" {
		t.Fatalf("jgf node parse: %v %v", nodes, err)
	}
	be, err := BackendFromAllocation(jgfAlloc, "qpu")
	if err != nil || be != "ibm_marrakesh" {
		t.Fatalf("jgf qpu parse: %q %v", be, err)
	}
}

func TestNamesFromAllocationRV1(t *testing.T) {
	nodes, err := NamesFromAllocation(rv1Alloc, "node")
	if err != nil || len(nodes) != 1 || nodes[0] != "kind-worker" {
		t.Fatalf("rv1 node parse: %v %v", nodes, err)
	}
	be, err := BackendFromAllocation(rv1Alloc, "qpu")
	if err != nil || be != "ibm_marrakesh" {
		t.Fatalf("rv1 qpu parse: %q %v", be, err)
	}
}
