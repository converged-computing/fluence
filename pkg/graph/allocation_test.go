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

// markedAlloc has node vertices carrying composed virtual markers plus a qpu
// child — the shape PlacementFromAllocation classifies.
const markedAlloc = `{"graph":{"nodes":[
  {"metadata":{"type":"node","name":"kind-worker","properties":{"virtual=false":""}}},
  {"metadata":{"type":"node","name":"rigetti","properties":{"virtual=true":"","class=qdevice":""}}},
  {"metadata":{"type":"qpu","name":"qpu0"}}]}}`

// NodesFromAllocation returns only node-typed vertices, each with its composed
// property keys, so callers classify by the virtual marker rather than the type.
func TestNodesFromAllocation(t *testing.T) {
	nodes, err := NodesFromAllocation(markedAlloc)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Fatalf("got %d node vertices, want 2 (qpu is not a node)", len(nodes))
	}

	byName := map[string]AllocatedNode{}
	for _, n := range nodes {
		byName[n.Name] = n
	}
	if !byName["kind-worker"].HasProperty("virtual=false") {
		t.Errorf("kind-worker missing virtual=false: %v", byName["kind-worker"].Properties)
	}
	if !byName["rigetti"].HasProperty("virtual=true") {
		t.Errorf("rigetti missing virtual=true: %v", byName["rigetti"].Properties)
	}
	if !byName["rigetti"].HasProperty("class=qdevice") {
		t.Errorf("rigetti missing class=qdevice: %v", byName["rigetti"].Properties)
	}
	if byName["kind-worker"].HasProperty("virtual=true") {
		t.Error("kind-worker should not carry virtual=true")
	}
}
