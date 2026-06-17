package cluster

import (
	"encoding/json"
	"testing"

	"github.com/converged-computing/fluence/pkg/jgf"
)

// buildResourceNodes appends the given config's resource trees to a fresh graph
// (simulating physical nodes already having consumed ranks 0..startRank-1) and
// returns the emitted vertex metadata for assertions.
func buildResourceNodes(t *testing.T, cfg string, startRank int64) []map[string]any {
	t.Helper()
	c, err := LoadResourcesConfig([]byte(cfg))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	b := jgf.NewBuilder()
	cluster := b.AddRoot("cluster", "cluster", jgf.Options{Name: "cluster"})
	parents := map[string]*jgf.Vertex{"cluster": cluster}
	rank := startRank
	if err := appendResources(b, parents, "cluster", c.Resources, &rank); err != nil {
		t.Fatalf("append: %v", err)
	}
	raw, _ := b.JSON()
	var doc struct {
		Graph struct {
			Nodes []struct {
				Metadata map[string]any `json:"metadata"`
			} `json:"nodes"`
		} `json:"graph"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, raw)
	}
	out := []map[string]any{}
	for _, n := range doc.Graph.Nodes {
		out = append(out, n.Metadata)
	}
	return out
}

func metaByName(nodes []map[string]any, name string) map[string]any {
	for _, m := range nodes {
		if m["name"] == name {
			return m
		}
	}
	return nil
}

// A configured virtual resource is modeled as a node vertex carrying
// virtual=true, the class of itself AND its descendants, its (inherited)
// attributes as namespaced RFC 31 properties, and a real rank. EVERY level
// (qdevice, qpu, qubit) is a node so each is independently selectable by a
// class=<type> constraint.
func TestVirtualResourceModeledAsNode(t *testing.T) {
	cfg := `
resources:
  - type: qdevice
    name: rigetti_cepheus
    count: 1
    attributes:
      region: us-east-1
    with:
      - type: qpu
        count: 1
        with:
          - type: qubit
            count: 80
`
	nodes := buildResourceNodes(t, cfg, 3) // ranks 0-2 taken by physical nodes

	dev := metaByName(nodes, "rigetti_cepheus")
	if dev == nil {
		t.Fatal("rigetti_cepheus vertex not found")
	}
	if dev["type"] != "node" {
		t.Errorf("virtual device type = %v, want node", dev["type"])
	}
	if dev["rank"] != float64(3) {
		t.Errorf("virtual device rank = %v, want 3 (continues physical ranks)", dev["rank"])
	}
	props, ok := dev["properties"].(map[string]any)
	if !ok {
		t.Fatalf("no properties object: %#v", dev["properties"])
	}
	// Properties are composed key=value strings (RFC 31 match is key-presence
	// only, so the value must be in the key), stored with empty values.
	if _, ok := props["virtual=true"]; !ok {
		t.Errorf("missing virtual=true property; got %#v", props)
	}
	// The qdevice node carries its own class AND its descendants' classes, so a
	// class=qpu / class=qubit constraint is not pruned at this node on the way
	// down to a nested target.
	for _, want := range []string{"class=qdevice", "class=qpu", "class=qubit"} {
		if _, ok := props[want]; !ok {
			t.Errorf("missing %q property on qdevice node; got %#v", want, props)
		}
	}
	if _, ok := props["fluxion.flux-framework.org/region=us-east-1"]; !ok {
		t.Errorf("missing composed region property; got %#v", props)
	}

	// The qpu is now ALSO a node, with class=qpu (and its descendant class=qubit),
	// virtual=true, and the inherited region attribute, on its own real rank.
	qpu := metaByName(nodes, "qpu0")
	if qpu == nil {
		t.Fatal("qpu child (qpu0) not found")
	}
	if qpu["type"] != "node" {
		t.Errorf("qpu type = %v, want node (every level is a node)", qpu["type"])
	}
	if qpu["rank"] == float64(-1) {
		t.Errorf("qpu rank = -1, want a real rank (it is a node)")
	}
	qprops, _ := qpu["properties"].(map[string]any)
	for _, want := range []string{"class=qpu", "class=qubit", "virtual=true", "fluxion.flux-framework.org/region=us-east-1"} {
		if _, ok := qprops[want]; !ok {
			t.Errorf("qpu node missing %q (class self+descendant, virtual, inherited attr); got %#v", want, qprops)
		}
	}
	// The qpu must NOT advertise class=qdevice (that is an ancestor, not in its
	// subtree) — otherwise class=qdevice would wrongly select the qpu too.
	if _, ok := qprops["class=qdevice"]; ok {
		t.Errorf("qpu node should not carry class=qdevice (ancestor class); got %#v", qprops)
	}
}

// FluxionResourceNames includes every type at every level (qdevice, qpu, qubit),
// because every virtual resource is a node selectable by class=<type>.
func TestFluxionResourceNamesIncludesAllLevels(t *testing.T) {
	cfg := `
resources:
  - type: qdevice
    name: d
    with:
      - type: qpu
        count: 1
        with:
          - type: qubit
            count: 4
`
	c, err := LoadResourcesConfig([]byte(cfg))
	if err != nil {
		t.Fatal(err)
	}
	names := FluxionResourceNames(c.Resources)
	want := map[string]bool{
		"fluxion.flux-framework.org/qdevice": true,
		"fluxion.flux-framework.org/qpu":     true,
		"fluxion.flux-framework.org/qubit":   true,
	}
	if len(names) != len(want) {
		t.Fatalf("names = %v, want all of %v", names, want)
	}
	for _, n := range names {
		if !want[n] {
			t.Errorf("unexpected name %q", n)
		}
	}
}

// An unknown parent reference is a hard error.
func TestUnknownParentErrors(t *testing.T) {
	cfg := `
resources:
  - type: qpu
    name: orphan
    parent: nonexistent-node
`
	c, err := LoadResourcesConfig([]byte(cfg))
	if err != nil {
		t.Fatal(err)
	}
	b := jgf.NewBuilder()
	cluster := b.AddRoot("cluster", "cluster", jgf.Options{Name: "cluster"})
	parents := map[string]*jgf.Vertex{"cluster": cluster}
	var rank int64
	if err := appendResources(b, parents, "cluster", c.Resources, &rank); err == nil {
		t.Fatal("expected an error for an unknown parent")
	}
}

// AttributeKeys returns the sorted union of attribute keys across all resources
// and children, deduplicated — this is the env-injection contract.
func TestAttributeKeysUnion(t *testing.T) {
	cfg := `
resources:
  - type: qdevice
    name: a
    attributes:
      region: us-east-1
      connectivity: all-to-all
    with:
      - type: qpu
        attributes:
          qubits: "80"
  - type: qdevice
    name: b
    attributes:
      region: us-west-2
      vendor: ibm
`
	c, err := LoadResourcesConfig([]byte(cfg))
	if err != nil {
		t.Fatal(err)
	}
	keys := AttributeKeys(c.Resources)
	// union: connectivity, qubits, region, vendor (region deduped)
	want := []string{"connectivity", "qubits", "region", "vendor"}
	if len(keys) != len(want) {
		t.Fatalf("keys = %v, want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("keys = %v, want %v", keys, want)
		}
	}
}
