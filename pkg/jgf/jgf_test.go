package jgf

import (
	"encoding/json"
	"testing"
)

// decodeNodes parses the builder's JSON into a slice of vertex metadata maps,
// keyed for easy assertions.
func decodeNodes(t *testing.T, b *Builder) []map[string]any {
	t.Helper()
	raw, err := b.JSON()
	if err != nil {
		t.Fatalf("JSON: %v", err)
	}
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
	out := make([]map[string]any, 0, len(doc.Graph.Nodes))
	for _, n := range doc.Graph.Nodes {
		out = append(out, n.Metadata)
	}
	return out
}

func findByName(nodes []map[string]any, name string) map[string]any {
	for _, m := range nodes {
		if m["name"] == name {
			return m
		}
	}
	return nil
}

// A vertex given NodeProperties emits a nested "properties" object with those
// keys/values — the RFC 31 form Fluxion matches constraints against.
func TestNodePropertiesEmittedAsNestedObject(t *testing.T) {
	b := NewBuilder()
	cluster := b.AddRoot("cluster", "cluster", Options{Name: "cluster"})
	b.AddChild(cluster, "node", "node", Options{
		Name: "qdevice0",
		NodeProperties: map[string]string{
			"virtual":                           "true",
			"fluxion.flux-framework.org/region": "us-east-1",
		},
	})

	meta := findByName(decodeNodes(t, b), "qdevice0")
	if meta == nil {
		t.Fatal("qdevice0 vertex not found")
	}
	props, ok := meta["properties"].(map[string]any)
	if !ok {
		t.Fatalf("properties is not a nested object: %#v", meta["properties"])
	}
	if props["virtual"] != "true" {
		t.Errorf("properties[virtual] = %v, want true", props["virtual"])
	}
	if props["fluxion.flux-framework.org/region"] != "us-east-1" {
		t.Errorf("properties[region] = %v, want us-east-1", props["fluxion.flux-framework.org/region"])
	}
}

// A vertex without NodeProperties emits no "properties" key at all, so existing
// graphs are byte-for-byte unchanged.
func TestNoNodePropertiesMeansNoPropertiesKey(t *testing.T) {
	b := NewBuilder()
	cluster := b.AddRoot("cluster", "cluster", Options{Name: "cluster"})
	b.AddChild(cluster, "node", "node", Options{Name: "node0"})

	meta := findByName(decodeNodes(t, b), "node0")
	if meta == nil {
		t.Fatal("node0 vertex not found")
	}
	if _, present := meta["properties"]; present {
		t.Errorf("expected no properties key, got %#v", meta["properties"])
	}
}

// NodeProperties is distinct from Properties: the latter still flattens into the
// top level, the former nests under "properties".
func TestPropertiesVsNodeProperties(t *testing.T) {
	b := NewBuilder()
	cluster := b.AddRoot("cluster", "cluster", Options{Name: "cluster"})
	b.AddChild(cluster, "qpu", "qpu", Options{
		Name:           "sv1",
		Properties:     map[string]any{"vendor": "amazon"},       // flattened, descriptive
		NodeProperties: map[string]string{"region": "us-east-1"}, // nested, matchable
	})

	meta := findByName(decodeNodes(t, b), "sv1")
	if meta == nil {
		t.Fatal("sv1 vertex not found")
	}
	if meta["vendor"] != "amazon" {
		t.Errorf("expected flattened vendor=amazon at top level, got %v", meta["vendor"])
	}
	props, ok := meta["properties"].(map[string]any)
	if !ok || props["region"] != "us-east-1" {
		t.Errorf("expected nested properties.region=us-east-1, got %#v", meta["properties"])
	}
}
