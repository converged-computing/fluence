package cluster

import (
	"testing"
)

// A realistic config: a virtual qdevice (under the default parent) holding a qpu
// child, with attributes given by reference, plus a second device with inline
// attributes. Exercises nesting, count, the parent default, and both attribute
// forms.
const sampleConfig = `
resources:
  - type: qdevice
    name: rigetti_cepheus
    attributes: aws-east
    with:
      - type: qpu
        count: 1
        attributes: aws-east
        with:
          - type: qubit
            count: 80
  - type: qdevice
    name: ibm_marrakesh
    parent: cluster
    attributes:
      region: us-west-2
      vendor: ibm

attributes:
  aws-east:
    region: us-east-1
    connectivity: all-to-all
`

func TestLoadResourcesConfig(t *testing.T) {
	c, err := LoadResourcesConfig([]byte(sampleConfig))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(c.Resources) != 2 {
		t.Fatalf("got %d top-level resources, want 2", len(c.Resources))
	}

	rigetti := c.Resources[0]
	if rigetti.Type != "qdevice" || rigetti.Name != "rigetti_cepheus" {
		t.Errorf("first resource = %+v", rigetti)
	}
	// Parent defaults to cluster when unset.
	if rigetti.Parent != DefaultParent {
		t.Errorf("rigetti parent = %q, want %q", rigetti.Parent, DefaultParent)
	}
	// Referenced attributes resolved.
	if rigetti.ResolvedAttributes["region"] != "us-east-1" ||
		rigetti.ResolvedAttributes["connectivity"] != "all-to-all" {
		t.Errorf("rigetti attributes = %v", rigetti.ResolvedAttributes)
	}

	// Nested child + count.
	if len(rigetti.With) != 1 || rigetti.With[0].Type != "qpu" {
		t.Fatalf("rigetti child = %+v", rigetti.With)
	}
	qpu := rigetti.With[0]
	if qpu.Count != 1 {
		t.Errorf("qpu count = %d, want 1", qpu.Count)
	}
	if qpu.ResolvedAttributes["region"] != "us-east-1" {
		t.Errorf("qpu resolved attributes = %v", qpu.ResolvedAttributes)
	}
	if len(qpu.With) != 1 || qpu.With[0].Type != "qubit" || qpu.With[0].Count != 80 {
		t.Errorf("qubit child = %+v", qpu.With)
	}

	// Inline attributes on the second device.
	ibm := c.Resources[1]
	if ibm.ResolvedAttributes["region"] != "us-west-2" || ibm.ResolvedAttributes["vendor"] != "ibm" {
		t.Errorf("ibm attributes = %v", ibm.ResolvedAttributes)
	}
}

// A referenced attribute set that doesn't exist is a hard error, not a silent
// empty map.
func TestUnknownAttributeReferenceErrors(t *testing.T) {
	cfg := `
resources:
  - type: qdevice
    name: x
    attributes: does-not-exist
`
	_, err := LoadResourcesConfig([]byte(cfg))
	if err == nil {
		t.Fatal("expected an error for an unknown attribute reference")
	}
}

// A resource missing 'type' is a hard error.
func TestMissingTypeErrors(t *testing.T) {
	cfg := `
resources:
  - name: x
`
	_, err := LoadResourcesConfig([]byte(cfg))
	if err == nil {
		t.Fatal("expected an error for a resource with no type")
	}
}

// Resolved attribute maps are copies, so mutating one resource's map does not
// bleed into the shared registry or another resource referencing the same set.
func TestReferencedAttributesAreCopied(t *testing.T) {
	cfg := `
resources:
  - type: a
    attributes: shared
  - type: b
    attributes: shared
attributes:
  shared:
    k: v
`
	c, err := LoadResourcesConfig([]byte(cfg))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	c.Resources[0].ResolvedAttributes["k"] = "mutated"
	if c.Resources[1].ResolvedAttributes["k"] != "v" {
		t.Errorf("mutation bled across resources: %v", c.Resources[1].ResolvedAttributes)
	}
}

// A resource with no attributes leaves ResolvedAttributes nil (not an empty map
// we'd then emit as an empty properties object).
func TestNoAttributesIsNil(t *testing.T) {
	cfg := `
resources:
  - type: node
    name: plain
`
	c, err := LoadResourcesConfig([]byte(cfg))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.Resources[0].ResolvedAttributes != nil {
		t.Errorf("expected nil attributes, got %v", c.Resources[0].ResolvedAttributes)
	}
}

// A config with content but no resources: key (e.g. an older "backends:" layout)
// must error loudly rather than silently yielding an empty graph.
func TestLoadResourcesConfigRejectsSchemaMismatch(t *testing.T) {
	_, err := LoadResourcesConfig([]byte("backends:\n  - name: ibm_fez\n    num_qubits: 156\n"))
	if err == nil {
		t.Fatal("a non-empty config defining no resources should error (schema mismatch)")
	}
}

// A genuinely empty or comment-only config stays classical-only (no error).
func TestLoadResourcesConfigEmptyIsClassical(t *testing.T) {
	for _, in := range []string{"", "   \n\n", "# only a comment\n"} {
		c, err := LoadResourcesConfig([]byte(in))
		if err != nil {
			t.Fatalf("empty/comment config %q should be OK, got: %v", in, err)
		}
		if len(c.Resources) != 0 {
			t.Fatalf("empty config %q should yield 0 resources", in)
		}
	}
}

// Attributes inherit downward: a child gets the parent's attributes, can override
// a key, and can clear one by setting it to empty. This makes a nested class +
// attribute constraint reachable (every node on the path carries the attribute).
func TestAttributeInheritance(t *testing.T) {
	cfg := `
resources:
  - type: qdevice
    name: d
    attributes:
      region: us-east-1
      vendor: ibm
    with:
      - type: qpu
        attributes:
          vendor: rigetti
          region: ""
        with:
          - type: qubit
            count: 4
`
	c, err := LoadResourcesConfig([]byte(cfg))
	if err != nil {
		t.Fatal(err)
	}
	dev := c.Resources[0]
	if dev.ResolvedAttributes["region"] != "us-east-1" || dev.ResolvedAttributes["vendor"] != "ibm" {
		t.Fatalf("qdevice attrs = %v", dev.ResolvedAttributes)
	}
	qpu := dev.With[0]
	// vendor overridden, region cleared (explicit ""), nothing else
	if qpu.ResolvedAttributes["vendor"] != "rigetti" {
		t.Errorf("qpu vendor = %q, want rigetti (override)", qpu.ResolvedAttributes["vendor"])
	}
	if _, ok := qpu.ResolvedAttributes["region"]; ok {
		t.Errorf("qpu region should be cleared (explicit empty); got %v", qpu.ResolvedAttributes)
	}
	// qubit inherits qpu's resolved attrs (vendor=rigetti, no region)
	qubit := qpu.With[0]
	if qubit.ResolvedAttributes["vendor"] != "rigetti" {
		t.Errorf("qubit should inherit vendor=rigetti; got %v", qubit.ResolvedAttributes)
	}
	if _, ok := qubit.ResolvedAttributes["region"]; ok {
		t.Errorf("qubit should not have region (cleared at qpu); got %v", qubit.ResolvedAttributes)
	}
}
