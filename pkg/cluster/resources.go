package cluster

import (
	"encoding/json"
	"fmt"
	"strings"

	"sigs.k8s.io/yaml"
)

// This file defines the generic resource-tree configuration that fluence reads
// to inject virtual resources into the Fluxion graph. It is deliberately free
// of any quantum-specific assumptions: a Resource has an arbitrary string type
// and may nest children, so any shape (a qdevice over qpus over qubits, a
// license server, an FPGA pool, ...) is expressible without per-type code.
//
// Two top-level keys:
//
//	resources:  a forest of resource trees to add to the graph. Each tree is
//	            attached under a named parent vertex (default "cluster"), so the
//	            virtual subtree sits alongside the physical "rack" tree without
//	            being linked to any specific physical node (today). A future
//	            design could set parent to a node name to link them.
//	attributes: a registry of named attribute sets. A Resource's Attributes may
//	            be given inline (a map) or by reference (a string naming an entry
//	            here), so a shared set (e.g. a region/connectivity profile) is
//	            defined once and reused.
//
// Every attribute becomes both a queryable/prunable RFC 31 graph property and an
// injected environment value, so what a user can constrain on, they can also
// read back. (Graph/jobspec wiring lives in later pieces; this file only parses
// and validates.)

// DefaultParent is the vertex a resource tree attaches under when none is given.
const DefaultParent = "cluster"

// Resource is one vertex in a configured resource tree. Type is the Fluxion
// resource type (any string); Count is the size at this vertex; With are child
// resources (recursive). Attributes are resolved to a concrete map after load.
type Resource struct {
	// Type is the Fluxion vertex type (e.g. "qdevice", "qpu", "qubit"). Required.
	Type string `json:"type"`
	// Name is an explicit vertex name (e.g. a backend id like "rigetti_cepheus").
	// Optional; the builder derives one from type+index when empty.
	Name string `json:"name,omitempty"`
	// Count is the resource quantity at this vertex (maps to the graph vertex
	// size). Defaults to 1 when zero.
	Count int64 `json:"count,omitempty"`
	// Parent is the vertex this tree attaches under. Only meaningful on a
	// top-level resource (children attach under their parent in With). Defaults
	// to DefaultParent ("cluster").
	Parent string `json:"parent,omitempty"`
	// Attributes is either an inline map (object) or a reference (string) into
	// the top-level attributes registry. Resolved into ResolvedAttributes at
	// load time; do not read this field directly afterward.
	Attributes AttributeSpec `json:"attributes,omitempty"`
	// With are child resources contained by this one (recursive).
	With []Resource `json:"with,omitempty"`

	// ResolvedAttributes is the concrete attribute map after reference
	// resolution. Populated by LoadResourcesConfig; nil if none.
	ResolvedAttributes map[string]string `json:"-"`

	// BackendName is the identity of the top-level device this resource belongs
	// to (the named qdevice), threaded down to descendants during resolution. It
	// is stamped as a match-only property (fluxion.flux-framework.org/backend=
	// <name>) so a require-backend constraint can pin a device, WITHOUT becoming
	// a user attribute (which would double-inject FLUXION_BACKEND into pods).
	BackendName string `json:"-"`
}

// AttributeSpec is a polymorphic attributes field: either a reference (a string
// naming a registry entry) or an inline map. Exactly one form is used.
type AttributeSpec struct {
	// Ref is the registry key when attributes were given as a string.
	Ref string
	// Inline is the attribute map when attributes were given as an object.
	Inline map[string]string
}

// UnmarshalJSON accepts either a JSON string (reference) or object (inline map).
// (sigs.k8s.io/yaml converts YAML to JSON first, so this covers YAML too.)
func (a *AttributeSpec) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || string(data) == "null" {
		return nil
	}
	switch data[0] {
	case '"':
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		a.Ref = s
		return nil
	case '{':
		m := map[string]string{}
		if err := json.Unmarshal(data, &m); err != nil {
			return err
		}
		a.Inline = m
		return nil
	default:
		return fmt.Errorf("attributes must be a string reference or a map, got: %s", data)
	}
}

// empty reports whether no attributes were specified.
func (a AttributeSpec) empty() bool {
	return a.Ref == "" && len(a.Inline) == 0
}

// ResourcesConfig is the on-disk configuration: a forest of resource trees plus
// a registry of named attribute sets they may reference.
type ResourcesConfig struct {
	Resources  []Resource                   `json:"resources"`
	Attributes map[string]map[string]string `json:"attributes,omitempty"`
}

// LoadResourcesConfig parses a YAML or JSON resources configuration and resolves
// every resource's attributes (inline or by reference) into a concrete map. A
// reference to an unknown attribute set is a hard error, as is a resource with
// no type.
func LoadResourcesConfig(data []byte) (*ResourcesConfig, error) {
	var c ResourcesConfig
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse resources config: %w", err)
	}
	// Guard against a silent empty graph: if the file has real content (not just
	// whitespace/comments) but produced zero resources, the schema almost
	// certainly doesn't match (e.g. a top-level key other than "resources:", like
	// an older "backends:" layout). Building a classical-only graph silently in
	// that case hides the misconfiguration — the configured devices simply never
	// appear in the graph. A genuinely empty file (an optional configmap not yet
	// populated) is fine and stays classical-only.
	if len(c.Resources) == 0 && hasContent(data) {
		return nil, fmt.Errorf(
			"resources config has content but defines no resources: " +
				"expected a top-level 'resources:' list (check the schema)")
	}
	for i := range c.Resources {
		if err := resolveResource(&c.Resources[i], c.Attributes, true); err != nil {
			return nil, err
		}
	}
	return &c, nil
}

// hasContent reports whether data has any non-comment, non-whitespace content,
// so a truly empty config (classical-only) is distinguished from a malformed or
// schema-mismatched one.
func hasContent(data []byte) bool {
	for _, line := range strings.Split(string(data), "\n") {
		t := strings.TrimSpace(line)
		if t == "" || strings.HasPrefix(t, "#") {
			continue
		}
		return true
	}
	return false
}

// resolveResource validates a resource and resolves its attributes, recursing
// into children. topLevel resources get a default parent.
func resolveResource(r *Resource, registry map[string]map[string]string, topLevel bool) error {
	return resolveResourceInherited(r, registry, topLevel, nil)
}

// resolveResourceInherited resolves a resource's attributes and threads them
// down to children. A child inherits every attribute of its parent, then applies
// its own: a key the child also sets overrides the inherited value, and a key the
// child sets to the empty string clears it (removed from the resolved set). This
// makes a constraint that selects a nested node by class AND filters on an
// attribute reachable — every node on the path (parent down to the target)
// carries the attribute, so none is pruned mid-descent.
func resolveResourceInherited(
	r *Resource,
	registry map[string]map[string]string,
	topLevel bool,
	inherited map[string]string,
) error {
	if r.Type == "" {
		return fmt.Errorf("resource is missing required field 'type'")
	}
	if topLevel && r.Parent == "" {
		r.Parent = DefaultParent
	}

	// own holds this resource's explicitly-set attributes (before inheritance).
	var own map[string]string
	if !r.Attributes.empty() {
		resolved, err := resolveAttributes(r.Attributes, registry, r.Type)
		if err != nil {
			return err
		}
		own = resolved
	}

	// Merge: start from inherited, apply own (override), honor explicit-clear.
	merged := make(map[string]string, len(inherited)+len(own))
	for k, v := range inherited {
		merged[k] = v
	}
	for k, v := range own {
		if v == "" {
			delete(merged, k) // explicit clear
			continue
		}
		merged[k] = v
	}
	if len(merged) > 0 {
		r.ResolvedAttributes = merged
	} else {
		r.ResolvedAttributes = nil
	}

	// Thread the device identity down: a named top-level device sets it; children
	// inherit it. Stamped as a match-only property by virtualProperties.
	if topLevel && r.Name != "" {
		r.BackendName = r.Name
	}
	for i := range r.With {
		r.With[i].BackendName = r.BackendName
		if err := resolveResourceInherited(&r.With[i], registry, false, merged); err != nil {
			return err
		}
	}
	return nil
}

// resolveAttributes turns an AttributeSpec into a concrete map: inline is used
// directly; a reference is looked up in the registry (error if absent).
func resolveAttributes(
	spec AttributeSpec,
	registry map[string]map[string]string,
	resourceType string,
) (map[string]string, error) {
	if spec.Ref != "" {
		set, ok := registry[spec.Ref]
		if !ok {
			return nil, fmt.Errorf(
				"resource %q references unknown attribute set %q", resourceType, spec.Ref)
		}
		// Copy so callers can't mutate the shared registry entry.
		out := make(map[string]string, len(set))
		for k, v := range set {
			out[k] = v
		}
		return out, nil
	}
	out := make(map[string]string, len(spec.Inline))
	for k, v := range spec.Inline {
		out[k] = v
	}
	return out, nil
}
