package cluster

import (
	"fmt"
	"sort"

	"github.com/converged-computing/fluence/pkg/jgf"
	"github.com/converged-computing/fluence/pkg/placement"
)

// Property keys fluence stamps on graph vertices. VirtualProperty marks whether
// a node vertex is a configured virtual resource (true) or a physical compute
// node (false); it is the discriminator the jobspec constrains on and that
// placement uses to tell a bind target from a backend. ClassProperty preserves
// the configured resource type when a virtual resource is modeled as a node, so
// the original type (e.g. "qdevice") is not lost.
const (
	VirtualProperty = "virtual"
	ClassProperty   = "class"
)

// ComposeProperty encodes a key/value as the single "key=value" string that
// Fluxion stores as a property key. RFC 31 PropertyConstraint matching is
// key-presence only — it never compares the value half of a vertex property —
// so to filter on a value the value must be part of the key. This is also
// exactly how the rv1 match writer emits properties (prop + "=" + value), so the
// encoding round-trips symmetrically between what we put in and what we read
// back from an allocation. A bare tag (empty value) composes to just "key".
func ComposeProperty(key, value string) string {
	if value == "" {
		return key
	}
	return key + "=" + value
}

// virtualProperties builds the property SET (composed key=value strings, stored
// as JGF property keys with empty values) for a configured virtual resource: the
// virtual marker, the class of this resource AND of every descendant in its
// subtree, and each user attribute namespaced under the Fluxion resource prefix.
// Every entry is filterable by an identical composed string in a jobspec
// constraint.
//
// Descendant classes are included because a jobspec constraint is GLOBAL and is
// evaluated against every node vertex the matcher descends through (RFC 31
// constraints prune a node, and its subtree, on mismatch). To select a nested
// node by class (e.g. class=qpu where qpu sits under a qdevice node), every
// ancestor node on the path must also satisfy class=qpu — so each node carries
// the classes of everything beneath it. The trees are shallow, so these sets
// stay small. A class=<type> constraint then reaches any node of that type
// because every ancestor down to it advertises that class.
func virtualProperties(res *Resource) map[string]string {
	props := map[string]string{
		ComposeProperty(VirtualProperty, "true"): "",
	}
	for t := range subtreeClasses(res) {
		props[ComposeProperty(ClassProperty, t)] = ""
	}
	for k, v := range res.ResolvedAttributes {
		props[ComposeProperty(placement.FluxionResourcePrefix+k, v)] = ""
	}
	// Match-only property for the device identity, so require-backend can pin a
	// device. Not a user attribute (avoids double-injecting FLUXION_BACKEND).
	if res.BackendName != "" {
		props[ComposeProperty(placement.FluxionResourcePrefix+"backend", res.BackendName)] = ""
	}
	return props
}

// subtreeClasses returns the set of resource types in res's subtree, including
// res itself — the classes a node must advertise so a constraint selecting any
// type within the subtree is not pruned at this node on the way down.
func subtreeClasses(res *Resource) map[string]bool {
	types := map[string]bool{}
	var walk func(r *Resource)
	walk = func(r *Resource) {
		types[r.Type] = true
		for i := range r.With {
			walk(&r.With[i])
		}
	}
	walk(res)
	return types
}

// appendResources attaches each configured resource tree under its parent in
// the builder, continuing the shared node-rank counter. parents maps vertex
// names (physical nodes + the cluster root) to their builder handles.
func appendResources(
	b *jgf.Builder,
	parents map[string]*jgf.Vertex,
	clusterName string,
	resources []Resource,
	rank *int64,
) error {
	for i := range resources {
		res := &resources[i]
		parentName := res.Parent
		if parentName == "" {
			parentName = clusterName
		}
		parent, ok := parents[parentName]
		if !ok {
			return fmt.Errorf(
				"resource %q references unknown parent vertex %q", res.Type, parentName)
		}
		if err := addResource(b, parent, res, rank); err != nil {
			return err
		}
	}
	return nil
}

// addResource adds one configured resource, at ANY depth, as a node vertex
// carrying virtual=true, its configured type as the class property, and its
// attributes as RFC 31 properties — then recurses on its children the same way.
//
// Every level is a node so that every level is independently selectable by a
// class=<type> constraint (RFC 31 property constraints match only node and
// storage_node vertices, so a non-node vertex could never be filtered). The
// vertex basename stays the configured type, so jgf auto-names it (qpu0, qubit0)
// and class=<type> is meaningful; only the graph TYPE is "node".
//
// Every node consumes a real, unique rank from the shared counter. Ranks must be
// real (not -1) for any node vertex; the virtual sub-resources therefore appear
// in the rv1 nodelist, which is fine here because the allocation is a hold whose
// response we read directly — we never dispatch work to those ranks (no
// flux-core execution).
func addResource(b *jgf.Builder, parent *jgf.Vertex, res *Resource, rank *int64) error {
	r := *rank
	*rank++
	size := res.Count
	if size == 0 {
		size = 1
	}
	v := b.AddChild(parent, "node", res.Type, jgf.Options{
		Name:           res.Name, // empty -> jgf auto-names as <type><index>
		Size:           size,
		Rank:           &r,
		NodeProperties: virtualProperties(res),
	})

	for i := range res.With {
		if err := addResource(b, v, &res.With[i], rank); err != nil {
			return err
		}
	}
	return nil
}

// FluxionResourceNames returns the distinct extended-resource names a device
// plugin should advertise for the configured resources: every counted resource
// TYPE under a top-level resource (the top-level virtual node is matched via the
// FluxionResourceNames returns the distinct extended-resource names a device
// plugin should advertise: every resource TYPE at every level of the configured
// trees. Every virtual resource is a node selectable by class=<type>, so every
// type — top-level (qdevice) and nested (qpu, qubit) alike — is requestable.
// Prefixed so they match what the scheduler strips off a pod request.
func FluxionResourceNames(resources []Resource) []string {
	types := map[string]bool{}
	for i := range resources {
		collectTypes(&resources[i], types)
	}
	names := make([]string, 0, len(types))
	for t := range types {
		names = append(names, placement.FluxionResourcePrefix+t)
	}
	sort.Strings(names)
	return names
}

func collectTypes(res *Resource, types map[string]bool) {
	types[res.Type] = true
	for i := range res.With {
		collectTypes(&res.With[i], types)
	}
}

// AttributeKeys returns the sorted union of every user attribute key across all
// configured resources (and their children). This is the set of attributes that
// may be injected into a workload's environment; computing it from the config
// (rather than hardcoding) means the env contract automatically tracks whatever
// attributes the backends declare. The backend identity itself is separate
// (FLUXION_BACKEND) and not included here.
func AttributeKeys(resources []Resource) []string {
	keys := map[string]bool{}
	var walk func(rs []Resource)
	walk = func(rs []Resource) {
		for i := range rs {
			for k := range rs[i].ResolvedAttributes {
				keys[k] = true
			}
			walk(rs[i].With)
		}
	}
	walk(resources)
	out := make([]string, 0, len(keys))
	for k := range keys {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
