package graph

import (
	"encoding/json"
	"fmt"
)

// vertexMeta is the subset of a JGF vertex metadata fluence needs from an
// allocation: type, name, and the RFC 31 properties object. Properties are
// stored as composed "key=value" keys (matching how the graph is built and how
// the rv1 writer emits them); the value half of the JSON object is unused.
type vertexMeta struct {
	Type       string            `json:"type"`
	Name       string            `json:"name"`
	Properties map[string]string `json:"properties"`
}

// graphVertices is the subset of a JGF graph we need: the metadata of each
// vertex. It appears at the top level of a "jgf" allocation, and under the
// "scheduling" key of an "rv1" allocation.
type graphVertices struct {
	Nodes []struct {
		Metadata vertexMeta `json:"metadata"`
	} `json:"nodes"`
}

// allocation captures the allocated vertices from a Fluxion allocation R in
// either match format:
//   - "jgf": the vertices are at the top level under "graph".
//   - "rv1": the vertices are under "scheduling.graph" (the rv1 scheduling key),
//     alongside an "execution" view we don't need for name extraction.
//
// We persist and replay rv1 (it is a superset of jgf and is the format flux
// recommends for failure recovery), but accepting both keeps callers and older
// allocations working.
type allocation struct {
	Graph      graphVertices `json:"graph"`
	Scheduling struct {
		Graph graphVertices `json:"graph"`
	} `json:"scheduling"`
}

// vertices returns the allocated vertices regardless of match format, preferring
// the rv1 scheduling key and falling back to a top-level jgf graph.
func (a allocation) vertices() []vertexMeta {
	src := a.Graph.Nodes
	if len(a.Scheduling.Graph.Nodes) > 0 {
		src = a.Scheduling.Graph.Nodes
	}
	out := make([]vertexMeta, 0, len(src))
	for _, n := range src {
		out = append(out, n.Metadata)
	}
	return out
}

// AllocatedNode is a node-typed vertex from an allocation, with the properties
// fluence classifies it by (virtual=true/false, class=..., user attributes).
type AllocatedNode struct {
	Name       string
	Properties map[string]string
}

// HasProperty reports whether the node carries the given composed property key
// (e.g. "virtual=true").
func (n AllocatedNode) HasProperty(key string) bool {
	_, ok := n.Properties[key]
	return ok
}

// NodesFromAllocation returns every node-typed vertex in an allocation together
// with its properties, so callers can classify them (physical vs virtual) by the
// composed marker keys rather than by vertex type. Under the virtual-resource
// model every allocatable thing is type "node"; the virtual marker, not the
// type, distinguishes a compute node from a virtual backend.
func NodesFromAllocation(alloc string) ([]AllocatedNode, error) {
	var a allocation
	if err := json.Unmarshal([]byte(alloc), &a); err != nil {
		return nil, fmt.Errorf("parse allocation: %w", err)
	}
	var nodes []AllocatedNode
	for _, v := range a.vertices() {
		if v.Type == "node" {
			nodes = append(nodes, AllocatedNode{Name: v.Name, Properties: v.Properties})
		}
	}
	return nodes, nil
}

// NamesFromAllocation returns the names of every vertex of vertexType in a
// Fluxion allocation (jgf or rv1). Retained for callers that key on a concrete
// vertex type (e.g. counting "qpu" children); placement now classifies node
// vertices via NodesFromAllocation and the virtual marker instead.
func NamesFromAllocation(alloc string, vertexType string) ([]string, error) {
	var a allocation
	if err := json.Unmarshal([]byte(alloc), &a); err != nil {
		return nil, fmt.Errorf("parse allocation: %w", err)
	}
	var names []string
	for _, v := range a.vertices() {
		if v.Type == vertexType {
			names = append(names, v.Name)
		}
	}
	return names, nil
}

// BackendFromAllocation returns the name of the first vertex of vertexType
// (e.g. "qpu" or "node") in a Fluxion allocation.
func BackendFromAllocation(alloc string, vertexType string) (string, error) {
	names, err := NamesFromAllocation(alloc, vertexType)
	if err != nil {
		return "", err
	}
	if len(names) == 0 {
		return "", fmt.Errorf("no %q vertex found in allocation", vertexType)
	}
	return names[0], nil
}
