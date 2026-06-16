package graph

import (
	"encoding/json"
	"fmt"
)

// graphVertices is the subset of a JGF graph we need: the type and name of each
// vertex. It appears at the top level of a "jgf" allocation, and under the
// "scheduling" key of an "rv1" allocation.
type graphVertices struct {
	Nodes []struct {
		Metadata struct {
			Type string `json:"type"`
			Name string `json:"name"`
		} `json:"metadata"`
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
func (a allocation) vertices() []struct {
	Metadata struct {
		Type string `json:"type"`
		Name string `json:"name"`
	} `json:"metadata"`
} {
	if len(a.Scheduling.Graph.Nodes) > 0 {
		return a.Scheduling.Graph.Nodes
	}
	return a.Graph.Nodes
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

// NamesFromAllocation returns the names of every vertex of vertexType in a
// Fluxion allocation (jgf or rv1). Used to map an allocation onto cluster node
// names (vertexType "node") for pod placement, or onto quantum backends ("qpu").
func NamesFromAllocation(alloc string, vertexType string) ([]string, error) {
	var a allocation
	if err := json.Unmarshal([]byte(alloc), &a); err != nil {
		return nil, fmt.Errorf("parse allocation: %w", err)
	}
	var names []string
	for _, n := range a.vertices() {
		if n.Metadata.Type == vertexType {
			names = append(names, n.Metadata.Name)
		}
	}
	return names, nil
}
