package quantum

import (
	"encoding/json"
	"fmt"
)

// allocation is the subset of a Fluxion allocation graph we need: the metadata
// type and name of each allocated vertex.
type allocation struct {
	Graph struct {
		Nodes []struct {
			Metadata struct {
				Type string `json:"type"`
				Name string `json:"name"`
			} `json:"metadata"`
		} `json:"nodes"`
	} `json:"graph"`
}

// BackendFromAllocation returns the name of the first vertex of vertexType
// (e.g. "qpu" or "node") in a Fluxion allocation graph.
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
// Fluxion allocation graph. Used to map an allocation onto cluster node names
// (vertexType "node") for pod placement, or onto quantum backends ("qpu").
func NamesFromAllocation(alloc string, vertexType string) ([]string, error) {
	var a allocation
	if err := json.Unmarshal([]byte(alloc), &a); err != nil {
		return nil, fmt.Errorf("parse allocation: %w", err)
	}
	var names []string
	for _, n := range a.Graph.Nodes {
		if n.Metadata.Type == vertexType {
			names = append(names, n.Metadata.Name)
		}
	}
	return names, nil
}
