package placement

import (
	"fmt"

	"github.com/converged-computing/fluence/pkg/jobspec"
	"github.com/converged-computing/fluence/pkg/quantum"
	corev1 "k8s.io/api/core/v1"
)

const (
	// QuantumResource is the extended resource a pod requests to be placed on a
	// quantum backend (a qpu vertex) instead of classical compute.
	QuantumResource corev1.ResourceName = "quantum.flux-framework.org/qpu"

	// PodGroupLabel and PodGroupSizeLabel mirror the native PodGroup wiring; a
	// pod carries its group name and the group's total size so the scheduler can
	// match the whole gang at once.
	PodGroupLabel     = "scheduling.k8s.io/pod-group"
	PodGroupSizeLabel = "fluence.flux-framework.org/group-size"
)

// podRes is the classical/quantum resource ask distilled from a pod.
type podRes struct {
	cpu     int
	gpu     int
	quantum bool
}

// podResources sums container requests into whole cores/gpus and detects a
// quantum request.
func podResources(p *corev1.Pod) podRes {
	var r podRes
	for i := range p.Spec.Containers {
		req := p.Spec.Containers[i].Resources.Requests
		if q, ok := req[corev1.ResourceCPU]; ok {
			r.cpu += int(q.Value()) // Value() rounds millicores up to whole cores
		}
		if q, ok := req["nvidia.com/gpu"]; ok {
			r.gpu += int(q.Value())
		}
		if _, ok := req[QuantumResource]; ok {
			r.quantum = true
		}
	}
	if !r.quantum && r.cpu == 0 {
		r.cpu = 1 // every classical pod needs at least one core to match
	}
	return r
}

// JobspecForGroup builds a Fluxion jobspec for a whole pod group: a slot per pod
// (count = group size), each holding the per-pod resources. The group is
// assumed homogeneous (same shape per pod), which is the common case for a gang;
// heterogeneous groups are a TODO.
func JobspecForGroup(groupName string, pods []corev1.Pod) (*jobspec.Jobspec, error) {
	if len(pods) == 0 {
		return nil, fmt.Errorf("pod group %q has no pods", groupName)
	}
	r := podResources(&pods[0])

	var with []jobspec.Resource
	if r.quantum {
		with = []jobspec.Resource{{Type: "qpu", Count: 1}}
	} else {
		if r.cpu > 0 {
			with = append(with, jobspec.Resource{Type: "core", Count: r.cpu})
		}
		if r.gpu > 0 {
			with = append(with, jobspec.Resource{Type: "gpu", Count: r.gpu})
		}
	}

	return &jobspec.Jobspec{
		Version: 9999,
		Resources: []jobspec.Resource{{
			Type:  "slot",
			Count: len(pods),
			Label: "default",
			With:  with,
		}},
		Tasks: []jobspec.Task{{
			Command: []string{groupName},
			Slot:    "default",
			Count:   map[string]int{"per_slot": 1},
		}},
	}, nil
}

// Placement is the result of matching a pod group: the cluster nodes to bind to
// (one per slot) and, for quantum groups, the allocated backend.
type Placement struct {
	Nodes   []string // allocated cluster node names
	Backend string   // allocated qpu/backend name (quantum groups only)
}

// PlacementFromAllocation parses a JGF allocation into node and backend names.
func PlacementFromAllocation(alloc string) (Placement, error) {
	nodes, err := quantum.NamesFromAllocation(alloc, "node")
	if err != nil {
		return Placement{}, err
	}
	backends, _ := quantum.NamesFromAllocation(alloc, "qpu")
	p := Placement{Nodes: nodes}
	if len(backends) > 0 {
		p.Backend = backends[0]
	}
	return p, nil
}
