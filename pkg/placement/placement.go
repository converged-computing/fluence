package placement

import (
	"fmt"
	"sort"
	"strings"

	"github.com/converged-computing/fluence/pkg/jobspec"
	"github.com/converged-computing/fluence/pkg/quantum"
	corev1 "k8s.io/api/core/v1"
)

const (
	// FluxionResourcePrefix marks an extended resource whose suffix is a Fluxion
	// graph type. A request for fluxion.flux-framework.org/<type> is translated
	// generically into a jobspec count of <type> — no per-type code. Anything the
	// graph models as a count (qpu, qubit, ...) is requestable this way.
	FluxionResourcePrefix = "fluxion.flux-framework.org/"

	// BackendAnnotation is where the scheduler records the Fluxion-allocated
	// backend for a pod. The mutating webhook wires a downward-API env
	// (QRMI_BACKEND) that reads this annotation.
	BackendAnnotation = "fluence.flux-framework.org/backend"
)

// PodGroupName returns the native (Kubernetes 1.36) scheduling-group name a pod
// belongs to, from spec.schedulingGroup.podGroupName, or "" if the pod is not
// part of a group. This is the first-class field that links a Pod to its
// PodGroup object; the pre-1.36 label/annotation pattern is gone.
func PodGroupName(pod *corev1.Pod) string {
	if sg := pod.Spec.SchedulingGroup; sg != nil && sg.PodGroupName != nil {
		return *sg.PodGroupName
	}
	return ""
}

// podResources distills a pod's container requests into Fluxion resource counts
// keyed by Fluxion graph type (e.g. "core", "gpu", "qpu", "qubit").
//
// Kubernetes names its native resources (cpu, memory, nvidia.com/gpu), so those
// get a small fixed mapping to graph types. Every resource named
// fluxion.flux-framework.org/<type> is passed through generically as <type>,
// with no knowledge of what the type means — if the graph has it as a count,
// Fluxion will verify it.
func podResources(p *corev1.Pod) map[string]int {
	counts := map[string]int{}
	for i := range p.Spec.Containers {
		for name, q := range p.Spec.Containers[i].Resources.Requests {
			switch {
			case name == corev1.ResourceCPU:
				counts["core"] += int(q.Value()) // rounds millicores up to whole cores
			case name == corev1.ResourceMemory:
				counts["memory"] += int(q.Value() / (1000 * 1000)) // bytes -> MB
			case name == "nvidia.com/gpu":
				counts["gpu"] += int(q.Value())
			case strings.HasPrefix(string(name), FluxionResourcePrefix):
				t := strings.TrimPrefix(string(name), FluxionResourcePrefix)
				counts[t] += int(q.Value())
			}
		}
	}
	// A pod that requested no exotic (non-classical) resource still needs at
	// least one core to land on.
	if !hasExotic(counts) && counts["core"] == 0 {
		counts["core"] = 1
	}
	return counts
}

// hasExotic reports whether counts contains any non-classical type (i.e. one
// that came through the Fluxion prefix, like qpu/qubit).
func hasExotic(counts map[string]int) bool {
	for t := range counts {
		switch t {
		case "core", "memory", "gpu":
		default:
			return true
		}
	}
	return false
}

// JobspecForGroup builds a Fluxion jobspec for a whole pod group: a slot per pod
// (count = group size), each holding the per-pod resources as `with` entries —
// one per requested Fluxion type. A hybrid pod (e.g. cores + a qpu) produces a
// slot with both, so classical and quantum are requested together. The group is
// assumed homogeneous (same shape per pod); heterogeneous groups are a TODO.
func JobspecForGroup(groupName string, pods []corev1.Pod) (*jobspec.Jobspec, error) {
	if len(pods) == 0 {
		return nil, fmt.Errorf("pod group %q has no pods", groupName)
	}
	counts := podResources(&pods[0])

	// Deterministic order for stable jobspecs/tests.
	types := make([]string, 0, len(counts))
	for t := range counts {
		types = append(types, t)
	}
	sort.Strings(types)

	var with []jobspec.Resource
	for _, t := range types {
		if counts[t] > 0 {
			with = append(with, jobspec.Resource{Type: t, Count: counts[t]})
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
