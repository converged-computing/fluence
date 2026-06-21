package placement

import (
	"fmt"
	"sort"
	"strings"

	"github.com/converged-computing/fluence/pkg/graph"
	"github.com/converged-computing/fluence/pkg/jobspec"
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

	// JobIDAnnotation records the Fluxion allocation (jobid) for a scheduled
	// group. It is written onto the owning object — the PodGroup for a gang, or
	// the pod itself for an ungrouped pod — so the allocation can be cancelled
	// when that object is deleted, and replayed on scheduler restart.
	JobIDAnnotation = "fluence.flux-framework.org/jobid"

	// AttributeAnnotationPrefix namespaces the matched backend's attributes when
	// the scheduler stamps them onto the pod (e.g.
	// fluence.flux-framework.org/attr-region=us-east-1). The webhook injects one
	// downward-API env per such annotation so the workload reads exactly what it
	// matched. Kept distinct from FluxionResourcePrefix (which marks resource
	// *requests*) so request and result annotations never collide.
	AttributeAnnotationPrefix = "fluence.flux-framework.org/attr-"

	// EnvVarPrefix is the normalized environment-variable namespace injected into
	// the workload. The backend name becomes <prefix>BACKEND; each attribute
	// <key> becomes <prefix><KEY> (uppercased). A vendor-agnostic container reads
	// these common names regardless of which backend it matched.
	EnvVarPrefix = "FLUXION_"
)

// EnvVarName maps an attribute key to its normalized environment-variable name:
// uppercased, non-alphanumeric runes to underscores, under EnvVarPrefix. E.g.
// "region" -> "FLUXION_REGION", "price/min" -> "FLUXION_PRICE_MIN".
func EnvVarName(attrKey string) string {
	var b strings.Builder
	b.WriteString(EnvVarPrefix)
	for _, r := range attrKey {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r - 'a' + 'A')
		case (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

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

// ComputeProperty / VirtualProperty mirror the markers cluster stamps on graph
// node vertices (composed key=value form). A compute jobspec constrains to
// virtual=false nodes; a device jobspec constrains to virtual=true nodes. Kept
// here (not imported from cluster) to avoid an import cycle: cluster imports
// placement.
const (
	// VirtualPropertyTrue selects configured virtual resource nodes.
	VirtualPropertyTrue = "virtual=true"
	// VirtualPropertyFalse selects physical compute nodes.
	VirtualPropertyFalse = "virtual=false"
	// ClassPropertyPrefix composes a class= constraint selecting a virtual node by
	// its configured resource type (e.g. class=qpu). Mirrors how the graph builder
	// stamps the class property; kept here (not imported from cluster) to avoid an
	// import cycle (cluster imports placement).
	ClassPropertyPrefix = "class="
)

// ComposeClassProperty builds the class= constraint string for a resource type.
func ComposeClassProperty(resourceType string) string {
	return ClassPropertyPrefix + resourceType
}

// computeTypes are the Fluxion graph types that live under a physical
// (virtual=false) compute node. Everything requested via the
// fluxion.flux-framework.org/<type> prefix is a virtual device type instead.
var computeTypes = map[string]bool{"core": true, "memory": true, "gpu": true}

// podResources distills a pod's container requests into Fluxion resource counts
// keyed by Fluxion graph type. Native Kubernetes resources (cpu, memory,
// nvidia.com/gpu) map to compute types; every fluxion.flux-framework.org/<type>
// request passes through generically as <type> (a virtual device type).
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

	// Every pod runs on a node, so always request at least one core. Without this
	// a device-only pod produces a compute slot with no resources and Fluxion has
	// no node to land the probing pod on.
	if counts["core"] == 0 {
		counts["core"] = 1
	}
	return counts
}

// splitResources separates a pod's resource counts into the compute resources
// (core/memory/gpu, satisfied by a virtual=false node) and the virtual device
// resources (everything requested via the fluxion prefix, satisfied by a
// virtual=true node). Returns counts keyed by graph type for each group.
func splitResources(counts map[string]int) (compute, devices map[string]int) {
	compute = map[string]int{}
	devices = map[string]int{}
	for t, c := range counts {
		if c <= 0 {
			continue
		}
		if computeTypes[t] {
			compute[t] = c
		} else {
			devices[t] = c
		}
	}
	return compute, devices
}

// withEntries renders a count map into sorted jobspec `with` entries (stable
// output for tests and reproducible jobspecs).
func withEntries(counts map[string]int) []jobspec.Resource {
	types := make([]string, 0, len(counts))
	for t := range counts {
		types = append(types, t)
	}
	sort.Strings(types)
	var with []jobspec.Resource
	for _, t := range types {
		with = append(with, jobspec.Resource{Type: t, Count: counts[t]})
	}
	return with
}

// systemAttributes builds the attributes.system block: a hold-until-cancel
// allocation (duration 0 runs to graph end) plus an RFC 31 property constraint
// selecting the eligible node set. properties is the AND-set of composed
// key=value property strings a matched node must carry.
func systemAttributes(properties []string) map[string]interface{} {
	return map[string]interface{}{
		"system": map[string]interface{}{
			// duration 0 => hold the allocation until we explicitly Cancel.
			"duration": 0,
			"constraints": map[string]interface{}{
				"properties": properties,
			},
		},
	}
}

// computeJobspec builds the physical-compute jobspec for a group: one slot per
// pod holding the compute resources, constrained to virtual=false nodes. This is
// the only jobspec for a group that requests no virtual devices.
func computeJobspec(groupName string, slots int, compute map[string]int) *jobspec.Jobspec {
	return &jobspec.Jobspec{
		Version: 9999,
		Resources: []jobspec.Resource{{
			Type:  "slot",
			Count: slots,
			Label: "default",
			With:  withEntries(compute),
		}},
		Attributes: systemAttributes([]string{VirtualPropertyFalse}),
		Tasks: []jobspec.Task{{
			Command: []string{groupName},
			Slot:    "default",
			Count:   map[string]int{"per_slot": 1},
		}},
	}
}

// deviceJobspec builds a jobspec selecting a single virtual device type. Every
// configured virtual resource is a node carrying class=<type> (and the classes
// of its descendants, so a nested type is reachable), so a device is selected by
// constraining to a virtual node of the requested class. count is the requested
// quantity.
//
// The constraint is virtual=true (scope to virtual backends, not physical nodes)
// AND class=<type> (the requested resource type). The slot requests `node`
// because every virtual resource is a node; the class constraint — not the `with`
// type — picks which one. A nested type (e.g. qpu under a qdevice node) is
// reachable because every ancestor node also advertises class=<type> for the
// types beneath it, so the global constraint does not prune the path.
func deviceJobspec(groupName, deviceType string, count int) *jobspec.Jobspec {
	return &jobspec.Jobspec{
		Version: 9999,
		Resources: []jobspec.Resource{{
			Type:  "slot",
			Count: 1,
			Label: "device",
			With:  []jobspec.Resource{{Type: "node", Count: count}},
		}},
		Attributes: systemAttributes([]string{
			VirtualPropertyTrue,
			ComposeClassProperty(deviceType),
		}),
		Tasks: []jobspec.Task{{
			Command: []string{groupName},
			Slot:    "device",
			Count:   map[string]int{"per_slot": 1},
		}},
	}
}

// JobspecsForGroup builds the set of Fluxion jobspecs to match for a pod group,
// each held independently (duration 0, released by Cancel) and combined all-or-
// nothing by the caller:
//
//   - exactly one compute jobspec (slot per pod, virtual=false) — always present,
//     so a plain pod or group with no virtual resources yields a single match;
//   - one device jobspec per distinct requested virtual resource type
//     (constraint virtual=true; the requested type+count rides the slot's `with`).
//
// knownDevices is the set of device types the graph actually models (the
// FluxionResourceNames the device plugin advertises, suffixes only). A request
// for a type not in the graph is a hard error, caught here rather than as an
// opaque match failure. A nil/empty knownDevices with no device requests is
// fine (classical-only).
func JobspecsForGroup(
	groupName string,
	pods []corev1.Pod,
	knownDevices map[string]bool,
) ([]*jobspec.Jobspec, error) {
	if len(pods) == 0 {
		return nil, fmt.Errorf("pod group %q has no pods", groupName)
	}
	// Compute sizing comes from a representative pod (the group is homogeneous in
	// its per-pod compute slot), but DEVICE requests must be unioned across the
	// whole group: in a quantum gang only the leader requests the qpu, and the
	// pod order here is not guaranteed (groupPods lists in informer order), so
	// keying off pods[0] alone would miss the leader's device entirely and emit
	// a compute-only match with no backend.
	compute, _ := splitResources(podResources(&pods[0]))

	devices := map[string]int{}
	for i := range pods {
		_, podDevices := splitResources(podResources(&pods[i]))
		for t, c := range podDevices {
			if c > devices[t] {
				devices[t] = c // take the max requested across the group
			}
		}
	}

	specs := []*jobspec.Jobspec{computeJobspec(groupName, len(pods), compute)}

	// Deterministic device order for stable output.
	deviceTypes := make([]string, 0, len(devices))
	for t := range devices {
		deviceTypes = append(deviceTypes, t)
	}
	sort.Strings(deviceTypes)

	for _, t := range deviceTypes {
		if len(knownDevices) > 0 && !knownDevices[t] {
			return nil, fmt.Errorf(
				"pod group %q requests virtual resource %q which is not modeled in the resources graph",
				groupName, t)
		}
		if knownDevices == nil {
			return nil, fmt.Errorf(
				"pod group %q requests virtual resource %q but no resources graph is configured",
				groupName, t)
		}
		specs = append(specs, deviceJobspec(groupName, t, devices[t]))
	}
	return specs, nil
}

// Placement is the result of matching one of a group's jobspecs. Nodes are the
// physical (virtual=false) compute nodes a pod binds to; Backend is the virtual
// (virtual=true) resource's identity, surfaced to the pod as env. A single
// allocation yields one or the other: the compute match yields nodes, a device
// match yields a backend.
type Placement struct {
	Nodes   []string // physical compute node names (virtual=false)
	Backend string   // virtual backend identity (virtual=true), if any
	// BackendAttributes are the matched virtual resource's user attributes
	// (region, qubits, ...), decomposed from the backend node's namespaced
	// properties. These are injected into the pod as env so the workload sees
	// exactly what it matched/queried — the same set that is filterable is also
	// readable back.
	BackendAttributes map[string]string
}

// decomposeProperty reverses ComposeProperty: "key=value" -> (key, value, true);
// a bare "key" -> (key, "", true). Used to recover attributes from a backend
// node's composed property keys.
func decomposeProperty(prop string) (key, value string) {
	if i := strings.IndexByte(prop, '='); i >= 0 {
		return prop[:i], prop[i+1:]
	}
	return prop, ""
}

// PlacementFromAllocation parses one Fluxion allocation and classifies its
// node-typed vertices by the virtual marker property: virtual=false nodes are
// physical compute (bind targets), virtual=true nodes are virtual backends
// (their name is the backend identity injected into the pod). Everything is
// type "node" now, so the marker — not the vertex type — does the split.
//
// A node carrying neither marker is treated as a physical compute node, so a
// plain graph built without markers still binds correctly. For the chosen
// backend, its user attributes (the fluxion.flux-framework.org/<key>=<value>
// properties) are decomposed into BackendAttributes for env injection.
func PlacementFromAllocation(alloc string) (Placement, error) {
	nodes, err := graph.NodesFromAllocation(alloc)
	if err != nil {
		return Placement{}, err
	}
	var p Placement
	for _, n := range nodes {
		if n.HasProperty(VirtualPropertyTrue) {
			// First virtual node is the backend identity (one backend per group).
			if p.Backend != "" {
				continue
			}
			p.Backend = n.Name
			p.BackendAttributes = backendAttributes(n.Properties)
			continue
		}
		// virtual=false, or unmarked: a physical compute node to bind.
		p.Nodes = append(p.Nodes, n.Name)
	}
	return p, nil
}

// backendAttributes extracts the user attributes from a backend node's composed
// property keys: every property of the form
// "fluxion.flux-framework.org/<key>=<value>" becomes <key> -> <value>. Reserved
// markers (virtual=..., class=...) are skipped.
func backendAttributes(props map[string]string) map[string]string {
	var attrs map[string]string
	for prop := range props {
		if !strings.HasPrefix(prop, FluxionResourcePrefix) {
			continue
		}
		key, value := decomposeProperty(strings.TrimPrefix(prop, FluxionResourcePrefix))
		if attrs == nil {
			attrs = map[string]string{}
		}
		attrs[key] = value
	}
	return attrs
}
