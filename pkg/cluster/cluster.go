// Package cluster builds a Fluxion resource graph from the live Kubernetes
// cluster. Physical compute (cpu/memory/gpu) is discovered from node capacity.
// virtual resources (e.g. quantum backends, but nothing here is quantum-specific)
// are injected from a generic resource-tree configuration so the same graph can
// have both physical and virtual vertices.
//
// Model. Every allocatable thing is a "node" vertex. Physical nodes have the
// property virtual=false; configured virtual resources have virtual=true plus
// their configured attributes as RFC 31 properties. A pod's jobspec constrains
// virtual=false for the compute it lands on and virtual=true for any virtual
// device it requests, so the two are matched from disjoint vertex sets out of a
// single shared rank space.
package cluster

import (
	"github.com/converged-computing/fluence/pkg/jgf"
	corev1 "k8s.io/api/core/v1"
)

// DefaultGPUResource is the resource name GPUs are advertised under.
const DefaultGPUResource = "nvidia.com/gpu"

// Options configures graph construction.
type Options struct {
	// ClusterName is the root vertex name (default "cluster").
	ClusterName string
	// GPUResource is the resource name GPUs are advertised under
	// (default DefaultGPUResource).
	GPUResource corev1.ResourceName
	// Resources are the configured virtual resource trees to inject.
	Resources []Resource
	// IncludeUnschedulable includes cordoned nodes (default false).
	IncludeUnschedulable bool
}

// BuildGraph turns cluster nodes (plus any configured virtual resources) into a
// Fluxion JGF resource graph, returned as JSON ready for FluxionGraph.Init.
//
// Physical nodes are built first, then the configured resource trees are
// appended under their parent vertices. A single contiguous rank counter spans
// physical and virtual node vertices. Every node-typed vertex gets a real rank
// (the rv1 writer requires it), and the virtual=true/false property keeps the
// two sets apart at match time.
func BuildGraph(nodes []corev1.Node, opts Options) ([]byte, error) {
	b := jgf.NewBuilder()

	clusterName := opts.ClusterName
	if clusterName == "" {
		clusterName = "cluster"
	}
	gpuName := opts.GPUResource
	if gpuName == "" {
		gpuName = DefaultGPUResource
	}

	cluster := b.AddRoot("cluster", "cluster", jgf.Options{Name: clusterName})

	// rank is the shared, contiguous execution-rank counter across all node
	// vertices (physical first, then virtual).
	var rank int64

	// parents maps a vertex name to its builder handle so a configured resource
	// tree can attach under a named parent (default the cluster root).
	parents := map[string]*jgf.Vertex{clusterName: cluster}

	for i := range nodes {
		n := &nodes[i]
		if n.Spec.Unschedulable && !opts.IncludeUnschedulable {
			continue
		}
		// Control-plane nodes are typically tainted (NoSchedule) rather than
		// cordoned, so the Unschedulable check above does not catch them. Skip by
		// taint, which is name- and type-independent (unlike matching the node
		// name), unless the caller opts to include unschedulable nodes.
		if isControlPlane(n) && !opts.IncludeUnschedulable {
			continue
		}
		r := rank
		rank++
		nodeV := b.AddChild(cluster, "node", "node", jgf.Options{
			Name:           n.Name,
			Rank:           &r,
			Properties:     map[string]any{"hostname": n.Name},
			NodeProperties: map[string]string{ComposeProperty(VirtualProperty, "false"): ""},
		})
		parents[n.Name] = nodeV

		// A vertex is the unit Fluxion allocates. Anything under a slot is taken
		// whole, so a node built as one sized vertex per type can host one slot
		// of a gang. Cores and GPUs are one vertex each, as in Flux. Memory is
		// chunked the way flux-sched's own graphs and Slurm's memory per CPU
		// do it, one chunk per core sized in MiB, so a request takes whole
		// chunks and the vertex count stays at twice the cores.
		cores := count(n, corev1.ResourceCPU)
		for i := int64(0); i < cores; i++ {
			b.AddChild(nodeV, "core", "core", jgf.Options{})
		}
		if chunk := memoryChunkMiB(n, cores); chunk > 0 {
			for i := int64(0); i < cores; i++ {
				b.AddChild(nodeV, "memory", "memory", jgf.Options{Size: chunk, Unit: "MiB"})
			}
		}
		for i := int64(0); i < count(n, gpuName); i++ {
			b.AddChild(nodeV, "gpu", "gpu", jgf.Options{})
		}
	}

	if err := appendResources(b, parents, clusterName, opts.Resources, &rank); err != nil {
		return nil, err
	}
	return b.JSON()
}

// controlPlaneTaints are the well-known taint keys Kubernetes places on
// control-plane nodes. They are tainted NoSchedule rather than cordoned, so a
// taint check (not an Unschedulable check) is what excludes them.
var controlPlaneTaints = map[string]bool{
	"node-role.kubernetes.io/control-plane": true,
	"node-role.kubernetes.io/master":        true,
}

// isControlPlane reports whether a node has a control-plane taint.
func isControlPlane(n *corev1.Node) bool {
	for i := range n.Spec.Taints {
		if controlPlaneTaints[n.Spec.Taints[i].Key] {
			return true
		}
	}
	return false
}

// count reads an integer resource count, preferring allocatable over capacity.
func count(n *corev1.Node, name corev1.ResourceName) int64 {
	if q, ok := n.Status.Allocatable[name]; ok {
		return q.Value()
	}
	if q, ok := n.Status.Capacity[name]; ok {
		return q.Value()
	}
	return 0
}

// memoryChunkMiB is the size of one memory chunk, the node's memory split
// evenly over its cores, in whole MiB rounded down. Zero when there is no
// memory or no cores to split it over.
func memoryChunkMiB(n *corev1.Node, cores int64) int64 {
	if cores <= 0 {
		return 0
	}
	q, ok := n.Status.Allocatable[corev1.ResourceMemory]
	if !ok {
		q, ok = n.Status.Capacity[corev1.ResourceMemory]
	}
	if !ok {
		return 0
	}
	return q.Value() / (1024 * 1024) / cores
}
