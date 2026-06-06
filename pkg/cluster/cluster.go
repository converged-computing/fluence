// Package cluster builds a Fluxion resource graph from the live Kubernetes
// cluster. Traditional compute (cpu/memory/gpu) is discovered from node
// capacity; virtual quantum resources are injected from configuration so the
// same graph can carry both classical and quantum vertices.
package cluster

import (
	"fmt"

	"github.com/converged-computing/fluence/pkg/jgf"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

// DefaultGPUResource is the resource name GPUs are advertised under.
const DefaultGPUResource = "nvidia.com/gpu"

// QuantumBackend describes a virtual quantum resource to model in the graph.
// The Name becomes the qpu vertex name (and the QRMI backend the job runs on).
type QuantumBackend struct {
	Name      string `json:"name"`
	NumQubits int    `json:"num_qubits,omitempty"`
	Vendor    string `json:"vendor,omitempty"`
	QRMIType  string `json:"qrmi_type,omitempty"`
}

// QuantumConfig is the on-disk config that adds quantum resources to the graph.
type QuantumConfig struct {
	Backends []QuantumBackend `json:"backends"`
}

// LoadQuantumConfig reads a YAML or JSON list of quantum backends.
func LoadQuantumConfig(data []byte) (*QuantumConfig, error) {
	var c QuantumConfig
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse quantum config: %w", err)
	}
	return &c, nil
}

// Options configures graph construction.
type Options struct {
	// ClusterName is the root vertex name (default "cluster").
	ClusterName string
	// GPUResource is the resource name GPUs are advertised under
	// (default DefaultGPUResource).
	GPUResource corev1.ResourceName
	// Quantum backends to inject under a qgateway.
	Quantum []QuantumBackend
	// IncludeUnschedulable includes cordoned nodes (default false).
	IncludeUnschedulable bool
}

// BuildGraph turns cluster nodes (plus any configured quantum backends) into a
// Fluxion JGF resource graph, returned as JSON ready for FluxionGraph.Init.
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

	for i := range nodes {
		n := &nodes[i]
		if n.Spec.Unschedulable && !opts.IncludeUnschedulable {
			continue
		}
		nodeV := b.AddChild(cluster, "node", "node", jgf.Options{
			Name:       n.Name,
			Properties: map[string]any{"hostname": n.Name},
		})

		if cpu := count(n, corev1.ResourceCPU); cpu > 0 {
			b.AddChild(nodeV, "core", "core", jgf.Options{Size: cpu})
		}
		if memMB := memoryMB(n); memMB > 0 {
			b.AddChild(nodeV, "memory", "memory", jgf.Options{Size: memMB, Unit: "MB"})
		}
		if gpu := count(n, gpuName); gpu > 0 {
			b.AddChild(nodeV, "gpu", "gpu", jgf.Options{Size: gpu})
		}
	}

	if len(opts.Quantum) > 0 {
		AddQuantum(b, cluster, opts.Quantum)
	}
	return b.JSON()
}

// AddQuantum injects a qgateway under the cluster with one qpu vertex per
// backend. Exposed so a graph built elsewhere can be augmented the same way.
func AddQuantum(b *jgf.Builder, cluster *jgf.Vertex, backends []QuantumBackend) {
	gw := b.AddChild(cluster, "qgateway", "qgateway", jgf.Options{
		Properties: map[string]any{"vendor": "ibm"},
	})
	for _, be := range backends {
		props := map[string]any{"qrmi_type": orDefault(be.QRMIType, "qiskit-runtime-service")}
		if be.NumQubits > 0 {
			props["num_qubits"] = be.NumQubits
		}
		if be.Vendor != "" {
			props["vendor"] = be.Vendor
		}
		b.AddChild(gw, "qpu", "qpu", jgf.Options{
			Name:       be.Name,
			Exclusive:  true,
			Properties: props,
		})
	}
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

// memoryMB returns node memory in mebibytes.
func memoryMB(n *corev1.Node) int64 {
	q, ok := n.Status.Allocatable[corev1.ResourceMemory]
	if !ok {
		q, ok = n.Status.Capacity[corev1.ResourceMemory]
	}
	if !ok {
		return 0
	}
	return q.Value() / (1024 * 1024)
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
