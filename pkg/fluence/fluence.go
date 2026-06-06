package fluence

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/converged-computing/fluence/pkg/cluster"
	"github.com/converged-computing/fluence/pkg/graph"
	"github.com/converged-computing/fluence/pkg/placement"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	fwk "k8s.io/kube-scheduler/framework"
)

// The scheduler-framework types live in the staging module
// k8s.io/kube-scheduler/framework (imported as fwk). The plugin is built into
// the scheduler binary via cmd/fluence. Signatures here are verified against
// k8s.io/kube-scheduler v0.36.0; keep them in lockstep with the k8s version you
// run on (CycleState and NodeInfo are interfaces, and PreFilter takes the
// candidate node list).

// Name is the plugin name registered with the scheduler and referenced in the
// KubeSchedulerConfiguration.
const Name = "Fluence"

// Fluence is a scheduler-framework plugin that places whole pod groups by
// matching them against a flux-sched resource graph built from the live cluster
// (plus any configured quantum resources). Gang/all-or-nothing semantics are
// delegated to the native PodGroup API; Fluence only decides placement.
type Fluence struct {
	handle  fwk.Handle
	matcher *graph.FluxionGraph

	mu sync.Mutex
	// placement maps a pod-group key to the nodes chosen for the group.
	placement map[string][]string
}

var (
	_ fwk.PreFilterPlugin = (*Fluence)(nil)
	_ fwk.FilterPlugin    = (*Fluence)(nil)
)

// New builds the plugin: discover cluster nodes, optionally inject quantum
// resources, write the JGF graph, and initialize the Fluxion matcher.
//
// Configuration (for now via env; can move to plugin args):
//
//	FLUENCE_QUANTUM_CONFIG  path to a YAML/JSON list of quantum backends
//	FLUENCE_MATCH_POLICY    Fluxion match policy (default "first")
func New(ctx context.Context, _ runtime.Object, h fwk.Handle) (fwk.Plugin, error) {
	// List nodes via the API. The scheduler's shared snapshot is empty at
	// plugin-construction time (it is populated per scheduling cycle once
	// informers have synced), so a direct List is what actually gives us the
	// cluster's compute. We assume a static cluster for now: this is read once
	// at startup and the graph is not updated as nodes come and go.
	nodeList, err := h.ClientSet().CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}

	// Classical compute always comes from the cluster nodes. Quantum resources
	// are added only when a backends config is provided.
	opts := cluster.Options{}
	if path := os.Getenv("FLUENCE_QUANTUM_CONFIG"); path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read quantum config: %w", err)
		}
		qc, err := cluster.LoadQuantumConfig(raw)
		if err != nil {
			return nil, err
		}
		opts.Quantum = qc.Backends
	}

	jgfBytes, err := cluster.BuildGraph(nodeList.Items, opts)
	if err != nil {
		return nil, fmt.Errorf("build resource graph: %w", err)
	}

	// FluxionGraph.Init reads from a file path, so stage the generated graph.
	tmp, err := os.CreateTemp("", "fluence-graph-*.json")
	if err != nil {
		return nil, err
	}
	if _, err := tmp.Write(jgfBytes); err != nil {
		return nil, err
	}
	_ = tmp.Close()

	matcher := &graph.FluxionGraph{MatchFormat: "jgf"}
	matcher.Init(tmp.Name(), os.Getenv("FLUENCE_MATCH_POLICY"), "")

	return &Fluence{
		handle:    h,
		matcher:   matcher,
		placement: map[string][]string{},
	}, nil
}

// Name returns the plugin name.
func (f *Fluence) Name() string { return Name }

// PreFilter runs once per scheduling cycle for a pod. The first pod of a group
// triggers a single match for the whole gang; the resulting node assignment is
// cached and consumed by Filter for every pod in the group.
func (f *Fluence) PreFilter(
	ctx context.Context,
	state fwk.CycleState,
	pod *corev1.Pod,
	nodes []fwk.NodeInfo,
) (*fwk.PreFilterResult, *fwk.Status) {
	group := groupKey(pod)

	f.mu.Lock()
	_, done := f.placement[group]
	f.mu.Unlock()
	if done {
		return nil, fwk.NewStatus(fwk.Success)
	}

	pods, err := f.groupPods(pod)
	if err != nil {
		return nil, fwk.AsStatus(err)
	}

	js, err := placement.JobspecForGroup(group, pods)
	if err != nil {
		return nil, fwk.AsStatus(err)
	}
	specYAML, err := js.YAML()
	if err != nil {
		return nil, fwk.AsStatus(err)
	}

	req, err := f.matcher.MatchAllocateSpec(specYAML)
	if err != nil {
		return nil, fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("fluxion match failed: %v", err))
	}
	place, err := placement.PlacementFromAllocation(req.Allocation)
	if err != nil {
		return nil, fwk.AsStatus(err)
	}
	if len(place.Nodes) == 0 {
		return nil, fwk.NewStatus(fwk.Unschedulable, "fluxion returned no node placement")
	}

	f.mu.Lock()
	f.placement[group] = place.Nodes
	f.mu.Unlock()

	// place.Backend (quantum) would be recorded on the pod(s) here so the
	// workload knows which QRMI backend to submit to (e.g. via annotation/env).
	return nil, fwk.NewStatus(fwk.Success)
}

// PreFilterExtensions: no add/remove pod handling for now.
func (f *Fluence) PreFilterExtensions() fwk.PreFilterExtensions { return nil }

// Filter permits a node only if Fluxion assigned it to this group.
func (f *Fluence) Filter(
	ctx context.Context,
	state fwk.CycleState,
	pod *corev1.Pod,
	nodeInfo fwk.NodeInfo,
) *fwk.Status {
	group := groupKey(pod)

	f.mu.Lock()
	nodes := f.placement[group]
	f.mu.Unlock()

	for _, n := range nodes {
		if n == nodeInfo.Node().Name {
			return fwk.NewStatus(fwk.Success)
		}
	}
	return fwk.NewStatus(fwk.Unschedulable, "node not in fluxion allocation for this group")
}

// groupPods returns the pods belonging to the same group as pod, by label.
func (f *Fluence) groupPods(pod *corev1.Pod) ([]corev1.Pod, error) {
	group := pod.Labels[placement.PodGroupLabel]
	if group == "" {
		// Singleton pod: treat it as its own group of one.
		return []corev1.Pod{*pod}, nil
	}
	sel := labels.SelectorFromSet(labels.Set{placement.PodGroupLabel: group})
	list, err := f.handle.SharedInformerFactory().Core().V1().Pods().Lister().
		Pods(pod.Namespace).List(sel)
	if err != nil {
		return nil, err
	}
	out := make([]corev1.Pod, 0, len(list))
	for _, p := range list {
		out = append(out, *p)
	}
	return out, nil
}

// groupKey is the cache key for a pod's group (namespace-scoped).
func groupKey(pod *corev1.Pod) string {
	if g := pod.Labels[placement.PodGroupLabel]; g != "" {
		return pod.Namespace + "/" + g
	}
	return pod.Namespace + "/" + pod.Name
}
