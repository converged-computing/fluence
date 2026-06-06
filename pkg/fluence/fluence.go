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
	"k8s.io/apimachinery/pkg/types"
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
	// placement maps a pod-group key to the placement chosen for the group
	// (nodes + allocated backend).
	placement map[string]placement.Placement
}

var (
	_ fwk.PreFilterPlugin = (*Fluence)(nil)
	_ fwk.FilterPlugin    = (*Fluence)(nil)
	_ fwk.PreBindPlugin   = (*Fluence)(nil)
)

// New builds the plugin: discover cluster nodes, optionally inject quantum
// resources, write the JGF graph, and initialize the Fluxion matcher.
//
// Configuration (for now via env; can move to plugin args):
//
//	FLUENCE_RESOURCES       path to a YAML/JSON resources config (e.g. quantum
//	                        backends). Unset = classical-only graph.
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

	// Classical compute always comes from the cluster nodes. Quantum/other
	// resources are added only when a resources config is present. FLUENCE_RESOURCES
	// is set on the base scheduler but the file only exists once the resources
	// add-on is applied, so a missing file is normal (classical-only), not fatal.
	opts := cluster.Options{}
	if path := os.Getenv("FLUENCE_RESOURCES"); path != "" {
		raw, err := os.ReadFile(path)
		switch {
		case err == nil:
			qc, err := cluster.LoadQuantumConfig(raw)
			if err != nil {
				return nil, err
			}
			opts.Quantum = qc.Backends
		case os.IsNotExist(err):
			// No resources config mounted -> classical-only graph.
		default:
			return nil, fmt.Errorf("read resources config %s: %w", path, err)
		}
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
		placement: map[string]placement.Placement{},
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
	if len(place.Nodes) == 0 && place.Backend == "" {
		return nil, fwk.NewStatus(fwk.Unschedulable, "fluxion returned no allocation")
	}
	// Note: a quantum-only allocation has a Backend but no Nodes (a qpu vertex
	// lives under the qgateway, not under a compute node). That is valid — the
	// backend is a remote API reachable from any node — so we do not require a
	// node here; Filter imposes no node constraint in that case.

	f.mu.Lock()
	f.placement[group] = place
	f.mu.Unlock()

	// The allocated backend is recorded onto each pod in PreBind (container env
	// is immutable post-creation, but annotations can be patched); the
	// webhook-injected downward-API env then surfaces it as QRMI_BACKEND.
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
	nodes := f.placement[group].Nodes
	f.mu.Unlock()

	// A quantum-only allocation pins no node (the backend is a remote API any
	// node can reach), so impose no constraint; the qpu device plugin already
	// gates which nodes can admit the pod.
	if len(nodes) == 0 {
		return fwk.NewStatus(fwk.Success)
	}

	for _, n := range nodes {
		if n == nodeInfo.Node().Name {
			return fwk.NewStatus(fwk.Success)
		}
	}
	return fwk.NewStatus(fwk.Unschedulable, "node not in fluxion allocation for this group")
}

// PreBindPreFlight runs before PreBind. It returns Success when this plugin has
// a backend to stamp on the pod (a quantum group), and Skip otherwise so the
// framework doesn't call PreBind needlessly. It is lightweight: it only reads
// the cached group placement, no API calls.
func (f *Fluence) PreBindPreFlight(
	ctx context.Context,
	state fwk.CycleState,
	pod *corev1.Pod,
	nodeName string,
) (*fwk.PreBindPreFlightResult, *fwk.Status) {
	f.mu.Lock()
	backend := f.placement[groupKey(pod)].Backend
	f.mu.Unlock()
	if backend == "" {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	return nil, fwk.NewStatus(fwk.Success)
}

// PreBind writes the backend Fluxion allocated for this pod's group onto the pod
// as the annotation placement.BackendAnnotation. The mutating webhook has
// already wired a downward-API env (QRMI_BACKEND) that reads this annotation, so
// the container sees the backend as an ordinary env var. Container env cannot be
// patched after creation, which is why the value travels via an annotation.
func (f *Fluence) PreBind(
	ctx context.Context,
	state fwk.CycleState,
	pod *corev1.Pod,
	nodeName string,
) *fwk.Status {
	f.mu.Lock()
	backend := f.placement[groupKey(pod)].Backend
	f.mu.Unlock()
	if backend == "" {
		return fwk.NewStatus(fwk.Success) // nothing to do; PreBindPreFlight skips these
	}

	patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, placement.BackendAnnotation, backend)
	_, err := f.handle.ClientSet().CoreV1().Pods(pod.Namespace).Patch(
		ctx, pod.Name, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	if err != nil {
		return fwk.AsStatus(fmt.Errorf("stamp backend annotation: %w", err))
	}
	return fwk.NewStatus(fwk.Success)
}

// groupPods returns the pods belonging to the same native PodGroup as pod
// (spec.schedulingGroup.podGroupName). That field is not label-selectable, so we
// list the namespace and filter in code. A pod with no scheduling group is its
// own group of one.
func (f *Fluence) groupPods(pod *corev1.Pod) ([]corev1.Pod, error) {
	group := placement.PodGroupName(pod)
	if group == "" {
		return []corev1.Pod{*pod}, nil
	}
	list, err := f.handle.SharedInformerFactory().Core().V1().Pods().Lister().
		Pods(pod.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}
	out := make([]corev1.Pod, 0, len(list))
	for _, p := range list {
		if placement.PodGroupName(p) == group {
			out = append(out, *p)
		}
	}
	return out, nil
}

// groupKey is the cache key for a pod's group (namespace-scoped).
func groupKey(pod *corev1.Pod) string {
	if g := placement.PodGroupName(pod); g != "" {
		return pod.Namespace + "/" + g
	}
	return pod.Namespace + "/" + pod.Name
}
