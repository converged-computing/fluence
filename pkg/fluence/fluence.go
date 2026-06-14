package fluence

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/converged-computing/fluence/pkg/cluster"
	"github.com/converged-computing/fluence/pkg/graph"
	"github.com/converged-computing/fluence/pkg/placement"

	corev1 "k8s.io/api/core/v1"
	schedv1a2 "k8s.io/api/scheduling/v1alpha2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	fwk "k8s.io/kube-scheduler/framework"
)

// The scheduler-framework types live in the staging module
// k8s.io/kube-scheduler/framework (imported as fwk). The plugin is built into
// the scheduler binary via cmd/fluence. Signatures here are verified against
// k8s.io/kube-scheduler v0.36.0.

// Name is the plugin name registered with the scheduler and referenced in the
// KubeSchedulerConfiguration.
const Name = "Fluence"

// groupAlloc is the in-memory record of a group's Fluxion allocation. It is a
// rebuildable, within-lifetime memo: its job is race-free "match once per group"
// dedup on the scheduling path (the durable record is the jobid annotation on
// the owning object). It does not survive a scheduler restart, which is fine —
// the graph itself is rebuilt fresh on restart.
type groupAlloc struct {
	place placement.Placement
	jobid uint64
}

// Fluence is a scheduler-framework plugin that places whole pod groups by
// matching them against a flux-sched resource graph built from the live cluster
// (plus any configured quantum resources). Gang/all-or-nothing semantics are
// delegated to the native PodGroup API; Fluence only decides placement.
type Fluence struct {
	handle  fwk.Handle
	matcher *graph.FluxionGraph

	// matcherMu serializes all access to the cgo Fluxion client, which is not
	// thread-safe. Match runs on the (sequential) scheduling path; Cancel runs in
	// informer handler goroutines, so the two can race without this.
	matcherMu sync.Mutex

	mu sync.Mutex
	// placement maps a group key to its allocation (nodes, backend, jobid).
	placement map[string]groupAlloc
}

var (
	_ fwk.PreFilterPlugin = (*Fluence)(nil)
	_ fwk.FilterPlugin    = (*Fluence)(nil)
	_ fwk.PreBindPlugin   = (*Fluence)(nil)
)

// New builds the plugin: discover cluster nodes, optionally inject quantum
// resources, write the JGF graph, initialize the Fluxion matcher, and register
// the delete handlers that cancel allocations when their owning object is gone.
//
//	FLUENCE_RESOURCES       path to a YAML/JSON resources config (e.g. quantum
//	                        backends). Unset = classical-only graph.
//	FLUENCE_MATCH_POLICY    Fluxion match policy (default "first")
func New(ctx context.Context, _ runtime.Object, h fwk.Handle) (fwk.Plugin, error) {
	// List nodes via the API. The scheduler's shared snapshot is empty at
	// plugin-construction time, so a direct List is what gives us the cluster's
	// compute. Static cluster for now: read once, graph not updated live.
	nodeList, err := h.ClientSet().CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}

	// Quantum/other resources are added only when a resources config is present.
	// FLUENCE_RESOURCES is set on the base scheduler but the file only exists once
	// the resources add-on is applied, so a missing file is classical-only, not
	// fatal.
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

	// rv1 (full writer, with the scheduling key) is a superset of jgf: its
	// scheduling key is the same JGF vertex subgraph we parse for placement, and
	// it carries the execution view flux uses to replay an allocation on restart.
	// This is the format we persist and feed back to UpdateAllocate for recovery.
	matcher := &graph.FluxionGraph{MatchFormat: "rv1"}
	matcher.Init(tmp.Name(), os.Getenv("FLUENCE_MATCH_POLICY"), "")

	f := &Fluence{
		handle:    h,
		matcher:   matcher,
		placement: map[string]groupAlloc{},
	}
	f.registerCancelHandlers()
	return f, nil
}

// Name returns the plugin name.
func (f *Fluence) Name() string { return Name }

// PreFilter runs once per scheduling cycle for a pod. The first pod of a group
// triggers a single match for the whole gang; the resulting placement (and the
// Fluxion jobid) is cached and consumed by Filter/PreBind for every pod.
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

	f.matcherMu.Lock()
	req, err := f.matcher.MatchAllocateSpec(specYAML)
	f.matcherMu.Unlock()
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
	// A quantum-only allocation has a Backend but no Nodes (a qpu vertex lives
	// under the qgateway, not under a compute node). That is valid — the backend
	// is reachable from any node — so Filter imposes no node constraint then.

	f.mu.Lock()
	f.placement[group] = groupAlloc{place: place, jobid: req.Number}
	f.mu.Unlock()

	// The jobid (for cancel) and any backend (for the webhook env) are written
	// onto the owning object in PreBind, the commit phase.
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
	nodes := f.placement[group].place.Nodes
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

// PreBindPreFlight runs before PreBind. It returns Success when we have a cached
// allocation for the pod's group (so PreBind can record the jobid, and stamp the
// backend for a quantum pod), and Skip otherwise.
func (f *Fluence) PreBindPreFlight(
	ctx context.Context,
	state fwk.CycleState,
	pod *corev1.Pod,
	nodeName string,
) (*fwk.PreBindPreFlightResult, *fwk.Status) {
	f.mu.Lock()
	_, ok := f.placement[groupKey(pod)]
	f.mu.Unlock()
	if !ok {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	return nil, fwk.NewStatus(fwk.Success)
}

// PreBind records, in the commit phase, the durable state for this group:
//   - the Fluxion jobid onto the owning object (the PodGroup for a gang, else the
//     pod) so the allocation can be cancelled when that object is deleted;
//   - for a quantum group, the allocated backend onto the pod, which the webhook-
//     injected downward-API env surfaces as QRMI_BACKEND (container env is
//     immutable post-creation, so the value must travel via an annotation).
func (f *Fluence) PreBind(
	ctx context.Context,
	state fwk.CycleState,
	pod *corev1.Pod,
	nodeName string,
) *fwk.Status {
	f.mu.Lock()
	alloc, ok := f.placement[groupKey(pod)]
	f.mu.Unlock()
	if !ok {
		return fwk.NewStatus(fwk.Success) // not ours; nothing to record
	}

	if err := f.recordJobID(ctx, pod, alloc.jobid); err != nil {
		return fwk.AsStatus(fmt.Errorf("record jobid: %w", err))
	}
	if alloc.place.Backend != "" {
		if err := f.patchPodAnnotation(ctx, pod.Namespace, pod.Name, placement.BackendAnnotation, alloc.place.Backend); err != nil {
			return fwk.AsStatus(fmt.Errorf("stamp backend annotation: %w", err))
		}
	}
	return fwk.NewStatus(fwk.Success)
}

// recordJobID writes the jobid annotation onto the allocation's owning object: a
// grouped pod's allocation belongs to the PodGroup; an ungrouped pod owns its own.
func (f *Fluence) recordJobID(ctx context.Context, pod *corev1.Pod, jobid uint64) error {
	val := strconv.FormatUint(jobid, 10)
	if group := placement.PodGroupName(pod); group != "" {
		patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, placement.JobIDAnnotation, val)
		_, err := f.handle.ClientSet().SchedulingV1alpha2().PodGroups(pod.Namespace).Patch(
			ctx, group, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
		return err
	}
	return f.patchPodAnnotation(ctx, pod.Namespace, pod.Name, placement.JobIDAnnotation, val)
}

func (f *Fluence) patchPodAnnotation(ctx context.Context, ns, name, key, val string) error {
	patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, key, val)
	_, err := f.handle.ClientSet().CoreV1().Pods(ns).Patch(
		ctx, name, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	return err
}

// registerCancelHandlers watches PodGroup and Pod deletions and frees the
// corresponding Fluxion allocation. Grouped pods are ignored by the pod handler
// (their allocation lives on the PodGroup); ungrouped pods are handled there.
// The framework has no deletion extension point, so this is informer-driven.
func (f *Fluence) registerCancelHandlers() {
	sif := f.handle.SharedInformerFactory()

	_, _ = sif.Scheduling().V1alpha2().PodGroups().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) {
			pg, ok := obj.(*schedv1a2.PodGroup)
			if !ok {
				tomb, ok := obj.(cache.DeletedFinalStateUnknown)
				if !ok {
					return
				}
				if pg, ok = tomb.Obj.(*schedv1a2.PodGroup); !ok {
					return
				}
			}
			f.cancelGroup(pg.Namespace+"/"+pg.Name, pg.Annotations)
		},
	})

	_, _ = sif.Core().V1().Pods().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				tomb, ok := obj.(cache.DeletedFinalStateUnknown)
				if !ok {
					return
				}
				if pod, ok = tomb.Obj.(*corev1.Pod); !ok {
					return
				}
			}
			// Grouped pods' allocation is owned by the PodGroup; only the
			// PodGroup's deletion frees it. Act on ungrouped pods only.
			if placement.PodGroupName(pod) != "" {
				return
			}
			f.cancelGroup(pod.Namespace+"/"+pod.Name, pod.Annotations)
		},
	})
}

// cancelGroup frees the allocation for a deleted owning object. The jobid comes
// from the object's annotation (the durable source of truth); if it is missing
// (e.g. deleted between PreFilter and PreBind, before the annotation was
// written) it falls back to the in-memory memo by key. Cancel is idempotent.
func (f *Fluence) cancelGroup(key string, ann map[string]string) {
	jobid, ok := parseJobID(ann)
	if !ok {
		f.mu.Lock()
		alloc, found := f.placement[key]
		f.mu.Unlock()
		if !found {
			return // never scheduled by us, or already cancelled
		}
		jobid = alloc.jobid
	}

	f.matcherMu.Lock()
	err := f.matcher.Cancel(jobid)
	f.matcherMu.Unlock()
	if err != nil {
		log.Printf("fluence: cancel jobid %d for %s failed: %v", jobid, key, err)
	}

	f.mu.Lock()
	delete(f.placement, key)
	f.mu.Unlock()
}

func parseJobID(ann map[string]string) (uint64, bool) {
	raw := ann[placement.JobIDAnnotation]
	if raw == "" {
		return 0, false
	}
	jobid, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return jobid, true
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
