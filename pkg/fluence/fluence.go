package fluence

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/converged-computing/fluence/pkg/cluster"
	"github.com/converged-computing/fluence/pkg/graph"
	"github.com/converged-computing/fluence/pkg/jobspec"
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

// matcher is the subset of *graph.FluxionGraph the plugin depends on. Declaring
// it as an interface lets tests inject a fake (the real matcher is cgo/flux and
// cannot run in a unit test). *graph.FluxionGraph satisfies this.
type matcher interface {
	MatchAllocateSpec(spec string) (graph.MatchAllocateRequest, error)
	Cancel(jobid uint64) error
}

// groupAlloc is the in-memory record of a group's Fluxion allocation. It is a
// rebuildable, within-lifetime memo: its job is race-free "match once per group"
// dedup on the scheduling path (the durable record is the jobid annotation on
// the owning object). It does not survive a scheduler restart, which is fine —
// the graph itself is rebuilt fresh on restart.
type groupAlloc struct {
	place placement.Placement
	// jobids are the Fluxion allocations backing this group — one per match
	// (compute, plus one per requested virtual device). All are held (duration 0)
	// and cancelled together; the group is all-or-nothing across them.
	jobids []uint64
}

// Fluence is a scheduler-framework plugin that places whole pod groups by
// matching them against a flux-sched resource graph built from the live cluster
// (plus any configured virtual resources). Gang/all-or-nothing semantics are
// delegated to the native PodGroup API; Fluence only decides placement.
type Fluence struct {
	handle  fwk.Handle
	matcher matcher

	// knownDevices is the set of virtual resource types the graph models (suffix
	// only, e.g. "qpu"), used to reject a pod requesting a device that does not
	// exist before issuing a match. Empty when no resources are configured.
	knownDevices map[string]bool

	// matcherMu serializes all access to the cgo Fluxion client, which is not
	// thread-safe. Match runs on the (sequential) scheduling path; Cancel runs in
	// informer handler goroutines, so the two can race without this.
	matcherMu sync.Mutex

	mu sync.Mutex
	// placement maps a group key to its allocation (nodes, backend, jobids).
	placement map[string]groupAlloc
	// excludedNodes maps a group key to the set of node names that have been
	// rejected for that group by other scheduler plugins (taints, affinity,
	// volume topology that Fluxion's graph does not model). PostFilter adds the
	// whole failed allocation's nodes here; PreFilter feeds them back as an RFC 31
	// negated-hostlist constraint so the re-match is forced onto untried nodes.
	// The set only grows for a group, guaranteeing the retry converges (finite
	// node pool) and is cleared on teardown. Guarded by mu.
	excludedNodes map[string]map[string]bool
}

var (
	_ fwk.PreFilterPlugin  = (*Fluence)(nil)
	_ fwk.FilterPlugin     = (*Fluence)(nil)
	_ fwk.PostFilterPlugin = (*Fluence)(nil)
	_ fwk.PreBindPlugin    = (*Fluence)(nil)
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
	knownDevices := map[string]bool{}
	if path := os.Getenv("FLUENCE_RESOURCES"); path != "" {
		raw, err := os.ReadFile(path)
		switch {
		case err == nil:
			rc, err := cluster.LoadResourcesConfig(raw)
			if err != nil {
				return nil, err
			}
			opts.Resources = rc.Resources
			// The requestable device types are the FluxionResourceNames, minus
			// the prefix — the suffixes a pod requests as
			// fluxion.flux-framework.org/<type>.
			for _, name := range cluster.FluxionResourceNames(rc.Resources) {
				knownDevices[strings.TrimPrefix(name, placement.FluxionResourcePrefix)] = true
			}
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
	fmt.Println("[fluence] === RESOURCE GRAPH (knownDevices=" +
		fmt.Sprintf("%v", keysOf(knownDevices)) + ") ===")
	fmt.Println(string(jgfBytes))

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
	// jgf match format: emits every allocated vertex (with properties) as a graph,
	// regardless of type. rv1 cannot represent our allocations — its R_lite is
	// built from core/gpu "reducer" children under a node, so a virtual allocation
	// that bottoms out in nodes (no cores) serializes to an empty R. jgf has no
	// such assumption and is exactly what PlacementFromAllocation parses (node
	// vertices + their composed marker/attribute properties).
	fluxion := &graph.FluxionGraph{MatchFormat: "jgf"}
	fluxion.Init(tmp.Name(), os.Getenv("FLUENCE_MATCH_POLICY"), "")

	f := &Fluence{
		handle:        h,
		matcher:       fluxion,
		knownDevices:  knownDevices,
		placement:     map[string]groupAlloc{},
		excludedNodes: map[string]map[string]bool{},
	}
	f.registerCancelHandlers()
	// Periodic + startup reconcile of completed Fluence-created PodGroups, so a
	// gang's allocation is freed even if a completion/delete event is missed
	// (informer resync gaps, controller restart, force-deleted pods). The
	// event-driven path (onPodUpdated) handles the common case promptly; this is
	// the correctness backstop.
	go f.runReconcileSweeps(ctx)
	return f, nil
}

// runReconcileSweeps reconciles all Fluence-created PodGroups on startup and then
// periodically. On startup it also catches allocations that outlived a scheduler
// restart: a completed gang whose PodGroup still exists is deleted here, freeing
// its allocation via onPodGroupDeleted (cancelGroup reads jobids from the
// PodGroup annotation, which survives restart, so the free works even though the
// in-memory placement memo was lost).
func (f *Fluence) runReconcileSweeps(ctx context.Context) {
	// Let informer caches sync before the first sweep so listers are populated.
	if !cache.WaitForCacheSync(ctx.Done(),
		f.handle.SharedInformerFactory().Scheduling().V1alpha2().PodGroups().Informer().HasSynced,
		f.handle.SharedInformerFactory().Core().V1().Pods().Informer().HasSynced) {
		return
	}
	f.reconcileAll(ctx)
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.reconcileAll(ctx)
		}
	}
}

// reconcileAll lists every Fluence-created PodGroup and reconciles it (deleting
// the ones whose gangs have fully completed). Listing is from the informer
// cache, so this is cheap to run periodically.
func (f *Fluence) reconcileAll(ctx context.Context) {
	pgs, err := f.handle.SharedInformerFactory().Scheduling().V1alpha2().
		PodGroups().Lister().List(labels.Everything())
	if err != nil {
		log.Printf("fluence: reconcile sweep list failed: %v", err)
		return
	}
	for _, pg := range pgs {
		if pg.Annotations[placement.CreatedByAnnotation] != placement.CreatedByValue {
			continue // only ever consider Fluence-created PodGroups
		}
		f.reconcileGroup(ctx, pg.Namespace, pg.Name)
	}
}

// reconcileInterval is how often the backstop sweep runs. The event-driven path
// handles the prompt case; this only needs to be frequent enough to bound how
// long a leaked allocation can persist after a missed event.
const reconcileInterval = 60 * time.Second

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

	f.mu.Lock()
	excluded := make([]string, 0, len(f.excludedNodes[group]))
	for n := range f.excludedNodes[group] {
		excluded = append(excluded, n)
	}
	f.mu.Unlock()
	sort.Strings(excluded) // deterministic constraint for stable matching/logs

	specs, err := placement.JobspecsForGroup(group, pods, f.knownDevices, excluded)
	if err != nil {
		return nil, fwk.AsStatus(err)
	}

	// Run every jobspec as an independent held allocation (duration 0). The group
	// is all-or-nothing: if any match fails, cancel the ones that already
	// succeeded so we never hold a partial allocation (e.g. compute without its
	// device, or vice versa).
	place, jobids, status := f.matchGroup(specs)
	if !status.IsSuccess() {
		return nil, status
	}

	f.mu.Lock()
	f.placement[group] = groupAlloc{place: place, jobids: jobids}
	f.mu.Unlock()

	// The jobid (for cancel) and any backend (for the webhook env) are written
	// onto the owning object in PreBind, the commit phase.
	return nil, fwk.NewStatus(fwk.Success)
}

// matchGroup runs each jobspec as an independent held Fluxion allocation and
// combines them into one placement. It is all-or-nothing: on the first failure
// it cancels every allocation already made and returns an Unschedulable status,
// so the group never holds a partial set (compute without its device, etc.).
//
// The combined placement unions the per-match results: the compute match
// supplies the bind nodes, a device match supplies the backend identity. (The
// per-match split of nodes vs backend is PlacementFromAllocation's job; here we
// merge.)
func (f *Fluence) matchGroup(specs []*jobspec.Jobspec) (placement.Placement, []uint64, *fwk.Status) {
	var combined placement.Placement
	var jobids []uint64

	for i, js := range specs {
		// Render the jobspec as JSON, not YAML. flux-sched's RFC 31 constraint
		// parser requires each property to be a QUOTED scalar (it checks the YAML
		// tag == "!"); sigs.k8s.io/yaml emits property strings unquoted
		// (e.g. "- virtual=false"), which the parser rejects with "non-string
		// property specified" -> the whole match fails with -1. JSON always quotes
		// strings, and JSON is valid YAML input to the matcher, so this is the
		// reliable encoding for jobspecs that carry constraints.
		spec, err := js.JSON()
		if err != nil {
			f.cancelJobids(jobids)
			return placement.Placement{}, nil, fwk.AsStatus(err)
		}

		fmt.Println(fmt.Sprintf("[fluence] === MATCH %d/%d: submitting jobspec to fluxion ===", i+1, len(specs)))
		fmt.Println(spec)

		f.matcherMu.Lock()
		req, err := f.matcher.MatchAllocateSpec(spec)
		f.matcherMu.Unlock()
		if err != nil {
			log.Printf("[fluence] MATCH %d/%d FAILED: %v — rolling back jobids %v",
				i+1, len(specs), err, jobids)
			f.cancelJobids(jobids)
			return placement.Placement{}, nil, fwk.NewStatus(
				fwk.Unschedulable, fmt.Sprintf("fluxion match failed: %v", err))
		}

		fmt.Println(fmt.Sprintf("[fluence] MATCH %d/%d allocated jobid %d; fluxion R:", i+1, len(specs), req.Number))
		fmt.Println(req.Allocation)

		place, err := placement.PlacementFromAllocation(req.Allocation)
		if err != nil {
			log.Printf("[fluence] MATCH %d/%d placement-parse FAILED: %v", i+1, len(specs), err)
			f.cancelJobids(append(jobids, req.Number))
			return placement.Placement{}, nil, fwk.AsStatus(err)
		}
		fmt.Println(fmt.Sprintf("[fluence] MATCH %d/%d parsed: nodes=%v backend=%q attrs=%v",
			i+1, len(specs), place.Nodes, place.Backend, place.BackendAttributes))

		jobids = append(jobids, req.Number)
		combined.Nodes = append(combined.Nodes, place.Nodes...)
		if place.Backend != "" {
			combined.Backend = place.Backend
			combined.BackendAttributes = place.BackendAttributes
		}
	}

	if len(combined.Nodes) == 0 && combined.Backend == "" {
		log.Printf("[fluence] match produced no nodes and no backend — unschedulable")
		f.cancelJobids(jobids)
		return placement.Placement{}, nil, fwk.NewStatus(
			fwk.Unschedulable, "fluxion returned no allocation")
	}
	fmt.Println(fmt.Sprintf("[fluence] GROUP MATCHED: nodes=%v backend=%q attrs=%v jobids=%v",
		combined.Nodes, combined.Backend, combined.BackendAttributes, jobids))
	return combined, jobids, fwk.NewStatus(fwk.Success)
}

// cancelJobids frees a set of held allocations, used to unwind a partial group
// match. Cancel is idempotent and best-effort; errors are logged, not returned.
func (f *Fluence) cancelJobids(jobids []uint64) {
	for _, id := range jobids {
		f.matcherMu.Lock()
		err := f.matcher.Cancel(id)
		f.matcherMu.Unlock()
		if err != nil {
			log.Printf("fluence: rollback cancel of jobid %d failed: %v", id, err)
		}
	}
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

// PostFilter runs when a pod could not be scheduled after Filter — for a Fluence
// group, this means the cached Fluxion allocation's nodes did not all survive
// the other scheduler plugins' Filter checks (a taint, node affinity, or volume
// topology constraint that Fluxion's resource graph does not model rejected one
// or more of them). Without intervention the group would retry forever against
// the same cached allocation while the Fluxion reservation leaked, because
// PreFilter short-circuits on the cache and nothing else releases it on a
// scheduling failure.
//
// We react by abandoning the failed allocation: the ENTIRE cached node set is
// added to the group's exclusion set, the Fluxion jobids are cancelled, and the
// cached placement is deleted. The next PreFilter for the group re-matches with
// an RFC 31 negated-hostlist constraint over the accumulated exclusion set, so
// Fluxion is forced onto untried nodes. We exclude the whole set (not just the
// individually-rejected nodes) deliberately: if the group as a whole could not
// be admitted, a node that happened to survive this round carries no guarantee
// for the next, and excluding the whole set makes each retry a strictly smaller,
// monotonic search that converges — either to a feasible allocation on untried
// nodes, or to a clean no-match (Unschedulable) once the graph is exhausted, at
// which point the pod waits for a cluster-state change rather than busy-looping.
func (f *Fluence) PostFilter(
	ctx context.Context,
	state fwk.CycleState,
	pod *corev1.Pod,
	filteredNodeStatusMap fwk.NodeToStatusReader,
) (*fwk.PostFilterResult, *fwk.Status) {
	group := groupKey(pod)

	f.mu.Lock()
	alloc, ok := f.placement[group]
	if !ok {
		// No cached allocation for this group — nothing of ours to reconcile.
		// (Another plugin's PostFilter, or a non-group pod.)
		f.mu.Unlock()
		return nil, fwk.NewStatus(fwk.Unschedulable)
	}
	// Accumulate the whole failed allocation's nodes into the exclusion set.
	if f.excludedNodes[group] == nil {
		f.excludedNodes[group] = map[string]bool{}
	}
	for _, n := range alloc.place.Nodes {
		f.excludedNodes[group][n] = true
	}
	excludedCount := len(f.excludedNodes[group])
	jobids := alloc.jobids
	delete(f.placement, group)
	f.mu.Unlock()

	// Release the Fluxion reservation for the abandoned allocation so the graph
	// does not leak it while the group retries.
	f.cancelJobids(jobids)

	log.Printf("[fluence] group %s unschedulable: abandoning allocation (nodes %v, "+
		"jobids %v); %d node(s) now excluded, will re-match on next cycle",
		group, alloc.place.Nodes, jobids, excludedCount)

	// Returning Unschedulable (no nominated node) lets the pod be requeued; the
	// next PreFilter re-matches with the enlarged exclusion set. We do not
	// nominate a node — Fluxion, not PostFilter preemption, chooses the next
	// placement.
	return nil, fwk.NewStatus(fwk.Unschedulable)
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

	if err := f.recordJobIDs(ctx, pod, alloc.jobids); err != nil {
		return fwk.AsStatus(fmt.Errorf("record jobids: %w", err))
	}
	if alloc.place.Backend != "" {
		// Stamp the backend name and all matched attributes in one patch. The
		// webhook injects a normalized env per annotation so the workload reads
		// exactly what it matched (backend + region/qubits/...).
		ann := map[string]string{placement.BackendAnnotation: alloc.place.Backend}
		for k, v := range alloc.place.BackendAttributes {
			ann[placement.AttributeAnnotationPrefix+k] = v
		}
		log.Printf("[fluence] group %s -> backend %q attrs %v (nodes %v, jobids %v)",
			groupKey(pod), alloc.place.Backend, alloc.place.BackendAttributes,
			alloc.place.Nodes, alloc.jobids)
		if err := f.patchPodAnnotations(ctx, pod.Namespace, pod.Name, ann); err != nil {
			return fwk.AsStatus(fmt.Errorf("stamp backend annotations: %w", err))
		}
	}
	return fwk.NewStatus(fwk.Success)
}

// recordJobIDs writes the jobid annotation (a comma-separated list of all the
// group's held allocations) onto the allocation's owning object: a grouped pod's
// allocation belongs to the PodGroup; an ungrouped pod owns its own.
func (f *Fluence) recordJobIDs(ctx context.Context, pod *corev1.Pod, jobids []uint64) error {
	val := formatJobIDs(jobids)
	if group := placement.PodGroupName(pod); group != "" {
		patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, placement.JobIDAnnotation, val)
		_, err := f.handle.ClientSet().SchedulingV1alpha2().PodGroups(pod.Namespace).Patch(
			ctx, group, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
		return err
	}
	return f.patchPodAnnotation(ctx, pod.Namespace, pod.Name, placement.JobIDAnnotation, val)
}

// keysOf returns the keys of a set, for logging.
func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// formatJobIDs renders jobids as a comma-separated string for the annotation.
func formatJobIDs(jobids []uint64) string {
	parts := make([]string, len(jobids))
	for i, id := range jobids {
		parts[i] = strconv.FormatUint(id, 10)
	}
	return strings.Join(parts, ",")
}

func (f *Fluence) patchPodAnnotation(ctx context.Context, ns, name, key, val string) error {
	return f.patchPodAnnotations(ctx, ns, name, map[string]string{key: val})
}

// patchPodAnnotations merges a set of annotations onto a pod in one patch.
func (f *Fluence) patchPodAnnotations(ctx context.Context, ns, name string, ann map[string]string) error {
	parts := make([]string, 0, len(ann))
	for k, v := range ann {
		parts = append(parts, fmt.Sprintf("%q:%q", k, v))
	}
	patch := fmt.Sprintf(`{"metadata":{"annotations":{%s}}}`, strings.Join(parts, ","))
	_, err := f.handle.ClientSet().CoreV1().Pods(ns).Patch(
		ctx, name, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	return err
}

// registerCancelHandlers watches PodGroup and Pod deletions AND pod completion,
// and frees the corresponding Fluxion allocation. The framework has no deletion
// or completion extension point, so this is informer-driven. A finished pod is
// not deleted (it lingers in Succeeded/Failed), so completion must be watched
// separately from deletion or the allocation leaks until the pod is removed.
func (f *Fluence) registerCancelHandlers() {
	sif := f.handle.SharedInformerFactory()
	_, _ = sif.Scheduling().V1alpha2().PodGroups().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: f.onPodGroupDeleted,
	})
	_, _ = sif.Core().V1().Pods().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: f.onPodUpdated,
		DeleteFunc: f.onPodDeleted,
	})
}

// podTerminal reports whether a pod has reached a terminal phase (run to
// completion or failed) — at which point its held allocation should be freed.
func podTerminal(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed
}

// onPodUpdated frees an ungrouped pod's allocation when the pod finishes, and
// for a GROUPED pod triggers gang cleanup when the whole group has finished. A
// completed pod is not deleted, so this is the path that releases resources when
// a job ends normally; the DeleteFunc path remains as a backstop. Cancel and
// PodGroup deletion are both idempotent, so repeated triggers are safe.
func (f *Fluence) onPodUpdated(oldObj, newObj interface{}) {
	oldPod, ok := oldObj.(*corev1.Pod)
	if !ok {
		return
	}
	newPod, ok := newObj.(*corev1.Pod)
	if !ok {
		return
	}
	// Only act on the transition INTO a terminal phase (UpdateFunc fires on every
	// pod update; this avoids re-acting on each subsequent update).
	if podTerminal(oldPod) || !podTerminal(newPod) {
		return
	}
	group := placement.PodGroupName(newPod)
	if group == "" {
		// Ungrouped pod: its allocation is owned by the pod; free on completion.
		f.cancelGroup(newPod.Namespace+"/"+newPod.Name, newPod.Annotations)
		return
	}
	// Grouped pod: the gang's allocation is owned by the PodGroup, so we must not
	// free it when a single member finishes. Instead, when the LAST member goes
	// terminal, reconcile the group — which (for a Fluence-created PodGroup)
	// deletes the PodGroup and lets onPodGroupDeleted free the allocation.
	f.reconcileGroup(context.Background(), newPod.Namespace, group)
}

// reconcileGroup deletes a COMPLETED, Fluence-created gang's PodGroup so its
// Fluxion allocation is freed (through the normal onPodGroupDeleted path). It is
// the single decision point for "is this gang done, and may we clean it up?" and
// is safe to call repeatedly (PodGroup deletion is idempotent).
//
// Safety rules, in order:
//  1. The PodGroup must exist and carry the Fluence ownership annotation. A
//     user-created PodGroup is NEVER deleted, even if all its pods are terminal.
//  2. The gang must have actually materialized: at least one member pod must
//     exist. This guards the window right after the leader is admitted, before
//     the gated workers are created — we must not reap a group that has not yet
//     grown its members.
//  3. EVERY member pod (gated or not) must be in a terminal phase. A single
//     running or still-gated member keeps the gang alive — exactly the property
//     that prevents freeing resources out from under workers that ungated after
//     the leader finished.
func (f *Fluence) reconcileGroup(ctx context.Context, namespace, group string) {
	// Defensive: the scheduler handle (and thus the API client / informers) may
	// be absent in unit tests or before the framework is fully wired. Reconcile
	// needs the API to read/delete PodGroups, so without a handle it is a no-op
	// rather than a nil dereference.
	if f.handle == nil || group == "" {
		return
	}
	pg, err := f.handle.ClientSet().SchedulingV1alpha2().PodGroups(namespace).
		Get(ctx, group, metav1.GetOptions{})
	if err != nil {
		return // gone already, or not ours to inspect
	}
	if pg.Annotations[placement.CreatedByAnnotation] != placement.CreatedByValue {
		return // rule 1: not Fluence-created — never delete a user's PodGroup
	}

	members, err := f.handle.SharedInformerFactory().Core().V1().Pods().Lister().
		Pods(namespace).List(labels.Everything())
	if err != nil {
		return
	}
	seen := 0
	for _, p := range members {
		// Match by the native scheduling-group field OR the group label, so a
		// gated worker that has not yet been linked still counts as a member.
		if placement.PodGroupName(p) != group &&
			p.Labels[webhookGroupLabel] != group {
			continue
		}
		seen++
		if !podTerminal(p) {
			return // rule 3: a live/gated member keeps the gang alive
		}
	}

	// Rule 2 (refined): distinguish two "zero live members" states, which member
	// count alone cannot:
	//   (a) brand-new gang: PodGroup just created, workers not yet created. Must
	//       NOT reap — that would kill a gang before it starts.
	//   (b) finished/abandoned gang: pods completed and were already removed,
	//       leaving the PodGroup holding a Fluxion allocation. MUST reap, or the
	//       allocation leaks and blocks future gangs ("qpu match failed -1").
	// We tell them apart by whether the gang was ever scheduled: a scheduled gang
	// carries the jobid annotation (written in PreBind). A jobid + zero live
	// members ⇒ case (b), reap. No jobid + zero members ⇒ case (a) or never
	// scheduled; protect it with a creation grace period as a backstop.
	if seen == 0 {
		_, scheduled := parseJobIDs(pg.Annotations)
		if !scheduled {
			if time.Since(pg.CreationTimestamp.Time) < reconcileGraceForEmpty {
				return // case (a): brand-new, give workers time to appear
			}
			// old, never scheduled, no members — nothing was ever allocated, but
			// it is safe (and tidy) to remove a stale empty PodGroup we created.
		}
		// else: scheduled (has jobid) with no live members ⇒ case (b), reap.
	}

	// All members terminal (or a scheduled gang whose pods are already gone) and
	// the PodGroup is ours: delete it. onPodGroupDeleted then frees the Fluxion
	// allocation (cancelGroup reads jobids from the annotation).
	if err := f.handle.ClientSet().SchedulingV1alpha2().PodGroups(namespace).
		Delete(ctx, group, metav1.DeleteOptions{}); err != nil {
		log.Printf("fluence: reconcile delete of completed PodGroup %s/%s failed: %v",
			namespace, group, err)
		return
	}
	log.Printf("fluence: reconciled completed gang %s/%s — deleted Fluence-created PodGroup, allocation freed",
		namespace, group)
}

// reconcileGraceForEmpty is how long a Fluence-created PodGroup with no live
// members and no allocation (never scheduled) is protected from reaping, to
// cover the window between PodGroup creation and the gang's pods appearing.
const reconcileGraceForEmpty = 2 * time.Minute

// webhookGroupLabel duplicates pkg/webhook.GroupLabel without importing that
// package (the scheduler must not depend on the webhook). Kept in sync with it.
const webhookGroupLabel = "fluence.flux-framework.org/group"

// onPodGroupDeleted frees the gang's allocation when its PodGroup is deleted.
func (f *Fluence) onPodGroupDeleted(obj interface{}) {
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
}

// onPodDeleted frees an ungrouped pod's allocation when the pod is deleted.
// Grouped pods are ignored: their allocation is owned by the PodGroup and is
// freed only when the PodGroup is deleted (freeing it on a single pod's delete
// would release the whole gang's resources while its other pods still run).
func (f *Fluence) onPodDeleted(obj interface{}) {
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
	if g := placement.PodGroupName(pod); g != "" {
		// Grouped pod deleted: do not free the gang's allocation on a single
		// pod's removal, but DO reconcile — if this was the last member, the
		// PodGroup is now empty-but-allocated and reconcileGroup will reap it
		// (and free the allocation). This closes the gap where a completed gang's
		// pods are deleted (not just completed): onPodUpdated never fires for a
		// deletion, so without this the cleanup would wait for the periodic sweep.
		f.reconcileGroup(context.Background(), pod.Namespace, g)
		return
	}
	f.cancelGroup(pod.Namespace+"/"+pod.Name, pod.Annotations)
}

// cancelGroup frees all allocations for a deleted owning object. The jobids come
// from the object's annotation (the durable source of truth); if it is missing
// (e.g. deleted between PreFilter and PreBind, before the annotation was
// written) it falls back to the in-memory memo by key. Cancel is idempotent.
func (f *Fluence) cancelGroup(key string, ann map[string]string) {
	jobids, ok := parseJobIDs(ann)
	if !ok {
		f.mu.Lock()
		alloc, found := f.placement[key]
		f.mu.Unlock()
		if !found {
			return // never scheduled by us, or already cancelled
		}
		jobids = alloc.jobids
	}

	for _, jobid := range jobids {
		f.matcherMu.Lock()
		err := f.matcher.Cancel(jobid)
		f.matcherMu.Unlock()
		if err != nil {
			log.Printf("fluence: cancel jobid %d for %s failed: %v", jobid, key, err)
		}
	}

	f.mu.Lock()
	delete(f.placement, key)
	delete(f.excludedNodes, key) // drop accumulated exclusions so a future group reusing the name starts clean
	f.mu.Unlock()
}

// parseJobIDs reads the comma-separated jobid annotation into a slice. Returns
// false when the annotation is absent or empty.
func parseJobIDs(ann map[string]string) ([]uint64, bool) {
	raw := ann[placement.JobIDAnnotation]
	if raw == "" {
		return nil, false
	}
	var jobids []uint64
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return nil, false
		}
		jobids = append(jobids, id)
	}
	if len(jobids) == 0 {
		return nil, false
	}
	return jobids, true
}

// groupPods returns the pods belonging to the same native PodGroup as pod
// (spec.schedulingGroup.podGroupName). That field is not label-selectable, so we
// list the namespace and filter in code. A pod with no scheduling group is its
// own group of one.
//
// Gated pods are excluded: a pod still carrying a scheduling gate is held at
// PreEnqueue and is not part of the live scheduling cycle. In a quantum gang the
// workers stay gated until the sidecar ungates them at QPU position==1, so
// including them here would make the leader's match reserve resources for pods
// that are not yet ready to run — defeating the whole point of gating. An
// ungated pod re-enters the queue and is matched then.
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
		if placement.PodGroupName(p) != group {
			continue
		}
		// Skip pods still holding a scheduling gate — they are not in the live
		// scheduling cycle yet. The pod currently being scheduled (pod) has
		// already cleared its gates by the time PreFilter runs, so it is included.
		if len(p.Spec.SchedulingGates) > 0 {
			continue
		}
		out = append(out, *p)
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
