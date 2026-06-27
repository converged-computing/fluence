package fluence

import (
	"context"
	"errors"
	"testing"

	"github.com/converged-computing/fluence/pkg/graph"
	"github.com/converged-computing/fluence/pkg/jobspec"
	"github.com/converged-computing/fluence/pkg/placement"

	corev1 "k8s.io/api/core/v1"
	schedv1a2 "k8s.io/api/scheduling/v1alpha2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	fwk "k8s.io/kube-scheduler/framework"
)

// fakeMatcher records Cancel calls so cancel behavior can be asserted without
// the real cgo/flux matcher. It satisfies the package-internal matcher interface.
// matchResults/matchErrs are consumed in order, one per MatchAllocateSpec call,
// to script multi-match (compute + device) scenarios.
type fakeMatcher struct {
	cancelled    []uint64
	cancelErr    error
	matchN       int
	matchResults []graph.MatchAllocateRequest
	matchErrs    []error
}

func (m *fakeMatcher) MatchAllocateSpec(string) (graph.MatchAllocateRequest, error) {
	i := m.matchN
	m.matchN++
	var res graph.MatchAllocateRequest
	if i < len(m.matchResults) {
		res = m.matchResults[i]
	}
	var err error
	if i < len(m.matchErrs) {
		err = m.matchErrs[i]
	}
	return res, err
}

func (m *fakeMatcher) Cancel(jobid uint64) error {
	m.cancelled = append(m.cancelled, jobid)
	return m.cancelErr
}

func newTestFluence(m matcher) *Fluence {
	return &Fluence{
		matcher:       m,
		placement:     map[string]groupAlloc{},
		excludedNodes: map[string]map[string]bool{},
	}
}

func ann(jobid string) map[string]string {
	return map[string]string{placement.JobIDAnnotation: jobid}
}

// groupedPod returns a pod that belongs to the named native PodGroup.
// NOTE: corev1.PodSchedulingGroup is the k8s 1.36 native gang field; if the
// type name differs in your vendored k8s, adjust just this constructor.
func groupedPod(ns, name, group string, annotations map[string]string) *corev1.Pod {
	g := group
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Annotations: annotations},
		Spec: corev1.PodSpec{
			SchedulingGroup: &corev1.PodSchedulingGroup{PodGroupName: &g},
		},
	}
}

func ungroupedPod(ns, name string, annotations map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Annotations: annotations},
	}
}

func TestParseJobIDs(t *testing.T) {
	cases := []struct {
		name   string
		ann    map[string]string
		want   []uint64
		wantOK bool
	}{
		{"single", ann("42"), []uint64{42}, true},
		{"multiple", ann("42,7"), []uint64{42, 7}, true},
		{"spaces", ann("42, 7 ,9"), []uint64{42, 7, 9}, true},
		{"absent", map[string]string{}, nil, false},
		{"nil map", nil, nil, false},
		{"empty value", ann(""), nil, false},
		{"garbage", ann("not-a-number"), nil, false},
		{"zero", ann("0"), []uint64{0}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := parseJobIDs(c.ann)
			if ok != c.wantOK {
				t.Fatalf("parseJobIDs(%v) ok = %t, want %t", c.ann, ok, c.wantOK)
			}
			if ok {
				if len(got) != len(c.want) {
					t.Fatalf("parseJobIDs(%v) = %v, want %v", c.ann, got, c.want)
				}
				for i := range got {
					if got[i] != c.want[i] {
						t.Fatalf("parseJobIDs(%v) = %v, want %v", c.ann, got, c.want)
					}
				}
			}
		})
	}
}

func TestGroupKey(t *testing.T) {
	if got := groupKey(ungroupedPod("default", "solo", nil)); got != "default/solo" {
		t.Fatalf("ungrouped groupKey = %q, want default/solo", got)
	}
	if got := groupKey(groupedPod("default", "training-abc", "training", nil)); got != "default/training" {
		t.Fatalf("grouped groupKey = %q, want default/training", got)
	}
}

// The jobid annotation is the durable source of truth and takes precedence over
// the in-memory memo, even when both are present.
func TestCancelGroupPrefersAnnotation(t *testing.T) {
	m := &fakeMatcher{}
	f := newTestFluence(m)
	f.placement["default/training"] = groupAlloc{jobids: []uint64{42}}

	f.cancelGroup("default/training", ann("99"))

	if len(m.cancelled) != 1 || m.cancelled[0] != 99 {
		t.Fatalf("cancelled = %v, want [99] (annotation wins over memo)", m.cancelled)
	}
	if _, still := f.placement["default/training"]; still {
		t.Fatal("placement entry should be deleted after cancel")
	}
}

// With no annotation (e.g. deleted before PreBind wrote it), cancel falls back
// to the in-memory memo jobid.
func TestCancelGroupMemoFallback(t *testing.T) {
	m := &fakeMatcher{}
	f := newTestFluence(m)
	f.placement["default/solo"] = groupAlloc{jobids: []uint64{7}}

	f.cancelGroup("default/solo", nil)

	if len(m.cancelled) != 1 || m.cancelled[0] != 7 {
		t.Fatalf("cancelled = %v, want [7] (memo fallback)", m.cancelled)
	}
}

// A key we never scheduled (no annotation, no memo) is a no-op, not an error.
func TestCancelGroupUnknownNoop(t *testing.T) {
	m := &fakeMatcher{}
	f := newTestFluence(m)

	f.cancelGroup("default/ghost", nil)

	if len(m.cancelled) != 0 {
		t.Fatalf("cancelled = %v, want none for an unknown group", m.cancelled)
	}
}

// Cancel is idempotent: a redelivered delete event for an already-freed group
// does nothing the second time.
func TestCancelGroupIdempotent(t *testing.T) {
	m := &fakeMatcher{}
	f := newTestFluence(m)
	f.placement["default/solo"] = groupAlloc{jobids: []uint64{7}}

	f.cancelGroup("default/solo", nil) // frees, deletes memo
	f.cancelGroup("default/solo", nil) // memo gone, no annotation -> no-op

	if len(m.cancelled) != 1 {
		t.Fatalf("cancelled = %v, want exactly one cancel (idempotent)", m.cancelled)
	}
}

// A matcher Cancel error is logged but must not block cleanup of the memo.
func TestCancelGroupMatcherErrorStillDeletes(t *testing.T) {
	m := &fakeMatcher{cancelErr: errors.New("flux boom")}
	f := newTestFluence(m)
	f.placement["default/solo"] = groupAlloc{jobids: []uint64{7}}

	f.cancelGroup("default/solo", ann("7"))

	if len(m.cancelled) != 1 || m.cancelled[0] != 7 {
		t.Fatalf("cancelled = %v, want [7] even on error", m.cancelled)
	}
	if _, still := f.placement["default/solo"]; still {
		t.Fatal("placement entry should be deleted even when matcher.Cancel errors")
	}
}

// Deleting an ungrouped pod frees its own allocation.
func TestOnPodDeletedUngroupedCancels(t *testing.T) {
	m := &fakeMatcher{}
	f := newTestFluence(m)

	f.onPodDeleted(ungroupedPod("default", "solo", ann("5")))

	if len(m.cancelled) != 1 || m.cancelled[0] != 5 {
		t.Fatalf("cancelled = %v, want [5] for ungrouped pod delete", m.cancelled)
	}
}

// Deleting a grouped pod must NOT cancel: the allocation belongs to the
// PodGroup and is freed only when the PodGroup is deleted. Cancelling here would
// free the gang's allocation while other pods still hold it.
func TestOnPodDeletedGroupedDoesNotCancelDirectly(t *testing.T) {
	m := &fakeMatcher{}
	f := newTestFluence(m)

	// A grouped pod being deleted must NOT directly cancel the gang's allocation
	// (the allocation is owned by the PodGroup, freed only when the whole gang is
	// done). onPodDeleted now triggers reconcileGroup instead of ignoring the
	// pod, but reconcile frees nothing here: there is no scheduler handle in this
	// unit context, so reconcile is a safe no-op. The invariant under test —
	// "one grouped pod's deletion never frees the gang" — still holds.
	f.onPodDeleted(groupedPod("default", "training-abc", "training", ann("5")))

	if len(m.cancelled) != 0 {
		t.Fatalf("cancelled = %v, want none: a grouped pod delete must not free the gang", m.cancelled)
	}
}

// A pod tombstone (DeletedFinalStateUnknown) is unwrapped and handled.
func TestOnPodDeletedTombstone(t *testing.T) {
	m := &fakeMatcher{}
	f := newTestFluence(m)

	tomb := cache.DeletedFinalStateUnknown{Key: "default/solo", Obj: ungroupedPod("default", "solo", ann("8"))}
	f.onPodDeleted(tomb)

	if len(m.cancelled) != 1 || m.cancelled[0] != 8 {
		t.Fatalf("cancelled = %v, want [8] from tombstone", m.cancelled)
	}
}

// A garbage delete object is ignored without panic or cancel.
func TestOnPodDeletedGarbage(t *testing.T) {
	m := &fakeMatcher{}
	f := newTestFluence(m)

	f.onPodDeleted("not a pod")
	f.onPodDeleted(cache.DeletedFinalStateUnknown{Obj: "still not a pod"})

	if len(m.cancelled) != 0 {
		t.Fatalf("cancelled = %v, want none for garbage objects", m.cancelled)
	}
}

// Deleting a PodGroup frees the gang's allocation using the PodGroup annotation.
func TestOnPodGroupDeletedCancels(t *testing.T) {
	m := &fakeMatcher{}
	f := newTestFluence(m)

	pg := &schedv1a2.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "training", Annotations: ann("3")},
	}
	f.onPodGroupDeleted(pg)

	if len(m.cancelled) != 1 || m.cancelled[0] != 3 {
		t.Fatalf("cancelled = %v, want [3] for PodGroup delete", m.cancelled)
	}
}

// A PodGroup tombstone is unwrapped and handled.
func TestOnPodGroupDeletedTombstone(t *testing.T) {
	m := &fakeMatcher{}
	f := newTestFluence(m)

	pg := &schedv1a2.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "training", Annotations: ann("4")},
	}
	f.onPodGroupDeleted(cache.DeletedFinalStateUnknown{Key: "default/training", Obj: pg})

	if len(m.cancelled) != 1 || m.cancelled[0] != 4 {
		t.Fatalf("cancelled = %v, want [4] from PodGroup tombstone", m.cancelled)
	}
}

// matchGroup combines a compute allocation (nodes) and a device allocation
// (backend) into one placement and records both jobids.
func TestMatchGroupCombinesAllocations(t *testing.T) {
	m := &fakeMatcher{
		matchResults: []graph.MatchAllocateRequest{
			{Number: 10, Allocation: `{"graph":{"nodes":[{"metadata":{"type":"node","name":"node-a","properties":{"virtual=false":""}}}]}}`},
			{Number: 11, Allocation: `{"graph":{"nodes":[{"metadata":{"type":"node","name":"rigetti","properties":{"virtual=true":""}}}]}}`},
		},
	}
	f := newTestFluence(m)

	place, jobids, status := f.matchGroup(twoSpecs())
	if !status.IsSuccess() {
		t.Fatalf("status = %v, want success", status)
	}
	if len(jobids) != 2 || jobids[0] != 10 || jobids[1] != 11 {
		t.Fatalf("jobids = %v, want [10 11]", jobids)
	}
	// Both allocations contributed (exact node/backend split is
	// PlacementFromAllocation's job, refined in the placement rewrite).
	if len(place.Nodes) == 0 {
		t.Errorf("expected at least one bind node, got %v", place.Nodes)
	}
}

// If a later match fails, every already-successful allocation in the group is
// cancelled (all-or-nothing) and the group is Unschedulable.
func TestMatchGroupAllOrNothing(t *testing.T) {
	m := &fakeMatcher{
		matchResults: []graph.MatchAllocateRequest{
			{Number: 20, Allocation: `{"graph":{"nodes":[{"metadata":{"type":"node","name":"node-a","properties":{"virtual=false":""}}}]}}`},
			{},
		},
		matchErrs: []error{nil, errors.New("no qpu available")},
	}
	f := newTestFluence(m)

	_, _, status := f.matchGroup(twoSpecs())
	if status.IsSuccess() {
		t.Fatal("expected Unschedulable when the device match fails")
	}
	if len(m.cancelled) != 1 || m.cancelled[0] != 20 {
		t.Fatalf("cancelled = %v, want [20] (roll back the successful compute match)", m.cancelled)
	}
}

// A multi-jobid group records a comma-separated annotation and cancels every id.
func TestCancelGroupMultipleJobids(t *testing.T) {
	m := &fakeMatcher{}
	f := newTestFluence(m)

	f.cancelGroup("default/training", ann("10,11"))

	if len(m.cancelled) != 2 || m.cancelled[0] != 10 || m.cancelled[1] != 11 {
		t.Fatalf("cancelled = %v, want [10 11]", m.cancelled)
	}
}

// twoSpecs returns two minimal jobspecs (compute + device) to drive matchGroup
// tests; their content doesn't matter since the fake matcher scripts results.
func twoSpecs() []*jobspec.Jobspec {
	return []*jobspec.Jobspec{
		{Version: 9999},
		{Version: 9999},
	}
}

// --- PostFilter allocation reconciliation -----------------------------------

// fakeNodeStatus is a minimal fwk.NodeToStatusReader for PostFilter tests: it
// maps node name -> status code so a test can mark some nodes incompatible
// (UnschedulableAndUnresolvable) and others merely busy (Unschedulable).
type fakeNodeStatus map[string]fwk.Code

func (s fakeNodeStatus) Get(node string) *fwk.Status {
	if c, ok := s[node]; ok {
		return fwk.NewStatus(c)
	}
	return nil
}
func (s fakeNodeStatus) NodesForStatusCode(fwk.NodeInfoLister, fwk.Code) ([]fwk.NodeInfo, error) {
	return nil, nil
}

// PostFilter abandons the failed allocation (cancel jobids, drop cache) and
// excludes ONLY genuinely-incompatible nodes (UnschedulableAndUnresolvable).
// A node that was merely busy (plain Unschedulable) MUST stay eligible.
func TestPostFilterExcludesOnlyIncompatibleNodes(t *testing.T) {
	m := &fakeMatcher{}
	f := newTestFluence(m)
	key := "default/training"
	f.placement[key] = groupAlloc{
		place:  placement.Placement{Nodes: []string{"node-a", "node-b", "node-c"}},
		jobids: []uint64{11, 12},
	}
	pod := groupedPod("default", "training-0", "training", nil)

	// node-a incompatible (taint); node-b busy; node-c survived Filter.
	status := fakeNodeStatus{
		"node-a": fwk.UnschedulableAndUnresolvable,
		"node-b": fwk.Unschedulable,
		"node-c": fwk.Success,
	}

	_, st := f.PostFilter(context.Background(), nil, pod, status)
	if st == nil || st.Code() != fwk.Unschedulable {
		t.Fatalf("expected Unschedulable status, got %v", st)
	}
	if _, still := f.placement[key]; still {
		t.Fatal("placement cache should be deleted after PostFilter")
	}
	if len(m.cancelled) != 2 {
		t.Fatalf("expected both jobids cancelled, got %v", m.cancelled)
	}
	excl := f.excludedNodes[key]
	if !excl["node-a"] {
		t.Fatalf("incompatible node-a should be excluded, set=%v", excl)
	}
	if excl["node-b"] || excl["node-c"] {
		t.Fatalf("busy/ok nodes must NOT be excluded (would strand a saturated gang), set=%v", excl)
	}
	if len(excl) != 1 {
		t.Fatalf("expected exactly 1 excluded node, got %v", excl)
	}
}

// A group blocked purely by contention (every node merely busy) excludes NOTHING
// so it can retry the same nodes once they free — the saturated-cluster property.
func TestPostFilterContentionExcludesNothing(t *testing.T) {
	m := &fakeMatcher{}
	f := newTestFluence(m)
	key := "default/training"
	f.placement[key] = groupAlloc{
		place:  placement.Placement{Nodes: []string{"node-a", "node-b"}},
		jobids: []uint64{1},
	}
	pod := groupedPod("default", "training-0", "training", nil)
	status := fakeNodeStatus{"node-a": fwk.Unschedulable, "node-b": fwk.Unschedulable}

	f.PostFilter(context.Background(), nil, pod, status)

	if len(f.excludedNodes[key]) != 0 {
		t.Fatalf("a purely-busy group must exclude no nodes, got %v", f.excludedNodes[key])
	}
	if _, still := f.placement[key]; still {
		t.Fatal("placement cache should be deleted even when nothing is excluded")
	}
	if len(m.cancelled) != 1 {
		t.Fatalf("expected the jobid cancelled, got %v", m.cancelled)
	}
}

// A nil status map (e.g. all nodes filtered out upstream) must be safe and
// exclude nothing rather than panic or ban the whole allocation.
func TestPostFilterNilStatusMapExcludesNothing(t *testing.T) {
	m := &fakeMatcher{}
	f := newTestFluence(m)
	key := "default/training"
	f.placement[key] = groupAlloc{place: placement.Placement{Nodes: []string{"node-a", "node-b"}}, jobids: []uint64{7}}
	pod := groupedPod("default", "training-0", "training", nil)

	_, st := f.PostFilter(context.Background(), nil, pod, nil)
	if st == nil || st.Code() != fwk.Unschedulable {
		t.Fatalf("expected Unschedulable, got %v", st)
	}
	if len(f.excludedNodes[key]) != 0 {
		t.Fatalf("nil status map must exclude nothing, got %v", f.excludedNodes[key])
	}
}

// Incompatible nodes accumulate across attempts; busy ones never do.
func TestPostFilterAccumulatesIncompatibleAcrossAttempts(t *testing.T) {
	m := &fakeMatcher{}
	f := newTestFluence(m)
	key := "default/training"
	pod := groupedPod("default", "training-0", "training", nil)

	f.placement[key] = groupAlloc{place: placement.Placement{Nodes: []string{"node-a", "node-b"}}, jobids: []uint64{1}}
	f.PostFilter(context.Background(), nil, pod, fakeNodeStatus{"node-a": fwk.UnschedulableAndUnresolvable, "node-b": fwk.Unschedulable})
	f.placement[key] = groupAlloc{place: placement.Placement{Nodes: []string{"node-c", "node-d"}}, jobids: []uint64{2}}
	f.PostFilter(context.Background(), nil, pod, fakeNodeStatus{"node-c": fwk.UnschedulableAndUnresolvable, "node-d": fwk.Unschedulable})

	excl := f.excludedNodes[key]
	for _, n := range []string{"node-a", "node-c"} {
		if !excl[n] {
			t.Fatalf("incompatible %s should accumulate, got %v", n, excl)
		}
	}
	if excl["node-b"] || excl["node-d"] {
		t.Fatalf("busy nodes must never accumulate, got %v", excl)
	}
	if len(excl) != 2 {
		t.Fatalf("exclusion set should be the 2 incompatible nodes, got %v", excl)
	}
}

// PostFilter on a group with no cached allocation (not ours, or already cleared)
// is a safe no-op: no panic, no cancel, returns Unschedulable.
func TestPostFilterUnknownGroupNoop(t *testing.T) {
	m := &fakeMatcher{}
	f := newTestFluence(m)
	pod := groupedPod("default", "stranger-0", "stranger", nil)

	_, status := f.PostFilter(context.Background(), nil, pod, nil)
	if status == nil || status.Code() != fwk.Unschedulable {
		t.Fatalf("expected Unschedulable, got %v", status)
	}
	if len(m.cancelled) != 0 {
		t.Fatalf("nothing should be cancelled for an unknown group, got %v", m.cancelled)
	}
	if len(f.excludedNodes) != 0 {
		t.Fatalf("no exclusion set should be created for an unknown group, got %v", f.excludedNodes)
	}
}

// Teardown (cancelGroup) must clear the exclusion set so a future group reusing
// the same key does not inherit stale exclusions.
func TestCancelGroupClearsExclusions(t *testing.T) {
	m := &fakeMatcher{}
	f := newTestFluence(m)
	key := "default/training"
	f.placement[key] = groupAlloc{jobids: []uint64{9}}
	f.excludedNodes[key] = map[string]bool{"node-a": true}

	f.cancelGroup(key, ann("9"))

	if _, still := f.excludedNodes[key]; still {
		t.Fatal("exclusion set should be cleared on teardown")
	}
}

// schedulableNodes must drop control-plane (NoSchedule taint), NoExecute-tainted,
// and cordoned nodes, keeping only nodes a normal gang pod can actually land on.
// This keeps the Fluxion graph from offering nodes Kubernetes will reject in
// Filter (which, with whole-allocation PostFilter exclusion, strands the gang).
func TestSchedulableNodesDropsTaintedAndCordoned(t *testing.T) {
	node := func(name string, unsched bool, effects ...corev1.TaintEffect) corev1.Node {
		n := corev1.Node{}
		n.Name = name
		n.Spec.Unschedulable = unsched
		for _, e := range effects {
			n.Spec.Taints = append(n.Spec.Taints, corev1.Taint{Key: "k", Effect: e})
		}
		return n
	}
	in := []corev1.Node{
		node("worker-1", false),
		node("worker-2", false),
		node("control-plane", false, corev1.TaintEffectNoSchedule),
		node("draining", false, corev1.TaintEffectNoExecute),
		node("cordoned", true),
		node("prefer-only", false, corev1.TaintEffectPreferNoSchedule), // soft taint: keep
	}
	got := schedulableNodes(in)
	gotNames := map[string]bool{}
	for _, n := range got {
		gotNames[n.Name] = true
	}
	want := []string{"worker-1", "worker-2", "prefer-only"}
	if len(got) != len(want) {
		t.Fatalf("expected %d schedulable nodes %v, got %d %v", len(want), want, len(got), gotNames)
	}
	for _, w := range want {
		if !gotNames[w] {
			t.Fatalf("expected %s kept, got set %v", w, gotNames)
		}
	}
}
