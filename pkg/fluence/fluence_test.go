package fluence

import (
	"errors"
	"testing"

	"github.com/converged-computing/fluence/pkg/graph"
	"github.com/converged-computing/fluence/pkg/placement"

	corev1 "k8s.io/api/core/v1"
	schedv1a2 "k8s.io/api/scheduling/v1alpha2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

// fakeMatcher records Cancel calls so cancel behavior can be asserted without
// the real cgo/flux matcher. It satisfies the package-internal matcher interface.
type fakeMatcher struct {
	cancelled []uint64
	cancelErr error
}

func (m *fakeMatcher) MatchAllocateSpec(string) (graph.MatchAllocateRequest, error) {
	return graph.MatchAllocateRequest{}, nil
}

func (m *fakeMatcher) Cancel(jobid uint64) error {
	m.cancelled = append(m.cancelled, jobid)
	return m.cancelErr
}

func newTestFluence(m matcher) *Fluence {
	return &Fluence{matcher: m, placement: map[string]groupAlloc{}}
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

func TestParseJobID(t *testing.T) {
	cases := []struct {
		name   string
		ann    map[string]string
		want   uint64
		wantOK bool
	}{
		{"present", ann("42"), 42, true},
		{"absent", map[string]string{}, 0, false},
		{"nil map", nil, 0, false},
		{"empty value", ann(""), 0, false},
		{"garbage", ann("not-a-number"), 0, false},
		{"zero", ann("0"), 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := parseJobID(c.ann)
			if got != c.want || ok != c.wantOK {
				t.Fatalf("parseJobID(%v) = (%d,%t), want (%d,%t)", c.ann, got, ok, c.want, c.wantOK)
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
	f.placement["default/training"] = groupAlloc{jobid: 42}

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
	f.placement["default/solo"] = groupAlloc{jobid: 7}

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
	f.placement["default/solo"] = groupAlloc{jobid: 7}

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
	f.placement["default/solo"] = groupAlloc{jobid: 7}

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
func TestOnPodDeletedGroupedIgnored(t *testing.T) {
	m := &fakeMatcher{}
	f := newTestFluence(m)

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
