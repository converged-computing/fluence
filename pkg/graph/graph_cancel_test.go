//go:build cgo

// Package graph cancel test. This exercises the REAL Fluxion matcher (cgo +
// flux-sched), so it only builds/runs where flux-sched is linkable — i.e. in CI
// inside fluence-base, or the devcontainer. It verifies cancel's actual effect:
// a cancelled jobid frees its allocation so the resource can be matched again.
package graph

import (
	"os"
	"strings"
	"testing"
)

// A tiny graph with a single exclusive core, so the first match consumes it and
// a second match must fail — until we cancel the first.
const tinyGraph = `{
  "graph": {
    "nodes": [
      {"id": "0", "metadata": {"type": "cluster", "basename": "cluster", "name": "cluster0", "id": 0, "uniq_id": 0, "rank": -1, "size": 1, "exclusive": false, "unit": "", "paths": {"containment": "/cluster0"}}},
      {"id": "1", "metadata": {"type": "node", "basename": "node", "name": "node0", "id": 0, "uniq_id": 1, "rank": 0, "size": 1, "exclusive": false, "unit": "", "paths": {"containment": "/cluster0/node0"}}},
      {"id": "2", "metadata": {"type": "core", "basename": "core", "name": "core0", "id": 0, "uniq_id": 2, "rank": 0, "size": 1, "exclusive": false, "unit": "", "paths": {"containment": "/cluster0/node0/core0"}}}
    ],
    "edges": [
      {"source": "0", "target": "1", "metadata": {"subsystem": "containment"}},
      {"source": "1", "target": "2", "metadata": {"subsystem": "containment"}}
    ]
  }
}`

const coreJobspec = `version: 9999
resources:
  - type: slot
    count: 1
    label: default
    with:
      - type: core
        count: 1
tasks:
  - command: ["app"]
    slot: default
    count:
      per_slot: 1
attributes:
  system:
    duration: 3600
`

// newGraph stages the tiny graph to a temp file and initializes a real matcher.
func newGraph(t *testing.T) *FluxionGraph {
	t.Helper()
	tmp, err := os.CreateTemp(t.TempDir(), "graph-*.json")
	if err != nil {
		t.Fatalf("temp file: %v", err)
	}
	if _, err := tmp.WriteString(tinyGraph); err != nil {
		t.Fatalf("write graph: %v", err)
	}
	_ = tmp.Close()

	g := &FluxionGraph{MatchFormat: "rv1"}
	g.Init(tmp.Name(), "first", "")
	return g
}

// TestCancelFreesAllocation is the real "cancel of the jobid" check: match the
// only core (consuming it), confirm a second match is refused, cancel the first
// jobid, then confirm the core can be matched again. This proves Cancel actually
// releases the allocation in the Fluxion graph, not just that it returns nil.
func TestCancelFreesAllocation(t *testing.T) {
	g := newGraph(t)

	// 1. First match consumes the single core.
	req1, err := g.MatchAllocateSpec(coreJobspec)
	if err != nil {
		t.Fatalf("first match failed: %v", err)
	}
	if req1.Number == 0 {
		t.Fatalf("expected a nonzero jobid from first match")
	}

	// 2. Second match must be refused — the core is taken.
	if _, err := g.MatchAllocateSpec(coreJobspec); err == nil {
		t.Fatal("second match should fail while the core is allocated, but it succeeded")
	}

	// 3. Cancel the first jobid.
	if err := g.Cancel(req1.Number); err != nil {
		t.Fatalf("cancel jobid %d failed: %v", req1.Number, err)
	}

	// 4. The core is free again: a fresh match must now succeed.
	req2, err := g.MatchAllocateSpec(coreJobspec)
	if err != nil {
		t.Fatalf("match after cancel failed (cancel did not free the allocation): %v", err)
	}
	if !strings.Contains(req2.Allocation, "core0") {
		t.Fatalf("post-cancel allocation does not contain the freed core: %s", req2.Allocation)
	}
}

// TestCancelUnknownJobIDIsHarmless confirms cancelling a jobid that was never
// allocated does not error (the binding is called with noent_ok), so a
// redelivered/duplicate delete event can't wedge the scheduler.
func TestCancelUnknownJobIDIsHarmless(t *testing.T) {
	g := newGraph(t)
	if err := g.Cancel(999999); err != nil {
		t.Fatalf("cancel of unknown jobid should be a no-op, got: %v", err)
	}
}
