// Command recovery-probe verifies the restart-recovery primitive against a real
// flux-sched. It match-allocates a jobspec in one graph (capturing the rv1 R +
// jobid), then builds a SECOND, fresh graph from the same cluster file — exactly
// what a scheduler restart does, a graph with no allocation history — and
// replays R verbatim with UpdateAllocate under the same jobid. Finally it
// re-matches against the fresh graph; for an exclusive, singular resource a
// refusal proves the replay re-held it.
//
// Run in the devcontainer (links flux-sched via cgo). Throwaway diagnostic.
//
//	go run ./cmd/recovery-probe -graph <cluster.jgf> -spec <jobspec.yaml>
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/converged-computing/fluence/pkg/graph"
)

func newGraph(graphFile string) *graph.FluxionGraph {
	g := &graph.FluxionGraph{MatchFormat: "rv1"}
	g.Init(graphFile, os.Getenv("FLUENCE_MATCH_POLICY"), "")
	return g
}

func main() {
	graphFile := flag.String("graph", "", "cluster resource graph (JGF)")
	specFile := flag.String("spec", "", "jobspec to allocate (YAML or JSON)")
	flag.Parse()
	if *graphFile == "" || *specFile == "" {
		fmt.Fprintln(os.Stderr, "usage: recovery-probe -graph <jgf> -spec <jobspec>")
		os.Exit(2)
	}

	// --- Graph instance #1: the "pre-restart" scheduler. ---
	fmt.Println("\n=== 1. instance #1: MatchAllocate (capture rv1 R + jobid) ===")
	pre := newGraph(*graphFile)
	req, err := pre.MatchAllocate(*specFile)
	if err != nil {
		fmt.Println("initial match failed (need a satisfiable spec):", err)
		os.Exit(1)
	}
	jobid, R := req.Number, req.Allocation
	fmt.Printf("captured jobid=%d, R=%d bytes (rv1)\n", jobid, len(R))

	// --- Graph instance #2: the "post-restart" scheduler. Fresh context, fresh
	// graph built from the same cluster file, NO allocation history. We never
	// cancel jobid in instance #1; instance #2 simply never knew about it. ---
	fmt.Println("\n=== 2. instance #2: fresh graph (simulate restart) ===")
	post := newGraph(*graphFile)

	// --- Replay the exact R under the same jobid into the fresh graph. ---
	fmt.Println("\n=== 3. instance #2: UpdateAllocate (replay R) ===")
	if err := post.UpdateAllocate(jobid, R); err != nil {
		fmt.Println("UpdateAllocate failed:", err)
		fmt.Println(">>> If this still fails on a FRESH graph, update_allocate likely")
		fmt.Println(">>> requires the jobid to already exist (an update, not a create).")
		fmt.Println(">>> Fallback: MatchAllocate an equivalent spec to mint a jobid, then")
		fmt.Println(">>> UpdateAllocate that jobid to the persisted R (the real assignment).")
		os.Exit(1)
	}

	// --- Prove the replay consumed the resource: re-match the same spec against
	// the fresh graph. For an exclusive, singular resource this must be refused. ---
	fmt.Println("\n=== 4. instance #2: re-MatchAllocate (refusal = replay held it) ===")
	req2, err := post.MatchAllocate(*specFile)
	if err != nil {
		fmt.Println("PASS: second match refused — the replayed allocation is holding the resource.")
		return
	}
	fmt.Printf("second match SUCCEEDED (jobid=%d) — interpret with your graph in mind:\n", req2.Number)
	fmt.Println("  - spare capacity for this spec exists -> expected, not a failure;")
	fmt.Println("  - resource is exclusive & singular -> replay did NOT consume it, investigate.")
}
