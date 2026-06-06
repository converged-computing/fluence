package graph

import (
	"errors"
	"os"

	"github.com/converged-computing/fluence/pkg/quantum"
	"github.com/flux-framework/flux-sched/resource/reapi/bindings/go/src/fluxcli"

	"fmt"
)

/*
Desired steps:

1. instantiate Fluxion
2. Create the context, pass in the graphml and specify instead of JFG we are using graphML
3. Then the defaults will work out of box
4. Then pass in a jobspec
5. Perform a satisfiability check rather than a match "can we represent this or not" There exist one or more matches for the schema in the resource graph.
6. Would we need/want a match? Is it possible to validate the schema without performing a match.

*/

type FluxionGraph struct {
	cli *fluxcli.ReapiClient

	// MatchFormat selects the Fluxion allocation output format ("simple",
	// "jgf", "rv1", ...). Empty defaults to "simple" (human-readable tree).
	// Set it to "jgf" when you need to parse the allocation programmatically
	// (e.g. quantum.BackendFromAllocation).
	MatchFormat string
}

// Init a new FlexGraph from a graphml filename
func (f *FluxionGraph) Init(confFile string, matchPolicy string, label string) {

	// 1. instantiate fluxion
	f.cli = fluxcli.NewReapiClient()
	fmt.Println("Created fluxion resource graph")

	// 2. Load in the resource graph
	conf, err := os.ReadFile(confFile)
	if err != nil {
		fmt.Println("Error reading JGF v1 file")
		return
	}

	// Set match policy to default (first) if not defined.
	if matchPolicy == "" {
		matchPolicy = "first"
	}

	// Allocation output format; default to the human-readable tree.
	matchFormat := f.MatchFormat
	if matchFormat == "" {
		matchFormat = "simple"
	}

	// Alert the user to all the chosen parameters
	fmt.Printf("  Match policy: %s\n", matchPolicy)
	fmt.Println("  Load format: JGF (jgf)")
	fmt.Printf("  Match format: %s\n", matchFormat)
	fmt.Printf("  Config file: %s\n", confFile)

	// 2. Create the context, specify instead of JGF (default) we want graphml
	// 3. Remainder of defaults should work out of the box
	// Note that the options get passed as a json string to here:
	// https://github.com/flux-framework/flux-sched/blob/master/resource/reapi/bindings/c%2B%2B/reapi_cli_impl.hpp#L412
	opts := `{"matcher_policy": "%s", "load_file": "%s", "load_format": "jgf", "match_format": "%s"}`
	p := fmt.Sprintf(opts, matchPolicy, confFile, matchFormat)

	// 4. Then pass in a jobspec... err, ice cream request :)
	err = f.cli.InitContext(string(conf), p)
	if err != nil {
		fmt.Printf("Error creating context: %s", err)
	}
	fmt.Printf("\n✨️ Init context complete!\n")
}

// MatchAllocate reads a jobspec file (YAML or JSON) and match-allocates it.
func (f *FluxionGraph) MatchAllocate(specFile string) (quantum.MatchAllocateRequest, error) {
	spec, err := os.ReadFile(specFile)
	if err != nil {
		return quantum.MatchAllocateRequest{}, errors.New("Error reading jobspec")
	}
	fmt.Printf("   🌀 Request (file): %s\n", specFile)
	return f.MatchAllocateSpec(string(spec))
}

// MatchAllocateSpec match-allocates against a jobspec provided as a string.
// Fluxion accepts YAML or JSON; use the jobspec package to convert/normalize if
// needed before calling this.
func (f *FluxionGraph) MatchAllocateSpec(spec string) (quantum.MatchAllocateRequest, error) {
	request := quantum.MatchAllocateRequest{}

	reserved, allocated, time_at, overhead, jobid, err := f.cli.MatchAllocate(false, spec)
	if err != nil {
		return request, err
	}

	fmt.Printf("  JobID    : %d\n", jobid)
	fmt.Printf("  Reserved : %t\n", reserved)
	fmt.Printf("  Overhead : %.6f seconds\n", overhead)
	fmt.Printf("  Time at  : %d\n", time_at)
	fmt.Printf("  Allocated :\n%s\n", allocated)
	request.Allocation = allocated
	request.Number = jobid
	return request, nil
}

// Satisfy determines if we can satisfy
func (f *FluxionGraph) Satisfy(specFile string) (bool, error) {
	fmt.Printf("   🌀 Request: %s\n", specFile)

	spec, err := os.ReadFile(specFile)
	if err != nil {
		return false, errors.New("Error reading jobspec")
	}

	satisfied, overhead, err := f.cli.MatchSatisfy(string(spec))
	fmt.Printf("  Satisfied : %t\n", satisfied)
	fmt.Printf("  Overhead : %.6f seconds\n", overhead)

	if err != nil {
		return false, err
	}
	return satisfied, nil
}
