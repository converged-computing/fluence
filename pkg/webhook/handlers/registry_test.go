/*
Copyright 2024 Lawrence Livermore National Security, LLC
 (c.f. AUTHORS, NOTICE.LLNS, COPYING)
SPDX-License-Identifier: Apache-2.0
*/

// Registry behavior: dispatch order comes from the active handler list (not a
// per-handler Order), and the list both selects and orders handlers.
package handlers

import (
	"context"
	"testing"

	"github.com/converged-computing/fluence/pkg/webhook"
	"github.com/converged-computing/fluence/pkg/webhook/spec"

	"k8s.io/client-go/kubernetes/fake"
)

// The default active order ships gang LAST so it only applies default gang
// sizing when no earlier handler shaped the gang.
func TestDefaultOrderGangLast(t *testing.T) {
	defer webhook.SetActiveHandlers(nil)
	active, _ := webhook.SetActiveHandlers(nil) // restore + read default
	if len(active) == 0 {
		t.Fatal("no active handlers")
	}
	if active[len(active)-1] != "gang" {
		t.Errorf("gang must be last in default order; got %v", active)
	}
	// default order is exactly fluxion, quantum, gang
	want := []string{"fluxion", "quantum", "gang"}
	if len(active) != len(want) {
		t.Fatalf("default order = %v, want %v", active, want)
	}
	for i := range want {
		if active[i] != want[i] {
			t.Errorf("default order = %v, want %v", active, want)
			break
		}
	}
}

// The active list IS the order: passing a custom order reorders dispatch, and
// unknown names are reported, not silently kept.
func TestActiveListSetsOrderAndReportsUnknown(t *testing.T) {
	defer webhook.SetActiveHandlers(nil)
	active, unknown := webhook.SetActiveHandlers([]string{"gang", "fluxion", "bogus"})
	if len(active) != 2 || active[0] != "gang" || active[1] != "fluxion" {
		t.Errorf("active = %v, want [gang fluxion] in that order", active)
	}
	if len(unknown) != 1 || unknown[0] != "bogus" {
		t.Errorf("unknown = %v, want [bogus]", unknown)
	}
}

// Dropping a handler from the list disables it: a quantum pod with quantum
// omitted gets no interceptor ops (only fluxion/gang act).
func TestOmittedHandlerDoesNotDispatch(t *testing.T) {
	defer webhook.SetActiveHandlers(nil)
	m := &webhook.Mutator{Clientset: fake.NewSimpleClientset()}

	webhook.SetActiveHandlers(nil) // default: quantum present
	if !hasInterceptor(m.Mutate(context.Background(), qpuPod("fluence"))) {
		t.Fatal("with quantum active, expected interceptor (init container) ops")
	}

	webhook.SetActiveHandlers([]string{"fluxion", "gang"}) // quantum omitted
	if hasInterceptor(m.Mutate(context.Background(), qpuPod("fluence"))) {
		t.Error("with quantum omitted, interceptor ops must NOT be present")
	}
}

func hasInterceptor(ops []spec.Op) bool {
	for _, op := range ops {
		if op.Path == "/spec/initContainers" || op.Path == "/spec/initContainers/-" {
			return true
		}
	}
	return false
}
