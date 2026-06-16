package graph

import (
	"testing"
)

func TestBackendFromAllocation(t *testing.T) {
	// Shape mirrors a Fluxion allocation graph: a qpu vertex named after the
	// QRMI backend, sitting under a gateway under the cluster.
	alloc := `{"graph":{"nodes":[
	  {"metadata":{"type":"cluster","name":"tiny0"}},
	  {"metadata":{"type":"qgateway","name":"qgateway0"}},
	  {"metadata":{"type":"qpu","name":"ibm_fez"}}
	]}}`

	got, err := BackendFromAllocation(alloc, "qpu")
	if err != nil {
		t.Fatalf("BackendFromAllocation: %v", err)
	}
	if got != "ibm_fez" {
		t.Fatalf("backend = %q, want ibm_fez", got)
	}

	if _, err := BackendFromAllocation(alloc, "qfridge"); err == nil {
		t.Fatal("expected an error when the vertex type is absent")
	}
	if _, err := BackendFromAllocation("not json", "qpu"); err == nil {
		t.Fatal("expected an error on malformed allocation")
	}
}
