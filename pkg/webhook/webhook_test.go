package webhook

import (
	"testing"
)

// EnvVarNames returns the FLUXION_* contract names (used by the scheduler plugin
// to recognize/strip injected env). Behavioral handler tests live in the
// handlers subpackage.
func TestEnvVarNames(t *testing.T) {
	m := &Mutator{AttributeKeys: []string{"region", "qrmi_type"}}
	names := m.EnvVarNames()
	want := map[string]bool{"FLUXION_BACKEND": true, "FLUXION_REGION": true, "FLUXION_QRMI_TYPE": true}
	if len(names) != len(want) {
		t.Fatalf("want %d env names, got %v", len(want), names)
	}
	for _, n := range names {
		if !want[n] {
			t.Errorf("unexpected env name %q", n)
		}
	}
}
