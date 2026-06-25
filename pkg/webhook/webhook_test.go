package webhook

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
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

func TestSidecarInheritsWorkloadSecretEnv(t *testing.T) {
	m := &Mutator{}
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: "gang",
				Env: []corev1.EnvVar{
					{Name: "GANG_ROLE", Value: "leader"}, // plain value: NOT copied
					{Name: "AWS_ACCESS_KEY_ID", ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "aws-braket-credentials"},
							Key:                  "AWS_ACCESS_KEY_ID",
						}}},
				},
			}},
		},
	}
	ops := m.SidecarContainerOps(pod, false, nil)
	var sidecar *corev1.Container
	for _, op := range ops {
		if c, ok := op.Value.(corev1.Container); ok && c.Name == "fluence-sidecar" {
			sidecar = &c
		}
	}
	if sidecar == nil {
		t.Fatal("no sidecar container added")
	}
	var gotSecret, gotPlain bool
	for _, e := range sidecar.Env {
		if e.Name == "AWS_ACCESS_KEY_ID" && e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil {
			gotSecret = true
		}
		if e.Name == "GANG_ROLE" {
			gotPlain = true
		}
	}
	if !gotSecret {
		t.Error("sidecar should inherit the workload's secret-sourced AWS creds")
	}
	if gotPlain {
		t.Error("sidecar should NOT copy plain-value workload env like GANG_ROLE")
	}
}
