// Tests for the handoff volume a submitting pod gets, and the session env a
// worker gets. Fixtures come from handlers_test.go and quantum_test.go.
package handlers

import (
	"context"
	"strings"
	"testing"

	"github.com/converged-computing/fluence/pkg/webhook"
	"github.com/converged-computing/fluence/pkg/webhook/spec"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func opsHaveVolume(ops []spec.Op, name string) bool {
	for _, op := range ops {
		switch v := op.Value.(type) {
		case corev1.Volume:
			if v.Name == name {
				return true
			}
		case []corev1.Volume:
			for _, vol := range v {
				if vol.Name == name {
					return true
				}
			}
		}
	}
	return false
}

// opsMountHandoffWritable reports a mount of the handoff volume with ReadOnly false
func opsMountHandoffWritable(ops []spec.Op) bool {
	for _, op := range ops {
		switch v := op.Value.(type) {
		case corev1.VolumeMount:
			if v.Name == HandoffVolumeName && !v.ReadOnly {
				return true
			}
		case []corev1.VolumeMount:
			for _, m := range v {
				if m.Name == HandoffVolumeName && !m.ReadOnly {
					return true
				}
			}
		}
	}
	return false
}

// sidecarMountsHandoffReadOnly finds the sidecar container in the ops and checks its mount
func sidecarMountsHandoffReadOnly(ops []spec.Op) bool {
	check := func(c corev1.Container) bool {
		if c.Name != SidecarContainerName {
			return false
		}
		for _, m := range c.VolumeMounts {
			if m.Name == HandoffVolumeName && m.ReadOnly {
				return true
			}
		}
		return false
	}
	for _, op := range ops {
		switch v := op.Value.(type) {
		case corev1.Container:
			if check(v) {
				return true
			}
		case []corev1.Container:
			for _, c := range v {
				if check(c) {
					return true
				}
			}
		}
	}
	return false
}

func handoffJobMutator(ns, job string, n int32) *webhook.Mutator {
	cs := fake.NewSimpleClientset(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: job, Namespace: ns},
		Spec:       batchv1.JobSpec{Parallelism: &n, Completions: &n},
	})
	return &webhook.Mutator{Clientset: cs}
}

func TestStandalonePodGetsWritableHandoff(t *testing.T) {
	m := &webhook.Mutator{}
	ops := m.Mutate(context.Background(), qpuPod("fluence"))
	if !opsHaveVolume(ops, HandoffVolumeName) {
		t.Fatalf("a submitting pod should get the %q volume", HandoffVolumeName)
	}
	if !opsMountHandoffWritable(ops) {
		t.Error("the workload container should mount the handoff volume writable")
	}
	if !contains(opEnvNames(ops), HandoffDirEnv) {
		t.Errorf("want %s on a submitting pod, got %v", HandoffDirEnv, opEnvNames(ops))
	}
}

func TestStageVolumeSurvivesTheHandoffAdd(t *testing.T) {
	// both volumes are added to a pod that had none, so the second must append
	m := &webhook.Mutator{}
	ops := m.Mutate(context.Background(), qpuPod("fluence"))
	if !opsHaveVolume(ops, StageVolumeName) || !opsHaveVolume(ops, HandoffVolumeName) {
		t.Fatal("both the stage and handoff volumes should be present")
	}
	for _, op := range ops {
		if op.Path == "/spec/volumes" {
			if vols, ok := op.Value.([]corev1.Volume); ok && len(vols) == 1 && vols[0].Name == HandoffVolumeName {
				t.Error("the handoff volume replaced the whole volumes list instead of appending")
			}
		}
	}
}

func TestWorkerGetsNoHandoff(t *testing.T) {
	ns, group, job := "default", "qgang", "qgang-job"
	m := handoffJobMutator(ns, job, 2)
	ops := m.Mutate(context.Background(), sharedQPUPod(ns, group, "qgang-1", job, "1"))
	if opsHaveVolume(ops, HandoffVolumeName) {
		t.Error("a worker should not get the handoff volume")
	}
	if contains(opEnvNames(ops), HandoffDirEnv) {
		t.Error("a worker should not get FLUENCE_HANDOFF_DIR")
	}
}

func TestScoutSidecarMountsHandoffReadOnly(t *testing.T) {
	ns, group, job := "default", "qgang2", "qgang2-job"
	m := handoffJobMutator(ns, job, 2)
	ops := m.Mutate(context.Background(), sharedQPUPod(ns, group, "qgang2-0", job, "0"))
	if !opsHaveVolume(ops, HandoffVolumeName) {
		t.Error("the scout should get the handoff volume")
	}
	if !opsMountHandoffWritable(ops) {
		t.Error("the scout workload should mount the handoff volume writable")
	}
	if !sidecarMountsHandoffReadOnly(ops) {
		t.Error("the scout sidecar should mount the handoff volume read only")
	}
	if !contains(opEnvNames(ops), HandoffDirEnv) {
		t.Errorf("the scout should get %s, got %v", HandoffDirEnv, opEnvNames(ops))
	}
}

func TestHandoffMountPathIsUnderOpt(t *testing.T) {
	if !strings.HasPrefix(HandoffMountPath, "/opt/") {
		t.Errorf("unexpected handoff mount path %q", HandoffMountPath)
	}
}

func TestWorkerGetsSessionEnvFromAnnotation(t *testing.T) {
	ns, group, job := "default", "qgang3", "qgang3-job"
	m := handoffJobMutator(ns, job, 2)
	ops := m.Mutate(context.Background(), sharedQPUPod(ns, group, "qgang3-1", job, "1"))
	e, ok := envOp(ops, QuantumSessionIDEnv)
	if !ok {
		t.Fatalf("worker missing %s", QuantumSessionIDEnv)
	}
	if e.ValueFrom == nil || e.ValueFrom.FieldRef == nil || !strings.Contains(e.ValueFrom.FieldRef.FieldPath, QuantumSessionIDAnnotation) {
		t.Errorf("%s should read the %s annotation, got %+v", QuantumSessionIDEnv, QuantumSessionIDAnnotation, e)
	}
}

func TestScoutGetsNoSessionEnv(t *testing.T) {
	ns, group, job := "default", "qgang4", "qgang4-job"
	m := handoffJobMutator(ns, job, 2)
	ops := m.Mutate(context.Background(), sharedQPUPod(ns, group, "qgang4-0", job, "0"))
	if _, ok := envOp(ops, QuantumSessionIDEnv); ok {
		t.Error("the scout acquires the session itself and must not get the worker env")
	}
}
