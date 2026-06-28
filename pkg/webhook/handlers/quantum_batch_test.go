package handlers

import (
	"context"
	"testing"

	"github.com/converged-computing/fluence/pkg/webhook"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// batchQPUPod is a quantum workload pod owned by a Job of parallelism N, with
// coordination=batch and a completion index. Same two roles as shared: index 0
// is the producer, any other index is a consumer -- batch changes only the
// submit fan-out and ungate strategy (carried by FLUENCE_COORDINATION_MODE).
func batchQPUPod(ns, group, name, job, index string) *corev1.Pod {
	p := qpuPod("fluence")
	p.Name = name
	p.Namespace = ns
	p.Labels = map[string]string{webhook.GroupLabel: group}
	p.Annotations = map[string]string{
		CoordinationAnnotation:    CoordinationBatch,
		CompletionIndexAnnotation: index,
	}
	p.OwnerReferences = []metav1.OwnerReference{{Kind: "Job", Name: job}}
	return p
}

func batchJobClientset(ns, job string, n int32) *fake.Clientset {
	return fake.NewSimpleClientset(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: job, Namespace: ns},
		Spec:       batchv1.JobSpec{Parallelism: &n, Completions: &n},
	})
}

// The batch PRODUCER (index 0) is wired exactly like the shared producer -- own
// group-of-one (minCount 1), real sidecar, never gated, keeps its qpu, role=producer
// -- and additionally carries mode=batch and the gang size N so its workload
// submits N tasks. No new role, no extra pod.
func TestBatchProducerIsProducerWithMode(t *testing.T) {
	ns, group, job := "default", "bg", "bg-job"
	cs := batchJobClientset(ns, job, 4)
	m := &webhook.Mutator{Clientset: cs}

	ops := m.Mutate(context.Background(), batchQPUPod(ns, group, "bg-0", job, "0"))

	if !hasSidecarOp(ops) {
		t.Error("batch producer must get the real sidecar")
	}
	if hasGateOp(ops) {
		t.Error("batch producer must NOT be gated")
	}
	if hasDropQuantumResourceOp(ops) {
		t.Error("batch producer must KEEP its qpu resource")
	}
	if e, ok := envOp(ops, CoordinationRoleEnv); !ok || e.Value != RoleProducer {
		t.Errorf("must reuse role=%s (no broker role), got %q (ok=%v)", RoleProducer, e.Value, ok)
	}
	if e, ok := envOp(ops, CoordinationModeEnv); !ok || e.Value != CoordinationBatch {
		t.Errorf("producer must carry %s=%s", CoordinationModeEnv, CoordinationBatch)
	}
	if e, ok := envOp(ops, GangSizeEnv); !ok || e.Value != "4" {
		t.Errorf("producer must be told the gang size %s=4, got %q (ok=%v)", GangSizeEnv, e.Value, ok)
	}
	if mc, ok := mincount(t, cs, ns, group+ProducerGroupSuffix); !ok || mc != 1 {
		t.Errorf("producer group-of-one minCount=%d (ok=%v), want 1", mc, ok)
	}
}

// A batch CONSUMER (index != 0) is wired exactly like the shared consumer --
// gated, role=consumer, joins the gang at minCount N-1, qpu stripped -- and
// additionally carries mode=batch so the sidecar releases it by its OWN result.
func TestBatchConsumerIsConsumerWithMode(t *testing.T) {
	ns, group, job := "default", "bg", "bg-job"
	cs := batchJobClientset(ns, job, 4)
	m := &webhook.Mutator{Clientset: cs}

	ops := m.Mutate(context.Background(), batchQPUPod(ns, group, "bg-2", job, "2"))

	if !hasGateOp(ops) {
		t.Error("batch consumer must be gated")
	}
	if hasSidecarOp(ops) {
		t.Error("batch consumer (gated) must NOT get a sidecar")
	}
	if !hasDropQuantumResourceOp(ops) {
		t.Error("batch consumer must have its qpu stripped")
	}
	if e, ok := envOp(ops, CoordinationRoleEnv); !ok || e.Value != RoleConsumer {
		t.Errorf("must reuse role=%s (no worker role), got %q (ok=%v)", RoleConsumer, e.Value, ok)
	}
	if e, ok := envOp(ops, CoordinationModeEnv); !ok || e.Value != CoordinationBatch {
		t.Errorf("consumer must carry %s=%s", CoordinationModeEnv, CoordinationBatch)
	}
	if mc, ok := mincount(t, cs, ns, group); !ok || mc != 3 {
		t.Errorf("consumer gang minCount=%d (ok=%v), want 3 (N-1 split)", mc, ok)
	}
}

// shared mode still stamps role=producer and mode=shared (no regression from the
// mode unification).
func TestSharedStillStampsModeShared(t *testing.T) {
	ns, group, job := "default", "sg", "sg-job"
	cs := batchJobClientset(ns, job, 3)
	m := &webhook.Mutator{Clientset: cs}
	p := batchQPUPod(ns, group, "sg-0", job, "0")
	p.Annotations[CoordinationAnnotation] = CoordinationShared

	ops := m.Mutate(context.Background(), p)
	if e, ok := envOp(ops, CoordinationRoleEnv); !ok || e.Value != RoleProducer {
		t.Errorf("shared producer role=%s", RoleProducer)
	}
	if e, ok := envOp(ops, CoordinationModeEnv); !ok || e.Value != CoordinationShared {
		t.Errorf("shared producer must carry %s=%s", CoordinationModeEnv, CoordinationShared)
	}
}
