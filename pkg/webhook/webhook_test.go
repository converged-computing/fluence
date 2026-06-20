package webhook

import (
	"context"
	"testing"

	"github.com/converged-computing/fluence/pkg/placement"
	corev1 "k8s.io/api/core/v1"
	schedulingv1alpha2 "k8s.io/api/scheduling/v1alpha2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func qpuPod(scheduler, presetEnv string) *corev1.Pod {
	c := corev1.Container{
		Name: "app",
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				placement.FluxionResourcePrefix + "qpu": *resource.NewQuantity(1, resource.DecimalSI),
			},
		},
	}
	if presetEnv != "" {
		c.Env = []corev1.EnvVar{{Name: presetEnv, Value: "preset"}}
	}
	return &corev1.Pod{Spec: corev1.PodSpec{SchedulerName: scheduler, Containers: []corev1.Container{c}}}
}

func cpuPod(scheduler string) *corev1.Pod {
	return &corev1.Pod{Spec: corev1.PodSpec{
		SchedulerName: scheduler,
		Containers: []corev1.Container{{
			Name: "c",
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU: *resource.NewQuantity(1, resource.DecimalSI),
				},
			},
		}},
	}}
}

func opEnvNames(ops []jsonPatchOp) []string {
	var names []string
	for _, op := range ops {
		switch v := op.Value.(type) {
		case corev1.EnvVar:
			names = append(names, v.Name)
		case []corev1.EnvVar:
			for _, e := range v {
				names = append(names, e.Name)
			}
		}
	}
	return names
}

func contains(names []string, want string) bool {
	for _, n := range names {
		if n == want {
			return true
		}
	}
	return false
}

func hasGateOp(ops []jsonPatchOp) bool {
	for _, op := range ops {
		switch v := op.Value.(type) {
		case corev1.PodSchedulingGate:
			if v.Name == QuantumGateName {
				return true
			}
		case []corev1.PodSchedulingGate:
			for _, g := range v {
				if g.Name == QuantumGateName {
					return true
				}
			}
		}
	}
	return false
}

func hasSidecarOp(ops []jsonPatchOp) bool {
	for _, op := range ops {
		switch v := op.Value.(type) {
		case corev1.Container:
			if v.Name == "fluence-sidecar" {
				return true
			}
		case []corev1.Container:
			for _, c := range v {
				if c.Name == "fluence-sidecar" {
					return true
				}
			}
		}
	}
	return false
}

func TestMutateInjectsContract(t *testing.T) {
	m := &Mutator{AttributeKeys: []string{"region", "qubits"}}
	ops := m.Mutate(context.Background(), qpuPod("fluence", ""))
	names := opEnvNames(ops)
	for _, want := range []string{"FLUXION_BACKEND", "FLUXION_REGION", "FLUXION_QUBITS"} {
		if !contains(names, want) {
			t.Errorf("missing injected env %q; got %v", want, names)
		}
	}
}

func TestMutateBackendOnly(t *testing.T) {
	m := &Mutator{}
	names := opEnvNames(m.Mutate(context.Background(), qpuPod("fluence", "")))
	// A quantum pod gets the FLUXION_BACKEND env contract AND the interceptor
	// (FLUENCE_POD_UID + PYTHONSTARTUP) so its submitted task is tagged for
	// discovery. It does NOT get the sidecar (standalone, nothing to coordinate).
	if !contains(names, "FLUXION_BACKEND") {
		t.Errorf("want FLUXION_BACKEND injected, got %v", names)
	}
	if !contains(names, "FLUENCE_POD_UID") || !contains(names, "PYTHONSTARTUP") {
		t.Errorf("want interceptor env (FLUENCE_POD_UID, PYTHONSTARTUP), got %v", names)
	}
}

// A standalone quantum pod is tagged (interceptor) but gets no sidecar.
func TestMutateSingleQuantumNoSidecar(t *testing.T) {
	m := &Mutator{}
	ops := m.Mutate(context.Background(), qpuPod("fluence", ""))
	if hasSidecarOp(ops) {
		t.Error("standalone quantum pod should not get a sidecar (nothing to coordinate)")
	}
	if hasGateOp(ops) {
		t.Error("standalone quantum pod should not get a scheduling gate")
	}
}

// A standalone quantum pod WITH the observe label gets the sidecar in
// observe-only mode.
func TestMutateObserveLabelInjectsSidecar(t *testing.T) {
	m := &Mutator{}
	pod := qpuPod("fluence", "")
	pod.Labels = map[string]string{ObserveLabel: "true"}
	ops := m.Mutate(context.Background(), pod)
	if !hasSidecarOp(ops) {
		t.Error("observe-labeled quantum pod should get the sidecar")
	}
	if hasGateOp(ops) {
		t.Error("observe-only pod should not be gated")
	}
}

func TestMutateSkipsOtherScheduler(t *testing.T) {
	m := &Mutator{AttributeKeys: []string{"region"}}
	if ops := m.Mutate(context.Background(), qpuPod("default-scheduler", "")); ops != nil {
		t.Fatalf("non-fluence pod should not be mutated, got %v", ops)
	}
}

func TestMutateRespectsExistingEnv(t *testing.T) {
	m := &Mutator{AttributeKeys: []string{"region"}}
	names := opEnvNames(m.Mutate(context.Background(), qpuPod("fluence", "FLUXION_BACKEND")))
	if contains(names, "FLUXION_BACKEND") {
		t.Errorf("should not re-inject existing FLUXION_BACKEND; got %v", names)
	}
	if !contains(names, "FLUXION_REGION") {
		t.Errorf("should still inject FLUXION_REGION; got %v", names)
	}
}

func TestMutateSkipsNonFluxion(t *testing.T) {
	m := &Mutator{AttributeKeys: []string{"region"}}
	if ops := m.Mutate(context.Background(), cpuPod("fluence")); ops != nil {
		t.Fatalf("classical pod should not be mutated, got %v", ops)
	}
}

func TestEnvVarNames(t *testing.T) {
	m := &Mutator{AttributeKeys: []string{"region", "connectivity"}}
	names := m.EnvVarNames()
	if len(names) != 3 || names[0] != "FLUXION_BACKEND" {
		t.Fatalf("EnvVarNames = %v, want FLUXION_BACKEND first then attrs", names)
	}
}

// A QPU pod with no group label gets no gate and no sidecar.
func TestMutateQPUSinglePodNoSidecar(t *testing.T) {
	m := &Mutator{}
	ops := m.Mutate(context.Background(), qpuPod("fluence", ""))
	if hasGateOp(ops) {
		t.Error("single QPU pod should not get a scheduling gate")
	}
	if hasSidecarOp(ops) {
		t.Error("single QPU pod should not get a sidecar injected")
	}
}

// quantumWorkerGateOps adds the gate to a pod with no existing gates.
func TestQuantumWorkerGateOpsEmpty(t *testing.T) {
	pod := qpuPod("fluence", "")
	ops := quantumWorkerGateOps(pod)
	if !hasGateOp(ops) {
		t.Errorf("expected gate op, got %v", ops)
	}
}

// quantumWorkerGateOps is idempotent.
func TestQuantumWorkerGateOpsIdempotent(t *testing.T) {
	pod := qpuPod("fluence", "")
	pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: QuantumGateName}}
	ops := quantumWorkerGateOps(pod)
	if len(ops) != 0 {
		t.Errorf("expected no ops when gate already present, got %v", ops)
	}
}

// groupName returns the quantum group label value.
func TestGroupName(t *testing.T) {
	pod := qpuPod("fluence", "")
	if groupName(pod) != "" {
		t.Error("pod without group label should return empty")
	}
	pod.Labels = map[string]string{QuantumGroupLabel: "my-workflow"}
	if groupName(pod) != "my-workflow" {
		t.Errorf("expected my-workflow, got %q", groupName(pod))
	}
}

// schedulingGroupOps stamps spec.schedulingGroup.podGroupName on a pod with no
// existing scheduling group — this is the field the scheduler gangs by.
func TestSchedulingGroupOpsEmpty(t *testing.T) {
	pod := qpuPod("fluence", "")
	ops := schedulingGroupOps(pod, "my-workflow")
	if len(ops) != 1 {
		t.Fatalf("expected 1 op, got %d: %v", len(ops), ops)
	}
	if ops[0].Path != "/spec/schedulingGroup" {
		t.Errorf("expected path /spec/schedulingGroup, got %q", ops[0].Path)
	}
	val, ok := ops[0].Value.(map[string]string)
	if !ok || val["podGroupName"] != "my-workflow" {
		t.Errorf("expected podGroupName=my-workflow, got %v", ops[0].Value)
	}
}

// schedulingGroupOps is idempotent when the pod is already linked to the group.
func TestSchedulingGroupOpsAlreadyLinked(t *testing.T) {
	pod := qpuPod("fluence", "")
	group := "my-workflow"
	pod.Spec.SchedulingGroup = &corev1.PodSchedulingGroup{PodGroupName: &group}
	if ops := schedulingGroupOps(pod, group); len(ops) != 0 {
		t.Errorf("expected no ops when already linked, got %v", ops)
	}
}

// schedulingGroupOps re-stamps when linked to a DIFFERENT group.
func TestSchedulingGroupOpsDifferentGroup(t *testing.T) {
	pod := qpuPod("fluence", "")
	other := "other-group"
	pod.Spec.SchedulingGroup = &corev1.PodSchedulingGroup{PodGroupName: &other}
	if ops := schedulingGroupOps(pod, "my-workflow"); len(ops) != 1 {
		t.Errorf("expected 1 op when linked to a different group, got %v", ops)
	}
}

// ── Handler integration: worker gating in a quantum group ────────────────────
//
// A classical worker (no QPU request) that is a non-leader member of a group
// whose leader IS a quantum pod must be gated. This exercises the multi-handler
// flow and the cluster-state lookup in quantumHandler.isWorkerOfQuantumGroup.

func quantumGroupFixture(ns, group, leaderName string) *fake.Clientset {
	// PodGroup with the leader recorded, and the leader pod (which requests QPU).
	pg := &schedulingv1alpha2.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      group,
			Namespace: ns,
			Annotations: map[string]string{
				QuantumLeaderAnnotation: leaderName,
			},
		},
	}
	leaderPod := qpuPod("fluence", "")
	leaderPod.Name = leaderName
	leaderPod.Namespace = ns
	leaderPod.Labels = map[string]string{QuantumGroupLabel: group}
	return fake.NewSimpleClientset(pg, leaderPod)
}

func TestQuantumWorkerIsGated(t *testing.T) {
	ns, group, leader := "default", "qaoa", "qaoa-leader"
	m := &Mutator{Client: quantumGroupFixture(ns, group, leader)}

	// Classical worker: no QPU request, in the group, not the leader.
	worker := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "qaoa-worker-0",
			Namespace: ns,
			Labels:    map[string]string{QuantumGroupLabel: group},
		},
		Spec: corev1.PodSpec{
			SchedulerName: "fluence",
			Containers: []corev1.Container{{
				Name: "worker",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU: *resource.NewQuantity(1, resource.DecimalSI),
					},
				},
			}},
		},
	}

	ops := m.Mutate(context.Background(), worker)
	if !hasGateOp(ops) {
		t.Errorf("classical worker in a quantum group should be gated; ops=%v", ops)
	}
	if hasSidecarOp(ops) {
		t.Error("worker should not get a sidecar")
	}
}

// A worker in a CLASSICAL gang (leader does not request QPU) must NOT be gated.
func TestClassicalGangWorkerNotGated(t *testing.T) {
	ns, group, leader := "default", "classical", "classical-leader"
	// Leader pod requests only CPU — not a quantum group.
	pg := &schedulingv1alpha2.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name: group, Namespace: ns,
			Annotations: map[string]string{QuantumLeaderAnnotation: leader},
		},
	}
	leaderPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: leader, Namespace: ns,
			Labels: map[string]string{QuantumGroupLabel: group}},
		Spec: corev1.PodSpec{SchedulerName: "fluence", Containers: []corev1.Container{{
			Name: "c",
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU: *resource.NewQuantity(1, resource.DecimalSI)}},
		}}},
	}
	m := &Mutator{Client: fake.NewSimpleClientset(pg, leaderPod)}

	worker := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "classical-worker-0", Namespace: ns,
			Labels: map[string]string{QuantumGroupLabel: group}},
		Spec: corev1.PodSpec{SchedulerName: "fluence", Containers: []corev1.Container{{
			Name: "c",
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU: *resource.NewQuantity(1, resource.DecimalSI)}},
		}}},
	}

	ops := m.Mutate(context.Background(), worker)
	if hasGateOp(ops) {
		t.Error("worker in a classical gang must NOT be gated (would deadlock)")
	}
}

// Pod-template gang: every pod has an identical spec — same group label AND
// every pod requests the quantum resource. Role must be decided by admission
// order (the recorded leader), NOT by resource request. The recorded leader
// gets the sidecar; a different pod in the same group is gated as a worker
// even though it also requests QPU.
func TestPodTemplateGangSecondPodIsWorker(t *testing.T) {
	ns, group, leader := "default", "qaoa", "qaoa-abc123"

	// Leader already recorded on the PodGroup; leader pod requests QPU.
	pg := &schedulingv1alpha2.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: group, Namespace: ns,
			Annotations: map[string]string{QuantumLeaderAnnotation: leader}},
	}
	leaderPod := qpuPod("fluence", "")
	leaderPod.Name = leader
	leaderPod.Namespace = ns
	leaderPod.Labels = map[string]string{QuantumGroupLabel: group}
	m := &Mutator{Client: fake.NewSimpleClientset(pg, leaderPod)}

	// A SECOND pod from the same template: identical spec, requests QPU, same
	// group label, but a different name — it is NOT the recorded leader.
	second := qpuPod("fluence", "")
	second.Name = "qaoa-def456"
	second.Namespace = ns
	second.Labels = map[string]string{QuantumGroupLabel: group}

	ops := m.Mutate(context.Background(), second)
	if !hasGateOp(ops) {
		t.Error("second pod in a pod-template gang must be gated as a worker, not treated as leader")
	}
	if hasSidecarOp(ops) {
		t.Error("second pod must NOT get a sidecar (it is a worker, not the leader)")
	}
}
