// quantum_test.go, all tests for the quantum handler. The scout/worker
// shared-coordination split (no separate submitter pod), independent mode,
// the coordination role + job-id handoff, the sidecar wiring, the Dependency primitive, and the
// standalone/observe paths. Shared fixtures (qpuPod, cpuPod, op helpers) live in
// handlers_test.go.
package handlers

import (
	"context"
	"testing"

	"github.com/converged-computing/fluence/pkg/webhook"
	"github.com/converged-computing/fluence/pkg/webhook/spec"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// ── standalone / observe ────────────────────────────────────────────────────────

func TestSingleQuantumGetsInterceptorNoSidecar(t *testing.T) {
	m := &webhook.Mutator{AttributeKeys: []string{"region"}}
	ops := m.Mutate(context.Background(), qpuPod("fluence"))
	names := opEnvNames(ops)
	if !contains(names, "FLUXION_BACKEND") {
		t.Errorf("want FLUXION_BACKEND, got %v", names)
	}
	if !contains(names, "PYTHONPATH") || !contains(names, "FLUENCE_POD_UID") {
		t.Errorf("want interceptor env (PYTHONPATH, FLUENCE_POD_UID), got %v", names)
	}
	if hasSidecarOp(ops) {
		t.Error("standalone quantum pod should not get a sidecar")
	}
	if hasGateOp(ops) {
		t.Error("standalone quantum pod should not be gated")
	}
}

func TestObserveLabelInjectsSidecar(t *testing.T) {
	m := &webhook.Mutator{}
	pod := qpuPod("fluence")
	pod.Labels = map[string]string{ObserveLabel: "true"}
	ops := m.Mutate(context.Background(), pod)
	if !hasSidecarOp(ops) {
		t.Error("observe-labeled quantum pod should get the sidecar")
	}
	if hasGateOp(ops) {
		t.Error("observe-only pod should not be gated")
	}
}

// ── shared coordination. Scout / worker split ──────────────────────────────

// sharedQPUPod is a quantum workload pod (requests the resource) in a group,
// owned by a Job of parallelism N, with coordination=shared and a completion
// index. Index "0" is the scout. Any other index is a worker. This is the
// real shape. An indexed Job whose identical template yields differentiated
// roles purely from the completion index.
func sharedQPUPod(ns, group, name, job, index string) *corev1.Pod {
	p := qpuPod("fluence")
	p.Name = name
	p.Namespace = ns
	p.Labels = map[string]string{webhook.GroupLabel: group}
	p.Annotations = map[string]string{
		CoordinationAnnotation:    CoordinationShared,
		CompletionIndexAnnotation: index,
	}
	p.OwnerReferences = []metav1.OwnerReference{{Kind: "Job", Name: job}}
	return p
}

// gangQPUPod is a quantum workload pod in a group owned by a Job, with NO
// coordination annotation, i.e. the default (independent) mode.
func gangQPUPod(ns, group, name, job string) *corev1.Pod {
	p := qpuPod("fluence")
	p.Name = name
	p.Namespace = ns
	p.Labels = map[string]string{webhook.GroupLabel: group}
	p.OwnerReferences = []metav1.OwnerReference{{Kind: "Job", Name: job}}
	return p
}

// mincount returns the gang minCount of the named PodGroup, or ok=false.
func mincount(t *testing.T, cs *fake.Clientset, ns, group string) (int32, bool) {
	t.Helper()
	pg, err := cs.SchedulingV1alpha2().PodGroups(ns).Get(context.Background(), group, metav1.GetOptions{})
	if err != nil || pg.Spec.SchedulingPolicy.Gang == nil {
		return 0, false
	}
	return pg.Spec.SchedulingPolicy.Gang.MinCount, true
}

// A shared-mode WORKER (completion index != 0, owned by Job parallelism=3) is
// gated, told its role (FLUENCE_COORDINATION_ROLE=worker), joins the <group>
// worker gang at minCount N-1 (the split), and gets NO sidecar (it is gated).
// No separate submitter pod is ever created, the scout is one of the N members.
func TestSharedWorkerGatedRoleAndSplit(t *testing.T) {
	ns, group, job := "default", "qg", "qg-job"
	par := int32(3)
	cs := fake.NewSimpleClientset(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: job, Namespace: ns},
		Spec:       batchv1.JobSpec{Parallelism: &par, Completions: &par}})
	m := &webhook.Mutator{Clientset: cs}

	ops := m.Mutate(context.Background(), sharedQPUPod(ns, group, "qg-1", job, "1"))

	if !hasGateOp(ops) {
		t.Error("worker must be gated")
	}
	if hasSidecarOp(ops) {
		t.Error("worker (gated) must NOT get a sidecar")
	}
	if !hasDropQuantumResourceOp(ops) {
		t.Error("worker (gated, uses the scout session) must have its qpu resource stripped")
	}
	if e, ok := envOp(ops, CoordinationRoleEnv); !ok || e.Value != RoleWorker {
		t.Errorf("worker must get %s=%s", CoordinationRoleEnv, RoleWorker)
	}
	// Worker gang is minCount N-1 (the scout/worker split).
	if mc, ok := mincount(t, cs, ns, group); !ok || mc != 2 {
		t.Errorf("worker PodGroup minCount=%d (ok=%v), want 2 (N-1 split)", mc, ok)
	}
	// No separate submitter pod is created.
	pods, _ := cs.CoreV1().Pods(ns).List(context.Background(), metav1.ListOptions{})
	if len(pods.Items) != 0 {
		t.Errorf("shared mode must NOT spawn a separate submitter pod; found %d pods", len(pods.Items))
	}
}

// The shared-mode SCOUT (completion index 0) is wired as the real coordinator.
// its own group-of-one <group>-scout at minCount 1, the real sidecar, not
// gated, role=scout, and told which worker group to ungate via
// FLUENCE_GANG_GROUP. It is one of the N members, no extra pod is created.
func TestSharedScoutWiredAsRealSidecar(t *testing.T) {
	ns, group, job := "default", "qg2", "qg2-job"
	par := int32(2)
	cs := fake.NewSimpleClientset(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: job, Namespace: ns},
		Spec:       batchv1.JobSpec{Parallelism: &par, Completions: &par}})
	m := &webhook.Mutator{Clientset: cs}

	ops := m.Mutate(context.Background(), sharedQPUPod(ns, group, "qg2-0", job, "0"))

	if !hasSidecarOp(ops) {
		t.Error("scout must get the real sidecar")
	}
	if hasGateOp(ops) {
		t.Error("scout must NOT be gated")
	}
	if hasDropQuantumResourceOp(ops) {
		t.Error("scout must KEEP its qpu resource (it runs the real submit)")
	}
	if e, ok := envOp(ops, CoordinationRoleEnv); !ok || e.Value != RoleScout {
		t.Errorf("scout must get %s=%s", CoordinationRoleEnv, RoleScout)
	}
	if _, ok := envOp(ops, QuantumJobIDEnv); ok {
		t.Error("scout must NOT get FLUENCE_QUANTUM_JOB_ID (it submits its own task)")
	}
	// FLUENCE_GANG_GROUP (the worker group to ungate) is on the sidecar.
	var sidecar *corev1.Container
	for _, op := range ops {
		if c, ok := op.Value.(corev1.Container); ok && c.Name == SidecarContainerName {
			cc := c
			sidecar = &cc
		}
	}
	if sidecar == nil {
		t.Fatal("no sidecar container on scout")
	}
	var gotGang bool
	for _, e := range sidecar.Env {
		if e.Name == GangGroupEnv && e.Value == group {
			gotGang = true
		}
	}
	if !gotGang {
		t.Errorf("scout sidecar must get %s=%q", GangGroupEnv, group)
	}
	// Scout is its own group-of-one (minCount 1).
	if mc, ok := mincount(t, cs, ns, group+ScoutGroupSuffix); !ok || mc != 1 {
		t.Errorf("scout PodGroup %s minCount=%d (ok=%v), want 1", group+ScoutGroupSuffix, mc, ok)
	}
	// No separate submitter pod.
	pods, _ := cs.CoreV1().Pods(ns).List(context.Background(), metav1.ListOptions{})
	if len(pods.Items) != 0 {
		t.Errorf("scout is a member, not a spawned pod; found %d pods", len(pods.Items))
	}
}

// The rehearsal label is the only thing that lets the sidecar release the
// workers when the scout submits nothing. It must reach the workload and the
// scout's sidecar, and be absent from both without the label.
func TestRehearsalLabelReachesWorkloadAndSidecar(t *testing.T) {
	ns, group, job := "default", "qr", "qr-job"
	par := int32(2)
	for _, labeled := range []bool{true, false} {
		cs := fake.NewSimpleClientset(&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: job, Namespace: ns},
			Spec:       batchv1.JobSpec{Parallelism: &par, Completions: &par}})
		m := &webhook.Mutator{Clientset: cs}
		pod := sharedQPUPod(ns, group, "qr-0", job, "0")
		if labeled {
			pod.Labels[RehearsalLabel] = "true"
		}
		ops := m.Mutate(context.Background(), pod)

		_, onWorkload := envOp(ops, RehearsalEnv)
		onSidecar := false
		for _, op := range ops {
			if c, ok := op.Value.(corev1.Container); ok && c.Name == SidecarContainerName {
				for _, e := range c.Env {
					if e.Name == RehearsalEnv && e.Value == "true" {
						onSidecar = true
					}
				}
			}
		}
		if onWorkload != labeled || onSidecar != labeled {
			t.Errorf("labeled=%v: %s on workload=%v, on sidecar=%v", labeled, RehearsalEnv, onWorkload, onSidecar)
		}
	}
}

// Shared mode never creates an extra pod. A full gang (scout index 0 +
// workers) is N members, so the application runs exactly N times (not N+1 as
// the old submitter-pod model did).
func TestSharedGangNoSeparateSubmitterPod(t *testing.T) {
	ns, group, job := "default", "qauto", "qauto-job"
	par := int32(2)
	cs := fake.NewSimpleClientset(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: job, Namespace: ns},
		Spec:       batchv1.JobSpec{Parallelism: &par, Completions: &par}})
	m := &webhook.Mutator{Clientset: cs}

	m.Mutate(context.Background(), sharedQPUPod(ns, group, "qauto-0", job, "0")) // scout
	m.Mutate(context.Background(), sharedQPUPod(ns, group, "qauto-1", job, "1")) // worker

	pods, _ := cs.CoreV1().Pods(ns).List(context.Background(), metav1.ListOptions{})
	if len(pods.Items) != 0 {
		t.Errorf("shared mode must not create any pods (no submitter); found %d", len(pods.Items))
	}
	// Both groups exist with the right minCounts.
	if mc, ok := mincount(t, cs, ns, group+ScoutGroupSuffix); !ok || mc != 1 {
		t.Errorf("scout group minCount=%d (ok=%v), want 1", mc, ok)
	}
	if mc, ok := mincount(t, cs, ns, group); !ok || mc != 1 {
		t.Errorf("worker group minCount=%d (ok=%v), want N-1=1", mc, ok)
	}
}

// ── independent mode (default) ──────────────────────────────────────────────────

// A grouped quantum pod with no coordination annotation is INDEPENDENT (default).
// it does its own real submit, is not gated, has no coordination role, and
// triggers no group split and no submitter pod.
func TestIndependentGroupedQuantumIsStandalone(t *testing.T) {
	ns, group, job := "default", "indep", "indep-job"
	par := int32(3)
	cs := fake.NewSimpleClientset(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: job, Namespace: ns},
		Spec:       batchv1.JobSpec{Parallelism: &par, Completions: &par}})
	m := &webhook.Mutator{Clientset: cs}

	ops := m.Mutate(context.Background(), gangQPUPod(ns, group, "indep-0", job))

	if hasGateOp(ops) {
		t.Error("independent member must not be gated")
	}
	if _, ok := envOp(ops, CoordinationRoleEnv); ok {
		t.Error("independent member must not get a coordination role env")
	}
	if _, ok := mincount(t, cs, ns, group+ScoutGroupSuffix); ok {
		t.Error("independent mode must not create a scout group")
	}
	pods, _ := cs.CoreV1().Pods(ns).List(context.Background(), metav1.ListOptions{})
	if len(pods.Items) != 0 {
		t.Error("independent mode must not spawn a submitter pod")
	}
}

// A standalone quantum pod (no group, no owner → group of one) does its own real
// submit interceptor staged, but no gating, no coordination role, no submitter.
func TestStandaloneQuantumIsReal(t *testing.T) {
	ns := "default"
	cs := fake.NewSimpleClientset()
	m := &webhook.Mutator{Clientset: cs}

	pod := qpuPod("fluence")
	pod.Name = "solo"
	pod.Namespace = ns

	ops := m.Mutate(context.Background(), pod)
	if hasGateOp(ops) {
		t.Error("standalone quantum pod must not be gated")
	}
	if _, ok := envOp(ops, CoordinationRoleEnv); ok {
		t.Error("standalone quantum pod must not get a coordination role env")
	}
	pods, _ := cs.CoreV1().Pods(ns).List(context.Background(), metav1.ListOptions{})
	if len(pods.Items) != 0 {
		t.Error("standalone quantum pod must not spawn a submitter")
	}
}

// Even with coordination=shared, a group of one (Job parallelism 1) has no
// workers to coordinate, so it falls through to the standalone real-submit path.
func TestSharedGroupOfOneIsStandalone(t *testing.T) {
	ns, group, job := "default", "one", "one-job"
	par := int32(1)
	cs := fake.NewSimpleClientset(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: job, Namespace: ns},
		Spec:       batchv1.JobSpec{Parallelism: &par, Completions: &par}})
	m := &webhook.Mutator{Clientset: cs}

	ops := m.Mutate(context.Background(), sharedQPUPod(ns, group, "one-0", job, "0"))
	if hasGateOp(ops) {
		t.Error("shared group-of-one must not be gated")
	}
	if _, ok := mincount(t, cs, ns, group+ScoutGroupSuffix); ok {
		t.Error("shared group-of-one must not create a scout group")
	}
}

// ── role + dependency ────────────────────────────────────────────────────

// envOp returns the env var op with the given name, if present (covers both
// single-EnvVar and []EnvVar op shapes).
func envOp(ops []spec.Op, name string) (corev1.EnvVar, bool) {
	for _, op := range ops {
		switch v := op.Value.(type) {
		case corev1.EnvVar:
			if v.Name == name {
				return v, true
			}
		case []corev1.EnvVar:
			for _, e := range v {
				if e.Name == name {
					return e, true
				}
			}
		}
	}
	return corev1.EnvVar{}, false
}

// annotationOps collects all annotation key=value pairs the ops would stamp.
func annotationOps(ops []spec.Op) map[string]string {
	out := map[string]string{}
	for _, op := range ops {
		// whole-map add. /metadata/annotations
		if op.Path == "/metadata/annotations" {
			if m, ok := op.Value.(map[string]string); ok {
				for k, v := range m {
					out[k] = v
				}
			}
			continue
		}
		// single-key add: /metadata/annotations/<escaped-key> -> string value
		const pfx = "/metadata/annotations/"
		if len(op.Path) > len(pfx) && op.Path[:len(pfx)] == pfx {
			if s, ok := op.Value.(string); ok {
				key := unescapeJSONPointer(op.Path[len(pfx):])
				out[key] = s
			}
		}
	}
	return out
}

// unescapeJSONPointer reverses escapeJSONPointer for assertion readability.
func unescapeJSONPointer(s string) string {
	// reverse order of escape: ~1 -> /, then ~0 -> ~
	out := ""
	for i := 0; i < len(s); i++ {
		if s[i] == '~' && i+1 < len(s) {
			switch s[i+1] {
			case '1':
				out += "/"
				i++
				continue
			case '0':
				out += "~"
				i++
				continue
			}
		}
		out += string(s[i])
	}
	return out
}

// A shared-mode worker is expressed as a general Dependency. Gated, stamped
// with depends-on-{kind,scout,gate}, and the scout is the <group>-scout
// group.
func TestQuantumWorkerIsGeneralDependency(t *testing.T) {
	ns, group, job := "default", "depq", "depq-job"
	par := int32(3)
	cs := fake.NewSimpleClientset(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: job, Namespace: ns},
		Spec:       batchv1.JobSpec{Parallelism: &par, Completions: &par}})
	m := &webhook.Mutator{Clientset: cs}

	ops := m.Mutate(context.Background(), sharedQPUPod(ns, group, "depq-1", job, "1"))

	if !hasGateOp(ops) {
		t.Errorf("worker not gated by the dependency (ops: %+v)", ops)
	}
	ann := annotationOps(ops)
	if ann[DependsOnKindAnnotation] != DependencyKindQuantumSubmit {
		t.Errorf("depends-on-kind=%q, want %q", ann[DependsOnKindAnnotation], DependencyKindQuantumSubmit)
	}
	if ann[DependsOnScoutAnnotation] != group+ScoutGroupSuffix {
		t.Errorf("depends-on-scout=%q, want %q (the scout group)", ann[DependsOnScoutAnnotation], group+ScoutGroupSuffix)
	}
	if ann[DependsOnGateAnnotation] != QuantumGate {
		t.Errorf("depends-on-gate=%q, want %q", ann[DependsOnGateAnnotation], QuantumGate)
	}
}

// DependencyOf round-trips the stamped annotations back into a Dependency, so a
// scheduler/sidecar observer can read what a gated pod waits for.
func TestDependencyOfRoundTrip(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
		DependsOnKindAnnotation:     DependencyKindQuantumSubmit,
		DependsOnScoutAnnotation: "grp",
		DependsOnGateAnnotation:     QuantumGate,
	}}}
	d, ok := DependencyOf(pod)
	if !ok || d.Kind != DependencyKindQuantumSubmit || d.Scout != "grp" || d.Gate != QuantumGate {
		t.Errorf("DependencyOf=%+v ok=%v, want quantum-submit/grp/%s", d, ok, QuantumGate)
	}
	if _, ok := DependencyOf(&corev1.Pod{}); ok {
		t.Errorf("DependencyOf on a pod with no dependency should be ok=false")
	}
}

// A worker gets FLUENCE_COORDINATION_ROLE=worker and the scout's task id in
// FLUENCE_QUANTUM_JOB_ID. It is not staged with the interceptor, since any
// submit it makes goes into the scout session and needs no tagging.
func TestQuantumWorkerStagedWithRole(t *testing.T) {
	ns, group, job := "default", "roleq", "roleq-job"
	par := int32(2)
	cs := fake.NewSimpleClientset(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: job, Namespace: ns},
		Spec:       batchv1.JobSpec{Parallelism: &par, Completions: &par}})
	m := &webhook.Mutator{Clientset: cs}

	ops := m.Mutate(context.Background(), sharedQPUPod(ns, group, "roleq-1", job, "1"))

	// Role surfaced to the container.
	if e, ok := envOp(ops, CoordinationRoleEnv); !ok || e.Value != RoleWorker {
		t.Errorf("worker missing %s=%s (got %+v, ok=%v)", CoordinationRoleEnv, RoleWorker, e, ok)
	}

	// no interceptor on a worker, its submits go into the scout session
	if _, ok := envOp(ops, "PYTHONPATH"); ok {
		t.Error("worker must not be staged with the interceptor")
	}

	// Scout's task id sourced from the annotation the ungating sidecar stamps.
	e, ok := envOp(ops, QuantumJobIDEnv)
	if !ok {
		t.Fatalf("worker missing %s env", QuantumJobIDEnv)
	}
	if e.ValueFrom == nil || e.ValueFrom.FieldRef == nil ||
		e.ValueFrom.FieldRef.FieldPath != "metadata.annotations['"+QuantumJobIDAnnotation+"']" {
		t.Errorf("%s should be a downward-API ref to %s, got %+v", QuantumJobIDEnv, QuantumJobIDAnnotation, e)
	}
}

// Classical override below the replica count: group-size=2 on a gang owned by a
// Job(parallelism=5) must yield minCount=2 (the override), not 5. With a cluster
// sized to 2, the gang reaches quorum and runs. If the override were dropped the
// gang would wait forever for 5 (the e2e hang that fails CI).
func TestClassicalOverrideBelowReplicaCount(t *testing.T) {
	ns, group, job := "default", "ovr2", "ovr2-job"
	pod := cpuPod("fluence")
	pod.Namespace = ns
	pod.Labels = map[string]string{webhook.GroupLabel: group}
	pod.Annotations = map[string]string{webhook.GroupSizeAnnotation: "2"}
	ownedBy(pod, "Job", job)

	got := minCountWithClient(t, pod, jobWithParallelism(ns, job, 5))
	if got != 2 {
		t.Errorf("override below replicas: minCount=%d, want 2 (override wins over Job=5)", got)
	}
}

// ── sidecar wiring ──────────────────────────────────────────────────────────────

// The sidecar inherits the workload's secret/configMap-sourced credentials so it
// can talk to the same backend, but NOT plain-value env. (Moved from the core
// webhook package. Sidecar construction is now quantum-owned.)
func TestSidecarInheritsWorkloadSecretEnv(t *testing.T) {
	m := &webhook.Mutator{Clientset: fake.NewSimpleClientset()}
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: "app",
				Env: []corev1.EnvVar{
					{Name: "PLAIN_VALUE", Value: "x"}, // plain value: NOT copied
					{Name: "AWS_ACCESS_KEY_ID", ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "aws-braket-credentials"},
							Key:                  "AWS_ACCESS_KEY_ID",
						}}},
				},
			}},
		},
	}
	ops := sidecarContainerOps(m, pod, false, nil)
	var sidecar *corev1.Container
	for _, op := range ops {
		if c, ok := op.Value.(corev1.Container); ok && c.Name == SidecarContainerName {
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
		if e.Name == "PLAIN_VALUE" {
			gotPlain = true
		}
	}
	if !gotSecret {
		t.Error("sidecar should inherit the workload's secret-sourced AWS creds")
	}
	if gotPlain {
		t.Error("sidecar should NOT copy plain-value workload env")
	}
}

// The scout member of a shared gang requests the quantum resource (it runs the
// real submit). Sanity check that the helper builds a quantum pod.
func TestSharedScoutRequestsQuantumResource(t *testing.T) {
	p := sharedQPUPod("default", "g", "g-0", "g-job", "0")
	if !spec.PodRequestsResource(p, QuantumResource) {
		t.Error("scout must request the quantum resource (it runs the real submit)")
	}
}
