/*
Copyright 2024 Lawrence Livermore National Security, LLC
 (c.f. AUTHORS, NOTICE.LLNS, COPYING)
SPDX-License-Identifier: Apache-2.0
*/

// quantum_test.go — all tests for the quantum handler: the gang + submitter
// model, faux-submit, the sidecar wiring, the Dependency primitive, and the
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

// ── gang + submitter ────────────────────────────────────────────────────────────

// gangQPUPod is a quantum workload pod (requests the resource) in a group,
// owned by a Job of parallelism N — the common real shape (a MiniCluster /
// indexed Job). No role annotation: the new model has no leader/worker.
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

// A quantum gang member (owned by Job parallelism=3) is gated + faux, its gang
// PodGroup is minCount 3 (full N — no N-1 split), and Fluence creates the
// separate <group>-submitter pod. It gets NO sidecar (it is gated).
func TestQuantumGangGatedFauxAndSubmitterCreated(t *testing.T) {
	ns, group, job := "default", "qg", "qg-job"
	par := int32(3)
	cs := fake.NewSimpleClientset(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: job, Namespace: ns},
		Spec:       batchv1.JobSpec{Parallelism: &par, Completions: &par}})
	m := &webhook.Mutator{Clientset: cs}

	ops := m.Mutate(context.Background(), gangQPUPod(ns, group, "qg-0", job))

	if !hasGateOp(ops) {
		t.Error("gang member must be gated")
	}
	if hasSidecarOp(ops) {
		t.Error("gang member (gated) must NOT get a sidecar")
	}
	if e, ok := envOp(ops, FauxSubmitEnv); !ok || e.Value != "true" {
		t.Errorf("gang member must get %s=true", FauxSubmitEnv)
	}
	if mc, ok := mincount(t, cs, ns, group); !ok || mc != 3 {
		t.Errorf("gang PodGroup minCount=%d (ok=%v), want 3 (full N, no split)", mc, ok)
	}
	// No <group>-workers subgroup in the new model.
	if _, ok := mincount(t, cs, ns, group+"-workers"); ok {
		t.Error("there must be no -workers subgroup in the gang+submitter model")
	}
	// Fluence created the submitter.
	sub, err := cs.CoreV1().Pods(ns).Get(context.Background(), group+SubmitterGroupSuffix, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("submitter pod not created: %v", err)
	}
	if sub.Annotations[SubmitterAnnotation] != "true" {
		t.Error("submitter must carry the submitter marker")
	}
	if sub.Annotations[GangGroupAnnotation] != group {
		t.Errorf("submitter gang-group=%q, want %q", sub.Annotations[GangGroupAnnotation], group)
	}
	if len(sub.Spec.SchedulingGates) != 0 {
		t.Error("submitter must NOT be gated")
	}
}

// The submitter pod, on its own admission, is wired as the real coordinator: its
// own PodGroup minCount 1, the real sidecar (not faux), not gated, and told which
// gang to ungate via FLUENCE_GANG_GROUP.
func TestSubmitterWiredAsRealSidecar(t *testing.T) {
	ns, group, job := "default", "qg2", "qg2-job"
	par := int32(2)
	cs := fake.NewSimpleClientset(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: job, Namespace: ns},
		Spec:       batchv1.JobSpec{Parallelism: &par, Completions: &par}})
	m := &webhook.Mutator{Clientset: cs}

	// First a gang member, which creates the submitter.
	m.Mutate(context.Background(), gangQPUPod(ns, group, "qg2-0", job))
	sub, err := cs.CoreV1().Pods(ns).Get(context.Background(), group+SubmitterGroupSuffix, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("submitter not created: %v", err)
	}

	ops := m.Mutate(context.Background(), sub)
	if !hasSidecarOp(ops) {
		t.Error("submitter must get the real sidecar")
	}
	if hasGateOp(ops) {
		t.Error("submitter must NOT be gated")
	}
	if _, ok := envOp(ops, FauxSubmitEnv); ok {
		t.Error("submitter must NOT be in faux mode")
	}
	// FLUENCE_GANG_GROUP is on the sidecar container itself.
	var sidecar *corev1.Container
	for _, op := range ops {
		if c, ok := op.Value.(corev1.Container); ok && c.Name == SidecarContainerName {
			cc := c
			sidecar = &cc
		}
	}
	if sidecar == nil {
		t.Fatal("no sidecar container on submitter")
	}
	var gotGang bool
	for _, e := range sidecar.Env {
		if e.Name == GangGroupEnv && e.Value == group {
			gotGang = true
		}
	}
	if !gotGang {
		t.Errorf("submitter sidecar must get %s=%q", GangGroupEnv, group)
	}
	if mc, ok := mincount(t, cs, ns, group+SubmitterGroupSuffix); !ok || mc != 1 {
		t.Errorf("submitter PodGroup minCount=%d (ok=%v), want 1", mc, ok)
	}
}

// A standalone quantum pod (no group, no owner → group of one) does its own real
// submit: interceptor staged, but no gating, no faux, and no separate submitter.
func TestStandaloneQuantumIsRealNoSubmitter(t *testing.T) {
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
	if _, ok := envOp(ops, FauxSubmitEnv); ok {
		t.Error("standalone quantum pod must not be faux")
	}
	pods, _ := cs.CoreV1().Pods(ns).List(context.Background(), metav1.ListOptions{})
	if len(pods.Items) != 0 {
		t.Error("standalone quantum pod must not spawn a submitter")
	}
}

// ── faux-submit + dependency ────────────────────────────────────────────────────

// envValueFrom returns the env var op with the given name, if present (covers
// both single-EnvVar and []EnvVar op shapes).
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
		// whole-map add: /metadata/annotations
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

// A quantum worker (no group-size of its own) is expressed as a general
// Dependency: gated, stamped with depends-on-{kind,producer,gate}, and the
// producer is the base group.
func TestQuantumWorkerIsGeneralDependency(t *testing.T) {
	ns, group, job := "default", "depq", "depq-job"
	par := int32(3)
	cs := fake.NewSimpleClientset(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: job, Namespace: ns},
		Spec:       batchv1.JobSpec{Parallelism: &par, Completions: &par}})
	m := &webhook.Mutator{Clientset: cs}

	ops := m.Mutate(context.Background(), gangQPUPod(ns, group, "depq-0", job))

	if !hasGateOp(ops) {
		t.Errorf("worker not gated by the dependency (ops: %+v)", ops)
	}
	ann := annotationOps(ops)
	if ann[DependsOnKindAnnotation] != DependencyKindQuantumSubmit {
		t.Errorf("depends-on-kind=%q, want %q", ann[DependsOnKindAnnotation], DependencyKindQuantumSubmit)
	}
	if ann[DependsOnProducerAnnotation] != group+SubmitterGroupSuffix {
		t.Errorf("depends-on-producer=%q, want %q (the submitter group)", ann[DependsOnProducerAnnotation], group+SubmitterGroupSuffix)
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
		DependsOnProducerAnnotation: "grp",
		DependsOnGateAnnotation:     QuantumGate,
	}}}
	d, ok := DependencyOf(pod)
	if !ok || d.Kind != DependencyKindQuantumSubmit || d.Producer != "grp" || d.Gate != QuantumGate {
		t.Errorf("DependencyOf=%+v ok=%v, want quantum-submit/grp/%s", d, ok, QuantumGate)
	}
	if _, ok := DependencyOf(&corev1.Pod{}); ok {
		t.Errorf("DependencyOf on a pod with no dependency should be ok=false")
	}
}

// The worker is staged with the SAME interceptor as the submitter (PYTHONPATH +
// FLUENCE_POD_UID), put into faux mode (FLUENCE_FAUX_SUBMIT=true), and handed the
// existing task id via the FLUENCE_QUANTUM_JOB_ID downward-API env. One
// mechanism, two modes — no separate ConfigMap shim. The user sets nothing.
func TestQuantumWorkerStagedWithFauxSubmit(t *testing.T) {
	ns, group, job := "default", "fauxq", "fauxq-job"
	par := int32(2)
	cs := fake.NewSimpleClientset(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: job, Namespace: ns},
		Spec:       batchv1.JobSpec{Parallelism: &par, Completions: &par}})
	m := &webhook.Mutator{Clientset: cs}

	ops := m.Mutate(context.Background(), gangQPUPod(ns, group, "fauxq-0", job))

	// Same interceptor staging as the submitter (PYTHONPATH set on the worker).
	if _, ok := envOp(ops, "PYTHONPATH"); !ok {
		t.Errorf("worker not staged with the interceptor (no PYTHONPATH); ops: %+v", ops)
	}

	// Faux mode selected.
	if e, ok := envOp(ops, FauxSubmitEnv); !ok || e.Value != "true" {
		t.Errorf("worker missing %s=true (got %+v, ok=%v)", FauxSubmitEnv, e, ok)
	}

	// Existing task id sourced from the annotation the ungating sidecar stamps.
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
// sized to 2, the gang reaches quorum and runs; if the override were dropped the
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
// webhook package: sidecar construction is now quantum-owned.)
func TestSidecarInheritsWorkloadSecretEnv(t *testing.T) {
	m := &webhook.Mutator{Clientset: fake.NewSimpleClientset()}
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

// A plain quantum workload pod (no role, owned by a Job of N>1) is gated as a
// faux gang member AND triggers creation of the one-off submitter. The user
// authors no submitter and no roles.
func TestGangMemberTriggersSubmitter(t *testing.T) {
	ns, group, job := "default", "qauto", "qauto-job"
	par := int32(2)
	cs := fake.NewSimpleClientset(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: job, Namespace: ns},
		Spec:       batchv1.JobSpec{Parallelism: &par, Completions: &par}})
	m := &webhook.Mutator{Clientset: cs}

	workload := gangQPUPod(ns, group, "qauto-0", job)
	ops := m.Mutate(context.Background(), workload)

	if !hasGateOp(ops) {
		t.Error("gang member must be gated")
	}
	if _, ok := envOp(ops, FauxSubmitEnv); !ok {
		t.Error("gang member must get FLUENCE_FAUX_SUBMIT")
	}
	sub, err := cs.CoreV1().Pods(ns).Get(context.Background(), group+SubmitterGroupSuffix, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("submitter pod not created: %v", err)
	}
	if !spec.PodRequestsResource(sub, QuantumResource) {
		t.Error("submitter must request the quantum resource (it runs the real submit)")
	}
}
