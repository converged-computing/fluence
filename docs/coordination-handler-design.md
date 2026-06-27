# Coordination handlers: producer/consumer gang split (no separate submitter)

> **Status: implemented.** This design is live in `pkg/webhook/handlers/quantum.go`
> (the coordination router + `mutateProducer`/`mutateConsumer`/`coordinationMode`/
> `isProducer`), `pkg/webhook/handlers/gang.go` (classical gangs defer quantum
> pods to the quantum handler), and `pkg/fluence/fluence.go` (reconcile reaps the
> `<group>-producer` PodGroup, never the producer pod — it is a real member).
> Unit tests are in `pkg/webhook/handlers/quantum_test.go`; structural e2e in
> `test/e2e/quantum/02–04`. Both shared-mode user contracts are served by the same
> wiring: the explicit-role script (consumers never call submit; the faux
> interceptor is unused) and the identical-script (every member calls submit; the
> faux interceptor dedups consumers to the producer's cached task).

## Why this replaces the submitter-pod model

The `add-sidecar-interface` branch coordinates a quantum gang by creating a
*separate* one-off submitter pod (`<group>-submitter`) that runs the user's
application image to do the real submit, then ungates a gang of N faux-submitting
members. That works, but it runs the user's application **N+1 times** for an
N-gang: once in the submitter (a full run whose post-processing nobody consumes)
plus once in each of the N members. The redundant run is not an implementation
wart — it is a symptom of modeling quantum work as a producer/consumer split
while pretending one image plays both roles, selected at runtime by a faux flag.

This design keeps the split (it is correct) but removes the separate pod: the
**producer is one of the N members**, promoted at admission, so the application
runs exactly **N times** — the needed number — with exactly **one real submit**.

The core thesis is unchanged: Fluence is a generic gang scheduler (native gangs
since k8s 1.36), and per-resource nuance lives in handlers. This is entirely a
change to the `quantum` handler plus a one-line deferral in the `gang` handler.

## The fundamental constraint

A quantum task's content (the circuit) comes from user code, so **the pod that
defines a task must run to submit it**. Therefore, per pod, *submit* and *gate*
are mutually exclusive — a pod either runs (and can submit) or is gated (and
cannot). Gating only ever buys resource savings for pods that **do not submit**:
pods that consume a result someone else produced.

That partitions a quantum gang into two kinds, decided per pod:

- **producer** — runs its code, submits its own task, holds a node through the
  queue wait. Not gateable, ever.
- **consumer** — never submits; reads the producer's result. Fully gateable until
  that result is ready.

## Coordination modes (user-facing contract)

Identical pod templates (a Job/Deployment) are genuinely ambiguous between "one
shared task, fan the result out to N pods" and "N independent tasks." Fluence
cannot infer this; the user declares it with one annotation on the pod template:

```yaml
metadata:
  annotations:
    fluence.flux-framework.org/coordination: shared      # or: independent
```

| mode | meaning | who submits | gating | app runs | real submits |
|------|---------|-------------|--------|----------|--------------|
| `independent` (default) | N pods each do their own quantum work | every pod | none possible (all are producers) | N | N |
| `shared` | one task; N pods consume the result | producer only | consumers gated until task ready | N | 1 |

`coordination` is an open enum so future designs (e.g. `scatter` — index-paired
task↔pod, §6.2 of the quantum doc) slot in as new modes without changing the
mechanism. Default is `independent`: never invent coordination the user did not
ask for, and never dedup tasks that were meant to be distinct.

### What each mode does to resources, honestly

- **shared**: the producer (1 node) holds its node through the queue wait;
  consumers (N−1) consume **zero** node resources while gated, then start at
  position==1. Idle cost during the wait ≈ 1 node, vs N for a traditional gang.
- **independent**: every pod is a producer, so every pod holds its node through
  its own queue wait — N nodes idle. There is nothing to coordinate (no shared
  result), so this is not a Fluence deficiency; it is the physics of "N
  independent tasks," and it is the user's explicit choice. The only way to
  reclaim even the producer's node in either mode is a resumable `.result()`
  (replay), which reuses the faux mechanism and is deliberately **out of scope
  for v1** (one idle node is cheap; replay imposes a replay-safe-code contract).

## Producer election

Exactly one member must be the producer. Election is deterministic for the
recommended workload and best-effort otherwise:

- **Indexed Job (recommended):** the pod carries
  `batch.kubernetes.io/job-completion-index`. **Index `0` is the producer**;
  every other index is a consumer. Deterministic, race-free, no recorded state —
  the controller already stamped the index, and identical templates yield
  differentiated behavior purely from it. This is why an indexed Job is the right
  shape and is what the experiments use.
- **Non-indexed gang (Deployment / raw grouped pods):** first arrival claims the
  producer slot by creating the producer PodGroup (create-if-absent); later pods
  find it present and become consumers. Best-effort (racy under simultaneous
  admission); documented, with indexed Job recommended for determinism.

## The two-group split

| | producer (index 0) | consumers (indices 1..N−1) |
|---|---|---|
| PodGroup | `<group>-producer`, `minCount=1` | `<group>`, `minCount=N−1` |
| schedules | immediately, alone | atomically as a gang, **after ungate** |
| gate | none | `quantum.braket/ready` + preempting priority |
| interceptor | staged, **real (tag) mode** | staged, **faux mode** |
| sidecar | yes — polls the task, ungates `<group>` at position==1 | no |
| app run | full; its `.run()` is the one real submit | full; its `.run()` is a faux no-op returning the producer's task |

`minCount=1` on the producer group is what removes the deadlock that forced a
separate submitter: a single-member group schedules alone, so the producer runs
during the wait while the `minCount=N−1` consumer group sits gated. The two
groups have independent minCounts; neither blocks the other. The consumer group
keeps a real gang `minCount` (N−1), so **gang scheduling is preserved and
demonstrable** (experiment requirement 1).

The faux path is retained verbatim, but its meaning is now honest: it is the
shared-result dedup. A consumer runs the same image and calls `.run()`; the faux
interceptor returns the producer's existing task (handed over as
`FLUENCE_QUANTUM_JOB_ID`, stamped by the sidecar at ungate) instead of
submitting. One real task, N consumers, each app run once, in full.

## Gate / ungate flow (shared mode)

```
1. Producer (index 0) admitted -> own group-of-one, ungated, sidecar attached
   (FLUENCE_GANG_GROUP=<group>), interceptor in REAL mode.
   Consumers (1..N-1) admitted -> group <group> (minCount N-1), GATED, faux mode,
   depends-on producer=<group>-producer.

2. Scheduler places the producer immediately (minCount=1). It runs the user app,
   .run() submits the ONE real task (tagged fluence-pod-uid).

3. Producer sidecar discovers the task by tag, polls queue position.

4. At position==1 (or RUNNING): for each gated pod in <group>:
     annotate fluence.flux-framework.org/quantum-job-id=<task id>
     remove the quantum.braket/ready gate (priority already set at admission)

5. Consumer group (now ungated, minCount N-1) gang-schedules atomically and
   starts as the quantum result arrives. Each consumer's .run() is faux: returns
   the producer's task; .result() returns the shared result; app post-processes.
```

`independent` mode skips all of this: each pod is its own group-of-one, ungated,
real submit, optional observe-only sidecar — i.e. today's standalone path applied
per pod.

---

## Patch

All changes are in `pkg/webhook/handlers/`. The webhook core, the `fluxion`
handler, `dependency.go`, `sidecar.go`, and the Python interceptor/sidecar are
**unchanged**.

### `gang.go` — defer on quantum pods (removes the ordering dependency)

The gang handler currently calls `EnsurePodGroup` unconditionally and relies on
idempotency to coexist with the quantum handler. With the two-group split the
quantum handler owns *both* quantum PodGroups (and the producer's group differs
from its admission-time label), so the gang handler must not also gang quantum
pods. Make it skip them:

```go
func (h *gangHandler) Applies(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod) bool {
	// Classical gangs only. A pod that requests the quantum resource is gang-
	// scheduled by the quantum handler (which owns the producer/consumer split);
	// handling it here too would create a second, conflicting PodGroup.
	if spec.PodRequestsResource(pod, QuantumResource) {
		return false
	}
	return webhook.GroupName(pod) != ""
}
```

### `quantum.go` — replace `Mutate` and the submitter machinery

**Add** these constants (near the existing const block):

```go
const (
	// CoordinationAnnotation selects how a quantum gang is coordinated. Open enum
	// so new designs (e.g. "scatter") add a mode without changing the mechanism.
	CoordinationAnnotation = "fluence.flux-framework.org/coordination"
	// CoordinationShared: one real task; the producer (index 0) submits, the
	// other members are gated consumers that dedup to the producer's task.
	CoordinationShared = "shared"
	// CoordinationIndependent (default): every member does its own quantum work;
	// no coordination, no gating, each holds its node through its own queue wait.
	CoordinationIndependent = "independent"

	// ProducerGroupSuffix names the producer's own group-of-one: <group>-producer
	// (minCount 1) so it schedules alone and never deadlocks against the gated
	// consumer gang.
	ProducerGroupSuffix = "-producer"

	// CompletionIndexAnnotation is the indexed-Job completion index the Job
	// controller stamps on each pod; index "0" is the producer (deterministic
	// election, no recorded state).
	CompletionIndexAnnotation = "batch.kubernetes.io/job-completion-index"
	// ProducerIndex is the completion index promoted to producer.
	ProducerIndex = "0"
)
```

Keep `GangGroupEnv` (`FLUENCE_GANG_GROUP`) — it now tells the **producer's**
sidecar which consumer group to ungate. **Delete** the separate-submitter
constants and helpers: `SubmitterAnnotation`, `GangGroupAnnotation`,
`SubmitterGroupSuffix`, `SubmitterPodSuffix`, and the functions
`mutateSubmitter` and `ensureSubmitterPod`. Everything else in the file
(`resolveGroup`, `resolveGangSize`, `ownerReplicaSetN`, `countGroupPods`,
`linkGroupOps`, the faux-submit section, the sidecar section) is reused unchanged.

**Replace** `Mutate` with the coordination router plus two small role functions:

```go
func (h *quantumHandler) Mutate(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod) []spec.Op {
	g := resolveGroup(pod)
	n := resolveGangSize(ctx, m, pod, g)
	mode := coordinationMode(pod)
	observe := spec.Label(pod, ObserveLabel) == "true"

	// No coordination: a standalone quantum pod, or an explicitly independent
	// member. The REAL submit happens in THIS pod; sidecar only for observe-only
	// telemetry. (independent mode routes every member here -> N standalone
	// producers, each owning its task and its own queue wait.)
	if mode != CoordinationShared || g == "" || n <= 1 {
		ops := interceptorOps(pod)
		if observe {
			sc := sidecarFor(m)
			sc.EnsureRBAC(ctx, pod.Namespace)
			ops = append(ops, sc.ContainerOps(pod, true, nil)...)
		}
		log.Printf("[fluence-webhook] quantum %s/%s mode=%s (standalone/independent, observe=%v)",
			pod.Namespace, pod.Name, mode, observe)
		return ops
	}

	// shared mode: promote one member to producer; the rest are gated consumers.
	if isProducer(ctx, m, pod, g) {
		return h.mutateProducer(ctx, m, pod, g)
	}
	return h.mutateConsumer(ctx, m, pod, g, n)
}

// mutateProducer: index-0 member. Its own group-of-one (minCount 1) so it
// schedules alone and runs the REAL submit; sidecar polls the task and ungates
// the consumer group. NOT gated, no faux. The producer is one of the N members,
// so the application is NOT run an extra time.
func (h *quantumHandler) mutateProducer(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod, group string) []spec.Op {
	pg := group + ProducerGroupSuffix
	m.EnsurePodGroup(ctx, pod.Namespace, pg, pod.Name, 1)
	ops := linkGroupOps(pod, pg)
	ops = append(ops, interceptorOps(pod)...) // real (tag) mode — no fauxSubmitEnvOps
	sc := sidecarFor(m)
	sc.EnsureRBAC(ctx, pod.Namespace)
	// Tell the sidecar which consumer group (the base group) to list + ungate.
	ops = append(ops, sc.ContainerOps(pod, false, []corev1.EnvVar{{Name: GangGroupEnv, Value: group}})...)
	log.Printf("[fluence-webhook] quantum producer %s/%s — group %s (ungates %q)",
		pod.Namespace, pod.Name, pg, group)
	return ops
}

// mutateConsumer: a non-producer member. Joins the <group> consumer gang
// (minCount N-1), is gated until the producer's task is ready, and runs the same
// image with the interceptor in FAUX mode so its .run() returns the producer's
// task instead of resubmitting (the shared-result dedup).
func (h *quantumHandler) mutateConsumer(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod, group string, n int32) []spec.Op {
	m.EnsurePodGroup(ctx, pod.Namespace, group, pod.Name, n-1)
	ops := linkGroupOps(pod, group)
	dep := Dependency{Kind: DependencyKindQuantumSubmit, Producer: group + ProducerGroupSuffix, Gate: QuantumGate}
	ops = append(ops, dep.applyOps(pod)...) // gate + preempting priority + depends-on
	ops = append(ops, interceptorOps(pod)...)
	ops = append(ops, fauxSubmitEnvOps(pod)...)
	log.Printf("[fluence-webhook] quantum consumer %s/%s — group %s minCount=%d, gated+faux",
		pod.Namespace, pod.Name, group, n-1)
	return ops
}

// coordinationMode reads the coordination annotation; default independent.
func coordinationMode(pod *corev1.Pod) string {
	if v := spec.Annotation(pod, CoordinationAnnotation); v != "" {
		return v
	}
	return CoordinationIndependent
}

// isProducer decides whether THIS pod is the gang's single producer. Indexed Job
// (recommended): completion index 0 is the producer — deterministic, race-free.
// Otherwise: first arrival claims the producer slot by the absence of the
// producer PodGroup (best-effort; prefer an indexed Job).
func isProducer(ctx context.Context, m webhook.MutatorAPI, pod *corev1.Pod, group string) bool {
	if idx, ok := pod.Annotations[CompletionIndexAnnotation]; ok {
		return idx == ProducerIndex
	}
	c := m.Client()
	if c == nil {
		return true // tests / no client: treat as producer
	}
	pg := group + ProducerGroupSuffix
	if _, err := c.SchedulingV1alpha2().PodGroups(pod.Namespace).Get(ctx, pg, metav1.GetOptions{}); err == nil {
		return false // already claimed by an earlier arrival
	}
	return true
}
```

Note: `pod.Annotations` may be nil; the `idx, ok := pod.Annotations[...]` form is
nil-safe in Go (indexing a nil map yields the zero value, `ok=false`).

### Sidecar (Python) — no change

The producer's sidecar already resolves the vendor at runtime from
`FLUXION_BACKEND`, discovers the task by the `fluence-pod-uid` tag, polls queue
position, and ungates the group named by `FLUENCE_GANG_GROUP` (now the consumer
group) at position==1, stamping `quantum-job-id` on each consumer before removing
its gate. That is exactly the existing flow with the producer in place of the
submitter pod.

---

## Experiments

Two requirements, both demonstrable on a kind cluster with the mock/faux path and
on a real cluster with Braket.

### Requirement 1 — Fluence still gang-schedules

Unchanged classical-gang coverage plus a shared-mode assertion:

- **Classical gang (regression):** keep `test/e2e/gang/*`. A `minCount=N` classical
  PodGroup schedules all-or-nothing. This proves the generic gang machinery is
  intact (the change only adds a quantum-pod deferral to `gang.Applies`).
- **Shared consumer gang (new assertion):** submit a `coordination: shared`
  indexed Job of N. Assert: exactly one `<job>-producer` PodGroup (minCount 1) and
  one `<job>` PodGroup (minCount N−1); the producer runs while the N−1 consumers
  are `SchedulingGated`; after ungate the N−1 schedule **together** (gang), not
  one-by-one. This proves gang scheduling still holds for the consumer group.

### Requirement 2 — Both modes work and shared beats a traditional gang

The metric that isolates the win is **classical node-seconds consumed during the
quantum queue wait** (lower is better), alongside correctness checks.

Three arms, same N, same workload (the QAOA sampler), same backend:

| arm | how | expected node-seconds during queue wait | correctness |
|-----|-----|------------------------------------------|-------------|
| **traditional gang** (baseline) | N pods all running, each waits the full queue (no Fluence coordination — e.g. a plain native gang, or `independent` with N=N) | ≈ **N × T_queue** | N pods each run; if they each submit, N real tasks |
| **shared** (new) | `coordination: shared` indexed Job, N pods | ≈ **1 × T_queue** (producer only; consumers gated) | **1** real task; all N pods produce the **same** result; app runs N times, never N+1 |
| **independent** (new) | `coordination: independent` indexed Job, N pods | ≈ **N × T_queue** (no coordination possible) | N distinct tasks/results; correct and the user's explicit choice (reported as the honest baseline, **not** claimed as an improvement) |

Headline comparison is **shared vs traditional**: same observable result to the N
pods, but shared idles ~1 node through the queue wait instead of N, saving
≈ (N−1) × T_queue node-seconds, and runs the application N times rather than N+1
(the submitter-pod model's extra run is gone).

Instrumentation (reuse the Experiment 2 harness):
- per-pod `TIMING` lines → derive each pod's gated interval vs running interval;
  sum running-but-pre-result node-seconds per arm.
- producer's sidecar logs queue position over time → T_queue.
- assert real-submit count: shared = 1 (one tagged task on the backend),
  independent/traditional = N (count tagged tasks).
- assert shared correctness: all N pods log the **same** task id / result hash.

Suggested location: a new `experiments/4-coordination/` modeled on
`experiments/2-gang/` (it already measures idle reclamation), parameterized by the
`coordination` annotation and N, emitting node-seconds-during-wait, real-submit
count, and result-agreement per arm. Plot node-seconds vs N for the three arms:
traditional and independent rise ~linearly in N; shared stays ~flat at one node.

### Build/run notes

- The producer/consumer split needs no new image: producers and consumers run the
  same sampler image; faux vs real is selected by `FLUENCE_FAUX_SUBMIT` (set on
  consumers by `fauxSubmitEnvOps`), exactly as today.
- Use an **indexed** Job (`completionMode: Indexed`, `parallelism == completions == N`)
  so producer election is deterministic (index 0) and `resolveGangSize` reads N
  from the owner. Stamp `fluence.flux-framework.org/coordination` in the pod
  template's annotations.
- kind/mock runs exercise the structural assertions (groups, gating, ungate
  ordering) without a backend; real-Braket runs add the node-seconds and
  real-submit-count measurements.
