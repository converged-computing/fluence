# Quantum-Classical Scheduling Coordination in Fluence

## Abstract

Hybrid quantum-classical workflows submit work to two independent queues:
the Kubernetes scheduler (classical compute) and a QPU vendor API (quantum
execution). Classical pods waste node resources while waiting for QPU queue
results. Fluence's coordination system thus gates classical worker pods until 
the QPU task is one position from executing, then releases them with high 
priority so they preempt lower-priority work and start immediately as the 
QPU result arrives. Yes, it could be the case the one task in the queue before
it takes a long time, but I think this is an improved approach than having worker
pods running (and waiting) for a much longer queue. This only is important
given that you have gangs, or leader worker designs where some leader is launching
the quantum work and otherwise the workers would be waiting and doing nothing
(and wasting resources).

## 1. The Two-Queue Problem

When a hybrid quantum-classical job runs on Kubernetes, the classical pod
starts immediately and blocks waiting for the QPU result. The QPU task
enters a vendor-managed queue shared across all users. The classical pod
consumes node resources — CPU, memory, potentially GPU — for the entire
duration of the QPU queue wait, which may be minutes to hours on real
hardware.

This waste scales with concurrency. With N concurrent hybrid jobs, each pod
idles for the full QPU queue wait. On real QPU backends (IQM Garnet, IQM
Emerald) we measure 15–30% classical idle fraction at N=10, rising to over
70% for individual pods at N=20. Wall time scales linearly with concurrency
on real QPUs — submitting 20 jobs takes 5–8× longer than 1 job due to
self-imposed queue depth.

## 2. Why Existing Mechanisms Are Insufficient

### 2.1 Fluxion reservations

Fluxion's backfill reservation policies (EASY, Conservative, Hybrid) compute
a future `time_at` from the internal resource graph — when currently running
classical jobs will finish. They cannot accept an externally-supplied time
derived from a vendor queue. Without a reliable `time_at`, a reservation
degenerates to a pending job. All reservations are cancelled and recomputed
from scratch at the start of every scheduling loop.

QPU queue time is unknowable in advance. It depends on other
users' submissions, hardware calibration windows, and network latency.
Average task time per QPU cannot be estimated reliably. Therefore Fluxion
reservations cannot help with the two-queue problem. I learned that we are
working on "advanced reservations" that are more like a hold, but it is
not clear if that can be merged soon.

### 2.2 Scheduling gates alone

A scheduling gate holds a pod out of the scheduling queue entirely, consuming
no node resources. But ungating N pods simultaneously on a busy cluster
creates a race — resources may not be available when ungating occurs, and
the Fluxion graph allocation happens after ungating, not before. Without
priority, ungated pods compete equally with all pending work.

### 2.3 Preemption alone

Submitting classical pods with a high PriorityClass causes Kubernetes to
evict lower-priority pods immediately at submit time — during the entire QPU
queue wait — which is worse than the original problem.

## 3. Design

The design combines four mechanisms:

1. **SDK interceptor** — tags every QPU task with the pod UID
2. **Fluence webhook** — gates worker pods, injects sidecar into quantum pods
3. **Sidecar controller** — discovers the QPU task, polls queue position,
   ungates workers when position==1
4. **High-priority ungating** — workers preempt lower-priority work at the
   last responsible moment

### 3.0 When Fluence acts: the decision matrix

Two orthogonal properties of a pod admitted with `schedulerName: fluence`
determine what Fluence does:

- **Q (quantum?)** — does any container request a quantum resource
  (`fluxion.flux-framework.org/qpu`)? If so, Fluence is scheduling the quantum
  work and there is a vendor backend behind it.
- **G (gang?)** — does the pod carry `fluence.flux-framework.org/group`?

|              | not quantum            | quantum                                                        |
|--------------|------------------------|----------------------------------------------------------------|
| **not gang** | group of 1 (nothing)   | inject provider interceptor + env; **sidecar only in observe-only mode if telemetry requested** (no workers to ungate) |
| **gang**     | gang-schedule only     | leader: interceptor + env + sidecar (gates + ungates workers); workers: gate only |

The crucial rule: **sidecar/interceptor injection is triggered by the quantum
resource request, not the group label.** The group label only controls gang
scheduling and worker gating. A group leader that requests no quantum resource
(e.g. a classical pod that happens to set `BRAKET_DEVICE` itself) is just
gang-scheduled — Fluence injects no sidecar, because there is no quantum work
for it to coordinate. `BRAKET_DEVICE` (or any direct device selection by the
user) is the signal that Fluence is *not* scheduling the quantum resource;
`fluxion.flux-framework.org/qpu` is the signal that it is.

### 3.1 User interface

The user labels all pods in a workflow group with:

```yaml
metadata:
  labels:
    fluence.flux-framework.org/group: my-workflow
spec:
  schedulerName: fluence
```

I initially started with having the user create a PodGroup object, and I found
that annoying. I do not want to require a PodGroup object when an annotation is easier,
and then I have fine-grained control of what the groups looks like. Fluence can handle
everything else automatically.

The namespace distinction:
- `fluence.flux-framework.org/*` — Fluence scheduler-plugin concerns
  (group label, leader annotation, gate name)
- `fluxion.flux-framework.org/*` — Fluxion resource-graph concerns
  (extended resource types, backend attribute env vars)

### 3.2 Webhook behavior

When the Fluence mutating webhook sees a pod with `schedulerName: fluence`
and `fluence.flux-framework.org/group=<name>`:

**First pod admitted (leader):**
1. Creates a PodGroup with `minCount: 1` — Fluence owns this PodGroup,
   the user never creates it. `minCount: 1` means the leader schedules
   immediately without waiting for gated workers. The assumption here is
   that this leader is going to submit the quantum work.
2. Records the leader pod name on the PodGroup via `QuantumLeaderAnnotation`.
3. Stamps `spec.schedulingGroup.podGroupName=<name>` on the pod. This is the
   native 1.36 field the scheduler reads to gang the group; the user only
   ever sets the LABEL, and the webhook translates it into the spec field so
   the user never has to create a PodGroup or know it exists.
4. Creates per-namespace RBAC: `fluence-sidecar` ServiceAccount, Role
   (patch pods, list PodGroups), RoleBinding.
5. Copies the `fluence-quantum-interceptor` ConfigMap from `kube-system` into
   the pod's namespace (ConfigMap volumes require same-namespace source). This
   one ConfigMap carries every provider's interceptor block; see §3.7.
6. Injects `fluence-sidecar` container into the leader pod.
7. Injects `FLUENCE_POD_UID` env var (downward API from `metadata.uid`).
8. Mounts the interceptor ConfigMap and sets `PYTHONSTARTUP` env var so
   the interceptor runs automatically before user code.
9. Sets `serviceAccountName: fluence-sidecar`.

**Subsequent pods (workers):**
1. Reads the PodGroup leader annotation — retries up to 3× with 100ms
   delay to handle concurrent admission race.
2. Stamps `spec.schedulingGroup.podGroupName=<name>` on the pod, linking it
   to the same PodGroup as the leader. The gate (next step) holds the worker
   at PreEnqueue, so the scheduler does not consider it — and the scheduler's
   `groupPods` excludes gated pods — until the sidecar ungates it. The
   linkage only takes effect once the worker is ungated.
3. Adds `quantum.braket/ready` scheduling gate — pod enters
   `SchedulingGated` state, invisible to Fluxion, consuming no resources.

### 3.3 Braket SDK interceptor

I created a consistent sidecar that is going to monitor the queue, and be able
to ungate the worker pods when the task submit by our pod is at position 1
(implicating it will run soon, and we assume the user wants the classical
gang to run at the same time or slightly sooner). Note that it is up to the
user application to orchestrate the leader and workers, and coordination
of the quantum results. A few examples: 

- The worker pods are guaranteed to get an ARN for where the Braket results are in S3, 
  and this is ensured by the sidecar. So a reasonable approach is for workers query 
  that bucket looking for a finished marker.  This would not require coordination from
  the leader.
- Given communication from the leader to workers, the leader can tell them exactly
  when the work is finished, and coordinate what they do with results.

I ran into the issue of needing to GET the task id from the primary pod from
the sidecar. What I decided on is a very simply injection - the call of the
script to submit the job can take arbitrary tags, and so I wrap that with a configmap
that is in the pythonpath, and ensure the task is tagged with a pod specific UID
that the sidecar also knows. More specifically, `fluence_braket_intercept.py` script is 
mounted via `PYTHONSTARTUP` into every container in the leader pod. It monkey-patches 
`AwsDevice.run()` to automatically tag every quantum task submission with `FLUENCE_POD_UID`:

```python
def _patched_run(self, task_specification, *args, **kwargs):
    pod_uid = os.environ.get("FLUENCE_POD_UID", "")
    if pod_uid:
        tags = kwargs.get("tags", {})
        tags["fluence-pod-uid"] = pod_uid
        kwargs["tags"] = tags
    return _original_run(self, task_specification, *args, **kwargs)
```

This is completely transparent to the user application. No code changes
are required.

### 3.4 Sidecar controller

The `fluence-sidecar` container runs alongside the user application in the
leader pod, sharing its AWS credentials and network namespace.

```console
1. READ  FLUXION_ARN, FLUENCE_POD_UID from env

2. DISCOVER task by tag:
   search_quantum_tasks(filters=[
     deviceArn == FLUXION_ARN,
     tags:fluence-pod-uid == FLUENCE_POD_UID
   ])
   Poll every 10s, timeout after 300s.
   On timeout: fall back to time-window heuristic (tasks submitted
   after pod start time on the same device).

3. DISCOVER worker pods:
   List pods in namespace with fluence.flux-framework.org/group label
   matching this pod's group, having quantum.braket/ready gate present.

4. POLL  task.queue_position() every 30s.
   Log position for experiment instrumentation.

5. WHEN  position == "1" OR state == RUNNING:
   For each worker pod:
     kubectl annotate pod <name> braket.quantum/task-arn=<arn>
     kubectl patch pod <name> --type=json \
       -p='[{"op":"add","path":"/spec/priorityClassName",
             "value":"fluence-quantum-classical"},
            {"op":"remove","path":"/spec/schedulingGates/0"}]'

6. EXIT
```

The priority class and gate removal are applied atomically in one patch.
This ensures workers enter the scheduling queue with high priority
immediately, without a window where they are ungated but low-priority.

### 3.5 Priority and preemption

The `fluence-quantum-classical` PriorityClass (value: 1,000,000) is applied
by the sidecar at ungate time, not by the webhook at pod creation. Setting
it at creation time causes an admission controller conflict (priority integer
already defaulted to 0).

When workers are ungated with high priority, Kubernetes preemption evicts
lower-priority pods to make room. Fluence's pod deletion informer catches
these evictions, calls `Cancel(jobid)` in Fluxion, and frees the graph
vertices so Fluxion can allocate them to the incoming high-priority workers.

### 3.6 Classical allocation follows quantum execution order

Because each workflow's gate is removed independently when its QPU task
reaches position==1, workflows whose QPU tasks execute earlier get classical
resources earlier — regardless of submission order. A workflow submitted to
a quiet backend gets its classical resources before one submitted earlier to
a busy backend. This aligns classical resource allocation with actual quantum
execution order across heterogeneous backends.

### 3.7 Provider interface

The webhook is provider-agnostic. It cannot know the vendor at admission time,
because the scheduler may choose a backend from a mixed set of vendors — the
vendor is only fixed once the scheduler writes the backend annotation in
PreBind, after the webhook has run. The design therefore splits by which
artifact needs the vendor and when:

- **Interceptor** — runs in the user's application container and must be mounted
  at admission, before the vendor is known. It is a single all-providers file
  assembled from each provider's `interceptor.py` block (see
  `sidecars/build-interceptor.sh`). Each block fail-soft skips if its vendor SDK
  is not importable in that container, so the one mounted file works in any
  quantum container regardless of which SDK is present. This is *forced* by
  mixed-vendor scheduling: the webhook cannot pick a single provider's
  interceptor at mount time. One source ConfigMap in `kube-system`
  (`fluence-quantum-interceptor`), copied per-namespace by the webhook.

- **Sidecar** — runs in the Fluence sidecar image and resolves the vendor at
  *runtime* from the backend annotation (`FLUXION_BACKEND`). It loads only the
  one provider module that matches, so an unrelated provider's SDK failure never
  affects the run.

Every provider implements a common interface (`sidecars/lib/provider.py`):

```
Provider:
    name                          # "braket", "ibm", ...
    matches(vendor, backend)      # runtime resolution from backend annotation
    find_my_task(pod_uid, ...)    # search by the fluence-pod-uid tag → opaque Task
    is_ready_to_ungate(task)      # decision primitive: position==1 OR running
    queue_position(task)          # optional richer telemetry; None if unavailable
    job_id(task)                  # cross-vendor id handed to workers (NOT the ARN)
```

Vendor-specific identifiers (a Braket task ARN, an IBM job id, a GCP operation
name) are never named in the interface — they live behind the opaque `Task` and
are surfaced generically through `job_id()`. The interceptor and the provider's
`find_my_task` are joined only by a shared tag convention (`fluence-pod-uid`),
not by shared code: the interceptor stamps the tag at submission, `find_my_task`
searches for it.

`is_ready_to_ungate` is the decision primitive and is always implementable, even
for vendors that do not expose a numeric queue position (it can key off the
QUEUED→RUNNING transition). `queue_position` is the optional richer signal used
for observe-only telemetry and position-series logging.

Adding a vendor is one folder under `sidecars/providers/<name>/`: a
`sidecar_provider.py` implementing the interface and an `interceptor.py` block.
The build wires both in — the interceptor block is concatenated into the single
mounted file, and the provider module is registered for runtime resolution.

#### Observe-only (telemetry) mode

A quantum pod that is *not* a gang (a single quantum pod, no workers to ungate)
gets the interceptor and env only — no sidecar — by default, so no surprise
machinery is injected. Telemetry is opt-in via the label
`fluence.flux-framework.org/observe: "true"`, surfaced to the sidecar as
`FLUENCE_OBSERVE=true`. In observe-only mode the sidecar discovers the task and
polls `queue_position`, logging the series for measurement, but ungates nothing.
Experiments use this to get a uniform queue-position measurement path across
singleton and gang runs.

## 4. Properties

| Property | Value |
|---|---|
| User code changes required | None |
| User manifest changes required | Add group label + schedulerName |
| Classical resources during QPU wait | Zero (SchedulingGated) |
| QPU queue time estimation needed | No — position==1 is observable |
| Works across heterogeneous backends | Yes — any backend in Fluxion graph |
| Multi-vendor | Yes — provider interface, vendor resolved at runtime |
| Vendor API cooperation needed | No — SDK interceptor handles tagging |

## 5. Limitations

### 5.1 Preemption disrupts lower-priority work

At position==1, workers preempt running lower-priority pods. This work is
re-queued and eventually runs, but there is a disruption cost. A future
design using a `MatchReserveAt(time_at, spec)` Fluxion primitive — where
`time_at` is supplied by the QPU vendor via an ETA or task-start event —
would allow graceful node draining instead of preemption. No current QPU
vendor exposes such an API.

### 5.2 Provider coverage

The provider interface (§3.7) makes adding a vendor a matter of implementing
`find_my_task`/`is_ready_to_ungate`/`job_id` and an interceptor block. Braket is
implemented. IBM Qiskit Runtime is a natural next provider — it supports
submit-time `job_tags` and tag-based filtering via `QiskitRuntimeService.jobs`,
so both halves of the mechanism are expressible. Vendors whose APIs expose
neither tag-search nor a queue position would need a fallback discovery
heuristic (e.g. a time window) rather than the tag mechanism.

### 5.3 Single task per workflow

The sidecar tracks one QPU task ARN per leader pod. Parameter-shift gradient
estimation and other multi-circuit workflows require tracking a set of ARNs.
See the scatter design issue for the proposed extension.

### 5.4 Namespace-scoped RBAC

The webhook creates `fluence-sidecar` RBAC in each namespace on first use.
This is correct behavior — the sidecar only needs permissions in its own
namespace. A Helm chart or operator would manage this more cleanly.

## 6. Future Work

### 6.1 MatchReserveAt Fluxion primitive

A new `MatchReserveAt(time_at, spec)` function in the Fluxion Go bindings
would allow an externally-supplied reservation time. The sidecar would feed
live QPU queue position into this estimate, enabling graceful node draining
rather than preemption. This requires the C++ reapi `match_allocate_multi`
function to be exposed through the Go bindings with a `starttime` parameter.

### 6.2 Scatter design

For workflows with N independent QPU tasks each paired with one classical
pod, an index-based pairing mechanism (`fluence.flux-framework.org/index`)
would allow the sidecar to ungate specific worker pods when their specific
task reaches position==1. See the open scatter design issue.

### 6.3 Vendor task-start events

If QPU vendors exposed SNS/EventBridge notifications when a task transitions
from QUEUED to RUNNING, the sidecar could react to events rather than
polling. This would eliminate the 30s polling latency and enable more
precise ungating.

### 6.4 PostFilter topology-aware preemption

A custom Fluence `PostFilter` plugin would ask Fluxion which graph vertices
are blocking a high-priority worker pod, then target preemption at exactly
those pods — rather than the default Kubernetes preemption which picks
lowest-priority pods regardless of graph topology. This ensures preemption
always produces a valid Fluxion allocation.
