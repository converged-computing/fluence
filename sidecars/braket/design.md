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
2. **Fluence webhook** — gates worker pods, injects sidecar into leader
3. **Sidecar controller** — discovers the QPU task, polls queue position,
   ungates workers when position==1
4. **High-priority ungating** — workers preempt lower-priority work at the
   last responsible moment

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
3. Creates per-namespace RBAC: `fluence-sidecar` ServiceAccount, Role
   (patch pods, list PodGroups), RoleBinding.
4. Copies `fluence-braket-interceptor` ConfigMap from `kube-system` into
   the pod's namespace (ConfigMap volumes require same-namespace source).
5. Injects `fluence-sidecar` container into the leader pod.
6. Injects `FLUENCE_POD_UID` env var (downward API from `metadata.uid`).
7. Mounts the interceptor ConfigMap and sets `PYTHONSTARTUP` env var so
   the interceptor runs automatically before user code.
8. Sets `serviceAccountName: fluence-sidecar`.

**Subsequent pods (workers):**
1. Reads the PodGroup leader annotation — retries up to 3× with 100ms
   delay to handle concurrent admission race.
2. Adds `quantum.braket/ready` scheduling gate — pod enters
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

## 4. Properties

| Property | Value |
|---|---|
| User code changes required | None |
| User manifest changes required | Add group label + schedulerName |
| Classical resources during QPU wait | Zero (SchedulingGated) |
| QPU queue time estimation needed | No — position==1 is observable |
| Works across heterogeneous backends | Yes — any backend in Fluxion graph |
| Vendor API cooperation needed | No — SDK interceptor handles tagging |

## 5. Limitations

### 5.1 Preemption disrupts lower-priority work

At position==1, workers preempt running lower-priority pods. This work is
re-queued and eventually runs, but there is a disruption cost. A future
design using a `MatchReserveAt(time_at, spec)` Fluxion primitive — where
`time_at` is supplied by the QPU vendor via an ETA or task-start event —
would allow graceful node draining instead of preemption. No current QPU
vendor exposes such an API.

### 5.2 Non-Braket SDKs

The interceptor patches `AwsDevice.run()`. IBM Qiskit Runtime, IQM native
SDK, and other vendors require separate interceptors in `sidecars/<vendor>/`.
The pattern is identical; only the SDK entry point differs. We will make
sidecars for different vendor interfaces.

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
