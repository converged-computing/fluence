# Quantum-Classical Scheduling Coordination in Fluence

## Abstract

Hybrid quantum-classical workflows submit work to two independent queues:
the Kubernetes scheduler (classical compute) and a QPU vendor API (quantum
execution). Classical pods waste node resources while waiting for QPU queue
results. We describe a design for Fluence that coordinates classical resource
allocation with quantum execution order across heterogeneous QPU backends,
without requiring any user application changes.

## 1. The Two-Queue Problem

When a hybrid quantum-classical job runs on Kubernetes, the classical pod
starts immediately and blocks waiting for the QPU result. The QPU task
enters a vendor-managed queue shared across all users. The classical pod
consumes node resources — CPU, memory, potentially GPU — for the entire
duration of the QPU queue wait, which may be minutes to hours on real
hardware.

This waste scales with concurrency. With N concurrent hybrid jobs and a
QPU queue depth of D, each classical pod may idle for D × t_avg seconds
where t_avg is the average QPU task execution time. On a shared cluster
with expensive GPU nodes this is a significant and unfair resource waste.

The problem has two components:

**Component 1 — Resource waste.** Classical pods consume node resources
while doing nothing useful.

**Component 2 — Ordering mismatch.** Classical resource allocation follows
job submission order, not QPU execution order. A job submitted to a busy
backend wastes resources longer than a job submitted to a quiet one.

## 2. Why Existing Mechanisms Don't Help

### 2.1 Fluxion reservations

Fluxion's backfill reservation policies (EASY, Conservative, Hybrid) compute
a future `time_at` from the internal resource graph timeline — when currently
running classical jobs will finish. They have no mechanism to accept an
externally-supplied time derived from a vendor queue. Without a reliable
`time_at`, a reservation degenerates to a pending job. Furthermore, all
reservations are cancelled and recomputed from scratch at the start of every
scheduling loop, so they provide no persistent resource hold.

### 2.2 Kubernetes scheduling gates alone

A scheduling gate holds a pod out of the scheduling queue entirely, consuming
no node resources. But ungating N pods simultaneously on a busy cluster
creates a race — resources may not be available, and the graph allocation
happens after ungating, not before. There is no atomicity guarantee between
ungating and placement.

### 2.3 Preemption alone

Submitting classical pods with a high `PriorityClass` causes Kubernetes to
evict lower-priority pods to make room. But without a gate, preemption
happens immediately at submit time — the classical pods displace other work
during the entire QPU queue wait, which is worse than the original problem.

## 3. Design

The design combines three mechanisms: a **transparent SDK interceptor**
injected by the Fluence webhook, a **sidecar controller** that observes
QPU queue state, and **gated high-priority classical pods** that are
allocated and dispatched only when the QPU is one position from executing.

### 3.1 Components

```
┌─────────────────────────────────────────────────────────┐
│ Quantum gateway pod                                      │
│                                                          │
│  ┌─────────────────────┐  ┌──────────────────────────┐  │
│  │  user application   │  │   fluence-sidecar        │  │
│  │                     │  │                          │  │
│  │  device.run(...)    │  │  1. find task by tag     │  │
│  │  ↓ (intercepted)    │  │  2. poll queue_position  │  │
│  │  tags injected      │  │  3. at position==1:      │  │
│  │  automatically      │  │     patch ARN → pods     │  │
│  │                     │  │     remove gates         │  │
│  └─────────────────────┘  └──────────────────────────┘  │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│ Classical pods (SchedulingGated until position==1)       │
│                                                          │
│  annotations:                                            │
│    braket.quantum/task-arn: <patched by sidecar>         │
│  schedulingGates:                                        │
│    - name: quantum.braket/ready  ← removed by sidecar   │
│  priorityClassName: quantum-classical-high               │
└─────────────────────────────────────────────────────────┘
```

### 3.2 Transparent SDK interceptor

The Fluence mutating webhook injects two things into every pod that requests
a QPU resource (`fluxion.flux-framework.org/qpu`):

**Environment variable:**
```
FLUENCE_POD_UID=<pod.metadata.uid>
```

**Python sitecustomize hook** (injected as a ConfigMap mounted at the
Python site-packages path):

```python
# fluence_braket_intercept.py — injected by Fluence webhook
import os
from braket.aws import AwsDevice

_original_run = AwsDevice.run

def _patched_run(self, task_specification, *args, **kwargs):
    pod_uid = os.environ.get("FLUENCE_POD_UID", "")
    if pod_uid:
        tags = kwargs.get("tags", {})
        tags["fluence-pod-uid"] = pod_uid
        kwargs["tags"] = tags
    return _original_run(self, task_specification, *args, **kwargs)

AwsDevice.run = _patched_run
```

This is completely transparent to the user application. Every `device.run()`
call — regardless of which QPU backend, regardless of circuit type — is
automatically tagged with the pod UID. No user code changes are required.

### 3.3 Sidecar controller

The `fluence-sidecar` container is injected automatically by the Fluence
webhook into any pod requesting a QPU resource. It runs alongside the user
application in the same pod, sharing the pod's AWS credentials via env vars.

**Algorithm:**

```
1. READ  FLUXION_ARN, FLUENCE_POD_UID from env
2. READ  gated sibling pod names from FLUENCE_GATED_PODS annotation

3. WAIT  for task tagged fluence-pod-uid=<pod-uid> on device <FLUXION_ARN>
         poll search_quantum_tasks every 10s
         timeout after FLUENCE_TASK_DISCOVERY_TIMEOUT (default: 300s)
         on timeout: fall back to time-window heuristic

4. POLL  task.queue_position() every 30s
         log position to stdout for experiment instrumentation

5. WHEN  position == "1" OR state == RUNNING:
         for each pod in FLUENCE_GATED_PODS:
             kubectl annotate pod <name> braket.quantum/task-arn=<arn>
             kubectl patch pod <name> remove schedulingGates

6. EXIT  (sidecar is done — pod continues running user application)
```

**Fallback heuristic (step 3 timeout):**

If no tagged task is found within the discovery timeout — e.g. because the
user application uses a non-standard SDK path — the sidecar searches for
tasks submitted to `FLUXION_ARN` with `createdAt >= pod_start_time` and
picks the most recently created one. This is less reliable but handles
edge cases gracefully.

### 3.4 Gated classical pods

Classical pods that depend on a quantum result are submitted with:

```yaml
spec:
  schedulingGates:
    - name: quantum.braket/ready
  priorityClassName: quantum-classical-high
  # No graph allocation yet — MatchAllocateSpec deferred until ungating
```

The high `PriorityClass` means nothing while the gate is present — the pod
is invisible to the scheduling queue. When the sidecar removes the gate at
position==1, the pod enters the queue with high priority and Kubernetes
preemption displaces lower-priority work to make room.

### 3.5 Fluence PostFilter for topology-aware preemption

The default Kubernetes preemption controller evicts pods based purely on
`PriorityClass`, with no awareness of Fluxion's resource graph. It may
evict pods whose removal does not actually free the graph vertices needed
for the incoming classical pod.

Fluence implements a custom `PostFilter` extension point that:

1. Receives the high-priority classical pod that failed `MatchAllocateSpec`
2. Asks Fluxion which graph vertices are blocking the match
3. Maps those vertices to currently running pods via Fluence's allocation
   tracking
4. Passes only those specific pods to the preemption logic
5. Returns the `nominatedNodeName` that Fluxion identified

This ensures preemption targets topologically correct pods — pods whose
eviction will actually let Fluxion satisfy the match — rather than
arbitrarily choosing the lowest-priority pods on the cluster.

## 4. Properties of the Design

### 4.1 Zero user cooperation required

The SDK interceptor is injected transparently by the webhook. The user
application requires no changes. The sidecar is injected automatically.
The only user-visible artifact is the `FLUXION_ARN` env var, which the
user already needs to know which backend to target.

### 4.2 Classical resources allocated at the last responsible moment

Graph allocation (`MatchAllocateSpec`) happens only when the QPU task
reaches position==1 — seconds to minutes before the result arrives. During
the entire QPU queue wait, no classical node resources are consumed and no
graph capacity is held.

### 4.3 Classical allocation follows quantum execution order

Because each workflow's gate is removed independently when its QPU task
reaches position==1, workflows whose QPU tasks execute earlier get classical
resources earlier — regardless of submission order. A workflow submitted to
a quiet backend gets its classical resources before a workflow submitted
earlier to a busy one. This aligns classical scheduling with actual quantum
execution order across heterogeneous backends.

### 4.4 No estimation of QPU queue time required

The design makes no attempt to predict when the QPU task will execute.
`position==1` is an observable state transition, not an estimate. The
design is robust to variable queue depths, hardware maintenance windows,
and concurrent submissions by other users.

### 4.5 Task ARN propagated to classical pods

When the sidecar removes the gate, it patches `braket.quantum/task-arn`
onto each classical pod as an annotation. Classical pods read this via
the downward API and can use it to retrieve results from S3, submit
follow-on circuits, perform error mitigation, or do anything else the
Braket SDK supports. The sidecar does not prescribe what classical pods
do with the result.

## 5. Limitations

### 5.1 Non-Braket SDKs

The SDK interceptor currently patches `AwsDevice.run()`. Support for
IBM Qiskit Runtime (`backend.run()`), IQM, and other vendors requires
additional interceptors. The pattern is identical; only the entry point
differs.

### 5.2 Preemption disrupts lower-priority work

At position==1, classical pods may preempt running lower-priority work.
This work is re-queued and eventually runs, but there is a disruption cost.
A future design using Fluxion's `MatchReserveAt` primitive with a
vendor-supplied ETA would allow graceful draining instead of preemption.
This requires QPU vendors to expose task ETA or start-event webhooks,
which no current vendor provides.

### 5.3 Multi-task workflows

The sidecar currently tracks one task per pod. Workflows that submit
multiple QPU tasks (e.g. parameter-shift gradient estimation with 2P
circuits) require the sidecar to track a set of task ARNs and ungate
classical pods when all tasks reach position==1 or a subset completes.
This is a straightforward extension.

### 5.4 Sidecar resource consumption

The sidecar consumes minimal CPU and memory (polling every 30s), but
it does hold an open AWS API connection for the duration of the QPU
queue wait. On clusters with many concurrent hybrid workflows this
may become a concern.

## 6. Required Vendor API Primitive

The remaining limitation that cannot be solved without vendor cooperation
is task provenance — associating a Braket task with the Kubernetes pod
that submitted it without SDK interception. If Braket were to expose a
`clientToken` or `podIdentity` field that the SDK set automatically from
the execution environment (analogous to how IAM roles work for EC2
instances), the interceptor would not be needed.

More significantly, if QPU vendors exposed a task-start event (webhook,
SNS notification, or EventBridge rule) when a task transitions from
QUEUED to RUNNING, the sidecar could react to that event rather than
polling. This would enable graceful draining rather than preemption, and
would allow Fluxion's reservation system to be used with an externally-
supplied `time_at` rather than requiring the position==1 heuristic.

## 7. Implementation Plan

### Phase 1 — Sidecar container (this repo)
- `docker/fluence-sidecar/` — sidecar image
- SDK interceptor (`fluence_braket_intercept.py`)
- Task discovery (tagged search + heuristic fallback)
- Queue position polling
- Pod annotation patching and gate removal

### Phase 2 — Fluence webhook changes
- Inject `FLUENCE_POD_UID` env var into QPU pods
- Inject sidecar container into QPU pods
- Inject SDK interceptor as a mounted ConfigMap
- Inject `FLUENCE_GATED_PODS` annotation listing sibling gated pods
- Create `quantum-classical-high` PriorityClass

### Phase 3 — Fluence PostFilter
- Custom preemption targeting Fluxion-graph-aware pod selection
- Integration with existing allocation tracking in placement.go

### Phase 4 — Experiment
- Demonstrate two-queue problem empirically (experiment 1, already running)
- Demonstrate gate + sidecar design reducing classical idle time
- Compare classical node-seconds consumed: ungated vs gated
- Show quantum execution order driving classical allocation order
  across heterogeneous backends (SV1, IQM, Rigetti)
EOF
