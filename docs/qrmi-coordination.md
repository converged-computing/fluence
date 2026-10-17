# QRMI in Fluence

Fluence has a **QRMI provider** that provides co-scheduling using [QRMI](https://github.com/qiskit-community/qrmi) (the Quantum Resource Management Interface) to open a session by the scout, and then deliver the session ID to any classical workers to use. Please read [`quantum-scheduling.md`](quantum-scheduling.md) first for the general design we are using across environments and interfaces.

## One provider, many vendors

QRMI is deliberately vendor-agnostic: a thin middleware (Rust core, Python/C
bindings) that exposes **one** lifecycle API over any resource it supports —
IBM Quantum, Pasqal, IQM, Alice & Bob, and more:

```python
from qrmi import QuantumResource, ResourceType, Payload, TaskStatus

qrmi   = QuantumResource("ibm_marrakesh", ResourceType.QiskitRuntimeService)
lock   = qrmi.acquire()                     # reserve the device -> session token
job_id = qrmi.task_start(Payload.QiskitPrimitive(input=..., program_id=...))
status = qrmi.task_status(job_id)           # TaskStatus.{Queued,Running,Completed,...}
result = qrmi.task_result(job_id)
qrmi.task_stop(job_id); qrmi.release(lock)
```

So Fluence has **one** provider — `fluence.providers.qrmi` — that drives this
API. It contains no per-vendor branches. Every vendor detail comes from the
Fluence **resource attributes**, which the scheduler stamps onto the match and
the webhook injects as `FLUXION_*` env into the sidecar and workload containers:

| resource attribute | env | role |
|---|---|---|
| `qrmi_type` | `FLUXION_QRMI_TYPE` | the QRMI resource type — the single vendor-selecting knob (`qiskit-runtime-service`, `ibm-quantum-system`, `pasqal-cloud`, …). Maps to the QRMI `ResourceType` enum member. |
| `transport` (optional) | `FLUXION_TRANSPORT` | explicit `qrmi` marker for routing (see below). |
| `backend` (the vertex name) | `FLUXION_BACKEND` | the backend/resource id (`ibm_marrakesh`, `FRESNEL`, …). |
| `vendor` (optional) | `FLUXION_VENDOR` | informational / per-vendor sidecar-image routing. |

Adding a new QRMI vendor is therefore a **config** change (a `qrmi_type` in
`resources.yaml`), not a code change. The `qrmi_type → ResourceType` mapping is
derived generically (kebab-case → the enum's PascalCase member, acronym-aware so
`ibm-quantum-system → IBMQuantumSystem`), with a tiny alias table for legacy
names (`direct-access → IBMQuantumSystem`) and a version-defensive fallback.

## Routing (which provider handles a device)

The sidecar resolves the provider at runtime from the injected attributes
(`resolve_from_env`). The QRMI provider matches when:

1. `transport == qrmi` (explicit and future-proof — recommended in the resource
   config), **or**
2. no `transport` is set but `qrmi_type` is present and is **not** a `braket-*`
   type. Fluence uses `braket-*` qrmi_types for Braket-reached devices and the
   QRMI resource-type strings for QRMI-reached ones, so "a qrmi_type that isn't
   braket" cleanly means QRMI. This is data-driven: whatever the config sets
   decides it, with no per-vendor list in the code.

Braket and QRMI never collide.

## Discovery: the one thing QRMI changes vs. Braket

Braket's `SearchQuantumTasks` lets the sidecar find the scout's task by a tag
the interceptor stamped. QRMI has no tag-able submit and no searchable tag —
`task_start(payload)` takes no tags and returns an opaque id. So instead of a
vendor-side search, the provider uses an **in-pod handoff** (`fluence.handoff`):

- the scout's interceptor patches `QuantumResource.task_start`, captures the
  returned id, and writes `{job_id, session_id?, tier}` to
  `$FLUENCE_HANDOFF_DIR/task.json`;
- the sidecar (always the same pod) reads that file, then polls
  `task_status(job_id)` for readiness.

The webhook mounts one writable `emptyDir` for this channel into the submitting
container(s) and the sidecar. It is RBAC-free because the scout and its
sidecar are co-located. This is a property of QRMI, not of any vendor, and the
handoff module is reusable by any future provider whose SDK can't be searched.

## Free vs. paid: the acquire nuance (also vendor-neutral)

QRMI separates *acquisition* (`acquire`/`release`, which reserves the device and,
where supported, grants a priority/session queue) from *submission*.

| | acquire() | queue | how Fluence coordinates |
|---|---|---|---|
| **free / open** | not permitted (raises) | fair-share | submit-only; ungate when the task starts RUNNING |
| **paid / premium** | returns a session token | priority | same wait; session token captured and handed to workers |

The provider **auto-detects** the tier — no config, no change to the user's code
path:

- The interceptor patches `acquire()` so a **success** records the session token
  + `tier=premium`, and a **failure** records `tier=free` and **re-raises
  unchanged** so a free-tier app sees the real error and proceeds submit-only.
  Fluence never forces `acquire()`.
- `task_start()` publishes `{job_id, session_id?, tier}` to the handoff.

The **ungate decision is identical in both tiers**: ready once `task_status` is
`RUNNING` or terminal — the QRMI-visible analogue of Braket's
`queue_position == 1`. The tier only affects how long the wait is and whether a
session token exists. When present, the sidecar stamps it onto each worker
(`fluence.flux-framework.org/quantum-session-id` → `FLUENCE_QUANTUM_SESSION_ID`)
so a role-aware worker can submit into the same priority session.

## Holding the session for the workers

The scout acquired the session, so it would close when the scout
application calls release, long before the workers run. The interceptor
patches release on a scout so it waits for a marker the sidecar writes.
After the sidecar ungates the workers it waits for every pod in the gang
other than itself to reach Succeeded or Failed, then writes the marker to the
handoff volume and exits. The scout application then releases and exits,
the pod finishes, and fluence frees its allocation. The scout keeps its
session and its qpu the whole time, the way the scout does in flux-quantum.

Two timeouts bound the hold. FLUENCE_WORKER_TIMEOUT on the sidecar is how
long it waits for the group, and FLUENCE_SESSION_HOLD_TIMEOUT in the scout
container is how long release waits for the marker. Both default to zero,
which waits without limit.

> QRMI exposes no numeric queue position, so `queue_position()` is best-effort
> telemetry (1 once RUNNING, else unknown) and never required for the ungate.

## The sidecar image (one image, per-vendor escape hatch)

The default sidecar is a single provider-agnostic image (`pip install ".[all]"`
= braket + `qrmi[ibm]`) that resolves the vendor at runtime. QRMI's `ibm` and
`iqm` extras pin incompatible Qiskit versions, so `all` includes `ibm` (the
default reachable QRMI vendor) but not `iqm`. When a vendor's SDK can't share the
combined image, build a slim single-vendor image and route to it:

```bash
docker build --build-arg EXTRAS=iqm -t ghcr.io/you/fluence-sidecar:iqm python/
```

Point the webhook at it globally (`FLUENCE_SIDECAR_IMAGE_<VENDOR>`, e.g.
`FLUENCE_SIDECAR_IMAGE_IQM`) or per-pod
(`fluence.flux-framework.org/sidecar-image`). A pod that hints a vendor
(`require-vendor: iqm`, or a `require-qrmi_type` the webhook maps to a vendor)
then picks up `FLUENCE_SIDECAR_IMAGE_IQM` automatically.

## Credentials

The scheduler never needs vendor credentials. The workload container holds them
(e.g. IBM's `IBM_CLOUD_TOKEN`/`IBM_CLOUD_CRN` or QRMI's `QRMI_*` env), and the
webhook copies the workload's secret/configMap-sourced env onto the sidecar so
it can build its own `QuantumResource` handle and poll `task_status`. Polling
status needs no `acquire()`, so this works for both tiers.

## Files

- `python/fluence/providers/qrmi.py` — the provider (interceptor + sidecar).
- `python/fluence/handoff.py` — the shared in-pod job-id channel.
- `pkg/webhook/handlers/quantum.go` — mounts the writable handoff volume, sets
  `FLUENCE_HANDOFF_DIR`, surfaces `FLUENCE_QUANTUM_SESSION_ID`, and resolves the
  per-vendor sidecar image.

## Tests

```bash
PYTHONPATH=python python3 python/tests/test_qrmi.py   # provider + handoff + tiers + multi-vendor routing
go test ./pkg/webhook/...                             # handoff wiring + per-vendor image selection
```
