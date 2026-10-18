# QRMI in Fluence

Fluence has a **QRMI provider** that provides co-scheduling using [QRMI](https://github.com/qiskit-community/qrmi) (the Quantum Resource Management Interface) to open a session by the scout, and then deliver the session ID to any classical workers to use. Please read [`quantum-scheduling.md`](quantum-scheduling.md) first for the general design we are using across environments and interfaces. QRMI is deliberately vendor-agnostic. It is a thin middleware (Rust core, Python/C bindings) that exposes a single API over any resource it supports. Examples include IBM Quantum, Pasqal, IQM, Alice & Bob, and more. Here is an example using the Python SDK.

```python
from qrmi import QuantumResource, ResourceType, Payload, TaskStatus

qrmi   = QuantumResource("ibm_marrakesh", ResourceType.QiskitRuntimeService)

# reserve the device -> session token, requires paid account
lock   = qrmi.acquire()
job_id = qrmi.task_start(Payload.QiskitPrimitive(input=..., program_id=...))

# TaskStatus.{Queued,Running,Completed,...}
status = qrmi.task_status(job_id)
result = qrmi.task_result(job_id)
qrmi.task_stop(job_id); qrmi.release(lock)
```

Fluence has the `fluence.providers.qrmi` provider that drives this API. We determine vendors via Fluence resource attributes. The attributes are set via environment variables by a scout sidecar that will ungate classical workers when we are at position 1 in the queue. They are set by the Fluence webhook on classical workers. They include.

| resource attribute | env | role |
|---|---|---|
| `qrmi_type` | `FLUXION_QRMI_TYPE` | the QRMI resource type, the single vendor-selecting knob (`qiskit-runtime-service`, `ibm-quantum-system`, `pasqal-cloud`, …). Maps to the QRMI `ResourceType` enum member. |
| `transport` (optional) | `FLUXION_TRANSPORT` | explicit `qrmi` marker for routing (see below). |
| `backend` (the vertex name) | `FLUXION_BACKEND` | the backend/resource id (`ibm_marrakesh`, `FRESNEL`, …). |
| `vendor` (optional) | `FLUXION_VENDOR` | informational / per-vendor sidecar-image routing. |

Adding a new QRMI vendor is therefore a **config** change (a `qrmi_type` in `resources.yaml`), and not a code change. The `qrmi_type → ResourceType` mapping is derived generically (kebab-case → the enum's PascalCase member, acronym-aware so `ibm-quantum-system → IBMQuantumSystem`), with a tiny alias table for legacy
names (`direct-access → IBMQuantumSystem`) and a version-defensive fallback.

## Routing

The sidecar resolves the provider at runtime from the injected attributes. The QRMI provider matches when.

1. `transport == qrmi` (explicit and future-proof, recommended in the resource config), **or**
2. no `transport` is set but `qrmi_type` is present and is **not** a `braket-*` type.

Braket and QRMI are mutually exclusive, as they both provide API interfaces to quantum queues.


## Discovery

Braket's `SearchQuantumTasks` lets the sidecar find the scout's task by a tag the interceptor stamped. We currently write to a shared emptyDir, and it would be useful if QRMI could provide some kind of tag associated with the job that could be retrieved by the user tokebn in another environment. See `fluence.handoff` for how this works.

- the scout's interceptor patches `QuantumResource.task_start`, captures the returned id, and writes `{job_id, session_id?, tier}` to `$FLUENCE_HANDOFF_DIR/task.json`.
- the sidecar (always the same pod) reads that file, then polls `task_status(job_id)` for readiness.

The webhook mounts one writable `emptyDir` for this channel into the submitting container(s) and the sidecar. We don't need RBAC because the scout and its sidecar are co-located. The handoff module is reusable by any future provider whose SDK can't be searched.

## Free vs. paid

QRMI separates *acquisition* (`acquire`/`release`, which reserves the device and, where supported, grants a priority/session queue) from *submission*. If you don't have a paid account you can only launch tasks (without priority) so the design here won't help you, as it gets a priority queue from a session.

## Holding the session for the workers

The scout acquired the session, so it would close when the scout application calls release, long before the workers run. The interceptor patches release on a scout so it waits for a marker the sidecar writes. After the sidecar ungates the workers it waits for every pod in the gang other than itself to reach Succeeded or Failed, then writes the marker to the handoff volume and exits. The scout application then releases and exits, the pod finishes, and fluence frees its allocation. The scout keeps its session and its qpu the whole time, the way the scout does in [flux-quantum](https://github.com/converged-computing/flux-quantum).

The environment variable FLUENCE_WORKER_TIMEOUT on the sidecar is how long it waits for the group, and FLUENCE_SESSION_HOLD_TIMEOUT in the scout container is how long release waits for the marker. Both default to zero, which waits without limit. Since QRMI exposes no numeric queue position, the `queue_position()` is best-effort. We return 1 once it is RUNNING, else unknown.

## The sidecar image

The default sidecar is a single provider-agnostic image that resolves the vendor at runtime. This means we put all the pip installs for vendor SDKs into one image. If we have too many vendors and this is unreasonable to do we can build more than one sidecar.

## Credentials

The scheduler never needs vendor credentials. The workload container holds them (e.g. IBM's `IBM_CLOUD_TOKEN`/`IBM_CLOUD_CRN` or QRMI's `QRMI_*` env), and the webhook copies the workload's secret/configMap-sourced env onto the sidecar so it can build its own `QuantumResource` handle and poll `task_status`. Polling status needs no `acquire()`, so this works for both tiers.
