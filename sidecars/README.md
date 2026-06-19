# Fluence Sidecars

Each subdirectory contains a sidecar for a specific quantum cloud vendor or
SDK. Sidecars are injected automatically by the Fluence mutating webhook into
any pod requesting a QPU resource, based on the `qrmi_type` attribute of the
matched backend.

## How sidecars work

When Fluence schedules a pod requesting `fluxion.flux-framework.org/qpu`, the
webhook:

1. Identifies the matched backend's `qrmi_type` (e.g. `braket-gate`, `braket-ahs`, `qrmi`)
2. Injects the corresponding sidecar container into the pod
3. Injects the SDK interceptor as a Python sitecustomize hook
4. Injects `FLUENCE_POD_UID`, `FLUENCE_GATED_PODS`, and other coordination env vars

The sidecar runs alongside the user's quantum application, discovers the
submitted task using the injected pod UID tag, polls the vendor queue, and
ungates paired classical pods when the quantum task is one position from
executing.

## Available sidecars

| Directory | Vendor | qrmi_type | Status |
|---|---|---|---|
| `braket/` | AWS Braket (gate + AHS) | `braket-gate`, `braket-ahs` | Active |
| `qrmi/` | QRMI-compatible backends | `qrmi` | Planned |

## Adding a new sidecar

1. Create a new subdirectory: `sidecars/<vendor>/`
2. Implement `sidecar.py` — must discover the task ARN and call the shared
   ungating logic in `sidecars/lib/ungate.py`
3. Implement `<vendor>_intercept.py` — patches the vendor SDK's submit method
   to tag tasks with `FLUENCE_POD_UID`
4. Add a `Dockerfile`
5. Add the image to `.github/workflows/sidecar-build-deploy.yaml`
6. Add an e2e mock test following `test/e2e/04-sidecar-ungate.sh`
