#!/usr/bin/env bash
# Quantum placement: a qpu pod is matched to a backend and the webhook injects QRMI_BACKEND.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; . "${HERE}/lib.sh"
NS=default
ANN="fluence.flux-framework.org/backend"

log "TEST 2: quantum placement and backend handoff"
kubectl apply -f examples/test/e2e/quantum-pod-mock.yaml

wait_pod_phase sampler-mock "$NS" Running 120 \
  || fail "sampler-mock did not reach Running"

# fluence must have stamped the chosen backend annotation.
backend="$(kubectl get pod sampler-mock -n "$NS" -o jsonpath="{.metadata.annotations.${ANN//./\\.}}" 2>/dev/null || true)"
[ -n "$backend" ] || fail "backend annotation ($ANN) was not set by fluence"
log "fluence chose backend: $backend"

# The webhook must have surfaced it as QRMI_BACKEND inside the container.
out="$(kubectl logs sampler-mock -n "$NS" || true)"
echo "$out" | grep -q "BACKEND=${backend}" \
  || fail "QRMI_BACKEND in container ('$out') does not match annotation ($backend)"

log "PASS: qpu pod scheduled, backend '$backend' chosen and injected as QRMI_BACKEND"
kubectl delete -f examples/test/e2e/quantum-pod-mock.yaml --wait=false || true
