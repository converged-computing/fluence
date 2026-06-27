#!/usr/bin/env bash
# Quantum placement: a qpu pod is matched to a backend and the webhook injects FLUXION_BACKEND.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; . "${HERE%/test/e2e/*}/test/e2e/lib.sh"
ANN="fluence.flux-framework.org/backend"

log "TEST 2: quantum placement and backend handoff"
kubectl apply -f examples/test/e2e/quantum/quantum-pod-mock.yaml

wait_pod_phase sampler-mock Running 120 || fail "sampler-mock did not reach Running"

# fluence must have stamped the chosen backend annotation.
backend="$(kubectl get pod sampler-mock -o jsonpath="{.metadata.annotations.${ANN//./\\.}}" 2>/dev/null || true)"
[ -n "$backend" ] || (show_webhook sampler-mock && fail "backend annotation ($ANN) was not set by fluence")
log "fluence chose backend: $backend"

# The webhook must have surfaced it as FLUXION_BACKEND inside the container.
out="$(kubectl logs sampler-mock || true)"
if ! echo "$out" | grep -q "BACKEND=${backend}"; then
  # Diagnostic (CI has no interactive shell): show whether the env var is ABSENT
  # (not injected -> webhook issue) or PRESENT-BUT-EMPTY (annotation not resolved
  # at container start -> delivery/timing issue), and what the container actually got.
  log "--- diagnostic: container env spec ---"
  kubectl get pod sampler-mock -o jsonpath='{.spec.containers[0].env}' ; echo
  log "--- diagnostic: live value via exec ---"
  kubectl exec sampler-mock -- sh -c 'echo "FLUXION_BACKEND=[$FLUXION_BACKEND]"' 2>&1 || true
  log "--- diagnostic: backend annotation on pod ---"
  kubectl get pod sampler-mock -o jsonpath="{.metadata.annotations.${ANN//./\\.}}" ; echo
  show_webhook sampler-mock
  fail "FLUXION_BACKEND in container ('$out') does not match annotation ($backend)"
fi

log "PASS: qpu pod scheduled, backend '$backend' chosen and injected as FLUXION_BACKEND"
kubectl delete -f examples/test/e2e/quantum/quantum-pod-mock.yaml --wait=false || true
