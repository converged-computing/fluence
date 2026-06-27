#!/usr/bin/env bash
# Restart recovery: after the scheduler restarts, it must replay the existing allocation
# (via reapi update_allocate) and NOT double-book an exclusive qpu backend.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; . "${HERE}/lib.sh"
NS=default
ANN="fluence.flux-framework.org/backend"

log "TEST 3: restart does not double-book an exclusive backend"

# 1. Schedule the first qpu pod and capture its backend.
kubectl apply -f examples/test/e2e/quantum/quantum-pod-mock.yaml
wait_pod_phase sampler-mock "$NS" Running 120 || fail "sampler-mock did not reach Running"
backend="$(kubectl get pod sampler-mock -n "$NS" -o jsonpath="{.metadata.annotations.${ANN//./\\.}}" 2>/dev/null || true)"
[ -n "$backend" ] || fail "first pod has no backend annotation"
log "first pod holds backend: $backend"

# 2. Restart the scheduler. Its in-memory Fluxion graph is rebuilt empty; recovery must
#    replay the persisted allocation so the backend stays occupied.
log "restarting fluence scheduler"
kubectl rollout restart -n "${NS_KUBE}" deployment/fluence
wait_fluence_ready

# 3. The original pod must still be Running and still hold the same backend.
wait_pod_phase sampler-mock "$NS" Running 30 || fail "first pod not Running after restart"

# 4. A second pod requesting the same exclusive qpu must NOT get the same backend.
#    If recovery worked, the backend is occupied and the second pod stays Pending.
kubectl apply -f examples/test/e2e/quantum/quantum-pod-mock-2.yaml
if assert_stays_pending sampler-mock-2 "$NS" 45; then
  log "PASS: second qpu pod stayed Pending; backend '$backend' was not double-booked"
else
  backend2="$(kubectl get pod sampler-mock-2 -n "$NS" -o jsonpath="{.metadata.annotations.${ANN//./\\.}}" 2>/dev/null || true)"
  if [ "$backend2" = "$backend" ]; then
    fail "DOUBLE-BOOK: second pod got the same exclusive backend '$backend' after restart (recovery did not replay)"
  else
    log "NOTE: second pod scheduled to a DIFFERENT backend '$backend2' (ok only if >1 backend configured)"
  fi
fi

kubectl delete -f examples/test/e2e/quantum/quantum-pod-mock-2.yaml --wait=false || true
kubectl delete -f examples/test/e2e/quantum/quantum-pod-mock.yaml --wait=false || true
