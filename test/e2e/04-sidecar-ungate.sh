#!/usr/bin/env bash
# Sidecar gate/ungate plumbing test.
#
# This test verifies the Kubernetes mechanics of the sidecar design:
#   1. A gated classical pod stays SchedulingGated until something removes the gate
#   2. A pod with kubectl access can patch an annotation and remove a gate
#   3. The classical pod reads the patched annotation via the downward API
#
# This does NOT test the braket sidecar itself (task discovery, SDK interceptor,
# queue position polling). Those require real AWS credentials and are covered
# by sidecars/braket/test/integration.sh which is run locally.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; . "${HERE}/lib.sh"

log "TEST 4: sidecar gate/ungate Kubernetes plumbing"

kubectl apply -f examples/test/e2e/sidecar-mock.yaml

# Classical pod must start SchedulingGated — verify it is NOT Running immediately
sleep 5
phase="$(kubectl get pod classical-mock -o jsonpath='{.status.phase}' 2>/dev/null || true)"
[ "$phase" != "Running" ] || fail "classical-mock should not be Running before gate is removed (phase=$phase)"
log "classical-mock is correctly gated (phase=${phase:-SchedulingGated})"

# Gateway pod should reach Running
wait_pod_phase quantum-gateway-mock Running 60 \
  || fail "quantum-gateway-mock did not reach Running"

# Mock sidecar should ungate classical-mock within 60s
log "waiting for mock sidecar to ungate classical-mock..."
for i in $(seq 1 60); do
  phase="$(kubectl get pod classical-mock -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  { [ "$phase" = "Running" ] || [ "$phase" = "Succeeded" ]; } && break
  sleep 2
done
wait_pod_phase classical-mock Running 30 \
  || fail "classical-mock did not reach Running after gate removal"

# Task ARN annotation must have been patched
arn="$(kubectl get pod classical-mock \
  -o jsonpath='{.metadata.annotations.braket\.quantum/task-arn}' 2>/dev/null || true)"
[ -n "$arn" ] || fail "braket.quantum/task-arn annotation not set on classical-mock"
log "task ARN annotation present: $arn"

# Classical pod must have read the annotation via downward API
out="$(kubectl logs classical-mock 2>/dev/null || true)"
echo "$out" | grep -q "TASK_ARN=" \
  || fail "BRAKET_TASK_ARN not visible in classical-mock logs (got: $out)"

log "PASS: gate/ungate plumbing works — annotation patched and read via downward API"
log "NOTE: braket sidecar integration test (SDK intercept, tag discovery,"
log "      queue polling) is in sidecars/braket/test/integration.sh"
kubectl delete -f examples/test/e2e/sidecar-mock.yaml --wait=false || true
