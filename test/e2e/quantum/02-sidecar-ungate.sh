#!/usr/bin/env bash
# Gang + submitter webhook test (no leader/worker).
#
# When a quantum workload (a gang of N pods all requesting QPU, no roles) is
# submitted, the webhook must:
#   1. create the fluence-sidecar RBAC in the namespace automatically
#   2. gate every gang pod with quantum.braket/ready
#   3. raise every gang pod to the fluence-quantum-classical priority class
#   4. ADDITIONALLY create the one-off submitter pod <group>-submitter
#   5. inject the fluence-stage init container + the sidecar container into the
#      submitter (Model C staging + the real coordinator)
#
# Does NOT test the sidecar runtime (task discovery, interceptor, queue polling)
# — that needs real AWS creds (sidecars/providers/braket/test/integration.sh).
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; . "${HERE%/test/e2e/*}/test/e2e/lib.sh"

GROUP=qgang
SUBMITTER=${GROUP}-submitter

log "TEST 4: gang+submitter webhook — RBAC, gating, priority, submitter creation"
kubectl apply -f examples/test/e2e/quantum/quantum-gang-pods.yaml
sleep 3

log "--- webhook logs ---"
kubectl logs -n kube-system deployment/fluence-webhook --tail=50 || true
log "--- end webhook logs ---"

# 1. RBAC created by the webhook (idempotent, per-namespace).
log "checking webhook created fluence-sidecar RBAC..."
for i in $(seq 1 30); do
  kubectl get serviceaccount fluence-sidecar -n default >/dev/null 2>&1 && break
  sleep 2
done
kubectl get serviceaccount fluence-sidecar -n default || fail "no fluence-sidecar ServiceAccount"
kubectl get role            fluence-sidecar -n default || fail "no fluence-sidecar Role"
kubectl get rolebinding     fluence-sidecar -n default || fail "no fluence-sidecar RoleBinding"
log "  RBAC present"

# 2 + 3. Every gang pod is gated and at the preempting priority class.
for p in ${GROUP}-0 ${GROUP}-1; do
  gate="$(kubectl get pod "$p" -o jsonpath='{.spec.schedulingGates[0].name}' 2>/dev/null || true)"
  [ "$gate" = "quantum.braket/ready" ] || fail "$p not gated (gate=$gate)"
  pc="$(kubectl get pod "$p" -o jsonpath='{.spec.priorityClassName}' 2>/dev/null || true)"
  [ "$pc" = "fluence-quantum-classical" ] || fail "$p priorityClass=$pc, want fluence-quantum-classical"
done
log "  gang pods gated + fluence-quantum-classical priority"

# 4. Fluence created the submitter pod.
log "checking webhook created the submitter pod $SUBMITTER..."
for i in $(seq 1 30); do
  kubectl get pod "$SUBMITTER" -n default >/dev/null 2>&1 && break
  sleep 2
done
kubectl get pod "$SUBMITTER" -n default || fail "webhook did not create submitter pod $SUBMITTER"
sub_marker="$(kubectl get pod "$SUBMITTER" -o jsonpath='{.metadata.annotations.fluence\.flux-framework\.org/submitter}' 2>/dev/null || true)"
[ "$sub_marker" = "true" ] || fail "submitter missing the submitter marker"
log "  submitter pod created"

# 5. Submitter has the staging init container + the sidecar container, and is NOT gated.
wait_pod_phase "$SUBMITTER" Running 120 \
  || { kubectl describe pod "$SUBMITTER"; fail "$SUBMITTER did not reach Running"; }
initc="$(kubectl get pod "$SUBMITTER" -o jsonpath='{.spec.initContainers[*].name}')"
echo "$initc" | grep -q fluence-stage || fail "fluence-stage init container not injected (init: $initc)"
conts="$(kubectl get pod "$SUBMITTER" -o jsonpath='{.spec.containers[*].name}')"
echo "$conts" | grep -q fluence-sidecar || fail "fluence-sidecar container not injected (containers: $conts)"
sgate="$(kubectl get pod "$SUBMITTER" -o jsonpath='{.spec.schedulingGates[0].name}' 2>/dev/null || true)"
[ -z "$sgate" ] || fail "submitter must NOT be gated (gate=$sgate)"
log "  submitter has fluence-stage + fluence-sidecar, not gated"

log "PASS: webhook gated the gang, set priority, created RBAC + the submitter"
log "NOTE: priority is set at admission (immutable post-creation)"
log "NOTE: braket sidecar runtime (SDK intercept, tag discovery, queue polling)"
log "      is in sidecars/providers/braket/test/integration.sh"

# Clean up pods + PodGroups; RBAC is namespace infra and persists.
kubectl delete -f examples/test/e2e/quantum/quantum-gang-pods.yaml --wait=false || true
kubectl delete pod "$SUBMITTER" --wait=false 2>/dev/null || true
for g in "$GROUP" "$SUBMITTER"; do
  kubectl patch podgroup "$g" --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
done
kubectl wait --for=delete pod -l app="$GROUP" --timeout=60s 2>/dev/null || true
