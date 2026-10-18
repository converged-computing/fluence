#!/usr/bin/env bash
# Shared-coordination webhook test (scout/worker, no submitter pod).
#
# When a shared quantum gang (coordination=shared, N pods all requesting QPU) is
# submitted, the webhook must.
#   1. create the fluence-sidecar RBAC in the namespace automatically
#   2. gate every WORKER pod with quantum.braket/ready
#   3. raise every WORKER pod to the fluence-quantum-classical priority class
#   4. leave the SCOUT (completion index 0) UNGATED, as a real member (NOT a
#      separate spawned pod)
#   5. inject the fluence-stage init container + the sidecar container into the
#      scout (Model C staging + the real coordinator)
#
# Does NOT test the sidecar runtime (task discovery, interceptor, queue polling)
#, that needs real AWS creds (sidecars/providers/braket/test/integration.sh).
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; . "${HERE%/test/e2e/*}/test/e2e/lib.sh"

GROUP=qgang
SCOUT=${GROUP}-0   # completion index 0
WORKER=${GROUP}-1   # completion index 1

log "TEST 4: shared-gang webhook, RBAC, worker gating, priority, scout wiring"
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

# 2 + 3. The WORKER is gated and at the preempting priority class.
gate="$(kubectl get pod "$WORKER" -o jsonpath='{.spec.schedulingGates[0].name}' 2>/dev/null || true)"
[ "$gate" = "quantum.braket/ready" ] || fail "$WORKER not gated (gate=$gate)"
pc="$(kubectl get pod "$WORKER" -o jsonpath='{.spec.priorityClassName}' 2>/dev/null || true)"
[ "$pc" = "fluence-quantum-classical" ] || fail "$WORKER priorityClass=$pc, want fluence-quantum-classical"
log "  worker gated + fluence-quantum-classical priority"

# 4. The SCOUT is NOT a separate spawned pod and is NOT gated. No <group>-submitter.
if kubectl get pod "${GROUP}-submitter" -n default >/dev/null 2>&1; then
  fail "found ${GROUP}-submitter pod, the obsolete separate-submitter model must not exist"
fi
pgate="$(kubectl get pod "$SCOUT" -o jsonpath='{.spec.schedulingGates[0].name}' 2>/dev/null || true)"
[ -z "$pgate" ] || fail "scout must NOT be gated (gate=$pgate)"
log "  scout is a real member, not gated; no separate submitter pod"

# 5. Scout has the staging init container + the sidecar container.
wait_pod_phase "$SCOUT" Running 120 \
  || { kubectl describe pod "$SCOUT"; fail "$SCOUT did not reach Running"; }
initc="$(kubectl get pod "$SCOUT" -o jsonpath='{.spec.initContainers[*].name}')"
echo "$initc" | grep -q fluence-stage || fail "fluence-stage init container not injected (init: $initc)"
conts="$(kubectl get pod "$SCOUT" -o jsonpath='{.spec.containers[*].name}')"
echo "$conts" | grep -q fluence-sidecar || fail "fluence-sidecar container not injected (containers: $conts)"
log "  scout has fluence-stage + fluence-sidecar"

log "PASS: webhook gated the workers, set priority, created RBAC + wired the scout"
log "NOTE: priority is set at admission (immutable post-creation)"
log "NOTE: braket sidecar runtime (SDK intercept, tag discovery, queue polling)"
log "      is in sidecars/providers/braket/test/integration.sh"

# Clean up pods + PodGroups. RBAC is namespace infra and persists.
kubectl delete -f examples/test/e2e/quantum/quantum-gang-pods.yaml --wait=false || true
for g in "$GROUP" "${GROUP}-scout"; do
  kubectl patch podgroup "$g" --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
done
kubectl wait --for=delete pod -l app="$GROUP" --timeout=60s 2>/dev/null || true
