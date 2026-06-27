#!/usr/bin/env bash
# Sidecar webhook test.
#
# Verifies that when a PodGroup of size > 1 with QPU resources is submitted:
#   1. The webhook creates fluence-sidecar RBAC in the namespace automatically
#   2. The leader pod gets the sidecar container injected
#   3. The worker pod gets the quantum.braket/ready scheduling gate added
#   4. The worker pod gets fluence-quantum-classical priority class set
#
# Does NOT test the sidecar itself (task discovery, interceptor,
# queue position polling). Those require real AWS credentials and are covered
# by sidecars/providers/braket/test/integration.sh which is run locally.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; . "${HERE%/test/e2e/*}/test/e2e/lib.sh"

log "TEST 4: sidecar webhook — RBAC creation, gate injection, sidecar injection"

kubectl apply -f examples/test/e2e/quantum/sidecar-mock-pods.yaml

# Give webhook time to process the leader pod admission
sleep 3

# Print webhook logs — always show these so we can see what happened
log "--- webhook logs ---"
kubectl logs -n kube-system deployment/fluence-webhook --tail=50 || true
log "--- end webhook logs ---"

# 1. Webhook should have created fluence-sidecar ServiceAccount
log "checking webhook created fluence-sidecar ServiceAccount..."
for i in $(seq 1 30); do
  kubectl get serviceaccount fluence-sidecar -n default > /dev/null 2>&1 && break
  sleep 2
done
kubectl get serviceaccount fluence-sidecar -n default \
  || fail "webhook did not create fluence-sidecar ServiceAccount"
log "  fluence-sidecar ServiceAccount created"

# 2. Webhook should have created fluence-sidecar Role
kubectl get role fluence-sidecar -n default \
  || fail "webhook did not create fluence-sidecar Role"
log "  fluence-sidecar Role created"

# 3. Webhook should have created fluence-sidecar RoleBinding
kubectl get rolebinding fluence-sidecar -n default \
  || fail "webhook did not create fluence-sidecar RoleBinding"
log "  fluence-sidecar RoleBinding created"

# 4. Leader pod should have the fluence-stage init container injected (Model C:
#    it stages the fluence Python package into a shared volume on PYTHONPATH).
log "checking webhook injected the fluence-stage init container..."
wait_pod_phase sidecar-test-leader Running 120 \
  || { kubectl describe pod sidecar-test-leader; fail "sidecar-test-leader did not reach Running"; }
initc=$(kubectl get pod sidecar-test-leader \
  -o jsonpath='{.spec.initContainers[*].name}')
echo "$initc" | grep -q "fluence-stage" \
  || fail "fluence-stage init container not injected (initContainers: $initc)"
log "  fluence-stage init container injected"

# 5. Leader pod should have the sidecar container injected
log "checking sidecar injected into leader pod..."
containers=$(kubectl get pod sidecar-test-leader \
  -o jsonpath='{.spec.containers[*].name}')
echo "$containers" | grep -q "fluence-sidecar" \
  || fail "fluence-sidecar container not injected into leader (containers: $containers)"
log "  fluence-sidecar container injected into leader"

# 6. Worker pod should have scheduling gate added by webhook
gate=$(kubectl get pod sidecar-test-worker \
  -o jsonpath='{.spec.schedulingGates[0].name}')
[ "$gate" = "quantum.braket/ready" ] \
  || fail "worker pod does not have quantum.braket/ready gate (got: $gate)"
log "  quantum.braket/ready gate set on worker"

# 7. Worker pod should have the fluence-quantum-classical priority class set by
#    the webhook at admission (so it schedules reliably once ungated).
pc=$(kubectl get pod sidecar-test-worker -o jsonpath='{.spec.priorityClassName}')
[ "$pc" = "fluence-quantum-classical" ] \
  || fail "worker pod missing fluence-quantum-classical priority class (got: $pc)"
log "  fluence-quantum-classical priority class set on worker"

log "PASS: webhook correctly created RBAC, injected sidecar, gated worker"
log "NOTE: fluence-quantum-classical priority is set by the webhook at admission (immutable post-creation)"
log "NOTE: braket sidecar integration test (SDK intercept, tag discovery,"
log "      queue polling) is in sidecars/providers/braket/test/integration.sh"

# Only clean up pods and PodGroup — RBAC is namespace infrastructure
# that persists for future quantum workflows in this namespace
kubectl delete -f examples/test/e2e/quantum/sidecar-mock-pods.yaml
