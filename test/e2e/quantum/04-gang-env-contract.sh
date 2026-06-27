#!/usr/bin/env bash
# Env-contract e2e: deploy a mock gang and verify the webhook injects the env the
# real gang workload (gang.py) depends on — IN-CLUSTER, on the real pod specs,
# with no Braket/AWS and WITHOUT requiring the pod to be scheduled. Guards the
# runtime seam that, if broken, makes a gang schedule fine then hang (a leader
# with no FLUENCE_ROLE defaults to worker -> no leader -> deadlock).
#
# This checks the SPEC layer only: the env references the webhook wires onto the
# right container at admission. These are downward-API valueFrom refs (their
# VALUES resolve later, at placement), but their PRESENCE is deterministic at
# admission, so this test needs no scheduling, no qpu add-on, no logs — it cannot
# flake on capacity. Injection paths verified in code:
#   FLUENCE_ROLE              roleEnvOps        (quantum handler)  -> all workload containers
#   FLUENCE_POD_UID, PYTHONPATH  InterceptorOps (core)            -> fluxion-resource containers
#   FLUXION_BACKEND           fluxion handler   (InjectEnvOps)     -> fluxion-resource containers
# The leader requests qpu (so it gets the full contract); the worker only needs
# FLUENCE_ROLE (it requests no fluxion resource, by design).
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; . "${HERE%/test/e2e/*}/test/e2e/lib.sh"

log "TEST 8: gang env contract (webhook injects what gang.py reads) — spec layer"
kubectl apply -f examples/test/e2e/quantum/gang-env-mock.yaml

# does container 'app' of pod $1 have an env entry named $2 ? (spec-level only)
has_env() {
  kubectl get pod "$1" -o jsonpath="{.spec.containers[?(@.name=='app')].env[*].name}" \
    2>/dev/null | tr ' ' '\n' | grep -qx "$2"
}

# the webhook mutates at admission; poll briefly for the spec to appear
log "checking the webhook wired the contract onto the leader (qpu) container"
for i in $(seq 1 15); do has_env gangenv-leader FLUENCE_ROLE && break; sleep 2; done

for v in FLUENCE_ROLE FLUENCE_POD_UID PYTHONPATH FLUXION_BACKEND; do
  has_env gangenv-leader "$v" \
    || { kubectl get pod gangenv-leader -o yaml | sed -n '/containers:/,/status:/p'; \
         fail "leader container missing env '$v' (webhook did not inject the contract)"; }
  log "  leader has env: $v"
done

# the worker carries FLUENCE_ROLE so gang.py selects 'worker' by contract, not luck
has_env gangenv-worker-0 FLUENCE_ROLE \
  || fail "worker container missing FLUENCE_ROLE (gang.py would default to worker by luck)"
log "  worker has env: FLUENCE_ROLE"

# and the role VALUE on the spec is correct per pod (downward-API ref to the
# role annotation, or a literal — either way the resolved fieldRef/value must
# encode leader vs worker). Assert the annotation the ref reads is right.
lr="$(kubectl get pod gangenv-leader  -o jsonpath='{.metadata.annotations.fluence\.flux-framework\.org/role}')"
wr="$(kubectl get pod gangenv-worker-0 -o jsonpath='{.metadata.annotations.fluence\.flux-framework\.org/role}')"
[ "$lr" = "leader" ] || fail "leader role annotation=$lr, want leader"
[ "$wr" = "worker" ] || fail "worker role annotation=$wr, want worker"
log "  role annotations correct (leader=$lr worker=$wr)"

log "PASS 8: webhook injects the gang env contract at admission"

kubectl delete -f examples/test/e2e/quantum/gang-env-mock.yaml --wait=false || true
for g in gangenv gangenv-workers; do
  kubectl patch podgroup $g --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
done
kubectl wait --for=delete pod -l app=gangenv --timeout=60s 2>/dev/null || true
