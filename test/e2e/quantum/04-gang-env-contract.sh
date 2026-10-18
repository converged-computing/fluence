#!/usr/bin/env bash
# Env-contract e2e (scout/worker). Verify the webhook injects, at admission,
# the env the runtime depends on, IN-CLUSTER, on the real pod specs, with no
# Braket/AWS and WITHOUT requiring scheduling. Guards the seam that, if broken,
# makes a gang schedule then hang or double-submit.
#
# Spec layer only (these are downward-API valueFrom refs whose VALUES resolve at
# placement, but whose PRESENCE is deterministic at admission), so no scheduling,
# no qpu capacity, no logs, it cannot flake on capacity. Contract.
#   worker (role):  FLUENCE_COORDINATION_ROLE=worker, FLUENCE_QUANTUM_JOB_ID, FLUXION_BACKEND
#                     (no interceptor, its submits go into the scout session)
#   scout (role):  FLUENCE_COORDINATION_ROLE=scout + FLUENCE_GANG_GROUP on the sidecar
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; . "${HERE%/test/e2e/*}/test/e2e/lib.sh"

GROUP=qgang
SCOUT=${GROUP}-0   # completion index 0
WORKER=${GROUP}-1   # completion index 1

log "TEST 8: scout/worker env contract, spec layer"
kubectl apply -f examples/test/e2e/quantum/quantum-gang-pods.yaml

# does container $2 of pod $1 have an env entry named $3 ? (spec-level only)
has_env() {
  kubectl get pod "$1" -o jsonpath="{.spec.containers[?(@.name=='$2')].env[*].name}" \
    2>/dev/null | tr ' ' '\n' | grep -qx "$3"
}
# value of env $3 in container $2 of pod $1 (empty if absent)
env_val() {
  kubectl get pod "$1" -o jsonpath="{.spec.containers[?(@.name=='$2')].env[?(@.name=='$3')].value}" \
    2>/dev/null || true
}

log "checking the webhook wired the worker role contract"
for i in $(seq 1 15); do has_env "$WORKER" app FLUENCE_COORDINATION_ROLE && break; sleep 2; done
# Present: the role (=worker), the scout's task id, and the backend.
for v in FLUENCE_COORDINATION_ROLE FLUENCE_QUANTUM_JOB_ID FLUXION_BACKEND; do
  has_env "$WORKER" app "$v" \
    || { kubectl get pod "$WORKER" -o yaml | sed -n '/containers:/,/status:/p'; \
         fail "worker 'app' container missing env '$v'"; }
  log "  worker has env: $v"
done
role="$(env_val "$WORKER" app FLUENCE_COORDINATION_ROLE)"
[ "$role" = "worker" ] || fail "worker role=$role, want worker"
# Absent. No interceptor staging, a worker submits into the scout session if at all.
for v in PYTHONPATH FLUENCE_FAUX_SUBMIT; do
  ! has_env "$WORKER" app "$v" || fail "worker must NOT have '$v' (it is not staged with the interceptor)"
done
log "  worker role=worker, no interceptor/faux"

# The scout's sidecar must know which worker group to ungate.
log "checking the scout sidecar has FLUENCE_GANG_GROUP=$GROUP"
for i in $(seq 1 30); do kubectl get pod "$SCOUT" >/dev/null 2>&1 && break; sleep 2; done
gg="$(kubectl get pod "$SCOUT" \
  -o jsonpath="{.spec.containers[?(@.name=='fluence-sidecar')].env[?(@.name=='FLUENCE_GANG_GROUP')].value}" \
  2>/dev/null || true)"
[ "$gg" = "$GROUP" ] || fail "scout sidecar FLUENCE_GANG_GROUP=$gg, want $GROUP"
log "  scout sidecar has FLUENCE_GANG_GROUP=$gg"

# The scout has role=scout and is the real submitter (no worker id).
prole="$(env_val "$SCOUT" app FLUENCE_COORDINATION_ROLE)"
[ "$prole" = "scout" ] || fail "scout role=$prole, want scout"
if has_env "$SCOUT" app FLUENCE_QUANTUM_JOB_ID; then
  fail "scout must NOT have FLUENCE_QUANTUM_JOB_ID (it submits its own task)"
fi
log "  scout role=scout, submits its own task"

log "PASS 8: webhook injects the worker(role) + scout(role) env contract at admission"

kubectl delete -f examples/test/e2e/quantum/quantum-gang-pods.yaml --wait=false || true
for g in "$GROUP" "${GROUP}-scout"; do
  kubectl patch podgroup "$g" --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
done
kubectl wait --for=delete pod -l app="$GROUP" --timeout=60s 2>/dev/null || true
