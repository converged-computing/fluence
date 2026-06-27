#!/usr/bin/env bash
# Env-contract e2e (producer/consumer): verify the webhook injects, at admission,
# the env the runtime depends on — IN-CLUSTER, on the real pod specs, with no
# Braket/AWS and WITHOUT requiring scheduling. Guards the seam that, if broken,
# makes a gang schedule then hang or double-submit.
#
# Spec layer only (these are downward-API valueFrom refs whose VALUES resolve at
# placement, but whose PRESENCE is deterministic at admission), so no scheduling,
# no qpu capacity, no logs — it cannot flake on capacity. Contract:
#   consumer (faux):  FLUENCE_FAUX_SUBMIT, FLUENCE_QUANTUM_JOB_ID, PYTHONPATH, FLUXION_BACKEND
#   producer:         FLUENCE_GANG_GROUP on the sidecar (real submit, ungates the consumers)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; . "${HERE%/test/e2e/*}/test/e2e/lib.sh"

GROUP=qgang
PRODUCER=${GROUP}-0   # completion index 0
CONSUMER=${GROUP}-1   # completion index 1

log "TEST 8: producer/consumer env contract — spec layer"
kubectl apply -f examples/test/e2e/quantum/quantum-gang-pods.yaml

# does container $2 of pod $1 have an env entry named $3 ? (spec-level only)
has_env() {
  kubectl get pod "$1" -o jsonpath="{.spec.containers[?(@.name=='$2')].env[*].name}" \
    2>/dev/null | tr ' ' '\n' | grep -qx "$3"
}

log "checking the webhook wired the faux contract onto the consumer"
for i in $(seq 1 15); do has_env "$CONSUMER" app FLUENCE_FAUX_SUBMIT && break; sleep 2; done
for v in FLUENCE_FAUX_SUBMIT FLUENCE_QUANTUM_JOB_ID PYTHONPATH FLUXION_BACKEND; do
  has_env "$CONSUMER" app "$v" \
    || { kubectl get pod "$CONSUMER" -o yaml | sed -n '/containers:/,/status:/p'; \
         fail "consumer 'app' container missing env '$v'"; }
  log "  consumer has env: $v"
done

# The producer's sidecar must know which consumer group to ungate.
log "checking the producer sidecar has FLUENCE_GANG_GROUP=$GROUP"
for i in $(seq 1 30); do kubectl get pod "$PRODUCER" >/dev/null 2>&1 && break; sleep 2; done
gg="$(kubectl get pod "$PRODUCER" \
  -o jsonpath="{.spec.containers[?(@.name=='fluence-sidecar')].env[?(@.name=='FLUENCE_GANG_GROUP')].value}" \
  2>/dev/null || true)"
[ "$gg" = "$GROUP" ] || fail "producer sidecar FLUENCE_GANG_GROUP=$gg, want $GROUP"
log "  producer sidecar has FLUENCE_GANG_GROUP=$gg"

# And the producer must NOT be in faux mode (it does the real submit).
if has_env "$PRODUCER" app FLUENCE_FAUX_SUBMIT; then
  fail "producer must NOT carry FLUENCE_FAUX_SUBMIT (it submits for real)"
fi
log "  producer is not faux"

log "PASS 8: webhook injects the consumer(faux) + producer(real) env contract at admission"

kubectl delete -f examples/test/e2e/quantum/quantum-gang-pods.yaml --wait=false || true
for g in "$GROUP" "${GROUP}-producer"; do
  kubectl patch podgroup "$g" --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
done
kubectl wait --for=delete pod -l app="$GROUP" --timeout=60s 2>/dev/null || true
