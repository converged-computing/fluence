#!/usr/bin/env bash
# Env-contract e2e (producer/consumer): verify the webhook injects, at admission,
# the env the runtime depends on — IN-CLUSTER, on the real pod specs, with no
# Braket/AWS and WITHOUT requiring scheduling. Guards the seam that, if broken,
# makes a gang schedule then hang or double-submit.
#
# Spec layer only (these are downward-API valueFrom refs whose VALUES resolve at
# placement, but whose PRESENCE is deterministic at admission), so no scheduling,
# no qpu capacity, no logs — it cannot flake on capacity. Contract:
#   consumer (role):  FLUENCE_COORDINATION_ROLE=consumer, FLUENCE_QUANTUM_JOB_ID, FLUXION_BACKEND
#                     (NO interceptor/PYTHONPATH — a consumer never submits)
#   producer (role):  FLUENCE_COORDINATION_ROLE=producer + FLUENCE_GANG_GROUP on the sidecar
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
# value of env $3 in container $2 of pod $1 (empty if absent)
env_val() {
  kubectl get pod "$1" -o jsonpath="{.spec.containers[?(@.name=='$2')].env[?(@.name=='$3')].value}" \
    2>/dev/null || true
}

log "checking the webhook wired the consumer role contract"
for i in $(seq 1 15); do has_env "$CONSUMER" app FLUENCE_COORDINATION_ROLE && break; sleep 2; done
# Present: the role (=consumer), the producer's task id, and the backend.
for v in FLUENCE_COORDINATION_ROLE FLUENCE_QUANTUM_JOB_ID FLUXION_BACKEND; do
  has_env "$CONSUMER" app "$v" \
    || { kubectl get pod "$CONSUMER" -o yaml | sed -n '/containers:/,/status:/p'; \
         fail "consumer 'app' container missing env '$v'"; }
  log "  consumer has env: $v"
done
role="$(env_val "$CONSUMER" app FLUENCE_COORDINATION_ROLE)"
[ "$role" = "consumer" ] || fail "consumer role=$role, want consumer"
# Absent: a consumer never submits, so no interceptor staging and no faux flag.
for v in PYTHONPATH FLUENCE_FAUX_SUBMIT; do
  ! has_env "$CONSUMER" app "$v" || fail "consumer must NOT carry '$v' (it does not submit)"
done
log "  consumer role=consumer, no interceptor/faux"

# The producer's sidecar must know which consumer group to ungate.
log "checking the producer sidecar has FLUENCE_GANG_GROUP=$GROUP"
for i in $(seq 1 30); do kubectl get pod "$PRODUCER" >/dev/null 2>&1 && break; sleep 2; done
gg="$(kubectl get pod "$PRODUCER" \
  -o jsonpath="{.spec.containers[?(@.name=='fluence-sidecar')].env[?(@.name=='FLUENCE_GANG_GROUP')].value}" \
  2>/dev/null || true)"
[ "$gg" = "$GROUP" ] || fail "producer sidecar FLUENCE_GANG_GROUP=$gg, want $GROUP"
log "  producer sidecar has FLUENCE_GANG_GROUP=$gg"

# The producer carries role=producer and is the real submitter (no consumer id).
prole="$(env_val "$PRODUCER" app FLUENCE_COORDINATION_ROLE)"
[ "$prole" = "producer" ] || fail "producer role=$prole, want producer"
if has_env "$PRODUCER" app FLUENCE_QUANTUM_JOB_ID; then
  fail "producer must NOT carry FLUENCE_QUANTUM_JOB_ID (it submits its own task)"
fi
log "  producer role=producer, submits its own task"

log "PASS 8: webhook injects the consumer(role) + producer(role) env contract at admission"

kubectl delete -f examples/test/e2e/quantum/quantum-gang-pods.yaml --wait=false || true
for g in "$GROUP" "${GROUP}-producer"; do
  kubectl patch podgroup "$g" --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
done
kubectl wait --for=delete pod -l app="$GROUP" --timeout=60s 2>/dev/null || true
