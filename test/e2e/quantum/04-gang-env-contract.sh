#!/usr/bin/env bash
# Env-contract e2e (gang + submitter): verify the webhook injects, at admission,
# the env the runtime depends on — IN-CLUSTER, on the real pod specs, with no
# Braket/AWS and WITHOUT requiring scheduling. Guards the seam that, if broken,
# makes a gang schedule then hang or double-submit.
#
# Spec layer only (these are downward-API valueFrom refs whose VALUES resolve at
# placement, but whose PRESENCE is deterministic at admission), so no scheduling,
# no qpu capacity, no logs — it cannot flake on capacity. Contract:
#   gang pod (faux):  FLUENCE_FAUX_SUBMIT, FLUENCE_QUANTUM_JOB_ID, PYTHONPATH, FLUXION_BACKEND
#   submitter:        FLUENCE_GANG_GROUP on the sidecar (real submit, ungates the gang)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; . "${HERE%/test/e2e/*}/test/e2e/lib.sh"

GROUP=qgang
SUBMITTER=${GROUP}-submitter

log "TEST 8: gang+submitter env contract — spec layer"
kubectl apply -f examples/test/e2e/quantum/quantum-gang-pods.yaml

# does container $2 of pod $1 have an env entry named $3 ? (spec-level only)
has_env() {
  kubectl get pod "$1" -o jsonpath="{.spec.containers[?(@.name=='$2')].env[*].name}" \
    2>/dev/null | tr ' ' '\n' | grep -qx "$3"
}

log "checking the webhook wired the faux contract onto a gang pod"
for i in $(seq 1 15); do has_env ${GROUP}-0 app FLUENCE_FAUX_SUBMIT && break; sleep 2; done
for v in FLUENCE_FAUX_SUBMIT FLUENCE_QUANTUM_JOB_ID PYTHONPATH FLUXION_BACKEND; do
  has_env ${GROUP}-0 app "$v" \
    || { kubectl get pod ${GROUP}-0 -o yaml | sed -n '/containers:/,/status:/p'; \
         fail "gang pod 'app' container missing env '$v'"; }
  log "  gang pod has env: $v"
done

# The submitter's sidecar must know which gang to ungate.
log "checking the submitter sidecar has FLUENCE_GANG_GROUP=$GROUP"
for i in $(seq 1 30); do kubectl get pod "$SUBMITTER" >/dev/null 2>&1 && break; sleep 2; done
gg="$(kubectl get pod "$SUBMITTER" \
  -o jsonpath="{.spec.containers[?(@.name=='fluence-sidecar')].env[?(@.name=='FLUENCE_GANG_GROUP')].value}" \
  2>/dev/null || true)"
[ "$gg" = "$GROUP" ] || fail "submitter sidecar FLUENCE_GANG_GROUP=$gg, want $GROUP"
log "  submitter sidecar has FLUENCE_GANG_GROUP=$gg"

# And the submitter must NOT be in faux mode (it does the real submit).
if has_env "$SUBMITTER" app FLUENCE_FAUX_SUBMIT; then
  fail "submitter must NOT carry FLUENCE_FAUX_SUBMIT (it submits for real)"
fi
log "  submitter is not faux"

log "PASS 8: webhook injects the gang(faux) + submitter(real) env contract at admission"

kubectl delete -f examples/test/e2e/quantum/quantum-gang-pods.yaml --wait=false || true
kubectl delete pod "$SUBMITTER" --wait=false 2>/dev/null || true
for g in "$GROUP" "$SUBMITTER"; do
  kubectl patch podgroup "$g" --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
done
kubectl wait --for=delete pod -l app="$GROUP" --timeout=60s 2>/dev/null || true
