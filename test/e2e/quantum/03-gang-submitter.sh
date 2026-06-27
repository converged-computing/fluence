#!/usr/bin/env bash
# Gang + submitter structure (replaces the old leader/worker split).
#
# The structural guarantee the ungate path depends on: a quantum gang of size N
# is ONE fully-gated PodGroup <group> (minCount N), and Fluence creates a
# SEPARATE submitter pod in its OWN group-of-one <group>-submitter (minCount 1,
# not gated) that does the real submit and ungates the gang. There is no
# <group>-workers subgroup and no leader among the user's pods. (The runtime
# ungate is covered by the braket integration test; here we prove the shape.)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; . "${HERE%/test/e2e/*}/test/e2e/lib.sh"

GROUP=qgang
SUBMITTER=${GROUP}-submitter

log "TEST 7: gang(N, gated) + separate submitter(1) structure"
kubectl apply -f examples/test/e2e/quantum/quantum-gang-pods.yaml

# Gang PodGroup <group> exists with minCount N=2 (full gang, no split).
log "checking gang group '$GROUP' minCount == 2 (full N)"
for i in $(seq 1 30); do
  gc="$(kubectl get podgroup "$GROUP" -o jsonpath='{.spec.schedulingPolicy.gang.minCount}' 2>/dev/null || true)"
  [ -n "$gc" ] && break; sleep 2
done
[ "$gc" = "2" ] || fail "gang group $GROUP minCount=$gc, want 2 (full N)"

# There must be NO <group>-workers subgroup (the old split is gone).
if kubectl get podgroup "${GROUP}-workers" >/dev/null 2>&1; then
  fail "found ${GROUP}-workers PodGroup — the obsolete leader/worker split must not exist"
fi
log "  gang group minCount=2, no -workers subgroup"

# Submitter PodGroup <group>-submitter exists with minCount 1 (schedules alone).
log "checking submitter group '$SUBMITTER' minCount == 1"
for i in $(seq 1 30); do
  sc="$(kubectl get podgroup "$SUBMITTER" -o jsonpath='{.spec.schedulingPolicy.gang.minCount}' 2>/dev/null || true)"
  [ -n "$sc" ] && break; sleep 2
done
[ "$sc" = "1" ] || fail "submitter group $SUBMITTER minCount=$sc, want 1"

# Submitter pod records the gang group it ungates, and is its own group.
gg="$(kubectl get pod "$SUBMITTER" -o jsonpath='{.metadata.annotations.fluence\.flux-framework\.org/gang-group}' 2>/dev/null || true)"
[ "$gg" = "$GROUP" ] || fail "submitter gang-group annotation=$gg, want $GROUP"
sl="$(kubectl get pod "$SUBMITTER" -o jsonpath='{.metadata.labels.fluence\.flux-framework\.org/group}' 2>/dev/null || true)"
[ "$sl" = "$SUBMITTER" ] || fail "submitter group label=$sl, want $SUBMITTER"
log "  submitter group minCount=1, ungates gang '$GROUP'"

# Gang pods stay in <group> (NOT relinked) and are gated.
for p in ${GROUP}-0 ${GROUP}-1; do
  g="$(kubectl get pod "$p" -o jsonpath='{.metadata.labels.fluence\.flux-framework\.org/group}' 2>/dev/null || true)"
  [ "$g" = "$GROUP" ] || fail "$p group label=$g, want $GROUP (gang pods must not be relinked)"
  gate="$(kubectl get pod "$p" -o jsonpath='{.spec.schedulingGates[0].name}' 2>/dev/null || true)"
  [ "$gate" = "quantum.braket/ready" ] || fail "$p not gated (gate=$gate)"
done
log "  gang pods remain in '$GROUP' and are gated"

log "PASS 7: gang(N=2, gated) + submitter(1, ungates gang), no leader/worker split"
kubectl delete -f examples/test/e2e/quantum/quantum-gang-pods.yaml --wait=false || true
kubectl delete pod "$SUBMITTER" --wait=false 2>/dev/null || true
for g in "$GROUP" "$SUBMITTER"; do
  kubectl patch podgroup "$g" --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
done
kubectl wait --for=delete pod -l app="$GROUP" --timeout=60s 2>/dev/null || true
