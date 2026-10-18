#!/usr/bin/env bash
# Scout/worker structure (replaces the old leader/worker and submitter-pod
# models).
#
# The structural guarantee the ungate path depends on. A shared quantum gang of
# size N is split, by completion index, into the WORKER gang <group>
# (minCount N-1, gated) and the SCOUT's group-of-one <group>-scout
# (minCount 1, not gated). The scout is a real member of the user's workload
# there is NO separate <group>-submitter pod, NO <group>-workers subgroup, and no
# leader among the user's pods. (The runtime ungate is covered by the braket
# integration test. Here we prove the shape.)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; . "${HERE%/test/e2e/*}/test/e2e/lib.sh"

GROUP=qgang
SCOUT_GROUP=${GROUP}-scout
SCOUT=${GROUP}-0   # completion index 0
WORKER=${GROUP}-1   # completion index 1

log "TEST 7: worker gang(N-1, gated) + scout(1, member) structure"
kubectl apply -f examples/test/e2e/quantum/quantum-gang-pods.yaml

# Worker PodGroup <group> exists with minCount N-1 = 1 (the split).
log "checking worker group '$GROUP' minCount == 1 (N-1)"
for i in $(seq 1 30); do
  gc="$(kubectl get podgroup "$GROUP" -o jsonpath='{.spec.schedulingPolicy.gang.minCount}' 2>/dev/null || true)"
  [ -n "$gc" ] && break; sleep 2
done
[ "$gc" = "1" ] || fail "worker group $GROUP minCount=$gc, want 1 (N-1)"

# There must be NO <group>-workers subgroup and NO <group>-submitter pod.
if kubectl get podgroup "${GROUP}-workers" >/dev/null 2>&1; then
  fail "found ${GROUP}-workers PodGroup, the obsolete leader/worker split must not exist"
fi
if kubectl get pod "${GROUP}-submitter" >/dev/null 2>&1; then
  fail "found ${GROUP}-submitter pod, the obsolete separate-submitter model must not exist"
fi
log "  worker group minCount=1, no -workers subgroup, no -submitter pod"

# Scout PodGroup <group>-scout exists with minCount 1 (schedules alone).
log "checking scout group '$SCOUT_GROUP' minCount == 1"
for i in $(seq 1 30); do
  sc="$(kubectl get podgroup "$SCOUT_GROUP" -o jsonpath='{.spec.schedulingPolicy.gang.minCount}' 2>/dev/null || true)"
  [ -n "$sc" ] && break; sleep 2
done
[ "$sc" = "1" ] || fail "scout group $SCOUT_GROUP minCount=$sc, want 1"

# Scout pod (index 0) is relinked into its own group-of-one and is NOT gated.
pl="$(kubectl get pod "$SCOUT" -o jsonpath='{.metadata.labels.fluence\.flux-framework\.org/group}' 2>/dev/null || true)"
[ "$pl" = "$SCOUT_GROUP" ] || fail "scout group label=$pl, want $SCOUT_GROUP"
pgate="$(kubectl get pod "$SCOUT" -o jsonpath='{.spec.schedulingGates[0].name}' 2>/dev/null || true)"
[ -z "$pgate" ] || fail "scout must NOT be gated (gate=$pgate)"
log "  scout in '$SCOUT_GROUP' (minCount 1), not gated"

# Worker pod (index 1+) stays in <group> and is gated.
g="$(kubectl get pod "$WORKER" -o jsonpath='{.metadata.labels.fluence\.flux-framework\.org/group}' 2>/dev/null || true)"
[ "$g" = "$GROUP" ] || fail "$WORKER group label=$g, want $GROUP"
gate="$(kubectl get pod "$WORKER" -o jsonpath='{.spec.schedulingGates[0].name}' 2>/dev/null || true)"
[ "$gate" = "quantum.braket/ready" ] || fail "$WORKER not gated (gate=$gate)"
# The worker's dependency points at the scout group.
dp="$(kubectl get pod "$WORKER" -o jsonpath='{.metadata.annotations.fluence\.flux-framework\.org/depends-on-scout}' 2>/dev/null || true)"
[ "$dp" = "$SCOUT_GROUP" ] || fail "worker depends-on-scout=$dp, want $SCOUT_GROUP"
log "  worker in '$GROUP', gated, depends on '$SCOUT_GROUP'"

log "PASS 7: worker gang(N-1, gated) + scout(1, member, ungates gang), no submitter/leader/worker"
kubectl delete -f examples/test/e2e/quantum/quantum-gang-pods.yaml --wait=false || true
for g in "$GROUP" "$SCOUT_GROUP"; do
  kubectl patch podgroup "$g" --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
done
kubectl wait --for=delete pod -l app="$GROUP" --timeout=60s 2>/dev/null || true
