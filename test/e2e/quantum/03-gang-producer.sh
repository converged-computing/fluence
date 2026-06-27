#!/usr/bin/env bash
# Producer/consumer structure (replaces the old leader/worker and submitter-pod
# models).
#
# The structural guarantee the ungate path depends on: a shared quantum gang of
# size N is split, by completion index, into the CONSUMER gang <group>
# (minCount N-1, gated) and the PRODUCER's group-of-one <group>-producer
# (minCount 1, not gated). The producer is a real member of the user's workload —
# there is NO separate <group>-submitter pod, NO <group>-workers subgroup, and no
# leader among the user's pods. (The runtime ungate is covered by the braket
# integration test; here we prove the shape.)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; . "${HERE%/test/e2e/*}/test/e2e/lib.sh"

GROUP=qgang
PRODUCER_GROUP=${GROUP}-producer
PRODUCER=${GROUP}-0   # completion index 0
CONSUMER=${GROUP}-1   # completion index 1

log "TEST 7: consumer gang(N-1, gated) + producer(1, member) structure"
kubectl apply -f examples/test/e2e/quantum/quantum-gang-pods.yaml

# Consumer PodGroup <group> exists with minCount N-1 = 1 (the split).
log "checking consumer group '$GROUP' minCount == 1 (N-1)"
for i in $(seq 1 30); do
  gc="$(kubectl get podgroup "$GROUP" -o jsonpath='{.spec.schedulingPolicy.gang.minCount}' 2>/dev/null || true)"
  [ -n "$gc" ] && break; sleep 2
done
[ "$gc" = "1" ] || fail "consumer group $GROUP minCount=$gc, want 1 (N-1)"

# There must be NO <group>-workers subgroup and NO <group>-submitter pod.
if kubectl get podgroup "${GROUP}-workers" >/dev/null 2>&1; then
  fail "found ${GROUP}-workers PodGroup — the obsolete leader/worker split must not exist"
fi
if kubectl get pod "${GROUP}-submitter" >/dev/null 2>&1; then
  fail "found ${GROUP}-submitter pod — the obsolete separate-submitter model must not exist"
fi
log "  consumer group minCount=1, no -workers subgroup, no -submitter pod"

# Producer PodGroup <group>-producer exists with minCount 1 (schedules alone).
log "checking producer group '$PRODUCER_GROUP' minCount == 1"
for i in $(seq 1 30); do
  sc="$(kubectl get podgroup "$PRODUCER_GROUP" -o jsonpath='{.spec.schedulingPolicy.gang.minCount}' 2>/dev/null || true)"
  [ -n "$sc" ] && break; sleep 2
done
[ "$sc" = "1" ] || fail "producer group $PRODUCER_GROUP minCount=$sc, want 1"

# Producer pod (index 0) is relinked into its own group-of-one and is NOT gated.
pl="$(kubectl get pod "$PRODUCER" -o jsonpath='{.metadata.labels.fluence\.flux-framework\.org/group}' 2>/dev/null || true)"
[ "$pl" = "$PRODUCER_GROUP" ] || fail "producer group label=$pl, want $PRODUCER_GROUP"
pgate="$(kubectl get pod "$PRODUCER" -o jsonpath='{.spec.schedulingGates[0].name}' 2>/dev/null || true)"
[ -z "$pgate" ] || fail "producer must NOT be gated (gate=$pgate)"
log "  producer in '$PRODUCER_GROUP' (minCount 1), not gated"

# Consumer pod (index 1+) stays in <group> and is gated.
g="$(kubectl get pod "$CONSUMER" -o jsonpath='{.metadata.labels.fluence\.flux-framework\.org/group}' 2>/dev/null || true)"
[ "$g" = "$GROUP" ] || fail "$CONSUMER group label=$g, want $GROUP"
gate="$(kubectl get pod "$CONSUMER" -o jsonpath='{.spec.schedulingGates[0].name}' 2>/dev/null || true)"
[ "$gate" = "quantum.braket/ready" ] || fail "$CONSUMER not gated (gate=$gate)"
# The consumer's dependency points at the producer group.
dp="$(kubectl get pod "$CONSUMER" -o jsonpath='{.metadata.annotations.fluence\.flux-framework\.org/depends-on-producer}' 2>/dev/null || true)"
[ "$dp" = "$PRODUCER_GROUP" ] || fail "consumer depends-on-producer=$dp, want $PRODUCER_GROUP"
log "  consumer in '$GROUP', gated, depends on '$PRODUCER_GROUP'"

log "PASS 7: consumer gang(N-1, gated) + producer(1, member, ungates gang), no submitter/leader/worker"
kubectl delete -f examples/test/e2e/quantum/quantum-gang-pods.yaml --wait=false || true
for g in "$GROUP" "$PRODUCER_GROUP"; do
  kubectl patch podgroup "$g" --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
done
kubectl wait --for=delete pod -l app="$GROUP" --timeout=60s 2>/dev/null || true
