#!/usr/bin/env bash
# Multi-pod gang scheduling on real nodes. Guards the two failures that the
# single-pod 01 test could NOT catch (and that shipped a minCount=1 bug):
#   A) a multi-pod gang must place ALL of them (minCount must equal the gang size, not 1)
#   B) under contention, a gang that cannot fully fit stays ENTIRELY pending —
#      never partially placed (no stranded pods holding nodes).
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; . "${HERE%/test/e2e/*}/test/e2e/lib.sh"

# ---- A) all-or-nothing placement of a 3-pod gang -------------------------------
log "TEST 6A: multi-pod gang (2) places all-or-nothing"
kubectl apply -f examples/test/e2e/gang/multi-gang.yaml

# the webhook must have created the PodGroup with minCount = 2 (the bug set it to 1)
log "checking PodGroup minCount == 2 (set by webhook from group-size)"
for i in $(seq 1 30); do
  mc="$(kubectl get podgroup gang3 -o jsonpath='{.spec.schedulingPolicy.gang.minCount}' 2>/dev/null || true)"
  [ -n "$mc" ] && break; sleep 2
done
[ "$mc" = "2" ] || fail "PodGroup gang3 minCount=$mc, want 2 (minCount=1 bug -> partial gangs)"

log "waiting for all 2 gang pods to be Ready"
wait_pods_ready "app=gang3" 2 180 || fail "gang3 did not place all 2 pods (gang scheduling failed)"

count="$(kubectl get pods -l app=gang3 --field-selector=status.phase=Running --no-headers | wc -l | tr -d ' ')"
[ "$count" = "2" ] || fail "expected 2 Running gang3 pods, got $count (partial placement)"
for p in $(kubectl get pods -l app=gang3 -o name); do
  pod="${p#pod/}"
  sched="$(kubectl get pod "$pod" -o jsonpath='{.spec.schedulerName}')"
  [ "$sched" = "fluence" ] || fail "$pod not scheduled by fluence (got: $sched)"
done
log "PASS 6A: 2-pod gang placed atomically by fluence (minCount=2)"

kubectl delete -f examples/test/e2e/gang/multi-gang.yaml --wait=false || true
kubectl patch podgroup gang3 --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
kubectl wait --for=delete pod -l app=gang3 --timeout=60s 2>/dev/null || true

# ---- B) contention: the gang that can't fully fit stays ENTIRELY pending --------
log "TEST 6B: contention — a gang that cannot fully fit must NOT partially place"
kubectl apply -f examples/test/e2e/gang/multi-gang-contention.yaml

# wait until the cluster settles. Three possible outcomes:
#   - one gang fully Running, other fully Pending  -> contention; assert no partial
#   - BOTH fully Running                            -> runner big enough, no contention to test (skip)
#   - any partial (1 of 2 in a gang scheduled)      -> the bug, fail
log "waiting for gangs to settle"
winner=""; loser=""; both=""
for i in $(seq 1 90); do
  ra="$(kubectl get pods -l app=gang-a --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')"
  rb="$(kubectl get pods -l app=gang-b --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')"
  if [ "$ra" = "2" ] && [ "$rb" = "2" ]; then both=1; break; fi
  if [ "$ra" = "2" ] && [ "$rb" = "0" ]; then winner=gang-a; loser=gang-b; break; fi
  if [ "$rb" = "2" ] && [ "$ra" = "0" ]; then winner=gang-b; loser=gang-a; break; fi
  sleep 2
done

if [ -n "$both" ]; then
  log "SKIP 6B: cluster placed both gangs (>=4 schedulable cores) — no contention on this runner"
else
  [ -n "$winner" ] || fail "no clean settle: gang-a=$ra gang-b=$rb running (possible PARTIAL placement)"
  log "winner=$winner (2 running), loser=$loser (expected 0 running)"
  # the loser must have ZERO pods scheduled to a node — the all-or-nothing guarantee.
  # A single scheduled loser pod = partial placement = the bug.
  scheduled_loser="$(kubectl get pods -l app=$loser -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | grep -c . || true)"
  [ "$scheduled_loser" = "0" ] || fail "$loser has $scheduled_loser pod(s) on a node — PARTIAL placement (gang violated)"
  log "PASS 6B: $loser stayed entirely pending — no partial placement under contention"
fi

kubectl delete -f examples/test/e2e/gang/multi-gang-contention.yaml --wait=false || true
for g in gang-a gang-b; do
  kubectl patch podgroup $g --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
done
kubectl wait --for=delete pod -l app=gang-a --timeout=60s 2>/dev/null || true
kubectl wait --for=delete pod -l app=gang-b --timeout=60s 2>/dev/null || true
log "PASS: multi-gang all-or-nothing verified"
