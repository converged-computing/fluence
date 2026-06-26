#!/usr/bin/env bash
# Multi-pod gang scheduling on real nodes. Guards the two failures that the
# single-pod 01 test could NOT catch (and that shipped a minCount=1 bug):
#   A) a 3-pod gang must place ALL 3 (minCount must equal the gang size, not 1)
#   B) under contention, a gang that cannot fully fit stays ENTIRELY pending —
#      never partially placed (no stranded pods holding nodes).
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; . "${HERE}/lib.sh"

# ---- A) all-or-nothing placement of a 3-pod gang -------------------------------
log "TEST 6A: multi-pod gang (3) places all-or-nothing"
kubectl apply -f examples/multi-gang.yaml

# the webhook must have created the PodGroup with minCount = 3 (the bug set it to 1)
log "checking PodGroup minCount == 3 (set by webhook from group-size)"
for i in $(seq 1 30); do
  mc="$(kubectl get podgroup gang3 -o jsonpath='{.spec.schedulingPolicy.gang.minCount}' 2>/dev/null || true)"
  [ -n "$mc" ] && break; sleep 2
done
[ "$mc" = "3" ] || fail "PodGroup gang3 minCount=$mc, want 3 (minCount=1 bug -> partial gangs)"

log "waiting for all 3 gang pods to be Ready"
wait_pods_ready "app=gang3" 3 180 || fail "gang3 did not place all 3 pods (gang scheduling failed)"

count="$(kubectl get pods -l app=gang3 --field-selector=status.phase=Running --no-headers | wc -l | tr -d ' ')"
[ "$count" = "3" ] || fail "expected 3 Running gang3 pods, got $count (partial placement)"
for p in $(kubectl get pods -l app=gang3 -o name); do
  pod="${p#pod/}"
  sched="$(kubectl get pod "$pod" -o jsonpath='{.spec.schedulerName}')"
  [ "$sched" = "fluence" ] || fail "$pod not scheduled by fluence (got: $sched)"
done
log "PASS 6A: 3-pod gang placed atomically by fluence (minCount=3)"

kubectl delete -f examples/multi-gang.yaml --wait=false || true
kubectl patch podgroup gang3 --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
kubectl wait --for=delete pod -l app=gang3 --timeout=60s 2>/dev/null || true

# ---- B) contention: the gang that can't fully fit stays ENTIRELY pending --------
log "TEST 6B: contention — a gang that cannot fully fit must NOT partially place"
kubectl apply -f examples/multi-gang-contention.yaml

# wait until the cluster settles: exactly one gang fully Running, the other fully Pending.
log "waiting for one gang to win placement"
winner=""; loser=""
for i in $(seq 1 90); do
  ra="$(kubectl get pods -l app=gangA --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')"
  rb="$(kubectl get pods -l app=gangB --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')"
  if [ "$ra" = "2" ] && [ "$rb" = "0" ]; then winner=gangA; loser=gangB; break; fi
  if [ "$rb" = "2" ] && [ "$ra" = "0" ]; then winner=gangB; loser=gangA; break; fi
  sleep 2
done
[ -n "$winner" ] || fail "neither gang reached a clean 2/0 placement (check for partial placement)"
log "winner=$winner (2 running), loser=$loser (expected 0 running)"

# the loser must have ZERO pods scheduled to a node — the all-or-nothing guarantee.
# A single scheduled loser pod = partial placement = the bug.
scheduled_loser="$(kubectl get pods -l app=$loser -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | grep -c . || true)"
[ "$scheduled_loser" = "0" ] || fail "$loser has $scheduled_loser pod(s) on a node — PARTIAL placement (gang violated)"
log "PASS 6B: $loser stayed entirely pending — no partial placement under contention"

kubectl delete -f examples/multi-gang-contention.yaml --wait=false || true
for g in gangA gangB; do
  kubectl patch podgroup $g --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
done
kubectl wait --for=delete pod -l app=gangA --timeout=60s 2>/dev/null || true
kubectl wait --for=delete pod -l app=gangB --timeout=60s 2>/dev/null || true
log "PASS: multi-gang all-or-nothing verified"
