#!/usr/bin/env bash
# Requeue-on-capacity + gang atomicity under contention.
#
# Two 2-pod gangs contend for a cluster that can only run one at a time. This
# guards two invariants that the GKE contention runs exposed:
#   1. ALL-OR-NOTHING: each gang places ALL its pods or NONE — never a partial
#      (e.g. 1-of-2 scheduled). The winner must be a clean 2/2; the loser a clean
#      0/2 while it waits.
#   2. REQUEUE: when the winner completes and frees its nodes, the loser is
#      re-attempted on its own (no manual nudge) and then ALSO places atomically
#      (2/2), driven by the shortened --pod-max-in-unschedulable-pods-duration.
#
# SCOPE / LIMITATION: this is a 3-node kind cluster with small (1-core) pods. It
# verifies the INVARIANTS on a minimal contention case. It does NOT reproduce the
# GKE-scale dynamics where the bug was first seen — one-pod-per-node (~80-core)
# saturation and ~20 simultaneous mixed-size gangs draining in sequence. That
# scale behavior is validated on the real cluster, not in CI; a pass here means
# the invariants hold on the simple case, not that large-scale draining is proven.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; . "${HERE%/test/e2e/*}/test/e2e/lib.sh"

# running-pod count for a gang (job-name label set by the Job controller)
running() { kubectl get pods -l job-name="$1" --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' '; }
# count of a gang's pods actually bound to a node (Running OR already Succeeded)
on_nodes() { kubectl get pods -l job-name="$1" -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' 2>/dev/null | grep -c . || true; }

log "TEST 9: contended gangs stay all-or-nothing, loser requeues when capacity frees"
kubectl apply -f examples/test/e2e/gang/multi-gang-requeue.yaml

# ---- 1. one gang wins CLEANLY (2/2); the other places NOTHING (0/2) ------------
log "waiting for a clean 2/0 split (one whole gang runs, the other entirely waits)"
winner=""; loser=""
for i in $(seq 1 60); do
  rw="$(running gang-win)"; ra="$(running gang-wait)"
  if [ "$rw" = "2" ] && [ "$ra" = "0" ]; then winner=gang-win;  loser=gang-wait; break; fi
  if [ "$ra" = "2" ] && [ "$rw" = "0" ]; then winner=gang-wait; loser=gang-win;  break; fi
  # a 1/x or x/1 state that persists is a PARTIAL gang — fail fast on it
  if [ "$rw" = "1" ] || [ "$ra" = "1" ]; then
    sleep 6  # allow a transient mid-bind moment to resolve
    rw="$(running gang-win)"; ra="$(running gang-wait)"
    { [ "$rw" = "1" ] || [ "$ra" = "1" ]; } && \
      fail "PARTIAL gang: gang-win=$rw gang-wait=$ra running (all-or-nothing violated)"
  fi
  sleep 2
done
[ -n "$winner" ] || fail "no clean 2/0 split (gang-win=$(running gang-win) gang-wait=$(running gang-wait))"
log "  winner=$winner (2/2 running), loser=$loser"

# loser must have ZERO pods on any node — not even one (that would be a partial)
sl="$(on_nodes "$loser")"
[ "$sl" = "0" ] || fail "$loser has $sl pod(s) bound while it should be entirely pending — PARTIAL placement"
log "  $loser entirely pending (0 pods bound) — all-or-nothing holds"

# ---- 2. winner completes -> loser is requeued AND places atomically ------------
log "waiting for winner=$winner to complete and free its nodes"
kubectl wait --for=condition=complete job/$winner --timeout=120s || fail "$winner did not complete"
log "  $winner completed; capacity freed"

# The loser must now place ALL its pods (2/2), on its own, within a window above
# the 30s recheck flush but below the 5m default — proving the shortened timeout
# is in effect AND that the requeued gang is still atomic (not a partial).
log "asserting $loser requeues and places ATOMICALLY (2/2) within ~75s"
ok=""
for i in $(seq 1 38); do   # ~75s
  rl="$(running $loser)"
  dl="$(kubectl get pods -l job-name=$loser --field-selector=status.phase=Succeeded --no-headers 2>/dev/null | wc -l | tr -d ' ')"
  # both pods accounted for (running and/or already completed) = atomic placement
  [ "$((rl + dl))" = "2" ] && { ok=1; break; }
  # a lone 1/2 that lingers = partial placement of the requeued gang
  if [ "$((rl + dl))" = "1" ]; then
    sleep 6
    rl="$(running $loser)"; dl="$(kubectl get pods -l job-name=$loser --field-selector=status.phase=Succeeded --no-headers 2>/dev/null | wc -l | tr -d ' ')"
    [ "$((rl + dl))" = "1" ] && fail "$loser placed 1 of 2 pods — PARTIAL placement of the requeued gang"
  fi
  sleep 2
done
[ -n "$ok" ] || fail "$loser did NOT place both pods within 75s of capacity freeing — \
either the shortened --pod-max-in-unschedulable-pods-duration is not taking effect \
(gang stuck) or the requeued gang did not assemble"
log "PASS 9: $loser requeued and placed atomically (2/2) after $winner freed capacity"

kubectl delete -f examples/test/e2e/gang/multi-gang-requeue.yaml --wait=false || true
for g in gang-win gang-wait; do
  kubectl patch podgroup $g --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
done
kubectl wait --for=delete pod -l job-name=gang-win  --timeout=60s 2>/dev/null || true
kubectl wait --for=delete pod -l job-name=gang-wait --timeout=60s 2>/dev/null || true
