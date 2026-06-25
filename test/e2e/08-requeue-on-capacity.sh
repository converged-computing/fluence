#!/usr/bin/env bash
# Requeue-on-capacity: a contended gang that loses the initial race must be
# RE-ATTEMPTED when the winner completes and frees nodes — driven by fluence's
# EventsToRegister, with no manual nudge. Guards the gap where Unschedulable
# gangs only woke on the backoff timer (so contended gangs stalled instead of
# draining as capacity freed).
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; . "${HERE}/lib.sh"

log "TEST 9: contended gang is requeued when capacity frees (EventsToRegister)"
kubectl apply -f examples/multi-gang-requeue.yaml

# Both gangs want the same nodes; only one fits. Identify winner/loser.
log "waiting for one gang to win and the other to be Unschedulable"
winner=""; loser=""
for i in $(seq 1 60); do
  rw="$(kubectl get pods -l job-name=gang-win   --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')"
  ra="$(kubectl get pods -l job-name=gang-wait  --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')"
  if [ "$rw" = "2" ] && [ "$ra" = "0" ]; then winner=gang-win;  loser=gang-wait; break; fi
  if [ "$ra" = "2" ] && [ "$rw" = "0" ]; then winner=gang-wait; loser=gang-win;  break; fi
  sleep 2
done
[ -n "$winner" ] || fail "no gang won a clean 2/0 placement (check capacity/contention)"
log "winner=$winner running; loser=$loser should be Unschedulable"

# the loser's PodGroup should be Unschedulable (entirely pending — all-or-nothing)
for i in $(seq 1 15); do
  st="$(kubectl get podgroup "$loser" -o jsonpath='{.status.conditions[*].type}{" "}{.status}' 2>/dev/null || true)"
  echo "$st" | grep -qi "unschedulable\|pending" && break
  # status field name varies; also accept: zero loser pods scheduled
  sched="$(kubectl get pods -l job-name=$loser -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | grep -c . || true)"
  [ "$sched" = "0" ] && break
  sleep 2
done
sched_loser="$(kubectl get pods -l job-name=$loser -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | grep -c . || true)"
[ "$sched_loser" = "0" ] || fail "$loser partially placed ($sched_loser pods on nodes) — gang violated"
log "  $loser is entirely pending (no pods placed) — correct"

# THE KEY ASSERTION: when the winner COMPLETES (frees nodes), the loser must be
# requeued and run — WITHOUT us touching it. The winner sleeps ~30s.
log "waiting for winner=$winner to complete and free capacity"
kubectl wait --for=condition=complete job/$winner --timeout=120s \
  || fail "$winner did not complete"
log "  $winner completed; capacity freed"

log "asserting $loser is now requeued and runs (EventsToRegister woke it)"
ok=""
for i in $(seq 1 60); do   # up to ~120s; must be well under the 5-min backoff flush
  rl="$(kubectl get pods -l job-name=$loser --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')"
  done_l="$(kubectl get pods -l job-name=$loser --field-selector=status.phase=Succeeded --no-headers 2>/dev/null | wc -l | tr -d ' ')"
  if [ "$((rl + done_l))" -ge 1 ]; then ok=1; break; fi
  sleep 2
done
[ -n "$ok" ] || fail "$loser was NOT requeued after capacity freed within 120s — \
EventsToRegister not waking unschedulable gangs (would only recover on the 5-min backoff flush)"
log "PASS 9: $loser was requeued and scheduled after $winner freed capacity"

kubectl delete -f examples/multi-gang-requeue.yaml --wait=false || true
for g in gang-win gang-wait; do
  kubectl patch podgroup $g --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
done
kubectl wait --for=delete pod -l job-name=gang-win  --timeout=60s 2>/dev/null || true
kubectl wait --for=delete pod -l job-name=gang-wait --timeout=60s 2>/dev/null || true