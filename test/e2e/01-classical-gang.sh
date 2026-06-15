#!/usr/bin/env bash
# Classical gang scheduling: a PodGroup of 2 must be placed all-or-nothing on real nodes.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; . "${HERE}/lib.sh"
NS=default

log "TEST 1: classical gang scheduling"
kubectl apply -f examples/podgroup.yaml

# All pods in the 'training' deployment must reach Running (scheduled + started).
log "waiting for both training pods to schedule"
kubectl wait --for=condition=Ready pod -l app=training -n "$NS" --timeout="${TIMEOUT}" \
  || fail "training gang did not all become Ready (gang scheduling failed)"

# Each pod must have a real node assigned by fluence.
for p in $(kubectl get pods -l app=training -n "$NS" -o name); do
  pod="${p#pod/}"
  assert_scheduled "$pod" "$NS" || fail "$pod has no nodeName"
  sched="$(kubectl get pod "$pod" -n "$NS" -o jsonpath='{.spec.schedulerName}')"
  [ "$sched" = "fluence" ] || fail "$pod was not scheduled by fluence (got: $sched)"
done

count="$(kubectl get pods -l app=training -n "$NS" --no-headers | wc -l | tr -d ' ')"
[ "$count" = "2" ] || fail "expected 2 training pods, got $count"

log "PASS: classical gang placed all $count pods via fluence"
kubectl delete -f examples/podgroup.yaml --wait=false || true
kubectl patch podgroup training -n "$NS" --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
