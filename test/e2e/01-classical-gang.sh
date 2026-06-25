#!/usr/bin/env bash
# Classical gang scheduling: a PodGroup of 2 must be placed all-or-nothing on real nodes.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; . "${HERE}/lib.sh"

log "TEST 1: classical gang scheduling"
kubectl apply -f examples/single-podgroup.yaml

# All pods in the 'training' deployment must reach Running (scheduled + started).
# Wait for the pod to EXIST before waiting for Ready — kubectl wait errors out
# immediately if the Deployment's pod hasn't been registered yet (a race that
# fails the test at 0s with "no matching resources found").
log "waiting for both training pods to schedule"
wait_pods_ready "app=training" 1 180 || fail "training gang did not all become Ready (gang scheduling failed)"

# Each pod must have a real node assigned by fluence.
for p in $(kubectl get pods -l app=training -o name); do
  pod="${p#pod/}"
  assert_scheduled "$pod" || fail "$pod has no nodeName"
  sched="$(kubectl get pod "$pod" -o jsonpath='{.spec.schedulerName}')"
  [ "$sched" = "fluence" ] || fail "$pod was not scheduled by fluence (got: $sched)"
done

count="$(kubectl get pods -l app=training --no-headers | wc -l | tr -d ' ')"
[ "$count" = "1" ] || fail "expected 2 training pods, got $count"

log "PASS: classical gang placed all $count pods via fluence"
kubectl delete -f examples/single-podgroup.yaml --wait=false || true
kubectl patch podgroup training --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
# Wait for the pods to actually be gone before the next test runs — otherwise a
# terminating 'training' pod (same name/labels reused by other scenarios) can be
# misread as the next test's placement.
kubectl wait --for=delete pod -l app=training --timeout=60s 2>/dev/null || true
