#!/usr/bin/env bash
# PostFilter re-match: when another scheduler plugin (TaintToleration) rejects a
# node Fluxion allocated, Fluence must abandon that allocation, exclude the node,
# and re-match onto an untainted node. Safety: the gang's RUNNING pod must NEVER
# bind to the tainted node.
#
# This test is self-isolating: it uses its own workload name (pf-rematch) and
# labels, distinct from the other e2e scenarios, and ensures a clean slate first,
# so a pod left over (terminating) from a previous test can never be mistaken for
# this test's placement. It also ignores terminating pods when asserting.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; . "${HERE}/lib.sh"

NAME=pf-rematch
SEL="app=${NAME}"

log "TEST 5: PostFilter abandons a taint-rejected allocation and re-matches"

# --- clean slate: no leftover pods from earlier tests under our name ----------
kubectl delete deployment "$NAME" --ignore-not-found >/dev/null 2>&1 || true
kubectl delete podgroup "$NAME" --ignore-not-found >/dev/null 2>&1 || true
kubectl patch podgroup "$NAME" --type=merge \
  -p '{"metadata":{"finalizers":null}}' >/dev/null 2>&1 || true
kubectl wait --for=delete pod -l "$SEL" --timeout=60s >/dev/null 2>&1 || true

TAINTED="$(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' \
  -o jsonpath='{.items[0].metadata.name}')"
[ -n "$TAINTED" ] || fail "no worker node found to taint"
log "tainting node $TAINTED with fluence-e2e=blocked:NoSchedule"
kubectl taint nodes "$TAINTED" fluence-e2e=blocked:NoSchedule --overwrite

cleanup() {
  kubectl taint nodes "$TAINTED" fluence-e2e- 2>/dev/null || true
  kubectl delete deployment "$NAME" --ignore-not-found --wait=false 2>/dev/null || true
  kubectl delete podgroup "$NAME" --ignore-not-found --wait=false 2>/dev/null || true
  kubectl patch podgroup "$NAME" --type=merge \
    -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
}
trap cleanup EXIT

# --- our own workload (distinct name/labels; does NOT tolerate the taint) ------
kubectl apply -f - <<YAML
apiVersion: scheduling.k8s.io/v1alpha2
kind: PodGroup
metadata:
  name: ${NAME}
spec:
  schedulingPolicy:
    gang:
      minCount: 1
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${NAME}
spec:
  replicas: 1
  selector:
    matchLabels: {app: ${NAME}}
  template:
    metadata:
      labels: {app: ${NAME}}
    spec:
      schedulerName: fluence
      schedulingGroup:
        podGroupName: ${NAME}
      containers:
        - name: worker
          image: busybox
          command: ["sleep", "3600"]
          resources:
            requests:
              cpu: "1"
YAML

log "waiting for the gang to schedule (must avoid the tainted node)"
wait_pods_ready "$SEL" 1 180 \
  || fail "gang never became Ready — PostFilter re-match did not recover (likely stuck on the taint-rejected allocation)"

# SAFETY: among NON-terminating (Running, no deletionTimestamp) pods, none may be
# on the tainted node. Terminating leftovers are ignored by construction (we use
# a unique name and cleaned the slate), but we still filter defensively.
checked=0
while read -r name node deleted; do
  [ -z "$name" ] && continue
  # custom-columns prints "<none>" for empty fields, so an empty deletionTimestamp
  # shows as "<none>", NOT "". Treat "<none>" as empty for both columns.
  if [ "$deleted" != "<none>" ] && [ -n "$deleted" ]; then continue; fi   # skip terminating
  if [ "$node" = "<none>" ] || [ -z "$node" ]; then continue; fi          # skip not-yet-bound
  checked=$((checked+1))
  if [ "$node" = "$TAINTED" ]; then
    fail "SAFETY VIOLATION: running pod $name is bound to the tainted node $TAINTED"
  fi
  log "$name correctly placed on $node (not the tainted $TAINTED)"
done < <(kubectl get pods -l "$SEL" \
  -o custom-columns='N:.metadata.name,NODE:.spec.nodeName,DEL:.metadata.deletionTimestamp' \
  --no-headers)

[ "$checked" -ge 1 ] || fail "no running ${NAME} pod found to check"

# Informational: did PostFilter actually fire (Fluxion picked the tainted node
# first and we re-matched), or did Fluxion place on the good node directly?
POD="$(kubectl -n kube-system get pods -l app=fluence \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
if [ -n "$POD" ] && kubectl -n kube-system logs "$POD" 2>/dev/null \
     | grep -q "unschedulable: abandoning allocation"; then
  log "observed PostFilter abandonment in scheduler log (re-match path exercised)"
else
  log "note: Fluxion placed on the untainted node directly this run (PostFilter not needed)"
fi

log "PASS: gang scheduled on an untainted node; no running pod on the tainted node"
