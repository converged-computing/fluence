#!/usr/bin/env bash
# Shared helpers for fluence e2e tests. Sourced by each scenario script.
set -euo pipefail

NS_KUBE="${NS_KUBE:-kube-system}"
TIMEOUT="${TIMEOUT:-180s}"

log()  { echo "=== $*"; }
fail() { echo "FAIL: $*" >&2; dump; exit 1; }

# Dump cluster state to help debug a CI failure.
dump() {
  echo "----- pods (all namespaces) -----"
  kubectl get pods -A -o wide || true
  echo "----- events -----"
  kubectl get events -A --sort-by=.lastTimestamp | tail -40 || true
  echo "----- fluence scheduler logs -----"
  kubectl logs -n "${NS_KUBE}" deployment/fluence --tail=120 || true
}

# Wait until the fluence scheduler deployment is Available.
wait_fluence_ready() {
  log "waiting for fluence scheduler to be ready"
  kubectl rollout status -n "${NS_KUBE}" deployment/fluence --timeout="${TIMEOUT}" \
    || fail "fluence deployment did not become ready"
}

# wait_pod_phase <pod> <namespace> <phase> [timeout]
wait_pod_phase() {
  local pod="$1" ns="$2" want="$3" t="${4:-120}"
  local i=0
  while [ "$i" -lt "$t" ]; do
    local got
    got="$(kubectl get pod "$pod" -n "$ns" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    [ "$got" = "$want" ] && return 0
    sleep 1; i=$((i+1))
  done
  return 1
}

# assert_scheduled <pod> <namespace>: pod has a node assigned.
assert_scheduled() {
  local pod="$1" ns="$2" node
  node="$(kubectl get pod "$pod" -n "$ns" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
  [ -n "$node" ] || return 1
  echo "  $pod scheduled on $node"
}

# Assert a pod stays Pending for the whole window (used for the "must not double-book" check).
assert_stays_pending() {
  local pod="$1" ns="$2" t="${3:-30}" i=0
  while [ "$i" -lt "$t" ]; do
    local node
    node="$(kubectl get pod "$pod" -n "$ns" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
    [ -n "$node" ] && return 1   # got scheduled -> failure for this assertion
    sleep 1; i=$((i+1))
  done
  return 0
}
