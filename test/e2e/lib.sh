#!/usr/bin/env bash
# Shared helpers for fluence e2e tests. Sourced by each scenario script.
set -euo pipefail

NS_KUBE="${NS_KUBE:-kube-system}"
TIMEOUT="${TIMEOUT:-180s}"

log()  { echo "=== $*"; }

# Wait until at least N pods matching a label selector EXIST, then wait for them
# to be Ready. `kubectl wait` errors immediately ("no matching resources found")
# if the pods are not yet registered, so a Deployment that has only just been
# applied races it, wait for existence first.
wait_pods_ready() {
  local selector="$1" want="${2:-1}" timeout="${3:-180}"
  local deadline=$(( $(date +%s) + timeout ))
  while :; do
    local n
    n="$(kubectl get pods -l "$selector" --no-headers 2>/dev/null | wc -l | tr -d ' ')"
    [ "${n:-0}" -ge "$want" ] && break
    [ "$(date +%s)" -ge "$deadline" ] && return 1
    sleep 2
  done
  kubectl wait --for=condition=Ready pod -l "$selector" --timeout="${timeout}s"
}
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

show_webhook() {
  pod=$1
  echo "FAIL: FLUXION_BACKEND mismatch"
  kubectl get pod $pod -o jsonpath='{.spec.containers[0].env}'; echo
  kubectl get pod $pod -o jsonpath='{.metadata.annotations}'; echo
  kubectl -n kube-system logs deploy/fluence-webhook --tail=50
}

# wait_pod_phase <pod> <namespace> <phase> [timeout]
wait_pod_phase() {
  local pod="$1" want="$2" t="${3:-120}"
  echo "pod: $pod"
  echo "want: $want"
  echo "t: $t"
  local i=0
  while [ "$i" -lt "$t" ]; do
    local got
    got="$(kubectl get pod "$pod" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    [ "$got" = "$want" ] && return 0
    sleep 1; i=$((i+1))
  done
  return 1
}

# assert_scheduled <pod> <namespace>: pod has a node assigned.
assert_scheduled() {
  local pod="$1" node
  node="$(kubectl get pod "$pod" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
  [ -n "$node" ] || return 1
  echo "  $pod scheduled on $node"
}

# Assert a pod stays Pending for the whole window (used for the "must not double-book" check).
assert_stays_pending() {
  local pod="$1" t="${2:-30}" i=0
  while [ "$i" -lt "$t" ]; do
    local node
    node="$(kubectl get pod "$pod" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
    [ -n "$node" ] && return 1   # got scheduled -> failure for this assertion
    sleep 1; i=$((i+1))
  done
  return 0
}

# cpu quantities to millicores, so "4" and "250m" add up
millicores() { awk '{ if ($0 ~ /m$/) { sub(/m$/, ""); t += $0 } else if ($0 != "") { t += $0 * 1000 } } END { print t + 0 }'; }

# Whole cores free on the fullest worker. Allocatable less what the pods already
# on it request, which is the arithmetic the kube-scheduler's fit check does. A
# pod that asks for this fills a worker, so a two pod gang takes two workers and
# two gangs contend, on any runner, with nothing assumed about the system pods.
free_cores_per_worker() {
  local min="" node alloc used free
  for node in $(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' -o jsonpath='{.items[*].metadata.name}'); do
    alloc="$(kubectl get node "$node" -o jsonpath='{.status.allocatable.cpu}' | millicores)"
    used="$(kubectl get pods -A --field-selector "spec.nodeName=$node,status.phase!=Succeeded,status.phase!=Failed" \
      -o jsonpath='{range .items[*].spec.containers[*]}{.resources.requests.cpu}{"\n"}{end}' | millicores)"
    free=$(( (alloc - used) / 1000 ))
    if [ -z "$min" ] || [ "$free" -lt "$min" ]; then min=$free; fi
  done
  if [ "${min:-0}" -lt 1 ]; then min=1; fi
  echo "$min"
}

# Render a manifest whose cpu request is ${GANG_CPU} with the free cores on a
# worker. Pipe it to kubectl apply or delete.
render_gang_manifest() {
  local cpu line
  cpu="$(free_cores_per_worker)"
  while IFS= read -r line || [ -n "$line" ]; do
    printf '%s\n' "${line//\$\{GANG_CPU\}/$cpu}"
  done < "$1"
}
