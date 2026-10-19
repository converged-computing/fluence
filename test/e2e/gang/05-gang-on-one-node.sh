#!/usr/bin/env bash
# The tutorial's gang, two pods with cpu and memory, on one node. The other
# gang tests let each slot take its own worker, so they never hit this.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; . "${HERE%/test/e2e/*}/test/e2e/lib.sh"

NAME=onenode
FIXTURE=examples/test/e2e/gang/gang-one-node.yaml

log "TEST 10: a two pod gang with memory places on one node"

workers=( $(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' -o jsonpath='{.items[*].metadata.name}') )
[ "${#workers[@]}" -ge 1 ] || fail "no worker node found"
keep="${workers[0]}"
others=( "${workers[@]:1}" )

cleanup() {
  kubectl delete -f "$FIXTURE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl patch podgroup "$NAME" --type=merge -p '{"metadata":{"finalizers":null}}' >/dev/null 2>&1 || true
  kubectl wait --for=delete pod -l "app=$NAME" --timeout=60s >/dev/null 2>&1 || true
  for n in "${others[@]}"; do kubectl uncordon "$n" >/dev/null 2>&1 || true; done
  # give the graph its nodes back
  kubectl rollout restart -n "${NS_KUBE}" deployment/fluence >/dev/null 2>&1 || true
  kubectl rollout status -n "${NS_KUBE}" deployment/fluence --timeout="${TIMEOUT}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# cordon the rest and restart fluence so its graph is that one node
for n in "${others[@]}"; do
  log "cordoning $n"
  kubectl cordon "$n"
done
kubectl rollout restart -n "${NS_KUBE}" deployment/fluence
wait_fluence_ready

kubectl apply -f "$FIXTURE"
log "waiting for both pods on $keep"
wait_pods_ready "app=$NAME" 2 180 \
  || fail "the gang did not place on one node"

placed="$(kubectl get pods -l "app=$NAME" -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort -u | tr '\n' ' ')"
[ "$placed" = "$keep " ] || fail "pods landed on '$placed', want only $keep"
log "PASS 10: both pods of the gang run on $keep"
