#!/usr/bin/env bash
# Two-group quantum split: a quantum gang of size N is split into a LEADER
# PodGroup <group> (minCount 1) and a WORKER PodGroup <group>-workers
# (minCount N-1). Workers are relinked into the worker group and gated. This is
# the structural guarantee that, combined with the sidecar ungating the worker
# group, makes quantum gangs work. (The runtime ungate is covered by 04; here we
# prove the group SPLIT the ungate path depends on.)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; . "${HERE%/test/e2e/*}/test/e2e/lib.sh"

log "TEST 7: quantum two-group split (leader=1, workers=N-1)"
kubectl apply -f examples/test/e2e/quantum/quantum-split-pods.yaml

# leader PodGroup <group> must exist with minCount 1
log "checking leader group 'qsplit' minCount == 1"
for i in $(seq 1 30); do
  lc="$(kubectl get podgroup qsplit -o jsonpath='{.spec.schedulingPolicy.gang.minCount}' 2>/dev/null || true)"
  [ -n "$lc" ] && break; sleep 2
done
[ "$lc" = "1" ] || fail "leader group qsplit minCount=$lc, want 1"

# worker PodGroup <group>-workers must exist with minCount N-1 = 2
log "checking worker group 'qsplit-workers' minCount == 2 (N-1)"
for i in $(seq 1 30); do
  wc="$(kubectl get podgroup qsplit-workers -o jsonpath='{.spec.schedulingPolicy.gang.minCount}' 2>/dev/null || true)"
  [ -n "$wc" ] && break; sleep 2
done
[ "$wc" = "2" ] || fail "worker group qsplit-workers minCount=$wc, want 2 (N-1); the split did not happen"

# workers must be RELINKED into the worker group (label rewritten by webhook)
log "checking workers were relinked into qsplit-workers"
for w in qsplit-worker-0 qsplit-worker-1; do
  g="$(kubectl get pod "$w" -o jsonpath='{.metadata.labels.fluence\.flux-framework\.org/group}' 2>/dev/null || true)"
  [ "$g" = "qsplit-workers" ] || fail "$w group label=$g, want qsplit-workers (relink failed)"
done

# workers must be GATED (scheduling gate held until leader's task is ready)
log "checking workers carry the quantum scheduling gate"
for w in qsplit-worker-0 qsplit-worker-1; do
  gate="$(kubectl get pod "$w" -o jsonpath='{.spec.schedulingGates[0].name}' 2>/dev/null || true)"
  [ "$gate" = "quantum.braket/ready" ] || fail "$w not gated (gate=$gate)"
done

# leader's sidecar must know where to find workers: FLUENCE_WORKER_GROUP_BASE set
log "checking leader sidecar has the worker-group env"
base="$(kubectl get pod qsplit-leader -o jsonpath='{range .spec.containers[*]}{range .env[*]}{.name}={.value}{"\n"}{end}{end}' 2>/dev/null | grep FLUENCE_WORKER_GROUP_BASE || true)"
[ -n "$base" ] || fail "leader sidecar missing FLUENCE_WORKER_GROUP_BASE (sidecar would look in the wrong group and never ungate)"

log "PASS 7: quantum gang split into leader(1) + workers(N-1), relinked + gated"
kubectl delete -f examples/test/e2e/quantum/quantum-split-pods.yaml --wait=false || true
for g in qsplit qsplit-workers; do
  kubectl patch podgroup $g --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
done
kubectl wait --for=delete pod -l app=qsplit --timeout=60s 2>/dev/null || true
