#!/usr/bin/env bash
# Quantum suite setup (run by the e2e-suite workflow before the NN-*.sh tests).
#
# Installs the qpu add-on so nodes advertise fluxion.flux-framework.org/qpu —
# without it every quantum pod stays Pending (fluence matches in its own graph,
# but the default NodeResourcesFit plugin rejects each node because the extended
# resource is not in allocatable, so the match is rolled back). The base deploy
# (deploy/fluence-test.yaml) does NOT include this; it is quantum-only.
#
# Also points the webhook-injected sidecar/stage image at the CI-loaded image:
# the default sidecar image (ghcr.io/.../fluence-sidecar:latest) is not loaded in
# kind, so the submitter's containers could not pull. The fluence-stage init is
# fail-soft (no python in this image -> it logs and exits 0), which is fine for
# the structural assertions; the submitter still schedules and runs.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; . "${HERE%/test/e2e/*}/test/e2e/lib.sh"
IMAGE="${IMAGE:-vanessa/fluence:test}"

log "quantum setup: installing the qpu add-on (resources ConfigMap + device plugin)"
kubectl apply -f deploy/fluence-resources-test.yaml

# Run the device plugin from the CI-loaded image (its manifest ships a registry
# image that kind has not pulled). Container name is 'deviceplugin'.
kubectl -n kube-system set image daemonset/fluence-deviceplugin deviceplugin="$IMAGE"
kubectl -n kube-system patch daemonset/fluence-deviceplugin --type=json \
  -p '[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"}]' \
  2>/dev/null || true

# Injected sidecar + stage init must use a present image too (see header).
kubectl -n kube-system set env deployment/fluence-webhook FLUENCE_SIDECAR_IMAGE="$IMAGE"
kubectl -n kube-system rollout status deployment/fluence-webhook --timeout=180s

# Scheduler re-reads the resources config now that the ConfigMap exists.
kubectl -n kube-system rollout restart deployment/fluence
kubectl -n kube-system rollout status  deployment/fluence --timeout=180s

log "waiting for the device plugin DaemonSet to be Ready"
kubectl -n kube-system rollout status daemonset/fluence-deviceplugin --timeout=180s

# Block until at least one node advertises the qpu extended resource, so the
# tests do not race the kubelet's device registration.
log "waiting for nodes to advertise fluxion.flux-framework.org/qpu"
ok=0
for i in $(seq 1 60); do
  if kubectl get nodes -o jsonpath='{.items[*].status.allocatable}' 2>/dev/null \
       | grep -q 'fluxion.flux-framework.org/qpu'; then
    ok=1; break
  fi
  sleep 3
done
[ "$ok" = 1 ] || fail "no node advertised fluxion.flux-framework.org/qpu after the add-on (device plugin not registering)"
log "qpu advertised on at least one node"

log "quantum setup complete: qpu add-on installed, scheduler restarted, sidecar image=$IMAGE"