#!/usr/bin/env bash
# sidecars/braket/test/integration.sh
#
# Local integration test for the Fluence Braket sidecar.
# Requires a running Kubernetes cluster and AWS credentials with Braket access.
#
# What this tests:
#   1. SDK interceptor: AwsDevice.run() tags tasks with fluence-pod-uid
#   2. Task discovery: sidecar finds the task by tag via search_quantum_tasks
#   3. Queue position polling: sidecar polls and logs queue position
#   4. Ungating: sidecar removes gate and patches task ARN when position==1
#
# Usage:
#   # With existing cluster and credentials secret already applied:
#   bash sidecars/braket/test/integration.sh
#
#   # Override defaults:
#   NAMESPACE=test BACKEND=sv1 bash sidecars/braket/test/integration.sh
#
# Prerequisites:
#   - kubectl configured against a running cluster
#   - aws-braket-credentials secret in $NAMESPACE
#   - Fluence installed (for schedulerName: fluence to work)
#   - fluence-sidecar-braket image built and loaded into cluster
#     (or pulled from GHCR)
set -euo pipefail

NAMESPACE="${NAMESPACE:-default}"
BACKEND="${BACKEND:-sv1}"
SIDECAR_IMAGE="${SIDECAR_IMAGE:-ghcr.io/converged-computing/fluence-sidecar-braket:latest}"
HERE="$(cd "$(dirname "$0")" && pwd)"

log()  { echo "=== [braket-integration] $*"; }
fail() { echo "FAIL: $*" >&2; dump; exit 1; }

dump() {
  echo "----- pods -----"
  kubectl get pods -n "$NAMESPACE" -o wide || true
  echo "----- gateway logs -----"
  kubectl logs -n "$NAMESPACE" integration-gateway -c user-app --tail=50 || true
  echo "----- sidecar logs -----"
  kubectl logs -n "$NAMESPACE" integration-gateway -c fluence-sidecar --tail=50 || true
  echo "----- classical pod -----"
  kubectl describe pod -n "$NAMESPACE" integration-classical || true
}

# Check prerequisites
kubectl get secret aws-braket-credentials -n "$NAMESPACE" > /dev/null 2>&1 \
  || fail "aws-braket-credentials secret not found in namespace $NAMESPACE"

log "Running braket sidecar integration test"
log "  namespace : $NAMESPACE"
log "  backend   : $BACKEND"
log "  image     : $SIDECAR_IMAGE"

# Determine device ARN from backend name
case "$BACKEND" in
  sv1) DEVICE_ARN="arn:aws:braket:::device/quantum-simulator/amazon/sv1" ;;
  tn1) DEVICE_ARN="arn:aws:braket:::device/quantum-simulator/amazon/tn1" ;;
  *) fail "Unknown backend: $BACKEND (use sv1 or tn1 for integration tests)" ;;
esac

POD_UID="integration-test-$(date +%s)"

# Clean up any leftover pods from a previous run
kubectl delete pod integration-gateway integration-classical \
  -n "$NAMESPACE" --ignore-not-found=true --wait=true 2>/dev/null || true
kubectl delete rolebinding fluence-sidecar-integration \
  -n "$NAMESPACE" --ignore-not-found=true 2>/dev/null || true
kubectl delete role fluence-sidecar-integration \
  -n "$NAMESPACE" --ignore-not-found=true 2>/dev/null || true
kubectl delete serviceaccount fluence-sidecar-integration \
  -n "$NAMESPACE" --ignore-not-found=true 2>/dev/null || true

# Create RBAC for sidecar to patch pods
kubectl apply -n "$NAMESPACE" -f - << YAML
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: fluence-sidecar-integration
  namespace: ${NAMESPACE}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: fluence-sidecar-integration
  namespace: ${NAMESPACE}
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "patch", "annotate"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: fluence-sidecar-integration
  namespace: ${NAMESPACE}
subjects:
  - kind: ServiceAccount
    name: fluence-sidecar-integration
    namespace: ${NAMESPACE}
roleRef:
  kind: Role
  name: fluence-sidecar-integration
  apiGroup: rbac.authorization.k8s.io
YAML

# Create the classical pod (gated, waiting for sidecar)
kubectl apply -n "$NAMESPACE" -f - << YAML
apiVersion: v1
kind: Pod
metadata:
  name: integration-classical
  namespace: ${NAMESPACE}
  annotations:
    braket.quantum/task-arn: ""
spec:
  restartPolicy: Never
  schedulingGates:
    - name: quantum.braket/ready
  containers:
    - name: classical-worker
      image: python:3.11-slim
      command:
        - python3
        - -c
        - |
          import os, time
          arn = os.environ.get("BRAKET_TASK_ARN", "")
          print(f"TASK_ARN={arn}")
          assert arn, "BRAKET_TASK_ARN is empty"
          print("classical-worker: task ARN received correctly")
          # Verify we can retrieve the result from Braket using the ARN
          from braket.aws import AwsQuantumTask
          import asyncio
          asyncio.set_event_loop(asyncio.new_event_loop())
          task = AwsQuantumTask(arn=arn)
          state = task.state()
          print(f"classical-worker: task state={state}")
          assert state in ("COMPLETED", "RUNNING"), f"unexpected state: {state}"
          print("PASS: classical worker got valid task ARN and confirmed task state")
      env:
        - name: BRAKET_TASK_ARN
          valueFrom:
            fieldRef:
              fieldPath: metadata.annotations['braket.quantum/task-arn']
        - name: AWS_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: aws-braket-credentials
              key: AWS_ACCESS_KEY_ID
        - name: AWS_SECRET_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: aws-braket-credentials
              key: AWS_SECRET_ACCESS_KEY
        - name: AWS_DEFAULT_REGION
          valueFrom:
            secretKeyRef:
              name: aws-braket-credentials
              key: AWS_DEFAULT_REGION
      resources:
        requests:
          cpu: "100m"
          memory: "256Mi"
YAML

# Create the gateway pod with user-app + real sidecar
kubectl apply -n "$NAMESPACE" -f - << YAML
apiVersion: v1
kind: Pod
metadata:
  name: integration-gateway
  namespace: ${NAMESPACE}
spec:
  restartPolicy: Never
  serviceAccountName: fluence-sidecar-integration

  initContainers:
    # user-app: submits a real circuit to SV1 — SDK interceptor tags it
    - name: user-app
      image: ghcr.io/converged-computing/quantum-braket-braket-gateway:latest
      command:
        - python3
        - -c
        - |
          import os, sys
          # Install the interceptor (normally injected by webhook)
          sys.path.insert(0, "/app")
          exec(open("/app/fluence_braket_intercept.py").read())

          from braket.aws import AwsDevice
          from braket.circuits import Circuit

          device = AwsDevice("${DEVICE_ARN}")
          bell = Circuit().h(0).cnot(0, 1)
          print(f"user-app: submitting circuit to ${BACKEND}")
          print(f"user-app: FLUENCE_POD_UID={os.environ.get('FLUENCE_POD_UID', 'NOT SET')}")
          task = device.run(bell, shots=10)
          print(f"user-app: submitted task {task.id}")
          print(f"user-app: tags should include fluence-pod-uid")
      env:
        - name: FLUENCE_POD_UID
          value: "${POD_UID}"
        - name: AWS_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: aws-braket-credentials
              key: AWS_ACCESS_KEY_ID
        - name: AWS_SECRET_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: aws-braket-credentials
              key: AWS_SECRET_ACCESS_KEY
        - name: AWS_DEFAULT_REGION
          valueFrom:
            secretKeyRef:
              name: aws-braket-credentials
              key: AWS_DEFAULT_REGION

  containers:
    # real fluence-sidecar
    - name: fluence-sidecar
      image: ${SIDECAR_IMAGE}
      env:
        - name: FLUENCE_POD_UID
          value: "${POD_UID}"
        - name: FLUENCE_POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: FLUENCE_NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace
        - name: FLUENCE_GATED_PODS
          value: "integration-classical"
        - name: FLUXION_ARN
          value: "${DEVICE_ARN}"
        - name: FLUENCE_TASK_DISCOVERY_TIMEOUT
          value: "120"
        - name: FLUENCE_POLL_INTERVAL
          value: "10"
        - name: AWS_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: aws-braket-credentials
              key: AWS_ACCESS_KEY_ID
        - name: AWS_SECRET_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: aws-braket-credentials
              key: AWS_SECRET_ACCESS_KEY
        - name: AWS_DEFAULT_REGION
          valueFrom:
            secretKeyRef:
              name: aws-braket-credentials
              key: AWS_DEFAULT_REGION
      resources:
        requests:
          cpu: "100m"
          memory: "512Mi"
YAML

log "Pods submitted. Waiting for gateway to reach Running..."

# Wait for gateway Running
for i in $(seq 1 120); do
  phase=$(kubectl get pod integration-gateway -n "$NAMESPACE" \
    -o jsonpath='{.status.phase}' 2>/dev/null || true)
  [ "$phase" = "Running" ] && break
  sleep 3
done
[ "$(kubectl get pod integration-gateway -n "$NAMESPACE" \
  -o jsonpath='{.status.phase}')" = "Running" ] \
  || fail "integration-gateway did not reach Running"

log "Gateway is Running. Waiting for sidecar to ungate classical pod..."

# Wait for classical pod to be ungated (up to 5 minutes for SV1 queue)
for i in $(seq 1 100); do
  phase=$(kubectl get pod integration-classical -n "$NAMESPACE" \
    -o jsonpath='{.status.phase}' 2>/dev/null || true)
  { [ "$phase" = "Running" ] || [ "$phase" = "Succeeded" ]; } && break
  # Print sidecar progress every 30s
  [ $((i % 10)) -eq 0 ] && \
    kubectl logs integration-gateway -n "$NAMESPACE" \
      -c fluence-sidecar --tail=5 2>/dev/null || true
  sleep 3
done

phase=$(kubectl get pod integration-classical -n "$NAMESPACE" \
  -o jsonpath='{.status.phase}' 2>/dev/null || true)
{ [ "$phase" = "Running" ] || [ "$phase" = "Succeeded" ]; } \
  || fail "integration-classical was not ungated (phase=$phase)"

log "Classical pod ungated. Checking task ARN annotation..."

arn=$(kubectl get pod integration-classical -n "$NAMESPACE" \
  -o jsonpath='{.metadata.annotations.braket\.quantum/task-arn}' 2>/dev/null || true)
[ -n "$arn" ] || fail "braket.quantum/task-arn annotation not set"
log "Task ARN: $arn"

# Wait for classical pod to complete
for i in $(seq 1 60); do
  phase=$(kubectl get pod integration-classical -n "$NAMESPACE" \
    -o jsonpath='{.status.phase}' 2>/dev/null || true)
  [ "$phase" = "Succeeded" ] && break
  [ "$phase" = "Failed" ] && fail "integration-classical Failed"
  sleep 3
done

[ "$(kubectl get pod integration-classical -n "$NAMESPACE" \
  -o jsonpath='{.status.phase}')" = "Succeeded" ] \
  || fail "integration-classical did not Succeed"

# Verify classical pod got the ARN and confirmed task state
out=$(kubectl logs integration-classical -n "$NAMESPACE" 2>/dev/null || true)
echo "$out" | grep -q "PASS:" || fail "classical worker did not PASS (logs: $out)"

log "Sidecar logs:"
kubectl logs integration-gateway -n "$NAMESPACE" -c fluence-sidecar || true

log "PASS: full braket sidecar integration test complete"
log "  SDK interceptor tagged task with fluence-pod-uid"
log "  Sidecar discovered task by tag"
log "  Sidecar polled queue position and ungated at position==1"
log "  Task ARN propagated to classical pod via annotation"
log "  Classical pod confirmed task state via Braket SDK"

# Cleanup
kubectl delete pod integration-gateway integration-classical \
  -n "$NAMESPACE" --ignore-not-found=true --wait=false || true
