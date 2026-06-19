#!/usr/bin/env python3
"""
fluence-sidecar: Quantum-classical scheduling coordination for Fluence.

Injected automatically by the Fluence mutating webhook into any pod
requesting a QPU resource (fluxion.flux-framework.org/qpu).

Responsibilities:
  1. Find the quantum task submitted by the sibling user application
     container, by searching for tasks tagged with FLUENCE_POD_UID.
  2. Poll task.queue_position() until position==1 or RUNNING.
  3. Patch braket.quantum/task-arn onto gated sibling classical pods.
  4. Remove scheduling gates from those pods — Kubernetes preemption
     and the Fluence PostFilter handle placement from there.

Environment variables (all injected by Fluence webhook):
  FLUENCE_POD_UID              UID of this pod
  FLUENCE_POD_NAME             Name of this pod
  FLUENCE_NAMESPACE            Kubernetes namespace
  FLUENCE_GATED_PODS           Comma-separated names of gated sibling pods
  FLUXION_ARN                  Braket device ARN for this pod
  FLUENCE_TASK_DISCOVERY_TIMEOUT  Seconds to wait for task discovery (default: 300)
  FLUENCE_POLL_INTERVAL        Seconds between queue position polls (default: 30)
  AWS_ACCESS_KEY_ID            } AWS credentials — shared from pod spec
  AWS_SECRET_ACCESS_KEY        }
  AWS_DEFAULT_REGION           }
"""

import asyncio
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone


# ── helpers ────────────────────────────────────────────────────────────────────
# Shared ungating logic lives in sidecars/lib/ungate.py so all vendor sidecars
# can reuse it. Add that directory to the path when running from the repo.
import sys
_lib = os.path.join(os.path.dirname(__file__), "..", "lib")
if os.path.isdir(_lib):
    sys.path.insert(0, _lib)
from ungate import log, kubectl, ungate_pods, gated_pods_from_env, namespace_from_env


# ── task discovery ─────────────────────────────────────────────────────────────

def find_task_by_tag(client, device_arn, pod_uid, timeout):
    """
    Search for a Braket task tagged fluence-pod-uid=<pod_uid> on device_arn.
    Polls until found or timeout. Returns task ARN or None.
    """
    log(f"Searching for task with tag fluence-pod-uid={pod_uid} on {device_arn}")
    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            # Extract region from device ARN
            # arn:aws:braket:<region>::device/...
            region = device_arn.split(":")[3] or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
            response = client.search_quantum_tasks(
                filters=[
                    {
                        "name": "deviceArn",
                        "operator": "EQUAL",
                        "values": [device_arn],
                    },
                    {
                        "name": "tags:fluence-pod-uid",
                        "operator": "EQUAL",
                        "values": [pod_uid],
                    },
                ],
                maxResults=10,
            )
            tasks = response.get("quantumTasks", [])
            if tasks:
                # Most recently created task is ours
                tasks.sort(key=lambda t: t.get("createdAt", ""), reverse=True)
                arn = tasks[0]["quantumTaskArn"]
                log(f"Found task by tag: {arn}")
                return arn
        except Exception as e:
            log(f"Search error (will retry): {e}")

        time.sleep(10)

    log("Task discovery by tag timed out")
    return None


def find_task_by_time_window(client, device_arn, pod_start_ts, timeout):
    """
    Fallback: find the most recently created task on device_arn submitted
    after pod_start_ts. Used when tag-based discovery fails.
    """
    log(f"Falling back to time-window heuristic (pod_start={pod_start_ts})")
    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            response = client.search_quantum_tasks(
                filters=[
                    {
                        "name": "deviceArn",
                        "operator": "EQUAL",
                        "values": [device_arn],
                    },
                    {
                        "name": "status",
                        "operator": "EQUAL",
                        "values": ["QUEUED"],
                    },
                ],
                maxResults=50,
            )
            tasks = response.get("quantumTasks", [])
            # Filter to tasks created after pod start
            candidates = [
                t for t in tasks
                if t.get("createdAt", "") >= pod_start_ts
            ]
            if candidates:
                candidates.sort(key=lambda t: t.get("createdAt", ""), reverse=True)
                arn = candidates[0]["quantumTaskArn"]
                log(f"Found task by time window (heuristic): {arn} "
                    f"(WARNING: may not be correct if multiple tasks submitted)")
                return arn
        except Exception as e:
            log(f"Search error (will retry): {e}")

        time.sleep(10)

    log("Time-window task discovery timed out")
    return None


# ── queue position polling ─────────────────────────────────────────────────────

def wait_for_position_one(task_arn, poll_interval):
    """
    Poll task.queue_position() until position==1 or task is RUNNING.
    Returns when it's time to ungate classical pods.
    """
    asyncio.set_event_loop(asyncio.new_event_loop())

    from braket.aws import AwsQuantumTask

    log(f"Polling queue position for task {task_arn.split('/')[-1]}")
    last_position = None

    while True:
        try:
            task = AwsQuantumTask(arn=task_arn)
            state = task.state()

            if state in ("COMPLETED", "FAILED", "CANCELLED"):
                log(f"Task reached terminal state: {state} — ungating now")
                return state

            if state == "RUNNING":
                log("Task is RUNNING — ungating classical pods")
                return state

            pos_info = task.queue_position()
            position  = pos_info.queue_position

            if position != last_position:
                log(f"Queue position: {position}  (state={state})")
                last_position = position

            if position == "1":
                log("Queue position is 1 — ungating classical pods")
                return state

        except Exception as e:
            log(f"Queue position poll error (will retry): {e}")

        time.sleep(poll_interval)


# ungate_pods is imported from sidecars/lib/ungate.py


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    pod_uid    = os.environ.get("FLUENCE_POD_UID", "")
    pod_name   = os.environ.get("FLUENCE_POD_NAME", "")
    namespace  = os.environ.get("FLUENCE_NAMESPACE", "default")
    gated_str  = os.environ.get("FLUENCE_GATED_PODS", "")
    device_arn = os.environ.get("FLUXION_ARN", "")
    discovery_timeout = int(os.environ.get("FLUENCE_TASK_DISCOVERY_TIMEOUT", 300))
    poll_interval     = int(os.environ.get("FLUENCE_POLL_INTERVAL", 30))

    gated_pods = [p.strip() for p in gated_str.split(",") if p.strip()]

    log(f"Starting fluence-sidecar")
    log(f"  pod_uid    : {pod_uid}")
    log(f"  pod_name   : {pod_name}")
    log(f"  namespace  : {namespace}")
    log(f"  device_arn : {device_arn}")
    log(f"  gated_pods : {gated_pods}")

    if not device_arn:
        log("ERROR: FLUXION_ARN not set — cannot discover task")
        sys.exit(1)

    if not gated_pods:
        log("No gated pods to ungate — exiting")
        sys.exit(0)

    # Get region from ARN or env
    region = device_arn.split(":")[3] or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    if not region:
        region = "us-east-1"

    import boto3
    asyncio.set_event_loop(asyncio.new_event_loop())
    client = boto3.client("braket", region_name=region)

    # Pod start time for fallback heuristic
    pod_start_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 1. Discover task ARN
    task_arn = find_task_by_tag(client, device_arn, pod_uid, discovery_timeout)

    if not task_arn:
        log("Tag-based discovery failed — trying time-window heuristic")
        task_arn = find_task_by_time_window(
            client, device_arn, pod_start_ts, discovery_timeout
        )

    if not task_arn:
        log("ERROR: could not find quantum task — ungating anyway to avoid deadlock")
        ungate_pods(gated_pods, "", namespace)
        sys.exit(1)

    # 2. Wait for position==1 or RUNNING
    wait_for_position_one(task_arn, poll_interval)

    # 3. Ungate classical pods with task ARN
    ungate_pods(gated_pods, task_arn, namespace)

    log("Done — classical pods ungated")


if __name__ == "__main__":
    main()
