"""
sidecars/lib/ungate.py — shared ungating logic for all Fluence sidecars.

Every vendor sidecar calls ungate_pods() once the quantum task is ready.
This module handles the Kubernetes side: patching the task ARN annotation
and removing the scheduling gate from each classical pod.
"""

import json
import os
import subprocess
from datetime import datetime, timezone


def log(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[fluence-sidecar] {ts} {msg}", flush=True)


def kubectl(args):
    result = subprocess.run(
        ["kubectl"] + args,
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"kubectl {' '.join(args)} failed: {result.stderr.strip()}"
        )
    return result.stdout.strip()


def ungate_pods(gated_pods, task_arn, namespace):
    """
    For each gated pod:
      1. Patch braket.quantum/task-arn annotation with the task ARN
      2. Remove the quantum.braket/ready scheduling gate

    gated_pods: list of pod names
    task_arn:   the vendor task ARN to propagate (may be empty string if unknown)
    namespace:  Kubernetes namespace
    """
    for pod_name in gated_pods:
        pod_name = pod_name.strip()
        if not pod_name:
            continue

        log(f"Ungating pod: {pod_name}")

        # 1. Patch task ARN annotation
        if task_arn:
            try:
                kubectl([
                    "annotate", "pod", pod_name,
                    "-n", namespace,
                    f"braket.quantum/task-arn={task_arn}",
                    "--overwrite",
                ])
                log(f"  Patched task ARN onto {pod_name}: {task_arn}")
            except RuntimeError as e:
                log(f"  WARNING: could not patch annotation on {pod_name}: {e}")
        else:
            log(f"  WARNING: no task ARN available to patch onto {pod_name}")

        # 2. Set high priority class and remove scheduling gate atomically
        # Priority is set here (not in webhook) to avoid admission controller
        # conflict where priority:0 is already defaulted before our patch.
        patch = json.dumps([
            {
                "op": "add",
                "path": "/spec/priorityClassName",
                "value": "fluence-quantum-classical"
            },
            {
                "op": "remove",
                "path": "/spec/schedulingGates/0"
            }
        ])
        try:
            kubectl([
                "patch", "pod", pod_name,
                "-n", namespace,
                "--type=json",
                f"-p={patch}",
            ])
            log(f"  Set priority and removed scheduling gate from {pod_name}")
        except RuntimeError as e:
            log(f"  WARNING: could not patch {pod_name}: {e}")


def gated_pods_from_env():
    """Read FLUENCE_GATED_PODS env var and return a list of pod names."""
    return [
        p.strip()
        for p in os.environ.get("FLUENCE_GATED_PODS", "").split(",")
        if p.strip()
    ]


def namespace_from_env():
    return os.environ.get("FLUENCE_NAMESPACE", "default")
