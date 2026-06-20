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


JOB_ID_ANNOTATION = "fluence.flux-framework.org/quantum-job-id"


def ungate_pods(gated_pods, job_id, namespace):
    """
    For each gated pod:
      1. Patch the fluence.flux-framework.org/quantum-job-id annotation with the
         (vendor-neutral) job id so the worker can locate the quantum result.
      2. Set the high-priority class and remove the scheduling gate atomically.

    gated_pods: list of pod names
    job_id:     the vendor-neutral job identifier (may be empty if unknown)
    namespace:  Kubernetes namespace
    """
    for pod_name in gated_pods:
        pod_name = pod_name.strip()
        if not pod_name:
            continue

        log(f"Ungating pod: {pod_name}")

        # 1. Patch job-id annotation
        if job_id:
            try:
                kubectl([
                    "annotate", "pod", pod_name,
                    "-n", namespace,
                    f"{JOB_ID_ANNOTATION}={job_id}",
                    "--overwrite",
                ])
                log(f"  Patched job id onto {pod_name}: {job_id}")
            except RuntimeError as e:
                log(f"  WARNING: could not patch annotation on {pod_name}: {e}")
        else:
            log(f"  WARNING: no job id available to patch onto {pod_name}")

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
