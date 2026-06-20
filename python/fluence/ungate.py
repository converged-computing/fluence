"""
fluence.ungate — generic worker ungating (Kubernetes side).

Once the sidecar determines the quantum task is ready, it ungates the gated
classical worker pods: stamp the vendor-neutral job-id annotation, set the
high-priority class, and remove the scheduling gate atomically. This is pure
Kubernetes plumbing — no vendor specifics.
"""

from __future__ import annotations

import json
import os
import subprocess

from fluence.providers.base import log

JOB_ID_ANNOTATION = "fluence.flux-framework.org/quantum-job-id"
QUANTUM_GATE_NAME = "quantum.braket/ready"
PRIORITY_CLASS = "fluence-quantum-classical"


def kubectl(args):
    result = subprocess.run(["kubectl"] + args, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"kubectl {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def ungate_pods(gated_pods, job_id, namespace):
    """
    For each gated worker pod:
      1. Stamp the vendor-neutral job-id annotation so the worker can locate
         the quantum result.
      2. Set the high-priority class and remove the scheduling gate atomically
         (priority is set here, not in the webhook, to avoid the admission
         controller conflict where priority:0 is already defaulted).
    """
    for pod_name in gated_pods:
        pod_name = pod_name.strip()
        if not pod_name:
            continue
        log(f"ungating pod: {pod_name}")

        if job_id:
            try:
                kubectl(["annotate", "pod", pod_name, "-n", namespace,
                         f"{JOB_ID_ANNOTATION}={job_id}", "--overwrite"])
                log(f"  patched job id onto {pod_name}: {job_id}")
            except RuntimeError as e:
                log(f"  WARNING: could not annotate {pod_name}: {e}")
        else:
            log(f"  WARNING: no job id to patch onto {pod_name}")

        patch = json.dumps([
            {"op": "add", "path": "/spec/priorityClassName", "value": PRIORITY_CLASS},
            {"op": "remove", "path": "/spec/schedulingGates/0"},
        ])
        try:
            kubectl(["patch", "pod", pod_name, "-n", namespace,
                     "--type=json", f"-p={patch}"])
            log(f"  set priority and removed gate from {pod_name}")
        except RuntimeError as e:
            log(f"  WARNING: could not patch {pod_name}: {e}")


def gated_pods_from_env():
    return [p.strip() for p in os.environ.get("FLUENCE_GATED_PODS", "").split(",")
            if p.strip()]


def namespace_from_env():
    return os.environ.get("FLUENCE_NAMESPACE", "default")
