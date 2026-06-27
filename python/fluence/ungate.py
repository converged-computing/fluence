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
import time

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
      2. Remove the scheduling gate so the pod can be scheduled.

    NOTE: priorityClassName is NOT set here — it is immutable after pod creation
    (the API server forbids changing any spec field but image/tolerations/
    activeDeadlineSeconds/terminationGracePeriodSeconds on an existing pod). If a
    priority class is wanted on the classical gang, the webhook must set it at
    admission, when the pod is created. Setting it in the ungate patch made the
    whole patch fail atomically, so the gate was never removed and workers stayed
    gated.
    """
    ok = 0
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
            {"op": "remove", "path": "/spec/schedulingGates/0"},
        ])
        try:
            kubectl(["patch", "pod", pod_name, "-n", namespace,
                     "--type=json", f"-p={patch}"])
            log(f"  removed gate from {pod_name}")
            ok += 1
        except RuntimeError as e:
            log(f"  WARNING: could not patch {pod_name}: {e}")
    return ok


def gated_pods_from_env():
    return [p.strip() for p in os.environ.get("FLUENCE_GATED_PODS", "").split(",")
            if p.strip()]


GROUP_LABEL = "fluence.flux-framework.org/group"


def discover_gated_pods(namespace, group, exclude=""):
    """
    Find the names of pods in the same group that still carry the quantum
    scheduling gate (i.e. the gang pods this submitter must ungate).

    The submitter is created alongside the gang, so the gated set is discovered
    at runtime rather than known at admission. We
    list pods by the group label and keep those with the QUANTUM_GATE_NAME gate
    still present, excluding the leader pod itself.
    """
    if not group:
        return []
    try:
        out = kubectl([
            "get", "pods", "-n", namespace,
            "-l", f"{GROUP_LABEL}={group}",
            "-o", "json",
        ])
    except RuntimeError as e:
        log(f"could not list group pods: {e}")
        return []
    import json as _json
    names = []
    for item in _json.loads(out).get("items", []):
        name = item["metadata"]["name"]
        if name == exclude:
            continue
        gates = item.get("spec", {}).get("schedulingGates", []) or []
        if any(g.get("name") == QUANTUM_GATE_NAME for g in gates):
            names.append(name)
    return names


def wait_for_gated_pods(namespace, group, exclude="", timeout=120, interval=3):
    """
    Wait until at least one gated gang pod is discovered in the group (the gang
    is created up front, so its pods appear quickly), then return all currently
    gated pods. The timeout is a backstop so the submitter never hangs if the
    gang never appears. Returns the discovered list (possibly empty on timeout).
    """
    deadline = time.time() + timeout
    found = []
    while time.time() < deadline:
        found = discover_gated_pods(namespace, group, exclude=exclude)
        if found:
            return found
        log("waiting for gated gang pods to appear")
        time.sleep(interval)
    log("WARNING: timed out waiting for gated gang pods; none found")
    return found


def namespace_from_env():
    return os.environ.get("FLUENCE_NAMESPACE", "default")