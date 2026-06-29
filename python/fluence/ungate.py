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


def ungate_one(pod_name, job_id, namespace):
    """Ungate a SINGLE gated pod: stamp its (own) job-id annotation so it can
    locate its quantum result, then remove the scheduling gate. Returns True on
    a successful gate removal. See the priorityClassName note below.

    priorityClassName is NOT set here -- it is immutable after pod creation, so
    the webhook must set it at admission. Setting it in this patch made the whole
    patch fail atomically, leaving the pod gated.
    """
    pod_name = (pod_name or "").strip()
    if not pod_name:
        return False
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

    patch = json.dumps([{"op": "remove", "path": "/spec/schedulingGates/0"}])
    try:
        kubectl(["patch", "pod", pod_name, "-n", namespace, "--type=json", f"-p={patch}"])
        log(f"  removed gate from {pod_name}")
        return True
    except RuntimeError as e:
        log(f"  WARNING: could not patch {pod_name}: {e}")
        return False


def ungate_pods(gated_pods, job_id, namespace):
    """SHARED mode: one quantum result is shared by the whole gang, so broadcast
    the same job_id to every gated pod and remove all gates together."""
    return sum(1 for p in gated_pods if ungate_one(p, job_id, namespace))


def ungate_per_result(provider, tasks, gated_pods, namespace, poll_interval=10,
                      timeout=3600, _sleep=time.sleep):
    """BATCH mode: the producer submitted N tasks; release each gated worker the
    moment ANY task completes -- assigning that task's result to whichever worker
    is still free.

      tasks       : list of provider Tasks (the producer's N submissions)
      gated_pods  : list of gated worker pod names (the gang minus the producer)

    Workers come from one template and are interchangeable, so there is no slot
    or completion-index mapping -- we never assume the gang is an indexed Job.
    A worker is ungated with the job_id of whatever task finished, as it finishes.
    Out-of-order completion is fine, and so is heterogeneous task duration: the
    first result out wakes the first worker, and so on. We assign at most one task
    per worker; any surplus tasks (e.g. the producer's own share) are left for the
    producer. Returns the count of workers released.

    This is the only K8s-side difference from shared mode: the same gate
    primitive, fired per-result and matched to a free worker, instead of one
    shared id broadcast to all.
    """
    pending = list(tasks)
    free = [p for p in (gated_pods or []) if p]
    ok = 0
    deadline = time.time() + timeout
    while pending and free and time.time() < deadline:
        progressed = False
        for task in list(pending):
            if not free:
                break
            try:
                if provider.is_ready_to_ungate(task):
                    jid = provider.job_id(task)
                    pod = free.pop(0)
                    if ungate_one(pod, jid, namespace):
                        ok += 1
                    log(f"task ready (job_id={jid}) -> released worker {pod}")
                    pending.remove(task)
                    progressed = True
            except Exception as e:  # noqa: BLE001 - keep polling the other tasks
                log(f"poll error (will retry): {e}")
        if free and pending and not progressed:
            _sleep(poll_interval)
    if free:
        log(f"WARNING: {len(free)} worker(s) never received a result before timeout: {free}")
    elif pending:
        log(f"{len(pending)} task(s) not assigned to a worker (left for the producer)")
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