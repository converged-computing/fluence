"""
fluence.sidecar — provider-agnostic quantum coordination sidecar main loop.

Injected by the Fluence webhook into the one-off SUBMITTER pod (gang + submitter
model — there is no leader/worker split). Resolves its vendor at runtime from the
backend annotation, discovers the task the user application submitted (tagged by
the interceptor), polls readiness, and either ungates the gated GANG group (gang
mode) or just logs the queue-position series (observe-only mode).

Entry point: `fluence-sidecar` console script (see pyproject.toml) -> main().

Environment (injected by the Fluence webhook):
  FLUENCE_POD_UID                 UID of this pod (matches interceptor tag)
  FLUENCE_NAMESPACE               Kubernetes namespace
  FLUENCE_GANG_GROUP              group label of the gated gang to ungate
  FLUENCE_GATED_PODS              optional explicit comma-separated gang pod names
  FLUENCE_OBSERVE                 "true" for observe-only telemetry mode
  FLUXION_BACKEND / FLUXION_VENDOR  scheduler-chosen backend / vendor
  FLUENCE_TASK_DISCOVERY_TIMEOUT  seconds to wait for discovery (default 300)
  FLUENCE_POLL_INTERVAL           seconds between polls (default 30)
  FLUENCE_UNGATE_POSITION         ungate at this queue position or closer
                                  (default 1, next in line)
"""

from __future__ import annotations

import os
import sys
import time

from fluence.providers import resolve_from_env
from fluence.providers.base import log, ungate_position
from fluence.ungate import ungate_pods, gated_pods_from_env, namespace_from_env, wait_for_gated_pods



def _poll(provider, task, poll_interval, ungate, position=1):
    """Poll until the task is ready or has failed. True when ready."""
    mode = "gang" if ungate else "observe-only"
    log(f"{mode} mode: polling queue position, ungating at {position} or closer")
    last = object()
    while True:
        try:
            if provider.task_failed(task):
                log("ERROR: task reached a terminal state with no result")
                return False
            if provider.is_ready_to_ungate(task, position):
                log(f"task ready (position={provider.queue_position(task)})")
                return True
            pos = provider.queue_position(task)
            if pos != last:
                log(f"queue position: {pos}")
                last = pos
        except Exception as e:
            log(f"poll error (will retry): {e}")
        time.sleep(poll_interval)


def main():
    pod_uid = os.environ.get("FLUENCE_POD_UID", "")
    pod_name = os.environ.get("FLUENCE_POD_NAME", "")
    group = os.environ.get("FLUENCE_GROUP", "")
    # Gang + submitter model: this sidecar runs in the one-off SUBMITTER pod
    # (its own group-of-one, <gang>-submitter). The gated workload it must ungate
    # is the GANG group, named by FLUENCE_GANG_GROUP (set by the webhook). There
    # is no leader/worker split and no -workers subgroup.
    gang_group = os.environ.get("FLUENCE_GANG_GROUP", "")
    backend = os.environ.get("FLUXION_BACKEND", "")
    observe = os.environ.get("FLUENCE_OBSERVE", "").lower() == "true"
    discovery_timeout = int(os.environ.get("FLUENCE_TASK_DISCOVERY_TIMEOUT", 300))
    poll_interval = int(os.environ.get("FLUENCE_POLL_INTERVAL", 30))
    ungate_timeout = int(os.environ.get("FLUENCE_UNGATE_TIMEOUT", 120))
    ungate_at = ungate_position()

    namespace = namespace_from_env()

    log("starting fluence quantum submitter sidecar")
    log(f"  pod_uid={pod_uid} namespace={namespace} group={group} "
        f"gang_group={gang_group} backend={backend} observe={observe}")

    provider = resolve_from_env()
    if provider is None:
        log("ERROR: could not resolve a quantum provider from the backend")
        sys.exit(1)
    log(f"resolved provider: {provider.name}")

    task = provider.find_my_task(pod_uid, backend, discovery_timeout)
    if task is None:
        log("ERROR: could not discover quantum task")
        if not observe:
            # Fail open: ungate the gang so it is not stranded forever.
            ungate_pods(wait_for_gated_pods(namespace, gang_group, exclude=pod_name,
                                            timeout=ungate_timeout),
                        "", namespace)
        sys.exit(1)

    job_id = provider.job_id(task)
    log(f"discovered task, job_id={job_id}")

    ready = _poll(provider, task, poll_interval, ungate=not observe,
                  position=ungate_at)

    if observe:
        log("observe-only run complete")
        sys.exit(0 if ready else 1)

    if not ready:
        # fail open like a discovery failure does, so the gang is not stranded.
        # The pods find no result and exit, and we exit non zero so it shows
        log("ERROR: ungating anyway so the gang is not stranded, but the task "
            "produced no result")
        ungate_pods(gated_pods_from_env() or wait_for_gated_pods(
            namespace, gang_group, exclude=pod_name, timeout=ungate_timeout),
            job_id, namespace)
        sys.exit(1)

    # Ungate the gang: discover the gated pods in the gang group and remove their
    # gate, stamping the job-id so each can fetch results by id. The gang pods are
    # created up front (Job/Deployment), so they are present by submit time.
    gated_pods = gated_pods_from_env() or wait_for_gated_pods(
        namespace, gang_group, exclude=pod_name, timeout=ungate_timeout)
    log(f"ungating {len(gated_pods)} gang pod(s): {gated_pods}")
    n_ok = ungate_pods(gated_pods, job_id, namespace)
    if n_ok == len(gated_pods):
        log(f"done — {n_ok} gang pod(s) ungated")
    else:
        log(f"WARNING: ungated only {n_ok}/{len(gated_pods)} gang pod(s) — see errors above")


if __name__ == "__main__":
    main()