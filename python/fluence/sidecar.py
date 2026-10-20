"""
The coordination sidecar. The webhook adds it to the scout pod of a shared
quantum gang. It resolves the vendor provider from the injected FLUXION_*
environment, finds the task the scout submitted, polls until the task is about
to run, ungates the workers with the job id and the session, then waits for
the workers to finish so the scout keeps its session until they are done. In
observe mode it only logs the queue position.

Environment, all set by the webhook
  FLUENCE_POD_UID                 uid of this pod
  FLUENCE_NAMESPACE               namespace
  FLUENCE_GANG_GROUP              group label of the workers to ungate
  FLUENCE_GATED_PODS              optional explicit list of worker pod names
  FLUENCE_OBSERVE                 true for observe mode
  FLUXION_BACKEND                 the backend the scheduler chose
  FLUENCE_TASK_DISCOVERY_TIMEOUT  seconds to wait for the task (default 300)
  FLUENCE_POLL_INTERVAL           seconds between polls (default 30)
  FLUENCE_UNGATE_POSITION         ungate at this queue position or closer (default 1)
  FLUENCE_WORKER_TIMEOUT          seconds to wait for the workers after ungating,
                                  0 waits without limit (default 0)
"""

from __future__ import annotations

import os
import sys
import time

from fluence.providers import resolve_from_env
from fluence.providers.base import log, ungate_position
from fluence import handoff
from fluence.ungate import (
    placement_from_env,
    ungate_pods,
    gated_pods_from_env,
    namespace_from_env,
    wait_for_gated_pods,
    wait_for_pods_done,
)


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


def _hold_for_workers(namespace, gang_group, pod_name):
    """Keep this pod, and with it the scout's session and its allocation,
    until the worker group has finished. Then write the marker the scout's
    release() waits for."""
    timeout = int(os.environ.get("FLUENCE_WORKER_TIMEOUT", 0))
    log("holding until the worker group finishes")
    wait_for_pods_done(namespace, gang_group, exclude=pod_name, timeout=timeout)
    if handoff.mark_workers_done():
        log("worker group finished, the scout may release its session")
    else:
        log("WARNING: could not write the workers done marker")


def main():
    pod_uid = os.environ.get("FLUENCE_POD_UID", "")
    pod_name = os.environ.get("FLUENCE_POD_NAME", "")
    group = os.environ.get("FLUENCE_GROUP", "")
    # the workers to ungate are the gang named by FLUENCE_GANG_GROUP
    gang_group = os.environ.get("FLUENCE_GANG_GROUP", "")
    backend = os.environ.get("FLUXION_BACKEND", "")
    observe = os.environ.get("FLUENCE_OBSERVE", "").lower() == "true"
    discovery_timeout = int(os.environ.get("FLUENCE_TASK_DISCOVERY_TIMEOUT", 300))
    poll_interval = int(os.environ.get("FLUENCE_POLL_INTERVAL", 30))
    ungate_timeout = int(os.environ.get("FLUENCE_UNGATE_TIMEOUT", 120))
    ungate_at = ungate_position()

    namespace = namespace_from_env()

    log("starting the fluence sidecar")
    log(
        f"  pod_uid={pod_uid} namespace={namespace} group={group} "
        f"gang_group={gang_group} backend={backend} observe={observe}"
    )

    placed_backend, placed_attrs = placement_from_env()
    provider = resolve_from_env()
    if provider is None:
        log("ERROR: could not resolve a quantum provider from the backend")
        sys.exit(1)
    log(f"resolved provider: {provider.name}")

    task = provider.find_my_task(pod_uid, backend, discovery_timeout)
    if task is None and handoff.scout_exited() and not observe:
        # a rehearsal, or an application that never submitted. Release the
        # workers with no session and hold for them as usual
        log(
            "the scout finished without submitting a task, releasing the workers with no session"
        )
        ungate_pods(
            wait_for_gated_pods(
                namespace, gang_group, exclude=pod_name, timeout=ungate_timeout
            ),
            "",
            namespace,
            backend=placed_backend,
            attributes=placed_attrs,
        )
        _hold_for_workers(namespace, gang_group, pod_name)
        sys.exit(0)
    if task is None:
        log("ERROR: could not discover quantum task")
        if not observe:
            # fail open so the workers are not stranded
            ungate_pods(
                wait_for_gated_pods(
                    namespace, gang_group, exclude=pod_name, timeout=ungate_timeout
                ),
                "",
                namespace,
                backend=placed_backend,
                attributes=placed_attrs,
            )
            _hold_for_workers(namespace, gang_group, pod_name)
        sys.exit(1)

    job_id = provider.job_id(task)
    log(f"discovered task, job_id={job_id}")

    ready = _poll(provider, task, poll_interval, ungate=not observe, position=ungate_at)

    if observe:
        log("observe-only run complete")
        sys.exit(0 if ready else 1)

    if not ready:
        # fail open so the workers are not stranded. They find no result and exit
        log(
            "ERROR: ungating anyway so the gang is not stranded, but the task "
            "produced no result"
        )
        ungate_pods(
            gated_pods_from_env()
            or wait_for_gated_pods(
                namespace, gang_group, exclude=pod_name, timeout=ungate_timeout
            ),
            job_id,
            namespace,
            backend=placed_backend,
            attributes=placed_attrs,
        )
        _hold_for_workers(namespace, gang_group, pod_name)
        sys.exit(1)

    # ungate the workers, stamping the job id and the session first
    gated_pods = gated_pods_from_env() or wait_for_gated_pods(
        namespace, gang_group, exclude=pod_name, timeout=ungate_timeout
    )
    log(f"ungating {len(gated_pods)} gang pod(s): {gated_pods}")
    session_id = provider.session_id(task)
    n_ok = ungate_pods(
        gated_pods,
        job_id,
        namespace,
        session_id=session_id,
        backend=placed_backend,
        attributes=placed_attrs,
    )
    if n_ok == len(gated_pods):
        log(f"done, {n_ok} gang pod(s) ungated")
    else:
        log(
            f"WARNING: ungated only {n_ok}/{len(gated_pods)} gang pod(s), see errors above"
        )
    _hold_for_workers(namespace, gang_group, pod_name)


if __name__ == "__main__":
    main()
