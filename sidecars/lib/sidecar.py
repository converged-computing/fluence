#!/usr/bin/env python3
"""
sidecars/lib/sidecar.py — provider-agnostic Fluence quantum coordination sidecar.

Injected by the Fluence webhook into any pod that requests a quantum resource
(fluxion.flux-framework.org/qpu). Resolves its vendor at runtime, discovers the
task the user application submitted, polls readiness, and:

  - GANG MODE (gated sibling workers present): ungates them when the task is
    ready (queue position == 1 or RUNNING), handing them the job id.
  - OBSERVE-ONLY MODE (no workers; opt-in via the observe label, surfaced as
    FLUENCE_OBSERVE=true): polls and logs the queue-position series as telemetry,
    ungating nothing.

Environment (injected by the Fluence webhook):
  FLUENCE_POD_UID                 UID of this pod (matches interceptor tag)
  FLUENCE_NAMESPACE               Kubernetes namespace
  FLUENCE_GATED_PODS              Comma-separated gated sibling worker names
  FLUENCE_OBSERVE                 "true" for observe-only telemetry mode
  FLUXION_BACKEND                 backend chosen by the scheduler
  FLUXION_VENDOR                  vendor (if the graph supplies it)
  FLUENCE_TASK_DISCOVERY_TIMEOUT  seconds to wait for task discovery (default 300)
  FLUENCE_POLL_INTERVAL           seconds between polls (default 30)
"""

from __future__ import annotations

import os
import sys
import time

# Make sibling lib modules and the providers package importable whether running
# from the repo (sidecars/) or the installed image (/app).
_here = os.path.dirname(os.path.abspath(__file__))
for p in (_here, os.path.join(_here, ".."), os.path.join(_here, "..", "..")):
    if os.path.isdir(p) and p not in sys.path:
        sys.path.insert(0, p)

from provider import log                      # noqa: E402
from registry import resolve_from_env         # noqa: E402
from ungate import ungate_pods, gated_pods_from_env, namespace_from_env  # noqa: E402


def observe_loop(provider, task, poll_interval):
    """Observe-only mode: log the queue-position series until the task runs."""
    log("observe-only mode: polling queue position (will not ungate)")
    last = object()
    while True:
        try:
            if provider.is_ready_to_ungate(task):
                pos = provider.queue_position(task)
                log(f"task ready (position={pos}) — observe-only, nothing to ungate")
                return
            pos = provider.queue_position(task)
            if pos != last:
                log(f"queue position: {pos}")
                last = pos
        except Exception as e:
            log(f"poll error (will retry): {e}")
        time.sleep(poll_interval)


def gang_loop(provider, task, poll_interval):
    """Gang mode: poll until ready, then return so workers can be ungated."""
    log("gang mode: polling until task is ready to ungate workers")
    last = object()
    while True:
        try:
            if provider.is_ready_to_ungate(task):
                log("task is ready — ungating workers")
                return
            pos = provider.queue_position(task)
            if pos != last:
                log(f"queue position: {pos}")
                last = pos
        except Exception as e:
            log(f"poll error (will retry): {e}")
        time.sleep(poll_interval)


def main():
    pod_uid    = os.environ.get("FLUENCE_POD_UID", "")
    backend    = os.environ.get("FLUXION_BACKEND", "")
    observe    = os.environ.get("FLUENCE_OBSERVE", "").lower() == "true"
    discovery_timeout = int(os.environ.get("FLUENCE_TASK_DISCOVERY_TIMEOUT", 300))
    poll_interval     = int(os.environ.get("FLUENCE_POLL_INTERVAL", 30))

    namespace  = namespace_from_env()
    gated_pods = gated_pods_from_env()

    log("starting fluence quantum sidecar")
    log(f"  pod_uid   : {pod_uid}")
    log(f"  namespace : {namespace}")
    log(f"  backend   : {backend}")
    log(f"  observe   : {observe}")
    log(f"  gated_pods: {gated_pods}")

    # Resolve the provider at runtime from the scheduler-chosen backend.
    provider = resolve_from_env()
    if provider is None:
        log("ERROR: could not resolve a quantum provider from the backend "
            "annotation — exiting")
        # Avoid deadlocking gated workers if any.
        if gated_pods and not observe:
            ungate_pods(gated_pods, "", namespace)
        sys.exit(1)

    if not observe and not gated_pods:
        log("no gated workers and not in observe mode — nothing to do")
        sys.exit(0)

    # 1. Discover the task the interceptor tagged with our pod uid.
    task = provider.find_my_task(pod_uid, backend, discovery_timeout)
    if task is None:
        log("ERROR: could not discover quantum task")
        if gated_pods and not observe:
            log("ungating workers anyway to avoid deadlock")
            ungate_pods(gated_pods, "", namespace)
        sys.exit(1)

    job_id = provider.job_id(task)
    log(f"discovered task, job_id={job_id}")

    # 2. Poll.
    if observe:
        observe_loop(provider, task, poll_interval)
        log("observe-only run complete")
        return

    gang_loop(provider, task, poll_interval)

    # 3. Ungate workers, handing them the job id.
    ungate_pods(gated_pods, job_id, namespace)
    log("done — workers ungated")


if __name__ == "__main__":
    main()
