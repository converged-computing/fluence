"""
fluence.sidecar — provider-agnostic quantum coordination sidecar main loop.

Injected by the Fluence webhook into the quantum-submitting pod. Resolves its
vendor at runtime from the backend annotation, discovers the task the user
application submitted (tagged by the interceptor), polls readiness, and either
ungates gated workers (gang mode) or just logs the queue-position series
(observe-only mode).

Entry point: `fluence-sidecar` console script (see pyproject.toml) -> main().

Environment (injected by the Fluence webhook):
  FLUENCE_POD_UID                 UID of this pod (matches interceptor tag)
  FLUENCE_NAMESPACE               Kubernetes namespace
  FLUENCE_GATED_PODS              comma-separated gated worker names
  FLUENCE_OBSERVE                 "true" for observe-only telemetry mode
  FLUXION_BACKEND / FLUXION_VENDOR  scheduler-chosen backend / vendor
  FLUENCE_TASK_DISCOVERY_TIMEOUT  seconds to wait for discovery (default 300)
  FLUENCE_POLL_INTERVAL           seconds between polls (default 30)
"""

from __future__ import annotations

import os
import sys
import time

from fluence.providers import resolve_from_env
from fluence.providers.base import log
from fluence.ungate import ungate_pods, gated_pods_from_env, namespace_from_env


def _poll(provider, task, poll_interval, ungate):
    mode = "gang" if ungate else "observe-only"
    log(f"{mode} mode: polling queue position")
    last = object()
    while True:
        try:
            if provider.is_ready_to_ungate(task):
                log(f"task ready (position={provider.queue_position(task)})")
                return
            pos = provider.queue_position(task)
            if pos != last:
                log(f"queue position: {pos}")
                last = pos
        except Exception as e:
            log(f"poll error (will retry): {e}")
        time.sleep(poll_interval)


def main():
    pod_uid = os.environ.get("FLUENCE_POD_UID", "")
    backend = os.environ.get("FLUXION_BACKEND", "")
    observe = os.environ.get("FLUENCE_OBSERVE", "").lower() == "true"
    discovery_timeout = int(os.environ.get("FLUENCE_TASK_DISCOVERY_TIMEOUT", 300))
    poll_interval = int(os.environ.get("FLUENCE_POLL_INTERVAL", 30))

    namespace = namespace_from_env()
    gated_pods = gated_pods_from_env()

    log("starting fluence quantum sidecar")
    log(f"  pod_uid={pod_uid} namespace={namespace} backend={backend} "
        f"observe={observe} gated_pods={gated_pods}")

    provider = resolve_from_env()
    if provider is None:
        log("ERROR: could not resolve a quantum provider from the backend")
        if gated_pods and not observe:
            ungate_pods(gated_pods, "", namespace)
        sys.exit(1)
    log(f"resolved provider: {provider.name}")

    if not observe and not gated_pods:
        log("no gated workers and not observe mode — nothing to do")
        return

    task = provider.find_my_task(pod_uid, backend, discovery_timeout)
    if task is None:
        log("ERROR: could not discover quantum task")
        if gated_pods and not observe:
            log("ungating workers anyway to avoid deadlock")
            ungate_pods(gated_pods, "", namespace)
        sys.exit(1)

    job_id = provider.job_id(task)
    log(f"discovered task, job_id={job_id}")

    _poll(provider, task, poll_interval, ungate=not observe)

    if observe:
        log("observe-only run complete")
        return

    ungate_pods(gated_pods, job_id, namespace)
    log("done — workers ungated")


if __name__ == "__main__":
    main()
