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
from fluence.ungate import ungate_pods, gated_pods_from_env, namespace_from_env, wait_for_gated_pods


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
    pod_name = os.environ.get("FLUENCE_POD_NAME", "")
    group = os.environ.get("FLUENCE_GROUP", "")
    backend = os.environ.get("FLUXION_BACKEND", "")
    observe = os.environ.get("FLUENCE_OBSERVE", "").lower() == "true"
    discovery_timeout = int(os.environ.get("FLUENCE_TASK_DISCOVERY_TIMEOUT", 300))
    poll_interval = int(os.environ.get("FLUENCE_POLL_INTERVAL", 30))
    expected_workers = int(os.environ.get("FLUENCE_EXPECTED_WORKERS", 0))
    ungate_timeout = int(os.environ.get("FLUENCE_UNGATE_TIMEOUT", 120))

    namespace = namespace_from_env()

    log("starting fluence quantum sidecar")
    log(f"  pod_uid={pod_uid} namespace={namespace} group={group} "
        f"backend={backend} observe={observe} expected_workers={expected_workers}")

    provider = resolve_from_env()
    if provider is None:
        log("ERROR: could not resolve a quantum provider from the backend")
        sys.exit(1)
    log(f"resolved provider: {provider.name}")

    task = provider.find_my_task(pod_uid, backend, discovery_timeout)
    if task is None:
        log("ERROR: could not discover quantum task")
        if not observe:
            ungate_pods(wait_for_gated_pods(namespace, group, expected_workers,
                                            exclude=pod_name, timeout=ungate_timeout),
                        "", namespace)
        sys.exit(1)

    job_id = provider.job_id(task)
    log(f"discovered task, job_id={job_id}")

    _poll(provider, task, poll_interval, ungate=not observe)

    if observe:
        log("observe-only run complete")
        return

    # Wait until all expected gated workers are present (gang is submitted
    # together), then ungate them. expected_workers is N-1, propagated by the
    # webhook from the leader at admission; if unset we ungate whatever is found.
    gated_pods = gated_pods_from_env() or wait_for_gated_pods(
        namespace, group, expected_workers, exclude=pod_name,
        timeout=ungate_timeout)
    log(f"ungating {len(gated_pods)} worker(s): {gated_pods}")
    n_ok = ungate_pods(gated_pods, job_id, namespace)
    if n_ok == len(gated_pods):
        log(f"done — {n_ok} worker(s) ungated")
    else:
        log(f"WARNING: ungated only {n_ok}/{len(gated_pods)} worker(s) — see errors above")


if __name__ == "__main__":
    main()
