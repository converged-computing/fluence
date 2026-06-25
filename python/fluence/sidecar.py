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

# MUST match handlers.WorkerGroupSuffix in the Go webhook. A quantum gang of size
# N is split into the leader group <group> (size 1) and the worker group
# <group>-workers (size N-1, all gated). The sidecar runs in the leader and
# discovers/ungates workers in the WORKER group, not the leader's group.
WORKER_GROUP_SUFFIX = "-workers"


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
    # Two-group quantum split: the leader (where this sidecar runs) is in
    # <group>; the gated workers were moved to <group>-workers by the webhook.
    # WORKER_GROUP_SUFFIX MUST match handlers.WorkerGroupSuffix in the Go webhook
    # (pkg/webhook/handlers/quantum.go). The webhook also passes the base group
    # via FLUENCE_WORKER_GROUP_BASE; prefer it, fall back to FLUENCE_GROUP.
    worker_group_base = os.environ.get("FLUENCE_WORKER_GROUP_BASE", group)
    worker_group = worker_group_base + WORKER_GROUP_SUFFIX if worker_group_base else ""
    backend = os.environ.get("FLUXION_BACKEND", "")
    observe = os.environ.get("FLUENCE_OBSERVE", "").lower() == "true"
    discovery_timeout = int(os.environ.get("FLUENCE_TASK_DISCOVERY_TIMEOUT", 300))
    poll_interval = int(os.environ.get("FLUENCE_POLL_INTERVAL", 30))
    expected_workers = int(os.environ.get("FLUENCE_EXPECTED_WORKERS", 0))
    ungate_timeout = int(os.environ.get("FLUENCE_UNGATE_TIMEOUT", 120))

    namespace = namespace_from_env()

    log("starting fluence quantum sidecar")
    log(f"  pod_uid={pod_uid} namespace={namespace} group={group} "
        f"backend={backend} observe={observe} expected_workers={expected_workers} worker_group={worker_group}")

    provider = resolve_from_env()
    if provider is None:
        log("ERROR: could not resolve a quantum provider from the backend")
        sys.exit(1)
    log(f"resolved provider: {provider.name}")

    task = provider.find_my_task(pod_uid, backend, discovery_timeout)
    if task is None:
        log("ERROR: could not discover quantum task")
        if not observe:
            ungate_pods(wait_for_gated_pods(namespace, worker_group, expected_workers,
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
        namespace, worker_group, expected_workers, exclude=pod_name,
        timeout=ungate_timeout)
    log(f"ungating {len(gated_pods)} worker(s): {gated_pods}")
    n_ok = ungate_pods(gated_pods, job_id, namespace)
    if n_ok == len(gated_pods):
        log(f"done — {n_ok} worker(s) ungated")
    else:
        log(f"WARNING: ungated only {n_ok}/{len(gated_pods)} worker(s) — see errors above")


if __name__ == "__main__":
    main()
