"""
sidecars/lib/provider.py — the common provider interface for Fluence quantum
coordination sidecars.

Every quantum cloud vendor (AWS Braket, IBM Qiskit Runtime, ...) implements
this interface so the sidecar framework can discover the submitted task, poll
its queue position, decide when to ungate classical workers, and hand a stable
job identifier to those workers.

Two halves of the mechanism live in different processes:

  - The INTERCEPTOR (a small snippet mounted into the user's application
    container via PYTHONSTARTUP) stamps a tag `fluence-pod-uid=<uid>` onto every
    task at submission time. It is NOT part of this interface — it is a separate
    per-provider snippet joined to the provider only by the shared tag
    convention (TAG_KEY below).

  - The SIDECAR (this framework, running in the fluence sidecar container)
    implements this Provider interface: it searches for the task by that tag,
    polls readiness, and ungates workers.

Vendor-specific identifiers (a Braket task ARN, an IBM job id, a GCP operation
name) are deliberately NOT in the interface. They are reachable through the
opaque Task object a provider returns, and surfaced generically through
`job_id()`. The cross-vendor concept the framework propagates to workers is a
job id, never an ARN.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone


# Shared convention between every interceptor snippet and every provider's
# find_my_task. The interceptor stamps this tag key with the pod UID; the
# provider searches for it. Changing this is a coordinated change across all
# interceptor snippets.
TAG_KEY = "fluence-pod-uid"


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[fluence-sidecar] {ts} {msg}", flush=True)


class Task:
    """
    Opaque handle to a vendor quantum task.

    A provider returns its own Task subclass (or any object) from find_my_task;
    the framework treats it opaquely and only ever passes it back to that same
    provider's methods. Vendor identifiers (ARN, job id) live inside.
    """
    pass


class Provider:
    """
    The contract a quantum vendor implements so the sidecar can coordinate it.

    A provider is resolved at RUNTIME (not at webhook admission) from the
    backend annotation the scheduler writes (fluence.flux-framework.org/backend),
    because the scheduler may choose a vendor from a mixed set — the vendor is
    not known at admission time.
    """

    # Short stable name, e.g. "braket", "ibm". Used in logs and registry keys.
    name: str = "base"

    def matches(self, vendor: str, backend: str) -> bool:
        """
        Return True if this provider handles the given vendor/backend, as
        resolved from the pod's backend annotation at runtime.
        """
        raise NotImplementedError

    def find_my_task(self, pod_uid: str, backend: str, timeout: int) -> "Task | None":
        """
        Search the vendor for the task tagged TAG_KEY=<pod_uid>, polling until
        found or timeout. Returns an opaque Task, or None on timeout.

        This is the discovery half that pairs with the interceptor snippet: the
        interceptor stamped the tag at submission, this finds it.
        """
        raise NotImplementedError

    def is_ready_to_ungate(self, task: "Task") -> bool:
        """
        The decision primitive: True when classical workers should be ungated —
        i.e. the quantum task is imminent (queue position == 1) or already
        RUNNING/terminal. Always implementable, even for vendors that do not
        expose a numeric queue position.
        """
        raise NotImplementedError

    def queue_position(self, task: "Task") -> "int | None":
        """
        Richer telemetry signal: the current integer queue position (1 == next),
        or None if the vendor does not expose a numeric position. Used for
        observe-only mode and for logging the position series. Not required for
        the ungate decision (see is_ready_to_ungate).
        """
        return None

    def job_id(self, task: "Task") -> str:
        """
        A stable, cross-vendor identifier for the task, handed to workers at
        ungate time via the annotation. NOT vendor-specific in concept — for
        Braket this happens to be the task ARN, for IBM the job id, but the
        framework only knows it as "the job id".
        """
        raise NotImplementedError
