"""
fluence.providers.base — the provider interface and registration machinery.

A provider is a per-vendor plug-in (AWS Braket, IBM Qiskit Runtime, ...) that
implements two halves of the quantum-coordination mechanism:

  - INTERCEPTOR hook (`install_interceptor`): runs inside the user's application
    container; monkey-patches the vendor SDK's submit call to stamp the shared
    `fluence-pod-uid` tag on every task. Must fail-soft if the vendor SDK is not
    importable in that container.

  - SIDECAR methods (`matches`, `find_my_task`, `is_ready_to_ungate`,
    `queue_position`, `job_id`): run inside the Fluence sidecar container; find
    the tagged task, poll readiness, and yield a vendor-neutral job id.

Providers self-register by calling `register()` at import time. The package
imports every provider submodule (see fluence.providers.__init__) so importing
the package registers them all. Registration is the single extension point:
adding a vendor is one new module that calls register().
"""

from __future__ import annotations

import os
from datetime import datetime, timezone


# Shared convention between every interceptor hook and every find_my_task.
# The interceptor stamps this tag key with the pod UID; the sidecar searches
# for it. Changing it is a coordinated change across all providers.
TAG_KEY = "fluence-pod-uid"


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[fluence] {ts} {msg}", flush=True)


class Task:
    """
    Opaque handle to a vendor quantum task. A provider returns its own subclass
    from find_my_task; the framework treats it opaquely and only passes it back
    to that provider. Vendor identifiers (ARN, job id) live inside.
    """


class Provider:
    """Interface every quantum vendor implements. See module docstring."""

    #: short stable name, e.g. "braket", "ibm"
    name: str = "base"

    # ── interceptor half (runs in the user container) ──────────────────────────

    def install_interceptor(self, pod_uid: str) -> bool:
        """
        Monkey-patch this vendor's SDK submit call to stamp TAG_KEY=<pod_uid>.
        Return True if the patch was installed, False if the SDK is absent
        (fail-soft). Must never raise.
        """
        raise NotImplementedError

    # ── sidecar half (runs in the sidecar container) ───────────────────────────

    def matches(self, vendor: str, backend: str) -> bool:
        """True if this provider handles the given vendor/backend (resolved at
        runtime from the pod's backend annotation)."""
        raise NotImplementedError

    def find_my_task(self, pod_uid: str, backend: str, timeout: int) -> "Task | None":
        """Search the vendor for the task tagged TAG_KEY=<pod_uid>, polling until
        found or timeout. Returns an opaque Task or None."""
        raise NotImplementedError

    def is_ready_to_ungate(self, task: "Task") -> bool:
        """True when workers should be ungated — queue position == 1 or the task
        is already RUNNING/terminal. Always implementable."""
        raise NotImplementedError

    def queue_position(self, task: "Task") -> "int | None":
        """Optional richer telemetry: integer queue position (1 == next), or None
        if the vendor does not expose one. Not required for the ungate decision."""
        return None

    def job_id(self, task: "Task") -> str:
        """Stable, vendor-neutral identifier handed to workers at ungate time."""
        raise NotImplementedError


# ── registry ────────────────────────────────────────────────────────────────────

_REGISTRY: "list[Provider]" = []


def register(provider: Provider) -> None:
    """Register a provider. Called by each provider module at import time."""
    _REGISTRY.append(provider)


def all_providers() -> "list[Provider]":
    return list(_REGISTRY)


def resolve(vendor: str = "", backend: str = "") -> "Provider | None":
    """Return the registered provider matching vendor/backend, or None."""
    for p in _REGISTRY:
        try:
            if p.matches(vendor, backend):
                return p
        except Exception as e:  # a provider's matches() must never break resolution
            log(f"provider {p.name!r} matches() error: {e}")
    return None


def resolve_from_env() -> "Provider | None":
    return resolve(os.environ.get("FLUXION_VENDOR", ""),
                   os.environ.get("FLUXION_BACKEND", ""))
