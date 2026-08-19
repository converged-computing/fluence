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


def ungate_position(default=1):
    """Queue position we ungate at. One means next in line."""
    try:
        return int(os.environ.get("FLUENCE_UNGATE_POSITION", default))
    except ValueError:
        return default


def position_at_most(pos, threshold):
    """True when the queue position is at or under threshold.

    Braket reports anything over 2000 as the string >2000, so a position that
    is not a number counts as far away.
    """
    try:
        return int(pos) <= int(threshold)
    except (TypeError, ValueError):
        return False


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

    def matches(self, attrs: "dict[str, str]") -> bool:
        """True if this provider handles the backend Fluxion selected.

        attrs is the full set of FLUXION_* values surfaced from the matched
        qdevice's properties (backend, vendor, qrmi_type, arn, region, ...), so a
        provider can route on whichever attribute is authoritative for it. For
        Braket the routing key is qrmi_type (e.g. "braket-ahs", "braket-gate"):
        the device is reached through the Braket SDK regardless of which company
        (amazon, quera, rigetti, iqm, ...) built the hardware.
        """
        raise NotImplementedError

    def find_my_task(self, pod_uid: str, backend: str, timeout: int) -> "Task | None":
        """Search the vendor for the task tagged TAG_KEY=<pod_uid>, polling until
        found or timeout. Returns an opaque Task or None."""
        raise NotImplementedError

    def is_ready_to_ungate(self, task: "Task", position=None) -> bool:
        """True when the gang should be ungated.

        Either the queue position is at or under position, or the task already
        left the queue and can still return a result. Ungating earlier gives
        the gang time to start up, so raise position when it is slow.
        """
        raise NotImplementedError

    def task_failed(self, task: "Task") -> bool:
        """True when the task ended with no result, so there is nothing to
        fetch. Defaults to False for vendors that cannot tell.
        """
        return False

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


def resolve(attrs: "dict[str, str] | None" = None, **legacy) -> "Provider | None":
    """Return the registered provider matching the selected backend, or None.

    attrs is the FLUXION_* attribute set from the matched qdevice. Accepts legacy
    keyword args (vendor=, backend=) for older callers/tests, folding them in.
    """
    a = dict(attrs or {})
    for k, v in legacy.items():
        if v:
            a.setdefault(k, v)
    for p in _REGISTRY:
        try:
            if p.matches(a):
                return p
        except Exception as e:  # a provider's matches() must never break resolution
            log(f"provider {p.name!r} matches() error: {e}")
    return None


def resolve_from_env() -> "Provider | None":
    # Gather every FLUXION_* var the webhook injected (backend, vendor,
    # qrmi_type, arn, region, ...) into a lowercased-key attribute dict.
    attrs = {}
    for k, v in os.environ.items():
        if k.startswith("FLUXION_"):
            attrs[k[len("FLUXION_"):].lower()] = v
    return resolve(attrs)