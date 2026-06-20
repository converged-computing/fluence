"""
sidecars/lib/registry.py — runtime provider resolution.

The sidecar resolves which provider to use at RUNTIME from the backend the
scheduler chose (the fluence.flux-framework.org/backend annotation, surfaced to
the sidecar via the FLUXION_BACKEND env var). This avoids the admission-time
ordering problem: the webhook cannot know the vendor (the scheduler may pick
from a mixed set), but by the time the sidecar runs, the choice is made.

Only the matching provider module is imported and used, so an SDK import failure
in an unrelated provider never affects the run.
"""

from __future__ import annotations

import importlib
import os

from provider import Provider, log


# Registry of provider module names. Each entry is a module under
# sidecars/providers/<name>/sidecar_provider.py exposing a PROVIDER instance.
# Adding a vendor = add a folder + one line here (or auto-discovery below).
_PROVIDER_MODULES = [
    "braket",
    # "ibm",
]


def _load(name: str) -> "Provider | None":
    """Import providers.<name>.sidecar_provider and return its PROVIDER."""
    try:
        mod = importlib.import_module(f"providers.{name}.sidecar_provider")
    except ImportError as e:
        log(f"provider '{name}' module not importable: {e}")
        return None
    provider = getattr(mod, "PROVIDER", None)
    if provider is None:
        log(f"provider '{name}' module has no PROVIDER instance")
    return provider


def resolve_provider(vendor: str, backend: str) -> "Provider | None":
    """
    Return the provider matching the given vendor/backend, or None.

    vendor:  fluence.flux-framework.org/vendor (if the graph supplies it)
    backend: fluence.flux-framework.org/backend (FLUXION_BACKEND)
    """
    for name in _PROVIDER_MODULES:
        provider = _load(name)
        if provider is None:
            continue
        try:
            if provider.matches(vendor, backend):
                log(f"resolved provider: {provider.name} "
                    f"(vendor={vendor!r} backend={backend!r})")
                return provider
        except Exception as e:
            log(f"provider '{name}' matches() error: {e}")
    log(f"no provider matched vendor={vendor!r} backend={backend!r}")
    return None


def resolve_from_env() -> "Provider | None":
    """Resolve the provider from environment (the common runtime path)."""
    vendor  = os.environ.get("FLUXION_VENDOR", "")
    backend = os.environ.get("FLUXION_BACKEND", "")
    return resolve_provider(vendor, backend)
