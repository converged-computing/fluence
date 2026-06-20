"""
fluence.interceptor — installs every registered provider's submit-time tag hook.

Runs inside the user's application container, triggered automatically by a
sitecustomize.py on PYTHONPATH (staged there by the Fluence init container). On
import it asks every registered provider to install its interceptor; each
provider fail-soft skips if its vendor SDK is not present in this container.

This module's import must never raise — sitecustomize guards it, but we also
guard here so a single provider bug cannot affect the user application.
"""

from __future__ import annotations

import os


def install() -> None:
    pod_uid = os.environ.get("FLUENCE_POD_UID", "")
    try:
        from fluence.providers import all_providers
    except Exception as e:  # pragma: no cover - defensive
        print(f"[fluence] interceptor: providers unavailable: {e}", flush=True)
        return

    for provider in all_providers():
        try:
            if provider.install_interceptor(pod_uid):
                print(f"[fluence] interceptor installed for provider "
                      f"{provider.name!r} (pod_uid={pod_uid})", flush=True)
        except Exception as e:
            # A provider's hook must never break the user app.
            print(f"[fluence] interceptor for {provider.name!r} skipped: {e}",
                  flush=True)


# Install on import.
install()
