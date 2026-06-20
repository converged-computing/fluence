"""
sidecars/test/test_provider.py — unit tests for the provider framework.

Run: python3 -m pytest sidecars/test/test_provider.py   (or plain python3)
These tests do NOT require any vendor SDK to be installed — they exercise the
interface wiring, registry resolution, and fail-soft behavior.
"""

import os
import sys

_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_here, "..", "lib"))
sys.path.insert(0, os.path.join(_here, ".."))


def test_braket_provider_matches():
    from providers.braket.sidecar_provider import PROVIDER
    assert PROVIDER.matches("braket", "")
    assert PROVIDER.matches("", "arn:aws:braket:::device/quantum-simulator/amazon/sv1")
    assert PROVIDER.matches("", "braket-sv1")
    assert not PROVIDER.matches("ibm", "ibm_marrakesh")
    assert not PROVIDER.matches("", "")


def test_registry_resolves_braket():
    import registry
    p = registry.resolve_provider("braket", "")
    assert p is not None
    assert p.name == "braket"


def test_registry_no_match_returns_none():
    import registry
    p = registry.resolve_provider("nonexistent-vendor", "nonexistent-backend")
    assert p is None


def test_resolve_from_env():
    import registry
    os.environ["FLUXION_VENDOR"] = "braket"
    os.environ["FLUXION_BACKEND"] = ""
    p = registry.resolve_from_env()
    assert p is not None and p.name == "braket"


def test_job_id_is_arn():
    from providers.braket.sidecar_provider import PROVIDER, BraketTask
    arn = "arn:aws:braket:us-east-1::quantum-task/abc-123"
    assert PROVIDER.job_id(BraketTask(arn)) == arn


def test_interceptor_failsoft_without_sdk():
    # Importing the braket interceptor block must NOT raise even though the
    # braket SDK is not installed in the test environment.
    import importlib.util
    path = os.path.join(_here, "..", "providers", "braket", "interceptor.py")
    spec = importlib.util.spec_from_file_location("braket_interceptor", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # runs _fluence_install_braket() — must no-op


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    failed = 0
    for fn in fns:
        try:
            fn()
            print(f"PASS {fn.__name__}")
        except Exception as e:
            failed += 1
            print(f"FAIL {fn.__name__}: {e}")
    print(f"\n{len(fns)-failed}/{len(fns)} passed")
    sys.exit(1 if failed else 0)
