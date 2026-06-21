"""
Tests for the fluence package. Run: python3 -m pytest python/tests/
None of these require a vendor SDK — they exercise registration, resolution,
fail-soft interceptor behavior, and the staging mechanism.
"""

import os
import subprocess
import sys
import tempfile


def test_braket_registered():
    from fluence.providers import all_providers
    assert "braket" in [p.name for p in all_providers()]


def test_resolve_braket_by_vendor():
    from fluence.providers import resolve
    assert resolve("braket", "").name == "braket"


def test_resolve_braket_by_arn_backend():
    from fluence.providers import resolve
    p = resolve("", "arn:aws:braket:us-east-1::device/quantum-simulator/amazon/sv1")
    assert p is not None and p.name == "braket"


def test_resolve_unknown_returns_none():
    from fluence.providers import resolve
    assert resolve("nope", "nope") is None


def test_interceptor_failsoft_without_sdk():
    # Importing the interceptor must not raise even though braket is absent.
    import importlib
    import fluence.interceptor
    importlib.reload(fluence.interceptor)  # re-run install(); must not raise


def test_braket_install_interceptor_failsoft():
    from fluence.providers.braket import PROVIDER
    # No braket SDK installed -> returns False, never raises.
    assert PROVIDER.install_interceptor("uid") is False


def test_job_id_is_arn():
    from fluence.providers.braket import PROVIDER, BraketTask
    arn = "arn:aws:braket:us-east-1::quantum-task/abc-123"
    assert PROVIDER.job_id(BraketTask(arn)) == arn


def test_stage_produces_importable_package():
    # python -m fluence.stage <dest> must create an importable fluence + a
    # top-level sitecustomize.py.
    with tempfile.TemporaryDirectory() as d:
        subprocess.run([sys.executable, "-m", "fluence.stage", d], check=True)
        assert os.path.isfile(os.path.join(d, "sitecustomize.py"))
        assert os.path.isfile(os.path.join(d, "fluence", "__init__.py"))
        assert os.path.isdir(os.path.join(d, "fluence", "providers"))
        # __pycache__ and tests must be excluded from the staged copy.
        assert not os.path.exists(os.path.join(d, "fluence", "tests"))


def test_staged_sitecustomize_runs_interceptor():
    # Stage, then run a subprocess with ONLY the staged dir on PYTHONPATH and a
    # fake braket SDK; the interceptor must patch AwsDevice.run to inject the tag.
    with tempfile.TemporaryDirectory() as d:
        staged = os.path.join(d, "staged")
        subprocess.run([sys.executable, "-m", "fluence.stage", staged], check=True)

        fakesdk = os.path.join(d, "fakesdk")
        os.makedirs(os.path.join(fakesdk, "braket", "aws"))
        open(os.path.join(fakesdk, "braket", "__init__.py"), "w").close()
        with open(os.path.join(fakesdk, "braket", "aws", "__init__.py"), "w") as f:
            f.write(
                "class AwsDevice:\n"
                "    def run(self, spec, *a, **k):\n"
                "        print('TAGS', k.get('tags'))\n"
            )
        app = os.path.join(d, "app.py")
        with open(app, "w") as f:
            f.write("from braket.aws import AwsDevice\nAwsDevice().run('c')\n")

        env = {
            "PATH": os.environ.get("PATH", ""),
            "HOME": d,
            "FLUENCE_POD_UID": "pod-xyz",
            "PYTHONPATH": staged + os.pathsep + fakesdk,
        }
        out = subprocess.run([sys.executable, app], env=env,
                             capture_output=True, text=True)
        assert "fluence-pod-uid" in out.stdout, out.stdout + out.stderr
        assert "pod-xyz" in out.stdout, out.stdout + out.stderr



def test_braket_matches_amazon_vendor():
    # The resource graph labels Braket devices vendor="amazon"; the provider
    # must resolve for that (and for sv1 by name/arn).
    from fluence.providers import resolve
    assert resolve("amazon", "sv1") is not None
    assert resolve("amazon", "sv1").name == "braket"
    assert resolve("", "arn:aws:braket:::device/quantum-simulator/amazon/sv1").name == "braket"


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
    print(f"\n{len(fns) - failed}/{len(fns)} passed")
    sys.exit(1 if failed else 0)
