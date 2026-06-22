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
    assert resolve({"vendor": "braket"}).name == "braket"


def test_resolve_braket_by_arn_backend():
    from fluence.providers import resolve
    p = resolve({"arn": "arn:aws:braket:us-east-1::device/quantum-simulator/amazon/sv1"})
    assert p is not None and p.name == "braket"


def test_resolve_unknown_returns_none():
    from fluence.providers import resolve
    assert resolve({"vendor": "nope", "backend": "nope"}) is None


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



def test_braket_matches_by_qrmi_type_and_vendor():
    # The authoritative routing key is qrmi_type (braket-*), which holds even
    # when the hardware vendor is quera/rigetti/iqm rather than amazon.
    from fluence.providers import resolve
    assert resolve({"vendor": "amazon", "backend": "sv1"}).name == "braket"
    # aquila: vendor=quera but qrmi_type=braket-ahs must still route to braket.
    assert resolve({"vendor": "quera", "backend": "aquila",
                    "qrmi_type": "braket-ahs"}).name == "braket"



def test_find_my_task_matches_tag_client_side():
    # SearchQuantumTasks has no tags filter; the provider must match the tag
    # client-side. It calls search_quantum_tasks DIRECTLY (not the paginator,
    # which rejects empty filters) and pages via nextToken. Fake that here.
    from fluence.providers.braket import BraketProvider, BraketTask
    from fluence.providers.base import TAG_KEY

    class FakeClient:
        def __init__(self):
            self.calls = 0
        def search_quantum_tasks(self, filters=None, maxResults=None, nextToken=None):
            assert isinstance(filters, list)          # empty list is valid
            # Two pages, to exercise nextToken paging.
            if nextToken is None:
                return {"quantumTasks": [
                    {"quantumTaskArn": "arn:other", "createdAt": "2026-01-01T00:00:00Z",
                     "tags": {TAG_KEY: "someone-else"}},
                ], "nextToken": "page2"}
            return {"quantumTasks": [
                {"quantumTaskArn": "arn:mine", "createdAt": "2026-01-02T00:00:00Z",
                 "tags": {TAG_KEY: "me-123"}},
            ]}  # no nextToken -> last page

    p = BraketProvider()
    p._client = lambda backend: FakeClient()   # bypass real boto3
    task = p.find_my_task("me-123", "sv1", timeout=5)
    assert task is not None and task.arn == "arn:mine"

    # No matching tag -> times out -> None (short timeout to keep the test fast).
    none_task = p.find_my_task("nobody", "sv1", timeout=1)
    assert none_task is None


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
