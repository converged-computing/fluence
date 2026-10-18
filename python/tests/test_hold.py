"""
Tests for holding the scout until the worker group is done. The sidecar
stamps the session at ungate, waits for the group, and writes a marker, and a
scout's release() waits for that marker.

Run: PYTHONPATH=python python3 python/tests/test_hold.py
"""

import json
import os
import sys
import tempfile
import time
import types


def _fake_qrmi(released):
    qrmi = types.ModuleType("qrmi")

    class QuantumResource:
        def __init__(self, *a, **k):
            pass

        def acquire(self):
            return "tok"

        def task_start(self, *a, **k):
            return "job"

        def release(self, lock=None):
            released.append(lock)

    qrmi.QuantumResource = QuantumResource
    qrmi.ResourceType = types.SimpleNamespace(
        QiskitRuntimeService="QiskitRuntimeService"
    )
    qrmi.Payload = types.SimpleNamespace()
    qrmi.TaskStatus = types.SimpleNamespace()
    sys.modules["qrmi"] = qrmi
    return QuantumResource


def _clean():
    for k in (
        "FLUENCE_HANDOFF_DIR",
        "FLUENCE_COORDINATION_ROLE",
        "FLUENCE_SESSION_HOLD_TIMEOUT",
    ):
        os.environ.pop(k, None)
    sys.modules.pop("qrmi", None)


def test_marker_roundtrip():
    from fluence import handoff

    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, "workers-done")
        assert not handoff.workers_done(p)
        assert handoff.mark_workers_done(p)
        assert handoff.workers_done(p)
        assert handoff.wait_workers_done(1, interval=0.1, path=p)


def test_wait_times_out_without_marker():
    from fluence import handoff

    with tempfile.TemporaryDirectory() as d:
        assert (
            handoff.wait_workers_done(
                0.3, interval=0.1, path=os.path.join(d, "workers-done")
            )
            is False
        )


def test_ungate_stamps_job_id_and_session():
    from fluence import ungate

    calls = []
    orig = ungate.kubectl
    ungate.kubectl = lambda args: calls.append(args) or ""
    try:
        assert ungate.ungate_pods(["w-1"], "job-1", "ns", session_id="sess-9") == 1
    finally:
        ungate.kubectl = orig
    annotate = [c for c in calls if c[0] == "annotate"][0]
    assert f"{ungate.JOB_ID_ANNOTATION}=job-1" in annotate
    assert f"{ungate.SESSION_ID_ANNOTATION}=sess-9" in annotate
    assert any(c[0] == "patch" for c in calls)


def test_ungate_without_session_stamps_only_job_id():
    from fluence import ungate

    calls = []
    orig = ungate.kubectl
    ungate.kubectl = lambda args: calls.append(args) or ""
    try:
        ungate.ungate_pods(["w-1"], "job-1", "ns")
    finally:
        ungate.kubectl = orig
    annotate = [c for c in calls if c[0] == "annotate"][0]
    assert not any(ungate.SESSION_ID_ANNOTATION in a for a in annotate)


def test_wait_for_pods_done_ignores_the_scout():
    from fluence import ungate

    rounds = iter(
        [["Running", "Running"], ["Succeeded", "Running"], ["Succeeded", "Failed"]]
    )

    def fake(args):
        items = [{"metadata": {"name": "scout"}, "status": {"phase": "Running"}}]
        items += [
            {"metadata": {"name": f"w-{i}"}, "status": {"phase": p}}
            for i, p in enumerate(next(rounds))
        ]
        return json.dumps({"items": items})

    orig = ungate.kubectl
    ungate.kubectl = fake
    try:
        assert (
            ungate.wait_for_pods_done("ns", "g", exclude="scout", timeout=5, interval=0)
            is True
        )
    finally:
        ungate.kubectl = orig


def test_wait_for_pods_done_times_out():
    from fluence import ungate

    orig = ungate.kubectl
    ungate.kubectl = lambda args: json.dumps(
        {"items": [{"metadata": {"name": "w-0"}, "status": {"phase": "Running"}}]}
    )
    try:
        assert ungate.wait_for_pods_done("ns", "g", timeout=0.2, interval=0.05) is False
    finally:
        ungate.kubectl = orig


def test_scout_release_waits_for_marker():
    from fluence.providers.qrmi import QRMIProvider
    from fluence import handoff

    released = []
    with tempfile.TemporaryDirectory() as d:
        os.environ["FLUENCE_HANDOFF_DIR"] = d
        os.environ["FLUENCE_COORDINATION_ROLE"] = "scout"
        os.environ["FLUENCE_SESSION_HOLD_TIMEOUT"] = "5"
        QR = _fake_qrmi(released)
        try:
            assert QRMIProvider().install_interceptor("uid") is True
            handoff.mark_workers_done()
            t0 = time.time()
            QR().release("tok")
            assert released == ["tok"]
            assert time.time() - t0 < 2
        finally:
            _clean()


def test_scout_release_gives_up_after_timeout():
    from fluence.providers.qrmi import QRMIProvider

    released = []
    with tempfile.TemporaryDirectory() as d:
        os.environ["FLUENCE_HANDOFF_DIR"] = d
        os.environ["FLUENCE_COORDINATION_ROLE"] = "scout"
        os.environ["FLUENCE_SESSION_HOLD_TIMEOUT"] = "1"
        QR = _fake_qrmi(released)
        try:
            QRMIProvider().install_interceptor("uid")
            t0 = time.time()
            QR().release("tok")
            assert released == ["tok"]
            assert time.time() - t0 >= 1
        finally:
            _clean()


def test_non_scout_release_passes_through():
    from fluence.providers.qrmi import QRMIProvider

    released = []
    with tempfile.TemporaryDirectory() as d:
        os.environ["FLUENCE_HANDOFF_DIR"] = d
        QR = _fake_qrmi(released)
        try:
            QRMIProvider().install_interceptor("uid")
            t0 = time.time()
            QR().release("tok")
            assert released == ["tok"]
            assert time.time() - t0 < 1
        finally:
            _clean()


if __name__ == "__main__":
    fns = [
        v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)
    ]
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
