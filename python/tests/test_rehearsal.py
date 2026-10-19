"""
Tests for the scout exit marker. A scout that ends without submitting a task,
a rehearsal with no credentials, leaves a marker that ends the sidecar's wait
for the handoff at once, so the workers are released instead of gated until
the discovery timeout.

Run: PYTHONPATH=python python3 python/tests/test_rehearsal.py
"""

import os
import sys
import tempfile
import time


def test_marker_roundtrip():
    from fluence import handoff

    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, "scout-exited")
        assert not handoff.scout_exited(p)
        assert handoff.mark_scout_exited(p)
        assert handoff.scout_exited(p)


def test_no_marker_when_a_task_was_published():
    from fluence import handoff

    with tempfile.TemporaryDirectory() as d:
        os.environ["FLUENCE_HANDOFF_DIR"] = d
        try:
            assert handoff.publish("job-1")
            assert not handoff.mark_scout_exited()
            assert not handoff.scout_exited()
        finally:
            os.environ.pop("FLUENCE_HANDOFF_DIR", None)


def test_wait_returns_at_once_after_the_scout_exits():
    from fluence import handoff

    with tempfile.TemporaryDirectory() as d:
        os.environ["FLUENCE_HANDOFF_DIR"] = d
        try:
            handoff.mark_scout_exited()
            start = time.time()
            assert handoff.wait(30, interval=1) is None
            assert time.time() - start < 5
        finally:
            os.environ.pop("FLUENCE_HANDOFF_DIR", None)


def test_wait_still_times_out_without_a_marker():
    from fluence import handoff

    with tempfile.TemporaryDirectory() as d:
        start = time.time()
        assert handoff.wait(1, interval=1, path=os.path.join(d, "task.json")) is None
        assert time.time() - start >= 1


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
