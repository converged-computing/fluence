"""
Tests for batch-mode (per-result) ungating in fluence.ungate.

Mocks kubectl (no cluster) and the provider (controllable, out-of-order task
readiness). Key properties, none of which assume an indexed Job:
  - each gated worker is released the moment ANY task completes,
  - stamped with that task's own job id,
  - workers are interchangeable (no slot / completion-index mapping),
  - surplus tasks (the producer's own share) and never-completing tasks are
    handled without stranding the rest.
"""
import json

import fluence.ungate as ung


class FakeTask:
    def __init__(self, name):
        self.name = name


class FakeProvider:
    """Tasks become ready in the given order, one per poll tick, to force a
    specific (out-of-order) completion sequence."""

    def __init__(self, ready_order):
        self._order = list(ready_order)
        self.ready = set()
        self.tick()  # first task ready immediately

    def tick(self):
        if self._order:
            self.ready.add(self._order.pop(0))

    def is_ready_to_ungate(self, task):
        return task.name in self.ready

    def job_id(self, task):
        return f"job-{task.name}"


def _record_kubectl(monkeypatch):
    calls = []
    monkeypatch.setattr(ung, "kubectl", lambda args: calls.append(list(args)) or "")
    return calls


def _annotations(calls):
    return {a[2]: a[5].split("=", 1)[1] for a in calls if a[0] == "annotate"}


def _gate_removals(calls):
    return [a[2] for a in calls if a[0] == "patch"]


def test_per_result_assigns_each_completed_task_to_a_free_worker(monkeypatch):
    calls = _record_kubectl(monkeypatch)
    tasks = [FakeTask("a"), FakeTask("b"), FakeTask("c")]
    workers = ["w-x", "w-y", "w-z"]
    prov = FakeProvider(ready_order=["c", "a", "b"])  # completes c, a, b

    n = ung.ungate_per_result(prov, tasks, workers, "ns",
                              poll_interval=0, _sleep=lambda _: prov.tick())
    assert n == 3
    # one distinct job id per worker; each worker gate-removed exactly once
    assert set(_annotations(calls).values()) == {"job-a", "job-b", "job-c"}
    assert sorted(_gate_removals(calls)) == ["w-x", "w-y", "w-z"]
    # first result out (c) wakes the first free worker (w-x), etc. -- assignment
    # follows completion order, NOT task or worker identity.
    assert _annotations(calls)["w-x"] == "job-c"
    assert _annotations(calls)["w-y"] == "job-a"
    assert _annotations(calls)["w-z"] == "job-b"


def test_surplus_tasks_left_for_producer(monkeypatch):
    # N tasks, N-1 workers (the producer keeps one result): only N-1 released.
    calls = _record_kubectl(monkeypatch)
    tasks = [FakeTask("a"), FakeTask("b"), FakeTask("c")]
    workers = ["w-x", "w-y"]
    prov = FakeProvider(ready_order=["a", "b", "c"])

    n = ung.ungate_per_result(prov, tasks, workers, "ns",
                              poll_interval=0, _sleep=lambda _: prov.tick())
    assert n == 2
    assert sorted(_gate_removals(calls)) == ["w-x", "w-y"]


def test_never_completing_task_does_not_strand_workers(monkeypatch):
    _record_kubectl(monkeypatch)
    tasks = [FakeTask("a"), FakeTask("b")]
    workers = ["w-x", "w-y"]
    prov = FakeProvider(ready_order=["a"])  # b never completes

    # one worker released; loop exits at timeout without hanging
    n = ung.ungate_per_result(prov, tasks, workers, "ns",
                              poll_interval=0, timeout=0.05, _sleep=lambda _: None)
    assert n == 1


def test_works_without_any_completion_index(monkeypatch):
    # worker names carry no index at all (e.g. a ReplicaSet's random suffixes).
    calls = _record_kubectl(monkeypatch)
    tasks = [FakeTask("t1"), FakeTask("t2")]
    workers = ["batch-rs-7f9-abcde", "batch-rs-7f9-zzzzz"]
    prov = FakeProvider(ready_order=["t2", "t1"])

    n = ung.ungate_per_result(prov, tasks, workers, "ns",
                              poll_interval=0, _sleep=lambda _: prov.tick())
    assert n == 2
    assert sorted(_gate_removals(calls)) == sorted(workers)


def test_shared_broadcast_still_uses_one_id(monkeypatch):
    """Regression: shared mode (ungate_pods) broadcasts ONE id to all pods."""
    calls = _record_kubectl(monkeypatch)
    n = ung.ungate_pods(["a", "b", "c"], "shared-job", "ns")
    assert n == 3
    assert _annotations(calls) == {"a": "shared-job", "b": "shared-job", "c": "shared-job"}
