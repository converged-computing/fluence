"""
fluence.providers.braket — AWS Braket provider.

Holds both halves of the Braket coordination mechanism:
  - install_interceptor: patches AwsDevice.run() to stamp the pod-uid tag
    (runs in the user container; fail-soft if amazon-braket-sdk is absent).
  - sidecar methods: discover the tagged task, poll queue position, yield the
    task ARN as the (vendor-neutral-typed) job id.

Self-registers via register(PROVIDER) at import. Importing this module never
requires the braket SDK; SDK imports are lazy, inside the methods.
"""

from __future__ import annotations

import os
import time

from fluence.providers.base import Provider, Task, TAG_KEY, log, register


class BraketTask(Task):
    def __init__(self, arn: str):
        self.arn = arn


def _region_from_arn(arn: str) -> str:
    parts = arn.split(":")
    region = parts[3] if len(parts) > 3 and parts[3] else ""
    return region or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")


class BraketProvider(Provider):
    name = "braket"

    # ── interceptor half ───────────────────────────────────────────────────────

    def install_interceptor(self, pod_uid: str) -> bool:
        try:
            from braket.aws import AwsDevice
        except ImportError:
            return False  # braket SDK not in this container — fail-soft

        original_run = AwsDevice.run

        def patched_run(self, task_specification, *args, **kwargs):
            if pod_uid:
                tags = kwargs.get("tags", {})
                tags[TAG_KEY] = pod_uid
                kwargs["tags"] = tags
            return original_run(self, task_specification, *args, **kwargs)

        AwsDevice.run = patched_run
        return True

    # ── sidecar half ───────────────────────────────────────────────────────────

    def matches(self, vendor: str, backend: str) -> bool:
        v, b = (vendor or "").lower(), (backend or "").lower()
        if v == "braket":
            return True
        return "braket" in b or b.startswith("arn:aws:braket")

    def _client(self, backend: str):
        import boto3
        region = (_region_from_arn(backend) if backend.startswith("arn:")
                  else os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
        return boto3.client("braket", region_name=region)

    def find_my_task(self, pod_uid, backend, timeout):
        client = self._client(backend)
        log(f"[braket] searching for task tagged {TAG_KEY}={pod_uid}")
        deadline = time.time() + timeout
        device_arn = backend if backend.startswith("arn:aws:braket") else None
        while time.time() < deadline:
            try:
                filters = [{"name": f"tags:{TAG_KEY}", "operator": "EQUAL",
                            "values": [pod_uid]}]
                if device_arn:
                    filters.append({"name": "deviceArn", "operator": "EQUAL",
                                    "values": [device_arn]})
                resp = client.search_quantum_tasks(filters=filters, maxResults=10)
                tasks = resp.get("quantumTasks", [])
                if tasks:
                    tasks.sort(key=lambda t: t.get("createdAt", ""), reverse=True)
                    arn = tasks[0]["quantumTaskArn"]
                    log(f"[braket] found task by tag: {arn}")
                    return BraketTask(arn)
            except Exception as e:
                log(f"[braket] search error (will retry): {e}")
            time.sleep(10)
        log("[braket] task discovery timed out")
        return None

    def _aws_task(self, task: BraketTask):
        import asyncio
        asyncio.set_event_loop(asyncio.new_event_loop())
        from braket.aws import AwsQuantumTask
        return AwsQuantumTask(arn=task.arn)

    def is_ready_to_ungate(self, task: BraketTask) -> bool:
        t = self._aws_task(task)
        if t.state() in ("RUNNING", "COMPLETED", "FAILED", "CANCELLED"):
            return True
        try:
            return str(t.queue_position().queue_position) == "1"
        except Exception:
            return False

    def queue_position(self, task: BraketTask):
        try:
            pos = self._aws_task(task).queue_position().queue_position
            return int(pos) if pos is not None and str(pos).isdigit() else None
        except Exception:
            return None

    def job_id(self, task: BraketTask) -> str:
        return task.arn


PROVIDER = BraketProvider()
register(PROVIDER)
