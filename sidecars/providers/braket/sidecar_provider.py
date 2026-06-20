"""
sidecars/providers/braket/sidecar_provider.py — AWS Braket provider.

Implements the Provider interface for AWS Braket. Preserves the working task
discovery and queue-position logic; vendor identifiers (the task ARN) stay
behind the opaque Task and are surfaced generically via job_id().

The matching interceptor snippet is providers/braket/interceptor.py — it stamps
the TAG_KEY tag on AwsDevice.run() submissions; find_my_task searches for it.
"""

from __future__ import annotations

import os
import time

from provider import Provider, Task, TAG_KEY, log


class BraketTask(Task):
    def __init__(self, arn: str):
        self.arn = arn


def _region_from_arn(arn: str) -> str:
    # arn:aws:braket:<region>::device/... or .../quantum-task/...
    parts = arn.split(":")
    region = parts[3] if len(parts) > 3 and parts[3] else ""
    return region or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")


class BraketProvider(Provider):
    name = "braket"

    def matches(self, vendor: str, backend: str) -> bool:
        v = (vendor or "").lower()
        b = (backend or "").lower()
        if v == "braket":
            return True
        # Braket device ARNs and Braket backend names contain "braket".
        return "braket" in b or b.startswith("arn:aws:braket")

    def _client(self, backend: str):
        import boto3
        region = _region_from_arn(backend) if backend.startswith("arn:") \
            else os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        return boto3.client("braket", region_name=region)

    def find_my_task(self, pod_uid, backend, timeout):
        client = self._client(backend)
        log(f"searching for Braket task tagged {TAG_KEY}={pod_uid}")
        deadline = time.time() + timeout
        # backend may be a device ARN; if so, constrain the search to it.
        device_arn = backend if backend.startswith("arn:aws:braket") else None

        while time.time() < deadline:
            try:
                filters = [{
                    "name": f"tags:{TAG_KEY}",
                    "operator": "EQUAL",
                    "values": [pod_uid],
                }]
                if device_arn:
                    filters.append({
                        "name": "deviceArn",
                        "operator": "EQUAL",
                        "values": [device_arn],
                    })
                resp = client.search_quantum_tasks(filters=filters, maxResults=10)
                tasks = resp.get("quantumTasks", [])
                if tasks:
                    tasks.sort(key=lambda t: t.get("createdAt", ""), reverse=True)
                    arn = tasks[0]["quantumTaskArn"]
                    log(f"found task by tag: {arn}")
                    return BraketTask(arn)
            except Exception as e:
                log(f"search error (will retry): {e}")
            time.sleep(10)

        log("Braket task discovery timed out")
        return None

    def _aws_task(self, task: BraketTask):
        import asyncio
        asyncio.set_event_loop(asyncio.new_event_loop())
        from braket.aws import AwsQuantumTask
        return AwsQuantumTask(arn=task.arn)

    def is_ready_to_ungate(self, task: BraketTask) -> bool:
        t = self._aws_task(task)
        state = t.state()
        if state in ("RUNNING", "COMPLETED", "FAILED", "CANCELLED"):
            return True
        try:
            pos = t.queue_position().queue_position
            return str(pos) == "1"
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
