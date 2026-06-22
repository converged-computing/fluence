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

    def matches(self, attrs: "dict[str, str]") -> bool:
        # Authoritative routing key: qrmi_type. Every Braket-accessed device —
        # gate simulators (sv1/tn1/dm1), gate QPUs (rigetti/iqm/ionq), and AHS
        # (aquila, ahs_local) — carries a "braket-*" qrmi_type, regardless of
        # which company (amazon, quera, rigetti, iqm, ...) made the hardware.
        qrmi = (attrs.get("qrmi_type") or "").lower()
        if qrmi.startswith("braket"):
            return True
        # Fallbacks for graphs that don't set qrmi_type: vendor or backend/arn.
        vendor  = (attrs.get("vendor") or "").lower()
        backend = (attrs.get("backend") or "").lower()
        arn     = (attrs.get("arn") or "").lower()
        if vendor in ("braket", "amazon", "aws"):
            return True
        return "braket" in backend or arn.startswith("arn:aws:braket")

    def _client(self, backend: str):
        import boto3
        region = (_region_from_arn(backend) if backend.startswith("arn:")
                  else os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
        return boto3.client("braket", region_name=region)

    def find_my_task(self, pod_uid, backend, timeout):
        client = self._client(backend)
        log(f"[braket] searching for task tagged {TAG_KEY}={pod_uid}")
        deadline = time.time() + timeout
        # SearchQuantumTasks only accepts filter names quantumTaskArn, deviceArn,
        # jobArn, status, createdAt — there is NO tags filter. An EMPTY filter
        # list is valid and returns all tasks, so we filter only by deviceArn when
        # the backend is an ARN (avoids the fragile createdAt timestamp format),
        # then match our tag CLIENT-SIDE from each task summary's `tags` map. The
        # newest matching task wins (createdAt is returned on each summary).
        device_arn = backend if backend.startswith("arn:aws:braket") else None
        filters = []
        if device_arn:
            filters = [{"name": "deviceArn", "operator": "EQUAL",
                        "values": [device_arn]}]
        while time.time() < deadline:
            try:
                # Call search_quantum_tasks DIRECTLY with manual nextToken paging.
                # The boto3 paginator rejects an empty filters=[] (it serializes
                # the required `filters` parameter differently); the direct call
                # accepts it and returns all tasks. We match our pod-uid tag
                # CLIENT-SIDE from each summary's `tags` map (there is no tags
                # filter on the API). Newest matching task wins.
                match = None
                next_token = None
                while True:
                    kwargs = {"filters": filters, "maxResults": 100}
                    if next_token:
                        kwargs["nextToken"] = next_token
                    resp = client.search_quantum_tasks(**kwargs)
                    for t in resp.get("quantumTasks", []):
                        if (t.get("tags") or {}).get(TAG_KEY) == pod_uid:
                            if match is None or str(t.get("createdAt", "")) > str(match.get("createdAt", "")):
                                match = t
                    next_token = resp.get("nextToken")
                    if not next_token:
                        break
                if match:
                    arn = match["quantumTaskArn"]
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
