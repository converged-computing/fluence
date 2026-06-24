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
    # Prefer the region the scheduler chose for this backend (FLUXION_REGION,
    # injected from the resource graph's `region` attribute) over the pod's
    # generic AWS_DEFAULT_REGION. A QPU like IQM Emerald lives in eu-north-1,
    # while the pod's AWS_DEFAULT_REGION is often us-east-1 — searching the wrong
    # region means the task is never found and workers never ungate.
    return (region
            or os.environ.get("FLUXION_REGION")
            or os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))


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

    def _region_client(self, region: str):
        """Construct a Braket client for an explicit region. Factored out so it
        can be stubbed in tests (find_my_task builds one client per candidate
        region through this method rather than calling boto3 directly)."""
        import boto3
        return boto3.client("braket", region_name=region)

    def _client(self, backend: str):
        region = (_region_from_arn(backend) if backend.startswith("arn:")
                  else (os.environ.get("FLUXION_REGION")
                        or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")))
        log(f"[braket] using region {region} for task search "
            f"(backend={backend!r} FLUXION_REGION={os.environ.get('FLUXION_REGION')!r})")
        return self._region_client(region)

    def _candidate_regions(self, backend: str):
        """Regions to search for the task, best guess first. If the backend is an
        ARN or FLUXION_REGION is set, try that first; then fall back to the
        regions Braket operates in, because the QPU's region (e.g. eu-north-1 for
        IQM) frequently differs from the pod's AWS_DEFAULT_REGION (us-east-1) and
        a task only appears in SearchQuantumTasks in the region it was created.

        Set FLUENCE_BRAKET_REGIONS (comma-separated) to restrict the search to
        regions the account can actually access — useful when a service control
        policy explicitly denies braket:SearchQuantumTasks in some regions (those
        raise AccessDeniedException and just add noise/latency)."""
        override = os.environ.get("FLUENCE_BRAKET_REGIONS", "").strip()
        if override:
            all_regions = [r.strip() for r in override.split(",") if r.strip()]
        else:
            # The fixed set of regions Amazon Braket operates in.
            all_regions = ["us-east-1", "us-west-1", "us-west-2", "eu-west-2", "eu-north-1"]
        preferred = []
        if backend.startswith("arn:aws:braket"):
            r = _region_from_arn(backend)
            if r:
                preferred.append(r)
        for r in (os.environ.get("FLUXION_REGION"),
                  os.environ.get("AWS_DEFAULT_REGION")):
            if r and r not in preferred:
                preferred.append(r)
        # preferred first, then any remaining regions (keep only ones in the
        # allowed set when an override is given, so a denied region is never hit)
        if override:
            preferred = [r for r in preferred if r in all_regions]
        return preferred + [r for r in all_regions if r not in preferred]

    def find_my_task(self, pod_uid, backend, timeout):
        regions = self._candidate_regions(backend)
        log(f"[braket] searching for task tagged {TAG_KEY}={pod_uid} "
            f"across regions {regions}")
        deadline = time.time() + timeout
        # SearchQuantumTasks has NO tags filter and is REGION-SCOPED: a task only
        # shows up in the region it was created in. We don't reliably know that
        # region up front (the backend may be a name, and the pod's default region
        # often differs from the QPU's), so we search each candidate region and
        # match our pod-uid tag CLIENT-SIDE. Newest matching task wins.
        device_arn = backend if backend.startswith("arn:aws:braket") else None
        filters = []
        if device_arn:
            filters = [{"name": "deviceArn", "operator": "EQUAL",
                        "values": [device_arn]}]
        clients = {}
        denied = set()  # regions that returned AccessDenied — skip them after the first hit
        while time.time() < deadline:
            for region in regions:
                if region in denied:
                    continue
                try:
                    client = clients.get(region)
                    if client is None:
                        client = clients[region] = self._region_client(region)
                    match = None
                    next_token = None
                    seen = 0
                    seen_tagged = 0
                    while True:
                        kwargs = {"filters": filters, "maxResults": 100}
                        if next_token:
                            kwargs["nextToken"] = next_token
                        resp = client.search_quantum_tasks(**kwargs)
                        for t in resp.get("quantumTasks", []):
                            seen += 1
                            tags = t.get("tags") or {}
                            if TAG_KEY in tags:
                                seen_tagged += 1
                            if tags.get(TAG_KEY) == pod_uid:
                                if match is None or str(t.get("createdAt", "")) > str(match.get("createdAt", "")):
                                    match = t
                        next_token = resp.get("nextToken")
                        if not next_token:
                            break
                    if match:
                        arn = match["quantumTaskArn"]
                        log(f"[braket] found task by tag in {region}: {arn}")
                        return BraketTask(arn)
                    # Diagnostic: how many tasks did this region have, and how many
                    # carried OUR tag key at all? Distinguishes "task not here yet"
                    # from "task here but tag not landing".
                    if seen:
                        log(f"[braket] {region}: {seen} task(s) seen, "
                            f"{seen_tagged} with a {TAG_KEY} tag, none matching {pod_uid}")
                except Exception as e:
                    msg = str(e)
                    if "AccessDenied" in msg or "not authorized" in msg or "explicit deny" in msg:
                        # This account can't search Braket in this region (e.g. a
                        # service control policy denies it). Skip it from now on
                        # instead of erroring every poll cycle.
                        denied.add(region)
                        log(f"[braket] region {region} not accessible "
                            f"(access denied) — skipping it for the rest of this search")
                    else:
                        log(f"[braket] search error in {region} (will retry): {e}")
            if len(denied) == len(regions):
                log("[braket] ERROR: every candidate region was access-denied — "
                    "set FLUENCE_BRAKET_REGIONS to a region this account can search")
                return None
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
