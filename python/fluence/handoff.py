"""
The handoff between the interceptor and the sidecar, a JSON file on a volume
the webhook mounts into both. Braket can find a task again by a tag the
interceptor stamped. QRMI cannot, so the interceptor writes the job id, and
the session when it acquired one, to $FLUENCE_HANDOFF_DIR/task.json and the
sidecar reads it. Writes are atomic and every failure is soft, since the
interceptor runs inside the user application.
"""

from __future__ import annotations

import json
import os
import tempfile
import time

# the default matches the mount path the webhook uses
HANDOFF_DIR_ENV = "FLUENCE_HANDOFF_DIR"
DEFAULT_HANDOFF_DIR = "/opt/fluence-handoff"
HANDOFF_FILENAME = "task.json"


def handoff_path(path: "str | None" = None) -> str:
    if path:
        return path
    d = os.environ.get(HANDOFF_DIR_ENV, DEFAULT_HANDOFF_DIR)
    return os.path.join(d, HANDOFF_FILENAME)


def publish(
    job_id, session_id=None, backend=None, pod_uid=None, tier=None, path=None
) -> bool:
    """Write the record. Returns False on any failure and never raises."""
    record = {"job_id": job_id}
    if session_id is not None:
        record["session_id"] = session_id
    if backend is not None:
        record["backend"] = backend
    if pod_uid is not None:
        record["pod_uid"] = pod_uid
    if tier is not None:
        record["tier"] = tier
    try:
        dest = handoff_path(path)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        fd, tmp = tempfile.mkstemp(
            dir=os.path.dirname(dest), prefix=".task.", suffix=".tmp"
        )
        with os.fdopen(fd, "w") as f:
            json.dump(record, f)
        os.replace(tmp, dest)
        return True
    except Exception:
        return False


def read(path=None) -> "dict | None":
    """The record, or None when it is not there yet or cannot be parsed."""
    try:
        with open(handoff_path(path)) as f:
            record = json.load(f)
        if isinstance(record, dict) and record.get("job_id"):
            return record
    except Exception:
        pass
    return None


def wait(timeout, interval=5, path=None, log=None) -> "dict | None":
    """Poll for the record until it appears or timeout seconds pass."""
    deadline = time.time() + timeout
    announced = False
    while time.time() < deadline:
        record = read(path)
        if record is not None:
            return record
        if log and not announced:
            log(f"waiting for the scout to publish its job id at {handoff_path(path)}")
            announced = True
        time.sleep(interval)
    return None


# the sidecar writes this once the workers are done and the scout's release waits for it
DONE_FILENAME = "workers-done"


def done_path(path=None) -> str:
    if path:
        return path
    d = os.environ.get(HANDOFF_DIR_ENV, DEFAULT_HANDOFF_DIR)
    return os.path.join(d, DONE_FILENAME)


def mark_workers_done(path=None) -> bool:
    """Write the marker. Never raises."""
    try:
        dest = done_path(path)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        fd, tmp = tempfile.mkstemp(
            dir=os.path.dirname(dest), prefix=".done.", suffix=".tmp"
        )
        with os.fdopen(fd, "w") as f:
            json.dump({"done_at": time.time()}, f)
        os.replace(tmp, dest)
        return True
    except Exception:
        return False


def workers_done(path=None) -> bool:
    return os.path.exists(done_path(path))


def wait_workers_done(timeout, interval=5, path=None, log=None) -> bool:
    """Wait for the marker. A timeout of zero or less waits without limit."""
    deadline = time.time() + timeout if timeout and timeout > 0 else None
    announced = False
    while True:
        if workers_done(path):
            return True
        if deadline is not None and time.time() >= deadline:
            return False
        if log and not announced:
            log(f"waiting for the worker group to finish, marker {done_path(path)}")
            announced = True
        time.sleep(interval)
