"""
The QRMI provider. QRMI gives one API over several vendors, so this provider
has no vendor branches. The vendor comes from the resource attributes the
webhook injects, FLUXION_QRMI_TYPE picks the QRMI resource type and
FLUXION_BACKEND names the backend.

QRMI has no way to search for a task by tag, so the interceptor writes the job
id and the session to the handoff file and the sidecar reads it from there.
"""

from __future__ import annotations

import os
import time

from fluence.providers.base import Provider, Task, log, register
from fluence import handoff

# qrmi_type values whose ResourceType name cannot be derived from them
_RESOURCE_TYPE_ALIASES = {
    "qiskit-runtime-service": "IBMQiskitRuntimeService",
    "direct-access": "IBMQuantumSystem",  # legacy name for ibm-quantum-system
    "ibm-direct-access": "IBMQuantumSystem",
}

# the infix QRMI puts in its variable names for each IBM type, as in
# <backend>_QRMI_IBM_QRS_IAM_APIKEY
_ENV_INFIX = {
    "IBMQiskitRuntimeService": "_QRMI_IBM_QRS",
    "IBMQuantumSystem": "_QRMI_IBM_QS",
}

# what the infix variables default to, or are derived from, when only the
# IBM_CLOUD_TOKEN and IBM_CLOUD_CRN a workload Secret provides are present.
# The sampler derives the same set for the scout.
_IBM_DEFAULTS = {
    "_ENDPOINT": "https://quantum.cloud.ibm.com/api/v1",
    "_IAM_ENDPOINT": "https://iam.cloud.ibm.com",
}

# kept upper case when deriving a ResourceType name
_ACRONYMS = {"ibm", "iqm", "qrmi", "id", "api", "hpc"}


class QRMITask(Task):
    def __init__(
        self,
        job_id: str,
        backend: str,
        resource_type: str,
        session_id: "str | None" = None,
        tier: "str | None" = None,
    ):
        self.job_id = job_id
        self.backend = backend
        self.resource_type = resource_type
        self.session_id = session_id
        self.tier = tier
        self._resource = None


def _pascal(qrmi_type: str, acronyms=True) -> str:
    parts = [p for p in qrmi_type.replace("_", "-").split("-") if p]
    out = []
    for p in parts:
        if acronyms and p.lower() in _ACRONYMS:
            out.append(p.upper())
        else:
            out.append(p[:1].upper() + p[1:])
    return "".join(out)


def _resource_type_enum(qrmi_type: str):
    """The QRMI ResourceType for a qrmi_type string. An alias first, then the
    name derived from the string. Raises when nothing matches. A guessed type
    reads the wrong credentials and asks the wrong service about the task, so
    it is never better than stopping."""
    from qrmi import ResourceType

    q = (qrmi_type or "").lower()
    candidates = []
    if q in _RESOURCE_TYPE_ALIASES:
        candidates.append(_RESOURCE_TYPE_ALIASES[q])
    candidates.append(_pascal(q, acronyms=True))
    candidates.append(_pascal(q, acronyms=False))
    for name in candidates:
        rt = getattr(ResourceType, name, None)
        if rt is not None:
            return rt
    members = [k for k in vars(ResourceType) if not k.startswith("_")]
    raise ValueError(
        f"no QRMI ResourceType matches {qrmi_type!r}, tried {candidates}, "
        f"QRMI has {members}"
    )


def _derive_ibm_env(backend: str, rtype) -> None:
    """Fill in <backend><infix>_* from IBM_CLOUD_TOKEN and IBM_CLOUD_CRN,
    never overwriting a variable that is already set. The webhook copies the
    workload's Secret env onto the sidecar, and that Secret holds the plain
    token and CRN, not QRMI's names."""
    name = rtype if isinstance(rtype, str) else getattr(rtype, "name", str(rtype))
    infix = _ENV_INFIX.get(name.rsplit(".", 1)[-1])
    if not infix:
        return
    values = dict(_IBM_DEFAULTS)
    if os.environ.get("IBM_CLOUD_TOKEN"):
        values["_IAM_APIKEY"] = os.environ["IBM_CLOUD_TOKEN"]
    if os.environ.get("IBM_CLOUD_CRN"):
        values["_SERVICE_CRN"] = os.environ["IBM_CLOUD_CRN"]
    for suffix, value in values.items():
        os.environ.setdefault(backend + infix + suffix, value)


def _status_name(status) -> str:
    """The upper case name of a QRMI TaskStatus, whatever type it arrives as."""
    for attr in ("name", "value"):
        v = getattr(status, attr, None)
        if isinstance(v, str) and v:
            return v.upper()
    s = str(status)
    return s.rsplit(".", 1)[-1].upper()


# the task is running on the device, or ran to the end
_READY_STATES = {"RUNNING", "COMPLETED", "COMPLETE", "DONE"}
# the task ended with nothing to use
_FAILED_STATES = {"FAILED", "CANCELLED", "CANCELED"}


class QRMIProvider(Provider):
    name = "qrmi"

    # the interceptor half runs in the user container

    def install_interceptor(self, pod_uid: str) -> bool:
        try:
            import qrmi

            QuantumResource = qrmi.QuantumResource
        except Exception:
            return False

        backend_hint = os.environ.get("FLUXION_BACKEND", "")
        published = {"session_id": None, "tier": None, "job_id": None}

        # acquire records the session and tier and otherwise behaves as before
        if hasattr(QuantumResource, "acquire") and not getattr(
            QuantumResource.acquire, "_fluence_patched", False
        ):
            original_acquire = QuantumResource.acquire

            def patched_acquire(self, *args, **kwargs):
                try:
                    token = original_acquire(self, *args, **kwargs)
                except Exception:
                    published["tier"] = "free"
                    log("[qrmi] acquire not permitted, continuing submit only")
                    raise
                published["session_id"] = str(token) if token is not None else None
                published["tier"] = "premium"
                log("[qrmi] acquired a session")
                return token

            patched_acquire._fluence_patched = True
            QuantumResource.acquire = patched_acquire

        # task_start publishes the job id for the sidecar
        if hasattr(QuantumResource, "task_start") and not getattr(
            QuantumResource.task_start, "_fluence_patched", False
        ):
            original_task_start = QuantumResource.task_start

            def patched_task_start(self, *args, **kwargs):
                job_id = original_task_start(self, *args, **kwargs)
                published["job_id"] = str(job_id)
                try:
                    handoff.publish(
                        job_id=str(job_id),
                        session_id=published["session_id"],
                        backend=backend_hint or _resource_name(self),
                        pod_uid=pod_uid,
                        tier=published["tier"],
                    )
                    log(
                        f"[qrmi] published job id {job_id} to handoff "
                        f"(tier={published['tier']})"
                    )
                except Exception as e:
                    log(f"[qrmi] WARNING: could not publish handoff: {e}")
                return job_id

            patched_task_start._fluence_patched = True
            QuantumResource.task_start = patched_task_start

        # a scout keeps its session until the sidecar says the workers are done
        if hasattr(QuantumResource, "release") and not getattr(
            QuantumResource.release, "_fluence_patched", False
        ):
            original_release = QuantumResource.release

            def patched_release(self, *args, **kwargs):
                # only a scout that published a task has workers using its
                # session. A scout that failed before submitting releases at
                # once, or the session would stay open forever
                if (
                    os.environ.get("FLUENCE_COORDINATION_ROLE") == "scout"
                    and published["job_id"]
                ):
                    timeout = int(
                        os.environ.get("FLUENCE_SESSION_HOLD_TIMEOUT", "3600") or 3600
                    )
                    log(
                        f"[qrmi] holding the session until the worker group is done, {timeout}s at most"
                    )
                    if handoff.wait_workers_done(timeout, log=log):
                        log("[qrmi] worker group done, releasing the session")
                    else:
                        log(
                            f"[qrmi] WARNING: hold timed out after {timeout}s, releasing anyway"
                        )
                return original_release(self, *args, **kwargs)

            patched_release._fluence_patched = True
            QuantumResource.release = patched_release

        return True

    # the sidecar half runs in the sidecar container

    def matches(self, attrs: "dict[str, str]") -> bool:
        # an explicit transport attribute wins
        transport = (attrs.get("transport") or "").lower()
        if transport:
            return transport == "qrmi"
        # otherwise any qrmi_type that is not braket is ours
        qrmi_type = (attrs.get("qrmi_type") or "").lower()
        if qrmi_type:
            return not qrmi_type.startswith("braket")
        return False

    def find_my_task(self, pod_uid, backend, timeout):
        log(f"[qrmi] waiting for the scout job id in the handoff (timeout {timeout}s)")
        record = handoff.wait(timeout, log=log)
        if record is None:
            log("[qrmi] no handoff appeared, task not discovered")
            return None
        if record.get("pod_uid") and pod_uid and record["pod_uid"] != pod_uid:
            log(
                f"[qrmi] WARNING: handoff pod_uid {record['pod_uid']!r} is not {pod_uid!r}, using it anyway"
            )
        qrmi_type = os.environ.get("FLUXION_QRMI_TYPE", "")
        task = QRMITask(
            job_id=record["job_id"],
            backend=record.get("backend") or backend,
            resource_type=qrmi_type,
            session_id=record.get("session_id"),
            tier=record.get("tier"),
        )
        log(
            f"[qrmi] discovered job {task.job_id} on {task.backend} "
            f"(type={qrmi_type!r}, tier={task.tier}, session={task.session_id})"
        )
        return task

    def _qrmi_resource(self, task: QRMITask):
        """A QRMI handle for status polling. Reading status needs no acquire."""
        if task._resource is None:
            import qrmi

            rt = _resource_type_enum(task.resource_type)
            _derive_ibm_env(
                task.backend,
                _RESOURCE_TYPE_ALIASES.get((task.resource_type or "").lower(), rt),
            )
            task._resource = qrmi.QuantumResource(task.backend, rt)
        return task._resource

    def _status(self, task: QRMITask) -> str:
        # Errors are raised, not swallowed. The sidecar's poll loop counts
        # them and gives up after FLUENCE_POLL_MAX_ERRORS, which tells the
        # scout to release its session. Swallowed, a status call that can
        # never work polled forever while the scout held the session.
        return _status_name(self._qrmi_resource(task).task_status(task.job_id))

    def is_ready_to_ungate(self, task: QRMITask, position=None) -> bool:
        # QRMI reports no queue position, so position is accepted to match the
        # interface and the sidecar's call, and ignored. Ready means the task
        # is running on the device or has completed. Failed and cancelled are
        # not ready, task_failed reports them.
        return self._status(task) in _READY_STATES

    def task_failed(self, task: QRMITask) -> bool:
        return self._status(task) in _FAILED_STATES

    def queue_position(self, task: QRMITask):
        # QRMI has no queue position, so report 1 once the task runs and None before
        try:
            name = self._status(task)
        except Exception:
            return None
        return 1 if name in _READY_STATES else None

    def job_id(self, task: QRMITask) -> str:
        return task.job_id

    def session_id(self, task: QRMITask):
        return task.session_id


def _resource_name(resource) -> str:
    """The backend name from a QRMI handle, when FLUXION_BACKEND is not set."""
    for attr in ("name", "resource_id", "backend", "id"):
        v = getattr(resource, attr, None)
        if isinstance(v, str) and v:
            return v
    return ""


PROVIDER = QRMIProvider()
register(PROVIDER)
