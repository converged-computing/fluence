"""
Tests for the QRMI provider. Like test_fluence.py, none of these require the
real qrmi SDK — they exercise registration, routing, the resource-type mapping,
the handoff channel, fail-soft interceptor behavior, the status->ready mapping,
and the free/paid (acquire) session capture using small fakes.

The provider is vendor-agnostic: the tests deliberately cover more than one QRMI
vendor (IBM's qiskit-runtime-service AND Pasqal's pasqal-cloud) to prove nothing
is IBM-specific.

Run: PYTHONPATH=python python3 python/tests/test_qrmi.py
"""

import os
import sys
import types
import tempfile

# ── registration & routing ───────────────────────────────────────────────────


def test_qrmi_registered():
    from fluence.providers import all_providers

    assert "qrmi" in [p.name for p in all_providers()]


def test_resolve_qrmi_ibm_runtime_service():
    # The qrmi_type the Fluence resource config ships for IBM.
    from fluence.providers import resolve

    assert resolve({"qrmi_type": "qiskit-runtime-service"}).name == "qrmi"


def test_resolve_qrmi_ibm_quantum_system_and_legacy():
    from fluence.providers import resolve

    assert resolve({"qrmi_type": "ibm-quantum-system"}).name == "qrmi"
    assert resolve({"qrmi_type": "direct-access"}).name == "qrmi"  # legacy alias


def test_resolve_qrmi_pasqal_proves_vendor_agnostic():
    # A DIFFERENT vendor (Pasqal) must route to the same provider with no code
    # change — routing is data-driven from qrmi_type.
    from fluence.providers import resolve

    assert resolve({"qrmi_type": "pasqal-cloud"}).name == "qrmi"


def test_resolve_qrmi_by_explicit_transport_marker():
    from fluence.providers import resolve

    # transport=qrmi is authoritative even without a qrmi_type.
    assert resolve({"transport": "qrmi"}).name == "qrmi"
    # transport=braket must NOT route to qrmi.
    assert (
        resolve({"transport": "braket", "qrmi_type": "pasqal-cloud"}) is None
        or resolve({"transport": "braket", "qrmi_type": "pasqal-cloud"}).name != "qrmi"
    )


def test_qrmi_and_braket_do_not_collide():
    from fluence.providers import resolve

    # Braket-reached devices carry a braket-* qrmi_type and must go to braket.
    assert resolve({"qrmi_type": "braket-gate", "vendor": "amazon"}).name == "braket"
    assert resolve({"qrmi_type": "braket-ahs", "vendor": "quera"}).name == "braket"
    # QRMI-reached devices must go to qrmi.
    assert resolve({"qrmi_type": "qiskit-runtime-service"}).name == "qrmi"
    assert resolve({"qrmi_type": "pasqal-cloud"}).name == "qrmi"


def test_no_routing_signal_returns_none():
    from fluence.providers import resolve

    assert resolve({"vendor": "someone"}) is None


# ── resource-type mapping (data-driven, vendor-agnostic) ──────────────────────


def test_resource_type_derivation_covers_multiple_vendors():
    # _resource_type_enum derives the ResourceType member name from the qrmi_type
    # string (with acronym awareness + a small alias table), so a new vendor is
    # just a new qrmi_type in config. Verify with a fake ResourceType enum.
    from fluence.providers import qrmi as qmod

    fake = types.SimpleNamespace(
        QiskitRuntimeService="QiskitRuntimeService",
        IBMQuantumSystem="IBMQuantumSystem",
        PasqalCloud="PasqalCloud",
    )
    fake_mod = types.ModuleType("qrmi")
    fake_mod.ResourceType = fake
    sys.modules["qrmi"] = fake_mod
    try:
        assert (
            qmod._resource_type_enum("qiskit-runtime-service") == "QiskitRuntimeService"
        )
        assert qmod._resource_type_enum("ibm-quantum-system") == "IBMQuantumSystem"
        assert qmod._resource_type_enum("direct-access") == "IBMQuantumSystem"  # alias
        assert qmod._resource_type_enum("pasqal-cloud") == "PasqalCloud"
    finally:
        sys.modules.pop("qrmi", None)


def test_pascal_transform_acronym_aware():
    from fluence.providers.qrmi import _pascal

    assert _pascal("qiskit-runtime-service") == "QiskitRuntimeService"
    assert _pascal("ibm-quantum-system") == "IBMQuantumSystem"
    assert _pascal("pasqal-cloud") == "PasqalCloud"


# ── handoff channel ───────────────────────────────────────────────────────────


def test_handoff_publish_and_read_roundtrip():
    from fluence import handoff

    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "task.json")
        assert (
            handoff.publish(
                "job-123",
                session_id="sess-abc",
                backend="ibm_fez",
                pod_uid="pod-1",
                tier="premium",
                path=path,
            )
            is True
        )
        rec = handoff.read(path)
        assert rec["job_id"] == "job-123"
        assert rec["session_id"] == "sess-abc"
        assert rec["tier"] == "premium"


def test_handoff_read_missing_returns_none():
    from fluence import handoff

    with tempfile.TemporaryDirectory() as d:
        assert handoff.read(os.path.join(d, "nope.json")) is None


def test_handoff_wait_times_out_without_file():
    from fluence import handoff

    with tempfile.TemporaryDirectory() as d:
        assert handoff.wait(1, interval=1, path=os.path.join(d, "task.json")) is None


def test_handoff_write_is_atomic_no_partial_file():
    from fluence import handoff

    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "task.json")
        handoff.publish("j", path=path)
        leftovers = [f for f in os.listdir(d) if f != "task.json"]
        assert leftovers == [], leftovers


# ── interceptor: fail-soft + free/paid session capture ────────────────────────


def test_qrmi_install_interceptor_failsoft_without_qrmi():
    from fluence.providers.qrmi import PROVIDER

    assert "qrmi" not in sys.modules
    assert PROVIDER.install_interceptor("uid") is False


def _install_fake_qrmi(acquire_impl, task_start_returns="job-xyz"):
    qrmi = types.ModuleType("qrmi")

    class QuantumResource:
        def __init__(self, *a, **k):
            pass

        def acquire(self, *a, **k):
            return acquire_impl()

        def task_start(self, *a, **k):
            return task_start_returns

    qrmi.QuantumResource = QuantumResource
    qrmi.ResourceType = types.SimpleNamespace(
        QiskitRuntimeService="QiskitRuntimeService",
        PasqalCloud="PasqalCloud",
    )
    qrmi.Payload = types.SimpleNamespace()
    qrmi.TaskStatus = types.SimpleNamespace()
    sys.modules["qrmi"] = qrmi
    return QuantumResource


def test_paid_tier_captures_session_and_publishes_job_id():
    from fluence.providers.qrmi import QRMIProvider
    from fluence import handoff

    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "task.json")
        os.environ["FLUENCE_HANDOFF_DIR"] = d
        os.environ["FLUXION_BACKEND"] = "ibm_fez"
        QR = _install_fake_qrmi(acquire_impl=lambda: "session-token-123")
        try:
            prov = QRMIProvider()
            assert prov.install_interceptor("pod-uid-9") is True
            qr = QR()
            token = qr.acquire()
            assert token == "session-token-123"
            assert qr.task_start() == "job-xyz"
            rec = handoff.read(path)
            assert rec["job_id"] == "job-xyz"
            assert rec["session_id"] == "session-token-123"
            assert rec["tier"] == "premium"
            assert rec["pod_uid"] == "pod-uid-9"
        finally:
            sys.modules.pop("qrmi", None)
            os.environ.pop("FLUENCE_HANDOFF_DIR", None)
            os.environ.pop("FLUXION_BACKEND", None)


def test_free_tier_acquire_reraises_and_submit_still_publishes():
    from fluence.providers.qrmi import QRMIProvider
    from fluence import handoff

    def denied():
        raise RuntimeError("acquire not permitted on the open plan")

    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "task.json")
        os.environ["FLUENCE_HANDOFF_DIR"] = d
        QR = _install_fake_qrmi(acquire_impl=denied)
        try:
            prov = QRMIProvider()
            assert prov.install_interceptor("pod-free") is True
            qr = QR()
            raised = False
            try:
                qr.acquire()
            except RuntimeError:
                raised = True
            assert raised, "patched acquire() must re-raise on a free account"
            assert qr.task_start() == "job-xyz"
            rec = handoff.read(path)
            assert rec["job_id"] == "job-xyz"
            assert rec.get("session_id") is None
            assert rec.get("tier") == "free"
        finally:
            sys.modules.pop("qrmi", None)
            os.environ.pop("FLUENCE_HANDOFF_DIR", None)


# ── status -> readiness mapping ───────────────────────────────────────────────


class _FakeStatusResource:
    def __init__(self, statuses):
        self._statuses = list(statuses)

    def task_status(self, job_id):
        return self._statuses.pop(0) if self._statuses else "RUNNING"


def _qrmi_task(statuses):
    from fluence.providers.qrmi import QRMIProvider, QRMITask

    prov = QRMIProvider()
    task = QRMITask(
        job_id="j", backend="ibm_fez", resource_type="qiskit-runtime-service"
    )
    task._resource = _FakeStatusResource(statuses)
    return prov, task


def test_queued_is_not_ready():
    prov, task = _qrmi_task(["QUEUED"])
    assert prov.is_ready_to_ungate(task) is False


def test_running_is_ready_and_position_one():
    prov, task = _qrmi_task(["RUNNING", "RUNNING"])
    assert prov.is_ready_to_ungate(task) is True
    assert prov.queue_position(task) == 1


def test_terminal_completed_is_ready():
    prov, task = _qrmi_task(["COMPLETED"])
    assert prov.is_ready_to_ungate(task) is True


def test_enum_like_status_is_normalized():
    class E:
        def __init__(self, s):
            self.s = s

        def __str__(self):
            return self.s

    prov, task = _qrmi_task([E("TaskStatus.Running")])
    assert prov.is_ready_to_ungate(task) is True


def test_status_error_is_not_ready_not_raising():
    class Boom:
        def task_status(self, job_id):
            raise RuntimeError("transient")

    from fluence.providers.qrmi import QRMIProvider, QRMITask

    prov = QRMIProvider()
    task = QRMITask(
        job_id="j", backend="ibm_fez", resource_type="qiskit-runtime-service"
    )
    task._resource = Boom()
    assert prov.is_ready_to_ungate(task) is False


# ── job id / session id accessors ─────────────────────────────────────────────


def test_job_id_and_session_id_accessors():
    from fluence.providers.qrmi import QRMIProvider, QRMITask

    prov = QRMIProvider()
    task = QRMITask(
        job_id="job-1",
        backend="ibm_fez",
        resource_type="qiskit-runtime-service",
        session_id="sess-1",
    )
    assert prov.job_id(task) == "job-1"
    assert prov.session_id(task) == "sess-1"
    free = QRMITask(
        job_id="job-2", backend="ibm_fez", resource_type="qiskit-runtime-service"
    )
    assert prov.session_id(free) is None


# ── find_my_task via handoff ──────────────────────────────────────────────────


def test_find_my_task_reads_handoff_uses_injected_type():
    from fluence.providers.qrmi import QRMIProvider
    from fluence import handoff

    with tempfile.TemporaryDirectory() as d:
        os.environ["FLUENCE_HANDOFF_DIR"] = d
        os.environ["FLUXION_QRMI_TYPE"] = "pasqal-cloud"  # a non-IBM vendor
        try:
            handoff.publish(
                "job-77",
                session_id="s-77",
                backend="FRESNEL",
                pod_uid="pod-a",
                tier="premium",
            )
            prov = QRMIProvider()
            task = prov.find_my_task("pod-a", "FRESNEL", timeout=5)
            assert task is not None
            assert task.job_id == "job-77"
            assert task.backend == "FRESNEL"
            assert task.resource_type == "pasqal-cloud"  # from the injected env
            assert task.session_id == "s-77"
        finally:
            os.environ.pop("FLUENCE_HANDOFF_DIR", None)
            os.environ.pop("FLUXION_QRMI_TYPE", None)


def test_find_my_task_times_out_to_none():
    from fluence.providers.qrmi import QRMIProvider

    with tempfile.TemporaryDirectory() as d:
        os.environ["FLUENCE_HANDOFF_DIR"] = d
        try:
            prov = QRMIProvider()
            assert prov.find_my_task("pod-a", "ibm_fez", timeout=1) is None
        finally:
            os.environ.pop("FLUENCE_HANDOFF_DIR", None)


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
