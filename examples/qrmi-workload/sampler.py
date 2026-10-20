#!/usr/bin/env python3
"""A QRMI workload for a pod that fluence placed.

fluence chose the backend and the webhook put its name in FLUXION_BACKEND.
This acquires a session on it, runs a Bell circuit through the QRMI sampler,
prints the counts, and releases the session. Without credentials it prints
what it would need and exits, so the same pod works as a rehearsal.
"""

import argparse
import os
import signal
import sys
import time

from qiskit import QuantumCircuit
from qiskit.transpiler.preset_passmanagers import generate_preset_pass_manager
from qrmi import QuantumResource, ResourceType
from qrmi.primitives.ibm import SamplerV2, get_backend

RESOURCE_TYPES = {
    "qiskit-runtime-service": ("IBMQiskitRuntimeService", "_QRMI_IBM_QRS"),
    "ibm-quantum-system": ("IBMQuantumSystem", "_QRMI_IBM_QS"),
}
REQUIRED = ("_ENDPOINT", "_IAM_ENDPOINT", "_IAM_APIKEY", "_SERVICE_CRN")
ENDPOINTS = {
    "_ENDPOINT": "https://quantum.cloud.ibm.com/api/v1",
    "_IAM_ENDPOINT": "https://iam.cloud.ibm.com",
}


def section(title):
    print("\n== {} ==".format(title))


def print_environment():
    section("environment from fluence")
    for name in sorted(os.environ):
        if name.startswith(("FLUXION_", "FLUENCE_")):
            print("{:<32} {}".format(name, os.environ[name]))
    section("credentials, names only")
    names = [n for n in sorted(os.environ) if "QRMI" in n or n.startswith("IBM_CLOUD_")]
    print("\n".join(names) or "(none)")


def backend_from_fluence():
    backend = os.environ.get("FLUXION_BACKEND")
    if not backend:
        sys.exit("no FLUXION_BACKEND in the environment")
    rtype = os.environ.get("FLUXION_QRMI_TYPE", "qiskit-runtime-service")
    if rtype not in RESOURCE_TYPES:
        sys.exit("unsupported QRMI type {} for {}".format(rtype, backend))
    section("backend")
    print("backend: ", backend)
    print("type:    ", rtype)
    return backend, rtype


def missing_credentials(backend, rtype):
    """Derive the QRMI variables from IBM_CLOUD_TOKEN and IBM_CLOUD_CRN, and
    return the names still unset."""
    prefix = backend + RESOURCE_TYPES[rtype][1]
    derived = dict(ENDPOINTS)
    if os.environ.get("IBM_CLOUD_TOKEN"):
        derived["_IAM_APIKEY"] = os.environ["IBM_CLOUD_TOKEN"]
    if os.environ.get("IBM_CLOUD_CRN"):
        derived["_SERVICE_CRN"] = os.environ["IBM_CLOUD_CRN"]
    for suffix, value in derived.items():
        os.environ.setdefault(prefix + suffix, value)
    return [prefix + s for s in REQUIRED if not os.environ.get(prefix + s)]


def resource(backend, rtype):
    return QuantumResource(backend, getattr(ResourceType, RESOURCE_TYPES[rtype][0]))


class Unanswered(Exception):
    pass


def _alarm(signum, frame):
    raise Unanswered()


def ask(fn, seconds):
    """Call fn and give up after seconds. A request that hangs must not hold
    the pod, and later a session, with it."""
    signal.signal(signal.SIGALRM, _alarm)
    signal.alarm(int(seconds))
    try:
        return fn()
    finally:
        signal.alarm(0)


def wait_operational(qr, backend, timeout, each=60):
    """The backend object once IBM reports it operational. Asked before a
    session is opened, so nothing is held while waiting."""
    deadline = time.time() + timeout
    while True:
        try:
            dev = ask(lambda: get_backend(qr), each)
            status = ask(dev.status, each)
            why = status.status_msg
            if status.operational:
                print("ready:   ", backend, why)
                return dev
        except Unanswered:
            why = "no answer from IBM in {}s".format(each)
        if time.time() >= deadline:
            sys.exit("{} not operational after {}s: {}".format(backend, timeout, why))
        print("waiting: ", backend, why)
        time.sleep(5)


def bell_circuit(qubits):
    qc = QuantumCircuit(qubits)
    qc.h(0)
    for q in range(1, qubits):
        qc.cx(q - 1, q)
    qc.measure_all()
    return qc


def run(qr, dev, qubits, shots):
    isa = generate_preset_pass_manager(optimization_level=1, target=dev.target).run(
        bell_circuit(qubits)
    )
    print("circuit: ", dict(isa.count_ops()))
    job = SamplerV2(qr, options={"default_shots": shots}).run([isa])
    print("task:    ", job.job_id())
    started = time.time()
    last = None
    while not job.in_final_state():
        if job.status() != last:
            last = job.status()
            print("status:  ", last, "at {:.0f}s".format(time.time() - started))
        time.sleep(2)
    print("status:  ", job.status(), "after {:.0f}s".format(time.time() - started))
    if job.errored():
        print(job.logs())
        sys.exit("the task failed")
    counts = job.result()[0].data.meas.get_counts()
    print("counts:  ", dict(sorted(counts.items(), key=lambda kv: -kv[1])))
    ideal = {"0" * qubits, "1" * qubits}
    good = sum(v for k, v in counts.items() if k in ideal)
    print("fidelity:", "{}/{} shots in {}".format(good, shots, sorted(ideal)))


def main():
    sys.stdout.reconfigure(line_buffering=True)
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--shots", type=int, default=100)
    ap.add_argument("--qubits", type=int, default=2, help="width of the circuit")
    ap.add_argument(
        "--ready-timeout",
        type=float,
        default=120,
        help="seconds to wait for the backend",
    )
    args = ap.parse_args()

    print_environment()
    backend, rtype = backend_from_fluence()
    missing = missing_credentials(backend, rtype)
    if missing:
        print("missing: ", ", ".join(missing))
        print(
            "\nrehearsal, nothing acquired or submitted. The placement and the handoff worked."
        )
        return

    qr = resource(backend, rtype)
    session = os.environ.get("FLUENCE_QUANTUM_SESSION_ID")
    if os.environ.get("FLUENCE_COORDINATION_ROLE") == "worker":
        # a worker submits into the scout's session instead of acquiring one
        if not session:
            print(
                "worker with no session, task",
                os.environ.get("FLUENCE_QUANTUM_JOB_ID", "?"),
            )
            return
        section("circuit in the scout's session")
        print("session: ", session)
        os.environ[backend + "_QRMI_JOB_ACQUISITION_TOKEN"] = session
        run(
            qr,
            wait_operational(qr, backend, args.ready_timeout),
            args.qubits,
            args.shots,
        )
        return

    section("session and circuit, through QRMI")
    # is the device usable before anything is held
    dev = wait_operational(qr, backend, args.ready_timeout)
    lock = qr.acquire()
    print("session: ", lock)
    os.environ[backend + "_QRMI_JOB_ACQUISITION_TOKEN"] = str(lock)
    try:
        run(qr, dev, args.qubits, args.shots)
    finally:
        qr.release(lock)
        print("released:", lock)


if __name__ == "__main__":
    main()
