#!/usr/bin/env python3
"""A QRMI workload for a pod that fluence placed.

fluence picked a backend for this pod and the webhook put its name in the
environment as FLUXION_BACKEND, with the backend attributes as FLUXION_<KEY>.
This script takes it from there with QRMI. It acquires a session on that
backend with the credentials mounted on the pod, runs a Bell circuit through
the QRMI sampler, prints the counts, and releases the session. fluence holds
the graph allocation and this pod holds the session. A worker in a shared gang
is handed the scout session instead and submits into it without acquiring.

For an IBM backend the credentials are the four <backend>_QRMI_IBM_QRS_*
variables, or IBM_CLOUD_TOKEN and IBM_CLOUD_CRN from which they are derived.
Without credentials this is a rehearsal. It prints what it would need and
exits cleanly, so the same pod proves the placement and the handoff on a
cluster with no account.
"""

import argparse
import os
import sys
import time

from qiskit import QuantumCircuit
from qiskit.transpiler.preset_passmanagers import generate_preset_pass_manager
from qrmi import QuantumResource, ResourceType
from qrmi.primitives.ibm import SamplerV2, get_target

# QRMI type strings to ResourceType names
RESOURCE_TYPES = {
    "qiskit-runtime-service": "IBMQiskitRuntimeService",
    "ibm-quantum-system": "IBMQuantumSystem",
    "pasqal-cloud": "PasqalCloud",
    "pasqal-local": "PasqalLocal",
    "iqm-server": "IQMServer",
    "alice-bob-felis": "AliceBobFelis",
}

# what QRMI reads for each type, looked up as <backend><suffix>
REQUIRED_ENV = {
    "qiskit-runtime-service": (
        "_QRMI_IBM_QRS_ENDPOINT",
        "_QRMI_IBM_QRS_IAM_ENDPOINT",
        "_QRMI_IBM_QRS_IAM_APIKEY",
        "_QRMI_IBM_QRS_SERVICE_CRN",
    ),
    "ibm-quantum-system": (
        "_QRMI_IBM_QS_ENDPOINT",
        "_QRMI_IBM_QS_IAM_ENDPOINT",
        "_QRMI_IBM_QS_IAM_APIKEY",
        "_QRMI_IBM_QS_SERVICE_CRN",
    ),
    "iqm-server": ("_QRMI_IQM_ISA_ENDPOINT", "_QRMI_IQM_ISA_TOKEN"),
}

# the IBM endpoints are fixed, so the credentials reduce to a key and a CRN
IBM_QRS_DERIVED = {
    "_QRMI_IBM_QRS_ENDPOINT": "https://quantum.cloud.ibm.com/api/v1",
    "_QRMI_IBM_QRS_IAM_ENDPOINT": "https://iam.cloud.ibm.com",
}


def section(title):
    print()
    print("== {} ==".format(title), flush=True)


def print_environment():
    """What fluence injected, and which credentials are set, by name only."""
    section("environment from fluence")
    for name in sorted(os.environ):
        if name.startswith("FLUXION_") or name.startswith("FLUENCE_"):
            print("{:<40} {}".format(name, os.environ[name]))
    section("QRMI environment")
    shown = False
    for name in sorted(os.environ):
        if "QRMI" in name or name in ("IBM_CLOUD_TOKEN", "IBM_CLOUD_CRN"):
            print("{:<40} (set, value not shown)".format(name))
            shown = True
    if not shown:
        print("(none)")


def backend_from_fluence():
    """The backend fluence chose and its QRMI type, from the injected env."""
    backend = os.environ.get("FLUXION_BACKEND")
    if not backend:
        sys.exit(
            "no FLUXION_BACKEND: this pod was not placed by fluence, or the "
            "webhook did not inject it (does the pod request a qpu?)"
        )
    rtype = os.environ.get("FLUXION_QRMI_TYPE", "qiskit-runtime-service")
    if rtype not in RESOURCE_TYPES:
        sys.exit("unknown QRMI type {!r} for backend {}".format(rtype, backend))
    section("backend")
    print("backend: ", backend)
    print("type:    ", rtype)
    return backend, rtype


def credentials(backend, rtype):
    """Derive the QRMI variables from the IBM Secret and return what is still missing."""
    if rtype == "qiskit-runtime-service":
        token = os.environ.get("IBM_CLOUD_TOKEN")
        crn = os.environ.get("IBM_CLOUD_CRN")
        derived = dict(IBM_QRS_DERIVED)
        if token:
            derived["_QRMI_IBM_QRS_IAM_APIKEY"] = token
        if crn:
            derived["_QRMI_IBM_QRS_SERVICE_CRN"] = crn
        for suffix, value in derived.items():
            os.environ.setdefault(backend + suffix, value)
    missing = [
        backend + s
        for s in REQUIRED_ENV.get(rtype, ())
        if not os.environ.get(backend + s)
    ]
    if missing:
        print("missing: ", ", ".join(missing))
    else:
        print("credentials present for", backend)
    return missing


def acquire(backend, rtype, ready_timeout):
    """Acquire a QRMI session on the backend and wait until it accepts work."""
    qr = QuantumResource(backend, getattr(ResourceType, RESOURCE_TYPES[rtype]))
    lock = qr.acquire()
    print("session: ", lock)
    # so anything else in this pod that constructs the resource joins this session
    os.environ[backend + "_QRMI_JOB_ACQUISITION_TOKEN"] = str(lock)
    deadline = time.time() + ready_timeout
    while ready_timeout > 0:
        try:
            if qr.is_accessible():
                print("backend: ", backend, "is accepting work")
                break
            why = "not accepting work"
        except Exception as e:  # keep polling until the deadline
            why = str(e)
        if time.time() >= deadline:
            qr.release(lock)
            sys.exit(
                "{} did not become usable within {}s: {}".format(
                    backend, ready_timeout, why
                )
            )
        time.sleep(5)
    return qr, lock


def build_circuit(qubits):
    qc = QuantumCircuit(qubits)
    qc.h(0)
    for q in range(1, qubits):
        qc.cx(q - 1, q)
    qc.measure_all()
    return qc


def transpile_for_device(qc, qr):
    target = get_target(qr)
    isa = generate_preset_pass_manager(optimization_level=1, target=target).run(qc)
    print("circuit: ", dict(isa.count_ops()))
    return isa


def run_sampler(qr, isa, shots):
    sampler = SamplerV2(qr, options={"default_shots": shots})
    job = sampler.run([isa])
    print("task:    ", job.job_id(), "submitted into the session", flush=True)
    return job


def wait_for_task(job, interval=2.0):
    started = time.time()
    last = None
    while not job.in_final_state():
        status = job.status()
        if status != last:
            print(
                "status:  ",
                status,
                "at {:.0f}s".format(time.time() - started),
                flush=True,
            )
            last = status
        time.sleep(interval)
    print("status:  ", job.status(), "after {:.0f}s".format(time.time() - started))
    if job.errored():
        print("logs:")
        print(job.logs())
        sys.exit("the task failed")


def report_counts(job, qubits, shots):
    counts = job.result()[0].data.meas.get_counts()
    print("counts:  ", dict(sorted(counts.items(), key=lambda kv: -kv[1])))
    ideal = {"0" * qubits, "1" * qubits}
    good = sum(v for k, v in counts.items() if k in ideal)
    print("fidelity:", "{}/{} shots in {}".format(good, shots, sorted(ideal)))


def release(qr, lock):
    qr.release(lock)
    print("released:", lock)


def linger(seconds, step=5):
    if seconds <= 0:
        return
    section("lingering for {}s so the pod can be watched".format(seconds))
    left = seconds
    while left > 0:
        print("working, {}s left".format(left), flush=True)
        time.sleep(min(step, left))
        left -= step


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--shots", type=int, default=100)
    ap.add_argument(
        "--qubits", type=int, default=2, help="width of the GHZ/Bell circuit, 2"
    )
    ap.add_argument(
        "--linger",
        type=int,
        default=int(os.environ.get("LINGER", "0")),
        help="seconds to stay alive after the work, 0",
    )
    ap.add_argument(
        "--ready-timeout",
        type=float,
        default=float(os.environ.get("READY_TIMEOUT", "120")),
        help="seconds to wait for the backend to accept work after acquiring, 120",
    )
    args = ap.parse_args()

    print_environment()
    backend, rtype = backend_from_fluence()

    section("credentials, names only")
    if credentials(backend, rtype):
        print()
        print("rehearsal: no credentials, so nothing is acquired or submitted. The")
        print("placement and the handoff from fluence worked. A Secret with the")
        print("variables above would make this a real run.")
        linger(args.linger)
        return

    role = os.environ.get("FLUENCE_COORDINATION_ROLE")
    session = os.environ.get("FLUENCE_QUANTUM_SESSION_ID")
    if role == "worker" and session:
        # a worker joins the scout session rather than acquiring its own
        section("circuit in the scout session, through QRMI")
        print("session: ", session, "from the scout")
        os.environ[backend + "_QRMI_JOB_ACQUISITION_TOKEN"] = session
        qr = QuantumResource(backend, getattr(ResourceType, RESOURCE_TYPES[rtype]))
        isa = transpile_for_device(build_circuit(args.qubits), qr)
        job = run_sampler(qr, isa, args.shots)
        wait_for_task(job)
        report_counts(job, args.qubits, args.shots)
        linger(args.linger)
        return
    if role == "worker":
        print(
            "role:     worker with no session, task",
            os.environ.get("FLUENCE_QUANTUM_JOB_ID", "?"),
        )
        linger(args.linger)
        return

    section("session and circuit, through QRMI")
    qr, lock = acquire(backend, rtype, args.ready_timeout)
    try:
        isa = transpile_for_device(build_circuit(args.qubits), qr)
        job = run_sampler(qr, isa, args.shots)
        wait_for_task(job)
        report_counts(job, args.qubits, args.shots)
    finally:
        release(qr, lock)
    linger(args.linger)


if __name__ == "__main__":
    main()
