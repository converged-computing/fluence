# The QRMI workload for pods that fluence places. Built for amd64 and arm64
# as ghcr.io/converged-computing/fluence-qrmi-workload.
FROM python:3.12-slim

LABEL org.opencontainers.image.source="https://github.com/converged-computing/fluence"
LABEL org.opencontainers.image.description="QRMI workload for fluence"

RUN pip install --no-cache-dir "qrmi[ibm]" qiskit qiskit-ibm-runtime \
 && python3 -c "import qrmi, qiskit, qiskit_ibm_runtime; from qrmi.primitives.ibm import SamplerV2"

COPY sampler.py /app/sampler.py
WORKDIR /app
ENTRYPOINT ["python3", "/app/sampler.py"]
