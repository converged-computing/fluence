FROM python:3.11-slim

LABEL org.opencontainers.image.source="https://github.com/converged-computing/fluence"
LABEL org.opencontainers.image.description="Fluence AWS Braket sidecar — quantum-classical scheduling coordination"

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -LO "https://dl.k8s.io/release/$(curl -Ls https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" \
    && chmod +x kubectl && mv kubectl /usr/local/bin/

WORKDIR /app

RUN pip install --no-cache-dir \
    amazon-braket-sdk==1.88.0 \
    boto3

# Copy shared lib first, then vendor-specific files
# Build context is sidecars/ so paths are relative to that
COPY sidecars/lib/ungate.py ./lib/ungate.py
COPY sidecars/braket/sidecar.py .
COPY sidecars/braket/fluence_braket_intercept.py .

ENV FLUENCE_TASK_DISCOVERY_TIMEOUT=300
ENV FLUENCE_POLL_INTERVAL=30

CMD ["python", "sidecar.py"]
