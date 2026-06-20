FROM python:3.11-slim

LABEL org.opencontainers.image.source="https://github.com/converged-computing/fluence"
LABEL org.opencontainers.image.description="Fluence quantum coordination sidecar — provider-agnostic; resolves vendor at runtime"

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -LO "https://dl.k8s.io/release/$(curl -Ls https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" \
    && chmod +x kubectl && mv kubectl /usr/local/bin/

WORKDIR /app

# Vendor SDKs. The sidecar resolves and uses only the one matching the
# scheduler-chosen backend at runtime, but the image carries each supported
# provider's SDK. There are few providers, so the image stays manageable.
RUN pip install --no-cache-dir \
    boto3 \
    amazon-braket-sdk==1.88.0
    # ibm: qiskit-ibm-runtime   (add when the IBM provider lands)

# Build context is sidecars/ so paths are relative to that.
COPY lib/ ./lib/
COPY providers/ ./providers/

ENV FLUENCE_TASK_DISCOVERY_TIMEOUT=300
ENV FLUENCE_POLL_INTERVAL=30
ENV PYTHONPATH=/app:/app/lib

CMD ["python", "lib/sidecar.py"]
