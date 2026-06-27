# Fluence quantum coordination sidecar image.
# TODO organize into subdirectories when we have >1 image
#sitecustomize; no install required in the user image
FROM python:3.11-slim

LABEL org.opencontainers.image.source="https://github.com/converged-computing/fluence"
LABEL org.opencontainers.image.description="Fluence quantum coordination sidecar (provider-agnostic; vendor resolved at runtime)"

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# kubectl for the ungate step.
RUN curl -LO "https://dl.k8s.io/release/$(curl -Ls https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" \
    && chmod +x kubectl && mv kubectl /usr/local/bin/

WORKDIR /app
COPY . /app

# Install the package with the vendor SDKs the SIDECAR needs for its own API
# calls (task discovery / queue polling). The interceptor staged into the user
# container carries NONE of these — it patches whatever SDK the user already has.
RUN pip install --no-cache-dir ".[all]" && ln -s $(which python3) /usr/bin/python

ENV FLUENCE_TASK_DISCOVERY_TIMEOUT=300000
ENV FLUENCE_POLL_INTERVAL=30
CMD ["fluence-sidecar"]
