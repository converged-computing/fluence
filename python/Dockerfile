# Fluence quantum coordination sidecar image.
#
# Bakes the `fluence` Python package in, so the SAME image serves three roles
# (versions locked together — they are built from this one source tree):
#   1. sidecar container   — runs `fluence-sidecar` (the coordination loop)
#   2. init container      — runs `python -m fluence.stage <dir>` to copy the
#                            pure-Python package + sitecustomize into a shared
#                            volume that the webhook mounts onto the user
#                            container's PYTHONPATH (Model C delivery)
#   3. (the staged copy)   — the user container imports the staged package via
#                            sitecustomize; no install required in the user image
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
RUN pip install --no-cache-dir ".[all]"

ENV FLUENCE_TASK_DISCOVERY_TIMEOUT=300
ENV FLUENCE_POLL_INTERVAL=30

# Default entrypoint is the sidecar loop; the init container overrides the
# command with `python -m fluence.stage <dir>`.
CMD ["fluence-sidecar"]
