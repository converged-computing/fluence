FROM fluxrm/flux-core:noble AS builder

# base image for fluence to speed up build time
# docker build -t ghcr.io/converged-computing/fluence-base:latest .

USER root
ENV LD_LIBRARY_PATH=/usr/lib:/usr/local/lib
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        libboost-graph-dev libboost-system-dev libboost-filesystem-dev \
        libboost-regex-dev libyaml-cpp-dev libedit-dev libczmq-dev \
        python3-yaml ninja-build cmake curl git wget ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Go toolchain, for the architecture being built. buildx sets TARGETARCH
# (amd64 or arm64) per platform. A plain docker build gets the host architecture.
ARG TARGETARCH
RUN arch="${TARGETARCH:-$(dpkg --print-architecture)}" \
 && wget -q "https://go.dev/dl/go1.26.0.linux-${arch}.tar.gz" \
 && tar -C /usr/local -xzf "go1.26.0.linux-${arch}.tar.gz" && rm "go1.26.0.linux-${arch}.tar.gz"
ENV PATH=$PATH:/usr/local/go/bin

RUN wget https://github.com/flux-framework/flux-sched/releases/download/v0.56.0/flux-sched-0.56.0.tar.gz && \
    tar -xzvf flux-sched-0.56.0.tar.gz && mv flux-sched-0.56.0 /opt/flux-sched \
 && cd /opt/flux-sched && export WITH_GO=yes && ./configure --prefix=/usr \
 && mkdir build && cd build && cmake ../ && cd ../ && make -j"$(nproc)" && make install
ENV FLUX_SCHED_ROOT=/opt/flux-sched
