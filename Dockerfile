# Multi-stage build for the fluence scheduler.
#
# The scheduler binary cgo-links flux-sched (Fluxion) for resource matching.
# It does NOT depend on QRMI — quantum job submission is a separate workload
# (github.com/converged-computing/qrmi-sampler). So this image needs only
# flux-sched, no Rust/QRMI. Mirrors the .devcontainer build.

# ---------- builder ----------
FROM fluxrm/flux-core:noble AS builder

USER root
ENV LD_LIBRARY_PATH=/usr/lib:/usr/local/lib
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        libboost-graph-dev libboost-system-dev libboost-filesystem-dev \
        libboost-regex-dev libyaml-cpp-dev libedit-dev libczmq-dev \
        python3-yaml ninja-build cmake curl git wget ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Go toolchain
RUN wget -q https://go.dev/dl/go1.26.0.linux-amd64.tar.gz \
 && tar -C /usr/local -xzf go1.26.0.linux-amd64.tar.gz && rm go1.26.0.linux-amd64.tar.gz
ENV PATH=$PATH:/usr/local/go/bin

# flux-sched (Fluxion) with the Go reapi bindings -> /usr; build tree at /opt/flux-sched
RUN git clone https://github.com/flux-framework/flux-sched /opt/flux-sched \
 && cd /opt/flux-sched && export WITH_GO=yes && ./configure --prefix=/usr \
 && mkdir build && cd build && cmake ../ && cd ../ && make -j"$(nproc)" && make install
ENV FLUX_SCHED_ROOT=/opt/flux-sched

# Build the scheduler
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download || true
COPY . .
RUN CGO_ENABLED=1 \
    CGO_CFLAGS="-I/opt/flux-sched" \
    CGO_LDFLAGS="-L/opt/flux-sched/resource -L/opt/flux-sched/resource/libjobspec -L/opt/flux-sched/resource/reapi/bindings -lresource -ljobspec_conv -lreapi_cli -lflux-idset -lstdc++ -lczmq -ljansson -lhwloc -lboost_system -lflux-hostlist -lboost_graph -lyaml-cpp" \
    go build -ldflags '-w' -o /bin/fluence ./cmd/fluence

FROM fluxrm/flux-core:noble AS runtime

USER root
ENV LD_LIBRARY_PATH=/usr/lib:/opt/flux-sched/resource:/opt/flux-sched/resource/reapi/bindings:/opt/flux-sched/resource/libjobspec

RUN apt-get update && apt-get install -y --no-install-recommends \
        libboost-graph1.83.0 libboost-system1.83.0 libyaml-cpp0.8 libczmq4 libjansson4 libhwloc15 \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/flux-sched/resource /opt/flux-sched/resource
COPY --from=builder /usr/lib/libresource.so* /usr/lib/
COPY --from=builder /usr/lib/libreapi_cli.so* /usr/lib/
COPY --from=builder /usr/lib/libjobspec_conv.so* /usr/lib/
RUN ldconfig

COPY --from=builder /bin/fluence /bin/fluence
ENTRYPOINT ["/bin/fluence"]