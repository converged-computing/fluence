FROM ghcr.io/converged-computing/fluence-base:latest AS builder

WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download || true
COPY . .
RUN CGO_ENABLED=1 \
    CGO_CFLAGS="-I/opt/flux-sched" \
    CGO_LDFLAGS="-L/opt/flux-sched/resource -L/opt/flux-sched/resource/libjobspec -L/opt/flux-sched/resource/reapi/bindings -lresource -ljobspec_conv -lreapi_cli -lflux-idset -lstdc++ -lczmq -ljansson -lhwloc -lboost_system -lflux-hostlist -lboost_graph -lyaml-cpp" \
    go build -ldflags '-w' -o /bin/fluence ./cmd/fluence && \
    CGO_ENABLED=0 go build -ldflags '-w' -o /bin/fluence-deviceplugin ./cmd/deviceplugin && \
    CGO_ENABLED=0 go build -ldflags '-w' -o /bin/fluence-webhook ./cmd/webhook

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
COPY --from=builder /bin/fluence-deviceplugin /bin/fluence-deviceplugin
COPY --from=builder /bin/fluence-webhook /bin/fluence-webhook
ENTRYPOINT ["/bin/fluence"]
