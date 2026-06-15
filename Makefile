# fluence — Kubernetes scheduler plugin backed by Fluxion (flux-sched), with
# optional quantum resources modeled in the resource graph.
#
# The scheduler only schedules. It links flux-sched (the matcher) and does NOT
# depend on QRMI — quantum job submission lives in a separate workload container
# (github.com/converged-computing/qrmi-sampler), not here.

FLUX_SCHED_ROOT ?= /opt/flux-sched
IMG             ?= ghcr.io/converged-computing/fluence:latest

# cgo flags for the scheduler binary: flux-sched only.
CGO_CFLAGS  = -I$(FLUX_SCHED_ROOT)
CGO_LDFLAGS = -L$(FLUX_SCHED_ROOT)/resource \
              -L$(FLUX_SCHED_ROOT)/resource/libjobspec \
              -L$(FLUX_SCHED_ROOT)/resource/reapi/bindings \
              -lresource -ljobspec_conv -lreapi_cli -lflux-idset \
              -lstdc++ -lczmq -ljansson -lhwloc -lboost_system \
              -lflux-hostlist -lboost_graph -lyaml-cpp

.PHONY: build
build: ## Build all binaries (scheduler needs flux-sched; helpers are pure Go)
	CGO_ENABLED=1 CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" \
	  go build -o bin/fluence ./cmd/fluence
	CGO_ENABLED=0 go build -o bin/fluence-deviceplugin ./cmd/deviceplugin
	CGO_ENABLED=0 go build -o bin/fluence-webhook ./cmd/webhook

.PHONY: test
test: ## Pure-Go unit tests (no flux, no k8s scheduler libs, no cluster)
	go test ./pkg/jgf/... ./pkg/cluster/... ./pkg/jobspec/... ./pkg/placement/... \
	  ./pkg/quantum/... ./pkg/webhook/... ./pkg/deviceplugin/...

.PHONY: test-graph
test-graph: ## Matcher tests (needs flux-sched)
	CGO_ENABLED=1 CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" \
	  go test ./pkg/graph/...

.PHONY: image
image: ## Build the scheduler container image
	docker build -t $(IMG) .

.PHONY: deploy
deploy: ## Install RBAC + scheduler into kube-system
	kubectl apply -f deploy/fluence.yaml

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN{FS=":.*?## "}{printf "  %-14s %s\n", $$1, $$2}'