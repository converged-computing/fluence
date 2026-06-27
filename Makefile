# fluence — Kubernetes scheduler plugin backed by Fluxion (flux-sched), with
# optional quantum resources modeled in the resource graph.
#
# The scheduler only schedules. It links flux-sched (the matcher) and does NOT
# depend on QRMI — quantum job submission lives in a separate workload container
# (github.com/converged-computing/qrmi-sampler), not here.

FLUX_SCHED_ROOT ?= /opt/flux-sched
IMG             ?= ghcr.io/converged-computing/fluence:latest
TEST_IMG	?= vanessa/fluence:test

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

.PHONY: python
python:
	docker build -f python/Dockerfile -t ghcr.io/converged-computing/fluence-sidecar:latest ./python
	docker push ghcr.io/converged-computing/fluence-sidecar:latest
	# kind load docker-image ghcr.io/converged-computing/fluence-sidecar:latest

.PHONY: test
test:
	CGO_ENABLED=1 CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" \
	go test ./...

.PHONY: test-restore
test-restore:
	CGO_ENABLED=1 CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS)" \
	  go run ./cmd/recovery-probe -graph ./examples/test/cluster.jgf -spec ./examples/test/jobspec-cpu.yaml

.PHONY: image
image: ## Build the scheduler container image
	docker build -t $(IMG) .

.PHONY: test-image
test-image: ## Build the scheduler container image
	docker build -t $(TEST_IMG) .
	docker push $(TEST_IMG)

.PHONY: test-image-deploy
test-image-deploy: test-image
	kubectl patch podgroup training -n default --type=merge -p '{"metadata":{"finalizers":null}}' || true
	kubectl delete deployments --all
	kubectl delete pods --all
	kubectl delete -f deploy/fluence-test.yaml || true
	kubectl delete pods --all

.PHONY: test-deploy-recreate
test-deploy-recreate: test-image-deploy
	kubectl apply -f deploy/fluence-pull-test.yaml

.PHONY: deploy
deploy: ## Install RBAC + scheduler into kube-system
	kubectl apply -f deploy/fluence-.yaml

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN{FS=":.*?## "}{printf "  %-14s %s\n", $$1, $$2}'
