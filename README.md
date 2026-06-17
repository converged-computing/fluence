# fluence

![img/fluence.png](img/fluence.png)

🚧 **UNDER DEVELOPMENT** 🚧 Thank you for your patience! -@vsoch

A Kubernetes scheduler plugin that places **pod groups** (and individual pods) by
matching them against a [Fluxion](https://github.com/flux-framework/flux-sched)
resource graph built from the live cluster. Beyond ordinary compute, it can model
**arbitrary virtual resources** — quantum backends today, anything with a count
and some attributes tomorrow — as first-class graph vertices that can be filtered
on and whose attributes are injected into the workload's environment. Nothing in
the design is quantum-specific: a resource `type` is an opaque string throughout.

This updates [flux-k8s](https://github.com/flux-framework/flux-k8s) to use the
native Kubernetes PodGroup (Gang) API — no sidecar, and no
`kubernetes-sigs/scheduler-plugins` dependency. The quantum resource model is
proven out first in
[fluxion-quantum](https://github.com/converged-computing/fluxion-quantum).

## How the pieces fit together

```console
 resources.yaml --+ 
   +--------------+-------------------------------+
   v              v                               v
scheduler     device plugin                    webhook
(pkg/fluence)  (advertises types)           (injects env)
   |
   |  build graph                (pkg/cluster -> pkg/jgf)
   |  generate jobspecs          (pkg/placement -> pkg/jobspec)
   |  match + cancel via Fluxion (pkg/graph, cgo)
   |  parse allocation           (pkg/graph, pkg/placement)
   v
 pod bound to a node + backend/attrs stamped as annotations
                          |
                          v
           webhook-injected env reads those annotations
```

A single `resources.yaml` is the source of truth, read by three independent
processes: the **scheduler** builds the graph and validates requests, the **device
plugin** advertises the requestable types as Kubernetes extended resources (so
pods requesting them pass admission), and the **webhook** computes the
environment-variable contract it injects into workloads. Note that the device
plugin can be removed from the design, assuming we do not need a `NodeResourcesFit` check.
I have kept it for now anticipating future cases where we have node-specific resources.
For now, the counts can be viewed as API quotas.

For one pod group the scheduler generates one or more jobspecs, match-allocates
each against the graph (all-or-nothing), combines them into a placement, binds the
pod, and stamps the matched backend + attributes as annotations. The webhook has
already injected downward-API env vars that read those annotations, so the
workload sees which backend and attributes it got.

## The resource model

**Every allocatable thing in the graph is a `node`-typed vertex** -- physical
compute nodes and every level of every virtual resource alike. This is deliberate:
Fluxion's RFC 31 property constraints (the only attribute-based pruning the matcher
offers) apply **only** to `node` and `storage_node` vertices, so modeling a virtual
resource as a node is what makes it filterable.

- **Physical nodes** carry `virtual=false`.
- **Virtual resources** carry `virtual=true`, a `class=<type>` for the resource's
  own type *and each descendant type in its subtree*, and their attributes.

Property values are encoded into the property **key** (`virtual=true`,
`class=qpu`, `region=us-east-1`) because RFC 31 matching is key-presence only -- it
never compares the value half. A resource is selected by a `class=<type>`
constraint, so any level is independently requestable (`class=qdevice`,
`class=qpu`, `class=qubit`). Descendant classes are propagated **up** so a global
constraint selecting a nested type isn't pruned at an ancestor node on the way
down; attributes are inherited **down** (a child gets its parent's attributes
unless it overrides or clears a key) so an attribute filter combined with a nested
class still reaches the target.

Because a jobspec carries one global constraint, compute (`virtual=false`) and a
virtual resource (`virtual=true`) cannot be co-selected in one match -- one would
prune the other. A pod needing both produces **two** match-allocate calls, held
together all-or-nothing.

---

## Components

### `pkg/jgf` — JGF graph builder

Builds the JSON Graph Format document Fluxion consumes. `AddRoot`/`AddChild`
create vertices with `Options` (size, unit, exclusivity, rank, properties).
`Options.NodeProperties` are RFC 31 resource properties (a nested `properties`
object the matcher prunes against); `Options.Properties` are descriptive metadata
only. `Options.Rank` sets a real execution rank — every `node` vertex needs one.

### `pkg/cluster` — config schema and graph construction

Parses the resource config and turns it (plus the live cluster nodes) into a graph.

```yaml
resources:
  - type: qdevice            # opaque type; the resource's class
    name: rigetti_cepheus    # vertex name / backend identity
    parent: cluster          # where it attaches (default "cluster")
    attributes: aws-east     # inline map OR a reference into the registry below
    with:                    # recursive children, same schema
      - type: qpu
        count: 1
        with:
          - type: qubit
            count: 80

attributes:                  # named attribute-set registry (reuse across backends)
  aws-east:
    region: us-east-1
    connectivity: all-to-all
```

`LoadResourcesConfig` parses YAML/JSON, resolves attributes (inline or by
reference) with downward inheritance, and errors on a missing `type`, an unknown
reference, or a non-empty config that defines no `resources:` (a schema mismatch,
so it fails loudly instead of building an empty graph). `BuildGraph` emits each
cluster node as a `virtual=false` node, then appends the resource trees: every
resource at every depth becomes a `virtual=true` node carrying its class set and
inherited attributes, on a real rank from a counter shared with the physical
nodes. `FluxionResourceNames` returns every requestable type (what the device
plugin advertises); `AttributeKeys` returns the union of attribute keys (the
webhook's env contract).

### `pkg/jobspec` — jobspec types

The Fluxion jobspec representation (`Jobspec`, `Resource`, `Task`) with YAML/JSON
round-tripping: a `resources` tree (a `slot` holding the request), an `attributes`
block (constraint + duration), and `tasks`.

### `pkg/placement` — jobspec generation and allocation parsing

`JobspecsForGroup` turns a pod group into the jobspecs to match:

- Always one **compute** jobspec — a slot per pod holding core/memory/gpu,
  constrained to `virtual=false`. A classical group produces only this (one match).
- One **device** jobspec per requested virtual type, requesting a `node`
  constrained to `virtual=true` + `class=<type>`. The class selects which virtual
  node; the count comes from the request.
- Every pod gets at least one core (a device-only pod still needs a host).
- Every jobspec sets `duration: 0` — held until explicitly cancelled.
- A request for an unmodeled type is a hard error.

Jobspecs are submitted as **JSON** (not YAML): the constraint parser requires
quoted property scalars, and JSON always quotes.

`PlacementFromAllocation` classifies an allocation's node vertices by the
`virtual` marker — `virtual=false`/unmarked are compute bind targets, a
`virtual=true` node is the backend identity, and its `fluxion.flux-framework.org/`
properties are decomposed into attributes for env injection. This package also
owns the shared names: the `fluxion.flux-framework.org/` request prefix, the
`fluence.flux-framework.org/{backend,jobid,attr-*}` annotations, and the
`FLUXION_` env prefix.

### `pkg/graph` — Fluxion binding (cgo)

Wraps the cgo matcher (`FluxionGraph`: `Init`, `MatchAllocateSpec`, `Cancel`,
`Satisfy`) and parses allocations. Fluence uses the **jgf** match format: it emits
every allocated vertex with its properties regardless of type, so a virtual
allocation that bottoms out in nodes (no cores) still serializes — which rv1
cannot. `NodesFromAllocation` returns node vertices with their properties for the
marker-based classification. This is the only cgo-dependent package, so it gates
local builds of everything importing it.

### `pkg/fluence` — the scheduler plugin

`New` lists the cluster, loads the config, builds and logs the graph, and inits the
matcher.

- **PreFilter** runs per group: generate jobspecs, then `matchGroup` runs each as
  an independently held allocation, **all-or-nothing** — any failure cancels the
  successes, so the group never holds a partial set.
- **Filter** permits only the nodes Fluxion assigned.
- **PreBind** records durable state: the jobids on the owning object (PodGroup for
  a gang, else the pod) for cancellation, and the matched backend + attributes as
  annotations the webhook reads.
- **Cancellation** is informer-driven (no framework delete hook): deleting a
  PodGroup or ungrouped pod frees its held allocations. The jobid annotation is the
  durable source of truth; the graph (and allocations) are rebuilt on restart from
  the same annotations.

Gang semantics are delegated to the native PodGroup API; fluence only places.

### `pkg/deviceplugin` — extended-resource advertisement

Advertises each requestable type (`fluxion.flux-framework.org/<type>`) as a
counted extended resource, so a pod requesting one passes `NodeResourcesFit`
admission. The real gating is Fluxion (and the backend's own limits); since a
virtual backend is reachable from any node, each type is advertised at a large
ceiling. Types come from the same config as the graph, so they can't drift.

### `pkg/webhook` — environment injection

A mutating webhook that surfaces scheduler-chosen values to a workload. Container
env is fixed at creation but the match happens after admission, so it injects
**downward-API** env vars whose values populate later from the annotations PreBind
writes. The injected set is a **config-derived contract**: `FLUXION_BACKEND` plus
one `FLUXION_<KEY>` per attribute key across all backends — add an attribute and
its env var appears, no code change. The webhook self-manages TLS (generates a CA
and patches its own `caBundle`), so no cert-manager is needed. A vendor-agnostic
workload reads these normalized names regardless of which backend it matched.

## Commands

- `cmd/fluence` — the scheduler binary (stock kube-scheduler + the plugin).
- `cmd/deviceplugin` — the extended-resource DaemonSet.
- `cmd/webhook` — the env-injection webhook.
- `cmd/recovery-probe` — verifies allocation replay survives a graph rebuild
  (what a restart does); see `make test-restore`. Note this was implemented but removed because the code in fluxion is only part of a PR branch, and I feel nervous about depending on it.

## Configuration

These are environment variables for fluence.

| Env var | Read by | Meaning |
|---|---|---|
| `FLUENCE_RESOURCES` | scheduler, device plugin, webhook | path to `resources.yaml`; absent = classical-only |
| `FLUENCE_MATCH_POLICY` | scheduler | Fluxion match policy (default `first`) |
| `FLUENCE_RESOURCE_CAPACITY` | device plugin | per-node ceiling per type (default 1000) |

## Observability

The scheduler logs (prefix `[fluence]`) the full graph and known devices at
startup and, per match, the submitted jobspec, the raw Fluxion allocation, and the
parsed placement. The webhook logs (`[fluence-webhook]`) the env contract at
startup. Because live behavior (cgo matcher, real Kubernetes) can't be fully
unit-tested, these logs are the primary debugging surface.

## Build

The scheduler links flux-sched (the matcher). It does **not** link QRMI or any
quantum backend — quantum job submission lives in a separate workload container
([qrmi-sampler](https://github.com/converged-computing/qrmi-sampler)), not here.

```bash
# Inside the .devcontainer (flux-sched at /opt/flux-sched):
make build      # bin/fluence (cgo+flux) + bin/fluence-deviceplugin + bin/fluence-webhook
make test
make image      # or build the container image with all three binaries
```

## Deploy

Create a cluster on a Kubernetes release with native gang scheduling and the
feature gates the kind config enables (`GangScheduling`, `GenericWorkload`, the
`scheduling.k8s.io/v1alpha2` API group):

```bash
kind create cluster --image kindest/node:v1.36.1 --config deploy/kind-config.yaml
kind load docker-image ghcr.io/converged-computing/fluence:latest
```

### 1. Gang scheduling (classical — all you need for non-quantum)

```bash
kubectl apply -f deploy/fluence.yaml   # scheduler, RBAC, and the webhook
```

Pods opt in with `schedulerName: fluence`; a multi-pod gang adds a
`scheduling.k8s.io/pod-group` label (a single pod is a group of one, no label
needed).

```bash
kubectl apply -f examples/podgroup.yaml
kubectl get podgroups.scheduling.k8s.io
```
```console
NAME       POLICY   WORKLOAD   STATUS      AGE
training   Gang     <none>     Scheduled   15s
```

Cleanup:

```bash
kubectl patch podgroup training -n default --type=merge -p '{"metadata":{"finalizers":null}}'
kubectl delete -f examples/podgroup.yaml
```

### 2. Quantum (the resources add-on)

This supplies the `fluence-resources` ConfigMap (the source of truth for which
backends exist) and the device plugin that advertises them:

```bash
kubectl apply -f deploy/fluence-resources.yaml
kubectl apply -f deploy/device-plugin.yaml
# The scheduler and webhook read the config at startup — restart to pick it up:
kubectl rollout restart deployment/fluence -n kube-system
kubectl rollout restart deployment/fluence-webhook -n kube-system
```

Confirm the resources are advertised:

```bash
kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.allocatable}{"\n"}{end}' \
  | grep fluxion.flux-framework.org
```

Create the IBM credentials the **workload** uses to submit (the scheduler never
needs them):

```bash
ibmcloud login --apikey <key>
export IBM_CLOUD_TOKEN=<key>
export IBM_CLOUD_CRN=$(ibmcloud resource service-instances --service-name quantum-computing --output json | jq -r '.[0].crn')
kubectl create secret generic ibm-quantum -n default \
  --from-literal=token="$IBM_CLOUD_TOKEN" --from-literal=crn="$IBM_CLOUD_CRN"
```

Run a single quantum pod — it just requests `fluxion.flux-framework.org/qpu`, with
no hard-coded backend (the webhook + PreBind supply `FLUXION_BACKEND`):

```bash
kubectl apply -f examples/quantum-pod.yaml

# fluence's chosen backend, also injected as $FLUXION_BACKEND in the container:
kubectl get pod sampler -o jsonpath='{.metadata.annotations.fluence\.flux-framework\.org/backend}{"\n"}'
kubectl logs sampler
```
```console
2026/06/06 19:04:38 submitting sampler job to ibm_marrakesh
{"results": [ ... samples ... ]}
2026/06/06 19:04:50 done: 2070 bytes from ibm_marrakesh
```

When the pod completes, fluence cancels the Fluxion allocation, freeing the
resources (visible in the scheduler logs as `Cancel jobid: N`). Note that I lost access to
my IBM account so this has not been tested live since mid June.

Submission is **not** done by the scheduler — the workload container holds the
user's credentials and submits via qrmi-go. Fluence only schedules and hands off
the backend. (When we control local quantum devices this will change.)

### Notes

- **Deletion hangs.** A PodGroup can hang on delete via finalizers if the workload
  controller isn't running; clear them with the `kubectl patch` above.
- **State restore.** On restart the plugin repopulates the graph from each group's
  jobid annotations and re-holds the allocations: `make test-restore`.

## License

Distributed under the MIT license; all contributions must be made under it. 

See [LICENSE](LICENSE), [COPYRIGHT](COPYRIGHT), and [NOTICE](NOTICE).

SPDX-License-Identifier: MIT

LLNL-CODE-842614