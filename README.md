# fluence

![img/fluence.png](img/fluence.png)

A Kubernetes scheduler plugin that places **pod groups** (and individual pods)
by matching them against a [Fluxion](https://github.com/flux-framework/flux-sched)
(flux-sched) resource graph built from the live cluster.

This is an update from [flux-k8s](https://github.com/flux-framework/flux-k8s)
that uses the native PodGroup and optionally allows for scheduling
against arbitrary resources such as **quantum resources** modeled in the same graph. 
I am also improving the design by not requiring a sidecar for fluence, and not
requiring the `kubernetes-sigs/scheduler-plugins` dependency. We use native Gang
scheduling provided by Kubernetes. 

For quantum resource modeling, we start from the prototype proven out in
[fluxion-quantum](https://github.com/converged-computing/fluxion-quantum).

## How it works

### Gang Scheduling

Gang semantics (all-or-nothing) come from the native `PodGroup` API. Fluence is
responsible only for **placement**:

1. **Discover** — on startup fluence lists cluster nodes and turns their
   cpu/memory/gpu capacity into a Fluxion JGF resource graph
   (`pkg/cluster` + `pkg/jgf`). If a resources config is provided (via
   `FLUENCE_RESOURCES`), its entries (e.g. quantum backends) are injected as
   `qpu`/`qubit` vertices. With no config the graph is classical-only.
2. **Match** — when the first pod of a group hits `PreFilter`, fluence builds a
   Fluxion jobspec for the whole gang (`pkg/placement.JobspecForGroup`), asks the
   matcher to allocate (`pkg/graph.FluxionGraph.MatchAllocateSpec`), and parses
   the allocation into node and backend names (`PlacementFromAllocation`).
3. **Place** — `Filter` permits each pod only on its allocated node. (A
   quantum-only pod allocates a `qpu` but no node — the backend is a remote API
   any node can reach — so fluence imposes no node constraint in that case.)
4. **Hand off** — for a quantum pod, `PreBind` records the allocated backend on
   the pod as the `fluence.flux-framework.org/backend` annotation. The mutating
   webhook (installed with the base) injects a downward-API env so the container
   reads it as `QRMI_BACKEND` with no boilerplate in the manifest.

### Design Choices

While Quantum resources are this first target, notably we should be able to support
any arbitrary resource in the graph. I decided that a pod can request a graph resource generically
e.g., `fluxion.flux-framework.org/<type>` (like `.../qpu: "1"`) and that becomes a jobspec count
of `<type>`. To support this, we deploy a **device plugin** that can advertise these virtual 
types on every node. We need to do this because of the in-tree `NodeResourcesFit` endpoint. 
If we do not have the device plugin, this call will not be satisfied. Note that
this device plugin will return True for any resources it sees added to the Fluxion resource graph,
but is not actually involved with scheduling. Fluxion does the real matching.

```console
nodes (kubectl get nodes) ──┐
                            ├─► JGF resource graph ─► Fluxion match ─► node + backend placement
fluence-resources ConfigMap ┘
```

I am also choosing to keep credentials and qrmi interactions on the level of the application.
I am not comfortable with the design of an operator holding any kind of credential or being
responsible for managing calls with qrmi in a multi-tenant environment. Finally, since
there are (and will continue to be) a lot of environment variables that I do not want 
to place on the user to define, we have a webhook to handle this. We can combine an annotation
added with the webhook with a PreBind call to define the annotation to orchestrate that.

## Build

The scheduler binary links flux-sched (the matcher). It does **not** link QRMI —
quantum job submission lives in a separate workload container
([qrmi-sampler](https://github.com/converged-computing/qrmi-sampler)), not here.

```bash
# Inside the .devcontainer (flux-sched at /opt/flux-sched):
# builds bin/fluence (cgo+flux) + bin/fluence-deviceplugin + bin/fluence-webhook
make build      
make test

# Or build the container image (all three binaries):
make image
```

## Deploy

Create a development cluster on a Kubernetes release that supports native gang
scheduling, with the feature gates enabled:

```bash
kind create cluster --image kindest/node:v1.36.1 --config deploy/kind-config.yaml
```

(See [installing kind](https://kind.sigs.k8s.io/docs/user/quick-start#installing-from-release-binaries).)
The kind config turns on the `GangScheduling` and `GenericWorkload` feature gates
and the `scheduling.k8s.io/v1alpha2` API group on the apiserver and scheduler. In
the future these will likely be enabled by default. 

Load the image (built above) into the cluster:

```bash
kind load docker-image ghcr.io/converged-computing/fluence:latest
```

### 1. Gang Scheduling

Install the **base** scheduler (this is all you need for classical scheduling —
no device plugin, no quantum):

```bash
kubectl apply -f deploy/fluence.yaml
```

This installs the scheduler, its RBAC, and the mutating webhook. Pods opt in with
`schedulerName: fluence`; a multi-pod gang adds a `scheduling.k8s.io/pod-group`
label (a single pod is treated as a group of one and needs no label).

## Testing

### 1. Classical (a pod group)

The base install is enough. Schedule a gang:

```bash
kubectl apply -f examples/podgroup.yaml
kubectl get pods -o wide  
kubectl get events --field-selector reason=Scheduled
kubectl get podgroups.scheduling.k8s.io
```
```console
NAME       POLICY   WORKLOAD   STATUS      AGE
training   Gang     <none>     Scheduled   15s
```

And cleanup.

```bash
kubectl patch podgroup training -n default --type=merge -p '{"metadata":{"finalizers":null}}'
kubectl delete -f examples/podgroup.yaml
```

### 2. Quantum

Quantum needs the resources add-on, which supplies the `fluence-resources`
ConfigMap (the single source of truth for which backends exist) **and** the
device plugin that advertises them:

```bash
kubectl apply -f deploy/fluence-resources.yaml
# The scheduler reads its resources config at startup, so restart it to pick up
# the quantum vertices:
kubectl rollout restart deployment/fluence -n kube-system
```

Confirm the device plugin advertised the resources on the nodes:

```bash
kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.allocatable}{"\n"}{end}' \
  | grep fluxion.flux-framework.org
```
```console
kind-control-plane	{"cpu":"16","ephemeral-storage":"982292956Ki","fluxion.flux-framework.org/qpu":"1k","fluxion.flux-framework.org/qubit":"1k","hugepages-1Gi":"0","hugepages-2Mi":"0","memory":"61400748Ki","pods":"110"}
kind-worker	{"cpu":"16","ephemeral-storage":"982292956Ki","fluxion.flux-framework.org/qpu":"1k","fluxion.flux-framework.org/qubit":"1k","hugepages-1Gi":"0","hugepages-2Mi":"0","memory":"61400748Ki","pods":"110"}
kind-worker2	{"cpu":"16","ephemeral-storage":"982292956Ki","fluxion.flux-framework.org/qpu":"1k","fluxion.flux-framework.org/qubit":"1k","hugepages-1Gi":"0","hugepages-2Mi":"0","memory":"61400748Ki","pods":"110"}
```

Create the IBM credentials the **workload** uses to submit (in the namespace
where the workload runs — the scheduler itself never needs them):

```bash
# If you don't have this yet
curl -fsSL https://clis.cloud.ibm.com/install/linux | sudo sh
ibmcloud login --apikey <key>
# 12 for us-east
```
```bash
export IBM_CLOUD_TOKEN=<key>
export IBM_CLOUD_CRN=$(ibmcloud resource service-instances --service-name quantum-computing --output json | jq -r '.[] | {name: .name, crn: .crn}' | jq -r .crn)
```

```bash
kubectl create secret generic ibm-quantum -n default --from-literal=token="$IBM_CLOUD_TOKEN" --from-literal=crn="$IBM_CLOUD_CRN"
```

Run a single quantum pod. It just requests `fluxion.flux-framework.org/qpu` — no
group, and no hard-coded backend (the webhook + PreBind supply `QRMI_BACKEND`):

```bash
kubectl apply -f examples/quantum-pod.yaml
kubectl get pod sampler -o wide

# fluence's chosen backend, injected as an environment variable:
kubectl get pod sampler -o jsonpath='{.metadata.annotations.fluence\.flux-framework\.org/backend}{"\n"}'
kubectl logs sampler
```
```console
kubectl logs sampler -f
2026/06/06 19:04:38 submitting sampler job to ibm_marrakesh
{"results": [{"data": {"c": {"samples": ["0x0", "0x1", "0x0", "0x0", "0x1", "0x1", "0x1", "0x1", "0x1", "0x0", "0x0", "0x0", "0x0", "0x1", "0x1", "0x1", "0x1", "0x1", "0x1", "0x1", "0x0", "0x1", "0x0", "0x0", "0x0", "0x1", "0x0", "0x1", "0x0", "0x1", "0x1", "0x0", "0x0", "0x0", "0x0", "0x1", "0x1", "0x1", "0x1", "0x1", "0x1", "0x1", "0x0", "0x0", "0x0", "0x1", "0x0", "0x1", "0x0", "0x0", "0x1", "0x1", "0x1", "0x0", "0x0", "0x0", "0x1", "0x1", "0x1", "0x0", "0x1", "0x1", "0x0", "0x1", "0x0", "0x1", "0x1", "0x1", "0x1", "0x0", "0x0", "0x1", "0x1", "0x1", "0x1", "0x1", "0x1", "0x1", "0x0", "0x0", "0x1", "0x0", "0x1", "0x1", "0x0", "0x1", "0x0", "0x1", "0x1", "0x1", "0x0", "0x0", "0x0", "0x1", "0x1", "0x1", "0x1", "0x0", "0x0", "0x1", "0x0", "0x0", "0x0", "0x0", "0x0", "0x0", "0x1", "0x1", "0x0", "0x1", "0x0", "0x0", "0x1", "0x1", "0x1", "0x1", "0x1", "0x0", "0x1", "0x0", "0x0", "0x0", "0x1", "0x1", "0x1", "0x0", "0x1", "0x1", "0x0", "0x0", "0x1", "0x0", "0x1", "0x0", "0x0", "0x1", "0x0", "0x0", "0x1", "0x1", "0x1", "0x0", "0x1", "0x1", "0x0", "0x0", "0x0", "0x1", "0x1", "0x1", "0x0", "0x1", "0x1", "0x0", "0x0", "0x1", "0x0", "0x1", "0x0", "0x0", "0x0", "0x0", "0x1", "0x1", "0x1", "0x1", "0x1", "0x1", "0x1", "0x0", "0x1", "0x1", "0x1", "0x0", "0x1", "0x1", "0x0", "0x0", "0x0", "0x0", "0x1", "0x0", "0x0", "0x0", "0x0", "0x1", "0x0", "0x0", "0x1", "0x1", "0x1", "0x0", "0x1", "0x0", "0x1", "0x0", "0x1", "0x1", "0x1", "0x1", "0x0", "0x0", "0x1", "0x1", "0x0", "0x1", "0x0", "0x0", "0x0", "0x1", "0x1", "0x1", "0x0", "0x0", "0x0", "0x1", "0x0", "0x0", "0x0", "0x1", "0x0", "0x1", "0x0", "0x0", "0x0", "0x1", "0x1", "0x0", "0x1", "0x0", "0x0", "0x0", "0x1", "0x0", "0x0", "0x1", "0x1", "0x0", "0x0", "0x0", "0x0", "0x0", "0x1", "0x1", "0x1", "0x0", "0x1", "0x1", "0x1", "0x1", "0x1", "0x1", "0x0", "0x0", "0x0", "0x0"], "num_bits": 1}}, "metadata": {"circuit_metadata": {}}}], "metadata": {"execution": {"execution_spans": [[{"date": "2026-06-06T19:04:43.221657"}, {"date": "2026-06-06T19:04:44.372421"}, {"0": [[256], [0, 1], [0, 256]]}]]}, "version": 2}}
2026/06/06 19:04:50 done: 2070 bytes from ibm_marrakesh
```
Boum! You will see in the fluence logs that when the pod completes, the fluxion job is cancelled, freeing the resources.

```bash
kubectl logs -n kube-system fluence-75d6848778-g4lh6 
...
I0610 18:33:05.843325       1 eventhandlers.go:443] "Delete event for scheduled pod" pod="default/sampler"
   🌀 Cancel jobid: 1
(env) (base) vanessa@vanessa-ThinkPad-P14s-Gen-4:~/Desktop/Code/fluence$ kubectl get pods
NAME      READY   STATUS      RESTARTS   AGE
sampler   0/1     Completed   0          24s
```

### A note on deletion

When developing/debugging, a PodGroup (or its pods) can hang on delete because of
finalizers (the workload controller may not be running). Clear them with:

```bash
kubectl patch podgroup training -n default --type=merge -p '{"metadata":{"finalizers":null}}'
```

Importantly, submission is **not** done by the scheduler — the workload container holds the
user's credentials and submits via qrmi-go (job mode on the IBM open plan; see
fluxion-quantum for that story). Fluence only schedules and hands off the backend.
When we actually have control of local quantum devices this will be different.

## License

HPCIC DevTools is distributed under the terms of the MIT license.
All new contributions must be made under this license.

See [LICENSE](LICENSE), [COPYRIGHT](COPYRIGHT), and [NOTICE](NOTICE) for details.

SPDX-License-Identifier: MIT

LLNL-CODE-842614
