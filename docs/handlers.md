# Webhook handlers & sidecar architecture

Fluence's value is not creating gangs (Kubernetes 1.36 native gang scheduling
already does that). It is **customizing the gang on the fly based on the
resources a pod requests** — e.g. a quantum leader/worker workload becomes a
size-1 leader gang plus a size-(N-1) worker gang, with the leader running a
sidecar that ungates its workers when the quantum task is ready.

## Handlers

Each handler is an interface implementation (`pkg/webhook/handler.go`):

```go
type Handler interface {
    Name() string
    Applies(ctx, m MutatorAPI, pod) bool
    Mutate(ctx, m MutatorAPI, pod) []spec.Op
}
```

Handlers self-register by name (`init()` -> `webhook.Register`); a blank import
of the handlers package makes them AVAILABLE. The core never names a handler.

**Ordering = the active list.** There is no per-handler priority. The active
handler list is BOTH the selection and the dispatch order:

```go
var DefaultHandlerOrder = []string{"fluxion", "quantum", "gang"}
```

Dispatch walks this list in order. `gang` is last because it is last in the
list — the fallback that applies common defaults (honor `group-size`, else
owner-derived N) only if no earlier handler already shaped the gang. A
custom-resource handler is inserted into the list before `gang` to shape its own
gang first. To change the order, or disable a handler, pass a different list.

## Enabling/disabling handlers

By default ALL registered handlers are enabled. Restrict the active set on the
webhook command:

```
fluence-webhook --handlers=fluxion,gang        # run without quantum
FLUENCE_HANDLERS=fluxion,quantum,gang fluence-webhook
```

Empty = the default list. The list is the order: `--handlers=gang,fluxion` runs
gang first; omitting a name disables it. Unknown names are warned and dropped.

(The handler set lives in the WEBHOOK, which mutates pods. `cmd/fluence` is the
scheduler plugin and runs no handlers.)

## Sidecar interface

The coordination sidecar is a handler-owned capability, not a core one. Handlers
that need a sidecar use `handlers.Sidecar`:

```go
type Sidecar interface {
    EnsureRBAC(ctx, namespace)
    InterceptorOps(pod) []spec.Op
    ContainerOps(pod, observe bool) []spec.Op
}
```

The default `coreSidecar` delegates to the core's staging primitives. The quantum
handler uses it today; a custom handler can supply its own implementation
(different image, env, gating) without touching the core or other handlers. The
core's `MutatorAPI` keeps the staging primitives only so the default
implementation can delegate — handlers do not call them directly.

## Group size resolution (the default gang handler)

`minCount` (the atomic-schedule count) resolves as:

1. explicit `fluence.flux-framework.org/group-size` annotation — honored verbatim
   (the override; e.g. a quantum split sets it directly);
2. else the owning indexed Job's `parallelism` (== MiniCluster size N);
3. else 1, logged.

This is a common default available to every gang; handler-specific annotations
(quantum role, expected-workers, etc.) live in their handlers and are not
required by the core.
