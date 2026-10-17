# Quantum and classical scheduling in Fluence

## The problem

A hybrid quantum and classical job submits to two queues, the Kubernetes
scheduler for the classical pods and the vendor queue for the quantum task. The
classical pods start right away and then sit on their nodes waiting for a result
that may be minutes or hours away. That waste grows with concurrency. On real
QPUs we measured 15 to 30 percent classical idle time at ten concurrent jobs and
over 70 percent for single pods at twenty.

Fluence holds the classical pods out of the cluster until the quantum task is
about to run, then releases them with high priority so they start as the result
lands. I think this beats having the pods run and wait, even when the task
ahead in the queue turns out to be slow. Better vendor estimates would help
everyone. This only matters when a gang of pods is waiting on one quantum task,
or a loop keeps resubmitting one.

## The design

A shared quantum gang is split into one scout and N minus one workers.

- The scout is one of the N pods. It runs the application, submits the one real
  quantum task, and is never gated. It carries a sidecar.
- The workers are the other pods. They start with a scheduling gate, so they
  hold no node resources while the task waits in the vendor queue. They are
  told their role and, once ungated, the job id and the session.
- The sidecar runs in the scout pod. It finds the task the scout submitted,
  polls the vendor queue, and when the task is next in line it ungates the
  workers. It then waits for the workers to finish, so the scout keeps its
  session until they are done.
- An interceptor is staged into the scout container. It patches the vendor SDK
  so the sidecar can find the task. For Braket it tags the task with the pod
  uid. For QRMI, which has no tag search, it writes the job id and the session
  to a small file the sidecar reads.

The workers are released with a high priority class, so if the cluster filled
up while they waited they preempt lower priority work rather than queue behind
it. This is what makes the hold useful. Fluence frees the preempted pods' graph
allocations as they go.

## What fluence looks at

Two properties of a pod with schedulerName fluence decide what happens.

- Does any container request a quantum resource, fluxion.flux-framework.org/qpu?
  Then there is a vendor backend behind the pod and fluence places it against
  the graph.
- Does the pod carry the group label fluence.flux-framework.org/group? Then it
  is part of a gang, scheduled through the native PodGroup API in Kubernetes
  1.36 and later.

A quantum gang also has a coordination mode, the annotation
fluence.flux-framework.org/coordination. The default is independent, every pod
does its own quantum work. In shared mode the gang shares one task and gets the
scout and worker split. The quantum resource request is what triggers the
interceptor and sidecar. The group label only controls gang scheduling and the
split.

## What the user writes

```yaml
metadata:
  labels:
    fluence.flux-framework.org/group: my-workflow
  annotations:
    fluence.flux-framework.org/coordination: shared
spec:
  schedulerName: fluence
```

That is all. The scout is the pod with completion index 0 in an indexed Job,
which the Job controller stamps, so an identical template yields the roles
with no state and no race. I started out requiring a PodGroup object and found
it annoying, so the webhook creates it from the label.

The two prefixes are deliberate. fluence.flux-framework.org is the scheduler
plugin, the group, the mode, the gate. fluxion.flux-framework.org is the
resource graph, the extended resources and the backend attributes.

## The webhook

The webhook core knows nothing about quantum. It dispatches to handlers that
register themselves, and a pod flows through every handler that applies. There
are three.

- fluxion applies to any pod requesting a fluxion resource and injects the
  FLUXION_* environment from the annotations the scheduler writes.
- gang applies to grouped pods that request no quantum resource. It creates the
  PodGroup and links the pod to it.
- quantum applies to any pod requesting the quantum resource. A standalone or
  independent pod gets the interceptor and submits for itself. In shared mode
  the scout gets its own PodGroup of one, named group-scout, the interceptor,
  the handoff volume, and the sidecar. A worker joins the gang PodGroup of
  N minus one, is gated, gets the high priority class, and is given its role
  plus downward API variables for the job id and the session, read from
  annotations the sidecar stamps at ungate.

The interceptor is delivered by an init container that copies the fluence
package into a shared volume on the application container's PYTHONPATH. Python
imports sitecustomize on every start, so the interceptor loads before user code
and patches whichever vendor SDK is present. No user install and no change to
the user image.

The quantum handler also enforces admission control when it is turned on. See
pkg/webhook/admission.go. A quantum group is only admitted when the cores all
of its pods will need fit in what is reachable, free cores plus cores held by
preemptible pods, less what is promised to groups that have not started.

## The sidecar

1. Resolve the provider from the FLUXION_* environment.
2. Find the task. Braket searches by the pod uid tag. QRMI reads the handoff file.
3. Find the workers, the pods in the group that still carry the gate.
4. Poll the task. Log the position for experiments.
5. When the task is next in line, or already running, annotate each worker
   with the job id and the session, then remove the gate.
6. Wait for the workers to reach Succeeded or Failed, write the done marker so
   the scout's release proceeds, and exit.

If the task is never found or fails, the sidecar ungates anyway so the workers
are not stranded, and exits non zero so it shows.

## Priority and preemption

Workers get the fluence-quantum-classical priority class, value one million,
at creation, since priorityClassName cannot change after a pod exists. The
gate keeps the priority from mattering until ungate. Once ungated, Kubernetes
preemption evicts lower priority pods to make room, and fluence's pod deletion
handler cancels their Fluxion allocations so the graph is free for the workers.

Because each gang is released when its own task is next in line, classical
resources follow quantum execution order rather than submission order. A gang
on a quiet backend gets its nodes before an earlier gang on a busy one.

## Providers

The webhook cannot know the vendor at admission, since the scheduler picks the
backend later. So the interceptor is staged into every quantum container and
asks every provider to install its hook, each skipping if its SDK is absent.
The sidecar resolves the one provider that matches at runtime.

A provider implements both halves in one module under
python/fluence/providers and calls register at import.

```
install_interceptor(pod_uid)   patch the SDK submit call
matches(attrs)                 does this provider own this backend
find_my_task(pod_uid, ...)     the task, as an opaque object
is_ready_to_ungate(task)       next in line or already running
queue_position(task)           optional, for telemetry
job_id(task)                   the id handed to workers
session_id(task)               the session handed to workers, if any
```

Braket and QRMI are implemented. QRMI covers IBM, Pasqal, IQM, and others
through one API, with the vendor chosen by the qrmi_type attribute.

A single quantum pod with no workers gets the interceptor and environment but
no sidecar. The label fluence.flux-framework.org/observe set to true adds the
sidecar in observe mode, which logs the queue position and ungates nothing.
Experiments use that to measure singletons and gangs the same way.

## Properties

| Property | Value |
|---|---|
| User code changes | none |
| User manifest changes | the group label, the mode, and schedulerName |
| Classical resources during the wait | none, the workers are gated |
| Queue time estimate needed | no, next in line is observable |
| Heterogeneous backends | yes, anything in the graph |
| Vendors | Braket and everything QRMI reaches |

## Limitations

- Ungating preempts lower priority work. That work requeues and runs later,
  but it is a disruption. A Fluxion reservation at a vendor supplied start
  time would allow draining instead, and no vendor supplies one today.
- One task per gang. Parameter shift gradients and other multi circuit loops
  need a set of tasks. See the scatter design below.
- The sidecar RBAC is created per namespace on first use. A chart or operator
  would manage it more cleanly.

## Future work

- A MatchReserveAt primitive in the Fluxion bindings, fed by the live queue
  position, to drain nodes rather than preempt.
- A scatter design, where N independent tasks each pair with one worker and the
  sidecar ungates each worker as its own task comes up.
- Vendor task start events in place of polling.
- Topology aware preemption in PostFilter, so eviction targets exactly the pods
  blocking a worker.
