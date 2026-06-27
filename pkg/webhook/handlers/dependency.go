package handlers

import (
	"github.com/converged-computing/fluence/pkg/webhook/spec"

	corev1 "k8s.io/api/core/v1"
)

// Dependency is Fluence's GENERAL "this set of pods must wait for a producer to
// be ready" primitive. It is deliberately NOT quantum-specific: quantum is the
// first resource type to use it (a gang waits for a quantum submission to reach
// the device queue), but the same primitive applies to any resource type whose
// readiness is produced out-of-band — a license server, a data stage-in job, a
// warmed cache, another gang, etc.
//
// A Dependency has three parts, each carried as a pod annotation so the
// relationship lives at the GROUP level (not duplicated as bespoke per-resource
// fields) and is readable by both the webhook (at admission) and the scheduler
// (in its reconcile loop):
//
//   - Kind:     what KIND of readiness this is (the resource type's name). The
//     producer side knows how to satisfy this kind; the consumer side
//     only knows it must wait. Quantum's kind is "quantum-submit".
//   - Producer: the identity of the thing that will signal ready. For quantum it
//     is the submitter's (base) group; generally it is whatever the
//     kind's handler records as the satisfier.
//   - Gate:     the scheduling gate held on the dependent (consumer) pods until
//     the producer signals ready. Removing the gate is the "ungate"
//     and is performed by whatever observes the producer's readiness
//     (the quantum sidecar for kind=quantum-submit; the scheduler's
//     reconcile loop for kinds whose readiness is in-cluster, e.g.
//     "another gang is Running").
//
// The webhook PRODUCES a Dependency (gates the consumers, stamps the
// annotations); REMOVING the gate is owned by the observer best placed to see
// the producer's readiness. That split — declare here, observe elsewhere — is
// what keeps the primitive general: a new resource type adds a Kind and an
// observer and reuses the gating/annotation machinery unchanged.
type Dependency struct {
	Kind     string // resource-type readiness kind, e.g. "quantum-submit"
	Producer string // identity of the readiness producer (e.g. the base group)
	Gate     string // scheduling gate held on dependents until ready
}

// Dependency annotation keys (stamped on the dependent pods). Generic — no
// quantum in the names, so any resource type reuses them.
const (
	// DependsOnKindAnnotation names the readiness kind the dependent waits for.
	DependsOnKindAnnotation = "fluence.flux-framework.org/depends-on-kind"
	// DependsOnProducerAnnotation names the producer expected to signal ready.
	DependsOnProducerAnnotation = "fluence.flux-framework.org/depends-on-producer"
	// DependsOnGateAnnotation records which scheduling gate encodes the wait, so
	// an observer knows exactly which gate to remove when the producer is ready.
	DependsOnGateAnnotation = "fluence.flux-framework.org/depends-on-gate"
)

// applyOps gates the dependent pod and stamps the dependency annotations so the
// relationship is self-describing on the pod. It reuses the gate machinery
// (gateWithName) verbatim — the gate is the universal "held until ready"
// mechanism regardless of resource type — so a new Kind costs only its readiness
// observer, not new gating code.
func (d Dependency) applyOps(pod *corev1.Pod) []spec.Op {
	ops := gateWithName(pod, d.Gate)
	ops = append(ops, annotateOp(pod, DependsOnKindAnnotation, d.Kind)...)
	ops = append(ops, annotateOp(pod, DependsOnProducerAnnotation, d.Producer)...)
	ops = append(ops, annotateOp(pod, DependsOnGateAnnotation, d.Gate)...)
	return ops
}

// DependencyOf reads a dependent pod's declared Dependency, or ok=false if it
// carries none. The scheduler's reconcile loop and the sidecar use this to learn
// what a gated pod is waiting for without hardcoding a kind.
func DependencyOf(pod *corev1.Pod) (Dependency, bool) {
	kind := spec.Annotation(pod, DependsOnKindAnnotation)
	if kind == "" {
		return Dependency{}, false
	}
	return Dependency{
		Kind:     kind,
		Producer: spec.Annotation(pod, DependsOnProducerAnnotation),
		Gate:     spec.Annotation(pod, DependsOnGateAnnotation),
	}, true
}

// annotateOp adds a single metadata annotation (creating the annotations map if
// the pod has none). The key is JSON-Pointer-escaped so slashes are handled.
func annotateOp(pod *corev1.Pod, key, value string) []spec.Op {
	if value == "" {
		return nil
	}
	if pod.Annotations == nil {
		return []spec.Op{{
			Op:    "add",
			Path:  "/metadata/annotations",
			Value: map[string]string{key: value},
		}}
	}
	return []spec.Op{{
		Op:    "add",
		Path:  "/metadata/annotations/" + escapeJSONPointer(key),
		Value: value,
	}}
}

// gateWithName adds a named scheduling gate (idempotent) and raises priority for
// the held pod, generalizing the quantum gating to ANY gate name so the
// dependency primitive is not tied to the quantum gate.
func gateWithName(pod *corev1.Pod, gateName string) []spec.Op {
	for _, g := range pod.Spec.SchedulingGates {
		if g.Name == gateName {
			return nil
		}
	}
	var ops []spec.Op
	gate := corev1.PodSchedulingGate{Name: gateName}
	if len(pod.Spec.SchedulingGates) == 0 {
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/schedulingGates", Value: []corev1.PodSchedulingGate{gate}})
	} else {
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/schedulingGates/-", Value: gate})
	}
	// Gated dependents schedule reliably once ungated only if they outrank other
	// pending work; priorityClassName is immutable post-creation so it must be
	// set now. Don't override a user's explicit class. spec.priority is cleared
	// to null so the priority admission controller recomputes it from the class
	// (add-null is valid whether the field is absent, 0, or set).
	if pod.Spec.PriorityClassName == "" {
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/priorityClassName", Value: QuantumClassicalPriorityClass})
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/priority", EmitNull: true})
	}
	return ops
}
