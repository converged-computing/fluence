// Package spec holds the JSON patch type and pure pod-inspection / patch-op
// builders shared by the webhook core and its handlers. Everything here is a
// pure function of its inputs — no Kubernetes client, no Mutator — so both the
// core package and the handlers subpackage can import it without an import
// cycle.
package spec

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/converged-computing/fluence/pkg/placement"

	corev1 "k8s.io/api/core/v1"
)

// Op is a single RFC 6902 JSON patch operation.
type Op struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value,omitempty"`
	// EmitNull forces "value": null in the output (JSON null cannot be expressed
	// through Value because of omitempty). Used to clear spec.priority so the
	// priority admission controller recomputes it from priorityClassName.
	EmitNull bool `json:"-"`
}

// MarshalJSON honors EmitNull; otherwise it omits value when nil and includes it
// when present, matching the previous struct-tag behavior.
func (o Op) MarshalJSON() ([]byte, error) {
	m := map[string]any{"op": o.Op, "path": o.Path}
	if o.EmitNull {
		m["value"] = nil
	} else if o.Value != nil {
		m["value"] = o.Value
	}
	return json.Marshal(m)
}

// ── pod inspection ──────────────────────────────────────────────────────────────

// RequestsFluxionResource reports whether a container requests any
// fluxion.flux-framework.org/* resource.
func RequestsFluxionResource(c corev1.Container) bool {
	for name := range c.Resources.Requests {
		if strings.HasPrefix(string(name), placement.FluxionResourcePrefix) {
			return true
		}
	}
	return false
}

// PodRequestsFluxionResource reports whether any container requests a
// fluxion.flux-framework.org/* resource.
func PodRequestsFluxionResource(pod *corev1.Pod) bool {
	for _, c := range pod.Spec.Containers {
		if RequestsFluxionResource(c) {
			return true
		}
	}
	return false
}

// PodRequestsResource reports whether any container requests the named resource.
func PodRequestsResource(pod *corev1.Pod, name string) bool {
	for _, c := range pod.Spec.Containers {
		for rn := range c.Resources.Requests {
			if string(rn) == name {
				return true
			}
		}
	}
	return false
}

// HasEnv reports whether a container already has an env var of the given name.
func HasEnv(c corev1.Container, name string) bool {
	for _, e := range c.Env {
		if e.Name == name {
			return true
		}
	}
	return false
}

// Label returns the value of the given label, or "".
func Label(pod *corev1.Pod, key string) string {
	if pod.Labels == nil {
		return ""
	}
	return pod.Labels[key]
}

func Annotation(pod *corev1.Pod, key string) string {
	if pod.Annotations == nil {
		return ""
	}
	return pod.Annotations[key]
}

// ── env var builders ──────────────────────────────────────────────────────────

func AnnotationEnv(envName, annotationKey string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: envName,
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{
				FieldPath: fmt.Sprintf("metadata.annotations['%s']", annotationKey),
			},
		},
	}
}

func FieldEnv(envName, fieldPath string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: envName,
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: fieldPath},
		},
	}
}

// ── env injection ─────────────────────────────────────────────────────────────

// InjectEnvOps appends each env var to every container that requests a Fluxion
// resource (skipping ones already present), mutating pod in place to keep
// subsequent ops index-consistent.
func InjectEnvOps(pod *corev1.Pod, envs []corev1.EnvVar) []Op {
	var ops []Op
	for i, c := range pod.Spec.Containers {
		if !RequestsFluxionResource(c) {
			continue
		}
		for _, e := range envs {
			if HasEnv(c, e.Name) {
				continue
			}
			if len(pod.Spec.Containers[i].Env) == 0 {
				ops = append(ops, Op{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/env", i), Value: []corev1.EnvVar{e}})
			} else {
				ops = append(ops, Op{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/env/-", i), Value: e})
			}
			pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, e)
		}
	}
	return ops
}
