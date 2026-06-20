// Package webhook is fluence's mutating admission webhook.
//
// Rules:
//
//  1. For a pod scheduled by fluence whose container requests a
//     fluxion.flux-framework.org/* resource, inject FLUXION_* env vars
//     sourced from annotations the scheduler writes in PreBind.
//
//  2. Quantum leader/worker split:
//     Pods with label fluence.flux-framework.org/group=<name> and
//     schedulerName=fluence trigger the split. The first pod admitted
//     becomes the leader — Fluence creates a PodGroup (minCount:1),
//     injects the sidecar, creates per-namespace RBAC, and records the
//     leader on the PodGroup. Every subsequent pod in the same group
//     gets a quantum.braket/ready scheduling gate added.
//
// The webhook self-manages TLS via a self-signed CA patched into the
// MutatingWebhookConfiguration caBundle at startup.
package webhook

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/converged-computing/fluence/pkg/placement"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	schedulingv1alpha2 "k8s.io/api/scheduling/v1alpha2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// ── Constants ──────────────────────────────────────────────────────────────────

const (
	SchedulerName                 = "fluence"
	QuantumGroupLabel             = "fluence.flux-framework.org/group"
	QuantumLeaderAnnotation       = "fluence.flux-framework.org/quantum-leader"
	QuantumGateName               = "quantum.braket/ready"
	QuantumClassicalPriorityClass = "fluence-quantum-classical"
	SidecarImage                  = "ghcr.io/converged-computing/fluence-sidecar:latest"
	SidecarServiceAccount         = "fluence-sidecar"
	InterceptorConfigMap          = "fluence-interceptor"
	InterceptorVolumeName         = "fluence-interceptor"
	InterceptorMountPath          = "/etc/fluence/fluence_intercept.py"

	// QuantumResourceName is the specific Fluxion resource a pod requests when
	// it wants Fluence to schedule quantum work. It is the trigger for the
	// quantum handler (sidecar + interceptor injection). Distinct from the
	// generic FluxionResourcePrefix, which marks any Fluxion-graph resource.
	QuantumResourceName = "fluxion.flux-framework.org/qpu"

	// ObserveLabel opts a quantum pod into observe-only telemetry: the sidecar
	// is injected and polls queue position but ungates nothing. Used by
	// experiments to get a uniform queue-position measurement path.
	ObserveLabel = "fluence.flux-framework.org/observe"
)

// ── Types ──────────────────────────────────────────────────────────────────────

type jsonPatchOp struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value,omitempty"`
}

type Mutator struct {
	AttributeKeys []string
	Client        kubernetes.Interface
	SidecarImage  string
	// Handlers overrides the default handler set; nil means defaultHandlers().
	Handlers []ResourceHandler
}

// ── Helpers ────────────────────────────────────────────────────────────────────

func (m *Mutator) sidecarImage() string {
	if m.SidecarImage != "" {
		return m.SidecarImage
	}
	return SidecarImage
}

// groupName returns the value of QuantumGroupLabel on the pod, or "".
func groupName(pod *corev1.Pod) string {
	if pod.Labels == nil {
		return ""
	}
	return pod.Labels[QuantumGroupLabel]
}

func annotationEnv(envName, annotationKey string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: envName,
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{
				FieldPath: fmt.Sprintf("metadata.annotations['%s']", annotationKey),
			},
		},
	}
}

func fieldEnv(envName, fieldPath string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: envName,
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: fieldPath},
		},
	}
}

func requestsFluxionResource(c corev1.Container) bool {
	for name := range c.Resources.Requests {
		if strings.HasPrefix(string(name), placement.FluxionResourcePrefix) {
			return true
		}
	}
	return false
}

// podRequestsFluxionResource reports whether any container requests a
// fluxion.flux-framework.org/* resource.
func podRequestsFluxionResource(pod *corev1.Pod) bool {
	for _, c := range pod.Spec.Containers {
		if requestsFluxionResource(c) {
			return true
		}
	}
	return false
}

// podRequestsQuantumResource reports whether any container requests the quantum
// resource specifically. This is the trigger for the quantum handler.
func podRequestsQuantumResource(pod *corev1.Pod) bool {
	for _, c := range pod.Spec.Containers {
		for name := range c.Resources.Requests {
			if string(name) == QuantumResourceName {
				return true
			}
		}
	}
	return false
}

func hasEnv(c corev1.Container, name string) bool {
	for _, e := range c.Env {
		if e.Name == name {
			return true
		}
	}
	return false
}

func resourceQuantity(s string) *resource.Quantity {
	q := resource.MustParse(s)
	return &q
}

// ── Env contract ───────────────────────────────────────────────────────────────

func (m *Mutator) injectedEnv() []corev1.EnvVar {
	envs := []corev1.EnvVar{annotationEnv(
		placement.EnvVarPrefix+"BACKEND", placement.BackendAnnotation)}
	for _, key := range m.AttributeKeys {
		envs = append(envs, annotationEnv(
			placement.EnvVarName(key), placement.AttributeAnnotationPrefix+key))
	}
	return envs
}

func (m *Mutator) EnvVarNames() []string {
	names := make([]string, 0, len(m.AttributeKeys)+1)
	for _, e := range m.injectedEnv() {
		names = append(names, e.Name)
	}
	return names
}

// ── PodGroup management ────────────────────────────────────────────────────────

func (m *Mutator) podGroupLeader(ctx context.Context, pod *corev1.Pod) string {
	if m.Client == nil {
		return ""
	}
	g := groupName(pod)
	if g == "" {
		return ""
	}
	// Retry briefly — the leader pod may have just created the PodGroup and
	// is recording itself; the worker pod admission may fire concurrently.
	for i := 0; i < 3; i++ {
		pg, err := m.Client.SchedulingV1alpha2().PodGroups(pod.Namespace).Get(
			ctx, g, metav1.GetOptions{})
		if err != nil {
			return ""
		}
		if pg.Annotations != nil && pg.Annotations[QuantumLeaderAnnotation] != "" {
			return pg.Annotations[QuantumLeaderAnnotation]
		}
		if i < 2 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	return ""
}

func (m *Mutator) ensureQuantumPodGroup(ctx context.Context, pod *corev1.Pod, g string) {
	if m.Client == nil {
		return
	}
	if _, err := m.Client.SchedulingV1alpha2().PodGroups(pod.Namespace).Get(
		ctx, g, metav1.GetOptions{}); err == nil {
		return
	}
	pg := &schedulingv1alpha2.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      g,
			Namespace: pod.Namespace,
			Labels:    map[string]string{"app": "fluence", QuantumGroupLabel: g},
		},
		Spec: schedulingv1alpha2.PodGroupSpec{
			SchedulingPolicy: schedulingv1alpha2.PodGroupSchedulingPolicy{
				Gang: &schedulingv1alpha2.GangSchedulingPolicy{MinCount: 1},
			},
		},
	}
	if _, err := m.Client.SchedulingV1alpha2().PodGroups(pod.Namespace).Create(
		ctx, pg, metav1.CreateOptions{}); err != nil {
		log.Printf("[fluence-webhook] could not create PodGroup %s/%s: %v", pod.Namespace, g, err)
	} else {
		log.Printf("[fluence-webhook] created PodGroup %s/%s (minCount=1)", pod.Namespace, g)
	}
}

func (m *Mutator) recordLeader(ctx context.Context, pod *corev1.Pod) {
	if m.Client == nil {
		return
	}
	g := groupName(pod)
	if g == "" {
		return
	}
	patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, QuantumLeaderAnnotation, pod.Name)
	if _, err := m.Client.SchedulingV1alpha2().PodGroups(pod.Namespace).Patch(
		ctx, g, types.MergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
		log.Printf("[fluence-webhook] could not record leader on PodGroup %s/%s: %v", pod.Namespace, g, err)
	}
}

// ── Per-namespace resource provisioning ───────────────────────────────────────

func (m *Mutator) ensureSidecarRBAC(ctx context.Context, namespace string) {
	if m.Client == nil {
		return
	}

	if _, err := m.Client.CoreV1().ServiceAccounts(namespace).Get(
		ctx, SidecarServiceAccount, metav1.GetOptions{}); err != nil {
		sa := &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{Name: SidecarServiceAccount, Namespace: namespace,
				Labels: map[string]string{"app": "fluence-sidecar"}},
		}
		if _, err := m.Client.CoreV1().ServiceAccounts(namespace).Create(ctx, sa, metav1.CreateOptions{}); err != nil {
			log.Printf("[fluence-webhook] could not create ServiceAccount %s/%s: %v", namespace, SidecarServiceAccount, err)
		} else {
			log.Printf("[fluence-webhook] created ServiceAccount %s/%s", namespace, SidecarServiceAccount)
		}
	}

	if _, err := m.Client.RbacV1().Roles(namespace).Get(
		ctx, SidecarServiceAccount, metav1.GetOptions{}); err != nil {
		role := &rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: SidecarServiceAccount, Namespace: namespace,
				Labels: map[string]string{"app": "fluence-sidecar"}},
			Rules: []rbacv1.PolicyRule{
				{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "list", "patch", "update"}},
				{APIGroups: []string{"scheduling.k8s.io"}, Resources: []string{"podgroups"}, Verbs: []string{"get", "list"}},
			},
		}
		if _, err := m.Client.RbacV1().Roles(namespace).Create(ctx, role, metav1.CreateOptions{}); err != nil {
			log.Printf("[fluence-webhook] could not create Role %s/%s: %v", namespace, SidecarServiceAccount, err)
		} else {
			log.Printf("[fluence-webhook] created Role %s/%s", namespace, SidecarServiceAccount)
		}
	}

	if _, err := m.Client.RbacV1().RoleBindings(namespace).Get(
		ctx, SidecarServiceAccount, metav1.GetOptions{}); err != nil {
		rb := &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: SidecarServiceAccount, Namespace: namespace,
				Labels: map[string]string{"app": "fluence-sidecar"}},
			Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: SidecarServiceAccount, Namespace: namespace}},
			RoleRef:  rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: SidecarServiceAccount},
		}
		if _, err := m.Client.RbacV1().RoleBindings(namespace).Create(ctx, rb, metav1.CreateOptions{}); err != nil {
			log.Printf("[fluence-webhook] could not create RoleBinding %s/%s: %v", namespace, SidecarServiceAccount, err)
		} else {
			log.Printf("[fluence-webhook] created RoleBinding %s/%s", namespace, SidecarServiceAccount)
		}
	}

	// Copy interceptor ConfigMap from kube-system into the pod namespace
	if _, err := m.Client.CoreV1().ConfigMaps(namespace).Get(
		ctx, InterceptorConfigMap, metav1.GetOptions{}); err != nil {
		if src, srcErr := m.Client.CoreV1().ConfigMaps("kube-system").Get(
			ctx, InterceptorConfigMap, metav1.GetOptions{}); srcErr != nil {
			log.Printf("[fluence-webhook] could not read interceptor ConfigMap from kube-system: %v", srcErr)
		} else {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: InterceptorConfigMap, Namespace: namespace,
					Labels: map[string]string{"app": "fluence-sidecar"}},
				Data: src.Data,
			}
			if _, err := m.Client.CoreV1().ConfigMaps(namespace).Create(ctx, cm, metav1.CreateOptions{}); err != nil {
				log.Printf("[fluence-webhook] could not create interceptor ConfigMap %s/%s: %v", namespace, InterceptorConfigMap, err)
			} else {
				log.Printf("[fluence-webhook] created interceptor ConfigMap %s/%s", namespace, InterceptorConfigMap)
			}
		}
	}
}

// ── Patch operation builders ───────────────────────────────────────────────────

// schedulingGroupOps links a pod to its PodGroup via the native 1.36 field
// spec.schedulingGroup.podGroupName — the field the Fluence scheduler plugin
// reads (placement.PodGroupName) to gang the group. Without this, the scheduler
// sees each pod as its own group of one and never gangs them. The user only
// sets the group LABEL; the webhook translates that into the spec field so the
// user never has to know the PodGroup exists.
//
// Applied to BOTH leader and workers. A gated worker is held at PreEnqueue, so
// the scheduler does not run PreFilter for it (and groupPods excludes it) until
// the sidecar removes its gate — at which point this linkage takes effect.
func schedulingGroupOps(pod *corev1.Pod, group string) []jsonPatchOp {
	if pod.Spec.SchedulingGroup != nil && pod.Spec.SchedulingGroup.PodGroupName != nil &&
		*pod.Spec.SchedulingGroup.PodGroupName == group {
		return nil // already linked
	}
	return []jsonPatchOp{{
		Op:    "add",
		Path:  "/spec/schedulingGroup",
		Value: map[string]string{"podGroupName": group},
	}}
}

func quantumWorkerGateOps(pod *corev1.Pod) []jsonPatchOp {
	for _, g := range pod.Spec.SchedulingGates {
		if g.Name == QuantumGateName {
			return nil
		}
	}
	gate := corev1.PodSchedulingGate{Name: QuantumGateName}
	if len(pod.Spec.SchedulingGates) == 0 {
		return []jsonPatchOp{{Op: "add", Path: "/spec/schedulingGates", Value: []corev1.PodSchedulingGate{gate}}}
	}
	return []jsonPatchOp{{Op: "add", Path: "/spec/schedulingGates/-", Value: gate}}
}

// interceptorOps mounts the all-providers interceptor file into the quantum-
// submitting container(s) and sets PYTHONSTARTUP so it runs before user code.
// This is what tags the submitted task with the pod uid for sidecar discovery.
// It does NOT add the sidecar container — a pod can be tagged without being
// coordinated (e.g. a standalone quantum pod with nothing to ungate).
func (m *Mutator) interceptorOps(pod *corev1.Pod) []jsonPatchOp {
	var ops []jsonPatchOp

	vol := corev1.Volume{
		Name: InterceptorVolumeName,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: InterceptorConfigMap},
			},
		},
	}
	if len(pod.Spec.Volumes) == 0 {
		ops = append(ops, jsonPatchOp{Op: "add", Path: "/spec/volumes", Value: []corev1.Volume{vol}})
	} else {
		ops = append(ops, jsonPatchOp{Op: "add", Path: "/spec/volumes/-", Value: vol})
	}

	mount := corev1.VolumeMount{Name: InterceptorVolumeName, MountPath: InterceptorMountPath,
		SubPath: "fluence_intercept.py", ReadOnly: true}
	startup := corev1.EnvVar{Name: "PYTHONSTARTUP", Value: InterceptorMountPath}
	for i, c := range pod.Spec.Containers {
		if !requestsFluxionResource(c) {
			continue
		}
		if len(c.VolumeMounts) == 0 {
			ops = append(ops, jsonPatchOp{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/volumeMounts", i), Value: []corev1.VolumeMount{mount}})
		} else {
			ops = append(ops, jsonPatchOp{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/volumeMounts/-", i), Value: mount})
		}
		if !hasEnv(c, "PYTHONSTARTUP") {
			if len(c.Env) == 0 {
				ops = append(ops, jsonPatchOp{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/env", i), Value: []corev1.EnvVar{startup}})
			} else {
				ops = append(ops, jsonPatchOp{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/env/-", i), Value: startup})
			}
		}
	}
	return ops
}

// sidecarContainerOps adds the fluence-sidecar container and sets the sidecar
// ServiceAccount. observe=true puts the sidecar in observe-only telemetry mode
// (polls queue position, ungates nothing).
func (m *Mutator) sidecarContainerOps(pod *corev1.Pod, observe bool) []jsonPatchOp {
	var ops []jsonPatchOp

	env := []corev1.EnvVar{
		fieldEnv("FLUENCE_POD_UID", "metadata.uid"),
		fieldEnv("FLUENCE_POD_NAME", "metadata.name"),
		fieldEnv("FLUENCE_NAMESPACE", "metadata.namespace"),
	}
	if observe {
		env = append(env, corev1.EnvVar{Name: "FLUENCE_OBSERVE", Value: "true"})
	}

	sidecar := corev1.Container{
		Name:            "fluence-sidecar",
		Image:           m.sidecarImage(),
		ImagePullPolicy: corev1.PullIfNotPresent,
		Env:             env,
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    *resourceQuantity("100m"),
				corev1.ResourceMemory: *resourceQuantity("256Mi"),
			},
		},
	}
	if len(pod.Spec.Containers) == 0 {
		ops = append(ops, jsonPatchOp{Op: "add", Path: "/spec/containers", Value: []corev1.Container{sidecar}})
	} else {
		ops = append(ops, jsonPatchOp{Op: "add", Path: "/spec/containers/-", Value: sidecar})
	}

	if pod.Spec.ServiceAccountName == "" || pod.Spec.ServiceAccountName == "default" {
		ops = append(ops, jsonPatchOp{Op: "add", Path: "/spec/serviceAccountName", Value: SidecarServiceAccount})
	}
	return ops
}

func podUIDOps(pod *corev1.Pod) []jsonPatchOp {
	uid := fieldEnv("FLUENCE_POD_UID", "metadata.uid")
	var ops []jsonPatchOp
	for i, c := range pod.Spec.Containers {
		if !requestsFluxionResource(c) || hasEnv(c, "FLUENCE_POD_UID") {
			continue
		}
		if len(c.Env) == 0 {
			ops = append(ops, jsonPatchOp{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/env", i), Value: []corev1.EnvVar{uid}})
			pod.Spec.Containers[i].Env = []corev1.EnvVar{uid}
		} else {
			ops = append(ops, jsonPatchOp{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/env/-", i), Value: uid})
			pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, uid)
		}
	}
	return ops
}

// ── Mutate ─────────────────────────────────────────────────────────────────────

// ── Resource handlers ────────────────────────────────────────────────────────
//
// The webhook's behavior is expressed as a set of independent handlers rather
// than hardcoded rules. Each handler decides for itself whether it applies to a
// pod (Applies) and, if so, returns the mutations to inject (Mutate). The
// webhook runs every registered handler in order and concatenates their ops.
//
// This keeps the core dispatcher domain-agnostic: quantum/braket is one handler,
// not a special case. New behavior is a new handler, matched on whatever pod
// condition it likes (a resource request, a label, an annotation, ...).

// ResourceHandler inspects a pod and, when it applies, contributes JSON patch
// operations. Applies is fully general — it receives the pod (and the Mutator,
// for handlers that must consult cluster state, e.g. resolving a group's
// leader) and decides.
type ResourceHandler interface {
	// Name identifies the handler in logs.
	Name() string
	// Applies reports whether this handler should act on the pod. It may use
	// the Mutator's client to consult cluster state.
	Applies(ctx context.Context, m *Mutator, pod *corev1.Pod) bool
	// Mutate returns the patch ops to inject. It may use the Mutator's client
	// for side effects (creating PodGroups, RBAC, copying ConfigMaps).
	Mutate(ctx context.Context, m *Mutator, pod *corev1.Pod) []jsonPatchOp
}

// defaultHandlers is the ordered set of handlers the webhook runs. Order
// matters only where handlers touch the same paths; these three are
// independent except that env injection is harmless before the others.
func defaultHandlers() []ResourceHandler {
	return []ResourceHandler{
		&fluxionEnvHandler{},
		&gangHandler{},
		&quantumHandler{},
	}
}

// fluxionEnvHandler injects the FLUXION_* env contract into any container that
// requests a fluxion.flux-framework.org/* resource. Generic to all Fluxion
// resources, not quantum-specific.
type fluxionEnvHandler struct{}

func (h *fluxionEnvHandler) Name() string { return "fluxion-env" }

func (h *fluxionEnvHandler) Applies(ctx context.Context, m *Mutator, pod *corev1.Pod) bool {
	return podRequestsFluxionResource(pod)
}

func (h *fluxionEnvHandler) Mutate(ctx context.Context, m *Mutator, pod *corev1.Pod) []jsonPatchOp {
	var ops []jsonPatchOp
	contract := m.injectedEnv()
	for i, c := range pod.Spec.Containers {
		if !requestsFluxionResource(c) {
			continue
		}
		for _, e := range contract {
			if hasEnv(c, e.Name) {
				continue
			}
			if len(c.Env) == 0 {
				ops = append(ops, jsonPatchOp{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/env", i), Value: []corev1.EnvVar{e}})
				pod.Spec.Containers[i].Env = []corev1.EnvVar{e}
			} else {
				ops = append(ops, jsonPatchOp{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/env/-", i), Value: e})
				pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, e)
			}
		}
	}
	return ops
}

// gangHandler gang-schedules pods that carry the group label, by creating a
// Fluence-owned PodGroup and stamping spec.schedulingGroup.podGroupName so the
// scheduler gangs them. Independent of whether the work is quantum: a non-
// quantum group is simply gang-scheduled with no sidecar.
type gangHandler struct{}

func (h *gangHandler) Name() string { return "gang" }

func (h *gangHandler) Applies(ctx context.Context, m *Mutator, pod *corev1.Pod) bool {
	return groupName(pod) != ""
}

func (h *gangHandler) Mutate(ctx context.Context, m *Mutator, pod *corev1.Pod) []jsonPatchOp {
	g := groupName(pod)
	// First pod admitted in the group creates the PodGroup; all pods are linked.
	if m.podGroupLeader(ctx, pod) == "" {
		m.ensureQuantumPodGroup(ctx, pod, g)
		m.recordLeader(ctx, pod)
	}
	return schedulingGroupOps(pod, g)
}

// quantumHandler injects the provider interceptor + sidecar for pods that
// request the quantum resource, and (when the pod is also part of a group)
// gates the non-leader workers. This is the only handler that knows about
// quantum coordination; the interceptor/sidecar machinery itself is generic.
type quantumHandler struct{}

func (h *quantumHandler) Name() string { return "quantum" }

// Applies if the pod participates in quantum coordination, in either role:
//   - it requests the quantum resource (the leader, or a standalone quantum
//     pod), or
//   - it is a non-leader member of a group whose leader is a quantum pod (a
//     classical worker to be gated). Workers are classical and do not request
//     the QPU resource themselves, so role is determined by group membership.
func (h *quantumHandler) Applies(ctx context.Context, m *Mutator, pod *corev1.Pod) bool {
	if podRequestsQuantumResource(pod) {
		return true
	}
	return h.isWorkerOfQuantumGroup(ctx, m, pod)
}

// isWorkerOfQuantumGroup reports whether pod is a non-leader member of a group
// whose recorded leader is a quantum (QPU-requesting) pod.
func (h *quantumHandler) isWorkerOfQuantumGroup(ctx context.Context, m *Mutator, pod *corev1.Pod) bool {
	g := groupName(pod)
	if g == "" || m.Client == nil {
		return false
	}
	leader := m.podGroupLeader(ctx, pod)
	if leader == "" || leader == pod.Name {
		return false // no leader yet, or this pod is the leader
	}
	lp, err := m.Client.CoreV1().Pods(pod.Namespace).Get(ctx, leader, metav1.GetOptions{})
	if err != nil {
		return false
	}
	return podRequestsQuantumResource(lp)
}

func (h *quantumHandler) Mutate(ctx context.Context, m *Mutator, pod *corev1.Pod) []jsonPatchOp {
	var ops []jsonPatchOp

	g := groupName(pod)

	// Determine role by ADMISSION ORDER, not by resource request. In a gang
	// built from a pod template (Deployment/Job/StatefulSet), every pod has an
	// identical spec — same group label, and every pod requests the quantum
	// resource. The leader is simply the first pod admitted, recorded on the
	// PodGroup by the gang handler. Every other pod in the group is a worker,
	// regardless of whether it requests the quantum resource itself.
	if g != "" {
		leader := m.podGroupLeader(ctx, pod)
		if leader != "" && leader != pod.Name {
			// Worker: not the recorded leader. Gate it; the leader's sidecar
			// ungates it when the quantum task is ready. (The gang handler has
			// already linked it to the PodGroup via spec.schedulingGroup.)
			log.Printf("[fluence-webhook] quantum worker %s/%s (leader=%s) — gating",
				pod.Namespace, pod.Name, leader)
			return quantumWorkerGateOps(pod)
		}
	}

	// Submitter role: the recorded group leader, or a standalone single quantum
	// pod. Always gets the interceptor + pod-uid env so its task is tagged for
	// discovery. It gets the SIDECAR only when there is coordination to do:
	//   - it is a group leader (workers to ungate), or
	//   - observe-only telemetry is requested via the observe label.
	// A standalone quantum pod with no group and no observe label has nothing to
	// coordinate, so no sidecar is injected (no surprise machinery).
	isLeader := g != ""
	observe := pod.Labels != nil && pod.Labels[ObserveLabel] == "true"

	log.Printf("[fluence-webhook] quantum pod %s/%s — interceptor (leader=%v observe=%v)",
		pod.Namespace, pod.Name, isLeader, observe)

	ops = append(ops, podUIDOps(pod)...)
	ops = append(ops, m.interceptorOps(pod)...)

	if isLeader || observe {
		m.ensureSidecarRBAC(ctx, pod.Namespace)
		ops = append(ops, m.sidecarContainerOps(pod, observe)...)
	}
	return ops
}

// ── Mutate ─────────────────────────────────────────────────────────────────────

// Mutate dispatches the pod to every registered handler and concatenates the
// patch operations from those that apply. Pods not scheduled by Fluence are
// left untouched.
func (m *Mutator) Mutate(ctx context.Context, pod *corev1.Pod) []jsonPatchOp {
	if pod.Spec.SchedulerName != SchedulerName {
		return nil
	}
	var ops []jsonPatchOp
	for _, h := range m.handlers() {
		if h.Applies(ctx, m, pod) {
			log.Printf("[fluence-webhook] handler %q applies to %s/%s",
				h.Name(), pod.Namespace, pod.Name)
			ops = append(ops, h.Mutate(ctx, m, pod)...)
		}
	}
	return ops
}

// handlers returns the Mutator's handler set, defaulting to defaultHandlers if
// none were configured (tests may inject their own).
func (m *Mutator) handlers() []ResourceHandler {
	if m.Handlers != nil {
		return m.Handlers
	}
	return defaultHandlers()
}

// ── HTTP handler ───────────────────────────────────────────────────────────────

func (m *Mutator) Handler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var review admissionv1.AdmissionReview
	if err := json.Unmarshal(body, &review); err != nil || review.Request == nil {
		http.Error(w, "bad admission review", http.StatusBadRequest)
		return
	}
	resp := &admissionv1.AdmissionResponse{UID: review.Request.UID, Allowed: true}
	var pod corev1.Pod
	if err := json.Unmarshal(review.Request.Object.Raw, &pod); err == nil {
		if ops := m.Mutate(r.Context(), &pod); len(ops) > 0 {
			if patch, err := json.Marshal(ops); err == nil {
				pt := admissionv1.PatchTypeJSONPatch
				resp.Patch = patch
				resp.PatchType = &pt
				log.Printf("[fluence-webhook] injected %d op(s) into pod %s/%s", len(ops), pod.Namespace, pod.Name)
			}
		}
	}
	out := admissionv1.AdmissionReview{TypeMeta: review.TypeMeta, Response: resp}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// ── TLS ────────────────────────────────────────────────────────────────────────

func GenerateCerts(dnsNames []string) (caPEM, certPEM, keyPEM []byte, err error) {
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, nil, err
	}
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "fluence-webhook-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().AddDate(10, 0, 0),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, nil, err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, nil, nil, err
	}
	leafKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, nil, err
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: dnsNames[0]},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().AddDate(10, 0, 0),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, DNSNames: dnsNames,
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, nil, err
	}
	caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(leafKey)})
	return caPEM, certPEM, keyPEM, nil
}

func EnsureCABundle(ctx context.Context, client kubernetes.Interface, configName string, caPEM []byte) error {
	patch := fmt.Sprintf(`[{"op":"replace","path":"/webhooks/0/clientConfig/caBundle","value":%q}]`,
		base64.StdEncoding.EncodeToString(caPEM))
	_, err := client.AdmissionregistrationV1().MutatingWebhookConfigurations().Patch(
		ctx, configName, types.JSONPatchType, []byte(patch), metav1.PatchOptions{})
	return err
}
