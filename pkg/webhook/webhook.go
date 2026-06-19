// Package webhook is fluence's mutating admission webhook. Its job is to make
// scheduler-chosen values reach a pod's containers without the user wiring
// anything. Container env is immutable after a pod is created, so the scheduler
// cannot write it directly; instead this webhook injects, at pod-creation time,
// a downward-API env that reads an annotation the scheduler fills in later
// (during PreBind). The user writes a plain pod; the plumbing is automatic.
//
// Current rules:
//
//  1. For a pod scheduled by fluence whose container requests a
//     fluxion.flux-framework.org/* resource, inject QRMI_BACKEND sourced from
//     the fluence backend annotation. New mutation rules can be added in Mutate.
//
//  2. Quantum leader/worker split for PodGroups of size > 1:
//     When a PodGroup contains pods that request a QPU resource, the first such
//     pod admitted becomes the leader — it gets the sidecar injected and
//     FLUENCE_POD_UID set. Every subsequent pod in the same PodGroup that
//     requests a QPU resource gets a quantum.braket/ready scheduling gate added,
//     preventing it from entering the Fluxion scheduling cycle until the sidecar
//     ungates it. The leader election is recorded as an annotation on the
//     PodGroup object so it survives webhook restarts.
//
//     A pod with no PodGroup (bare pod, Deployment, StatefulSet, Job) is always
//     treated as a group of 1 — no gating, no sidecar, independent allocation.
//
// The webhook also manages its own TLS: it generates a self-signed CA + serving
// certificate at startup and patches its MutatingWebhookConfiguration's caBundle,
// so the install needs no cert-manager and no committed keys.
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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// SchedulerName is the scheduler whose pods this webhook mutates.
const SchedulerName = "fluence"

// QuantumGateName is the scheduling gate added to worker pods in a quantum
// PodGroup. The fluence sidecar removes this gate when the QPU task is ready.
const QuantumGateName = "quantum.braket/ready"

// QuantumLeaderAnnotation is written onto the PodGroup object when the first
// QPU-requesting pod of the group is admitted. Its value is the leader pod name.
// Subsequent QPU-requesting pods in the same group check for this annotation to
// determine they are workers and should be gated.
const QuantumLeaderAnnotation = "fluence.flux-framework.org/quantum-leader"

// SidecarImage is the default fluence braket sidecar image. Can be overridden
// via the FLUENCE_SIDECAR_IMAGE env var at webhook startup.
const SidecarImage = "ghcr.io/converged-computing/fluence-sidecar-braket:latest"

// jsonPatchOp is a single RFC 6902 JSON Patch operation.
type jsonPatchOp struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value,omitempty"`
}

// Mutator injects fluence's scheduler-chosen values into a pod's containers. It
// carries the env contract — the union of attribute keys across the configured
// backends — so it injects a stable, predictable set of environment variables
// regardless of which backend a given pod ends up matching. Values flow via the
// downward API from annotations the scheduler writes in PreBind, so the env var
// NAMES are fixed at pod-creation time (here) while their VALUES populate later.
type Mutator struct {
	// AttributeKeys is the union of user attribute keys across all backends. Each
	// becomes a FLUXION_<KEY> env var sourced from its attr-<key> annotation.
	AttributeKeys []string

	// Client is used to look up and patch PodGroup objects for quantum
	// leader/worker split. May be nil in unit tests that do not exercise
	// quantum group logic.
	Client kubernetes.Interface

	// SidecarImage is the sidecar container image to inject into leader pods.
	// Defaults to SidecarImage constant if empty.
	SidecarImage string
}

// injectedEnv returns the full normalized env set this mutator injects into a
// fluxion-requesting container: FLUXION_BACKEND plus one FLUXION_<KEY> per
// configured attribute key. Each reads its annotation via the downward API; an
// annotation the scheduler did not set resolves to empty, which is harmless.
func (m *Mutator) injectedEnv() []corev1.EnvVar {
	envs := []corev1.EnvVar{annotationEnv(
		placement.EnvVarPrefix+"BACKEND", placement.BackendAnnotation)}
	for _, key := range m.AttributeKeys {
		envs = append(envs, annotationEnv(
			placement.EnvVarName(key), placement.AttributeAnnotationPrefix+key))
	}
	return envs
}

// EnvVarNames returns the names of every env var this mutator injects, for
// startup logging so the developer sees the exact contract their container can
// rely on.
func (m *Mutator) EnvVarNames() []string {
	names := make([]string, 0, len(m.AttributeKeys)+1)
	for _, e := range m.injectedEnv() {
		names = append(names, e.Name)
	}
	return names
}

// annotationEnv builds a downward-API env var that reads a pod annotation.
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

// fieldEnv builds a downward-API env var that reads a pod field.
func fieldEnv(envName, fieldPath string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: envName,
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{
				FieldPath: fieldPath,
			},
		},
	}
}

// podGroupSize returns the minMember of the PodGroup the pod belongs to,
// or 1 if the pod is not in a PodGroup or the PodGroup cannot be retrieved.
func (m *Mutator) podGroupSize(ctx context.Context, pod *corev1.Pod) int {
	if m.Client == nil {
		return 1
	}
	groupName := placement.PodGroupName(pod)
	if groupName == "" {
		return 1
	}
	pg, err := m.Client.SchedulingV1alpha2().PodGroups(pod.Namespace).Get(
		ctx, groupName, metav1.GetOptions{})
	if err != nil {
		log.Printf("[fluence-webhook] could not get PodGroup %s/%s: %v",
			pod.Namespace, groupName, err)
		return 1
	}
	if pg.Spec.SchedulingPolicy.Gang.MinCount <= 1 {
		return 1
	}
	return int(pg.Spec.SchedulingPolicy.Gang.MinCount)
}

// podGroupLeader returns the name of the quantum leader already recorded for
// this pod's PodGroup, or "" if none has been recorded yet.
func (m *Mutator) podGroupLeader(ctx context.Context, pod *corev1.Pod) string {
	if m.Client == nil {
		return ""
	}
	groupName := placement.PodGroupName(pod)
	if groupName == "" {
		return ""
	}
	pg, err := m.Client.SchedulingV1alpha2().PodGroups(pod.Namespace).Get(
		ctx, groupName, metav1.GetOptions{})
	if err != nil {
		return ""
	}
	if pg.Annotations == nil {
		return ""
	}
	return pg.Annotations[QuantumLeaderAnnotation]
}

// ensureSidecarRBAC creates the fluence-sidecar ServiceAccount, Role, and
// RoleBinding in the pod's namespace if they do not already exist. Called once
// per namespace when the first leader pod is admitted. Errors are logged but
// do not block pod admission — the sidecar may fail to patch pods if RBAC is
// missing, but the pod itself should not be blocked.
func (m *Mutator) ensureSidecarRBAC(ctx context.Context, namespace string) {
	if m.Client == nil {
		return
	}

	// ServiceAccount
	_, err := m.Client.CoreV1().ServiceAccounts(namespace).Get(
		ctx, SidecarServiceAccount, metav1.GetOptions{})
	if err != nil {
		sa := &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name:      SidecarServiceAccount,
				Namespace: namespace,
				Labels:    map[string]string{"app": "fluence-sidecar"},
			},
		}
		if _, err := m.Client.CoreV1().ServiceAccounts(namespace).Create(
			ctx, sa, metav1.CreateOptions{}); err != nil {
			log.Printf("[fluence-webhook] could not create ServiceAccount %s/%s: %v",
				namespace, SidecarServiceAccount, err)
		} else {
			log.Printf("[fluence-webhook] created ServiceAccount %s/%s",
				namespace, SidecarServiceAccount)
		}
	}

	// Role
	_, err = m.Client.RbacV1().Roles(namespace).Get(
		ctx, SidecarServiceAccount, metav1.GetOptions{})
	if err != nil {
		role := &rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{
				Name:      SidecarServiceAccount,
				Namespace: namespace,
				Labels:    map[string]string{"app": "fluence-sidecar"},
			},
			Rules: []rbacv1.PolicyRule{
				{
					APIGroups: []string{""},
					Resources: []string{"pods"},
					Verbs:     []string{"get", "list", "patch", "update"},
				},
				{
					APIGroups: []string{"scheduling.k8s.io"},
					Resources: []string{"podgroups"},
					Verbs:     []string{"get", "list"},
				},
			},
		}
		if _, err := m.Client.RbacV1().Roles(namespace).Create(
			ctx, role, metav1.CreateOptions{}); err != nil {
			log.Printf("[fluence-webhook] could not create Role %s/%s: %v",
				namespace, SidecarServiceAccount, err)
		} else {
			log.Printf("[fluence-webhook] created Role %s/%s",
				namespace, SidecarServiceAccount)
		}
	}

	// RoleBinding
	_, err = m.Client.RbacV1().RoleBindings(namespace).Get(
		ctx, SidecarServiceAccount, metav1.GetOptions{})
	if err != nil {
		rb := &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      SidecarServiceAccount,
				Namespace: namespace,
				Labels:    map[string]string{"app": "fluence-sidecar"},
			},
			Subjects: []rbacv1.Subject{{
				Kind:      "ServiceAccount",
				Name:      SidecarServiceAccount,
				Namespace: namespace,
			}},
			RoleRef: rbacv1.RoleRef{
				APIGroup: "rbac.authorization.k8s.io",
				Kind:     "Role",
				Name:     SidecarServiceAccount,
			},
		}
		if _, err := m.Client.RbacV1().RoleBindings(namespace).Create(
			ctx, rb, metav1.CreateOptions{}); err != nil {
			log.Printf("[fluence-webhook] could not create RoleBinding %s/%s: %v",
				namespace, SidecarServiceAccount, err)
		} else {
			log.Printf("[fluence-webhook] created RoleBinding %s/%s",
				namespace, SidecarServiceAccount)
		}
	}
}

// recordLeader writes the QuantumLeaderAnnotation onto the PodGroup object,
// recording this pod as the quantum leader for the group.
func (m *Mutator) recordLeader(ctx context.Context, pod *corev1.Pod) {
	if m.Client == nil {
		return
	}
	groupName := placement.PodGroupName(pod)
	if groupName == "" {
		return
	}
	patch := fmt.Sprintf(
		`{"metadata":{"annotations":{%q:%q}}}`,
		QuantumLeaderAnnotation, pod.Name,
	)
	_, err := m.Client.SchedulingV1alpha2().PodGroups(pod.Namespace).Patch(
		ctx, groupName, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	if err != nil {
		log.Printf("[fluence-webhook] could not record leader on PodGroup %s/%s: %v",
			pod.Namespace, groupName, err)
	}
}

// sidecarImage returns the sidecar image to use, falling back to the default.
func (m *Mutator) sidecarImage() string {
	if m.SidecarImage != "" {
		return m.SidecarImage
	}
	return SidecarImage
}

// quantumWorkerGateOps returns patch ops that add the quantum scheduling gate
// to the pod, preventing it from entering the Fluxion scheduling cycle.
func quantumWorkerGateOps(pod *corev1.Pod) []jsonPatchOp {
	gate := corev1.PodSchedulingGate{Name: QuantumGateName}
	if len(pod.Spec.SchedulingGates) == 0 {
		return []jsonPatchOp{{
			Op:    "add",
			Path:  "/spec/schedulingGates",
			Value: []corev1.PodSchedulingGate{gate},
		}}
	}
	// Check gate not already present
	for _, g := range pod.Spec.SchedulingGates {
		if g.Name == QuantumGateName {
			return nil
		}
	}
	return []jsonPatchOp{{
		Op:    "add",
		Path:  "/spec/schedulingGates/-",
		Value: gate,
	}}
}

// InterceptorConfigMap is the name of the ConfigMap holding the SDK interceptor.
const InterceptorConfigMap = "fluence-braket-interceptor"

// InterceptorVolumeName is the volume name for the SDK interceptor mount.
const InterceptorVolumeName = "fluence-braket-interceptor"

// InterceptorMountPath is where the interceptor script is mounted.
const InterceptorMountPath = "/etc/fluence/fluence_braket_intercept.py"

// SidecarServiceAccount is the ServiceAccount the sidecar runs as.
const SidecarServiceAccount = "fluence-sidecar"

// sidecarOps returns patch ops that:
//  1. Inject the fluence sidecar container into the leader pod
//  2. Add the SDK interceptor ConfigMap as a volume
//  3. Mount the interceptor into every user container that requests QPU
//  4. Set the pod's ServiceAccount to fluence-sidecar
func (m *Mutator) sidecarOps(pod *corev1.Pod) []jsonPatchOp {
	sidecar := corev1.Container{
		Name:            "fluence-sidecar",
		Image:           m.sidecarImage(),
		ImagePullPolicy: corev1.PullIfNotPresent,
		Env: []corev1.EnvVar{
			fieldEnv("FLUENCE_POD_UID", "metadata.uid"),
			fieldEnv("FLUENCE_POD_NAME", "metadata.name"),
			fieldEnv("FLUENCE_NAMESPACE", "metadata.namespace"),
			// FLUXION_ARN is already injected by the existing env contract
			// via the downward API from the backend annotation.
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    *resourceQuantity("100m"),
				corev1.ResourceMemory: *resourceQuantity("256Mi"),
			},
		},
	}

	var ops []jsonPatchOp

	// 1. Inject sidecar container
	if len(pod.Spec.Containers) == 0 {
		ops = append(ops, jsonPatchOp{
			Op:    "add",
			Path:  "/spec/containers",
			Value: []corev1.Container{sidecar},
		})
	} else {
		ops = append(ops, jsonPatchOp{
			Op:    "add",
			Path:  "/spec/containers/-",
			Value: sidecar,
		})
	}

	// 2. Add interceptor ConfigMap volume
	interceptorVolume := corev1.Volume{
		Name: InterceptorVolumeName,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: InterceptorConfigMap,
				},
			},
		},
	}
	if len(pod.Spec.Volumes) == 0 {
		ops = append(ops, jsonPatchOp{
			Op:    "add",
			Path:  "/spec/volumes",
			Value: []corev1.Volume{interceptorVolume},
		})
	} else {
		ops = append(ops, jsonPatchOp{
			Op:    "add",
			Path:  "/spec/volumes/-",
			Value: interceptorVolume,
		})
	}

	// 3. Mount interceptor and inject PYTHONSTARTUP into every container
	// requesting a QPU resource. PYTHONSTARTUP works for any Python version,
	// unlike a site-packages path which is version-specific.
	interceptorMount := corev1.VolumeMount{
		Name:      InterceptorVolumeName,
		MountPath: InterceptorMountPath,
		SubPath:   "fluence_braket_intercept.py",
		ReadOnly:  true,
	}
	pythonStartup := corev1.EnvVar{
		Name:  "PYTHONSTARTUP",
		Value: InterceptorMountPath,
	}
	for i, c := range pod.Spec.Containers {
		if !requestsFluxionResource(c) {
			continue
		}
		// volume mount
		if len(c.VolumeMounts) == 0 {
			ops = append(ops, jsonPatchOp{
				Op:    "add",
				Path:  fmt.Sprintf("/spec/containers/%d/volumeMounts", i),
				Value: []corev1.VolumeMount{interceptorMount},
			})
			pod.Spec.Containers[i].VolumeMounts = []corev1.VolumeMount{interceptorMount}
		} else {
			ops = append(ops, jsonPatchOp{
				Op:    "add",
				Path:  fmt.Sprintf("/spec/containers/%d/volumeMounts/-", i),
				Value: interceptorMount,
			})
			pod.Spec.Containers[i].VolumeMounts = append(pod.Spec.Containers[i].VolumeMounts, interceptorMount)
		}
		// PYTHONSTARTUP env var
		if hasEnv(c, "PYTHONSTARTUP") {
			continue
		}
		if len(c.Env) == 0 {
			ops = append(ops, jsonPatchOp{
				Op:    "add",
				Path:  fmt.Sprintf("/spec/containers/%d/env", i),
				Value: []corev1.EnvVar{pythonStartup},
			})
			pod.Spec.Containers[i].Env = []corev1.EnvVar{pythonStartup}
		} else {
			ops = append(ops, jsonPatchOp{
				Op:    "add",
				Path:  fmt.Sprintf("/spec/containers/%d/env/-", i),
				Value: pythonStartup,
			})
			pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, pythonStartup)
		}
	}

	// 4. Set ServiceAccount so the sidecar can patch pods.
	// Use "add" not "replace" — the field may not be set yet at admission time.
	if pod.Spec.ServiceAccountName == "" || pod.Spec.ServiceAccountName == "default" {
		ops = append(ops, jsonPatchOp{
			Op:    "add",
			Path:  "/spec/serviceAccountName",
			Value: SidecarServiceAccount,
		})
	}

	return ops
}

// podUIDOps returns patch ops that inject FLUENCE_POD_UID into every container
// that requests a fluxion resource. The sidecar reads this to tag Braket tasks.
func podUIDOps(pod *corev1.Pod) []jsonPatchOp {
	uidEnv := fieldEnv("FLUENCE_POD_UID", "metadata.uid")
	var ops []jsonPatchOp
	for i, c := range pod.Spec.Containers {
		if !requestsFluxionResource(c) {
			continue
		}
		if hasEnv(c, "FLUENCE_POD_UID") {
			continue
		}
		if len(c.Env) == 0 {
			ops = append(ops, jsonPatchOp{
				Op:    "add",
				Path:  fmt.Sprintf("/spec/containers/%d/env", i),
				Value: []corev1.EnvVar{uidEnv},
			})
			pod.Spec.Containers[i].Env = []corev1.EnvVar{uidEnv}
			continue
		}
		ops = append(ops, jsonPatchOp{
			Op:    "add",
			Path:  fmt.Sprintf("/spec/containers/%d/env/-", i),
			Value: uidEnv,
		})
		pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, uidEnv)
	}
	return ops
}

// Mutate returns the JSON Patch operations for a pod, or nil if nothing applies.
//
// For each container that requests a fluxion.flux-framework.org/* resource:
//   - inject the FLUXION_* env contract (existing behaviour)
//
// Additionally, for QPU-requesting pods in a PodGroup of size > 1:
//   - if no leader has been recorded: this pod is the leader — inject sidecar,
//     inject FLUENCE_POD_UID, record leader on PodGroup
//   - if a leader already exists: this pod is a worker — add scheduling gate
func (m *Mutator) Mutate(ctx context.Context, pod *corev1.Pod) []jsonPatchOp {
	if pod.Spec.SchedulerName != SchedulerName {
		return nil
	}
	contract := m.injectedEnv()
	var ops []jsonPatchOp

	// --- existing env injection ---
	for i, c := range pod.Spec.Containers {
		if !requestsFluxionResource(c) {
			continue
		}
		for _, e := range contract {
			if hasEnv(c, e.Name) {
				continue
			}
			if len(c.Env) == 0 {
				ops = append(ops, jsonPatchOp{
					Op:    "add",
					Path:  fmt.Sprintf("/spec/containers/%d/env", i),
					Value: []corev1.EnvVar{e},
				})
				pod.Spec.Containers[i].Env = []corev1.EnvVar{e}
				continue
			}
			ops = append(ops, jsonPatchOp{
				Op:    "add",
				Path:  fmt.Sprintf("/spec/containers/%d/env/-", i),
				Value: e,
			})
			pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, e)
		}
	}

	// --- quantum leader/worker split ---
	// Only applies to pods in a PodGroup of size > 1 that request a QPU resource.
	if !podRequestsQPU(pod) {
		return ops
	}
	groupSize := m.podGroupSize(ctx, pod)
	if groupSize <= 1 {
		// Single pod or no PodGroup — independent allocation, no gating needed.
		return ops
	}

	leader := m.podGroupLeader(ctx, pod)
	if leader == "" {
		// No leader recorded yet — this pod becomes the leader.
		log.Printf("[fluence-webhook] pod %s/%s is quantum leader for group (size=%d)",
			pod.Namespace, pod.Name, groupSize)
		m.ensureSidecarRBAC(ctx, pod.Namespace)
		m.recordLeader(ctx, pod)
		ops = append(ops, m.sidecarOps(pod)...)
		ops = append(ops, podUIDOps(pod)...)
	} else {
		// Leader already exists — this pod is a worker, add the gate.
		log.Printf("[fluence-webhook] pod %s/%s is quantum worker (leader=%s)",
			pod.Namespace, pod.Name, leader)
		ops = append(ops, quantumWorkerGateOps(pod)...)
	}

	return ops
}

// podRequestsQPU returns true if any container in the pod requests a QPU
// resource (fluxion.flux-framework.org/qpu).
func podRequestsQPU(pod *corev1.Pod) bool {
	for _, c := range pod.Spec.Containers {
		for name := range c.Resources.Requests {
			if string(name) == placement.FluxionResourcePrefix+"qpu" {
				return true
			}
		}
	}
	return false
}

func requestsFluxionResource(c corev1.Container) bool {
	for name := range c.Resources.Requests {
		if strings.HasPrefix(string(name), placement.FluxionResourcePrefix) {
			return true
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

// resourceQuantity is a helper to build a resource.Quantity inline.
func resourceQuantity(s string) *resource.Quantity {
	q := resource.MustParse(s)
	return &q
}

// Handler is the /mutate endpoint. It always admits the pod (failure to mutate
// must not block creation); it only adds a patch when Mutate returns one.
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
				log.Printf("[fluence-webhook] injected %d op(s) into pod %s/%s",
					len(ops), pod.Namespace, pod.Name)
			}
		}
	}

	out := admissionv1.AdmissionReview{TypeMeta: review.TypeMeta, Response: resp}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// GenerateCerts returns a self-signed CA (PEM) and a serving cert+key (PEM) valid
// for the given DNS names. The CA PEM is what the apiserver must trust (caBundle).
func GenerateCerts(dnsNames []string) (caPEM, certPEM, keyPEM []byte, err error) {
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, nil, err
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "fluence-webhook-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
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
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: dnsNames[0]},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().AddDate(10, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     dnsNames,
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

// EnsureCABundle patches the named MutatingWebhookConfiguration so its first
// webhook trusts caPEM.
func EnsureCABundle(ctx context.Context, client kubernetes.Interface, configName string, caPEM []byte) error {
	patch := fmt.Sprintf(
		`[{"op":"replace","path":"/webhooks/0/clientConfig/caBundle","value":%q}]`,
		base64.StdEncoding.EncodeToString(caPEM),
	)
	_, err := client.AdmissionregistrationV1().MutatingWebhookConfigurations().Patch(
		ctx, configName, types.JSONPatchType, []byte(patch), metav1.PatchOptions{})
	return err
}
