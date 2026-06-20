// Package webhook is fluence's mutating admission webhook.
//
// The core here is domain-agnostic plumbing: it owns the Mutator, the handler
// dispatcher, per-namespace PodGroup/RBAC provisioning, the Model C package
// staging (init container + shared volume on PYTHONPATH), the HTTP entrypoint,
// and self-managed TLS. It knows nothing about quantum, Braket, gate names, or
// observe labels — that policy lives entirely in the handlers (pkg/webhook/
// handlers), which self-register via Register().
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
	"time"

	"github.com/converged-computing/fluence/pkg/placement"
	"github.com/converged-computing/fluence/pkg/webhook/spec"

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
	// SchedulerName gates the whole webhook: only pods scheduled by Fluence are
	// considered. This is the one cross-cutting constant the core owns.
	SchedulerName = "fluence"

	// GroupLabel marks a pod's gang membership. The core treats its value as an
	// opaque group identity for PodGroup management; it ascribes no quantum
	// meaning to it (a handler decides what a group means).
	GroupLabel = "fluence.flux-framework.org/group"

	// LeaderAnnotation records the admission-order leader on a PodGroup.
	LeaderAnnotation = "fluence.flux-framework.org/leader"

	// Sidecar/staging infrastructure (generic — not quantum-specific).
	SidecarImage          = "ghcr.io/converged-computing/fluence-sidecar:latest"
	SidecarServiceAccount = "fluence-sidecar"

	// StageVolumeName / StageMountPath: the shared emptyDir the init container
	// stages the fluence Python package into, mounted into the user container and
	// prepended to PYTHONPATH (Model C delivery).
	StageVolumeName = "fluence-pkg"
	StageMountPath  = "/opt/fluence-staged"
)

// ── Mutator ─────────────────────────────────────────────────────────────────────

type Mutator struct {
	AttributeKeys []string
	Clientset     kubernetes.Interface
	SidecarImage  string
}

// compile-time check that *Mutator satisfies the handler capability interface.
var _ MutatorAPI = (*Mutator)(nil)

func (m *Mutator) sidecarImage() string {
	if m.SidecarImage != "" {
		return m.SidecarImage
	}
	return SidecarImage
}

// GroupName returns the value of GroupLabel on the pod, or "".
func GroupName(pod *corev1.Pod) string { return spec.Label(pod, GroupLabel) }

func resourceQuantity(s string) *resource.Quantity {
	q := resource.MustParse(s)
	return &q
}

// ── MutatorAPI: capabilities exposed to handlers ────────────────────────────────

// Client implements MutatorAPI: returns the Kubernetes client (nil in tests).
func (m *Mutator) Client() kubernetes.Interface { return m.Clientset }

// InjectedEnv is the FLUXION_* env contract sourced from scheduler annotations.
func (m *Mutator) InjectedEnv() []corev1.EnvVar {
	envs := []corev1.EnvVar{spec.AnnotationEnv(
		placement.EnvVarPrefix+"BACKEND", placement.BackendAnnotation)}
	for _, key := range m.AttributeKeys {
		envs = append(envs, spec.AnnotationEnv(
			placement.EnvVarName(key), placement.AttributeAnnotationPrefix+key))
	}
	return envs
}

// EnvVarNames returns the names of the injected env contract (for the scheduler
// plugin to recognize/strip).
func (m *Mutator) EnvVarNames() []string {
	names := make([]string, 0, len(m.AttributeKeys)+1)
	for _, e := range m.InjectedEnv() {
		names = append(names, e.Name)
	}
	return names
}

// PodGroupLeader returns the recorded admission-order leader for the group, or
// "". Retries briefly to absorb the concurrent leader/worker admission race.
func (m *Mutator) PodGroupLeader(ctx context.Context, namespace, group string) string {
	if m.Clientset == nil || group == "" {
		return ""
	}
	for i := 0; i < 3; i++ {
		pg, err := m.Clientset.SchedulingV1alpha2().PodGroups(namespace).Get(ctx, group, metav1.GetOptions{})
		if err != nil {
			return ""
		}
		if pg.Annotations != nil && pg.Annotations[LeaderAnnotation] != "" {
			return pg.Annotations[LeaderAnnotation]
		}
		if i < 2 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	return ""
}

// EnsurePodGroup creates a Fluence-owned PodGroup (minCount:1) if absent.
func (m *Mutator) EnsurePodGroup(ctx context.Context, namespace, group, leaderPod string) {
	if m.Clientset == nil {
		return
	}
	if _, err := m.Clientset.SchedulingV1alpha2().PodGroups(namespace).Get(ctx, group, metav1.GetOptions{}); err == nil {
		return
	}
	pg := &schedulingv1alpha2.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name: group, Namespace: namespace,
			Labels: map[string]string{"app": "fluence", GroupLabel: group},
		},
		Spec: schedulingv1alpha2.PodGroupSpec{
			SchedulingPolicy: schedulingv1alpha2.PodGroupSchedulingPolicy{
				Gang: &schedulingv1alpha2.GangSchedulingPolicy{MinCount: 1},
			},
		},
	}
	if _, err := m.Clientset.SchedulingV1alpha2().PodGroups(namespace).Create(ctx, pg, metav1.CreateOptions{}); err != nil {
		log.Printf("[fluence-webhook] could not create PodGroup %s/%s: %v", namespace, group, err)
	} else {
		log.Printf("[fluence-webhook] created PodGroup %s/%s (minCount=1)", namespace, group)
	}
}

// RecordLeader records leaderPod as the group's admission-order leader.
func (m *Mutator) RecordLeader(ctx context.Context, namespace, group, leaderPod string) {
	if m.Clientset == nil || group == "" {
		return
	}
	patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, LeaderAnnotation, leaderPod)
	if _, err := m.Clientset.SchedulingV1alpha2().PodGroups(namespace).Patch(
		ctx, group, types.MergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
		log.Printf("[fluence-webhook] could not record leader on PodGroup %s/%s: %v", namespace, group, err)
	}
}

// EnsureSidecarRBAC provisions the per-namespace ServiceAccount/Role/RoleBinding
// the sidecar uses to patch pods and read PodGroups.
func (m *Mutator) EnsureSidecarRBAC(ctx context.Context, namespace string) {
	if m.Clientset == nil {
		return
	}
	lbl := map[string]string{"app": "fluence-sidecar"}

	if _, err := m.Clientset.CoreV1().ServiceAccounts(namespace).Get(ctx, SidecarServiceAccount, metav1.GetOptions{}); err != nil {
		sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: SidecarServiceAccount, Namespace: namespace, Labels: lbl}}
		if _, err := m.Clientset.CoreV1().ServiceAccounts(namespace).Create(ctx, sa, metav1.CreateOptions{}); err != nil {
			log.Printf("[fluence-webhook] could not create ServiceAccount %s/%s: %v", namespace, SidecarServiceAccount, err)
		}
	}
	if _, err := m.Clientset.RbacV1().Roles(namespace).Get(ctx, SidecarServiceAccount, metav1.GetOptions{}); err != nil {
		role := &rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: SidecarServiceAccount, Namespace: namespace, Labels: lbl},
			Rules: []rbacv1.PolicyRule{
				{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "list", "patch", "update"}},
				{APIGroups: []string{"scheduling.k8s.io"}, Resources: []string{"podgroups"}, Verbs: []string{"get", "list"}},
			},
		}
		if _, err := m.Clientset.RbacV1().Roles(namespace).Create(ctx, role, metav1.CreateOptions{}); err != nil {
			log.Printf("[fluence-webhook] could not create Role %s/%s: %v", namespace, SidecarServiceAccount, err)
		}
	}
	if _, err := m.Clientset.RbacV1().RoleBindings(namespace).Get(ctx, SidecarServiceAccount, metav1.GetOptions{}); err != nil {
		rb := &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: SidecarServiceAccount, Namespace: namespace, Labels: lbl},
			Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: SidecarServiceAccount, Namespace: namespace}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: SidecarServiceAccount},
		}
		if _, err := m.Clientset.RbacV1().RoleBindings(namespace).Create(ctx, rb, metav1.CreateOptions{}); err != nil {
			log.Printf("[fluence-webhook] could not create RoleBinding %s/%s: %v", namespace, SidecarServiceAccount, err)
		}
	}
}

// InterceptorOps implements Model C delivery. It injects an init container (the
// sidecar image) that stages the fluence Python package into a shared emptyDir,
// mounts that volume into every Fluxion-resource container, and prepends it to
// PYTHONPATH plus sets FLUENCE_POD_UID. Python auto-imports the staged
// sitecustomize on startup, which runs the interceptor — no user code changes,
// no PYTHONSTARTUP (which only fires interactively), no vendor SDK on our side.
func (m *Mutator) InterceptorOps(pod *corev1.Pod) []spec.Op {
	var ops []spec.Op

	// Shared volume.
	vol := corev1.Volume{Name: StageVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}
	if len(pod.Spec.Volumes) == 0 {
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/volumes", Value: []corev1.Volume{vol}})
	} else {
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/volumes/-", Value: vol})
	}

	// Init container that stages the package into the shared volume.
	//
	// Fail-soft: the interceptor is best-effort, so its delivery must be too. We
	// wrap the stage command so a failure (bad image, missing python, package
	// problem) leaves the shared volume empty and exits 0 rather than blocking
	// the user's pod with Init:Error. An empty staged dir simply means the
	// interceptor does not run — the user application is unaffected. (This also
	// lets CI use a minimal placeholder sidecar image for placement-only tests.)
	initc := corev1.Container{
		Name:            "fluence-stage",
		Image:           m.sidecarImage(),
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command: []string{"sh", "-c",
			fmt.Sprintf("python -m fluence.stage %s || echo '[fluence] staging skipped (interceptor unavailable)'", StageMountPath)},
		VolumeMounts: []corev1.VolumeMount{{Name: StageVolumeName, MountPath: StageMountPath}},
	}
	if len(pod.Spec.InitContainers) == 0 {
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/initContainers", Value: []corev1.Container{initc}})
	} else {
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/initContainers/-", Value: initc})
	}

	// Mount the staged volume + set PYTHONPATH and FLUENCE_POD_UID on each
	// Fluxion-resource container.
	mount := corev1.VolumeMount{Name: StageVolumeName, MountPath: StageMountPath, ReadOnly: true}
	pythonpath := corev1.EnvVar{Name: "PYTHONPATH", Value: StageMountPath}
	uid := spec.FieldEnv("FLUENCE_POD_UID", "metadata.uid")
	for i, c := range pod.Spec.Containers {
		if !spec.RequestsFluxionResource(c) {
			continue
		}
		if len(c.VolumeMounts) == 0 {
			ops = append(ops, spec.Op{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/volumeMounts", i), Value: []corev1.VolumeMount{mount}})
		} else {
			ops = append(ops, spec.Op{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/volumeMounts/-", i), Value: mount})
		}
		if !spec.HasEnv(c, "PYTHONPATH") {
			if len(c.Env) == 0 {
				ops = append(ops, spec.Op{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/env", i), Value: []corev1.EnvVar{pythonpath}})
				pod.Spec.Containers[i].Env = []corev1.EnvVar{pythonpath}
			} else {
				ops = append(ops, spec.Op{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/env/-", i), Value: pythonpath})
				pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, pythonpath)
			}
		}
		if !spec.HasEnv(c, "FLUENCE_POD_UID") {
			ops = append(ops, spec.Op{Op: "add", Path: fmt.Sprintf("/spec/containers/%d/env/-", i), Value: uid})
			pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, uid)
		}
	}
	return ops
}

// SidecarContainerOps adds the fluence-sidecar container and sets its
// ServiceAccount. observe=true selects observe-only telemetry mode.
func (m *Mutator) SidecarContainerOps(pod *corev1.Pod, observe bool) []spec.Op {
	var ops []spec.Op
	env := []corev1.EnvVar{
		spec.FieldEnv("FLUENCE_POD_UID", "metadata.uid"),
		spec.FieldEnv("FLUENCE_POD_NAME", "metadata.name"),
		spec.FieldEnv("FLUENCE_NAMESPACE", "metadata.namespace"),
	}
	if observe {
		env = append(env, corev1.EnvVar{Name: "FLUENCE_OBSERVE", Value: "true"})
	}
	sidecar := corev1.Container{
		Name: "fluence-sidecar", Image: m.sidecarImage(), ImagePullPolicy: corev1.PullIfNotPresent,
		Env: env,
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU: *resourceQuantity("100m"), corev1.ResourceMemory: *resourceQuantity("256Mi"),
		}},
	}
	if len(pod.Spec.Containers) == 0 {
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/containers", Value: []corev1.Container{sidecar}})
	} else {
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/containers/-", Value: sidecar})
	}
	if pod.Spec.ServiceAccountName == "" || pod.Spec.ServiceAccountName == "default" {
		ops = append(ops, spec.Op{Op: "add", Path: "/spec/serviceAccountName", Value: SidecarServiceAccount})
	}
	return ops
}

// ── Dispatcher ──────────────────────────────────────────────────────────────────

// Mutate dispatches the pod to every registered handler and concatenates the
// patch operations from those that apply. Pods not scheduled by Fluence are left
// untouched.
func (m *Mutator) Mutate(ctx context.Context, pod *corev1.Pod) []spec.Op {
	if pod.Spec.SchedulerName != SchedulerName {
		return nil
	}
	var ops []spec.Op
	for _, h := range registered() {
		if h.Applies(ctx, m, pod) {
			log.Printf("[fluence-webhook] handler %q applies to %s/%s", h.Name(), pod.Namespace, pod.Name)
			ops = append(ops, h.Mutate(ctx, m, pod)...)
		}
	}
	return ops
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
