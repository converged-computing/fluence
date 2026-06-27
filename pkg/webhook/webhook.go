// Package webhook is fluence's mutating admission webhook.
//
// The core here is domain-agnostic plumbing: it owns the Mutator, the handler
// dispatcher, per-namespace PodGroup provisioning, the HTTP entrypoint, and
// self-managed TLS. It knows nothing about quantum, Braket, gate names, sidecars,
// RBAC, or interceptor staging — that policy and machinery lives entirely in the
// handlers (pkg/webhook/handlers), which self-register via Register() and perform
// their own create/edit side-effects through the generic MutatorAPI.
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
	schedulingv1alpha2 "k8s.io/api/scheduling/v1alpha2"
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

	// GroupSizeAnnotation is the gang member count N, set by the workload on each
	// pod. It is the authoritative override for the PodGroup gang minCount when
	// the size cannot (or should not) be derived from the owning controller — and
	// for loose grouped pods where counting at admission is unreliable. The core
	// treats it as an opaque integer string.
	GroupSizeAnnotation = "fluence.flux-framework.org/group-size"
)

// ── Mutator ─────────────────────────────────────────────────────────────────────

type Mutator struct {
	AttributeKeys []string
	Clientset     kubernetes.Interface
}

// compile-time check that *Mutator satisfies the handler capability interface.
var _ MutatorAPI = (*Mutator)(nil)

// GroupName returns the value of GroupLabel on the pod, or "".
func GroupName(pod *corev1.Pod) string { return spec.Label(pod, GroupLabel) }

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

// EnsurePodGroup creates a Fluence-owned PodGroup with gang minCount = the full
// gang size N (the whole group schedules atomically) if absent. minCount<=0
// falls back to 1.
func (m *Mutator) EnsurePodGroup(ctx context.Context, namespace, group, leaderPod string, minCount int32) {
	if minCount <= 0 {
		minCount = 1
	}
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
			// Ownership claim: the reconciler in pkg/fluence deletes a completed
			// gang's PodGroup (to free its Fluxion allocation) ONLY if this
			// annotation is present. A user-created PodGroup (e.g. a native gang
			// not created by this webhook) never carries it and is never touched.
			Annotations: map[string]string{placement.CreatedByAnnotation: placement.CreatedByValue},
		},
		Spec: schedulingv1alpha2.PodGroupSpec{
			SchedulingPolicy: schedulingv1alpha2.PodGroupSchedulingPolicy{
				Gang: &schedulingv1alpha2.GangSchedulingPolicy{MinCount: minCount},
			},
		},
	}
	if _, err := m.Clientset.SchedulingV1alpha2().PodGroups(namespace).Create(ctx, pg, metav1.CreateOptions{}); err != nil {
		log.Printf("[fluence-webhook] could not create PodGroup %s/%s: %v", namespace, group, err)
	} else {
		log.Printf("[fluence-webhook] created PodGroup %s/%s (minCount=%d)", namespace, group, minCount)
	}
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
