// Package webhook is fluence's mutating admission webhook. Its job is to make
// scheduler-chosen values reach a pod's containers without the user wiring
// anything. Container env is immutable after a pod is created, so the scheduler
// cannot write it directly; instead this webhook injects, at pod-creation time,
// a downward-API env that reads an annotation the scheduler fills in later
// (during PreBind). The user writes a plain pod; the plumbing is automatic.
//
// Current rule: for a pod scheduled by fluence whose container requests a
// fluxion.flux-framework.org/* resource, inject QRMI_BACKEND sourced from the
// fluence backend annotation. New mutation rules can be added in Mutate.
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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// SchedulerName is the scheduler whose pods this webhook mutates.
const SchedulerName = "fluence"

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

// Mutate returns the JSON Patch operations for a pod, or nil if nothing applies.
// For each container that requests a fluxion.flux-framework.org/* resource, it
// appends every contract env var the container does not already define.
func (m *Mutator) Mutate(pod *corev1.Pod) []jsonPatchOp {
	if pod.Spec.SchedulerName != SchedulerName {
		return nil
	}
	contract := m.injectedEnv()
	var ops []jsonPatchOp
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
				// Subsequent vars append to the now-existing slice.
				c.Env = []corev1.EnvVar{e}
				continue
			}
			ops = append(ops, jsonPatchOp{
				Op:    "add",
				Path:  fmt.Sprintf("/spec/containers/%d/env/-", i),
				Value: e,
			})
			c.Env = append(c.Env, e)
		}
	}
	return ops
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
		if ops := m.Mutate(&pod); len(ops) > 0 {
			if patch, err := json.Marshal(ops); err == nil {
				pt := admissionv1.PatchTypeJSONPatch
				resp.Patch = patch
				resp.PatchType = &pt
				log.Printf("[fluence-webhook] injected %d env op(s) into pod %s/%s",
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
