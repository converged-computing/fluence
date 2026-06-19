// Command fluence-webhook runs fluence's mutating admission webhook. At startup
// it generates a self-signed CA + serving certificate, patches its
// MutatingWebhookConfiguration's caBundle so the apiserver trusts it, then
// serves the /mutate endpoint over HTTPS. No cert-manager or committed keys.
//
//	WEBHOOK_SERVICE     Service name        (default fluence-webhook)
//	WEBHOOK_NAMESPACE   Service namespace   (default kube-system)
//	WEBHOOK_CONFIG      MutatingWebhookConfiguration name (default fluence-webhook)
//	WEBHOOK_ADDR        listen address      (default :8443)
package main

import (
	"context"
	"crypto/tls"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/converged-computing/fluence/pkg/cluster"
	"github.com/converged-computing/fluence/pkg/webhook"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	svc := env("WEBHOOK_SERVICE", "fluence-webhook")
	ns := env("WEBHOOK_NAMESPACE", "kube-system")
	cfgName := env("WEBHOOK_CONFIG", "fluence-webhook")
	addr := env("WEBHOOK_ADDR", ":8443")

	dnsNames := []string{
		svc + "." + ns + ".svc",
		svc + "." + ns + ".svc.cluster.local",
	}
	caPEM, certPEM, keyPEM, err := webhook.GenerateCerts(dnsNames)
	if err != nil {
		log.Fatalf("generate certs: %v", err)
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		log.Fatalf("load serving cert: %v", err)
	}

	cfg, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("in-cluster config: %v", err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := webhook.EnsureCABundle(ctx, client, cfgName, caPEM); err != nil {
		cancel()
		log.Fatalf("patch caBundle on %s: %v", cfgName, err)
	}
	cancel()
	log.Printf("patched caBundle on MutatingWebhookConfiguration %q", cfgName)

	// The env contract is the union of attribute keys across the configured
	// backends (plus FLUXION_BACKEND), so the set of injected env vars tracks the
	// config automatically. Loaded from the same FLUENCE_RESOURCES the scheduler
	// and device plugin use; absent/unset means just FLUXION_BACKEND.
	var attrKeys []string
	if path := os.Getenv("FLUENCE_RESOURCES"); path != "" {
		if data, rerr := os.ReadFile(path); rerr == nil {
			rc, perr := cluster.LoadResourcesConfig(data)
			if perr != nil {
				log.Fatalf("parse resources config %s: %v", path, perr)
			}
			attrKeys = cluster.AttributeKeys(rc.Resources)
		} else {
			log.Printf("no resources config at %s (%v); injecting FLUXION_BACKEND only", path, rerr)
		}
	}
	mutator := &webhook.Mutator{
		AttributeKeys: attrKeys,
		Client:        client,
		SidecarImage:  env("FLUENCE_SIDECAR_IMAGE", ""),
	}
	log.Printf("[fluence-webhook] env contract injected into fluxion pods: %v", mutator.EnvVarNames())

	mux := http.NewServeMux()
	mux.HandleFunc("/mutate", mutator.Handler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	srv := &http.Server{
		Addr:      addr,
		Handler:   mux,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}},
	}
	log.Printf("serving webhook on %s", addr)
	if err := srv.ListenAndServeTLS("", ""); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
