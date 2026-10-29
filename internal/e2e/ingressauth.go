// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package e2e

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/clustertrustbundle"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/podidentity"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// The router client's identity.
const (
	RouterClientNamespace      = "ate-e2e-router-client"
	RouterClientServiceAccount = "router-client"
)

// RouterClientSPIFFEID is the SPIFFE ID the podidentity signer issues for
// serviceAccount in RouterClientNamespace.
func RouterClientSPIFFEID(serviceAccount string) string {
	return installdefaults.SPIFFEID(RouterClientNamespace, serviceAccount)
}

// routerClientCertTimeout bounds the wait for the signer to issue.
const routerClientCertTimeout = 2 * time.Minute

var (
	routerClientCertsMu sync.Mutex
	routerClientCerts   = map[string]tls.Certificate{}
)

// RouterClientCertificate returns a certificate the podidentity signer issued
// for serviceAccount in RouterClientNamespace. Each ServiceAccount's
// certificate is requested once per test process, and lasts longer than any
// suite.
func RouterClientCertificate(t *testing.T, ctx context.Context, serviceAccount string) tls.Certificate {
	t.Helper()
	routerClientCertsMu.Lock()
	defer routerClientCertsMu.Unlock()
	if cert, ok := routerClientCerts[serviceAccount]; ok {
		return cert
	}

	clientset := GetClients().K8s
	if err := ensureRouterClientNamespace(ctx, clientset); err != nil {
		t.Fatalf("preparing namespace %s: %v", RouterClientNamespace, err)
	}
	cert, err := podidentity.RequestCertificate(ctx, clientset, "e2e", RouterClientNamespace, serviceAccount, routerClientCertTimeout)
	if err != nil {
		t.Fatalf("requesting a podidentity certificate for %s/%s: %v", RouterClientNamespace, serviceAccount, err)
	}
	routerClientCerts[serviceAccount] = cert
	return cert
}

// ensureRouterClientNamespace creates RouterClientNamespace, leaving it in
// place if it already exists. The namespace carries NamespaceLabel, so
// hack/cleanup-e2e.sh removes it with the rest.
func ensureRouterClientNamespace(ctx context.Context, clientset kubernetes.Interface) error {
	_, err := clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   RouterClientNamespace,
			Labels: map[string]string{NamespaceLabel: "true"},
		},
	}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("creating namespace: %w", err)
	}
	return nil
}

// RouterServerName is the DNS name on the router's servicedns serving
// certificate.
func RouterServerName() string {
	return fmt.Sprintf("%s.%s.svc", ResourceName("atenet-router"), SystemNamespace())
}

// RouterTLSConfig returns a TLS config for the router's TLS listeners that
// verifies the router's serving certificate and presents clientCert, which
// may be nil to present none. The caller sets NextProtos.
func RouterTLSConfig(ctx context.Context, clientCert *tls.Certificate) (*tls.Config, error) {
	roots, err := serviceDNSTrustPool(ctx, GetClients().K8s)
	if err != nil {
		return nil, fmt.Errorf("loading the servicedns trust bundle: %w", err)
	}
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: RouterServerName(),
	}
	if clientCert != nil {
		cfg.Certificates = []tls.Certificate{*clientCert}
	}
	return cfg, nil
}

const (
	// serviceDNSSignerName and liveBundleSelector mirror the
	// clusterTrustBundle projected-volume sources that in-cluster clients
	// mount to verify ateapi's serving cert.
	serviceDNSSignerName = "servicedns.podcert.ate.dev/identity"
	liveBundleSelector   = "podcert.ate.dev/canarying=live"
)

// serviceDNSTrustPool returns the live trust bundle of the servicedns signer,
// which issues the serving certificates of substrate's in-cluster Services
// (ateapi and atenet-router among them).
func serviceDNSTrustPool(ctx context.Context, clientset kubernetes.Interface) (*x509.CertPool, error) {
	bundles, err := clustertrustbundle.NewClient(clientset, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to discover ClusterTrustBundle API: %w", err)
	}
	ctbs, err := bundles.List(ctx, metav1.ListOptions{
		LabelSelector: liveBundleSelector,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list ClusterTrustBundles: %w", err)
	}

	pool := x509.NewCertPool()
	found := false
	for _, ctb := range ctbs.Items {
		if ctb.Spec.SignerName != serviceDNSSignerName {
			continue
		}
		if !pool.AppendCertsFromPEM([]byte(ctb.Spec.TrustBundle)) {
			return nil, fmt.Errorf("ClusterTrustBundle %q contains no valid certificates", ctb.ObjectMeta.Name)
		}
		found = true
	}
	if !found {
		return nil, fmt.Errorf("no live ClusterTrustBundle found for signer %q", serviceDNSSignerName)
	}
	return pool, nil
}
