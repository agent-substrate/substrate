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

package ateclient

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func TestInitTracingDisabledReturnsNoProvider(t *testing.T) {
	tp, err := initTracing(context.Background(), false)
	if err != nil {
		t.Fatalf("initTracing(disabled): %v", err)
	}
	if tp != nil {
		t.Error("initTracing(disabled) returned a provider; want nil so no traceparent is injected")
	}
}

func TestBearerTokenCreds(t *testing.T) {
	md, err := bearerTokenCreds("some-token").GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata: %v", err)
	}
	if got, want := md["authorization"], "Bearer some-token"; got != want {
		t.Errorf("authorization=%q want %q", got, want)
	}

	if _, err := bearerTokenCreds("").GetRequestMetadata(context.Background()); err == nil {
		t.Error("GetRequestMetadata with empty token: want error, got nil")
	}

	if !bearerTokenCreds("some-token").RequireTransportSecurity() {
		t.Error("RequireTransportSecurity() = false, want true")
	}
}

// testCAPEM generates a self-signed CA and returns its PEM encoding.
func testCAPEM(t *testing.T, cn string) []byte {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generating key for %s: %v", cn, err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatalf("creating CA cert for %s: %v", cn, err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func trustBundle(name, signer string, live bool, pemData []byte) *certsv1beta1.ClusterTrustBundle {
	ctb := &certsv1beta1.ClusterTrustBundle{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: certsv1beta1.ClusterTrustBundleSpec{
			SignerName:  signer,
			TrustBundle: string(pemData),
		},
	}
	if live {
		ctb.ObjectMeta.Labels = map[string]string{"podcert.ate.dev/canarying": "live"}
	}
	return ctb
}

func TestServerTLSConfig(t *testing.T) {
	servicednsCA1 := testCAPEM(t, "servicedns-ca-1")
	servicednsCA2 := testCAPEM(t, "servicedns-ca-2")
	podidentityCA := testCAPEM(t, "podidentity-ca")
	canaryCA := testCAPEM(t, "servicedns-ca-canary")

	clientset := fake.NewSimpleClientset(
		trustBundle("servicedns.podcert.ate.dev:identity:primary-bundle", serviceDNSSignerName, true, append(servicednsCA1, servicednsCA2...)),
		trustBundle("podidentity.podcert.ate.dev:identity:primary-bundle", "podidentity.podcert.ate.dev/identity", true, podidentityCA),
		trustBundle("servicedns.podcert.ate.dev:identity:canary-bundle", serviceDNSSignerName, false, canaryCA),
	)

	cfg, err := serverTLSConfig(context.Background(), clientset)
	if err != nil {
		t.Fatalf("serverTLSConfig: %v", err)
	}

	if got, want := cfg.ServerName, "api.ate-system.svc"; got != want {
		t.Errorf("ServerName=%q want %q", got, want)
	}
	if cfg.MinVersion < tls.VersionTLS13 {
		t.Errorf("MinVersion=%x want at least %x", cfg.MinVersion, tls.VersionTLS13)
	}
	if cfg.InsecureSkipVerify {
		t.Error("InsecureSkipVerify=true, want false")
	}

	// The pool must contain exactly the live servicedns CAs: not the
	// podidentity bundle and not the non-live canary bundle.
	wantPool := x509.NewCertPool()
	wantPool.AppendCertsFromPEM(servicednsCA1)
	wantPool.AppendCertsFromPEM(servicednsCA2)
	if !cfg.RootCAs.Equal(wantPool) {
		t.Error("RootCAs does not match the live servicedns trust bundle")
	}
}

func TestServerTLSConfigErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		objects []runtime.Object
	}{
		{name: "no bundles"},
		{
			name: "only other signers",
			objects: []runtime.Object{
				trustBundle("podidentity.podcert.ate.dev:identity:primary-bundle", "podidentity.podcert.ate.dev/identity", true, testCAPEM(t, "podidentity-ca")),
			},
		},
		{
			name: "bundle with no valid certificates",
			objects: []runtime.Object{
				trustBundle("servicedns.podcert.ate.dev:identity:primary-bundle", serviceDNSSignerName, true, []byte("not a pem")),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clientset := fake.NewSimpleClientset(tc.objects...)
			if _, err := serverTLSConfig(context.Background(), clientset); err == nil {
				t.Error("serverTLSConfig: want error, got nil")
			}
		})
	}
}
