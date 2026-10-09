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

package steps

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"maps"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/internal/localjwtauthority"
)

// The expected strings here are what the shell installer's
// create_api_authentication_config produced, so a regression shows up as a
// diff rather than as an apiserver that silently trusts the wrong issuer.
func TestBuildAuthenticationConfig(t *testing.T) {
	for _, tc := range []struct {
		name   string
		issuer string
		want   string
	}{
		{
			name:   "external issuer needs no discovery credentials",
			issuer: "https://container.googleapis.com/v1/projects/p/locations/l/clusters/c",
			want: "actorIdentityJWTProvider: kubernetes\n" +
				"jwtProviders:\n" +
				"- name: kubernetes\n" +
				"  issuer: https://container.googleapis.com/v1/projects/p/locations/l/clusters/c\n" +
				"  audiences: [api.ate-system.svc]",
		},
		{
			name:   "in-cluster issuer projects the service account CA and token",
			issuer: inClusterIssuer,
			want: "actorIdentityJWTProvider: kubernetes\n" +
				"jwtProviders:\n" +
				"- name: kubernetes\n" +
				"  issuer: https://kubernetes.default.svc\n" +
				"  audiences: [api.ate-system.svc]\n" +
				"  certificateAuthorityFile: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt\n" +
				"  discoveryTokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token",
		},
		{
			name:   "cluster.local form is treated as in-cluster too",
			issuer: inClusterIssuer + ".cluster.local",
			want: "actorIdentityJWTProvider: kubernetes\n" +
				"jwtProviders:\n" +
				"- name: kubernetes\n" +
				"  issuer: https://kubernetes.default.svc.cluster.local\n" +
				"  audiences: [api.ate-system.svc]\n" +
				"  certificateAuthorityFile: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt\n" +
				"  discoveryTokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := buildAuthenticationConfig(tc.issuer); got != tc.want {
				t.Errorf("buildAuthenticationConfig(%q) =\n%q\nwant\n%q", tc.issuer, got, tc.want)
			}
		})
	}
}

// The pool format is a contract with every component that reads these Secrets,
// and internal/localca is free to evolve its generation API underneath. Pinning
// the shape here — one named CA, marked active, with a usable root and the
// requested key type — turns a library change into a failing unit test rather
// than a cluster whose signers pick the wrong CA.
func TestNewCAPoolSecretData(t *testing.T) {
	for _, tc := range []struct {
		name    string
		id      string
		keyType localca.KeyType
		wantAlg x509.PublicKeyAlgorithm
	}{
		{"actor identity", poolKeyID, localca.KeyTypeED25519, x509.Ed25519},
		{"egress mitm", poolKeyID, localca.KeyTypeECDSAP256, x509.ECDSA},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := newCAPoolSecretData(tc.id, tc.keyType)
			if err != nil {
				t.Fatalf("newCAPoolSecretData() error = %v", err)
			}

			// The egress dataplanes mount tls.crt and tls.key from this
			// Secret non-optionally, so dropping either one wedges their
			// pods in ContainerCreating rather than failing anything here.
			wantKeys := []string{"pool", corev1.TLSCertKey, corev1.TLSPrivateKeyKey}
			if diff := cmp.Diff(wantKeys, slices.Sorted(maps.Keys(data))); diff != "" {
				t.Errorf("secret keys differ (-want +got):\n%s", diff)
			}
			if _, err := tls.X509KeyPair(data[corev1.TLSCertKey], data[corev1.TLSPrivateKeyKey]); err != nil {
				t.Errorf("tls.X509KeyPair() error = %v, want the CA certificate and key to form a usable pair", err)
			}

			pool, err := localca.Unmarshal(data["pool"])
			if err != nil {
				t.Fatalf("Unmarshal() error = %v", err)
			}
			if len(pool.CAs) != 1 {
				t.Fatalf("pool holds %d CAs, want 1", len(pool.CAs))
			}
			if pool.CAs[0].ID != tc.id {
				t.Errorf("CA ID = %q, want %q", pool.CAs[0].ID, tc.id)
			}
			// Without this the pool signs with its first CA only as a
			// backwards-compatibility fallback.
			if pool.ActiveForSigning != tc.id {
				t.Errorf("ActiveForSigning = %q, want %q", pool.ActiveForSigning, tc.id)
			}

			root := pool.CAs[0].RootCertificate
			if root == nil {
				t.Fatal("CA has no root certificate")
			}
			if root.PublicKeyAlgorithm != tc.wantAlg {
				t.Errorf("root key algorithm = %v, want %v", root.PublicKeyAlgorithm, tc.wantAlg)
			}
			if !root.IsCA {
				t.Error("root certificate is not a CA")
			}
			if got := root.NotAfter.Sub(root.NotBefore); got != caValidity {
				t.Errorf("root validity = %v, want %v", got, caValidity)
			}
		})
	}
}

func TestNewJWTPoolSecretData(t *testing.T) {
	for _, alg := range []string{"ES256", "RS256"} {
		t.Run(alg, func(t *testing.T) {
			data, err := newJWTPoolSecretData(alg)
			if err != nil {
				t.Fatalf("newJWTPoolSecretData() error = %v", err)
			}
			if diff := cmp.Diff([]string{"pool"}, slices.Sorted(maps.Keys(data))); diff != "" {
				t.Errorf("secret keys differ (-want +got):\n%s", diff)
			}

			pool, err := localjwtauthority.Unmarshal(data["pool"])
			if err != nil {
				t.Fatalf("Unmarshal() error = %v", err)
			}
			if len(pool.Authorities) != 1 {
				t.Fatalf("pool has %d authorities, want 1", len(pool.Authorities))
			}
			authority := pool.Authorities[0]
			if authority.Algorithm != alg {
				t.Errorf("Algorithm = %q, want %q", authority.Algorithm, alg)
			}
			thumbprint, err := localjwtauthority.Thumbprint(authority.SigningKey.Public())
			if err != nil {
				t.Fatalf("Thumbprint() error = %v", err)
			}
			if authority.ID != thumbprint || pool.ActiveForSigning != thumbprint {
				t.Errorf("key ID %q, active %q; want both to be the thumbprint %q", authority.ID, pool.ActiveForSigning, thumbprint)
			}
		})
	}
}

// An existing install must gain the dedicated PostgreSQL CA without rotating
// the signer pools it already uses.
func TestEnsurePodCertificateCAs(t *testing.T) {
	e := &Env{Kube: fakeKube(t, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: NamespacePodCert}})}
	if err := e.CreatePodCertificateControllerCAs(t.Context()); err != nil {
		t.Fatal(err)
	}
	before, err := e.Kube.Typed.CoreV1().Secrets(NamespacePodCert).Get(t.Context(), SecretServiceDNSCA, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Kube.Typed.CoreV1().Secrets(NamespacePodCert).Delete(t.Context(), SecretPostgresCA, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := e.EnsurePodCertificateCAs(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{SecretServiceDNSCA, SecretPodIdentityCA, SecretPostgresCA} {
		secret, err := e.Kube.Typed.CoreV1().Secrets(NamespacePodCert).Get(t.Context(), name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if name == SecretServiceDNSCA && !bytes.Equal(secret.Data["pool"], before.Data["pool"]) {
			t.Fatal("existing service DNS CA rotated")
		}
		pool, err := localca.Unmarshal(secret.Data["pool"])
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.TrustAnchors(); err != nil {
			t.Fatal(err)
		}
	}
}
