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

package controlapi

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/substratex509"
)

// makeTestCA mints a self-signed CA and returns it along with a pool
// containing it as the sole root.
func makeTestCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey, *x509.CertPool) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating CA key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("creating CA certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parsing CA certificate: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return cert, key, pool
}

// makeLeafCert mints a server leaf certificate signed by the given CA. If
// podUID is non-empty it is embedded in a PodIdentity extension.
func makeLeafCert(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, podUID string) *x509.Certificate {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating leaf key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if podUID != "" {
		// AddPodIdentityToCertificate requires all fields to be non-empty;
		// only PodUID matters to these tests.
		err := substratex509.AddPodIdentityToCertificate(&substratex509.PodIdentity{
			Namespace:          "ate-system",
			ServiceAccountName: "atelet",
			ServiceAccountUID:  "sa-uid",
			PodName:            "atelet-abc",
			PodUID:             podUID,
			NodeName:           "node-1",
			NodeUID:            "node-uid",
		}, template)
		if err != nil {
			t.Fatalf("adding PodIdentity extension: %v", err)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("creating leaf certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parsing leaf certificate: %v", err)
	}
	return cert
}

func TestVerifyAteletServerCert(t *testing.T) {
	ca, caKey, roots := makeTestCA(t)
	otherCA, otherCAKey, _ := makeTestCA(t)

	const uid = "5a2e1c9f-0b57-4a52-9f6e-2f6d3a1b8c4d"

	tests := []struct {
		name        string
		leaf        *x509.Certificate
		expectedUID string
		wantErr     bool
	}{
		{
			name:        "matching UID succeeds",
			leaf:        makeLeafCert(t, ca, caKey, uid),
			expectedUID: uid,
		},
		{
			name:        "mismatched UID fails",
			leaf:        makeLeafCert(t, ca, caKey, "some-other-uid"),
			expectedUID: uid,
			wantErr:     true,
		},
		{
			name:        "missing pod UID extension fails",
			leaf:        makeLeafCert(t, ca, caKey, ""),
			expectedUID: uid,
			wantErr:     true,
		},
		{
			name:        "cert from untrusted CA fails",
			leaf:        makeLeafCert(t, otherCA, otherCAKey, uid),
			expectedUID: uid,
			wantErr:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			verify, err := verifyAteletServerCert(roots, tc.expectedUID)
			if err != nil {
				t.Fatalf("constructing verifier: %v", err)
			}
			err = verify(tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{tc.leaf},
			})
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("verify returned error %v, wantErr=%v", err, tc.wantErr)
			}
		})
	}

	t.Run("no peer certificate fails", func(t *testing.T) {
		verify, err := verifyAteletServerCert(roots, uid)
		if err != nil {
			t.Fatalf("constructing verifier: %v", err)
		}
		if err := verify(tls.ConnectionState{}); err == nil {
			t.Fatal("verify succeeded, want error")
		}
	})

	t.Run("empty expected UID fails at construction", func(t *testing.T) {
		if _, err := verifyAteletServerCert(roots, ""); err == nil {
			t.Fatal("verifyAteletServerCert succeeded, want error")
		}
	})
}
