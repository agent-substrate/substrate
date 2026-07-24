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
	"net/url"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

const testAteletSPIFFEID = "spiffe://cluster.local/ns/ate-system/sa/atelet"

// makeTestCA mints a self-signed CA and returns it along with an X.509 bundle
// containing it as the sole authority for the cluster.local trust domain.
func makeTestCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey, *x509bundle.Bundle) {
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
	td := spiffeid.RequireTrustDomainFromString("cluster.local")
	bundle := x509bundle.FromX509Authorities(td, []*x509.Certificate{cert})
	return cert, key, bundle
}

// leafOpts controls the contents of a test leaf certificate.
type leafOpts struct {
	// podUID, if non-empty, is embedded in a PodIdentity extension.
	podUID string
	// spiffeID, if non-empty, is added as a URI SAN.
	spiffeID string
	// noServerAuth omits the serverAuth EKU.
	noServerAuth bool
}

// makeLeafCert mints a server leaf certificate signed by the given CA.
func makeLeafCert(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, opts leafOpts) *x509.Certificate {
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
	if opts.noServerAuth {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	if opts.spiffeID != "" {
		uri, err := url.Parse(opts.spiffeID)
		if err != nil {
			t.Fatalf("parsing SPIFFE ID %q: %v", opts.spiffeID, err)
		}
		template.URIs = []*url.URL{uri}
	}
	if opts.podUID != "" {
		// AddPodIdentityToCertificate requires all fields to be non-empty;
		// only PodUID matters to these tests.
		err := substratex509.AddPodIdentityToCertificate(&substratex509.PodIdentity{
			Namespace:          "ate-system",
			ServiceAccountName: "atelet",
			ServiceAccountUID:  "sa-uid",
			PodName:            "atelet-abc",
			PodUID:             opts.podUID,
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
	ca, caKey, bundle := makeTestCA(t)
	otherCA, otherCAKey, _ := makeTestCA(t)

	expectedID := spiffeid.RequireFromString(testAteletSPIFFEID)

	const uid = "5a2e1c9f-0b57-4a52-9f6e-2f6d3a1b8c4d"

	tests := []struct {
		name        string
		leaf        *x509.Certificate
		expectedUID string
		wantErr     bool
	}{
		{
			name:        "matching UID succeeds",
			leaf:        makeLeafCert(t, ca, caKey, leafOpts{podUID: uid, spiffeID: testAteletSPIFFEID}),
			expectedUID: uid,
		},
		{
			name:        "mismatched UID fails",
			leaf:        makeLeafCert(t, ca, caKey, leafOpts{podUID: "some-other-uid", spiffeID: testAteletSPIFFEID}),
			expectedUID: uid,
			wantErr:     true,
		},
		{
			name:        "missing pod UID extension fails",
			leaf:        makeLeafCert(t, ca, caKey, leafOpts{spiffeID: testAteletSPIFFEID}),
			expectedUID: uid,
			wantErr:     true,
		},
		{
			name:        "cert from untrusted CA fails",
			leaf:        makeLeafCert(t, otherCA, otherCAKey, leafOpts{podUID: uid, spiffeID: testAteletSPIFFEID}),
			expectedUID: uid,
			wantErr:     true,
		},
		{
			name:        "wrong SPIFFE ID fails",
			leaf:        makeLeafCert(t, ca, caKey, leafOpts{podUID: uid, spiffeID: "spiffe://cluster.local/ns/other/sa/other"}),
			expectedUID: uid,
			wantErr:     true,
		},
		{
			name:        "missing URI SAN fails",
			leaf:        makeLeafCert(t, ca, caKey, leafOpts{podUID: uid}),
			expectedUID: uid,
			wantErr:     true,
		},
		{
			name:        "missing serverAuth EKU fails",
			leaf:        makeLeafCert(t, ca, caKey, leafOpts{podUID: uid, spiffeID: testAteletSPIFFEID, noServerAuth: true}),
			expectedUID: uid,
			wantErr:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			verify, err := verifyAteletServerCert(bundle, expectedID, tc.expectedUID)
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
		verify, err := verifyAteletServerCert(bundle, expectedID, uid)
		if err != nil {
			t.Fatalf("constructing verifier: %v", err)
		}
		if err := verify(tls.ConnectionState{}); err == nil {
			t.Fatal("verify succeeded, want error")
		}
	})

	t.Run("empty expected UID fails at construction", func(t *testing.T) {
		if _, err := verifyAteletServerCert(bundle, expectedID, ""); err == nil {
			t.Fatal("verifyAteletServerCert succeeded, want error")
		}
	})
}
