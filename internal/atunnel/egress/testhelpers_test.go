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

package egress

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net/url"
	"os"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/localca"
)

// testCA signs the throwaway certificates the tests present to each other.
type testCA struct {
	pool    *localca.ConcretePool
	certPEM []byte
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	ca, err := localca.GenerateCA("test", localca.KeyTypeECDSAP256, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	certPEM, err := ca.TLSCertificateChainPEM()
	if err != nil {
		t.Fatal(err)
	}
	return &testCA{
		pool:    &localca.ConcretePool{CAs: []*localca.CA{ca}, ActiveForSigning: ca.ID},
		certPEM: certPEM,
	}
}

// sign issues template to a fresh key, valid for an hour, with the CA
// certificate in the chain.
func (ca *testCA) sign(t *testing.T, template *x509.Certificate) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template.SerialNumber = big.NewInt(now.UnixNano())
	template.NotBefore = now.Add(-time.Minute)
	template.NotAfter = now.Add(time.Hour)
	template.KeyUsage = x509.KeyUsageDigitalSignature
	chain, err := ca.pool.CreateCertificate(template, &key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(
		append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: chain[0]}), ca.certPEM...),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

func (ca *testCA) issue(t *testing.T, spiffeID string, usages []x509.ExtKeyUsage) tls.Certificate {
	t.Helper()
	template := &x509.Certificate{ExtKeyUsage: usages}
	if spiffeID != "" {
		uri, err := url.Parse(spiffeID)
		if err != nil {
			t.Fatal(err)
		}
		template.URIs = []*url.URL{uri}
	}
	return ca.sign(t, template)
}

func writeCredentialBundle(t *testing.T, path string, cert tls.Certificate) {
	t.Helper()
	keyDER, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	var bundle []byte
	for _, der := range cert.Certificate {
		bundle = append(bundle, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	}
	bundle = append(bundle, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})...)
	if err := os.WriteFile(path, bundle, 0o600); err != nil {
		t.Fatal(err)
	}
}

func receiveWithin[T any](t *testing.T, channel <-chan T, description string) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
		var zero T
		return zero
	}
}

// waitFor polls until cond holds, failing the test if it never does.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
