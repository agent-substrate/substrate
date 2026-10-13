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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/internal/pemutil"
)

// TestAddCA pins what lets the identity suite run alongside the suites doing
// TLS through the egress gateway: the added CA joins the bundle, but the pool
// keeps signing with the CA it signed with before.
func TestAddCA(t *testing.T) {
	for _, tc := range []struct {
		name   string
		active string
	}{
		{"named signer", "install"},
		{"first CA signs", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			signer := generateCA(t, "install")
			before, err := localca.Marshal(&localca.ConcretePool{CAs: []*localca.CA{signer}, ActiveForSigning: tc.active})
			if err != nil {
				t.Fatal(err)
			}

			after, bundle, err := addCA(before, "added")
			if err != nil {
				t.Fatalf("addCA: %v", err)
			}
			pool, err := localca.Unmarshal(after)
			if err != nil {
				t.Fatal(err)
			}
			if len(pool.CAs) != 2 {
				t.Fatalf("pool has %d CAs, want 2", len(pool.CAs))
			}
			if want := certificatePEM(signer.RootCertificate) + certificatePEM(pool.CAs[1].RootCertificate); !SameCertificates(bundle, want) {
				t.Errorf("bundle = %q, want both roots %q", bundle, want)
			}

			key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			chain, err := pool.CreateCertificate(&x509.Certificate{
				SerialNumber: big.NewInt(1),
				NotBefore:    time.Now(),
				NotAfter:     time.Now().Add(time.Hour),
			}, &key.PublicKey)
			if err != nil {
				t.Fatalf("signing with the new pool: %v", err)
			}
			leaf, err := x509.ParseCertificate(chain[0])
			if err != nil {
				t.Fatal(err)
			}
			if err := leaf.CheckSignatureFrom(signer.RootCertificate); err != nil {
				t.Errorf("the new pool no longer signs with the original CA: %v", err)
			}
		})
	}
}

func TestSameCertificates(t *testing.T) {
	ca := generateCA(t, "a")
	a := certificatePEM(ca.RootCertificate)
	b := certificatePEM(generateCA(t, "b").RootCertificate)
	withHeader := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Headers: map[string]string{"k": "v"}, Bytes: ca.RootCertificate.Raw}))
	key := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("not a key")}))
	sanitized, err := pemutil.SanitizeCertificateBundle([]byte(a+b), 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		x, y string
		want bool
	}{
		{"same", a + b, a + b, true},
		{"reordered", a + b, b + a, true},
		{"as atelet writes it", string(sanitized), a + b, true},
		{"one missing", a + b, a, false},
		{"duplicated", a + a, a, false},
		{"different", a, b, false},
		{"empty", "", a, false},
		{"text between blocks", a + "junk\n" + b, a + b, false},
		{"other block type", a + key, a, false},
		{"header", withHeader, a, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := SameCertificates(tc.x, tc.y); got != tc.want {
				t.Errorf("SameCertificates = %v, want %v", got, tc.want)
			}
		})
	}
}

func generateCA(t *testing.T, id string) *localca.CA {
	t.Helper()
	ca, err := localca.GenerateCA(id, localca.KeyTypeECDSAP256, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return ca
}

func certificatePEM(cert *x509.Certificate) string {
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}))
}
