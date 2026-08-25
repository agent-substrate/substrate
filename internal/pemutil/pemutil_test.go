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

package pemutil

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"
)

// selfSignedCertPEM mints a throwaway self-signed certificate, PEM-encoded.
func selfSignedCertPEM(t *testing.T, cn string) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestSanitizeCertificateBundle(t *testing.T) {
	certA := selfSignedCertPEM(t, "a")
	certB := selfSignedCertPEM(t, "b")

	junkKey := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: []byte("not-a-cert")})
	withHeaders := func(certPEM []byte) []byte {
		block, _ := pem.Decode(certPEM)
		block.Headers = map[string]string{"Comment": "should be stripped"}
		return pem.EncodeToMemory(block)
	}

	t.Run("keeps only certificates, strips headers, dedupes", func(t *testing.T) {
		in := bytes.Join([][]byte{
			[]byte("leading garbage\n"),
			withHeaders(certA),
			junkKey,
			certB,
			certA, // duplicate
		}, nil)
		got, err := SanitizeCertificateBundle(in)
		if err != nil {
			t.Fatalf("SanitizeCertificateBundle: %v", err)
		}
		// Order-insensitive: the anchors are deliberately shuffled. Compare
		// the decoded block SET (which also proves headers were stripped —
		// re-encoding a block with headers would not match a bare cert).
		want := map[string]int{string(certA): 1, string(certB): 1}
		if diff := blockCounts(t, got); !mapsEqual(diff, want) {
			t.Errorf("sanitized bundle blocks = %v certs, want exactly certA and certB once each", diff)
		}
	})

	t.Run("output order is a shuffle, not source order", func(t *testing.T) {
		certs := [][]byte{certA, certB, selfSignedCertPEM(t, "c"), selfSignedCertPEM(t, "d")}
		in := bytes.Join(certs, nil)
		orders := map[string]bool{}
		for i := 0; i < 32; i++ {
			got, err := SanitizeCertificateBundle(in)
			if err != nil {
				t.Fatalf("SanitizeCertificateBundle: %v", err)
			}
			orders[string(got)] = true
		}
		// 4 anchors have 24 orderings; 32 draws landing on one ordering has
		// probability (1/24)^31 — if this fires, the shuffle is gone.
		if len(orders) < 2 {
			t.Errorf("32 sanitizations produced a single ordering; anchors are no longer shuffled")
		}
	})

	t.Run("errors when no certificates present", func(t *testing.T) {
		for name, in := range map[string][]byte{
			"empty":     nil,
			"junk only": junkKey,
			"not pem":   []byte("hello"),
		} {
			if _, err := SanitizeCertificateBundle(in); err == nil {
				t.Errorf("%s: SanitizeCertificateBundle = nil error, want error", name)
			}
		}
	})
}

// blockCounts decodes a PEM stream into a multiset of re-encoded
// header-free CERTIFICATE blocks.
func blockCounts(t *testing.T, in []byte) map[string]int {
	t.Helper()
	out := map[string]int{}
	rest := in
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			return out
		}
		if len(block.Headers) != 0 {
			t.Errorf("block has headers %v, want none", block.Headers)
		}
		out[string(pem.EncodeToMemory(&pem.Block{Type: block.Type, Bytes: block.Bytes}))]++
	}
}

func mapsEqual(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
