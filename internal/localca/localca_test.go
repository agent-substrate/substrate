//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package localca

import (
	"bytes"
	"crypto"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestGenerateED25519CA(t *testing.T) {
	before := time.Now().UTC().Truncate(time.Second)
	ca, err := GenerateCA("test-ca", KeyTypeED25519, 365*24*time.Hour)
	after := time.Now().UTC().Truncate(time.Second).Add(time.Second)
	if err != nil {
		t.Fatalf("GenerateED25519CA() error = %v", err)
	}

	if ca.ID != "test-ca" {
		t.Errorf("ID = %q, want %q", ca.ID, "test-ca")
	}

	if _, ok := ca.SigningKey.(ed25519.PrivateKey); !ok {
		t.Fatalf("SigningKey type = %T, want ed25519.PrivateKey", ca.SigningKey)
	}

	cert := ca.RootCertificate
	if cert == nil {
		t.Fatal("RootCertificate is nil")
	}
	if !cert.IsCA {
		t.Error("IsCA = false, want true")
	}
	if !cert.BasicConstraintsValid {
		t.Error("BasicConstraintsValid = false, want true")
	}

	wantUsage := x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign
	if cert.KeyUsage != wantUsage {
		t.Errorf("KeyUsage = %v, want %v", cert.KeyUsage, wantUsage)
	}

	if cert.NotBefore.Before(before) || cert.NotBefore.After(after) {
		t.Errorf("NotBefore = %v, want between %v and %v", cert.NotBefore, before, after)
	}

	validity := cert.NotAfter.Sub(cert.NotBefore)
	want365d := 365 * 24 * time.Hour
	if validity != want365d {
		t.Errorf("validity = %v, want %v", validity, want365d)
	}

	roots := x509.NewCertPool()
	roots.AddCert(cert)
	if _, err := cert.Verify(x509.VerifyOptions{Roots: roots}); err != nil {
		t.Errorf("self-signed verification failed: %v", err)
	}
}

func TestTLSMaterialPEM(t *testing.T) {
	ca, err := GenerateCA("test-ca", KeyTypeED25519, 365*24*time.Hour)
	if err != nil {
		t.Fatalf("GenerateED25519CA() error = %v", err)
	}

	chain, err := ca.TLSCertificateChainPEM()
	if err != nil {
		t.Fatalf("TLSCertificateChainPEM() error = %v", err)
	}
	block, rest := pem.Decode(chain)
	if block == nil || block.Type != "CERTIFICATE" {
		t.Fatalf("TLSCertificateChainPEM() first block = %#v, want CERTIFICATE", block)
	}
	if len(rest) != 0 {
		t.Fatalf("TLSCertificateChainPEM() has unexpected additional data: %q", rest)
	}
	if !bytes.Equal(block.Bytes, ca.RootCertificate.Raw) {
		t.Error("TLSCertificateChainPEM() certificate differs from the issuing certificate")
	}

	keyPEM, err := ca.TLSPrivateKeyPEM()
	if err != nil {
		t.Fatalf("TLSPrivateKeyPEM() error = %v", err)
	}
	block, rest = pem.Decode(keyPEM)
	if block == nil || block.Type != "PRIVATE KEY" {
		t.Fatalf("TLSPrivateKeyPEM() block = %#v, want PRIVATE KEY", block)
	}
	if len(rest) != 0 {
		t.Fatalf("TLSPrivateKeyPEM() has unexpected additional data: %q", rest)
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		t.Fatalf("TLSPrivateKeyPEM() did not contain PKCS#8: %v", err)
	}
	publicKey, err := x509.MarshalPKIXPublicKey(key.(crypto.Signer).Public())
	if err != nil {
		t.Fatalf("marshaling private key's public key: %v", err)
	}
	if !bytes.Equal(ca.RootCertificate.RawSubjectPublicKeyInfo, publicKey) {
		t.Error("TLSPrivateKeyPEM() key does not match the issuing certificate")
	}
}

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	ca1, err := GenerateCA("1", KeyTypeED25519, 365*24*time.Hour)
	if err != nil {
		t.Fatalf("GenerateED25519CA(ca-1): %v", err)
	}
	ca2, err := GenerateCA("2", KeyTypeED25519, 365*24*time.Hour)
	if err != nil {
		t.Fatalf("GenerateED25519CA(ca-2): %v", err)
	}

	pool := &ConcretePool{CAs: []*CA{ca1, ca2}}

	data, err := Marshal(pool)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	restored, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	if diff := cmp.Diff(restored, pool); diff != "" {
		t.Fatalf("Pool didn't round-trip; diff (-got +want)")
	}
}

func TestRefreshingPool(t *testing.T) {
	ca1, err := GenerateCA("1", KeyTypeED25519, 365*24*time.Hour)
	if err != nil {
		t.Fatalf("Unexpected error generating CA 1: %v", err)
	}
	pool1 := &ConcretePool{
		CAs:              []*CA{ca1},
		ActiveForSigning: "1",
	}
	pool1Bytes, err := Marshal(pool1)
	if err != nil {
		t.Fatalf("Unexpected error marshaling pool 1: %v", err)
	}

	ca2, err := GenerateCA("2", KeyTypeED25519, 365*24*time.Hour)
	if err != nil {
		t.Fatalf("Unexpected error generating CA 2: %v", err)
	}
	pool2 := &ConcretePool{
		CAs:              []*CA{ca2},
		ActiveForSigning: "2",
	}
	pool2Bytes, err := Marshal(pool2)
	if err != nil {
		t.Fatalf("Unexpected error marshaling pool 2: %v", err)
	}

	synctest.Test(t, func(t *testing.T) {
		tempDir := t.TempDir()
		poolFile := filepath.Join(tempDir, "pool.json")

		if err := os.WriteFile(poolFile, pool1Bytes, 0o600); err != nil {
			t.Fatalf("Unexpected error writing pool 1: %v", err)
		}

		refreshingPool, err := NewRefreshingPool(poolFile)
		if err != nil {
			t.Fatalf("Unexpected error creating refreshing pool: %v", err)
		}

		gotAnchors, err := refreshingPool.TrustAnchors()
		if err != nil {
			t.Fatalf("Unexpected errors getting anchors from refreshing pool: %v", err)
		}
		wantAnchors, err := pool1.TrustAnchors()
		if err != nil {
			t.Fatalf("Unexpected errors getting anchors from pool 1: %v", err)
		}
		if diff := cmp.Diff(gotAnchors, wantAnchors); diff != "" {
			t.Fatalf("Refreshing pool returned wrong trust anchors; diff (-got +want)\n%s", diff)
		}

		// Write pool2 and advance past the cache threshold.
		if err := os.WriteFile(poolFile, pool2Bytes, 0o600); err != nil {
			t.Fatalf("Unexpected error writing pool 2: %v", err)
		}
		time.Sleep(61 * time.Second)

		gotAnchors, err = refreshingPool.TrustAnchors()
		if err != nil {
			t.Fatalf("Unexpected errors getting anchors from refreshing pool: %v", err)
		}
		wantAnchors, err = pool2.TrustAnchors()
		if err != nil {
			t.Fatalf("Unexpected errors getting anchors from pool 2: %v", err)
		}
		if diff := cmp.Diff(gotAnchors, wantAnchors); diff != "" {
			t.Fatalf("Refreshing pool returned wrong trust anchors after file update; diff (-got +want)\n%s", diff)
		}
	})
}
