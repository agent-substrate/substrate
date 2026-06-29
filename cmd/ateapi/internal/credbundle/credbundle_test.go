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

package credbundle

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"testing"
	"time"
)

func TestParsePKCS8PrivateKeyBlock(t *testing.T) {
	key := generateRSAKey(t)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal PKCS8 key: %v", err)
	}
	certDER := generateCertificate(t)
	bundle := append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})...)

	bundlePath := writeBundle(t, bundle)
	cert, err := Parse(bundlePath)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(cert.Certificate) != 1 {
		t.Fatalf("Parse() certificate chain length = %d, want 1", len(cert.Certificate))
	}
	if cert.PrivateKey == nil {
		t.Fatalf("Parse() private key is nil")
	}
	if cert.Leaf == nil {
		t.Fatalf("Parse() leaf certificate is nil")
	}
}

func TestParseRejectsNonPKCS8PrivateKeyBlock(t *testing.T) {
	certDER := generateCertificate(t)
	bundle := append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}), pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(generateRSAKey(t))})...)

	bundlePath := writeBundle(t, bundle)
	if _, err := Parse(bundlePath); err == nil {
		t.Fatalf("Parse() error = nil, want unsupported private key block error")
	}
}

func generateRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	return key
}

func generateCertificate(t *testing.T) []byte {
	t.Helper()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "api.ate-system.svc"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"api.ate-system.svc"},
	}
	key := generateRSAKey(t)
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	return der
}

func writeBundle(t *testing.T, bundle []byte) string {
	t.Helper()
	path := t.TempDir() + "/bundle.pem"
	if err := os.WriteFile(path, bundle, 0o600); err != nil {
		t.Fatalf("write bundle: %v", err)
	}
	return path
}
