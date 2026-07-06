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
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestGenerateED25519CA(t *testing.T) {
	before := time.Now().UTC().Truncate(time.Second)
	ca, err := GenerateED25519CA("test-ca")
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

	if len(ca.IntermediateCertificates) != 0 {
		t.Errorf("IntermediateCertificates length = %d, want 0", len(ca.IntermediateCertificates))
	}
}

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	ca1, err := GenerateED25519CA("ca-1")
	if err != nil {
		t.Fatalf("GenerateED25519CA(ca-1): %v", err)
	}
	ca2, err := GenerateED25519CA("ca-2")
	if err != nil {
		t.Fatalf("GenerateED25519CA(ca-2): %v", err)
	}

	pool := &Pool{CAs: []*CA{ca1, ca2}}

	data, err := Marshal(pool)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	restored, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	if len(restored.CAs) != 2 {
		t.Fatalf("restored pool has %d CAs, want 2", len(restored.CAs))
	}

	for i, orig := range pool.CAs {
		got := restored.CAs[i]
		if got.ID != orig.ID {
			t.Errorf("CA[%d].ID = %q, want %q", i, got.ID, orig.ID)
		}

		origKey := orig.SigningKey.(ed25519.PrivateKey)
		gotKey, ok := got.SigningKey.(ed25519.PrivateKey)
		if !ok {
			t.Fatalf("CA[%d].SigningKey type = %T, want ed25519.PrivateKey", i, got.SigningKey)
		}
		if !bytes.Equal(origKey, gotKey) {
			t.Errorf("CA[%d].SigningKey bytes differ", i)
		}

		if !bytes.Equal(got.RootCertificate.Raw, orig.RootCertificate.Raw) {
			t.Errorf("CA[%d].RootCertificate.Raw differs", i)
		}

		// Verify the deserialized key can actually sign and the cert can verify.
		msg := []byte("round-trip-check")
		sig := ed25519.Sign(gotKey, msg)
		pubKey := got.RootCertificate.PublicKey.(ed25519.PublicKey)
		if !ed25519.Verify(pubKey, msg, sig) {
			t.Errorf("CA[%d]: sign/verify with deserialized key failed", i)
		}
	}
}

func TestMarshalUnmarshalWithIntermediates(t *testing.T) {
	root, err := GenerateED25519CA("root")
	if err != nil {
		t.Fatalf("GenerateED25519CA(): %v", err)
	}

	intermPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey(): %v", err)
	}

	intermTemplate := &x509.Certificate{
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
	}

	intermDER, err := x509.CreateCertificate(rand.Reader, intermTemplate, root.RootCertificate, intermPub, root.SigningKey)
	if err != nil {
		t.Fatalf("CreateCertificate(): %v", err)
	}
	intermCert, err := x509.ParseCertificate(intermDER)
	if err != nil {
		t.Fatalf("ParseCertificate(): %v", err)
	}

	root.IntermediateCertificates = []*x509.Certificate{intermCert}

	pool := &Pool{CAs: []*CA{root}}

	data, err := Marshal(pool)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	restored, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	if len(restored.CAs[0].IntermediateCertificates) != 1 {
		t.Fatalf("IntermediateCertificates length = %d, want 1", len(restored.CAs[0].IntermediateCertificates))
	}

	if !bytes.Equal(restored.CAs[0].IntermediateCertificates[0].Raw, intermCert.Raw) {
		t.Error("intermediate certificate Raw bytes differ after round-trip")
	}

	// Verify intermediate chains to root.
	roots := x509.NewCertPool()
	roots.AddCert(restored.CAs[0].RootCertificate)
	restoredInterm := restored.CAs[0].IntermediateCertificates[0]
	if err := restoredInterm.CheckSignatureFrom(restored.CAs[0].RootCertificate); err != nil {
		t.Errorf("intermediate cert signature verification against root failed: %v", err)
	}

	// Verify the intermediate's public key matches the generated key pair.
	intermPubFromCert := restoredInterm.PublicKey.(ed25519.PublicKey)
	if !bytes.Equal(intermPubFromCert, intermPub) {
		t.Error("intermediate cert public key does not match generated key pair")
	}
}

func TestUnmarshalPEMPool(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey(): %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "session-id-ca"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate(): %v", err)
	}
	keyPEM := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	certPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}))

	data, err := json.Marshal(&serializedPool{
		CAs: []*serializedCA{{
			ID:                 "1",
			SigningKeyPEM:      keyPEM,
			RootCertificatePEM: certPEM,
		}},
	})
	if err != nil {
		t.Fatalf("Marshal(): %v", err)
	}

	pool, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal(): %v", err)
	}
	if len(pool.CAs) != 1 {
		t.Fatalf("CAs length = %d, want 1", len(pool.CAs))
	}
	if _, ok := pool.CAs[0].SigningKey.(*rsa.PrivateKey); !ok {
		t.Fatalf("SigningKey type = %T, want *rsa.PrivateKey", pool.CAs[0].SigningKey)
	}
	if pool.CAs[0].RootCertificate.Subject.CommonName != "session-id-ca" {
		t.Fatalf("RootCertificate CN = %q, want session-id-ca", pool.CAs[0].RootCertificate.Subject.CommonName)
	}
}

func TestUnmarshalErrors(t *testing.T) {
	ca, err := GenerateED25519CA("err-test")
	if err != nil {
		t.Fatalf("GenerateED25519CA(): %v", err)
	}
	validData, err := Marshal(&Pool{CAs: []*CA{ca}})
	if err != nil {
		t.Fatalf("Marshal(): %v", err)
	}

	corruptField := func(field string, value any) []byte {
		var raw map[string]json.RawMessage
		json.Unmarshal(validData, &raw)

		var cas []map[string]json.RawMessage
		json.Unmarshal(raw["CAs"], &cas)

		b, _ := json.Marshal(value)
		cas[0][field] = b

		casBytes, _ := json.Marshal(cas)
		raw["CAs"] = casBytes
		out, _ := json.Marshal(raw)
		return out
	}

	tests := []struct {
		name      string
		input     []byte
		wantInErr string
	}{
		{"invalid JSON", []byte("{bad"), "unmarshaling JSON"},
		{"corrupted signing key", corruptField("SigningKeyPKCS8", []byte{0xDE, 0xAD}), "signing key"},
		{"corrupted root cert", corruptField("RootCertificateDER", []byte{0xDE, 0xAD}), "root certificate"},
		{"corrupted intermediate cert", corruptField("IntermediateCertificatesDER", [][]byte{{0xDE, 0xAD}}), "intermediate certificate"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Unmarshal(tt.input)
			if err == nil {
				t.Fatal("Unmarshal() error = nil, want error")
			}
			if !strings.Contains(err.Error(), tt.wantInErr) {
				t.Errorf("error = %q, want substring %q", err.Error(), tt.wantInErr)
			}
		})
	}
}
