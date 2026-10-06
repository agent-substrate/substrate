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

package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMacletdTLSConfigReloadsCertificateAndClientCA(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath, caPath := filepath.Join(dir, "server.pem"), filepath.Join(dir, "key.pem"), filepath.Join(dir, "ca.pem")
	writeMacletPair(t, certPath, keyPath, 10)
	ca1, caKey1 := writeMacletCA(t, caPath, 1)
	client1 := macletClientCert(t, ca1, caKey1, 101)
	cfg, err := macletdTLSConfig(certPath, keyPath, caPath)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GetConfigForClient != nil {
		t.Fatal("GetConfigForClient must remain nil so gRPC's h2 ALPN config is not replaced")
	}
	firstCert, err := cfg.GetCertificate(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := macletSerial(t, firstCert.Certificate[0]); got != 10 {
		t.Fatalf("initial server serial = %d, want 10", got)
	}
	if err := cfg.VerifyPeerCertificate([][]byte{client1.Raw}, nil); err != nil {
		t.Fatalf("initial client CA absent: %v", err)
	}

	writeMacletPair(t, certPath, keyPath, 20)
	ca2, caKey2 := writeMacletCA(t, caPath, 2)
	client2 := macletClientCert(t, ca2, caKey2, 102)
	secondCert, err := cfg.GetCertificate(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := macletSerial(t, secondCert.Certificate[0]); got != 20 {
		t.Fatalf("rotated server serial = %d, want 20", got)
	}
	if err := cfg.VerifyPeerCertificate([][]byte{client2.Raw}, nil); err != nil {
		t.Fatalf("rotated CA absent: %v", err)
	}
	if err := cfg.VerifyPeerCertificate([][]byte{client1.Raw}, nil); err == nil {
		t.Fatal("removed client CA remains trusted")
	}
}

func writeMacletPair(t *testing.T, certPath, keyPath string, serial int64) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "server"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeMacletCA(t *testing.T, path string, serial int64) (*x509.Certificate, *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert, key
}

func macletClientCert(t *testing.T, ca *x509.Certificate, caKey *rsa.PrivateKey, serial int64) *x509.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "client"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

func macletSerial(t *testing.T, der []byte) int64 {
	t.Helper()
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert.SerialNumber.Int64()
}
