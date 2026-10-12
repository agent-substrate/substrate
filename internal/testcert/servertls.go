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

package testcert

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/localca"
)

// ServerTLS holds an origin key pair and the public CA trusted by its client.
type ServerTLS struct{ RootCA, Certificate, PrivateKey []byte }

// NewServerTLS issues an origin certificate for the allocated Service IP.
// Only the public CA is returned; its signing key stays in this process.
func NewServerTLS(t *testing.T, ip net.IP) ServerTLS {
	t.Helper()
	ca, err := localca.GenerateCA("e2e-origin", localca.KeyTypeECDSAP256, 24*time.Hour)
	if err != nil {
		t.Fatalf("generating origin CA: %v", err)
	}
	return ServerTLSWithCA(t, ca, ip)
}

// ServerTLSWithCA issues a Service IP and optional DNS certificate from a CA
// already trusted by the client or the MITM gateway's upstream TLS connection.
func ServerTLSWithCA(t *testing.T, ca *localca.CA, ip net.IP, dnsNames ...string) ServerTLS {
	t.Helper()
	if ip == nil {
		t.Fatal("origin certificate requires a Service IP")
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating origin key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("generating origin serial: %v", err)
	}
	leaf := &x509.Certificate{
		SerialNumber: serial,
		IPAddresses:  []net.IP{ip},
		DNSNames:     dnsNames,
		NotBefore:    time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, leaf, ca.RootCertificate, key.Public(), ca.SigningKey)
	if err != nil {
		t.Fatalf("issuing origin certificate: %v", err)
	}
	privateDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("encoding origin key: %v", err)
	}
	return ServerTLS{
		RootCA:      pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.RootCertificate.Raw}),
		Certificate: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		PrivateKey:  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER}),
	}
}
