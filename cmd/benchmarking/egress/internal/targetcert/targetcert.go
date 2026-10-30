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

// Package targetcert issues the name-constrained CA and the certificate the
// target serves over HTTPS, for the driver to deploy and the actor to verify
// against.
package targetcert

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

// validity is long enough that a test cluster never sees the certificate
// expire; there is no rotation, since nothing can sign a replacement.
const validity = 5 * 365 * 24 * time.Hour

// clockSkew backdates NotBefore so a node whose clock runs slightly behind
// still accepts a certificate issued moments ago.
const clockSkew = time.Hour

// Bundle is a CA certificate and the target certificate and key it signed,
// all PEM-encoded.
type Bundle struct {
	CA, Cert, Key []byte
}

// Generate issues a new CA and the certificate the target serves over HTTPS,
// valid from now, for every name directly under domain, the DNS domain the
// endpoint names sit in. The egress gateway re-originates TLS to the target
// and has to trust the CA, and the gateway is shared, so the CA vouches for
// nothing but the endpoint names: it is name-constrained to domain, and its
// private key is thrown away once the target's certificate is signed.
func Generate(now time.Time, domain string) (*Bundle, error) {
	b, _, _, err := generateWithCA(now, domain)
	return b, err
}

// generateWithCA is Generate, also returning the CA certificate and key so tests
// can show what the CA refuses to vouch for.
func generateWithCA(now time.Time, domain string) (*Bundle, *x509.Certificate, *ecdsa.PrivateKey, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("generating CA key: %w", err)
	}
	caSerial, err := serialNumber()
	if err != nil {
		return nil, nil, nil, err
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          caSerial,
		Subject:               pkix.Name{CommonName: "egress-tests target CA"},
		NotBefore:             now.Add(-clockSkew),
		NotAfter:              now.Add(validity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
		// Critical, so a verifier that cannot enforce the constraint rejects
		// the CA instead of trusting it for every name.
		PermittedDNSDomainsCritical: true,
		PermittedDNSDomains:         []string{domain},
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("creating CA certificate: %w", err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("parsing CA certificate: %w", err)
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("generating target key: %w", err)
	}
	leafSerial, err := serialNumber()
	if err != nil {
		return nil, nil, nil, err
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: leafSerial,
		Subject:      pkix.Name{CommonName: "egress-tests target"},
		// One wildcard covers every endpoint name, which differ only in the
		// leftmost label.
		DNSNames:    []string{"*." + domain},
		NotBefore:   now.Add(-clockSkew),
		NotAfter:    now.Add(validity),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, ca, &leafKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("creating target certificate: %w", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("encoding target key: %w", err)
	}

	b := &Bundle{
		CA:   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		Cert: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}),
		Key:  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	}
	return b, ca, caKey, nil
}

// Write stores the bundle in dir as ca.crt, tls.crt and tls.key; only the key
// is private. The files are what the deploy script turns into the target's
// TLS Secret, named as its keys.
func (b *Bundle) Write(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	for _, f := range []struct {
		name string
		data []byte
		mode os.FileMode
	}{
		{"ca.crt", b.CA, 0o644},
		{"tls.crt", b.Cert, 0o644},
		{"tls.key", b.Key, 0o600},
	} {
		if err := os.WriteFile(filepath.Join(dir, f.name), f.data, f.mode); err != nil {
			return err
		}
	}
	return nil
}

func serialNumber() (*big.Int, error) {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("generating serial number: %w", err)
	}
	return n, nil
}
