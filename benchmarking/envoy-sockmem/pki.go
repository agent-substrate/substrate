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

// Files writePKI leaves in its directory. One CA signs Envoy's serving
// certificate and the client's certificate, and each side trusts only it.
const (
	caFile         = "ca.pem"
	envoyCertFile  = "envoy-cert.pem"
	envoyKeyFile   = "envoy-key.pem"
	clientCertFile = "client-cert.pem"
	clientKeyFile  = "client-key.pem"
)

// envoyHost is the name Envoy's certificate is valid for, and the name the
// client dials.
const envoyHost = "envoy"

func writePKI(dir string) error {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	now := time.Now()
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "envoy-sockmem CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(7 * 24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		return err
	}
	if err := writePEM(filepath.Join(dir, caFile), "CERTIFICATE", caDER); err != nil {
		return err
	}
	leaf := func(serial int64, certFile, keyFile string, usage x509.ExtKeyUsage, dnsNames []string) error {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return err
		}
		template := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: certFile},
			NotBefore:    now.Add(-time.Hour),
			NotAfter:     now.Add(7 * 24 * time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{usage},
			DNSNames:     dnsNames,
		}
		der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
		if err != nil {
			return err
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			return err
		}
		if err := writePEM(filepath.Join(dir, certFile), "CERTIFICATE", der); err != nil {
			return err
		}
		return writePEM(filepath.Join(dir, keyFile), "PRIVATE KEY", keyDER)
	}
	if err := leaf(2, envoyCertFile, envoyKeyFile, x509.ExtKeyUsageServerAuth, []string{envoyHost}); err != nil {
		return err
	}
	return leaf(3, clientCertFile, clientKeyFile, x509.ExtKeyUsageClientAuth, nil)
}

func writePEM(path, blockType string, der []byte) error {
	// Envoy runs as another user in its container and must read the key.
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	return nil
}
