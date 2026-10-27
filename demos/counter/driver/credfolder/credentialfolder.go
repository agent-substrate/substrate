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

// Package credfolder configures a tls.Config to read from a credential folder
// that contains a client key and spiffe certificate, and a trust bundle for
// verifying standard DNS SAN certificates issued by a private CA.
package credfolder

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
)

// Loader is a credential bundle loader that handles the "SPIFFE client,
// standard server" use case, where the client presents a SPIFFE certificate (in
// our case, signed by podidentity.podcert.ate.dev/identity), and the server
// presents a standard certificate with a DNS SAN (in our case, issued by
// service-dns.podcert.ate.dev/identity)
type Loader struct {
	path string

	serverRootPool *x509.CertPool
}

// NewLoader creates a Loader that reads from the folder at path.
//
// The folder should contain
//   - credential-bundle.private-key.pem, which contains the SPIFFE client
//     certificate and key (this name comes from the SPIFFE Filesystem Delivery specification).
//   - service-trust-bundle.pem, which contains the root certificates for verifying the server certificate.
func NewLoader(path string) (*Loader, error) {
	return &Loader{
		path: path,
	}, nil
}

func (f *Loader) PrepareClientTLSConfig(cfg *tls.Config) {
	cfg.GetClientCertificate = f.GetClientCertificate

	// Disable built-in processing in order to reload the root certificates.
	cfg.InsecureSkipVerify = true
	cfg.VerifyConnection = f.clientVerifyConnection
}

func (f *Loader) clientVerifyConnection(cs tls.ConnectionState) error {
	if cs.ServerName == "" {
		return fmt.Errorf("no ServerName provided to verify certificate")
	}
	if len(cs.PeerCertificates) == 0 {
		return fmt.Errorf("server provided no certificates")
	}

	rootBytes, err := os.ReadFile(filepath.Join(f.path, "service-trust-bundle.pem"))
	if err != nil {
		return fmt.Errorf("while reading server trust bundle: %w", err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(rootBytes)

	opts := x509.VerifyOptions{
		Roots:         roots,
		DNSName:       cs.ServerName,
		Intermediates: x509.NewCertPool(),
	}
	for _, cert := range cs.PeerCertificates[1:] {
		opts.Intermediates.AddCert(cert)
	}
	if _, err := cs.PeerCertificates[0].Verify(opts); err != nil {
		return fmt.Errorf("while verifying server certificate: %w", err)
	}
	return err
}

func (f *Loader) GetClientCertificate(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
	credentialBundleBytes, err := os.ReadFile(filepath.Join(f.path, "credential-bundle.private-key.pem"))
	if err != nil {
		return nil, fmt.Errorf("while reading credential-bundle.private-key.pem: %w", err)
	}

	cert := &tls.Certificate{}

	var block *pem.Block
	rest := credentialBundleBytes
	for {
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}

		switch block.Type {
		case "PRIVATE KEY":
			cert.PrivateKey, err = x509.ParsePKCS8PrivateKey(block.Bytes)
			if err != nil {
				return nil, fmt.Errorf("while parsing private key from credential bundle: %w", err)
			}
		case "CERTIFICATE":
			cert.Certificate = append(cert.Certificate, block.Bytes)
		}
	}

	if cert.PrivateKey == nil {
		return nil, fmt.Errorf("client credential bundle had no private key")
	}

	if len(cert.Certificate) == 0 {
		return nil, fmt.Errorf("client credential bundle had no certificates")
	}

	return cert, nil
}
