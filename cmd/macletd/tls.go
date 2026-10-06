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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"

	"github.com/agent-substrate/substrate/internal/credbundle"
)

// macletdTLSConfig builds a server config which reloads both sides of mTLS on
// each new handshake while retaining the existing separate cert/key files.
func macletdTLSConfig(certPath, keyPath, clientCAPath string) (*tls.Config, error) {
	loadKeyPair := credbundle.KeyPairLoader(certPath, keyPath)
	if _, err := loadKeyPair(); err != nil {
		return nil, fmt.Errorf("load server certificate: %w", err)
	}
	loadClientCAs := credbundle.PoolLoader(clientCAPath)
	if _, err := loadClientCAs(); err != nil {
		return nil, fmt.Errorf("load client CA: %w", err)
	}
	return &tls.Config{
		MinVersion:     tls.VersionTLS13,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return loadKeyPair() },
		// Verify the client chain ourselves so the trust bundle can rotate on
		// each handshake without replacing this config and losing gRPC's h2
		// ALPN settings.
		ClientAuth: tls.RequireAnyClientCert,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("client certificate is required")
			}
			leaf, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return fmt.Errorf("parse client certificate: %w", err)
			}
			intermediates := x509.NewCertPool()
			for _, raw := range rawCerts[1:] {
				cert, err := x509.ParseCertificate(raw)
				if err != nil {
					return fmt.Errorf("parse client certificate chain: %w", err)
				}
				intermediates.AddCert(cert)
			}
			roots, err := loadClientCAs()
			if err != nil {
				return err
			}
			_, err = leaf.Verify(x509.VerifyOptions{
				Roots: roots, Intermediates: intermediates,
				KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
			})
			return err
		},
	}, nil
}
