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
	"encoding/pem"
	"fmt"
	"os"
)

// Reading the two file shapes a Kubernetes projected volume writes.
//
// A *credential bundle*, from a podCertificate source, is one file holding a
// PRIVATE KEY block followed by the certificate chain, leaf first. A *trust
// bundle*, from a clusterTrustBundle source, is CA certificates and nothing
// else.
//
// Substrate parses both in internal/credbundle, deliberately not imported
// here. This directory is meant to be copied out of the repository as the
// starting point for someone else's ext_proc plugin, and an example that only
// builds inside this module is not much of an example.

// loadCredentialBundle returns a GetCertificate function for a tls.Config.
//
// The file is re-read per handshake rather than cached, which is what picks up
// a pod-certificate rotation without a restart. The cost lands once per
// connection, not once per request: Envoy holds its ext_proc gRPC connections
// open and multiplexes every intercepted request over them, so handshakes here
// are rare.
func loadCredentialBundle(path string) func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
		return parseCredentialBundle(path)
	}
}

// loadTrustBundle is the other half of an mTLS config: the CAs peers are
// verified against, re-read per handshake for the same reason.
//
// Reading it once at startup instead is a latent outage. Nothing fails at the
// moment a CA rotates, and then every handshake starts failing whenever the
// process next restarts or the old CA leaves the bundle -- which for a
// fail-closed checkpoint means denied egress rather than degraded egress.
//
// Wire the result in through GetConfigForClient, not ClientCAs: that field is
// read once, when the config is built.
func loadTrustBundle(path string) func() (*x509.CertPool, error) {
	return func() (*x509.CertPool, error) {
		pemBytes, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("while reading trust bundle: %w", err)
		}
		pool := x509.NewCertPool()
		// An empty or certificate-free file is an error rather than an empty
		// pool. An empty pool verifies nothing, so as the ClientCAs of a server
		// requiring client certificates it rejects every peer -- worth
		// reporting where it is caused rather than one handshake later.
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("no CA certificates found in trust bundle %q", path)
		}
		return pool, nil
	}
}

// parseCredentialBundle reads the private key and certificate chain out of one
// credential bundle file.
func parseCredentialBundle(path string) (*tls.Certificate, error) {
	bundleBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("while reading credential bundle: %w", err)
	}

	var leafKeyBytes []byte
	var chainBytes [][]byte

	for {
		var block *pem.Block
		block, bundleBytes = pem.Decode(bundleBytes)
		if block == nil {
			break
		}
		switch block.Type {
		case "CERTIFICATE":
			chainBytes = append(chainBytes, block.Bytes)
		case "PRIVATE KEY":
			leafKeyBytes = block.Bytes
		default:
			return nil, fmt.Errorf("unknown PEM block type %q", block.Type)
		}
	}

	if leafKeyBytes == nil {
		return nil, fmt.Errorf("no PRIVATE KEY block found in %q", path)
	}
	if len(chainBytes) == 0 {
		return nil, fmt.Errorf("no CERTIFICATE blocks found in %q", path)
	}

	leafKey, err := x509.ParsePKCS8PrivateKey(leafKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("while parsing private key: %w", err)
	}
	leafCert, err := x509.ParseCertificate(chainBytes[0])
	if err != nil {
		return nil, fmt.Errorf("while parsing leaf certificate: %w", err)
	}

	return &tls.Certificate{
		Certificate: chainBytes,
		Leaf:        leafCert,
		PrivateKey:  leafKey,
	}, nil
}
