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

// Package credbundle handles credential bundle files written by Kubernetes Pod Certificates.
//
// A credential bundle is a single file with multiple PEM entries. The first entry is a PRIVATE KEY
// block, and all remaining entries are CERTIFICATE blocks. The CERTIFICATE blocks are in
// leaf-to-root order, and may or may not include the root.
package credbundle

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
)

// Loader reads a private key and certificate chain from a credential bundle file as written by the
// Kubernetes Pod Certificates mechanism.
//
// Returns a function that can be used as GetCertificate in a tls.Config
func Loader(path string) func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	// TODO: Introduce caching.
	return func(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
		return Parse(path)
	}
}

// Parse reads a private key and certificate chain from a credential bundle file as written by the
// Kubernetes Pod Certificates mechanism.
func Parse(bundlePath string) (*tls.Certificate, error) {
	bundleBytes, err := os.ReadFile(bundlePath)
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
		return nil, fmt.Errorf("no PRIVATE KEY block found")
	}

	if len(chainBytes) == 0 {
		return nil, fmt.Errorf("no CERTIFICATE blocks found")
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
