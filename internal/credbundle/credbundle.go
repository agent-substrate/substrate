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
	"sync"
)

// Loader reads a private key and certificate chain from a credential bundle file as written by the
// Kubernetes Pod Certificates mechanism.
//
// Returns a function that can be used as GetCertificate in a tls.Config. The parsed bundle is
// cached: each handshake stats the file and re-reads it only when the file has changed, so
// pod-certificate rotations are picked up on the next handshake without paying the read and
// parse cost when nothing changed.
func Loader(path string) func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	c := &certCache{path: path}
	return func(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
		return c.get()
	}
}

// ClientLoader is the client-side counterpart to Loader. It returns a function
// suitable for use as GetClientCertificate in a tls.Config, caching the parsed
// bundle in the same way so that pod-certificate rotations are picked up on
// the next handshake.
func ClientLoader(path string) func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	c := &certCache{path: path}
	return func(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
		return c.get()
	}
}

// certCache holds the parse of a credential bundle file together with the stat
// of the file it was parsed from, so unchanged files are not re-parsed on
// every TLS handshake.
type certCache struct {
	path string

	mu sync.Mutex
	// fi is the stat of path taken just before cert was parsed; nil until the
	// first successful parse. cert is served while a fresh stat of path still
	// matches it.
	fi   os.FileInfo
	cert *tls.Certificate
}

// get returns the parsed bundle, re-reading the file only when it has changed
// since the last successful parse.
//
// A change is any difference in file identity (os.SameFile: device and inode
// on Unix), modification time, or size. The kubelet rotates projected volume
// contents, including pod certificates, by writing a fresh timestamped
// directory and atomically swapping a symlink to it, so a rotation always
// surfaces here as a change of file identity; mtime and size additionally
// cover in-place rewrites, whose mid-write states a reader may also observe.
//
// The file can still change between the stat and the read below. The parse of
// the newer content is then stored against the older stat, so the next call
// sees a stat mismatch and re-reads; the cache never lags a rotation by more
// than one handshake. Errors leave the previous entry in place and are
// returned to the caller: handshakes fail exactly as they would without
// caching, and every later call retries until a parse succeeds.
func (c *certCache) get() (*tls.Certificate, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	fi, err := os.Stat(c.path)
	if err != nil {
		return nil, fmt.Errorf("while getting file info for credential bundle %q: %w", c.path, err)
	}
	if c.cert != nil && os.SameFile(c.fi, fi) && fi.ModTime().Equal(c.fi.ModTime()) && fi.Size() == c.fi.Size() {
		return c.cert, nil
	}

	cert, err := Parse(c.path)
	if err != nil {
		return nil, err
	}
	c.fi, c.cert = fi, cert
	return cert, nil
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
