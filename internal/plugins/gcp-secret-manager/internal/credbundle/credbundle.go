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

// Package credbundle loads Kubernetes Pod Certificate credential bundles and PEM
// trust bundles. A credential bundle is one PEM file holding a PRIVATE KEY block
// and the CERTIFICATE chain, leaf first, with or without the root.
//
// It is the server-side subset of substrate's internal/credbundle, copied so
// this module depends only on substrate's public packages.
package credbundle

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"sync"
	"time"
)

// recheckInterval bounds how often get() stats the file: a cached value
// younger than this is served with no syscall. Tests lower it via
// SetRecheckIntervalForTesting to avoid waiting it out.
var recheckInterval = time.Minute

// SetRecheckIntervalForTesting overrides how often Loader and PoolLoader
// re-stat their file. Returns a restore function for t.Cleanup; for tests
// in other packages that need a file change visible immediately.
func SetRecheckIntervalForTesting(d time.Duration) (restore func()) {
	prev := recheckInterval
	recheckInterval = d
	return func() { recheckInterval = prev }
}

// Loader returns a tls.Config GetCertificate function serving the credential
// bundle at path. The parse is cached and redone only when the file changes, so
// a rotation is picked up within recheckInterval.
func Loader(path string) func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	c := &certCache{path: path}
	return func(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
		return c.get()
	}
}

// PoolLoader returns a function yielding the trust pool in the PEM file at
// path, cached like Loader's. Call it per connection, e.g. from
// GetConfigForClient: a tls.Config's ClientCAs is fixed once in use, so a pool
// built at startup would miss a CA rotation.
func PoolLoader(path string) func() (*x509.CertPool, error) {
	c := &poolCache{path: path}
	return c.get
}

// certCache caches a credential bundle's parse with the file stat it came from.
type certCache struct {
	path string

	mu sync.Mutex
	// fi is the stat taken just before cert was parsed; cert is served while a
	// fresh stat matches it.
	fi      os.FileInfo
	cert    *tls.Certificate
	checked time.Time
}

// get returns the cached bundle, stat'ing the file at most once per
// recheckInterval and re-parsing when that stat shows the file's identity
// (os.SameFile), mtime or size has changed. The kubelet rotates projected
// volumes by swapping a symlink, which changes identity; mtime and size catch
// in-place rewrites.
//
// A change between the stat and the read stores newer content under the older
// stat, so the next stat re-reads: a rotation is visible within one
// recheckInterval of occurring, not necessarily on the very next call. Errors
// are returned, never masked by the cache, and a later stat retries.
func (c *certCache) get() (*tls.Certificate, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cert != nil && time.Since(c.checked) < recheckInterval {
		return c.cert, nil
	}

	fi, err := os.Stat(c.path)
	if err != nil {
		return nil, fmt.Errorf("while getting file info for credential bundle %q: %w", c.path, err)
	}
	c.checked = time.Now()
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

// poolCache is certCache for a trust bundle.
type poolCache struct {
	path string

	mu      sync.Mutex
	fi      os.FileInfo
	pool    *x509.CertPool
	checked time.Time
}

// get returns the cached trust pool, re-parsing as certCache.get does.
func (c *poolCache) get() (*x509.CertPool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.pool != nil && time.Since(c.checked) < recheckInterval {
		return c.pool, nil
	}

	fi, err := os.Stat(c.path)
	if err != nil {
		return nil, fmt.Errorf("while getting file info for trust bundle %q: %w", c.path, err)
	}
	c.checked = time.Now()
	if c.pool != nil && os.SameFile(c.fi, fi) && fi.ModTime().Equal(c.fi.ModTime()) && fi.Size() == c.fi.Size() {
		return c.pool, nil
	}

	pool, err := ParsePool(c.path)
	if err != nil {
		return nil, err
	}
	c.fi, c.pool = fi, pool
	return pool, nil
}

// Parse reads the PKCS#8 private key and certificate chain from a credential
// bundle file.
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

// ParsePool reads a PEM trust bundle into a pool. A file with no certificates
// is an error, since an empty pool would silently reject every peer.
func ParsePool(path string) (*x509.CertPool, error) {
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("while reading trust bundle: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("trust bundle %q contains no certificates", path)
	}
	return pool, nil
}
