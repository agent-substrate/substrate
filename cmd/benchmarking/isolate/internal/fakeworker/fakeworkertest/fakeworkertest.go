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

// Package fakeworkertest issues pod-identity-shaped certificates for tests of
// the fake data plane's capacity relay: a throwaway CA, and leaf certificates
// that carry a SPIFFE ID as their only SAN, as the pod-identity signer's do.
package fakeworkertest

// TODO: Move this to internal/credbundle/credbundletest. Nothing here is
// specific to fake Workers: it writes the files internal/credbundle loads.

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/localca"
)

// CA is a throwaway signer whose files live in a test's temp dir.
type CA struct {
	pool *localca.ConcretePool
	dir  string
	// TrustBundle is the path of the CA certificate as a PEM trust bundle.
	TrustBundle string
}

// NewCA creates a CA and writes its trust bundle.
func NewCA(t *testing.T) *CA {
	t.Helper()
	root, err := localca.GenerateCA("fakeworkertest", localca.KeyTypeECDSAP256, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	ca := &CA{pool: &localca.ConcretePool{CAs: []*localca.CA{root}}, dir: dir, TrustBundle: filepath.Join(dir, "trust-bundle.pem")}
	writePEM(t, ca.TrustBundle, pem.Block{Type: "CERTIFICATE", Bytes: root.RootCertificate.Raw})
	return ca
}

// Issue writes a credential bundle (leaf certificate, then its PKCS #8 key)
// for spiffeID and returns its path.
func (ca *CA) Issue(t *testing.T, spiffeID string) string {
	t.Helper()
	id, err := url.Parse(spiffeID)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		URIs:         []*url.URL{id},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	chain, err := ca.pool.CreateCertificate(tmpl, &key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	pkcs8, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(ca.dir, serial.String()+"-bundle.pem")
	writePEM(t, path, pem.Block{Type: "CERTIFICATE", Bytes: chain[0]}, pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8})
	return path
}

// Rotate replaces the trust bundle at path with the certificates of cas, as
// kubelet rotates a projected trust bundle: a new file renamed over the old.
func Rotate(t *testing.T, path string, cas ...*CA) {
	t.Helper()
	var blocks []pem.Block
	for _, ca := range cas {
		blocks = append(blocks, pem.Block{Type: "CERTIFICATE", Bytes: ca.pool.CAs[0].RootCertificate.Raw})
	}
	tmp := path + ".new"
	writePEM(t, tmp, blocks...)
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

func writePEM(t *testing.T, path string, blocks ...pem.Block) {
	t.Helper()
	var out []byte
	for _, b := range blocks {
		out = append(out, pem.EncodeToMemory(&b)...)
	}
	if err := os.WriteFile(path, out, 0o600); err != nil {
		t.Fatal(err)
	}
}
