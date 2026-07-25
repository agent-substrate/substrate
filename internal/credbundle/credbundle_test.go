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

package credbundle

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"sync"
	"testing"
	"time"
)

func TestParsePKCS8PrivateKeyBlock(t *testing.T) {
	key := generateRSAKey(t)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal PKCS8 key: %v", err)
	}
	certDER := generateCertificate(t, 1)
	bundle := append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})...)

	bundlePath := writeBundle(t, bundle)
	cert, err := Parse(bundlePath)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(cert.Certificate) != 1 {
		t.Fatalf("Parse() certificate chain length = %d, want 1", len(cert.Certificate))
	}
	if cert.PrivateKey == nil {
		t.Fatalf("Parse() private key is nil")
	}
	if cert.Leaf == nil {
		t.Fatalf("Parse() leaf certificate is nil")
	}
}

func TestParseRejectsNonPKCS8PrivateKeyBlock(t *testing.T) {
	certDER := generateCertificate(t, 1)
	bundle := append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}), pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(generateRSAKey(t))})...)

	bundlePath := writeBundle(t, bundle)
	if _, err := Parse(bundlePath); err == nil {
		t.Fatalf("Parse() error = nil, want unsupported private key block error")
	}
}

func TestLoaderServesCachedParseWhileFileUnchanged(t *testing.T) {
	bundle := makeBundle(t, 7)
	path := writeBundle(t, bundle)
	getCert := Loader(path)

	if _, err := getCert(nil); err != nil {
		t.Fatalf("Loader() first call error = %v", err)
	}

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat bundle: %v", err)
	}
	// Replace the content with same-length garbage and restore the mtime, so
	// file identity (inode), size, and mtime all still match the cached stat.
	// The cached parse must be served; any re-read would fail loudly on the
	// garbage.
	if err := os.WriteFile(path, bytes.Repeat([]byte("x"), len(bundle)), 0o600); err != nil {
		t.Fatalf("overwrite bundle: %v", err)
	}
	if err := os.Chtimes(path, fi.ModTime(), fi.ModTime()); err != nil {
		t.Fatalf("restore mtime: %v", err)
	}

	cert, err := getCert(nil)
	if err != nil {
		t.Fatalf("Loader() with unchanged stat error = %v", err)
	}
	if got := leafSerial(t, cert); got != 7 {
		t.Fatalf("Loader() leaf serial = %d, want cached 7", got)
	}
}

func TestLoaderPicksUpAtomicRotation(t *testing.T) {
	path := writeBundle(t, makeBundle(t, 1))
	getCert := Loader(path)

	cert, err := getCert(nil)
	if err != nil {
		t.Fatalf("Loader() first call error = %v", err)
	}
	if got := leafSerial(t, cert); got != 1 {
		t.Fatalf("Loader() leaf serial = %d, want 1", got)
	}

	// Rotate the way the kubelet does: write the new bundle to a fresh file
	// and atomically rename it over the old path, giving it a new inode.
	next := path + ".next"
	if err := os.WriteFile(next, makeBundle(t, 2), 0o600); err != nil {
		t.Fatalf("write rotated bundle: %v", err)
	}
	if err := os.Rename(next, path); err != nil {
		t.Fatalf("rename rotated bundle: %v", err)
	}

	cert, err = getCert(nil)
	if err != nil {
		t.Fatalf("Loader() after rotation error = %v", err)
	}
	if got := leafSerial(t, cert); got != 2 {
		t.Fatalf("Loader() leaf serial after rotation = %d, want 2", got)
	}
}

func TestLoaderPicksUpInPlaceRewrite(t *testing.T) {
	path := writeBundle(t, makeBundle(t, 1))
	getCert := Loader(path)

	if _, err := getCert(nil); err != nil {
		t.Fatalf("Loader() first call error = %v", err)
	}

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat bundle: %v", err)
	}
	// Rewrite the file in place (same inode) and push the mtime forward so
	// the change is visible even on filesystems with coarse timestamps.
	if err := os.WriteFile(path, makeBundle(t, 2), 0o600); err != nil {
		t.Fatalf("rewrite bundle: %v", err)
	}
	bumped := fi.ModTime().Add(time.Second)
	if err := os.Chtimes(path, bumped, bumped); err != nil {
		t.Fatalf("bump mtime: %v", err)
	}

	cert, err := getCert(nil)
	if err != nil {
		t.Fatalf("Loader() after rewrite error = %v", err)
	}
	if got := leafSerial(t, cert); got != 2 {
		t.Fatalf("Loader() leaf serial after rewrite = %d, want 2", got)
	}
}

func TestLoaderErrorWhenBundleMissing(t *testing.T) {
	getCert := Loader(t.TempDir() + "/absent.pem")
	if _, err := getCert(nil); err == nil {
		t.Fatalf("Loader() error = nil, want missing-file error")
	}
}

func TestLoaderRecoversAfterError(t *testing.T) {
	bundle := makeBundle(t, 3)
	path := writeBundle(t, bundle)
	getCert := Loader(path)

	if _, err := getCert(nil); err != nil {
		t.Fatalf("Loader() first call error = %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove bundle: %v", err)
	}
	if _, err := getCert(nil); err == nil {
		t.Fatalf("Loader() error = nil after bundle removed, want error")
	}
	if err := os.WriteFile(path, bundle, 0o600); err != nil {
		t.Fatalf("restore bundle: %v", err)
	}

	cert, err := getCert(nil)
	if err != nil {
		t.Fatalf("Loader() after restore error = %v", err)
	}
	if got := leafSerial(t, cert); got != 3 {
		t.Fatalf("Loader() leaf serial after restore = %d, want 3", got)
	}
}

func TestClientLoaderCachesAndReloads(t *testing.T) {
	path := writeBundle(t, makeBundle(t, 1))
	getCert := ClientLoader(path)

	cert, err := getCert(nil)
	if err != nil {
		t.Fatalf("ClientLoader() first call error = %v", err)
	}
	if got := leafSerial(t, cert); got != 1 {
		t.Fatalf("ClientLoader() leaf serial = %d, want 1", got)
	}

	next := path + ".next"
	if err := os.WriteFile(next, makeBundle(t, 2), 0o600); err != nil {
		t.Fatalf("write rotated bundle: %v", err)
	}
	if err := os.Rename(next, path); err != nil {
		t.Fatalf("rename rotated bundle: %v", err)
	}

	cert, err = getCert(nil)
	if err != nil {
		t.Fatalf("ClientLoader() after rotation error = %v", err)
	}
	if got := leafSerial(t, cert); got != 2 {
		t.Fatalf("ClientLoader() leaf serial after rotation = %d, want 2", got)
	}
}

func TestLoaderConcurrentHandshakes(t *testing.T) {
	bundles := [][]byte{makeBundle(t, 1), makeBundle(t, 2)}
	path := writeBundle(t, bundles[0])
	getCert := Loader(path)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 25; i++ {
			// Atomic-rename rotation, so readers never observe a torn file.
			next := path + ".next"
			if err := os.WriteFile(next, bundles[i%2], 0o600); err != nil {
				t.Errorf("write rotated bundle: %v", err)
				return
			}
			if err := os.Rename(next, path); err != nil {
				t.Errorf("rename rotated bundle: %v", err)
				return
			}
		}
	}()
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				cert, err := getCert(nil)
				if err != nil {
					t.Errorf("Loader() error = %v", err)
					return
				}
				if cert.Leaf == nil {
					t.Errorf("Loader() returned certificate with nil leaf")
					return
				}
				if s := cert.Leaf.SerialNumber.Int64(); s != 1 && s != 2 {
					t.Errorf("Loader() leaf serial = %d, want 1 or 2", s)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func generateRSAKey(t testing.TB) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	return key
}

func generateCertificate(t testing.TB, serial int64) []byte {
	t.Helper()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: "api.ate-system.svc"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"api.ate-system.svc"},
	}
	key := generateRSAKey(t)
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	return der
}

func writeBundle(t testing.TB, bundle []byte) string {
	t.Helper()
	path := t.TempDir() + "/bundle.pem"
	if err := os.WriteFile(path, bundle, 0o600); err != nil {
		t.Fatalf("write bundle: %v", err)
	}
	return path
}

// makeBundle returns a PEM credential bundle whose leaf certificate carries
// the given serial number, so tests can tell which bundle a parsed
// certificate came from.
func makeBundle(t testing.TB, serial int64) []byte {
	t.Helper()
	keyDER, err := x509.MarshalPKCS8PrivateKey(generateRSAKey(t))
	if err != nil {
		t.Fatalf("marshal PKCS8 key: %v", err)
	}
	return append(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: generateCertificate(t, serial)}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})...,
	)
}

func leafSerial(t *testing.T, cert *tls.Certificate) int64 {
	t.Helper()
	if cert == nil || cert.Leaf == nil {
		t.Fatalf("parsed bundle has no leaf certificate")
	}
	return cert.Leaf.SerialNumber.Int64()
}

func BenchmarkLoaderCached(b *testing.B) {
	getCert := Loader(writeBundle(b, makeBundle(b, 1)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := getCert(nil); err != nil {
			b.Fatalf("Loader() error = %v", err)
		}
	}
}

// BenchmarkParseUncached is the per-handshake cost before caching: a full
// read and parse of the bundle on every call.
func BenchmarkParseUncached(b *testing.B) {
	path := writeBundle(b, makeBundle(b, 1))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Parse(path); err != nil {
			b.Fatalf("Parse() error = %v", err)
		}
	}
}
