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
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/credbundle"
)

func testCertPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
	}, &x509.Certificate{SerialNumber: big.NewInt(1)}, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestServerTLSConfigRejectsUnreadableCACerts(t *testing.T) {
	if _, err := serverTLSConfig("/nonexistent-cred-bundle.pem", filepath.Join(t.TempDir(), "absent.pem")); err == nil {
		t.Fatal("serverTLSConfig() error = nil, want an error for a missing CA file")
	}
}

// A pod-identity CA rotation on disk must reach the next handshake: a pool
// frozen at startup would refuse ate-api-server once its certificate is signed
// by the new CA, which a long benchmark would read as a data plane outage.
func TestServerTLSConfigReloadsCACertsWithoutRestart(t *testing.T) {
	t.Cleanup(credbundle.SetRecheckIntervalForTesting(0))
	path := filepath.Join(t.TempDir(), "trust-bundle.pem")
	if err := os.WriteFile(path, testCertPEM(t), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := serverTLSConfig("/nonexistent-cred-bundle.pem", path)
	if err != nil {
		t.Fatalf("serverTLSConfig() error = %v", err)
	}
	if cfg.GetConfigForClient == nil {
		t.Fatal("serverTLSConfig() did not set GetConfigForClient")
	}
	before, err := cfg.GetConfigForClient(nil)
	if err != nil {
		t.Fatalf("GetConfigForClient() first call error = %v", err)
	}
	if err := os.WriteFile(path, testCertPEM(t), 0o600); err != nil {
		t.Fatal(err)
	}
	after, err := cfg.GetConfigForClient(nil)
	if err != nil {
		t.Fatalf("GetConfigForClient() second call error = %v", err)
	}
	if before.ClientCAs.Equal(after.ClientCAs) {
		t.Fatal("GetConfigForClient() returned the same trust pool after the CA file changed, want the rotated one")
	}
}
