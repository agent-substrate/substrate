//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/agent-substrate/substrate/internal/credbundle"
)

func TestAteletServerTLSConfigRejectsUnreadableCACerts(t *testing.T) {
	_, err := ateletServerTLSConfig("/nonexistent-cred-bundle.pem", filepath.Join(t.TempDir(), "absent.pem"))
	if err == nil {
		t.Fatalf("ateletServerTLSConfig() error = nil, want an error for a missing CA file")
	}
}

// TestAteletServerTLSConfigReloadsCACertsWithoutRestart verifies that a
// pod-identity CA rotation on disk is picked up by the next handshake, not
// frozen at the config's construction.
func TestAteletServerTLSConfigReloadsCACertsWithoutRestart(t *testing.T) {
	t.Cleanup(credbundle.SetRecheckIntervalForTesting(0))
	path := filepath.Join(t.TempDir(), "trust-bundle.pem")
	if err := os.WriteFile(path, testCertPEM(t), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	cfg, err := ateletServerTLSConfig("/nonexistent-cred-bundle.pem", path)
	if err != nil {
		t.Fatalf("ateletServerTLSConfig() error = %v", err)
	}
	if cfg.GetConfigForClient == nil {
		t.Fatalf("ateletServerTLSConfig() did not set GetConfigForClient")
	}

	before, err := cfg.GetConfigForClient(nil)
	if err != nil {
		t.Fatalf("GetConfigForClient() first call error = %v", err)
	}

	if err := os.WriteFile(path, testCertPEM(t), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	after, err := cfg.GetConfigForClient(nil)
	if err != nil {
		t.Fatalf("GetConfigForClient() second call error = %v", err)
	}

	if before.ClientCAs.Equal(after.ClientCAs) {
		t.Fatalf("GetConfigForClient() returned the same trust pool after the CA file changed, want the rotated one")
	}
}
