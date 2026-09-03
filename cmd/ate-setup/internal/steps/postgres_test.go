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

package steps

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
)

func TestUseBundledPostgres(t *testing.T) {
	for _, tc := range []struct {
		name       string
		connString string
		want       bool
	}{
		{name: "no external database", connString: "", want: true},
		{
			name:       "external database configured",
			connString: "postgresql://user@db.example.com:5432/atepg",
			want:       false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &Env{Cfg: &config.Config{PostgresConnectionString: tc.connString}}
			if got := e.useBundledPostgres(); got != tc.want {
				t.Errorf("useBundledPostgres() = %v, want %v", got, tc.want)
			}
		})
	}
}

// The StatefulSet lives in a subdirectory that the bundle render does not
// descend into, so DeployAteSystem has to apply it by name. A rename that
// misses Manifest("postgres", "postgres.yaml") would otherwise only surface
// as a failed GKE install.
func TestBundledPostgresManifestExists(t *testing.T) {
	cfg := &config.Config{Root: repoRoot(t)}
	path := cfg.Manifest("postgres", "postgres.yaml")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("os.Stat(%s) = %v, want the bundled PostgreSQL manifest", path, err)
	}
	// It must not also sit at the top level, where `ko resolve -f
	// manifests/ate-install` would apply it regardless of the skip.
	stray := filepath.Join(cfg.Root, "manifests", "ate-install", "postgres.yaml")
	if _, err := os.Stat(stray); err == nil {
		t.Errorf("%s exists; the bundle render would apply it even for external databases", stray)
	}
}
