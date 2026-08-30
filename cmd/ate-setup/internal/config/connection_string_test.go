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

package config

import (
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// installerDSN matches the single echo inside default_postgres_connection_string
// in hack/install-ate.sh.
var installerDSN = regexp.MustCompile(`default_postgres_connection_string\(\) \{\s*echo "([^"]+)"`)

// repoRoot walks up from this test file to the repository root.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed; cannot locate the repository root")
	}
	// cmd/ate-setup/internal/config/<this file> -> four levels up.
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
}

// TestDefaultPostgresConnectionStringMatchesInstaller pins the Go constant to
// the shell installer's copy. The two are hand-duplicated -- ate-setup and
// hack/install-ate.sh each build the same install -- and nothing but this test
// stops them drifting into two different pool sizes against one
// max_connections budget.
func TestDefaultPostgresConnectionStringMatchesInstaller(t *testing.T) {
	installer := filepath.Join(repoRoot(t), "hack", "install-ate.sh")
	script, err := os.ReadFile(installer)
	if err != nil {
		t.Fatalf("reading %s: %v", installer, err)
	}
	m := installerDSN.FindSubmatch(script)
	if m == nil {
		t.Fatalf("no default_postgres_connection_string() echo found in %s; if the function was reshaped, update installerDSN here", installer)
	}
	if got, want := string(m[1]), DefaultPostgresConnectionString; got != want {
		t.Errorf("DSN drift between the installer and DefaultPostgresConnectionString.\nhack/install-ate.sh: %s\nconfig.go:           %s", got, want)
	}
}

// TestDefaultPostgresConnectionStringCapsThePool asserts the shipped DSN
// actually caps ateapi's main pgxpool rather than merely carrying a parameter
// the driver ignores. pool_max_conns is a pgxpool-level key: pgxpool.ParseConfig
// consumes it into MaxConns and strips it, while plain pgx.ParseConfig would
// leave it in RuntimeParams and the server would reject it as an unrecognized
// configuration parameter. atepg only ever parses this DSN through
// pgxpool.ParseConfig, so both halves are checked here.
func TestDefaultPostgresConnectionStringCapsThePool(t *testing.T) {
	const wantMaxConns = 12

	u, err := url.Parse(DefaultPostgresConnectionString)
	if err != nil {
		t.Fatalf("parsing DefaultPostgresConnectionString: %v", err)
	}
	q := u.Query()
	if got := q.Get("pool_max_conns"); got != "12" {
		t.Fatalf("pool_max_conns = %q, want \"12\"; it is a term in the max_connections budget in manifests/ate-install/postgres.yaml -- change both together", got)
	}

	// The TLS material lives on the apiserver pod, not on a test machine, and
	// ParseConfig reads those files eagerly. Drop them: the pool sizing this
	// test is about is independent of the transport.
	for _, key := range []string{"sslrootcert", "sslcert", "sslkey"} {
		q.Del(key)
	}
	q.Set("sslmode", "disable")
	u.RawQuery = q.Encode()

	cfg, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		t.Fatalf("pgxpool.ParseConfig: %v", err)
	}
	if cfg.MaxConns != wantMaxConns {
		t.Errorf("pool MaxConns = %d, want %d", cfg.MaxConns, wantMaxConns)
	}
	if v, ok := cfg.ConnConfig.RuntimeParams["pool_max_conns"]; ok {
		t.Errorf("pool_max_conns survived into RuntimeParams as %q; it would reach the server as an unrecognized configuration parameter", v)
	}
}
