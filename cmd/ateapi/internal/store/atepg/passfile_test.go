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

package atepg

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// writePassfile atomically replaces path with a one-line passfile, the way
// the kubelet swaps a ConfigMap or Secret volume.
func writePassfile(t *testing.T, path, password string) {
	t.Helper()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte("*:*:*:*:"+password+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

// pgx reads the passfile when the connection string is parsed, so without
// BeforeConnect a pool would keep presenting the password it started with.
func TestPoolConfigRereadsPassfile(t *testing.T) {
	passfile := filepath.Join(t.TempDir(), "pgpass")
	writePassfile(t, passfile, "first")

	cfg, err := poolConfig("postgres://runtime@postgres:5432/atepg?sslmode=disable&passfile="+passfile, "test_role")
	if err != nil {
		t.Fatalf("poolConfig: %v", err)
	}
	if cfg.ConnConfig.Password != "first" {
		t.Fatalf("password at parse time = %q, want first", cfg.ConnConfig.Password)
	}

	writePassfile(t, passfile, "second")
	conn := cfg.ConnConfig.Copy()
	if err := cfg.BeforeConnect(context.Background(), conn); err != nil {
		t.Fatalf("BeforeConnect: %v", err)
	}
	if conn.Password != "second" {
		t.Errorf("password for a new connection = %q, want second", conn.Password)
	}
}

// End to end against a real server: after the login's password changes and
// the passfile follows, the same pool opens new connections without a
// restart.
func TestPoolFollowsPassfileRotation(t *testing.T) {
	admin := requirePool(t)
	ctx := t.Context()
	const login = "passfile_rotation_test"
	if _, err := admin.Exec(ctx, `DROP ROLE IF EXISTS `+login+`; CREATE ROLE `+login+` LOGIN PASSWORD 'first'`); err != nil {
		t.Fatalf("creating login: %v", err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), `DROP ROLE IF EXISTS `+login) })

	u, err := url.Parse(containerDSN)
	if err != nil {
		t.Fatal(err)
	}
	passfile := filepath.Join(t.TempDir(), "pgpass")
	writePassfile(t, passfile, "first")
	q := u.Query()
	q.Set("passfile", passfile)
	u.RawQuery = q.Encode()
	u.User = url.User(login)

	cfg, err := poolConfig(u.String(), login)
	if err != nil {
		t.Fatalf("poolConfig: %v", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	assertCurrentUser(t, pool, login)

	if _, err := admin.Exec(ctx, `ALTER ROLE `+login+` PASSWORD 'second'`); err != nil {
		t.Fatalf("changing password: %v", err)
	}
	writePassfile(t, passfile, "second")
	// Drop the connection opened with the old password, so the next query
	// has to log in again.
	pool.Reset()
	assertCurrentUser(t, pool, login)
}

func assertCurrentUser(t *testing.T, pool *pgxpool.Pool, want string) {
	t.Helper()
	var got string
	if err := pool.QueryRow(t.Context(), `SELECT session_user`).Scan(&got); err != nil {
		t.Fatalf("querying through the pool: %v", err)
	}
	if got != want {
		t.Fatalf("session_user = %q, want %q", got, want)
	}
}
