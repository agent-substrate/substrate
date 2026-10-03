// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package atemy

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/authz"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// clearAll empties every table so the next test starts from an empty store
// without paying for a fresh database. Nothing in production mass-deletes
// state, so the statements live here rather than on Persistence.
func clearAll(t *testing.T, p *Persistence) {
	t.Helper()
	for _, stmt := range []string{
		`DELETE FROM atespaces`,
		`DELETE FROM global_access_policy`,
		`DELETE FROM atespace_access_policies`,
		`DELETE FROM actors`,
		`DELETE FROM actor_egress_policies`,
		`DELETE FROM actor_templates`,
		`DELETE FROM tags`,
		`DELETE FROM workers`,
		`DELETE FROM worker_assignments`,
		`DELETE FROM leases`,
		`DELETE FROM worker_outbox`,
		`UPDATE worker_outbox_trim SET seq = 0`,
		`DELETE FROM tuple`,
		`DELETE FROM changelog`,
	} {
		if _, err := p.db.ExecContext(context.Background(), stmt); err != nil {
			t.Fatalf("clearing tables (%s): %v", stmt, err)
		}
	}
}

func setupMySQLPersistence(t *testing.T) *Persistence {
	t.Helper()
	ctx := context.Background()
	p, err := NewPersistence(ctx, requireDB(t))
	if err != nil {
		t.Fatalf("NewPersistence failed: %v", err)
	}
	t.Cleanup(p.Close)
	clearAll(t, p)
	setTestPolicyManager(t, p)
	return p
}

// setTestPolicyManager gives p an OpenFGA-backed PolicyManager, as the server
// always does.
func setTestPolicyManager(t *testing.T, p *Persistence) {
	t.Helper()
	fgaServer, err := authz.NewOpenFGAServer(authz.MySQLBackend(p.db))
	if err != nil {
		t.Fatalf("NewOpenFGAServer failed: %v", err)
	}
	t.Cleanup(fgaServer.Close)
	_, policyManager, err := authz.New(t.Context(), authz.MySQLBackend(p.db), fgaServer, nil)
	if err != nil {
		t.Fatalf("authz.New failed: %v", err)
	}
	p.SetPolicyManager(policyManager)
}

func newTestAtespace(name string) *ateapipb.Atespace {
	return &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: name}}
}

func createTestAtespace(t *testing.T, s *Persistence, name string) {
	t.Helper()
	if _, err := s.CreateAtespace(context.Background(), newTestAtespace(name)); err != nil {
		t.Fatalf("CreateAtespace(%q) failed: %v", name, err)
	}
}

// newReplica opens a second Persistence on the shared database, standing in
// for another ateapi replica. Its watchers see only polled outbox rows, never
// the commit-time publish of writes made through another Persistence.
func newReplica(t *testing.T, p *Persistence) *Persistence {
	t.Helper()
	replica, err := NewPersistence(t.Context(), p.db)
	if err != nil {
		t.Fatalf("NewPersistence for a replica failed: %v", err)
	}
	t.Cleanup(replica.Close)
	return replica
}

// adminDSN returns a root connection string for database on the shared
// container. The testcontainers module gives root the application password.
func adminDSN(t *testing.T, database string) string {
	t.Helper()
	requireDB(t)
	cfg, err := mysql.ParseDSN(containerDSN)
	if err != nil {
		t.Fatalf("parsing container DSN: %v", err)
	}
	cfg.User = "root"
	cfg.DBName = database
	return cfg.FormatDSN()
}

// openAdmin opens a root pool on database, for statements the application
// user may not run: creating databases and reading server-wide lock state.
func openAdmin(t *testing.T, database string) *sql.DB {
	t.Helper()
	db, err := Open(adminDSN(t, database))
	if err != nil {
		t.Fatalf("opening admin pool: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// createTestDatabase creates an empty database, dropped when the test ends,
// and returns a root connection string for it.
func createTestDatabase(t *testing.T, name string) string {
	t.Helper()
	admin := openAdmin(t, "mysql")
	if _, err := admin.ExecContext(t.Context(), "DROP DATABASE IF EXISTS `"+name+"`"); err != nil {
		t.Fatalf("dropping database %s: %v", name, err)
	}
	if _, err := admin.ExecContext(t.Context(), "CREATE DATABASE `"+name+"`"); err != nil {
		t.Fatalf("creating database %s: %v", name, err)
	}
	t.Cleanup(func() {
		if _, err := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS `"+name+"`"); err != nil {
			t.Errorf("dropping database %s: %v", name, err)
		}
	})
	return adminDSN(t, name)
}

// waitForLockWait blocks until a transaction is waiting on a row lock, so a
// test can order a commit after another transaction has blocked. It fails if
// done delivers first: the operation expected to block finished instead.
func waitForLockWait(t *testing.T, done <-chan error) {
	t.Helper()
	admin := openAdmin(t, "mysql")
	deadline := time.Now().Add(10 * time.Second)
	for {
		select {
		case err := <-done:
			t.Fatalf("operation finished with %v, want it blocked on a row lock", err)
		default:
		}
		var waiting int
		// performance_schema is live; information_schema.INNODB_TRX is a
		// cache that frequent polling never refreshes.
		if err := admin.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM performance_schema.data_lock_waits`).Scan(&waiting); err != nil {
			t.Fatalf("reading lock waits: %v", err)
		}
		if waiting > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no transaction ever waited on a row lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// countRows runs a COUNT query against p's database.
func countRows(t *testing.T, p *Persistence, query string, args ...any) int {
	t.Helper()
	var n int
	if err := p.db.QueryRowContext(t.Context(), query, args...).Scan(&n); err != nil {
		t.Fatalf("counting rows (%s): %v", query, err)
	}
	return n
}
