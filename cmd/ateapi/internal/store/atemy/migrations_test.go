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

package atemy

import (
	"database/sql"
	"io/fs"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/go-cmp/cmp"
	"github.com/openfga/openfga/assets"
	"github.com/pressly/goose/v3/database"
)

const pinnedOpenFGAMigrationVersion = 8

var (
	transactionControl     = regexp.MustCompile(`(?im)^\s*(BEGIN|START\s+TRANSACTION|COMMIT|ROLLBACK)\s*;`)
	createTable            = regexp.MustCompile(`(?i)\bCREATE\s+TABLE\b`)
	createTableIfNotExists = regexp.MustCompile(`(?i)\bCREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\b`)
	insertStatement        = regexp.MustCompile(`(?is)\bINSERT\s+INTO\b[^;]*;`)
)

// migrationVersion parses the numeric prefix of a migration file name.
func migrationVersion(t *testing.T, name string) int {
	t.Helper()
	prefix, _, ok := strings.Cut(filepath.Base(name), "_")
	if !ok {
		t.Fatalf("unexpected migration filename %q", name)
	}
	v, err := strconv.Atoi(prefix)
	if err != nil {
		t.Fatalf("parse migration version from %q: %v", name, err)
	}
	return v
}

// TestOpenFGAMigrationVersionGuard fails when a github.com/openfga/openfga bump
// adds an upstream MySQL migration. migrations/000002_openfga.sql holds the
// final state of upstream migrations 001 through 008, and
// cmd/ateapi/internal/authz/datastore_mysql.go adapts upstream queries to run
// on the caller's transaction. Port each new upstream migration and recheck
// those queries.
func TestOpenFGAMigrationVersionGuard(t *testing.T) {
	entries, err := fs.ReadDir(assets.EmbedMigrations, assets.MySQLMigrationDir)
	if err != nil {
		t.Fatalf("read embedded OpenFGA migrations: %v", err)
	}
	var maxVersion int
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}
		maxVersion = max(maxVersion, migrationVersion(t, entry.Name()))
	}
	if maxVersion != pinnedOpenFGAMigrationVersion {
		t.Fatalf(
			"OpenFGA embedded MySQL migrations are at version %d, but 000002_openfga.sql and cmd/ateapi/internal/authz/datastore_mysql.go are pinned to version %d; port any new OpenFGA DDL to cmd/ateapi/internal/store/atemy/migrations/ and verify the MySQL datastore before updating pinnedOpenFGAMigrationVersion",
			maxVersion, pinnedOpenFGAMigrationVersion,
		)
	}
}

func TestMigrationPolicy(t *testing.T) {
	err := fs.WalkDir(migrationFiles, "migrations", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		if !strings.HasSuffix(path, ".sql") {
			return nil
		}
		data, err := fs.ReadFile(migrationFiles, path)
		if err != nil {
			return err
		}
		sql := string(data)
		upperSQL := strings.ToUpper(sql)
		if strings.Count(sql, "-- +goose Up") != 1 {
			t.Errorf("%s must contain exactly one Goose Up annotation", path)
		}
		if strings.Contains(upperSQL, "-- +GOOSE DOWN") {
			t.Errorf("%s must not contain a Goose Down migration", path)
		}
		if strings.Contains(upperSQL, "-- +GOOSE NO TRANSACTION") {
			t.Errorf("%s must let Goose run it in a transaction", path)
		}
		// MySQL commits each DDL statement on its own, so a file that fails
		// partway must be safe to run again from the top.
		statements := stripSQLComments(sql)
		if creates, guarded := createTable.FindAllStringIndex(statements, -1), createTableIfNotExists.FindAllStringIndex(statements, -1); len(creates) != len(guarded) {
			t.Errorf("%s has %d CREATE TABLE statements but only %d use IF NOT EXISTS", path, len(creates), len(guarded))
		}
		for _, stmt := range insertStatement.FindAllString(statements, -1) {
			if !strings.Contains(strings.ToUpper(stmt), "ON DUPLICATE KEY UPDATE") {
				t.Errorf("%s seeds rows without ON DUPLICATE KEY UPDATE: %q", path, stmt)
			}
		}
		if strings.Contains(upperSQL, "-- +GOOSE ENVSUB") {
			t.Errorf("%s must not use Goose environment substitution", path)
		}
		if transactionControl.MatchString(sql) {
			t.Errorf("%s must let Goose control the transaction", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("check MySQL migrations: %v", err)
	}
}

// stripSQLComments drops whole-line -- comments, including Goose annotations.
func stripSQLComments(sql string) string {
	var kept []string
	for line := range strings.Lines(sql) {
		if !strings.HasPrefix(strings.TrimSpace(line), "--") {
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, "")
}

// Tables Goose did not create are refused rather than adopted by CREATE TABLE
// IF NOT EXISTS.
func TestMigrations_RejectTablesWithoutALedger(t *testing.T) {
	createTestDatabase(t, "atemy_unversioned")
	db := openAdmin(t, "atemy_unversioned")
	if _, err := db.ExecContext(t.Context(), `CREATE TABLE atespaces (name VARCHAR(255) PRIMARY KEY)`); err != nil {
		t.Fatalf("creating a stray atespaces table: %v", err)
	}
	if _, err := NewPersistence(t.Context(), db); err == nil || !strings.Contains(err.Error(), "without a migration ledger") {
		t.Fatalf("NewPersistence over tables without a ledger = %v, want a refusal", err)
	}
}

// TestMigrations_ResumeAPartialInitialMigration simulates a crash partway
// through 000001: MySQL committed some of its tables and a seed row, but
// Goose recorded no version. The next startup must complete the file.
func TestMigrations_ResumeAPartialInitialMigration(t *testing.T) {
	createTestDatabase(t, "atemy_partial_migration")
	db := openAdmin(t, "atemy_partial_migration")
	ctx := t.Context()
	initial, err := fs.ReadFile(migrationFiles, "migrations/000001_initial.sql")
	if err != nil {
		t.Fatalf("reading 000001: %v", err)
	}
	// Goose creates its ledger, holding version 0, before it runs 000001.
	ledger, err := database.NewStore(database.DialectMySQL, migrationTableName)
	if err != nil {
		t.Fatalf("creating the migration store: %v", err)
	}
	if err := ledger.CreateVersionTable(ctx, db); err != nil {
		t.Fatalf("creating the migration ledger: %v", err)
	}
	if err := ledger.Insert(ctx, db, database.InsertRequest{Version: 0}); err != nil {
		t.Fatalf("recording version 0: %v", err)
	}
	var ran []string
	for stmt := range strings.SplitSeq(stripSQLComments(string(initial)), ";") {
		if stmt = strings.TrimSpace(stmt); stmt == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("running %q: %v", stmt, err)
		}
		ran = append(ran, stmt)
		if strings.Contains(stmt, "INSERT INTO worker_outbox_trim") {
			break
		}
	}
	if len(ran) < 3 {
		t.Fatalf("ran %d statements of 000001 before the seed row, want a partial run", len(ran))
	}
	var tables int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'leases'`).Scan(&tables); err != nil || tables != 0 {
		t.Fatalf("leases table count = %d, %v; want 0 so the run is partial", tables, err)
	}

	p, err := NewPersistence(ctx, db)
	if err != nil {
		t.Fatalf("NewPersistence after a partial migration failed: %v", err)
	}
	p.Close()
	if diff := cmp.Diff(embeddedMigrationVersions(t), appliedMigrationVersions(t, db)); diff != "" {
		t.Fatalf("applied migration versions (-embedded +applied):\n%s", diff)
	}
	var trimRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM worker_outbox_trim`).Scan(&trimRows); err != nil || trimRows != 1 {
		t.Fatalf("worker_outbox_trim rows = %d, %v; want 1", trimRows, err)
	}
}

func embeddedMigrationVersions(t *testing.T) []int64 {
	t.Helper()
	names, err := fs.Glob(migrationFiles, "migrations/*.sql")
	if err != nil {
		t.Fatalf("listing migrations: %v", err)
	}
	var versions []int64
	for _, name := range names {
		versions = append(versions, int64(migrationVersion(t, name)))
	}
	slices.Sort(versions)
	return versions
}

func appliedMigrationVersions(t *testing.T, db *sql.DB) []int64 {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `
		SELECT version_id FROM schema_migrations
		WHERE version_id > 0 AND is_applied ORDER BY version_id`)
	if err != nil {
		t.Fatalf("reading applied migration versions: %v", err)
	}
	defer rows.Close()
	var versions []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scanning migration version: %v", err)
		}
		versions = append(versions, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("reading applied migration versions: %v", err)
	}
	return versions
}

func TestMigrationsConcurrentStartup(t *testing.T) {
	const database = "atemy_concurrent_startup"
	dsn := createTestDatabase(t, database)
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			p, err := Connect(t.Context(), ConnectConfig{ReadWriteDSN: dsn, OwnerDSN: dsn})
			if p != nil {
				p.Close()
				p.DB().Close()
			}
			errs <- err
		}()
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("Connect failed: %v", err)
		}
	}
	// Every migration applied once and no more: two racing starts must not each
	// record the same version.
	if diff := cmp.Diff(embeddedMigrationVersions(t), appliedMigrationVersions(t, openAdmin(t, database))); diff != "" {
		t.Fatalf("applied migration versions (-embedded +applied):\n%s", diff)
	}
}

func TestMigrationsWaitForInProgressMigration(t *testing.T) {
	const database = "atemy_migration_lock_wait"
	createTestDatabase(t, database)
	ctx := t.Context()
	admin := openAdmin(t, "mysql")
	lockConn, err := admin.Conn(ctx)
	if err != nil {
		t.Fatalf("acquiring lock connection: %v", err)
	}
	defer lockConn.Close()
	lockName := migrationLockName(database)
	var got int
	if err := lockConn.QueryRowContext(ctx, `SELECT GET_LOCK(?, 0)`, lockName).Scan(&got); err != nil || got != 1 {
		t.Fatalf("taking migration lock = %d, %v; want 1", got, err)
	}

	db := openAdmin(t, database)
	result := make(chan error, 1)
	go func() { result <- applyMigrations(ctx, db) }()

	deadline := time.Now().Add(10 * time.Second)
	for {
		var pending int
		if err := admin.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM performance_schema.metadata_locks
			WHERE OBJECT_TYPE = 'USER LEVEL LOCK' AND OBJECT_NAME = ? AND LOCK_STATUS = 'PENDING'`, lockName).Scan(&pending); err != nil {
			t.Fatalf("reading pending locks: %v", err)
		}
		if pending > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("migration never waited on the held lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case err := <-result:
		t.Fatalf("migration returned while its lock was held: %v", err)
	default:
	}
	if versions := appliedMigrationVersions(t, db); len(versions) != 0 {
		t.Fatalf("migrations %v applied while another session held the lock", versions)
	}

	var released int
	if err := lockConn.QueryRowContext(ctx, `SELECT RELEASE_LOCK(?)`, lockName).Scan(&released); err != nil || released != 1 {
		t.Fatalf("releasing migration lock = %d, %v; want 1", released, err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("migration failed after the lock was released: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("migration did not finish after the lock was released")
	}
	if diff := cmp.Diff(embeddedMigrationVersions(t), appliedMigrationVersions(t, db)); diff != "" {
		t.Fatalf("applied migration versions (-embedded +applied):\n%s", diff)
	}
}

// A schema migrated by a newer binary is ahead of this one's files. An older
// replica in a rolling deploy must still start against it.
func TestMigrations_StartAgainstASchemaAhead(t *testing.T) {
	createTestDatabase(t, "atemy_migration_ahead")
	db := openAdmin(t, "atemy_migration_ahead")
	ctx := t.Context()
	p, err := NewPersistence(ctx, db)
	if err != nil {
		t.Fatalf("creating current schema: %v", err)
	}
	p.Close()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO schema_migrations (version_id, is_applied)
		SELECT MAX(version_id) + 1, TRUE FROM schema_migrations`); err != nil {
		t.Fatalf("setting ahead migration state: %v", err)
	}
	p, err = NewPersistence(ctx, db)
	if err != nil {
		t.Fatalf("NewPersistence against a schema ahead failed: %v", err)
	}
	p.Close()
}

// Multi-primary replication hands out AUTO_INCREMENT values in steps, which
// watchers would track as gaps that never fill.
func TestRequireAutoIncrementStep(t *testing.T) {
	requireDB(t)
	db, err := Open(containerDSN)
	if err != nil {
		t.Fatalf("opening pool: %v", err)
	}
	defer db.Close()
	// One connection, kept, so the session setting applies to every query.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx := t.Context()
	if err := requireAutoIncrementStep(ctx, db); err != nil {
		t.Fatalf("requireAutoIncrementStep with the default step = %v, want nil", err)
	}
	if _, err := db.ExecContext(ctx, `SET SESSION auto_increment_increment = 2`); err != nil {
		t.Fatalf("setting auto_increment_increment: %v", err)
	}
	if err := requireAutoIncrementStep(ctx, db); err == nil || !strings.Contains(err.Error(), "auto_increment_increment = 1, got 2") {
		t.Errorf("requireAutoIncrementStep with a step of 2 = %v, want a step error", err)
	}
	if _, err := NewPersistence(ctx, db); err == nil || !strings.Contains(err.Error(), "auto_increment_increment") {
		t.Errorf("NewPersistence with a step of 2 = %v, want a step error", err)
	}
}

// The read/write DSN can turn strict mode off for the sessions that write,
// while the owner pool that migrates stays strict.
func TestNewPersistence_RequiresStrictReadWriteSessions(t *testing.T) {
	ctx := t.Context()
	owner := requireDB(t)
	cfg, err := mysql.ParseDSN(containerDSN)
	if err != nil {
		t.Fatalf("parsing DSN: %v", err)
	}
	cfg.Params = map[string]string{"sql_mode": "''"}
	db, err := Open(cfg.FormatDSN())
	if err != nil {
		t.Fatalf("opening pool: %v", err)
	}
	defer db.Close()
	if _, err := newPersistence(ctx, db, db, owner); err == nil || !strings.Contains(err.Error(), "strict sql_mode") {
		t.Errorf("newPersistence with a non-strict read/write pool = %v, want a strict mode error", err)
	}
}
