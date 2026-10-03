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
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/openfga/openfga/assets"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storecontract"
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

// TestOpenFGAMigrationVersionGuard ensures that bumping
// github.com/openfga/openfga in go.mod cannot silently introduce schema or
// query drift.
//
// Substrate manages the OpenFGA MySQL tables directly in
// migrations/000002_openfga.sql, the final state of upstream MySQL migrations
// 001 through 008, so they share the database and migration ledger with the
// Substrate tables, and cmd/ateapi/internal/authz/datastore_mysql.go adapts
// upstream MySQL queries to run on the caller's transaction. A new upstream
// migration fails this test until its DDL is ported as a new migration here
// and the adapted queries are checked.
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

// TestMigrations_ResumeAPartialInitialMigration simulates a crash partway
// through 000001: MySQL committed some of its tables and a seed row, but
// Goose recorded no version. The next startup must complete the file, and the
// store must then work.
// TestMigrations_RejectTablesWithoutALedger mirrors atepg: tables Goose did
// not create are refused rather than adopted by CREATE TABLE IF NOT EXISTS.
func TestMigrations_RejectTablesWithoutALedger(t *testing.T) {
	db := openTestDatabase(t, createTestDatabase(t, "atemy_unversioned"))
	if _, err := db.ExecContext(t.Context(), `CREATE TABLE atespaces (name VARCHAR(255) PRIMARY KEY)`); err != nil {
		t.Fatalf("creating a stray atespaces table: %v", err)
	}
	if _, err := NewPersistence(t.Context(), db); err == nil || !strings.Contains(err.Error(), "without a migration ledger") {
		t.Fatalf("NewPersistence over tables without a ledger = %v, want a refusal", err)
	}
}

func TestMigrations_ResumeAPartialInitialMigration(t *testing.T) {
	db := openTestDatabase(t, createTestDatabase(t, "atemy_partial_migration"))
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

	storecontract.RunContractTests(t, func(t *testing.T) store.Interface {
		t.Helper()
		p, err := NewPersistence(t.Context(), db)
		if err != nil {
			t.Fatalf("NewPersistence failed: %v", err)
		}
		t.Cleanup(p.Close)
		clearAll(t, p)
		setTestPolicyManager(t, p)
		return p
	})
}

func TestMigrationLockName(t *testing.T) {
	long := strings.Repeat("d", 64)
	if got := migrationLockName(long); len(got) > 64 {
		t.Errorf("migrationLockName(%d-character database) is %d characters, over MySQL's 64", len(long), len(got))
	}
	// Replicas of different releases must contend on the same lock, so the
	// name may never change.
	if got, want := migrationLockName("atemy"), "atemy-migrations:bc3eedb3aab9c4330ac9e5d96b355016"; got != want {
		t.Errorf("migrationLockName(atemy) = %q, want %q", got, want)
	}
	if migrationLockName("a") == migrationLockName("b") {
		t.Error("migrationLockName gives two databases the same lock")
	}
}

// embeddedMigrationVersions lists the versions of the embedded migrations.
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

func openTestDatabase(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := Open(dsn)
	if err != nil {
		t.Fatalf("opening database: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestMigrations_IdempotentAcrossRestarts(t *testing.T) {
	db := requireDB(t)
	for range 2 {
		p, err := NewPersistence(t.Context(), db)
		if err != nil {
			t.Fatalf("NewPersistence failed: %v", err)
		}
		p.Close()
		if diff := cmp.Diff(embeddedMigrationVersions(t), appliedMigrationVersions(t, db)); diff != "" {
			t.Fatalf("applied migration versions (-embedded +applied):\n%s", diff)
		}
	}
}

func TestMigrationsConcurrentStartup(t *testing.T) {
	dsn := createTestDatabase(t, "atemy_concurrent_startup")
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
	if diff := cmp.Diff(embeddedMigrationVersions(t), appliedMigrationVersions(t, openTestDatabase(t, dsn))); diff != "" {
		t.Fatalf("applied migration versions (-embedded +applied):\n%s", diff)
	}
}

func TestMigrationsWaitForInProgressMigration(t *testing.T) {
	const database = "atemy_migration_lock_wait"
	dsn := createTestDatabase(t, database)
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

	db := openTestDatabase(t, dsn)
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

func TestMigrationLocker_UnlockWithoutLockFails(t *testing.T) {
	conn, err := requireDB(t).Conn(t.Context())
	if err != nil {
		t.Fatalf("acquiring connection: %v", err)
	}
	defer conn.Close()
	locker := migrationLocker{name: migrationLockName("atemy_never_locked")}
	if err := locker.SessionUnlock(t.Context(), conn); err == nil || !strings.Contains(err.Error(), "did not hold it") {
		t.Errorf("SessionUnlock without the lock = %v, want a did-not-hold-it error", err)
	}
	if err := locker.SessionLock(t.Context(), conn); err != nil {
		t.Fatalf("SessionLock failed: %v", err)
	}
	if err := locker.SessionUnlock(t.Context(), conn); err != nil {
		t.Errorf("SessionUnlock after SessionLock = %v, want nil", err)
	}
}

// A schema migrated by a newer binary is ahead of this one's files. An older
// replica in a rolling deploy must still start against it.
func TestMigrations_StartAgainstASchemaAhead(t *testing.T) {
	dsn := createTestDatabase(t, "atemy_migration_ahead")
	db := openTestDatabase(t, dsn)
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

func TestMigrationFailureLeavesCompletedPrefixAndResumes(t *testing.T) {
	const database = "atemy_migration_resume"
	db := openTestDatabase(t, createTestDatabase(t, database))
	ctx := t.Context()
	newProvider := func(files fs.FS) *goose.Provider {
		t.Helper()
		// Closing the provider would close db, so it is left open.
		provider, err := newMigrationProvider(ctx, db, files)
		if err != nil {
			t.Fatalf("creating migration provider: %v", err)
		}
		return provider
	}
	files := fstest.MapFS{
		"000001_create.sql":    {Data: []byte("-- +goose Up\nCREATE TABLE resume_test (id INT PRIMARY KEY);")},
		"000002_add_value.sql": {Data: []byte("-- +goose Up\nALTER TABLE resume_test ADD COLUMN value TEXT;")},
		"000003_fail.sql":      {Data: []byte("-- +goose Up\nALTER TABLE missing_table ADD COLUMN value TEXT;")},
	}
	provider := newProvider(files)
	if _, err := provider.UpTo(ctx, 1); err != nil {
		t.Fatalf("applying pre-run migration: %v", err)
	}
	migrationErr := migrateToLatest(ctx, provider)
	var partial *goose.PartialError
	if !errors.As(migrationErr, &partial) {
		t.Fatalf("migration error = %v, want goose.PartialError", migrationErr)
	}
	if got := partial.Failed.Source.Version; got != 3 {
		t.Fatalf("failed migration version = %d, want 3", got)
	}
	if len(partial.Applied) != 1 || partial.Applied[0].Source.Version != 2 {
		t.Fatalf("migrations completed before failure = %#v, want version 2", partial.Applied)
	}
	if diff := cmp.Diff([]int64{1, 2}, appliedMigrationVersions(t, db)); diff != "" {
		t.Fatalf("applied migration versions after failure (-want +got):\n%s", diff)
	}

	files["000003_fail.sql"] = &fstest.MapFile{Data: []byte("-- +goose Up\nCREATE TABLE resumed_migration (id INT);")}
	if err := migrateToLatest(ctx, newProvider(files)); err != nil {
		t.Fatalf("resuming migrations: %v", err)
	}
	if diff := cmp.Diff([]int64{1, 2, 3}, appliedMigrationVersions(t, db)); diff != "" {
		t.Fatalf("applied migration versions after resume (-want +got):\n%s", diff)
	}
	var tables int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = DATABASE()
		AND ((table_name = 'resume_test' AND column_name = 'value') OR table_name = 'resumed_migration')`).Scan(&tables); err != nil {
		t.Fatalf("checking migrated tables: %v", err)
	}
	if tables != 2 {
		t.Errorf("found %d of the 2 columns migrations 2 and 3 create", tables)
	}
}

// Multi-primary replication hands out AUTO_INCREMENT values in steps, which
// watchers would track as gaps that never fill.
func TestRequireAutoIncrementStep(t *testing.T) {
	db, err := Open(containerDSNForTest(t))
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

// containerDSNForTest returns the shared container's DSN once it is up.
func containerDSNForTest(t *testing.T) string {
	t.Helper()
	requireDB(t)
	return containerDSN
}

func TestRequireMySQL8(t *testing.T) {
	for _, tc := range []struct {
		version string
		ok      bool
	}{
		{"8.0.36", true},
		{"8.4.3", true},
		{"8.0.40-Vitess", true},
		{"9.1.0", true},
		{"5.7.44-log", false},
		{"11.4.2-MariaDB", false},
		{"not-a-version", false},
	} {
		t.Run(tc.version, func(t *testing.T) {
			db := sql.OpenDB(versionConnector{version: tc.version})
			defer db.Close()
			err := requireMySQL8(t.Context(), db)
			if (err == nil) != tc.ok {
				t.Fatalf("requireMySQL8 with VERSION() %q = %v, want ok: %t", tc.version, err, tc.ok)
			}
			if !tc.ok {
				if !strings.Contains(err.Error(), tc.version) {
					t.Errorf("error %q does not name the server version", err)
				}
				// The check runs before any migration work.
				if _, err := NewPersistence(t.Context(), db); err == nil || !strings.Contains(err.Error(), "requires MySQL 8.0") {
					t.Errorf("NewPersistence on MySQL %s = %v, want a version error", tc.version, err)
				}
			}
		})
	}
}

// versionConnector is a database/sql driver that answers SELECT VERSION()
// with a fixed version and rejects every other statement.
type versionConnector struct{ version string }

func (c versionConnector) Connect(context.Context) (driver.Conn, error) {
	return versionConn(c), nil
}

func (versionConnector) Driver() driver.Driver { return versionDriver{} }

type versionDriver struct{}

func (versionDriver) Open(string) (driver.Conn, error) { return nil, errors.New("unsupported") }

type versionConn struct{ version string }

func (versionConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unsupported") }
func (versionConn) Close() error                        { return nil }
func (versionConn) Begin() (driver.Tx, error)           { return nil, errors.New("unsupported") }

func (c versionConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if query != `SELECT VERSION()` {
		return nil, fmt.Errorf("unexpected query %q", query)
	}
	return &versionRows{version: c.version}, nil
}

type versionRows struct {
	version string
	done    bool
}

func (*versionRows) Columns() []string { return []string{"VERSION()"} }
func (*versionRows) Close() error      { return nil }

func (r *versionRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = r.version
	return nil
}
