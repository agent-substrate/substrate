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
	"errors"
	"io/fs"
	"regexp"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"
)

var transactionControl = regexp.MustCompile(`(?im)^\s*(BEGIN|START\s+TRANSACTION|COMMIT|ROLLBACK)\s*;`)

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
			t.Errorf("%s must run in a PostgreSQL transaction", path)
		}
		if strings.Contains(upperSQL, "IF NOT EXISTS") {
			t.Errorf("%s must not contain an IF NOT EXISTS guard", path)
		}
		if strings.Contains(upperSQL, "-- +GOOSE ENVSUB") {
			t.Errorf("%s must not use Goose environment substitution", path)
		}
		if transactionControl.MatchString(sql) {
			t.Errorf("%s must let Goose control the PostgreSQL transaction", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("check PostgreSQL migrations: %v", err)
	}
}

func TestMigrationsConcurrentStartup(t *testing.T) {
	pool := requirePool(t)
	ctx := t.Context()
	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS "concurrent-startup" CASCADE`); err != nil {
		t.Fatalf("resetting PostgreSQL schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS "concurrent-startup" CASCADE`)
	})

	errs := make(chan error, 2)
	for range 2 {
		go func() {
			p, err := Connect(ctx, containerDSN, "concurrent-startup")
			if p != nil {
				p.Close()
				p.pool.Close()
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
	want, err := fs.Glob(migrationFiles, "migrations/*.sql")
	if err != nil {
		t.Fatalf("listing migrations: %v", err)
	}
	var applied int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM "concurrent-startup".schema_migrations WHERE version_id > 0 AND is_applied`).Scan(&applied); err != nil {
		t.Fatalf("reading applied migrations: %v", err)
	}
	if applied != len(want) {
		t.Fatalf("applied migration rows = %d, want %d", applied, len(want))
	}
}

func TestMigrationsWaitForInProgressMigration(t *testing.T) {
	pool := requirePool(t)
	ctx := t.Context()
	const schema = "migration-lock-wait"
	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS "migration-lock-wait" CASCADE`); err != nil {
		t.Fatalf("resetting schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS "migration-lock-wait" CASCADE`)
	})

	if _, err := pool.Exec(ctx, `CREATE SCHEMA "migration-lock-wait"`); err != nil {
		t.Fatalf("creating migration schema: %v", err)
	}
	migrationPool, err := pgxpool.New(ctx, containerDSN+"&search_path="+schema)
	if err != nil {
		t.Fatalf("opening migration pool: %v", err)
	}
	t.Cleanup(migrationPool.Close)
	lockID, err := migrationLockID(ctx, migrationPool)
	if err != nil {
		t.Fatalf("generating migration lock ID: %v", err)
	}
	lockConn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquiring migration lock connection: %v", err)
	}
	locked := true
	t.Cleanup(func() {
		if locked {
			_, _ = lockConn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, lockID)
		}
		lockConn.Release()
	})
	if _, err := lockConn.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockID); err != nil {
		t.Fatalf("locking migrations: %v", err)
	}

	result := make(chan error, 1)
	go func() { result <- applyMigrations(ctx, migrationPool) }()
	select {
	case err := <-result:
		t.Fatalf("migration returned while its advisory lock was held: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	if _, err := lockConn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, lockID); err != nil {
		t.Fatalf("unlocking migrations: %v", err)
	}
	locked = false
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("migration failed after the advisory lock was released: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("migration did not finish after the advisory lock was released")
	}
}

func TestMigrationSchemaStates(t *testing.T) {
	pool := requirePool(t)
	ctx := t.Context()

	t.Run("ahead", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS "migration-ahead" CASCADE`); err != nil {
			t.Fatalf("resetting schema: %v", err)
		}
		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS "migration-ahead" CASCADE`)
		})

		p, err := Connect(ctx, containerDSN, "migration-ahead")
		if err != nil {
			t.Fatalf("creating current schema: %v", err)
		}
		p.Close()
		p.pool.Close()
		if _, err := pool.Exec(ctx, `INSERT INTO "migration-ahead".schema_migrations (version_id, is_applied) VALUES (2, true)`); err != nil {
			t.Fatalf("setting ahead migration state: %v", err)
		}

		p, err = Connect(ctx, containerDSN, "migration-ahead")
		if err != nil {
			t.Fatalf("Connect with an ahead clean schema failed: %v", err)
		}
		p.Close()
		p.pool.Close()
	})

	t.Run("tables without metadata", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `
			DROP SCHEMA IF EXISTS "migration-legacy" CASCADE;
			CREATE SCHEMA "migration-legacy";
			CREATE TABLE "migration-legacy".atespaces (id integer)`); err != nil {
			t.Fatalf("creating legacy schema: %v", err)
		}
		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS "migration-legacy" CASCADE`)
		})

		_, err := Connect(ctx, containerDSN, "migration-legacy")
		if err == nil || !strings.Contains(err.Error(), "Substrate tables exist without a migration ledger") {
			t.Fatalf("Connect error = %v, want unsupported schema error", err)
		}
		var metadataExists bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass('"migration-legacy".schema_migrations') IS NOT NULL`).Scan(&metadataExists); err != nil {
			t.Fatalf("checking migration metadata: %v", err)
		}
		if metadataExists {
			t.Error("Connect created a migration ledger for an unsupported schema")
		}
	})
}

func TestMigrationFailureLeavesCompletedPrefixAndResumes(t *testing.T) {
	pool := requirePool(t)
	ctx := t.Context()
	const schema = "migration-resume"
	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS "migration-resume" CASCADE; CREATE SCHEMA "migration-resume"`); err != nil {
		t.Fatalf("resetting schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS "migration-resume" CASCADE`)
	})

	migrationPool, err := pgxpool.New(ctx, containerDSN+"&search_path="+schema)
	if err != nil {
		t.Fatalf("opening migration pool: %v", err)
	}
	t.Cleanup(migrationPool.Close)
	files := fstest.MapFS{
		"000001_create.sql": {Data: []byte("-- +goose Up\nCREATE TABLE resume_test (id integer PRIMARY KEY);")},
		"000002_add_value.sql": {Data: []byte("-- +goose Up\n" +
			"ALTER TABLE resume_test ADD COLUMN value text;")},
		"000003_fail.sql": {Data: []byte("-- +goose Up\n" +
			"CREATE TABLE failed_transaction (id integer);\n" +
			"ALTER TABLE missing_table ADD COLUMN value text;")},
	}
	provider, err := openMigrationProvider(ctx, migrationPool, files)
	if err != nil {
		t.Fatalf("creating migration provider: %v", err)
	}
	if _, err := provider.UpTo(ctx, 1); err != nil {
		t.Fatalf("applying pre-run migration: %v", err)
	}
	migrationErr := migrateToLatest(ctx, provider)
	if migrationErr == nil {
		t.Fatal("migration succeeded, want version 3 failure")
	}
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
	if err := provider.Close(); err != nil {
		t.Fatalf("closing failed migration provider: %v", err)
	}

	var valueColumnExists bool
	if err := migrationPool.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = 'resume_test' AND column_name = 'value'
	)`, schema).Scan(&valueColumnExists); err != nil {
		t.Fatalf("checking completed migration: %v", err)
	}
	if !valueColumnExists {
		t.Error("migration 2 was not left applied")
	}
	var failedSQLApplied bool
	if err := migrationPool.QueryRow(ctx, `SELECT to_regclass('failed_transaction') IS NOT NULL`).Scan(&failedSQLApplied); err != nil {
		t.Fatalf("checking failed migration transaction: %v", err)
	}
	if failedSQLApplied {
		t.Error("SQL from failed migration 3 was committed")
	}
	if diff := cmp.Diff([]int64{1, 2}, appliedMigrationVersions(t, migrationPool)); diff != "" {
		t.Fatalf("applied migration versions after failure (-want +got):\n%s", diff)
	}

	files["000003_fail.sql"] = &fstest.MapFile{Data: []byte("-- +goose Up\nCREATE TABLE resumed_migration (id integer);")}
	resumedProvider, err := openMigrationProvider(ctx, migrationPool, files)
	if err != nil {
		t.Fatalf("creating resumed migration provider: %v", err)
	}
	t.Cleanup(func() {
		if err := resumedProvider.Close(); err != nil {
			t.Errorf("closing resumed migration provider: %v", err)
		}
	})
	if err := migrateToLatest(ctx, resumedProvider); err != nil {
		t.Fatalf("resuming migrations: %v", err)
	}
	if diff := cmp.Diff([]int64{1, 2, 3}, appliedMigrationVersions(t, migrationPool)); diff != "" {
		t.Fatalf("applied migration versions after resume (-want +got):\n%s", diff)
	}
	var resumed bool
	if err := migrationPool.QueryRow(ctx, `SELECT to_regclass('resumed_migration') IS NOT NULL`).Scan(&resumed); err != nil {
		t.Fatalf("checking resumed migration: %v", err)
	}
	if !resumed {
		t.Error("migration 3 was not applied after restart")
	}
}

func appliedMigrationVersions(t *testing.T, pool *pgxpool.Pool) []int64 {
	t.Helper()
	var versions []int64
	if err := pool.QueryRow(t.Context(), `
		SELECT COALESCE(array_agg(version_id ORDER BY version_id), '{}'::bigint[])
		FROM schema_migrations
		WHERE version_id > 0 AND is_applied`).Scan(&versions); err != nil {
		t.Fatalf("reading applied migration versions: %v", err)
	}
	return versions
}
