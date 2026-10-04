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
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storesql"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/pressly/goose/v3/lock"
)

const (
	migrationTableName = "schema_migrations"
	// migrationLockTimeout bounds how long a replica waits for another
	// replica's migration run.
	migrationLockTimeout = 5 * time.Minute
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

func applyMigrations(ctx context.Context, db *sql.DB) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := rejectUnversionedSubstrateSchema(ctx, db); err != nil {
		return err
	}
	migrations, err := fs.Sub(migrationFiles, "migrations")
	if err != nil {
		return fmt.Errorf("open embedded MySQL migrations: %w", err)
	}
	provider, err := newMigrationProvider(ctx, db, migrations)
	if err != nil {
		return err
	}
	// provider.Close would close db, which the caller owns.
	return storesql.MigrateToLatest(ctx, provider, "MySQL")
}

// newMigrationProvider returns a Goose provider for migrations on db that
// serializes runs across replicas.
func newMigrationProvider(ctx context.Context, db *sql.DB, migrations fs.FS) (*goose.Provider, error) {
	var name string
	if err := db.QueryRowContext(ctx, `SELECT DATABASE()`).Scan(&name); err != nil {
		return nil, fmt.Errorf("get MySQL database name: %w", err)
	}
	base, err := database.NewStore(database.DialectMySQL, migrationTableName)
	if err != nil {
		return nil, fmt.Errorf("create MySQL migration store: %w", err)
	}
	provider, err := goose.NewProvider(
		goose.DialectCustom,
		db,
		migrations,
		goose.WithStore(migrationStore{Store: base}),
		goose.WithSessionLocker(migrationLocker{name: migrationLockName(name)}),
	)
	if err != nil {
		return nil, fmt.Errorf("create MySQL migration provider: %w", err)
	}
	return provider, nil
}

// migrationStore is Goose's MySQL store with a version-table check that also
// works on Vitess. Goose compares table_schema to DATABASE() in a SELECT with
// no FROM clause, which vtgate answers with the keyspace name rather than the
// MySQL schema behind it, so the table never appears to exist and the next
// startup fails to create it again.
type migrationStore struct {
	database.Store
}

var _ database.StoreExtender = migrationStore{}

func (s migrationStore) TableExists(ctx context.Context, db database.DBTxConn) (bool, error) {
	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name = ?`, s.Tablename()).Scan(&n); err != nil {
		return false, fmt.Errorf("check MySQL migration ledger: %w", err)
	}
	return n > 0, nil
}

// requireStrictMode refuses a session that would truncate an overlong value
// with a warning instead of an error. PostgreSQL never truncates.
func requireStrictMode(ctx context.Context, db *sql.DB) error {
	var mode string
	if err := db.QueryRowContext(ctx, `SELECT @@SESSION.sql_mode`).Scan(&mode); err != nil {
		return fmt.Errorf("get MySQL sql_mode: %w", err)
	}
	for m := range strings.SplitSeq(strings.ToUpper(mode), ",") {
		if m == "STRICT_TRANS_TABLES" || m == "STRICT_ALL_TABLES" {
			return nil
		}
	}
	return fmt.Errorf("atemy requires a strict sql_mode (STRICT_TRANS_TABLES or STRICT_ALL_TABLES), got %q", mode)
}

// rejectUnversionedSubstrateSchema stops Goose from adopting tables it did not
// create, as atepg does: CREATE TABLE IF NOT EXISTS would otherwise accept a
// same-named table of any shape. Goose creates the migration ledger before
// the first migration, so a partial run still has one.
func rejectUnversionedSubstrateSchema(ctx context.Context, db *sql.DB) error {
	var hasMetadata, hasSubstrateTables bool
	err := db.QueryRowContext(ctx, `
		SELECT
			EXISTS (SELECT 1 FROM information_schema.tables
				WHERE table_schema = DATABASE() AND table_name = ?),
			EXISTS (SELECT 1 FROM information_schema.tables
				WHERE table_schema = DATABASE()
				AND table_name IN (
					'atespaces', 'actors', 'actor_egress_policies', 'actor_templates',
					'tags', 'workers', 'worker_assignments', 'worker_outbox',
					'worker_outbox_trim', 'leases'
				))`, migrationTableName).Scan(&hasMetadata, &hasSubstrateTables)
	if err != nil {
		return fmt.Errorf("check MySQL migration ledger: %w", err)
	}
	if hasSubstrateTables && !hasMetadata {
		return errors.New("unsupported MySQL schema: Substrate tables exist without a migration ledger")
	}
	return nil
}

// requireAutoIncrementStep refuses a server that hands out AUTO_INCREMENT
// values in steps, as multi-primary replication does. Watchers treat every
// skipped worker outbox seq as a write still committing, so steps would leave
// them tracking gaps that never fill.
func requireAutoIncrementStep(ctx context.Context, db *sql.DB) error {
	var step int
	if err := db.QueryRowContext(ctx, `SELECT @@auto_increment_increment`).Scan(&step); err != nil {
		return fmt.Errorf("get MySQL auto_increment_increment: %w", err)
	}
	if step != 1 {
		return fmt.Errorf("atemy requires auto_increment_increment = 1, got %d", step)
	}
	return nil
}

// requireMySQL8 reports a clear error before an older server rejects the
// schema with an opaque one. The schema needs MySQL 8.0 for utf8mb4_0900_bin,
// FOR SHARE and SKIP LOCKED. Vitess reports a version such as 8.0.40-Vitess.
// MariaDB numbers its releases 10 and up but lacks utf8mb4_0900_bin.
func requireMySQL8(ctx context.Context, db *sql.DB) error {
	var version string
	if err := db.QueryRowContext(ctx, `SELECT VERSION()`).Scan(&version); err != nil {
		return fmt.Errorf("get MySQL version: %w", err)
	}
	major, _, _ := strings.Cut(version, ".")
	if n, err := strconv.Atoi(major); err != nil || n < 8 || strings.Contains(strings.ToLower(version), "mariadb") {
		return fmt.Errorf("atemy requires MySQL 8.0 or newer. VERSION() is %q", version)
	}
	return nil
}

// migrationLockName scopes the migration lock to one database. MySQL caps
// lock names at 64 characters, so the database name is hashed.
func migrationLockName(database string) string {
	sum := sha256.Sum256([]byte(database))
	return "atemy-migrations:" + hex.EncodeToString(sum[:16])
}

// migrationLocker serializes migration runs across replicas with a MySQL
// named lock. The lock belongs to the session, so goose holds one connection
// for the whole run, and the server releases the lock if that connection drops.
type migrationLocker struct {
	name string
}

var _ lock.SessionLocker = migrationLocker{}

func (l migrationLocker) SessionLock(ctx context.Context, conn *sql.Conn) error {
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, `SELECT GET_LOCK(?, ?)`, l.name, int(migrationLockTimeout.Seconds())).Scan(&got); err != nil {
		return fmt.Errorf("acquire MySQL migration lock: %w", err)
	}
	if !got.Valid || got.Int64 != 1 {
		return fmt.Errorf("acquire MySQL migration lock: timed out after %s", migrationLockTimeout)
	}
	return nil
}

func (l migrationLocker) SessionUnlock(ctx context.Context, conn *sql.Conn) error {
	var released sql.NullInt64
	if err := conn.QueryRowContext(ctx, `SELECT RELEASE_LOCK(?)`, l.name).Scan(&released); err != nil {
		return fmt.Errorf("release MySQL migration lock: %w", err)
	}
	if !released.Valid || released.Int64 != 1 {
		return fmt.Errorf("release MySQL migration lock: this session did not hold it")
	}
	return nil
}
