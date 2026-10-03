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
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/pressly/goose/v3"
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
	if err := requireMySQL8(ctx, db); err != nil {
		return err
	}
	migrations, err := fs.Sub(migrationFiles, "migrations")
	if err != nil {
		return fmt.Errorf("open embedded MySQL migrations: %w", err)
	}
	var database string
	if err := db.QueryRowContext(ctx, `SELECT DATABASE()`).Scan(&database); err != nil {
		return fmt.Errorf("get MySQL database name: %w", err)
	}
	provider, err := goose.NewProvider(
		goose.DialectMySQL,
		db,
		migrations,
		goose.WithTableName(migrationTableName),
		goose.WithSessionLocker(migrationLocker{name: migrationLockName(database)}),
	)
	if err != nil {
		return fmt.Errorf("create MySQL migration provider: %w", err)
	}
	// provider.Close would close db, which the caller owns.
	return migrateToLatest(ctx, provider)
}

// requireMySQL8 reports a clear error before an older server rejects the
// schema with an opaque one. The schema needs MySQL 8.0 for utf8mb4_0900_bin,
// FOR SHARE and SKIP LOCKED. Vitess reports a version such as 8.0.40-Vitess.
func requireMySQL8(ctx context.Context, db *sql.DB) error {
	var version string
	if err := db.QueryRowContext(ctx, `SELECT VERSION()`).Scan(&version); err != nil {
		return fmt.Errorf("get MySQL version: %w", err)
	}
	major, _, _ := strings.Cut(version, ".")
	if n, err := strconv.Atoi(major); err != nil || n < 8 {
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

func migrateToLatest(ctx context.Context, provider *goose.Provider) (migrationErr error) {
	started := time.Now()
	current, latest, err := provider.GetVersions(ctx)
	if err != nil {
		return fmt.Errorf("get MySQL migration versions: %w", err)
	}
	starting := current

	applied := 0
	defer func() {
		attributes := []any{
			slog.Int64("starting_version", starting),
			slog.Int64("current_version", current),
			slog.Int64("latest_version", latest),
			slog.Int("applied_migrations", applied),
			slog.Duration("duration", time.Since(started)),
		}
		if migrationErr != nil {
			attributes = append(attributes, slog.Any("err", migrationErr))
			slog.ErrorContext(ctx, "MySQL migrations failed", attributes...)
			return
		}
		slog.InfoContext(ctx, "MySQL migrations ready", attributes...)
	}()

	results, err := provider.Up(ctx)
	if err != nil {
		var partial *goose.PartialError
		if errors.As(err, &partial) {
			applied = len(partial.Applied)
		}
		applyErr := fmt.Errorf("apply MySQL migrations: %w", err)
		failedCurrent, _, versionErr := provider.GetVersions(ctx)
		if versionErr != nil {
			return errors.Join(applyErr, fmt.Errorf("get MySQL migration versions after a failure: %w", versionErr))
		}
		current = failedCurrent
		return applyErr
	}
	applied = len(results)
	current, _, err = provider.GetVersions(ctx)
	if err != nil {
		return fmt.Errorf("get MySQL migration versions after migration: %w", err)
	}
	return nil
}
