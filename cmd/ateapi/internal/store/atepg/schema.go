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
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"time"

	"github.com/golang-migrate/migrate/v4"
	migratepgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

func applyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// The schema needs PostgreSQL 13+ (xid8, pg_current_xact_id,
	// pg_current_snapshot); fail with a clear message rather than an
	// opaque DDL or function error.
	var version int
	if err := pool.QueryRow(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&version); err != nil {
		return fmt.Errorf("reading PostgreSQL version: %w", err)
	}
	if version < 130000 {
		return fmt.Errorf("atepg requires PostgreSQL 13 or newer (xid8 and pg_current_snapshot); server_version_num is %d", version)
	}
	if err := rejectUnversionedSubstrateSchema(ctx, pool); err != nil {
		return err
	}

	migrator, latestVersion, err := openMigrator(pool)
	if err != nil {
		return err
	}

	migrationErr := migrateToLatest(ctx, migrator, latestVersion)
	sourceErr, databaseErr := migrator.Close()
	return errors.Join(migrationErr, sourceErr, databaseErr)
}

func openMigrator(pool *pgxpool.Pool) (*migrate.Migrate, uint, error) {
	db := stdlib.OpenDBFromPool(pool)
	databaseDriver, err := migratepgx.WithInstance(db, &migratepgx.Config{})
	if err != nil {
		_ = db.Close()
		return nil, 0, fmt.Errorf("opening PostgreSQL migration database: %w", err)
	}
	sourceDriver, err := iofs.New(migrationFiles, "migrations")
	if err != nil {
		_ = databaseDriver.Close()
		return nil, 0, fmt.Errorf("opening embedded PostgreSQL migrations: %w", err)
	}
	latestVersion, err := latestMigrationVersion(sourceDriver)
	if err != nil {
		_ = sourceDriver.Close()
		_ = databaseDriver.Close()
		return nil, 0, err
	}
	migrator, err := migrate.NewWithInstance("iofs", sourceDriver, "pgx5", databaseDriver)
	if err != nil {
		_ = sourceDriver.Close()
		_ = databaseDriver.Close()
		return nil, 0, fmt.Errorf("creating PostgreSQL migrator: %w", err)
	}
	return migrator, latestVersion, nil
}

// rejectUnversionedSubstrateSchema prevents golang-migrate from creating
// metadata and dirtying a database built by pre-migration ateapi versions.
// The table list is the frozen pre-migration schema; later tables always have
// migration metadata and do not belong here.
func rejectUnversionedSubstrateSchema(ctx context.Context, pool *pgxpool.Pool) error {
	var hasMetadata, hasSubstrateTables bool
	err := pool.QueryRow(ctx, `
		SELECT
			to_regclass('schema_migrations') IS NOT NULL,
			EXISTS (
				SELECT 1 FROM information_schema.tables
				WHERE table_schema = current_schema()
				AND table_name IN (
					'atespaces', 'actors', 'actor_templates',
					'actor_snapshots', 'actor_snapshot_tags', 'workers',
					'worker_outbox', 'worker_outbox_default',
					'worker_outbox_trim', 'leases'
				)
			)`).Scan(&hasMetadata, &hasSubstrateTables)
	if err != nil {
		return fmt.Errorf("checking PostgreSQL migration metadata: %w", err)
	}
	if hasSubstrateTables && !hasMetadata {
		return errors.New("unsupported PostgreSQL schema: Substrate tables exist without migration metadata")
	}
	return nil
}

func latestMigrationVersion(driver source.Driver) (uint, error) {
	latest, err := driver.First()
	if err != nil {
		return 0, fmt.Errorf("reading first PostgreSQL migration: %w", err)
	}
	for {
		next, err := driver.Next(latest)
		if errors.Is(err, fs.ErrNotExist) {
			return latest, nil
		}
		if err != nil {
			return 0, fmt.Errorf("reading PostgreSQL migration after version %d: %w", latest, err)
		}
		latest = next
	}
}

func migrateToLatest(ctx context.Context, migrator *migrate.Migrate, latest uint) (migrationErr error) {
	started := time.Now()
	current, dirty, err := migrator.Version()
	if errors.Is(err, migrate.ErrNilVersion) {
		current, dirty, err = 0, false, nil
	}
	if err != nil {
		return fmt.Errorf("reading PostgreSQL migration state: %w", err)
	}

	applied := 0
	defer func() {
		attributes := []any{
			slog.Uint64("current_version", uint64(current)),
			slog.Uint64("latest_version", uint64(latest)),
			slog.Int("applied_migrations", applied),
			slog.Duration("duration", time.Since(started)),
			slog.Bool("dirty", dirty),
		}
		if migrationErr != nil {
			attributes = append(attributes, slog.Any("err", migrationErr))
			slog.ErrorContext(ctx, "PostgreSQL migrations failed", attributes...)
			return
		}
		slog.InfoContext(ctx, "PostgreSQL migrations ready", attributes...)
	}()

	if !dirty && current >= latest {
		return nil
	}
	if dirty && current > latest {
		return fmt.Errorf("dirty PostgreSQL migration version %d is ahead of this binary's latest version %d, so an operator must recover it: %w", current, latest, migrate.ErrDirty{Version: int(current)})
	}

	previous := current
	if err := migrator.Up(); errors.Is(err, migrate.ErrNoChange) {
		newCurrent, newDirty, stateErr := migrator.Version()
		if stateErr != nil {
			return fmt.Errorf("reading PostgreSQL migration state after lock wait: %w", stateErr)
		}
		current, dirty = newCurrent, newDirty
		return nil
	} else if err != nil {
		applyErr := fmt.Errorf("applying PostgreSQL migrations: %w", err)
		recoveredCurrent, recoveredDirty, recovered, rollbackErr := rollbackToVersion(migrator, previous, latest)
		current, dirty = recoveredCurrent, recoveredDirty
		if rollbackErr != nil {
			return errors.Join(applyErr, rollbackErr)
		}
		if !recovered {
			if !dirty && current >= latest {
				return nil
			}
			return applyErr
		}
		return fmt.Errorf("rolled back PostgreSQL migrations to pre-run version %d: %w", current, applyErr)
	}
	current = latest
	dirty = false
	applied = int(latest - previous)
	return nil
}

// rollbackToVersion clears a known dirty migration and runs down migrations for
// each migration that completed after target.
func rollbackToVersion(migrator *migrate.Migrate, target, latest uint) (current uint, dirty, recovered bool, err error) {
	current, dirty, err = migrator.Version()
	if errors.Is(err, migrate.ErrNilVersion) {
		return 0, false, false, nil
	}
	if err != nil {
		return 0, false, false, fmt.Errorf("reading PostgreSQL migration state for rollback: %w", err)
	}
	if !dirty {
		return current, false, false, nil
	}
	if current > latest {
		return current, true, false, fmt.Errorf("dirty PostgreSQL migration version %d is ahead of this binary's latest version %d, so an operator must recover it: %w", current, latest, migrate.ErrDirty{Version: int(current)})
	}

	cleanVersion := int(current) - 1
	forceTarget := cleanVersion
	if forceTarget < 1 {
		forceTarget = -1
	}
	if err := migrator.Force(forceTarget); err != nil {
		return current, true, false, fmt.Errorf("clearing dirty PostgreSQL migration version %d: %w", current, err)
	}
	if forceTarget < 0 {
		return 0, false, true, nil
	}
	current = uint(cleanVersion)

	steps := int(current) - int(target)
	if steps <= 0 {
		return current, false, true, nil
	}
	if err := migrator.Steps(-steps); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return current, false, true, fmt.Errorf("rolling back %d PostgreSQL migration(s) to version %d: %w", steps, target, err)
	}
	return target, false, true, nil
}
