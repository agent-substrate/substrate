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

	"github.com/golang-migrate/migrate/v4"
	migratepgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
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

	db := stdlib.OpenDBFromPool(pool)
	databaseDriver, err := migratepgx.WithInstance(db, &migratepgx.Config{})
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("opening PostgreSQL migration database: %w", err)
	}
	sourceDriver, err := iofs.New(migrationFiles, "migrations")
	if err != nil {
		_ = databaseDriver.Close()
		return fmt.Errorf("opening embedded PostgreSQL migrations: %w", err)
	}
	migrator, err := migrate.NewWithInstance("iofs", sourceDriver, "pgx5", databaseDriver)
	if err != nil {
		_ = sourceDriver.Close()
		_ = databaseDriver.Close()
		return fmt.Errorf("creating PostgreSQL migrator: %w", err)
	}

	migrationErr := migrator.Up()
	if errors.Is(migrationErr, migrate.ErrNoChange) {
		migrationErr = nil
	} else if migrationErr != nil {
		migrationErr = fmt.Errorf("applying PostgreSQL migrations: %w", migrationErr)
	}
	sourceErr, databaseErr := migrator.Close()
	return errors.Join(migrationErr, sourceErr, databaseErr)
}
