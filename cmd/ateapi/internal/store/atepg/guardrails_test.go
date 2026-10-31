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

// Fixtures shared by the schema guardrail tests, TestActorsTablePartitionable
// and TestAtespaceTablesShardable: a migrated copy of the schema to alter,
// and the catalog queries that describe it.

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// migratedPool opens a pool on a fresh schema with the migrations applied.
func migratedPool(t *testing.T, schema string) *pgxpool.Pool {
	t.Helper()
	admin := requirePool(t)
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(t.Context(), `DROP SCHEMA IF EXISTS `+quoted+` CASCADE; CREATE SCHEMA `+quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+quoted+` CASCADE`) })
	pool := openPool(t, schema, nil)
	p, err := NewPersistence(t.Context(), pool)
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
	return pool
}

// openPool opens a pool on schema, tracing every statement with tracer.
func openPool(t *testing.T, schema string, tracer pgx.QueryTracer) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(containerDSN)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = pgx.Identifier{schema}.Sanitize()
	cfg.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// schemaTables lists the tables in the pool's schema, leaving out the
// partitions of a partitioned table.
func schemaTables(t *testing.T, pool *pgxpool.Pool) []string {
	t.Helper()
	return collectStrings(t, pool, `
		SELECT c.relname FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = current_schema() AND c.relkind IN ('r', 'p') AND NOT c.relispartition
		ORDER BY c.relname`)
}

// tablesWithColumn lists the tables in the pool's schema that have column.
func tablesWithColumn(t *testing.T, pool *pgxpool.Pool, column string) []string {
	t.Helper()
	tables := collectStrings(t, pool, `
		SELECT table_name FROM information_schema.columns
		WHERE table_schema = current_schema() AND column_name = $1
		ORDER BY table_name`, column)
	if len(tables) == 0 {
		t.Fatalf("no table has a %s column", column)
	}
	return tables
}

func collectStrings(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) []string {
	t.Helper()
	rows, err := pool.Query(t.Context(), sql, args...)
	if err != nil {
		t.Fatal(err)
	}
	values, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return values
}

// foreignKey is a foreign key from one table of the pool's schema to another.
type foreignKey struct {
	Name, From, To string
	Definition     string // as pg_get_constraintdef renders it
}

// ddl is the statement that adds the foreign key to its table again.
func (fk foreignKey) ddl() string {
	return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s", fk.From, pgx.Identifier{fk.Name}.Sanitize(), fk.Definition)
}

// foreignKeys lists the foreign keys of the pool's schema, by name.
func foreignKeys(ctx context.Context, pool *pgxpool.Pool) ([]foreignKey, error) {
	rows, err := pool.Query(ctx, `
		SELECT conname, conrelid::regclass::text, confrelid::regclass::text, pg_get_constraintdef(c.oid)
		FROM pg_constraint c
		JOIN pg_namespace n ON n.oid = c.connamespace
		WHERE n.nspname = current_schema() AND contype = 'f' AND conparentid = 0
		ORDER BY conname`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[foreignKey])
}
