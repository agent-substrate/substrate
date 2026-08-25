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
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// newSchemaScopedPool returns a pool whose connections resolve unqualified names
// in a PostgreSQL schema of their own, so a test can apply the atepg schema over
// a table shape it controls without disturbing the shared one. It also exercises
// the current_schema() scoping checkActorsState relies on.
func newSchemaScopedPool(t *testing.T, name string) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	shared := requirePool(t)
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err := shared.Exec(ctx, `DROP SCHEMA IF EXISTS `+quoted+` CASCADE`); err != nil {
		t.Fatalf("dropping a leftover %q schema: %v", name, err)
	}
	if _, err := shared.Exec(ctx, `CREATE SCHEMA `+quoted); err != nil {
		t.Fatalf("creating the %q schema: %v", name, err)
	}

	config, err := pgxpool.ParseConfig(containerDSN)
	if err != nil {
		t.Fatalf("parsing the testcontainer DSN: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = name
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("connecting with search_path=%q: %v", name, err)
	}
	t.Cleanup(func() {
		pool.Close()
		if _, err := shared.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+quoted+` CASCADE`); err != nil {
			t.Errorf("dropping the %q schema: %v", name, err)
		}
	})
	return pool
}

func TestApplySchema_RejectsActorsWithoutState(t *testing.T) {
	pool := newSchemaScopedPool(t, "atepg_legacy_actors")
	ctx := context.Background()

	// The actors table as it stood before the state column was projected.
	if _, err := pool.Exec(ctx, `
		CREATE TABLE actors (
		    atespace  text NOT NULL,
		    name      text NOT NULL,
		    uid       text NOT NULL,
		    version   bigint NOT NULL,
		    proto     bytea NOT NULL,
		    PRIMARY KEY (atespace, name)
		)`); err != nil {
		t.Fatalf("creating the pre-state actors table: %v", err)
	}

	err := applySchema(ctx, pool)
	if err == nil {
		t.Fatal("applySchema accepted an actors table with no state column")
	}
	if !strings.Contains(err.Error(), "state column") {
		t.Errorf("applySchema error = %q, want it to name the missing state column", err)
	}
}

func TestApplySchema_FreshDatabaseIsIdempotent(t *testing.T) {
	pool := newSchemaScopedPool(t, "atepg_fresh_actors")
	ctx := context.Background()

	// The second pass must not trip the precheck the first pass satisfied.
	for pass := 1; pass <= 2; pass++ {
		if err := applySchema(ctx, pool); err != nil {
			t.Fatalf("applySchema pass %d failed: %v", pass, err)
		}
	}
}
