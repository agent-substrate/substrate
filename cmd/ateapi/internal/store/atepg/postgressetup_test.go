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
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agent-substrate/substrate/pkg/postgressetup"
)

func TestPostgresSetupScript(t *testing.T) {
	ctx := t.Context()
	admin := requirePool(t)
	runSetup := func() error {
		_, err := admin.Exec(ctx, postgressetup.Script())
		return err
	}

	if _, err := admin.Exec(ctx, `CREATE ROLE substrate_owner LOGIN`); err != nil {
		t.Fatalf("creating incompatible owner role: %v", err)
	}
	if err := runSetup(); err == nil || !strings.Contains(err.Error(), "conflicts with the required attributes") {
		t.Fatalf("setup with incompatible role error = %v", err)
	}
	if _, err := admin.Exec(ctx, `DROP ROLE substrate_owner`); err != nil {
		t.Fatalf("dropping incompatible owner role: %v", err)
	}

	if _, err := admin.Exec(ctx, `CREATE SCHEMA substrate`); err != nil {
		t.Fatalf("creating incompatible schema: %v", err)
	}
	if err := runSetup(); err == nil || !strings.Contains(err.Error(), `schema "substrate" is owned by`) {
		t.Fatalf("setup with incompatible schema error = %v", err)
	}
	if _, err := admin.Exec(ctx, `DROP SCHEMA substrate`); err != nil {
		t.Fatalf("dropping incompatible schema: %v", err)
	}

	for i := 0; i < 2; i++ {
		if err := runSetup(); err != nil {
			t.Fatalf("setup run %d: %v", i+1, err)
		}
	}

	owner := setupRolePool(t, postgressetup.OwnerUser, postgressetup.OwnerPassword, "substrate_owner")
	readWrite := setupRolePool(t, postgressetup.ReadWriteUser, postgressetup.ReadWritePassword, "substrate_readwrite")
	if _, err := owner.Exec(ctx, `CREATE TABLE setup_permissions (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, value text)`); err != nil {
		t.Fatalf("owner creating table: %v", err)
	}
	if _, err := readWrite.Exec(ctx, `INSERT INTO setup_permissions (value) VALUES ('before')`); err != nil {
		t.Fatalf("read/write role inserting row: %v", err)
	}
	if _, err := readWrite.Exec(ctx, `UPDATE setup_permissions SET value = 'after'`); err != nil {
		t.Fatalf("read/write role updating row: %v", err)
	}
	var value string
	if err := readWrite.QueryRow(ctx, `SELECT value FROM setup_permissions`).Scan(&value); err != nil || value != "after" {
		t.Fatalf("read/write role selected value %q: %v", value, err)
	}
	if _, err := readWrite.Exec(ctx, `DELETE FROM setup_permissions`); err != nil {
		t.Fatalf("read/write role deleting row: %v", err)
	}
	if _, err := readWrite.Exec(ctx, `CREATE TABLE forbidden (id integer)`); err == nil {
		t.Fatal("read/write role created a table")
	}
}

func setupRolePool(t *testing.T, user, password, role string) *pgxpool.Pool {
	t.Helper()
	cfg, err := poolConfig(containerDSN, role)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.User = user
	cfg.ConnConfig.Password = password
	cfg.ConnConfig.RuntimeParams["search_path"] = "substrate"
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatalf("opening %s pool: %v", role, err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(t.Context()); err != nil {
		t.Fatalf("connecting as %s: %v", role, err)
	}
	return pool
}
