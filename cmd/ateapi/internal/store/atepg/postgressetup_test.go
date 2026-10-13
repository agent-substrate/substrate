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
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	tcexec "github.com/testcontainers/testcontainers-go/exec"

	"github.com/agent-substrate/substrate/pkg/postgressetup"
)

func TestPostgresSetupScript(t *testing.T) {
	requirePool(t)
	const scriptPath = "/tmp/substrate-postgres-setup.sql"
	if err := containerPG.CopyToContainer(t.Context(), []byte(postgressetup.Script()), scriptPath, 0o600); err != nil {
		t.Fatalf("copying setup script to PostgreSQL container: %v", err)
	}

	// These cases exercise optional password provisioning for external databases.
	defaults := postgressetup.DefaultConfig()
	defaults.OwnerPassword = "test-owner-password"
	defaults.ReadWritePassword = "test-runtime-password"
	for _, tc := range []struct {
		name   string
		config postgressetup.Config
	}{
		{name: "bundled identity names", config: defaults},
		{name: "operator values", config: postgressetup.Config{
			Schema:            "custom-substrate",
			OwnerRole:         "custom_substrate_owner",
			OwnerUser:         "custom_substrate_owner_user",
			OwnerPassword:     "custom owner's password",
			ReadWriteRole:     "custom_substrate_readwrite",
			ReadWriteUser:     "custom_substrate_readwrite_user",
			ReadWritePassword: "custom runtime password",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testPostgresSetupScript(t, scriptPath, tc.config)
		})
	}
}

func testPostgresSetupScript(t *testing.T, scriptPath string, config postgressetup.Config) {
	ctx := t.Context()
	admin := requirePool(t)
	resetPostgresSetup(t, admin, config)
	t.Cleanup(func() { resetPostgresSetup(t, admin, config) })
	runSetup := func() error {
		command := []string{
			"psql", "--no-psqlrc", "--set=ON_ERROR_STOP=1", "--username", "atepg", "--dbname", "atepg",
		}
		command = append(command, config.PSQLArgs()...)
		command = append(command, "--file="+scriptPath)
		exitCode, output, err := containerPG.Exec(ctx, command, tcexec.Multiplexed())
		if err != nil {
			return err
		}
		detail, err := io.ReadAll(output)
		if err != nil {
			return fmt.Errorf("reading psql output: %w", err)
		}
		if exitCode != 0 {
			return fmt.Errorf("psql exited with code %d: %s", exitCode, strings.TrimSpace(string(detail)))
		}
		return nil
	}

	ownerRole := pgx.Identifier{config.OwnerRole}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE ROLE "+ownerRole+" LOGIN"); err != nil {
		t.Fatalf("creating incompatible owner role: %v", err)
	}
	if err := runSetup(); err == nil || !strings.Contains(err.Error(), "conflicts with the required attributes") {
		t.Fatalf("setup with incompatible role error = %v", err)
	}
	if _, err := admin.Exec(ctx, "DROP ROLE "+ownerRole); err != nil {
		t.Fatalf("dropping incompatible owner role: %v", err)
	}

	schema := pgx.Identifier{config.Schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("creating incompatible schema: %v", err)
	}
	if err := runSetup(); err == nil || !strings.Contains(err.Error(), "is owned by") {
		t.Fatalf("setup with incompatible schema error = %v", err)
	}
	if _, err := admin.Exec(ctx, "DROP SCHEMA "+schema); err != nil {
		t.Fatalf("dropping incompatible schema: %v", err)
	}

	for i := 0; i < 2; i++ {
		if err := runSetup(); err != nil {
			t.Fatalf("setup run %d: %v", i+1, err)
		}
	}

	owner := setupRolePool(t, config.OwnerUser, config.OwnerPassword, config.OwnerRole, config.Schema)
	readWrite := setupRolePool(t, config.ReadWriteUser, config.ReadWritePassword, config.ReadWriteRole, config.Schema)
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

func resetPostgresSetup(t *testing.T, admin *pgxpool.Pool, config postgressetup.Config) {
	t.Helper()
	ctx := context.Background()
	if _, err := admin.Exec(ctx, "DROP SCHEMA IF EXISTS "+pgx.Identifier{config.Schema}.Sanitize()+" CASCADE"); err != nil {
		t.Errorf("dropping setup schema: %v", err)
		return
	}
	roles := []string{config.OwnerUser, config.ReadWriteUser, config.OwnerRole, config.ReadWriteRole}
	for _, role := range roles {
		var exists bool
		if err := admin.QueryRow(ctx, `SELECT EXISTS (SELECT FROM pg_roles WHERE rolname = $1)`, role).Scan(&exists); err != nil {
			t.Errorf("checking setup role %q: %v", role, err)
			return
		}
		if exists {
			if _, err := admin.Exec(ctx, "DROP OWNED BY "+pgx.Identifier{role}.Sanitize()); err != nil {
				t.Errorf("dropping objects owned by %q: %v", role, err)
				return
			}
		}
	}
	for _, role := range roles {
		if _, err := admin.Exec(ctx, "DROP ROLE IF EXISTS "+pgx.Identifier{role}.Sanitize()); err != nil {
			t.Errorf("dropping setup role %q: %v", role, err)
			return
		}
	}
}

func setupRolePool(t *testing.T, user, password, role, schema string) *pgxpool.Pool {
	t.Helper()
	cfg, err := poolConfig(containerDSN, role)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.User = user
	cfg.ConnConfig.Password = password
	cfg.ConnConfig.RuntimeParams["search_path"] = pgx.Identifier{schema}.Sanitize()
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
