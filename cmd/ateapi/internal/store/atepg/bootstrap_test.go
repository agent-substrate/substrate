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
	"net/url"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestBootstrapCreatesManagedIdentities(t *testing.T) {
	admin := requirePool(t)
	ctx := t.Context()
	const schema = "substrate_bootstrap_test"
	cleanupManagedBootstrap(t, admin, schema)
	t.Cleanup(func() { cleanupManagedBootstrap(t, admin, schema) })

	ownerDSN := strings.Replace(containerDSN, "://atepg:atepg@", "://"+OwnerUserName+":"+defaultOwnerPassword+"@", 1)
	readWriteDSN := strings.Replace(containerDSN, "://atepg:atepg@", "://"+ReadWriteUserName+":"+defaultReadWritePassword+"@", 1)
	cfg := BootstrapConfig{
		EndpointSource:  ownerDSN,
		ReadWriteSource: readWriteDSN,
		AdminUsername:   "atepg",
		AdminPassword:   "atepg",
		Schema:          schema,
	}
	errs := make(chan error, 2)
	for range 2 {
		go func() { errs <- Bootstrap(ctx, cfg) }()
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("Bootstrap: %v", err)
		}
	}

	var owner string
	if err := admin.QueryRow(ctx, `SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = $1`, schema).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	if owner != OwnerRoleName {
		t.Errorf("schema owner = %q, want %q", owner, OwnerRoleName)
	}
	for user, role := range map[string]string{OwnerUserName: OwnerRoleName, ReadWriteUserName: ReadWriteRoleName} {
		var member bool
		if err := admin.QueryRow(ctx, `SELECT pg_has_role($1, $2, 'MEMBER')`, user, role).Scan(&member); err != nil {
			t.Fatal(err)
		}
		if !member {
			t.Errorf("%s is not a member of %s", user, role)
		}
	}

	// A retry validates existing identities. It must not reset passwords changed by an operator.
	if _, err := admin.Exec(ctx, `ALTER ROLE substrate_admin_user PASSWORD 'replacement-owner-password'`); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `ALTER ROLE substrate_readwrite_user PASSWORD 'replacement-read-write-password'`); err != nil {
		t.Fatal(err)
	}
	if err := Bootstrap(ctx, cfg); err != nil {
		t.Fatalf("second Bootstrap: %v", err)
	}

	ownerConn := connectManagedUser(t, OwnerUserName, "replacement-owner-password")
	defer ownerConn.Close(ctx) //nolint:errcheck
	if _, err := ownerConn.Exec(ctx, "SET ROLE "+pgx.Identifier{OwnerRoleName}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	if _, err := ownerConn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.bootstrap_data (id integer)`, pgx.Identifier{schema}.Sanitize())); err != nil {
		t.Fatalf("owner creates table: %v", err)
	}

	readWriteConn := connectManagedUser(t, ReadWriteUserName, "replacement-read-write-password")
	defer readWriteConn.Close(ctx) //nolint:errcheck
	if _, err := readWriteConn.Exec(ctx, "SET ROLE "+pgx.Identifier{ReadWriteRoleName}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	if _, err := readWriteConn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.bootstrap_data VALUES (1)`, pgx.Identifier{schema}.Sanitize())); err != nil {
		t.Fatalf("read/write insert: %v", err)
	}
	if _, err := readWriteConn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.forbidden (id integer)`, pgx.Identifier{schema}.Sanitize())); err == nil {
		t.Fatal("read/write role created a table")
	}
	if _, err := admin.Exec(ctx, `
		DROP SCHEMA IF EXISTS kagent_isolation_test CASCADE;
		CREATE SCHEMA kagent_isolation_test;
		REVOKE ALL ON SCHEMA kagent_isolation_test FROM PUBLIC;
		CREATE TABLE kagent_isolation_test.private_data (id integer)`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA IF EXISTS kagent_isolation_test CASCADE`)
	})
	if _, err := readWriteConn.Exec(ctx, `SELECT * FROM kagent_isolation_test.private_data`); err == nil {
		t.Fatal("read/write role accessed the Kagent schema")
	}
}

func TestBootstrapPublicSchema(t *testing.T) {
	admin := requirePool(t)
	ctx := t.Context()
	const database = "substrate_public_bootstrap_test"
	cleanupManagedBootstrap(t, admin, database)
	_, err := admin.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{database}.Sanitize())
	if err != nil {
		t.Fatal(err)
	}
	_, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{database}.Sanitize())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+pgx.Identifier{database}.Sanitize()); err != nil {
			t.Fatal(err)
		}
		cleanupManagedBootstrap(t, admin, database)
	})

	parsedDSN, err := url.Parse(containerDSN)
	if err != nil {
		t.Fatal(err)
	}
	parsedDSN.Path = "/" + database
	baseDSN := parsedDSN.String()
	ownerDSN := strings.Replace(baseDSN, "://atepg:atepg@", "://"+OwnerUserName+":"+defaultOwnerPassword+"@", 1)
	readWriteDSN := strings.Replace(baseDSN, "://atepg:atepg@", "://"+ReadWriteUserName+":"+defaultReadWritePassword+"@", 1)
	if err := Bootstrap(ctx, BootstrapConfig{
		EndpointSource:  ownerDSN,
		ReadWriteSource: readWriteDSN,
		AdminUsername:   "atepg",
		AdminPassword:   "atepg",
		Schema:          "public",
	}); err != nil {
		t.Fatal(err)
	}
	p, err := Connect(ctx, readWriteDSN, ownerDSN, ReadWriteRoleName, OwnerRoleName, "public", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer p.pool.Close()
	defer p.Close()

	ownerConfig, err := pgx.ParseConfig(ownerDSN)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := pgx.ConnectConfig(ctx, ownerConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(ctx) //nolint:errcheck
	if _, err := owner.Exec(ctx, "SET ROLE substrate_owner"); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Exec(ctx, "CREATE TABLE public.bootstrap_data (id integer)"); err != nil {
		t.Fatal(err)
	}

	readWriteConfig, err := pgx.ParseConfig(readWriteDSN)
	if err != nil {
		t.Fatal(err)
	}
	readWrite, err := pgx.ConnectConfig(ctx, readWriteConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer readWrite.Close(ctx) //nolint:errcheck
	if _, err := readWrite.Exec(ctx, "SET ROLE substrate_readwrite"); err != nil {
		t.Fatal(err)
	}
	if _, err := readWrite.Exec(ctx, "INSERT INTO public.bootstrap_data VALUES (1)"); err != nil {
		t.Fatal(err)
	}
}

func TestBootstrapRequiresConnectionString(t *testing.T) {
	if err := Bootstrap(t.Context(), BootstrapConfig{}); err == nil || !strings.Contains(err.Error(), "connection string must not be empty") {
		t.Fatalf("Bootstrap error = %v", err)
	}
}

func TestBootstrapRejectsCustomLogin(t *testing.T) {
	err := Bootstrap(t.Context(), BootstrapConfig{
		EndpointSource:  "postgresql://custom:password@localhost/atepg",
		ReadWriteSource: "postgresql://substrate_readwrite_user:password@localhost/atepg",
		AdminUsername:   "postgres",
		AdminPassword:   "postgres",
		Schema:          "substrate",
	})
	if err == nil || !strings.Contains(err.Error(), `must contain the "substrate_admin_user" user`) {
		t.Fatalf("Bootstrap error = %v, want fixed-login error", err)
	}
}

func TestBootstrapRejectsCustomPassword(t *testing.T) {
	err := Bootstrap(t.Context(), BootstrapConfig{
		EndpointSource:  "postgresql://substrate_admin_user:custom@localhost/atepg",
		ReadWriteSource: "postgresql://substrate_readwrite_user:substrate-readwrite@localhost/atepg",
		AdminUsername:   "postgres",
		AdminPassword:   "postgres",
		Schema:          "public",
	})
	if err == nil || !strings.Contains(err.Error(), "do not match the fixed development passwords") {
		t.Fatalf("Bootstrap error = %v, want fixed-password error", err)
	}
}

func TestIdentitySQLSupportsOperatorLogins(t *testing.T) {
	admin := requirePool(t)
	ctx := t.Context()
	const (
		schema            = "substrate_operator_identity_test"
		ownerUser         = "owner's-login"
		ownerPassword     = "owner's-password"
		readWriteUser     = "writer's-login"
		readWritePassword = "writer's-password"
		ownerRole         = "operator's-owner"
		readWriteRole     = "operator's-writer"
	)
	cleanupManagedBootstrap(t, admin, schema)
	t.Cleanup(func() { cleanupManagedBootstrap(t, admin, schema) })
	t.Cleanup(func() {
		_, err := admin.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		if err != nil {
			t.Fatal(err)
		}
		_, err = admin.Exec(context.Background(), "DROP ROLE IF EXISTS "+pgx.Identifier{readWriteUser}.Sanitize())
		if err != nil {
			t.Fatal(err)
		}
		_, err = admin.Exec(context.Background(), "DROP ROLE IF EXISTS "+pgx.Identifier{ownerUser}.Sanitize())
		if err != nil {
			t.Fatal(err)
		}
		_, err = admin.Exec(context.Background(), "DROP ROLE IF EXISTS "+pgx.Identifier{readWriteRole}.Sanitize())
		if err != nil {
			t.Fatal(err)
		}
		_, err = admin.Exec(context.Background(), "DROP ROLE IF EXISTS "+pgx.Identifier{ownerRole}.Sanitize())
		if err != nil {
			t.Fatal(err)
		}
	})

	tx, err := admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	for setting, value := range map[string]string{
		"substrate.bootstrap_owner_username":     ownerUser,
		"substrate.bootstrap_owner_password":     ownerPassword,
		"substrate.bootstrap_owner_role":         ownerRole,
		"substrate.bootstrap_readwrite_username": readWriteUser,
		"substrate.bootstrap_readwrite_password": readWritePassword,
		"substrate.bootstrap_readwrite_role":     readWriteRole,
		"substrate.bootstrap_schema":             schema,
	} {
		if _, err := tx.Exec(ctx, `SELECT set_config($1, $2, true)`, setting, value); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec(ctx, identitySQL); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var schemaOwner string
	if err := admin.QueryRow(ctx, `SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = $1`, schema).Scan(&schemaOwner); err != nil {
		t.Fatal(err)
	}
	if schemaOwner != ownerRole {
		t.Fatalf("schema owner = %q, want %q", schemaOwner, ownerRole)
	}
	for _, credentials := range []struct{ user, password, role string }{
		{ownerUser, ownerPassword, ownerRole},
		{readWriteUser, readWritePassword, readWriteRole},
	} {
		config, err := pgx.ParseConfig(containerDSN)
		if err != nil {
			t.Fatal(err)
		}
		config.User, config.Password = credentials.user, credentials.password
		conn, err := pgx.ConnectConfig(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, "SET ROLE "+pgx.Identifier{credentials.role}.Sanitize()); err != nil {
			conn.Close(ctx) //nolint:errcheck
			t.Fatal(err)
		}
		if credentials.user == ownerUser {
			_, err = conn.Exec(ctx, "CREATE TABLE "+pgx.Identifier{schema}.Sanitize()+".bootstrap_data (id integer)")
		} else {
			_, err = conn.Exec(ctx, "INSERT INTO "+pgx.Identifier{schema}.Sanitize()+".bootstrap_data VALUES (1)")
		}
		if err != nil {
			conn.Close(ctx) //nolint:errcheck
			t.Fatal(err)
		}
		conn.Close(ctx) //nolint:errcheck
	}
}

func connectManagedUser(t *testing.T, user, password string) *pgx.Conn {
	t.Helper()
	config, err := pgx.ParseConfig(containerDSN)
	if err != nil {
		t.Fatal(err)
	}
	config.User = user
	config.Password = password
	conn, err := pgx.ConnectConfig(t.Context(), config)
	if err != nil {
		t.Fatalf("connect as %s: %v", user, err)
	}
	return conn
}

func cleanupManagedBootstrap(t *testing.T, admin *pgxpool.Pool, schema string) {
	t.Helper()
	ctx := context.Background()
	if _, err := admin.Exec(ctx, `DROP SCHEMA IF EXISTS `+pgx.Identifier{schema}.Sanitize()+` CASCADE`); err != nil {
		t.Fatalf("drop managed schema: %v", err)
	}
	for _, role := range []string{ReadWriteRoleName, OwnerRoleName} {
		var exists bool
		if err := admin.QueryRow(ctx, `SELECT EXISTS (SELECT FROM pg_roles WHERE rolname = $1)`, role).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists {
			if _, err := admin.Exec(ctx, `DROP OWNED BY `+pgx.Identifier{role}.Sanitize()); err != nil {
				t.Fatalf("drop objects owned by %s: %v", role, err)
			}
		}
	}
	_, err := admin.Exec(ctx, fmt.Sprintf(`
		DROP ROLE IF EXISTS %s;
		DROP ROLE IF EXISTS %s;
		DROP ROLE IF EXISTS %s;
		DROP ROLE IF EXISTS %s`,
		pgx.Identifier{ReadWriteUserName}.Sanitize(),
		pgx.Identifier{OwnerUserName}.Sanitize(),
		pgx.Identifier{ReadWriteRoleName}.Sanitize(),
		pgx.Identifier{OwnerRoleName}.Sanitize()))
	if err != nil {
		t.Fatalf("clean managed bootstrap state: %v", err)
	}
}
