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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestPoolConfigRereadsRotatedCredentials covers the pod certificate rotation
// that a long-lived pool would otherwise miss: pgx reads sslcert and sslkey
// when the connection string is parsed, so without BeforeConnect every
// connection this process ever makes presents the certificate that was on disk
// at startup, which expires about a day later.
func TestPoolConfigRereadsRotatedCredentials(t *testing.T) {
	dir := t.TempDir()
	bundlePath := filepath.Join(dir, "credential-bundle.pem")
	rootPath := filepath.Join(dir, "trust-bundle.pem")

	writeCredentialBundle(t, bundlePath, rootPath, 1)

	dsn := fmt.Sprintf(
		"postgres://postgres@postgres.ate-system.svc:5432/atepg?sslmode=verify-full&sslrootcert=%s&sslcert=%s&sslkey=%s",
		rootPath, bundlePath, bundlePath)

	cfg, err := poolConfig(dsn, "test_role")
	if err != nil {
		t.Fatalf("poolConfig: %v", err)
	}
	if got := clientCertSerial(t, cfg.ConnConfig.TLSConfig.Certificates); got != 1 {
		t.Fatalf("client certificate serial at parse time = %d, want 1", got)
	}
	if cfg.BeforeConnect == nil {
		t.Fatal("BeforeConnect is nil, rotated credentials would never be re-read")
	}

	// The kubelet swaps in a fresh certificate under the same path.
	writeCredentialBundle(t, bundlePath, rootPath, 2)

	// pgxpool hands BeforeConnect a copy per connection, so the pinned config
	// is left alone and every new connection picks up what is on disk now.
	conn := cfg.ConnConfig.Copy()
	if err := cfg.BeforeConnect(context.Background(), conn); err != nil {
		t.Fatalf("BeforeConnect: %v", err)
	}
	if got := clientCertSerial(t, conn.TLSConfig.Certificates); got != 2 {
		t.Errorf("client certificate serial for a new connection = %d, want 2", got)
	}
	if !conn.TLSConfig.RootCAs.Equal(rootPool(t, rootPath)) {
		t.Error("root CAs for a new connection are not the ones on disk")
	}
}

func TestPoolConfigRequiresRole(t *testing.T) {
	_, err := poolConfig("postgres://runtime@postgres:5432/atepg?sslmode=disable", "")
	if err == nil || !strings.Contains(err.Error(), "role must not be empty") {
		t.Fatalf("poolConfig error = %v, want missing-role error", err)
	}
}

func TestConnectRequiresRoles(t *testing.T) {
	for _, tc := range []struct {
		name          string
		readWriteRole string
		ownerRole     string
		want          string
	}{
		{name: "read/write", ownerRole: "owner", want: "read/write role must not be empty"},
		{name: "owner", readWriteRole: "readwrite", want: "owner role must not be empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Connect(t.Context(), ConnectConfig{
				ReadWriteDSN:  "unused",
				OwnerDSN:      "unused",
				ReadWriteRole: tc.readWriteRole,
				OwnerRole:     tc.ownerRole,
				Schema:        "substrate",
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Connect error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestConnectRequiresOwnerConnectionString(t *testing.T) {
	_, err := Connect(t.Context(), ConnectConfig{
		ReadWriteDSN:  "unused",
		ReadWriteRole: "runtime",
		OwnerRole:     "owner",
		Schema:        "substrate",
	})
	if err == nil || !strings.Contains(err.Error(), "owner connection string must not be empty") {
		t.Fatalf("Connect error = %v, want missing-owner error", err)
	}
}

// writeCredentialBundle writes a self-signed certificate with the given serial
// to certPath in the layout of a Kubernetes credential bundle (private key
// first, then the chain) and its certificate alone to rootPath.
func writeCredentialBundle(t *testing.T, certPath, rootPath string, serial int64) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: "postgres.ate-system.svc"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("signing certificate: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshaling key: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(certPath, append(keyPEM, certPEM...), 0o600); err != nil {
		t.Fatalf("writing credential bundle: %v", err)
	}
	if err := os.WriteFile(rootPath, certPEM, 0o644); err != nil {
		t.Fatalf("writing trust bundle: %v", err)
	}
}

func clientCertSerial(t *testing.T, certs []tls.Certificate) int64 {
	t.Helper()
	if len(certs) != 1 {
		t.Fatalf("got %d client certificates, want 1", len(certs))
	}
	leaf, err := x509.ParseCertificate(certs[0].Certificate[0])
	if err != nil {
		t.Fatalf("parsing client certificate: %v", err)
	}
	return leaf.SerialNumber.Int64()
}

func rootPool(t *testing.T, path string) *x509.CertPool {
	t.Helper()
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading trust bundle: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		t.Fatal("trust bundle holds no certificates")
	}
	return pool
}

func TestConnectUsesConfiguredSchema(t *testing.T) {
	pool := requirePool(t)
	ctx := t.Context()
	const schema = "substrate-test"
	if _, err := pool.Exec(ctx, `
		DROP SCHEMA IF EXISTS "substrate-test" CASCADE;
		DROP SCHEMA IF EXISTS "substrate-other-test" CASCADE;
		CREATE SCHEMA "substrate-other-test";
		CREATE TABLE "substrate-other-test".worker_outbox (
			created_at timestamptz NOT NULL
		) PARTITION BY RANGE (created_at);
		CREATE TABLE "substrate-other-test".worker_outbox_p200001010000
			PARTITION OF "substrate-other-test".worker_outbox
			FOR VALUES FROM ('2000-01-01 00:00:00+00') TO ('2000-01-01 00:05:00+00');
		CREATE TABLE IF NOT EXISTS public.substrate_schema_test_marker (id integer)`); err != nil {
		t.Fatalf("preparing schema test: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `
			DROP SCHEMA IF EXISTS "substrate-test" CASCADE;
			DROP SCHEMA IF EXISTS "substrate-other-test" CASCADE;
			DROP TABLE IF EXISTS public.substrate_schema_test_marker`)
	})

	dsn, err := containerPG.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("getting PostgreSQL connection string: %v", err)
	}
	persistence, err := Connect(ctx, ConnectConfig{
		ReadWriteDSN:  dsn + "&search_path=public",
		OwnerDSN:      dsn,
		ReadWriteRole: "atepg",
		OwnerRole:     "atepg",
		Schema:        schema,
	})
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer persistence.pool.Close()
	defer persistence.Close()

	if _, err := persistence.CreateAtespace(ctx, newTestAtespace("schema-test")); err != nil {
		t.Fatalf("creating atespace in configured schema: %v", err)
	}

	for _, table := range []string{"atespaces", "schema_migrations"} {
		var exists bool
		if err := persistence.pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.tables
				WHERE table_schema = $1 AND table_name = $2
			)`, schema, table).Scan(&exists); err != nil {
			t.Fatalf("checking %s.%s: %v", schema, table, err)
		}
		if !exists {
			t.Errorf("expected %s.%s to exist", schema, table)
		}
	}

	var markerExists bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('public.substrate_schema_test_marker') IS NOT NULL`).Scan(&markerExists); err != nil {
		t.Fatalf("checking unrelated table: %v", err)
	}
	if !markerExists {
		t.Error("migration removed an unrelated table")
	}

	if err := persistence.dropExpiredWorkerOutboxPartitions(ctx, persistence.ownerPool, time.Now()); err != nil {
		t.Fatalf("dropping expired partitions in the configured schema: %v", err)
	}
	var unrelatedPartitionExists bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('"substrate-other-test".worker_outbox_p200001010000') IS NOT NULL`).Scan(&unrelatedPartitionExists); err != nil {
		t.Fatalf("checking unrelated outbox partition: %v", err)
	}
	if !unrelatedPartitionExists {
		t.Error("outbox maintenance removed a partition from another schema")
	}
}

func TestConnectSeparatesRuntimeAndDDLPrivileges(t *testing.T) {
	admin := requirePool(t)
	ctx := t.Context()
	const (
		schema       = "separate-role-test"
		runtimeRole  = "atepg_runtime_test"
		ddlRole      = "atepg_ddl_test"
		runtimeLogin = "atepg_runtime_login_a"
		ddlLogin     = "atepg_ddl_login_a"
		password     = "test-password"
	)
	if _, err := admin.Exec(ctx, fmt.Sprintf(`
		DROP SCHEMA IF EXISTS %s CASCADE;
		DROP ROLE IF EXISTS %s;
		DROP ROLE IF EXISTS %s;
		DROP ROLE IF EXISTS %s;
		DROP ROLE IF EXISTS %s;
		CREATE ROLE %s NOLOGIN;
		CREATE ROLE %s NOLOGIN;
		CREATE ROLE %s LOGIN PASSWORD '%s';
		CREATE ROLE %s LOGIN PASSWORD '%s';
		GRANT %s TO %s;
		GRANT %s TO %s;
		GRANT CREATE ON DATABASE atepg TO %s`,
		pgx.Identifier{schema}.Sanitize(),
		pgx.Identifier{runtimeLogin}.Sanitize(),
		pgx.Identifier{ddlLogin}.Sanitize(),
		pgx.Identifier{runtimeRole}.Sanitize(), pgx.Identifier{ddlRole}.Sanitize(),
		pgx.Identifier{runtimeRole}.Sanitize(), pgx.Identifier{ddlRole}.Sanitize(),
		pgx.Identifier{runtimeLogin}.Sanitize(), password,
		pgx.Identifier{ddlLogin}.Sanitize(), password,
		pgx.Identifier{runtimeRole}.Sanitize(), pgx.Identifier{runtimeLogin}.Sanitize(),
		pgx.Identifier{ddlRole}.Sanitize(), pgx.Identifier{ddlLogin}.Sanitize(),
		pgx.Identifier{ddlRole}.Sanitize())); err != nil {
		t.Fatalf("creating PostgreSQL test roles: %v", err)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf(`
		CREATE SCHEMA %s AUTHORIZATION %s;
		GRANT USAGE ON SCHEMA %s TO %s;
		ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA %s
			GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %s`,
		pgx.Identifier{schema}.Sanitize(), pgx.Identifier{ddlRole}.Sanitize(),
		pgx.Identifier{schema}.Sanitize(), pgx.Identifier{runtimeRole}.Sanitize(),
		pgx.Identifier{ddlRole}.Sanitize(), pgx.Identifier{schema}.Sanitize(), pgx.Identifier{runtimeRole}.Sanitize())); err != nil {
		t.Fatalf("creating PostgreSQL test default privileges: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), fmt.Sprintf(`ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA %s
			REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLES FROM %s`,
			pgx.Identifier{ddlRole}.Sanitize(), pgx.Identifier{schema}.Sanitize(), pgx.Identifier{runtimeRole}.Sanitize()))
		_, _ = admin.Exec(context.Background(), fmt.Sprintf(`
			DROP SCHEMA IF EXISTS %s CASCADE;
			REVOKE ALL ON DATABASE atepg FROM %s;
			DROP ROLE IF EXISTS %s;
			DROP ROLE IF EXISTS %s;
			DROP ROLE IF EXISTS %s;
			DROP ROLE IF EXISTS %s`, pgx.Identifier{schema}.Sanitize(),
			pgx.Identifier{ddlRole}.Sanitize(),
			pgx.Identifier{runtimeLogin}.Sanitize(),
			pgx.Identifier{ddlLogin}.Sanitize(),
			pgx.Identifier{runtimeRole}.Sanitize(), pgx.Identifier{ddlRole}.Sanitize()))
	})

	runtimeDSN := strings.Replace(containerDSN, "://atepg:atepg@", "://"+runtimeLogin+":"+password+"@", 1)
	ddlDSN := strings.Replace(containerDSN, "://atepg:atepg@", "://"+ddlLogin+":"+password+"@", 1)
	if runtimeDSN == containerDSN || ddlDSN == containerDSN {
		t.Fatalf("unexpected test DSN format: %q", containerDSN)
	}
	p, err := Connect(ctx, ConnectConfig{
		ReadWriteDSN:  runtimeDSN,
		OwnerDSN:      ddlDSN,
		ReadWriteRole: runtimeRole,
		OwnerRole:     ddlRole,
		Schema:        schema,
		PoolMaxConns:  20,
	})
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer p.pool.Close()
	defer p.Close()
	if got := p.pool.Config().MaxConns; got != 20 {
		t.Fatalf("read/write pool MaxConns = %d, want 20", got)
	}
	if got := p.ownerPool.Config().MaxConns; got != ownerPoolMaxConns {
		t.Fatalf("owner pool MaxConns = %d, want %d", got, ownerPoolMaxConns)
	}

	if _, err := p.CreateAtespace(ctx, newTestAtespace("runtime-write")); err != nil {
		t.Fatalf("read/write operation failed: %v", err)
	}
	if _, err := p.pool.Exec(ctx, `CREATE TABLE forbidden (id integer)`); err == nil {
		t.Fatal("read/write role created a table")
	}
	var canUpdateLedger bool
	if err := p.pool.QueryRow(ctx, `SELECT has_table_privilege(current_user, 'schema_migrations', 'UPDATE')`).Scan(&canUpdateLedger); err != nil || !canUpdateLedger {
		t.Fatalf("read/write role lacks default table privileges on migration ledger: %v", err)
	}
	if err := p.createWorkerOutboxPartitions(ctx, time.Now().Add(24*time.Hour)); err != nil {
		t.Fatalf("owner maintenance failed: %v", err)
	}

	var canUseOpenFGA bool
	if err := p.pool.QueryRow(ctx, `
		SELECT has_table_privilege(current_user, 'tuple', 'SELECT')
			AND has_table_privilege(current_user, 'tuple', 'INSERT')
			AND has_table_privilege(current_user, 'tuple', 'UPDATE')
			AND has_table_privilege(current_user, 'tuple', 'DELETE')`).Scan(&canUseOpenFGA); err != nil || !canUseOpenFGA {
		t.Fatalf("read/write role lacks OpenFGA table privileges: %v", err)
	}
	var owner string
	if err := admin.QueryRow(ctx, `SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = $1::regclass`,
		pgx.Identifier{schema, "tuple"}.Sanitize()).Scan(&owner); err != nil {
		t.Fatalf("querying OpenFGA table owner: %v", err)
	}
	if owner != ddlRole {
		t.Errorf("OpenFGA table owner = %q, want stable owner role %q", owner, ddlRole)
	}
}
