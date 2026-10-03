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

package main

import (
	"bytes"
	"context"
	"log/slog"
	"net/url"
	"strings"
	"testing"

	"github.com/spf13/pflag"
)

// saveFlag restores a flag variable when the test ends.
func saveFlag[T any](t *testing.T, p *T) {
	t.Helper()
	old := *p
	t.Cleanup(func() { *p = old })
}

// markFlagChanged makes a flag look as if it was set on the command line.
func markFlagChanged(t *testing.T, name string) {
	t.Helper()
	f := pflag.CommandLine.Lookup(name)
	old := f.Changed
	t.Cleanup(func() { f.Changed = old })
	f.Changed = true
}

func TestConnectStoreRequiresPostgresReadWriteConnectionString(t *testing.T) {
	saveFlag(t, storeBackend)
	saveFlag(t, postgresReadWriteConnectionString)
	*storeBackend = storeBackendPostgres
	*postgresReadWriteConnectionString = ""

	_, err := connectStore(context.Background())
	if err == nil || !strings.Contains(err.Error(), "--postgres-read-write-connection-string is required") {
		t.Fatalf("connectStore() error = %v, want missing-connection-string error", err)
	}
}

func TestConnectStoreRequiresMySQLReadWriteConnectionString(t *testing.T) {
	saveFlag(t, storeBackend)
	saveFlag(t, mysqlReadWriteConnectionString)
	saveFlag(t, postgresReadWriteConnectionString)
	*storeBackend = storeBackendMySQL
	*mysqlReadWriteConnectionString = ""
	*postgresReadWriteConnectionString = ""

	_, err := connectStore(context.Background())
	if err == nil || !strings.Contains(err.Error(), "--mysql-read-write-connection-string is required") {
		t.Fatalf("connectStore() error = %v, want missing MySQL connection string error", err)
	}
}

func TestConnectStoreRejectsNegativeMySQLPoolMaxConns(t *testing.T) {
	saveFlag(t, storeBackend)
	saveFlag(t, mysqlReadWriteConnectionString)
	saveFlag(t, mysqlPoolMaxConns)
	*storeBackend = storeBackendMySQL
	*mysqlReadWriteConnectionString = "ateapi@tcp(db.example.internal:3306)/substrate"
	*mysqlPoolMaxConns = -1

	_, err := connectStore(context.Background())
	if err == nil || !strings.Contains(err.Error(), "--mysql-pool-max-conns must not be negative") {
		t.Fatalf("connectStore() error = %v, want pool-size validation", err)
	}
}

func TestConnectStoreRejectsUnknownBackend(t *testing.T) {
	saveFlag(t, storeBackend)
	*storeBackend = "sqlite"

	_, err := connectStore(context.Background())
	if err == nil || !strings.Contains(err.Error(), "--store-backend") {
		t.Fatalf("connectStore() error = %v, want backend validation", err)
	}
}

func TestLoadFlagsFromEnvStoreBackend(t *testing.T) {
	tests := []struct {
		name        string
		flagValue   string
		flagChanged bool
		env         string
		want        string
		wantErr     bool
	}{
		{name: "default", flagValue: storeBackendPostgres, want: storeBackendPostgres},
		{name: "env applies when flag unset", flagValue: storeBackendPostgres, env: storeBackendMySQL, want: storeBackendMySQL},
		{name: "flag wins over env", flagValue: storeBackendPostgres, flagChanged: true, env: storeBackendMySQL, want: storeBackendPostgres},
		{name: "flag selects mysql", flagValue: storeBackendMySQL, flagChanged: true, want: storeBackendMySQL},
		{name: "invalid env", flagValue: storeBackendPostgres, env: "sqlite", wantErr: true},
		{name: "invalid flag", flagValue: "sqlite", flagChanged: true, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			saveFlag(t, storeBackend)
			*storeBackend = tt.flagValue
			if tt.flagChanged {
				markFlagChanged(t, "store-backend")
			}
			t.Setenv("ATE_API_STORE_BACKEND", tt.env)

			err := loadFlagsFromEnv()
			if tt.wantErr {
				if err == nil || !strings.Contains(err.Error(), "--store-backend must be") {
					t.Fatalf("loadFlagsFromEnv() error = %v, want backend validation", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if *storeBackend != tt.want {
				t.Fatalf("store backend = %q, want %q", *storeBackend, tt.want)
			}
		})
	}
}

func TestLoadFlagsFromEnvResolvesMySQLSources(t *testing.T) {
	flags := []struct {
		value *string
		env   string
	}{
		{mysqlReadWriteConnectionString, "ATE_API_MYSQL_READ_WRITE_CONNECTION_STRING"},
		{mysqlOwnerConnectionString, "ATE_API_MYSQL_OWNER_CONNECTION_STRING"},
		{mysqlTLSCAFile, "ATE_API_MYSQL_TLS_CA_FILE"},
		{mysqlTLSCertFile, "ATE_API_MYSQL_TLS_CERT_FILE"},
		{mysqlTLSKeyFile, "ATE_API_MYSQL_TLS_KEY_FILE"},
	}
	for _, f := range flags {
		saveFlag(t, f.value)
		*f.value = "@env"
		t.Setenv(f.env, f.env+"-a")
	}
	*mysqlTLSKeyFile = "/etc/mysql/client.key"

	if err := loadFlagsFromEnv(); err != nil {
		t.Fatal(err)
	}
	for _, f := range flags[:4] {
		if *f.value != f.env+"-a" {
			t.Errorf("%s resolved to %q, want %q", f.env, *f.value, f.env+"-a")
		}
	}
	if *mysqlTLSKeyFile != "/etc/mysql/client.key" {
		t.Errorf("explicit --mysql-tls-key-file was replaced with %q", *mysqlTLSKeyFile)
	}
	t.Setenv("ATE_API_MYSQL_READ_WRITE_CONNECTION_STRING", "runtime-b")
	if err := loadFlagsFromEnv(); err != nil {
		t.Fatal(err)
	}
	if *mysqlReadWriteConnectionString != "ATE_API_MYSQL_READ_WRITE_CONNECTION_STRING-a" {
		t.Fatal("environment-backed connection string changed after startup resolution")
	}
}

func TestLoadFlagsFromEnvMySQLPoolMaxConns(t *testing.T) {
	saveFlag(t, storeBackend)
	saveFlag(t, mysqlPoolMaxConns)
	*storeBackend = storeBackendMySQL
	*mysqlPoolMaxConns = 0
	t.Setenv("ATE_API_POSTGRES_POOL_MAX_CONNS", "invalid")
	t.Setenv("ATE_API_MYSQL_POOL_MAX_CONNS", "20")
	if err := loadFlagsFromEnv(); err != nil {
		t.Fatal(err)
	}
	if *mysqlPoolMaxConns != 20 {
		t.Fatalf("pool max connections = %d, want 20", *mysqlPoolMaxConns)
	}
	for _, raw := range []string{"invalid", "0", "-5", "4294967296"} {
		t.Setenv("ATE_API_MYSQL_POOL_MAX_CONNS", raw)
		if err := loadFlagsFromEnv(); err == nil || !strings.Contains(err.Error(), "ATE_API_MYSQL_POOL_MAX_CONNS must be a positive integer") {
			t.Fatalf("loadFlagsFromEnv() with %q error = %v, want pool-size validation", raw, err)
		}
	}

	*mysqlPoolMaxConns = 7
	markFlagChanged(t, "mysql-pool-max-conns")
	t.Setenv("ATE_API_MYSQL_POOL_MAX_CONNS", "invalid")
	if err := loadFlagsFromEnv(); err != nil {
		t.Fatalf("loadFlagsFromEnv() read the environment for an explicit flag: %v", err)
	}
	if *mysqlPoolMaxConns != 7 {
		t.Fatalf("pool max connections = %d, want the flag value 7", *mysqlPoolMaxConns)
	}
}

func TestMySQLConnectConfig(t *testing.T) {
	for _, p := range []*string{mysqlReadWriteConnectionString, mysqlOwnerConnectionString, mysqlTLSCAFile, mysqlTLSCertFile, mysqlTLSKeyFile} {
		saveFlag(t, p)
	}
	saveFlag(t, mysqlPoolMaxConns)
	const readWrite = "ateapi@tcp(db.example.internal:3306)/substrate"
	*mysqlReadWriteConnectionString = readWrite
	*mysqlOwnerConnectionString = ""
	*mysqlTLSCAFile, *mysqlTLSCertFile, *mysqlTLSKeyFile = "ca.pem", "cert.pem", "key.pem"
	*mysqlPoolMaxConns = 12

	cfg := mysqlConnectConfig()
	if cfg.ReadWriteDSN != readWrite || cfg.OwnerDSN != readWrite {
		t.Fatalf("DSNs = %q, %q, want the owner to default to the read/write DSN", cfg.ReadWriteDSN, cfg.OwnerDSN)
	}
	if cfg.TLS.CAFile != "ca.pem" || cfg.TLS.CertFile != "cert.pem" || cfg.TLS.KeyFile != "key.pem" || cfg.PoolMaxConns != 12 {
		t.Fatalf("config = %+v", cfg)
	}

	const owner = "owner@tcp(db.example.internal:3306)/substrate"
	*mysqlOwnerConnectionString = owner
	if got := mysqlConnectConfig().OwnerDSN; got != owner {
		t.Fatalf("owner DSN = %q, want %q", got, owner)
	}
}

func TestLoadFlagsFromEnvResolvesPostgresSourcesOnce(t *testing.T) {
	oldRuntime, oldDDL := *postgresReadWriteConnectionString, *postgresOwnerConnectionString
	oldRuntimeRole, oldDDLRole := *postgresReadWriteRole, *postgresOwnerRole
	oldAuthz := *experimentalEnableAuthz
	t.Cleanup(func() {
		*postgresReadWriteConnectionString = oldRuntime
		*postgresOwnerConnectionString = oldDDL
		*postgresReadWriteRole = oldRuntimeRole
		*postgresOwnerRole = oldDDLRole
		*experimentalEnableAuthz = oldAuthz
	})
	*postgresReadWriteConnectionString = "@env"
	*postgresOwnerConnectionString = "@env"
	*postgresReadWriteRole = "@env"
	*postgresOwnerRole = "@env"
	*experimentalEnableAuthz = false
	t.Setenv("ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING", "runtime-a")
	t.Setenv("ATE_API_POSTGRES_OWNER_CONNECTION_STRING", "ddl-a")
	t.Setenv("ATE_API_POSTGRES_READ_WRITE_ROLE", "runtime-role")
	t.Setenv("ATE_API_POSTGRES_OWNER_ROLE", "ddl-role")
	t.Setenv("ATE_API_EXPERIMENTAL_ENABLE_AUTHZ", "true")

	if err := loadFlagsFromEnv(); err != nil {
		t.Fatal(err)
	}
	if *postgresReadWriteConnectionString != "runtime-a" || *postgresOwnerConnectionString != "ddl-a" ||
		*postgresReadWriteRole != "runtime-role" || *postgresOwnerRole != "ddl-role" {
		t.Fatalf("resolved values = %q, %q, %q, %q", *postgresReadWriteConnectionString, *postgresOwnerConnectionString, *postgresReadWriteRole, *postgresOwnerRole)
	}
	if !*experimentalEnableAuthz {
		t.Fatal("authorization environment flag was not resolved alongside PostgreSQL settings")
	}
	t.Setenv("ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING", "runtime-b")
	t.Setenv("ATE_API_POSTGRES_OWNER_CONNECTION_STRING", "ddl-b")
	if err := loadFlagsFromEnv(); err != nil {
		t.Fatal(err)
	}
	if *postgresReadWriteConnectionString != "runtime-a" || *postgresOwnerConnectionString != "ddl-a" {
		t.Fatal("environment-backed connection strings changed after startup resolution")
	}
}

func TestLoadFlagsFromEnvPoolMaxConns(t *testing.T) {
	saveFlag(t, storeBackend)
	saveFlag(t, postgresPoolMaxConns)
	*storeBackend = storeBackendPostgres
	t.Setenv("ATE_API_MYSQL_POOL_MAX_CONNS", "invalid")
	t.Setenv("ATE_API_POSTGRES_POOL_MAX_CONNS", "20")
	if err := loadFlagsFromEnv(); err != nil {
		t.Fatal(err)
	}
	if *postgresPoolMaxConns != 20 {
		t.Fatalf("pool max connections = %d, want 20", *postgresPoolMaxConns)
	}
	t.Setenv("ATE_API_POSTGRES_POOL_MAX_CONNS", "invalid")
	if err := loadFlagsFromEnv(); err == nil || !strings.Contains(err.Error(), "ATE_API_POSTGRES_POOL_MAX_CONNS must be a positive integer") {
		t.Fatalf("loadFlagsFromEnv() error = %v, want pool-size validation", err)
	}
}

func TestResolveActorJWTIssuer(t *testing.T) {
	tests := []struct {
		name      string
		flagValue string
		namespace string
		want      string
		wantErr   bool
	}{
		{name: "unset uses the namespace's idp Service", namespace: "ate-system", want: "https://idp.ate-system.svc"},
		{name: "unset in a relocated install", namespace: "team-a", want: "https://idp.team-a.svc"},
		{name: "set is used as given", flagValue: "https://idp.example.com/prod/", namespace: "ate-system", want: "https://idp.example.com/prod/"},
		{name: "set but invalid", flagValue: "http://idp.example.com", namespace: "ate-system", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveActorJWTIssuer(tt.flagValue, tt.namespace)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("resolveActorJWTIssuer(%q, %q) = %q, want error", tt.flagValue, tt.namespace, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveActorJWTIssuer(%q, %q) returned error: %v", tt.flagValue, tt.namespace, err)
			}
			if got != tt.want {
				t.Errorf("resolveActorJWTIssuer(%q, %q) = %q, want %q", tt.flagValue, tt.namespace, got, tt.want)
			}
		})
	}
}

func TestPostgresConnectionAttrNeverLogsThePassword(t *testing.T) {
	const password = "hunter2-very-secret"
	render := func(connString string) string {
		var buf bytes.Buffer
		slog.New(slog.NewJSONHandler(&buf, nil)).LogAttrs(context.Background(), slog.LevelInfo, "Final flag values", postgresConnectionAttr("postgres-connection-string", connString))
		return buf.String()
	}

	for name, connString := range map[string]string{
		"uri":   "postgresql://ateapi:" + password + "@db.example.internal:5433/atepg?sslmode=disable",
		"libpq": "host=db.example.internal port=5433 user=ateapi password=" + password + " dbname=atepg sslmode=disable",
		// A password with URI-reserved characters is percent-encoded in the
		// string; neither the encoded nor the decoded form may appear.
		"uri_percent_encoded": "postgresql://ateapi:" + url.QueryEscape(password+"/#@:?") + "@db.example.internal:5433/atepg?sslmode=disable",
		"libpq_quoted":        "host=db.example.internal port=5433 user=ateapi password='" + password + " with space' dbname=atepg sslmode=disable",
	} {
		t.Run(name, func(t *testing.T) {
			got := render(connString)
			if strings.Contains(got, password) {
				t.Fatalf("log line contains the password: %s", got)
			}
			for _, want := range []string{`"host":"db.example.internal"`, `"port":5433`, `"database":"atepg"`, `"user":"ateapi"`, `"password-set":true`, `"tls":false`} {
				if !strings.Contains(got, want) {
					t.Errorf("log line missing %s: %s", want, got)
				}
			}
		})
	}

	t.Run("tls", func(t *testing.T) {
		got := render("postgresql://ateapi:" + password + "@db.example.internal:5432/atepg?sslmode=require")
		if strings.Contains(got, password) || !strings.Contains(got, `"tls":true`) {
			t.Errorf("expected tls true and no password: %s", got)
		}
	})

	t.Run("passwordless", func(t *testing.T) {
		got := render("postgresql://postgres@postgres.ate-system.svc:5432/atepg?sslmode=disable")
		if !strings.Contains(got, `"password-set":false`) {
			t.Errorf("expected password-set false: %s", got)
		}
	})

	t.Run("unparseable", func(t *testing.T) {
		raw := "postgresql://ateapi:" + password + "@[broken"
		got := render(raw)
		if strings.Contains(got, password) || strings.Contains(got, raw) {
			t.Fatalf("log line echoes an unparseable connection string: %s", got)
		}
		if !strings.Contains(got, `"postgres-connection-string":"<invalid pg connection string>"`) {
			t.Errorf("expected the unparseable marker: %s", got)
		}
	})

	t.Run("empty", func(t *testing.T) {
		if got := render(""); !strings.Contains(got, `"postgres-connection-string":""`) {
			t.Errorf("expected empty marker: %s", got)
		}
	})
}

func TestMySQLConnectionAttrNeverLogsThePassword(t *testing.T) {
	const password = "hunter2-very-secret"
	render := func(connString string) string {
		var buf bytes.Buffer
		slog.New(slog.NewJSONHandler(&buf, nil)).LogAttrs(context.Background(), slog.LevelInfo, "Final flag values", mysqlConnectionAttr("mysql-connection-string", connString))
		return buf.String()
	}

	for name, tc := range map[string]struct {
		connString string
		want       []string
	}{
		"tcp": {
			connString: "ateapi:" + password + "@tcp(db.example.internal:3307)/substrate?parseTime=true",
			want:       []string{`"addr":"db.example.internal:3307"`, `"database":"substrate"`, `"user":"ateapi"`, `"password-set":true`, `"tls":""`},
		},
		"reserved_characters": {
			connString: "ateapi:" + password + "@:x@tcp(db.example.internal:3306)/substrate",
			want:       []string{`"addr":"db.example.internal:3306"`, `"password-set":true`},
		},
		"tls": {
			connString: "ateapi:" + password + "@tcp(db.example.internal:3306)/substrate?tls=true",
			want:       []string{`"tls":"true"`},
		},
		"tls_skip_verify": {
			connString: "ateapi:" + password + "@tcp(db.example.internal:3306)/substrate?tls=skip-verify",
			want:       []string{`"tls":"skip-verify"`},
		},
		"passwordless": {
			connString: "ateapi@tcp(mysql.ate-system.svc:3306)/substrate",
			want:       []string{`"user":"ateapi"`, `"password-set":false`},
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := render(tc.connString)
			if strings.Contains(got, password) {
				t.Fatalf("log line contains the password: %s", got)
			}
			for _, want := range tc.want {
				if !strings.Contains(got, want) {
					t.Errorf("log line missing %s: %s", want, got)
				}
			}
		})
	}

	for name, raw := range map[string]string{
		"missing_database_separator": "ateapi:" + password + "@tcp(db.example.internal:3306)",
		"unknown_tls_config":         "ateapi:" + password + "@tcp(db.example.internal:3306)/substrate?tls=unregistered",
	} {
		t.Run(name, func(t *testing.T) {
			got := render(raw)
			if strings.Contains(got, password) || strings.Contains(got, raw) {
				t.Fatalf("log line echoes an unparseable connection string: %s", got)
			}
			if !strings.Contains(got, `"mysql-connection-string":"<invalid mysql connection string>"`) {
				t.Errorf("expected the unparseable marker: %s", got)
			}
		})
	}

	t.Run("empty", func(t *testing.T) {
		if got := render(""); !strings.Contains(got, `"mysql-connection-string":""`) {
			t.Errorf("expected empty marker: %s", got)
		}
	})
}

// TestLogFlagValuesDoesNotLogADatabasePassword goes through the real startup
// log call, so a change at the call site (logging the flag directly again)
// fails here even if the helper stays correct.
func TestLogFlagValuesDoesNotLogADatabasePassword(t *testing.T) {
	const password = "hunter2-very-secret"
	for _, p := range []*string{postgresReadWriteConnectionString, postgresOwnerConnectionString, mysqlReadWriteConnectionString, mysqlOwnerConnectionString} {
		saveFlag(t, p)
	}
	*postgresReadWriteConnectionString = "postgresql://runtime:" + password + "@db.example.internal:5432/atepg?sslmode=disable"
	*postgresOwnerConnectionString = "postgresql://owner:" + password + "@db.example.internal:5432/atepg?sslmode=disable"
	*mysqlReadWriteConnectionString = "runtime:" + password + "@tcp(db.example.internal:3306)/substrate"
	*mysqlOwnerConnectionString = "owner:" + password + "@tcp(db.example.internal:3306)/substrate"

	var buf bytes.Buffer
	origLogger := slog.Default()
	t.Cleanup(func() { slog.SetDefault(origLogger) })
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))

	logFlagValues(context.Background())

	got := buf.String()
	if !strings.Contains(got, `"msg":"Final flag values"`) {
		t.Fatalf("startup line not emitted: %s", got)
	}
	if strings.Contains(got, password) {
		t.Fatalf("startup line contains the database password: %s", got)
	}
	for _, key := range []string{"postgres-read-write-connection-string", "postgres-owner-connection-string"} {
		if !strings.Contains(got, `"`+key+`":{"host":"db.example.internal"`) {
			t.Errorf("startup line missing the structured %s summary: %s", key, got)
		}
	}
	for _, key := range []string{"mysql-read-write-connection-string", "mysql-owner-connection-string"} {
		if !strings.Contains(got, `"`+key+`":{"addr":"db.example.internal:3306"`) {
			t.Errorf("startup line missing the structured %s summary: %s", key, got)
		}
	}
	if !strings.Contains(got, `"store-backend":`) {
		t.Errorf("startup line missing store-backend: %s", got)
	}
}
