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
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"testing"
	"time"

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
	*storeBackend = storeBackendMySQL
	*mysqlReadWriteConnectionString = ""

	_, err := connectStore(context.Background())
	if err == nil || !strings.Contains(err.Error(), "--mysql-read-write-connection-string is required") {
		t.Fatalf("connectStore() error = %v, want missing MySQL connection string error", err)
	}
}

func TestConnectStoreRejectsNegativePoolMaxConns(t *testing.T) {
	saveFlag(t, storePoolMaxConns)
	*storePoolMaxConns = -1

	_, err := connectStore(context.Background())
	if err == nil || !strings.Contains(err.Error(), "--store-pool-max-conns must not be negative") {
		t.Fatalf("connectStore() error = %v, want pool-size validation", err)
	}
}

func TestConnectWithRetries(t *testing.T) {
	saveFlag(t, &storeConnectTries)
	saveFlag(t, &storeConnectPeriod)
	storeConnectTries = 3
	storeConnectPeriod = time.Millisecond
	unavailable := errors.New("unavailable")
	permanent := errors.New("bad credentials")
	retryable := fmt.Errorf("dial: %w", unavailable)

	tests := []struct {
		name      string
		errs      []error
		wantCalls int
		wantErr   error
	}{
		{name: "succeeds after unavailable", errs: []error{retryable, retryable, nil}, wantCalls: 3},
		{name: "other error returns at once", errs: []error{permanent}, wantCalls: 1, wantErr: permanent},
		{name: "unavailable on every try", errs: []error{retryable, retryable, retryable}, wantCalls: 3, wantErr: unavailable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			got, err := connectWithRetries(t.Context(), "test", unavailable, func() (int, error) {
				if calls == len(tc.errs) {
					t.Fatalf("connect called more than %d times", len(tc.errs))
				}
				err := tc.errs[calls]
				calls++
				if err != nil {
					return 0, err
				}
				return 1, nil
			})
			if calls != tc.wantCalls {
				t.Errorf("connect called %d times, want %d", calls, tc.wantCalls)
			}
			if tc.wantErr == nil {
				if err != nil || got != 1 {
					t.Fatalf("connectWithRetries() = %d, %v, want 1, nil", got, err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("connectWithRetries() error = %v, want %v", err, tc.wantErr)
			}
		})
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
		{name: "env applies when flag unset", flagValue: storeBackendPostgres, env: storeBackendMySQL, want: storeBackendMySQL},
		{name: "flag wins over env", flagValue: storeBackendPostgres, flagChanged: true, env: storeBackendMySQL, want: storeBackendPostgres},
		{name: "invalid env", flagValue: storeBackendPostgres, env: "sqlite", wantErr: true},
		{name: "invalid flag", flagValue: "sqlite", flagChanged: true, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			saveFlag(t, storeBackend)
			*storeBackend = tc.flagValue
			if tc.flagChanged {
				markFlagChanged(t, "store-backend")
			}
			t.Setenv("ATE_API_STORE_BACKEND", tc.env)

			err := loadFlagsFromEnv()
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "--store-backend must be") {
					t.Fatalf("loadFlagsFromEnv() error = %v, want backend validation", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if *storeBackend != tc.want {
				t.Fatalf("store backend = %q, want %q", *storeBackend, tc.want)
			}
		})
	}
}

func TestLoadFlagsFromEnvResolvesSourcesOnce(t *testing.T) {
	tests := []struct {
		flag *string
		env  string
		// explicit is a value set on the command line instead of @env.
		explicit string
	}{
		{flag: postgresReadWriteConnectionString, env: "ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING"},
		{flag: postgresOwnerConnectionString, env: "ATE_API_POSTGRES_OWNER_CONNECTION_STRING"},
		{flag: postgresReadWriteRole, env: "ATE_API_POSTGRES_READ_WRITE_ROLE"},
		{flag: postgresOwnerRole, env: "ATE_API_POSTGRES_OWNER_ROLE"},
		{flag: postgresSchema, env: "ATE_API_POSTGRES_SCHEMA"},
		{flag: mysqlReadWriteConnectionString, env: "ATE_API_MYSQL_READ_WRITE_CONNECTION_STRING"},
		{flag: mysqlOwnerConnectionString, env: "ATE_API_MYSQL_OWNER_CONNECTION_STRING"},
		{flag: mysqlTLSCAFile, env: "ATE_API_MYSQL_TLS_CA_FILE"},
		{flag: mysqlTLSCertFile, env: "ATE_API_MYSQL_TLS_CERT_FILE"},
		{flag: mysqlTLSKeyFile, env: "ATE_API_MYSQL_TLS_KEY_FILE", explicit: "/etc/mysql/client.key"},
	}
	for _, tc := range tests {
		saveFlag(t, tc.flag)
		*tc.flag = "@env"
		if tc.explicit != "" {
			*tc.flag = tc.explicit
		}
		t.Setenv(tc.env, tc.env+"-a")
	}
	saveFlag(t, experimentalEnableAuthz)
	*experimentalEnableAuthz = false
	t.Setenv("ATE_API_EXPERIMENTAL_ENABLE_AUTHZ", "true")

	check := func() {
		t.Helper()
		for _, tc := range tests {
			want := tc.env + "-a"
			if tc.explicit != "" {
				want = tc.explicit
			}
			if *tc.flag != want {
				t.Errorf("%s resolved to %q, want %q", tc.env, *tc.flag, want)
			}
		}
	}
	if err := loadFlagsFromEnv(); err != nil {
		t.Fatal(err)
	}
	check()
	if !*experimentalEnableAuthz {
		t.Error("authorization environment flag was not resolved")
	}

	for _, tc := range tests {
		t.Setenv(tc.env, tc.env+"-b")
	}
	if err := loadFlagsFromEnv(); err != nil {
		t.Fatal(err)
	}
	check()
}

func TestLoadFlagsFromEnvPoolMaxConns(t *testing.T) {
	saveFlag(t, storePoolMaxConns)
	*storePoolMaxConns = 0
	t.Setenv("ATE_API_STORE_POOL_MAX_CONNS", "20")
	if err := loadFlagsFromEnv(); err != nil {
		t.Fatal(err)
	}
	if *storePoolMaxConns != 20 {
		t.Fatalf("pool max connections = %d, want 20", *storePoolMaxConns)
	}
	for _, raw := range []string{"invalid", "0", "-5", "4294967296"} {
		t.Setenv("ATE_API_STORE_POOL_MAX_CONNS", raw)
		if err := loadFlagsFromEnv(); err == nil || !strings.Contains(err.Error(), "ATE_API_STORE_POOL_MAX_CONNS must be a positive integer") {
			t.Fatalf("loadFlagsFromEnv() with %q error = %v, want pool-size validation", raw, err)
		}
	}

	*storePoolMaxConns = 7
	markFlagChanged(t, "store-pool-max-conns")
	t.Setenv("ATE_API_STORE_POOL_MAX_CONNS", "invalid")
	if err := loadFlagsFromEnv(); err != nil {
		t.Fatalf("loadFlagsFromEnv() read the environment for an explicit flag: %v", err)
	}
	if *storePoolMaxConns != 7 {
		t.Fatalf("pool max connections = %d, want the flag value 7", *storePoolMaxConns)
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
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveActorJWTIssuer(tc.flagValue, tc.namespace)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("resolveActorJWTIssuer(%q, %q) = %q, want error", tc.flagValue, tc.namespace, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveActorJWTIssuer(%q, %q) returned error: %v", tc.flagValue, tc.namespace, err)
			}
			if got != tc.want {
				t.Errorf("resolveActorJWTIssuer(%q, %q) = %q, want %q", tc.flagValue, tc.namespace, got, tc.want)
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
			want:       []string{`"host":"db.example.internal"`, `"port":3307`, `"database":"substrate"`, `"user":"ateapi"`, `"password-set":true`, `"tls":false`},
		},
		"reserved_characters": {
			connString: "ateapi:" + password + "@:x@tcp(db.example.internal:3306)/substrate",
			want:       []string{`"host":"db.example.internal"`, `"port":3306`, `"password-set":true`},
		},
		"tls": {
			connString: "ateapi:" + password + "@tcp(db.example.internal:3306)/substrate?tls=true",
			want:       []string{`"tls":true`},
		},
		"tls_false": {
			connString: "ateapi:" + password + "@tcp(db.example.internal:3306)/substrate?tls=false",
			want:       []string{`"tls":false`},
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

	t.Run("tls_files", func(t *testing.T) {
		saveFlag(t, mysqlTLSCAFile)
		*mysqlTLSCAFile = "/run/mysql-server-ca/server-ca.pem"
		if got := render("ateapi@tcp(db.example.internal:3306)/substrate"); !strings.Contains(got, `"tls":true`) {
			t.Errorf("log line reports TLS off with a CA file set: %s", got)
		}
	})

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
		if !strings.Contains(got, `"`+key+`":{"host":"db.example.internal","port":3306`) {
			t.Errorf("startup line missing the structured %s summary: %s", key, got)
		}
	}
	if !strings.Contains(got, `"store-backend":`) {
		t.Errorf("startup line missing store-backend: %s", got)
	}
}
