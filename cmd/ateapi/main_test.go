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
)

func TestConnectStoreRequiresPostgresReadWriteConnectionString(t *testing.T) {
	oldDSN := *postgresReadWriteConnectionString
	t.Cleanup(func() {
		*postgresReadWriteConnectionString = oldDSN
	})
	*postgresReadWriteConnectionString = ""

	_, err := connectStore(context.Background())
	if err == nil || !strings.Contains(err.Error(), "--postgres-read-write-connection-string is required") {
		t.Fatalf("connectStore() error = %v, want missing-connection-string error", err)
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
	old := *postgresPoolMaxConns
	t.Cleanup(func() { *postgresPoolMaxConns = old })
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

// TestLogFlagValuesDoesNotLogThePostgresPassword goes through the real startup
// log call, so a change at the call site (logging the flag directly again)
// fails here even if the helper stays correct.
func TestLogFlagValuesDoesNotLogThePostgresPassword(t *testing.T) {
	const password = "hunter2-very-secret"
	origReadWrite, origOwner := *postgresReadWriteConnectionString, *postgresOwnerConnectionString
	t.Cleanup(func() {
		*postgresReadWriteConnectionString = origReadWrite
		*postgresOwnerConnectionString = origOwner
	})
	*postgresReadWriteConnectionString = "postgresql://runtime:" + password + "@db.example.internal:5432/atepg?sslmode=disable"
	*postgresOwnerConnectionString = "postgresql://owner:" + password + "@db.example.internal:5432/atepg?sslmode=disable"

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
}
