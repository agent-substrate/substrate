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
	"context"
	"strings"
	"testing"
	"time"
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

func TestConnectStoreRejectsNegativeMaxConnectionLifetime(t *testing.T) {
	oldDSN, oldLifetime := *postgresReadWriteConnectionString, *postgresMaxConnLifetime
	t.Cleanup(func() {
		*postgresReadWriteConnectionString = oldDSN
		*postgresMaxConnLifetime = oldLifetime
	})
	*postgresReadWriteConnectionString = "postgres://runtime@postgres/atepg"
	*postgresMaxConnLifetime = -time.Second

	_, err := connectStore(context.Background())
	if err == nil || !strings.Contains(err.Error(), "--postgres-max-conn-lifetime must not be negative") {
		t.Fatalf("connectStore() error = %v, want invalid-lifetime error", err)
	}
}

func TestLoadFlagsFromEnvResolvesPostgresSourcesOnce(t *testing.T) {
	oldRuntime, oldDDL := *postgresReadWriteConnectionString, *postgresOwnerConnectionString
	oldRuntimeRole, oldDDLRole := *postgresReadWriteRole, *postgresOwnerRole
	oldBootstrap := *postgresBootstrap
	t.Cleanup(func() {
		*postgresReadWriteConnectionString = oldRuntime
		*postgresOwnerConnectionString = oldDDL
		*postgresReadWriteRole = oldRuntimeRole
		*postgresOwnerRole = oldDDLRole
		*postgresBootstrap = oldBootstrap
	})
	*postgresReadWriteConnectionString = "@env"
	*postgresOwnerConnectionString = "@env"
	*postgresReadWriteRole = "@env"
	*postgresOwnerRole = "@env"
	*postgresBootstrap = false
	t.Setenv("ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING", "runtime-a")
	t.Setenv("ATE_API_POSTGRES_OWNER_CONNECTION_STRING", "ddl-a")
	t.Setenv("ATE_API_POSTGRES_READ_WRITE_ROLE", "runtime-role")
	t.Setenv("ATE_API_POSTGRES_OWNER_ROLE", "ddl-role")
	t.Setenv("ATE_API_POSTGRES_BOOTSTRAP", "true")

	if err := loadFlagsFromEnv(); err != nil {
		t.Fatal(err)
	}
	if *postgresReadWriteConnectionString != "runtime-a" || *postgresOwnerConnectionString != "ddl-a" ||
		*postgresReadWriteRole != "runtime-role" || *postgresOwnerRole != "ddl-role" || !*postgresBootstrap {
		t.Fatalf("resolved values = %q, %q, %q, %q", *postgresReadWriteConnectionString, *postgresOwnerConnectionString, *postgresReadWriteRole, *postgresOwnerRole)
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

func TestLoadFlagsFromEnvRejectsInvalidBootstrap(t *testing.T) {
	t.Setenv("ATE_API_POSTGRES_BOOTSTRAP", "initialize")
	if err := loadFlagsFromEnv(); err == nil || !strings.Contains(err.Error(), "must be true or false") {
		t.Fatalf("loadFlagsFromEnv() error = %v, want boolean validation", err)
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
