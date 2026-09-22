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

func TestConnectStoreRequiresPostgresConnectionString(t *testing.T) {
	oldDSN := *postgresConnectionString
	t.Cleanup(func() {
		*postgresConnectionString = oldDSN
	})
	*postgresConnectionString = ""

	_, err := connectStore(context.Background())
	if err == nil || !strings.Contains(err.Error(), "--postgres-connection-string is required") {
		t.Fatalf("connectStore() error = %v, want missing-connection-string error", err)
	}
}

func TestConnectStoreRejectsNegativeMaxConnectionLifetime(t *testing.T) {
	oldDSN, oldLifetime := *postgresConnectionString, *postgresMaxConnLifetime
	t.Cleanup(func() {
		*postgresConnectionString = oldDSN
		*postgresMaxConnLifetime = oldLifetime
	})
	*postgresConnectionString = "postgres://runtime@postgres/atepg"
	*postgresMaxConnLifetime = -time.Second

	_, err := connectStore(context.Background())
	if err == nil || !strings.Contains(err.Error(), "--postgres-max-conn-lifetime must not be negative") {
		t.Fatalf("connectStore() error = %v, want invalid-lifetime error", err)
	}
}

func TestLoadFlagsFromEnvResolvesPostgresSourcesOnce(t *testing.T) {
	oldRuntime, oldDDL := *postgresConnectionString, *postgresDDLConnectionString
	oldRuntimeRole, oldDDLRole := *postgresRuntimeRole, *postgresDDLRole
	t.Cleanup(func() {
		*postgresConnectionString = oldRuntime
		*postgresDDLConnectionString = oldDDL
		*postgresRuntimeRole = oldRuntimeRole
		*postgresDDLRole = oldDDLRole
	})
	*postgresConnectionString = "@env"
	*postgresDDLConnectionString = "@env"
	*postgresRuntimeRole = "@env"
	*postgresDDLRole = "@env"
	t.Setenv("ATE_API_POSTGRES_CONNECTION_STRING", "runtime-a")
	t.Setenv("ATE_API_POSTGRES_DDL_CONNECTION_STRING", "ddl-a")
	t.Setenv("ATE_API_POSTGRES_RUNTIME_ROLE", "runtime-role")
	t.Setenv("ATE_API_POSTGRES_DDL_ROLE", "ddl-role")

	loadFlagsFromEnv()
	if *postgresConnectionString != "runtime-a" || *postgresDDLConnectionString != "ddl-a" ||
		*postgresRuntimeRole != "runtime-role" || *postgresDDLRole != "ddl-role" {
		t.Fatalf("resolved values = %q, %q, %q, %q", *postgresConnectionString, *postgresDDLConnectionString, *postgresRuntimeRole, *postgresDDLRole)
	}
	t.Setenv("ATE_API_POSTGRES_CONNECTION_STRING", "runtime-b")
	t.Setenv("ATE_API_POSTGRES_DDL_CONNECTION_STRING", "ddl-b")
	loadFlagsFromEnv()
	if *postgresConnectionString != "runtime-a" || *postgresDDLConnectionString != "ddl-a" {
		t.Fatal("environment-backed connection strings changed after startup resolution")
	}
}
