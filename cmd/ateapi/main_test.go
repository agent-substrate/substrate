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
	t.Cleanup(func() {
		*postgresConnectionString = oldRuntime
		*postgresDDLConnectionString = oldDDL
	})
	*postgresConnectionString = "@env"
	*postgresDDLConnectionString = "@env"
	t.Setenv("ATE_API_POSTGRES_CONNECTION_STRING", "runtime-a")
	t.Setenv("ATE_API_POSTGRES_DDL_CONNECTION_STRING", "ddl-a")

	loadFlagsFromEnv()
	if *postgresConnectionString != "runtime-a" || *postgresDDLConnectionString != "ddl-a" {
		t.Fatalf("resolved values = %q, %q", *postgresConnectionString, *postgresDDLConnectionString)
	}
	t.Setenv("ATE_API_POSTGRES_CONNECTION_STRING", "runtime-b")
	t.Setenv("ATE_API_POSTGRES_DDL_CONNECTION_STRING", "ddl-b")
	loadFlagsFromEnv()
	if *postgresConnectionString != "runtime-a" || *postgresDDLConnectionString != "ddl-a" {
		t.Fatal("environment-backed connection strings changed after startup resolution")
	}
}
