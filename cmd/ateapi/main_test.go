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
