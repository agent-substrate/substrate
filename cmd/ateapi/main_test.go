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
)

func TestConnectStoreRejectsUnknownBackend(t *testing.T) {
	oldBackend := *storeBackend
	t.Cleanup(func() { *storeBackend = oldBackend })
	*storeBackend = "unknown"

	_, err := connectStore(context.Background())
	if err == nil || !strings.Contains(err.Error(), `unknown --store-backend "unknown"`) {
		t.Fatalf("connectStore() error = %v, want unknown-backend error", err)
	}
}

func TestConnectStoreRequiresPostgresConnectionString(t *testing.T) {
	oldBackend, oldDSN := *storeBackend, *postgresConnectionString
	t.Cleanup(func() {
		*storeBackend = oldBackend
		*postgresConnectionString = oldDSN
	})
	*storeBackend = "postgres"
	*postgresConnectionString = ""

	_, err := connectStore(context.Background())
	if err == nil || !strings.Contains(err.Error(), "requires --postgres-connection-string") {
		t.Fatalf("connectStore() error = %v, want missing-connection-string error", err)
	}
}
