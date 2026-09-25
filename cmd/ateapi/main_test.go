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
	"strings"
	"testing"
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

// The startup log is readable by anyone who can read the pod's logs, so the
// DSN has to reach it without its password even though the flag carries one
// for an external database.
func TestLogFlagValuesRedactsPostgresPassword(t *testing.T) {
	oldDSN := *postgresConnectionString
	t.Cleanup(func() {
		*postgresConnectionString = oldDSN
	})
	*postgresConnectionString = "postgresql://ate:hunter2@db.example.com:5432/atepg?sslmode=require"

	var line bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&line, nil))
	restore := slog.Default()
	slog.SetDefault(logger)
	t.Cleanup(func() { slog.SetDefault(restore) })

	logFlagValues(context.Background())

	if strings.Contains(line.String(), "hunter2") {
		t.Errorf("startup log leaks the DSN password: %s", line.String())
	}
	if !strings.Contains(line.String(), "postgresql://ate:***@db.example.com:5432/atepg") {
		t.Errorf("startup log does not report the redacted DSN: %s", line.String())
	}
}
