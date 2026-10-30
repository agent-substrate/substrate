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

// Package pgtestdb lets PostgreSQL tests run against an existing server
// instead of a testcontainer, for hosts without Docker.
package pgtestdb

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
)

// AdminDSNEnv names the environment variable holding a superuser URI, such as
// postgres://postgres@localhost:5432/postgres?sslmode=disable. When it is
// set, tests create their databases on that server.
const AdminDSNEnv = "ATE_TEST_POSTGRES_ADMIN_DSN"

// AdminDSN returns the value of AdminDSNEnv, or "" when unset.
func AdminDSN() string { return os.Getenv(AdminDSNEnv) }

var databaseCount atomic.Uint64

// UniqueName returns prefix with the process ID, a counter, and a random
// suffix, so test processes sharing a server pick different names.
func UniqueName(prefix string) string {
	return fmt.Sprintf("%s_%d_%d_%s", prefix, os.Getpid(), databaseCount.Add(1), strings.ToLower(rand.Text()[:8]))
}

// Create creates a database named UniqueName(prefix) on the server at
// adminDSN. It returns adminDSN pointed at the new database, and a func that
// drops it.
func Create(ctx context.Context, adminDSN, prefix string) (string, func(context.Context) error, error) {
	u, err := url.Parse(adminDSN)
	if err != nil || u.Scheme == "" {
		return "", nil, fmt.Errorf("%s must be a postgres:// URI", AdminDSNEnv)
	}
	name := UniqueName(prefix)
	if err := exec(ctx, adminDSN, "CREATE DATABASE "+name); err != nil {
		return "", nil, fmt.Errorf("creating database %s: %w", name, err)
	}
	u.Path = "/" + name
	drop := func(ctx context.Context) error {
		return exec(ctx, adminDSN, "DROP DATABASE "+name+" WITH (FORCE)")
	}
	return u.String(), drop, nil
}

func exec(ctx context.Context, dsn, sql string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, sql)
	return err
}
