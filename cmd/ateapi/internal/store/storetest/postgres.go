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

package storetest

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/authz"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/atepg"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/dockerenv"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

var (
	pgOnce      sync.Once
	adminPool   *pgxpool.Pool
	containerPG *postgres.PostgresContainer
	pgErr       error
)

// SetupPostgresPersistence returns an isolated atepg persistence instance.
func SetupPostgresPersistence(t *testing.T) *atepg.Persistence {
	t.Helper()
	ctx := context.Background()
	admin := requireAdminPool(t)
	databaseName := nextDatabaseName()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+databaseName); err != nil {
		t.Fatalf("creating PostgreSQL test database: %v", err)
	}

	config := admin.Config().Copy()
	config.ConnConfig.Database = databaseName
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("connecting to PostgreSQL test database: %v", err)
	}
	persistence, err := atepg.NewPersistence(ctx, pool)
	if err != nil {
		pool.Close()
		t.Fatalf("creating PostgreSQL persistence: %v", err)
	}
	t.Cleanup(func() {
		persistence.Close()
		pool.Close()
		if _, err := admin.Exec(context.Background(), "DROP DATABASE "+databaseName); err != nil {
			t.Errorf("dropping PostgreSQL test database: %v", err)
		}
	})
	// The server always wires a PolicyManager, so test stores do too.
	fgaServer, err := authz.NewOpenFGAServer(authz.PostgresBackend(pool))
	if err != nil {
		t.Fatalf("creating OpenFGA server: %v", err)
	}
	t.Cleanup(fgaServer.Close)
	_, policyManager, err := authz.New(ctx, authz.PostgresBackend(pool), fgaServer, nil)
	if err != nil {
		t.Fatalf("initializing OpenFGA authz: %v", err)
	}
	persistence.SetPolicyManager(policyManager)
	return persistence
}

func shutdownPostgres() {
	if adminPool != nil {
		adminPool.Close()
		adminPool = nil
	}
	if containerPG != nil {
		if err := containerPG.Terminate(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "terminating PostgreSQL testcontainer: %v\n", err)
		}
		containerPG = nil
	}
}

func requireAdminPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pgOnce.Do(func() {
		ctx := context.Background()
		if err := dockerenv.Configure(ctx); err != nil {
			pgErr = err
			return
		}
		containerPG, pgErr = postgres.Run(ctx, "postgres:18-alpine",
			postgres.WithDatabase("postgres"),
			postgres.WithUsername("postgres"),
			postgres.WithPassword("postgres"),
		)
		if pgErr != nil {
			return
		}
		dsn, err := containerPG.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			pgErr = err
			return
		}
		adminPool, pgErr = pgxpool.New(ctx, dsn)
		if pgErr != nil {
			return
		}
		var pingErr error
		for i := 0; i < 30; i++ {
			pingErr = adminPool.Ping(ctx)
			if pingErr == nil {
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
		adminPool.Close()
		adminPool = nil
		pgErr = fmt.Errorf("pinging PostgreSQL testcontainer after retries: %w", pingErr)
	})
	if pgErr != nil {
		if dockerenv.Required() {
			t.Fatalf("PostgreSQL testcontainer unavailable and required (CI or REQUIRE_DOCKER is set): %v", pgErr)
		}
		t.Skipf("PostgreSQL testcontainer unavailable (requires Docker): %v", pgErr)
	}
	return adminPool
}
