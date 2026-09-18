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

package atepg

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/dockerenv"
)

// One Postgres container serves every test in this package; each test gets
// isolation via clearAll rather than a fresh container, which would be
// far slower. Tests in this package are not safe to run with -parallel.
var (
	containerOnce sync.Once
	containerPool *pgxpool.Pool
	containerDSN  string
	containerPG   *postgres.PostgresContainer
	containerErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if containerPool != nil {
		containerPool.Close()
	}
	if containerPG != nil {
		if err := containerPG.Terminate(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "terminating PostgreSQL testcontainer: %v\n", err)
			if code == 0 {
				code = 1
			}
		}
	}
	os.Exit(code)
}

func requirePool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	containerOnce.Do(func() {
		ctx := context.Background()
		if err := dockerenv.Configure(ctx); err != nil {
			containerErr = err
			return
		}
		pgContainer, err := postgres.Run(ctx, "postgres:18-alpine",
			postgres.WithDatabase("atepg"),
			postgres.WithUsername("atepg"),
			postgres.WithPassword("atepg"),
		)
		if err != nil {
			containerErr = err
			return
		}
		containerPG = pgContainer
		dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			containerErr = err
			return
		}
		containerDSN = dsn
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			containerErr = err
			return
		}
		// The official postgres image restarts its server process once after
		// initdb; the port accepts (and briefly resets) connections during
		// that window, so ping with retries rather than failing on the first
		// attempt.
		var pingErr error
		for i := 0; i < 30; i++ {
			pingErr = pool.Ping(ctx)
			if pingErr == nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		if pingErr != nil {
			containerErr = fmt.Errorf("pinging PostgreSQL testcontainer after retries: %w", pingErr)
			return
		}
		containerPool = pool
	})
	if containerErr != nil {
		if dockerenv.Required() {
			t.Fatalf("PostgreSQL testcontainer unavailable and required (CI or REQUIRE_DOCKER is set): %v", containerErr)
		}
		t.Skipf("PostgreSQL testcontainer unavailable (requires Docker): %v", containerErr)
	}
	return containerPool
}
