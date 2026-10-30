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
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"go.ate.dev/substrate/cmd/ateapi/internal/store/dockerenv"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/pgtestdb"
)

// One Postgres container serves every test in this package; each test gets
// isolation via clearAll rather than a fresh container, which would be
// far slower. Tests in this package are not safe to run with -parallel.
// With pgtestdb.AdminDSNEnv set, a database on that server replaces the
// container and containerPG stays nil.
var (
	containerOnce sync.Once
	containerPool *pgxpool.Pool
	containerDSN  string
	containerRole = "atepg" // the role containerDSN logs in as
	containerPG   *postgres.PostgresContainer
	containerErr  error
	dropDatabase  func(context.Context) error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if containerPool != nil {
		containerPool.Close()
	}
	if dropDatabase != nil {
		if err := dropDatabase(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "dropping PostgreSQL test database: %v\n", err)
			if code == 0 {
				code = 1
			}
		}
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
		dsn, err := startPostgres(ctx)
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
		if pgtestdb.AdminDSN() != "" {
			t.Fatalf("PostgreSQL at %s unavailable: %v", pgtestdb.AdminDSNEnv, containerErr)
		}
		if dockerenv.Required() {
			t.Fatalf("PostgreSQL testcontainer unavailable and required (CI or REQUIRE_DOCKER is set): %v", containerErr)
		}
		t.Skipf("PostgreSQL testcontainer unavailable (requires Docker): %v", containerErr)
	}
	return containerPool
}

// startPostgres returns the URI of a fresh database that containerRole, a
// superuser, logs in to. The URI always has a query string, so tests can
// append parameters with "&".
func startPostgres(ctx context.Context) (string, error) {
	if admin := pgtestdb.AdminDSN(); admin != "" {
		dsn, drop, err := pgtestdb.Create(ctx, admin, "atepg_test")
		if err != nil {
			return "", err
		}
		dropDatabase = drop
		u, err := url.Parse(dsn)
		if err != nil {
			return "", err
		}
		containerRole = u.User.Username()
		if u.RawQuery == "" {
			u.RawQuery = "sslmode=prefer"
		}
		return u.String(), nil
	}
	if err := dockerenv.Configure(ctx); err != nil {
		return "", err
	}
	pgContainer, err := postgres.Run(ctx, "postgres:18-alpine",
		postgres.WithDatabase("atepg"),
		postgres.WithUsername("atepg"),
		postgres.WithPassword("atepg"),
	)
	if err != nil {
		return "", err
	}
	containerPG = pgContainer
	return pgContainer.ConnectionString(ctx, "sslmode=disable")
}

// requireContainer skips tests that need Docker when pgtestdb.AdminDSNEnv
// replaces the container. Like requirePool, it fails instead in CI.
func requireContainer(t *testing.T) {
	t.Helper()
	requirePool(t)
	if containerPG == nil {
		if dockerenv.Required() {
			t.Fatalf("test needs Docker and is required (CI or REQUIRE_DOCKER is set); unset %s", pgtestdb.AdminDSNEnv)
		}
		t.Skipf("test needs Docker; %s is set", pgtestdb.AdminDSNEnv)
	}
}

// withLogin returns dsn with its user and password replaced.
func withLogin(t *testing.T, dsn, user, password string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parsing test DSN: %v", err)
	}
	u.User = url.UserPassword(user, password)
	return u.String()
}

func testConnectConfig(schema string) ConnectConfig {
	return ConnectConfig{
		ReadWriteDSN:  containerDSN,
		OwnerDSN:      containerDSN,
		ReadWriteRole: containerRole,
		OwnerRole:     containerRole,
		Schema:        schema,
	}
}
