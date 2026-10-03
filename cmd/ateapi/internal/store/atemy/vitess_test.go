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

//go:build vitess

package atemy

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/dockerenv"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storecontract"
)

// vitessImage runs vtgate and an unsharded keyspace in one container, the
// query layer PlanetScale serves. It is built for linux/amd64 only and is
// large, so these tests sit behind the vitess build tag and CI runs them in a
// step of their own.
const vitessImage = "vitess/vttestserver:mysql80"

// TestVitess runs the store through vtgate, so a statement Vitess rejects or
// rewrites fails here rather than on PlanetScale. Foreign keys are disallowed,
// as on a PlanetScale database by default.
func TestVitess(t *testing.T) {
	db := startVitess(t)
	ctx := t.Context()

	// A restart must find the migration ledger the first start wrote.
	for range 2 {
		p, err := NewPersistence(ctx, db)
		if err != nil {
			t.Fatalf("NewPersistence through vtgate failed: %v", err)
		}
		p.Close()
	}

	storecontract.RunContractTests(t, func(t *testing.T) store.Interface {
		t.Helper()
		return setupPersistenceOn(t, db)
	})
	// The rollback probe's FOR SHARE NOWAIT must pass through vtgate.
	t.Run("DropsOnlyRolledBackSeqs", func(t *testing.T) {
		testDropsOnlyRolledBackSeqs(t, setupPersistenceOn(t, db))
	})
}

func startVitess(t *testing.T) *sql.DB {
	t.Helper()
	ctx := context.Background()
	fail := t.Skipf
	if dockerenv.Required() {
		fail = t.Fatalf
	}
	if err := dockerenv.Configure(ctx); err != nil {
		fail("configuring Docker for the Vitess testcontainer: %v", err)
	}
	const port = "33577/tcp" // vtgate's MySQL port: PORT + 3
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:         vitessImage,
			ImagePlatform: "linux/amd64",
			Env: map[string]string{
				"PORT":             "33574",
				"KEYSPACES":        "atemy",
				"NUM_SHARDS":       "1",
				"MYSQL_BIND_HOST":  "0.0.0.0",
				"FOREIGN_KEY_MODE": "disallow",
			},
			ExposedPorts: []string{port},
			WaitingFor:   wait.ForListeningPort(port).WithStartupTimeout(10 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		fail("Vitess testcontainer unavailable (requires Docker; CI or REQUIRE_DOCKER makes this fatal): %v", err)
	}
	t.Cleanup(func() {
		if err := c.Terminate(context.Background()); err != nil {
			t.Errorf("terminating Vitess testcontainer: %v", err)
		}
	})
	endpoint, err := c.PortEndpoint(ctx, port, "")
	if err != nil {
		t.Fatalf("reading the vtgate endpoint: %v", err)
	}
	db, err := Open(fmt.Sprintf("root@tcp(%s)/atemy", endpoint))
	if err != nil {
		t.Fatalf("opening vtgate: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	// vtgate accepts connections before the keyspace's tablet is serving.
	var pingErr error
	for deadline := time.Now().Add(5 * time.Minute); time.Now().Before(deadline); time.Sleep(time.Second) {
		if _, pingErr = db.ExecContext(ctx, `SELECT 1 FROM dual`); pingErr == nil {
			var n int
			if pingErr = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE()`).Scan(&n); pingErr == nil {
				return db
			}
		}
	}
	t.Fatalf("vtgate never served the atemy keyspace: %v", pingErr)
	return nil
}
