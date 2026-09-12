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
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPoolSnapshotsMapDistinctPoolsByRole(t *testing.T) {
	requirePool(t)
	operational := poolWithMaxConns(t, 7)
	watch := poolWithMaxConns(t, 4)

	operationalConn, err := operational.Acquire(t.Context())
	if err != nil {
		t.Fatalf("acquire operational connection: %v", err)
	}
	defer operationalConn.Release()
	watchConn1, err := watch.Acquire(t.Context())
	if err != nil {
		t.Fatalf("acquire first watch connection: %v", err)
	}
	defer watchConn1.Release()
	watchConn2, err := watch.Acquire(t.Context())
	if err != nil {
		t.Fatalf("acquire second watch connection: %v", err)
	}
	defer watchConn2.Release()

	persistence := &Persistence{pool: operational, watchPool: watch}
	got := persistence.PoolSnapshots()
	if got.Operational.AcquiredConns != 1 || got.Operational.IdleConns != 0 || got.Operational.MaxConns != 7 {
		t.Fatalf("operational snapshot = %#v, want acquired=1 idle=0 max=7", got.Operational)
	}
	if got.Watch.AcquiredConns != 2 || got.Watch.IdleConns != 0 || got.Watch.MaxConns != 4 {
		t.Fatalf("watch snapshot = %#v, want acquired=2 idle=0 max=4", got.Watch)
	}
}

func TestPoolSnapshotsPreserveAliasedNewPersistenceRoles(t *testing.T) {
	admin := requirePool(t)
	const schema = "pool-snapshots-aliased"
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(t.Context(), `DROP SCHEMA IF EXISTS `+quoted+` CASCADE; CREATE SCHEMA `+quoted); err != nil {
		t.Fatalf("prepare schema: %v", err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+quoted+` CASCADE`) })

	pool := openPool(t, schema, nil)
	persistence, err := NewPersistence(t.Context(), pool)
	if err != nil {
		t.Fatalf("NewPersistence: %v", err)
	}
	// Stop maintenance so the only pool transitions below come from the test.
	persistence.Close()

	const workers = 8
	ctx, cancel := context.WithCancel(t.Context())
	start := make(chan struct{})
	errs := make(chan error, workers)
	var ready sync.WaitGroup
	var wg sync.WaitGroup
	ready.Add(workers)
	for worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ready.Done()
			for iteration := 0; ; iteration++ {
				conn, err := pool.Acquire(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					errs <- fmt.Errorf("worker %d iteration %d: %w", worker, iteration, err)
					return
				}
				conn.Release()
			}
		}()
	}
	close(start)
	ready.Wait()

	var mismatch *PoolSnapshots
	for range 20_000 {
		got := persistence.PoolSnapshots()
		if got.Operational != got.Watch {
			mismatch = &got
			break
		}
	}
	cancel()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if mismatch != nil {
		t.Fatalf("aliased role snapshots differ: operational=%#v watch=%#v", mismatch.Operational, mismatch.Watch)
	}
}

func poolWithMaxConns(t *testing.T, maxConns int32) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(containerDSN)
	if err != nil {
		t.Fatalf("parse pool config: %v", err)
	}
	cfg.MaxConns = maxConns
	cfg.MinConns = 0
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}
