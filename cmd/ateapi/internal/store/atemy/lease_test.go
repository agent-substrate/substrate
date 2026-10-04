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

package atemy

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storesql"
)

// countLeases returns how many lease rows exist for key.
func countLeases(t *testing.T, s *Persistence, key string) int {
	t.Helper()
	return countRows(t, s, `SELECT COUNT(*) FROM leases WHERE lease_key = ?`, key)
}

// seedExpiredLeases inserts n expired leases named expired-1 through
// expired-n.
func seedExpiredLeases(t *testing.T, s *Persistence, n int) {
	t.Helper()
	ctx := t.Context()
	conn, err := s.db.Conn(ctx)
	if err != nil {
		t.Fatalf("acquiring connection: %v", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `SET SESSION cte_max_recursion_depth = ?`, n+1); err != nil {
		t.Fatalf("raising recursion depth: %v", err)
	}
	if _, err := conn.ExecContext(ctx, `
		INSERT INTO leases (lease_key, token, expires_at)
		WITH RECURSIVE n (i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?)
		SELECT CONCAT('expired-', i), 'old', UTC_TIMESTAMP(6) - INTERVAL 1 MINUTE FROM n`, n); err != nil {
		t.Fatalf("seeding leases: %v", err)
	}
}

// TestAcquireLease_LeavesOtherKeysExpiredRows pins the acquisition path down
// to its own key: an expired row for some other key is the maintenance
// loop's to remove, not one more DELETE on every workflow's critical path.
func TestAcquireLease_LeavesOtherKeysExpiredRows(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO leases (lease_key, token, expires_at) VALUES
		('expired', 'old', UTC_TIMESTAMP(6) - INTERVAL 1 MINUTE)`); err != nil {
		t.Fatalf("seeding leases: %v", err)
	}
	lease, err := s.AcquireLease(ctx, "new")
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	defer lease.Close()

	if got := countLeases(t, s, "expired"); got != 1 {
		t.Errorf("expired row for another key after AcquireLease: got %d, want 1 (left for cleanupExpiredLeases)", got)
	}
}

func TestCleanupExpiredLeases_RemovesOnlyExpiredRows(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO leases (lease_key, token, expires_at) VALUES
		('expired', 'old', UTC_TIMESTAMP(6) - INTERVAL 1 MINUTE),
		('active', 'live', UTC_TIMESTAMP(6) + INTERVAL 1 HOUR)`); err != nil {
		t.Fatalf("seeding leases: %v", err)
	}

	deleted, err := s.cleanupExpiredLeases(ctx)
	if err != nil {
		t.Fatalf("cleanupExpiredLeases: %v", err)
	}
	if deleted != 1 {
		t.Errorf("deleted = %d, want 1", deleted)
	}
	if expired, active := countLeases(t, s, "expired"), countLeases(t, s, "active"); expired != 0 || active != 1 {
		t.Errorf("lease counts = expired:%d active:%d, want 0 and 1", expired, active)
	}
}

// TestCleanupExpiredLeases_DrainsAcrossBatches seeds more expired rows than
// one batch holds and checks a single pass keeps going until they are gone.
func TestCleanupExpiredLeases_DrainsAcrossBatches(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	const seeded = storesql.LeaseCleanupBatch*2 + 7
	seedExpiredLeases(t, s, seeded)

	deleted, err := s.cleanupExpiredLeases(ctx)
	if err != nil {
		t.Fatalf("cleanupExpiredLeases: %v", err)
	}
	if deleted != seeded {
		t.Errorf("deleted = %d, want %d", deleted, seeded)
	}
	if remaining := countRows(t, s, `SELECT COUNT(*) FROM leases`); remaining != 0 {
		t.Errorf("rows left after a full pass: %d, want 0", remaining)
	}
}

// TestCleanupExpiredLeases_SkipsLockedRows holds a row lock on one expired
// lease, as a concurrent acquire reclaiming it or another replica's pass
// would, and checks the pass returns without waiting on it and takes the
// row on the next pass once the lock is gone.
func TestCleanupExpiredLeases_SkipsLockedRows(t *testing.T) {
	testCleanupSkipsLockedRows(t, setupMySQLPersistence(t))
}

func testCleanupSkipsLockedRows(t *testing.T, s *Persistence) {
	t.Helper()
	ctx := t.Context()
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO leases (lease_key, token, expires_at) VALUES
		('locked', 'old', UTC_TIMESTAMP(6) - INTERVAL 1 MINUTE),
		('free', 'old', UTC_TIMESTAMP(6) - INTERVAL 1 MINUTE)`); err != nil {
		t.Fatalf("seeding leases: %v", err)
	}
	holder, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("beginning holder transaction: %v", err)
	}
	defer holder.Rollback() //nolint:errcheck // no-op once committed
	var one int
	if err := holder.QueryRowContext(ctx, `SELECT 1 FROM leases WHERE lease_key = 'locked' FOR UPDATE`).Scan(&one); err != nil {
		t.Fatalf("locking row: %v", err)
	}

	passCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	deleted, err := s.cleanupExpiredLeases(passCtx)
	if err != nil {
		t.Fatalf("cleanupExpiredLeases with a locked row: %v", err)
	}
	if deleted != 1 {
		t.Errorf("deleted with one row locked = %d, want 1", deleted)
	}
	if locked, free := countLeases(t, s, "locked"), countLeases(t, s, "free"); locked != 1 || free != 0 {
		t.Errorf("lease counts = locked:%d free:%d, want 1 and 0", locked, free)
	}

	if err := holder.Commit(); err != nil {
		t.Fatalf("releasing row lock: %v", err)
	}
	deleted, err = s.cleanupExpiredLeases(ctx)
	if err != nil {
		t.Fatalf("cleanupExpiredLeases after unlock: %v", err)
	}
	if deleted != 1 || countLeases(t, s, "locked") != 0 {
		t.Errorf("after unlock: deleted = %d and %d rows left, want 1 and 0", deleted, countLeases(t, s, "locked"))
	}
}

func TestAcquireLease_ExpiresAfterHolderStops(t *testing.T) {
	s := setupMySQLPersistence(t)
	s.leaseTTL = 300 * time.Millisecond
	holderCtx, cancelHolder := context.WithCancel(t.Context())
	lease, err := s.AcquireLease(holderCtx, "test-lease")
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}
	// Canceling the holder stops renewal without calling Close, modeling a
	// process that disappeared and left its lease to expire.
	cancelHolder()
	select {
	case <-lease.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("lease context was not cancelled with its holder")
	}
	if _, err := s.AcquireLease(t.Context(), "test-lease"); !errors.Is(err, store.ErrLeaseConflict) {
		t.Fatalf("AcquireLease before the TTL ran out = %v, want ErrLeaseConflict", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		newLease, err := s.AcquireLease(t.Context(), "test-lease")
		if err == nil {
			newLease.Close()
			return
		}
		if !errors.Is(err, store.ErrLeaseConflict) {
			t.Fatalf("AcquireLease after the holder stopped: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("lease was never released by expiry")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// The holder renews the lease past its TTL, and the lease's context ends once
// another token holds the row.
func TestAcquireLease_RenewsUntilTheRowChangesHands(t *testing.T) {
	s := setupMySQLPersistence(t)
	s.leaseTTL = time.Second
	ctx := t.Context()
	lease, err := s.AcquireLease(ctx, "renewed-lease")
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}
	defer lease.Close()

	time.Sleep(2 * s.leaseTTL)
	if err := lease.Context().Err(); err != nil {
		t.Fatalf("lease context ended while its holder was renewing: %v", err)
	}
	if other, err := s.AcquireLease(ctx, "renewed-lease"); !errors.Is(err, store.ErrLeaseConflict) {
		if other != nil {
			other.Close()
		}
		t.Fatalf("AcquireLease after %v of renewal = %v, want ErrLeaseConflict", 2*s.leaseTTL, err)
	}

	if _, err := s.db.ExecContext(ctx, `UPDATE leases SET token = 'usurper' WHERE lease_key = 'renewed-lease'`); err != nil {
		t.Fatalf("replacing the lease token: %v", err)
	}
	select {
	case <-lease.Context().Done():
	case <-time.After(5 * time.Second):
		t.Fatal("lease context outlived the loss of its row")
	}
}

// raceAcquire runs racers concurrent acquisitions of key and returns how many
// won. Winners hold their lease until every racer has tried, so later racers
// cannot win sequentially.
func raceAcquire(t *testing.T, s *Persistence, key string, racers int) int {
	t.Helper()
	winners := make(chan *store.Lease, racers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range racers {
		wg.Go(func() {
			<-start
			lease, err := s.AcquireLease(t.Context(), key)
			if err != nil {
				if !errors.Is(err, store.ErrLeaseConflict) {
					t.Errorf("AcquireLease racer %d failed: %v", i, err)
				}
				return
			}
			winners <- lease
		})
	}
	close(start)
	wg.Wait()
	close(winners)
	won := len(winners)
	for lease := range winners {
		lease.Close()
	}
	return won
}

// TestAcquireLease_ConcurrentTakeover races many goroutines to take over an
// already-expired lease, which they all find through the UPDATE path, and
// asserts exactly one wins.
func TestAcquireLease_ConcurrentTakeover(t *testing.T) {
	s := setupMySQLPersistence(t)
	s.leaseTTL = time.Millisecond
	holderCtx, cancelHolder := context.WithCancel(t.Context())
	initial, err := s.AcquireLease(holderCtx, "contested-lease")
	if err != nil {
		t.Fatalf("seeding initial lease failed: %v", err)
	}
	cancelHolder()
	<-initial.Context().Done()
	deadline := time.Now().Add(5 * time.Second)
	for countRows(t, s, `SELECT COUNT(*) FROM leases WHERE lease_key = 'contested-lease' AND expires_at <= UTC_TIMESTAMP(6)`) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("seeded lease never expired")
		}
		time.Sleep(5 * time.Millisecond)
	}
	s.leaseTTL = 10 * time.Second

	if got := raceAcquire(t, s, "contested-lease", 20); got != 1 {
		t.Errorf("expected exactly 1 racer to win the expired lease, got %d", got)
	}
}

// TestAcquireLease_ConcurrentFirstAcquire races many goroutines to acquire a
// key with no row, which they all find through the INSERT path; the
// duplicate key must leave exactly one winner.
func TestAcquireLease_ConcurrentFirstAcquire(t *testing.T) {
	s := setupMySQLPersistence(t)
	if got := raceAcquire(t, s, "fresh-lease", 20); got != 1 {
		t.Errorf("expected exactly 1 racer to win a fresh lease, got %d", got)
	}
}

// TestAcquireLease_ChurnKeepsOneHolder has many clients acquire and release
// one key at once. Every failure must read as a lost race, and the lease must
// never have two holders.
func TestAcquireLease_ChurnKeepsOneHolder(t *testing.T) {
	s := setupMySQLPersistence(t)
	var holders, maxHolders, acquired atomic.Int32
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Go(func() {
			for i := range 25 {
				lease, err := s.AcquireLease(t.Context(), "churned-lease")
				if err != nil {
					if !errors.Is(err, store.ErrLeaseConflict) {
						t.Errorf("client %d attempt %d: AcquireLease = %v, want nil or ErrLeaseConflict", g, i, err)
					}
					continue
				}
				acquired.Add(1)
				n := holders.Add(1)
				for {
					m := maxHolders.Load()
					if n <= m || maxHolders.CompareAndSwap(m, n) {
						break
					}
				}
				// Hold the lease briefly so overlapping holders would be seen.
				time.Sleep(time.Millisecond)
				holders.Add(-1)
				lease.Close()
			}
		})
	}
	wg.Wait()
	if got := maxHolders.Load(); got != 1 {
		t.Errorf("at most %d clients held the lease at once, want 1", got)
	}
	if acquired.Load() == 0 {
		t.Error("no client ever acquired the lease")
	}
}
