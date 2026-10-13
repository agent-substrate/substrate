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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
)

// countLeases returns how many lease rows exist for key.
func countLeases(t *testing.T, s *Persistence, key string) int {
	t.Helper()
	var n int
	if err := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM leases WHERE key = $1`, key).Scan(&n); err != nil {
		t.Fatalf("counting leases for %q: %v", key, err)
	}
	return n
}

// TestAcquireLease_LeavesOtherKeysExpiredRows pins the acquisition path down
// to its own key: an expired row for some other key is the maintenance
// loop's to remove, not one more DELETE on every workflow's critical path.
func TestAcquireLease_LeavesOtherKeysExpiredRows(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO leases (key, token, expires_at) VALUES
		('expired', 'old', clock_timestamp() - interval '1 minute')`); err != nil {
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
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO leases (key, token, expires_at) VALUES
		('expired', 'old', clock_timestamp() - interval '1 minute'),
		('active', 'live', clock_timestamp() + interval '1 hour')`); err != nil {
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
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	const seeded = leaseCleanupBatch*2 + 7
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO leases (key, token, expires_at)
		SELECT 'expired-' || i, 'old', clock_timestamp() - interval '1 minute'
		FROM generate_series(1, $1) AS i`, seeded); err != nil {
		t.Fatalf("seeding leases: %v", err)
	}

	deleted, err := s.cleanupExpiredLeases(ctx)
	if err != nil {
		t.Fatalf("cleanupExpiredLeases: %v", err)
	}
	if deleted != seeded {
		t.Errorf("deleted = %d, want %d", deleted, seeded)
	}
	var remaining int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM leases`).Scan(&remaining); err != nil {
		t.Fatalf("counting leases: %v", err)
	}
	if remaining != 0 {
		t.Errorf("rows left after a full pass: %d, want 0", remaining)
	}
}

// TestCleanupExpiredLeases_SkipsLockedRows holds a row lock on one expired
// lease, as a concurrent acquire reclaiming it or another replica's pass
// would, and checks the pass returns without waiting on it and takes the
// row on the next pass once the lock is gone.
func TestCleanupExpiredLeases_SkipsLockedRows(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO leases (key, token, expires_at) VALUES
		('locked', 'old', clock_timestamp() - interval '1 minute'),
		('free', 'old', clock_timestamp() - interval '1 minute')`); err != nil {
		t.Fatalf("seeding leases: %v", err)
	}
	holder, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("beginning holder transaction: %v", err)
	}
	defer holder.Rollback(ctx) //nolint:errcheck // no-op once committed
	if _, err := holder.Exec(ctx, `SELECT 1 FROM leases WHERE key = 'locked' FOR UPDATE`); err != nil {
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

	if err := holder.Commit(ctx); err != nil {
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
	s := setupPostgresPersistence(t)
	s.leaseTTL = 200 * time.Millisecond
	holderCtx, cancelHolder := context.WithCancel(context.Background())
	lease, err := s.AcquireLease(holderCtx, "test-lease")
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}
	cancelHolder()
	select {
	case <-lease.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("lease context was not cancelled with its holder")
	}

	// Canceling the holder stops renewal without calling Close, modeling a
	// process that disappeared and left its lease to expire.
	time.Sleep(s.leaseTTL + 500*time.Millisecond)

	newLease, err := s.AcquireLease(context.Background(), "test-lease")
	if err != nil {
		t.Fatalf("AcquireLease after lease expiration failed: %v", err)
	}
	newLease.Close()
}

// TestAcquireLease_ConcurrentTakeover races many goroutines to acquire an
// already-expired lease against the real database, and asserts exactly one
// wins -- the property the doc's conditional-upsert SQL is meant to
// guarantee under real concurrency, which a single-connection unit test
// can't exercise.
func TestAcquireLease_ConcurrentTakeover(t *testing.T) {
	s := setupPostgresPersistence(t)
	s.leaseTTL = time.Millisecond
	holderCtx, cancelHolder := context.WithCancel(context.Background())
	initial, err := s.AcquireLease(holderCtx, "contested-lease")
	if err != nil {
		t.Fatalf("seeding initial lease failed: %v", err)
	}
	cancelHolder()
	<-initial.Context().Done()
	time.Sleep(50 * time.Millisecond) // let the 1ms lease expire.
	s.leaseTTL = 10 * time.Second

	const numRacers = 20
	winners := make(chan *store.Lease, numRacers)
	var wg sync.WaitGroup
	for i := 0; i < numRacers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			lease, err := s.AcquireLease(context.Background(), "contested-lease")
			if err != nil {
				if !errors.Is(err, store.ErrLeaseConflict) {
					t.Errorf("AcquireLease racer %d failed: %v", i, err)
				}
				return
			}
			// Keep the winning lease held until every racer has attempted
			// acquisition. Releasing it here would let later racers win
			// sequentially rather than testing concurrent takeover.
			winners <- lease
		}(i)
	}
	wg.Wait()
	close(winners)

	if got := len(winners); got != 1 {
		t.Errorf("expected exactly 1 racer to win the expired lease, got %d", got)
	}
	for lease := range winners {
		lease.Close()
	}
}
