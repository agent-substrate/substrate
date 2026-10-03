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

package storesql

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/google/uuid"
)

// DefaultLeaseTTL is how long a lease may go unrenewed before another client
// can reclaim it.
const DefaultLeaseTTL = 30 * time.Second

// LeaseCleanupBatch bounds one DELETE of a cleanup pass, so a backlog of
// expired rows drains over several short statements rather than one long
// one holding many row locks.
const LeaseCleanupBatch = 1000

// LeaseSQL is the database half of a distributed lease. Each func is atomic
// and decides expiry by the database clock; AcquireLease supplies the token
// and drives renewal and release.
type LeaseSQL struct {
	// Database names the backend in log lines.
	Database string
	// Acquire takes key for token unless an unexpired holder has it.
	Acquire func(ctx context.Context, key, token string, ttl time.Duration) (bool, error)
	// Renew extends key while token still holds it unexpired.
	Renew func(ctx context.Context, key, token string, ttl time.Duration) (bool, error)
	// Release drops key if token still holds it.
	Release func(ctx context.Context, key, token string) error
}

// AcquireLease takes key for ttl and renews it until the returned Lease is
// closed. Returns store.ErrLeaseConflict if another client holds it.
func (l LeaseSQL) AcquireLease(ctx context.Context, key string, ttl time.Duration) (*store.Lease, error) {
	token := uuid.NewString()
	// Acquisition runs before any workflow step span opens, so log the
	// query's duration to make this window attributable.
	t := time.Now()
	acquired, err := l.Acquire(ctx, key, token, ttl)
	dAcquire := time.Since(t)
	slog.InfoContext(ctx, "lease acquisition finished",
		slog.String("database", l.Database),
		slog.String("key", key),
		slog.Bool("acquired", acquired && err == nil),
		slog.Duration("acquire", dAcquire))
	if err != nil {
		return nil, err
	}
	if !acquired {
		return nil, store.ErrLeaseConflict
	}

	leaseCtx, cancel := context.WithCancel(ctx)
	renewalDone := make(chan struct{})
	go func() {
		defer close(renewalDone)
		defer cancel()
		l.renewLoop(leaseCtx, key, token, ttl)
	}()

	closeFn := func() {
		// Close runs after the last workflow step span ends but inside the
		// operation, so log its two waits: the renewal goroutine may be
		// mid-query when cancelled, and the release DELETE may wait on a
		// concurrent write to the same row.
		t := time.Now()
		cancel()
		<-renewalDone
		dRenewalStop := time.Since(t)

		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer releaseCancel()
		t = time.Now()
		if err := l.Release(releaseCtx, key, token); err != nil {
			slog.WarnContext(releaseCtx, "failed to release lease, relying on TTL to reclaim it", "database", l.Database, "key", key, "error", err)
		}
		slog.InfoContext(releaseCtx, "lease released",
			slog.String("database", l.Database),
			slog.String("key", key),
			slog.Duration("renewal_stop", dRenewalStop),
			slog.Duration("release", time.Since(t)))
	}
	return store.NewLease(leaseCtx, closeFn), nil
}

const (
	renewIntervalDivisor    = 3
	renewRetryPeriodDivisor = 10
	renewDeadlineFraction   = 2.0 / 3.0
)

func (l LeaseSQL) renewLoop(ctx context.Context, key, token string, ttl time.Duration) {
	interval := ttl / renewIntervalDivisor
	renewDeadline := time.Duration(float64(ttl) * renewDeadlineFraction)

	lastRenewed := time.Now()
	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			renewCtx, cancel := context.WithDeadline(ctx, lastRenewed.Add(renewDeadline))
			renewed := l.tryRenew(renewCtx, key, token, ttl)
			cancel()
			if !renewed {
				return
			}
			lastRenewed = time.Now()
			timer.Reset(interval)
		}
	}
}

func (l LeaseSQL) tryRenew(ctx context.Context, key, token string, ttl time.Duration) bool {
	retryPeriod := ttl / renewRetryPeriodDivisor
	retry := time.NewTimer(0)
	defer retry.Stop()

	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				slog.WarnContext(ctx, "failed to renew lease before its deadline", "database", l.Database, "key", key)
			}
			return false
		case <-retry.C:
			renewed, err := l.Renew(ctx, key, token, ttl)
			if ctx.Err() != nil {
				return false
			}
			switch {
			case err == nil && renewed:
				return true
			case err == nil:
				slog.WarnContext(ctx, "lease renewal found lease no longer owned", "database", l.Database, "key", key)
				return false
			default:
				slog.WarnContext(ctx, "failed to renew lease, retrying", "database", l.Database, "key", key, "error", err)
				retry.Reset(retryPeriod)
			}
		}
	}
}
