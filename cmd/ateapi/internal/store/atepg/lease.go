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
	"fmt"
	"log/slog"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// defaultLeaseTTL is how long a lease may go unrenewed before another client
// can reclaim it.
const defaultLeaseTTL = 30 * time.Second

func (p *Persistence) AcquireLease(ctx context.Context, key string) (*store.Lease, error) {
	ttl := p.leaseTTL
	token := uuid.NewString()
	// Acquisition runs before any workflow step span opens, so log the two
	// queries' durations to make this window attributable: the cleanup DELETE
	// scans the whole table and contends with concurrent acquires/releases.
	t := time.Now()
	if err := p.cleanupExpiredLeases(ctx); err != nil {
		slog.WarnContext(ctx, "failed to clean up expired PostgreSQL leases", "error", err)
	}
	dCleanup := time.Since(t)

	t = time.Now()
	acquired, err := p.acquireLease(ctx, key, token, ttl)
	dAcquire := time.Since(t)
	slog.InfoContext(ctx, "PostgreSQL lease acquisition finished",
		slog.String("key", key),
		slog.Bool("acquired", acquired && err == nil),
		slog.Duration("cleanup_expired", dCleanup),
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
		p.renewLeaseLoop(leaseCtx, key, token, ttl)
	}()

	closeFn := func() {
		// Close runs after the last workflow step span ends but inside the
		// operation, so log its two waits: the renewal goroutine may be
		// mid-query when cancelled, and the release DELETE contends with
		// concurrent acquires' full-table cleanup DELETEs.
		t := time.Now()
		cancel()
		<-renewalDone
		dRenewalStop := time.Since(t)

		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer releaseCancel()
		t = time.Now()
		if err := p.releaseLease(releaseCtx, key, token); err != nil {
			slog.WarnContext(releaseCtx, "failed to release PostgreSQL lease, relying on TTL to reclaim it", "key", key, "error", err)
		}
		slog.InfoContext(releaseCtx, "PostgreSQL lease released",
			slog.String("key", key),
			slog.Duration("renewal_stop", dRenewalStop),
			slog.Duration("release", time.Since(t)))
	}
	return store.NewLease(leaseCtx, closeFn), nil
}

func (p *Persistence) cleanupExpiredLeases(ctx context.Context) error {
	if _, err := p.pool.Exec(ctx, `DELETE FROM leases WHERE expires_at <= clock_timestamp()`); err != nil {
		return fmt.Errorf("deleting expired leases: %w", err)
	}
	return nil
}

func (p *Persistence) acquireLease(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	var returnedKey string
	err := p.pool.QueryRow(ctx, `
		INSERT INTO leases (key, token, expires_at)
		VALUES ($1, $2, clock_timestamp() + make_interval(secs => $3))
		ON CONFLICT (key) DO UPDATE
		SET token = EXCLUDED.token,
		    expires_at = EXCLUDED.expires_at
		WHERE leases.expires_at <= clock_timestamp()
		RETURNING key`, key, token, ttl.Seconds()).Scan(&returnedKey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("acquiring lease for %q: %w", key, err)
	}
	return true, nil
}

const (
	renewIntervalDivisor    = 3
	renewRetryPeriodDivisor = 10
	renewDeadlineFraction   = 2.0 / 3.0
)

func (p *Persistence) renewLeaseLoop(ctx context.Context, key, token string, ttl time.Duration) {
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
			renewed := p.tryRenewLease(renewCtx, key, token, ttl)
			cancel()
			if !renewed {
				return
			}
			lastRenewed = time.Now()
			timer.Reset(interval)
		}
	}
}

func (p *Persistence) tryRenewLease(ctx context.Context, key, token string, ttl time.Duration) bool {
	retryPeriod := ttl / renewRetryPeriodDivisor
	retry := time.NewTimer(0)
	defer retry.Stop()

	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				slog.WarnContext(ctx, "failed to renew PostgreSQL lease before its deadline", "key", key)
			}
			return false
		case <-retry.C:
			renewed, err := p.renewLease(ctx, key, token, ttl)
			if ctx.Err() != nil {
				return false
			}
			switch {
			case err == nil && renewed:
				return true
			case err == nil:
				slog.WarnContext(ctx, "PostgreSQL lease renewal found lease no longer owned", "key", key)
				return false
			default:
				slog.WarnContext(ctx, "failed to renew PostgreSQL lease, retrying", "key", key, "error", err)
				retry.Reset(retryPeriod)
			}
		}
	}
}

func (p *Persistence) renewLease(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	var returnedKey string
	err := p.pool.QueryRow(ctx, `
		UPDATE leases
		SET expires_at = clock_timestamp() + make_interval(secs => $3)
		WHERE key = $1 AND token = $2 AND expires_at > clock_timestamp()
		RETURNING key`, key, token, ttl.Seconds()).Scan(&returnedKey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("renewing lease for %q: %w", key, err)
	}
	return true, nil
}

func (p *Persistence) releaseLease(ctx context.Context, key, token string) error {
	if _, err := p.pool.Exec(ctx, `DELETE FROM leases WHERE key = $1 AND token = $2`, key, token); err != nil {
		return fmt.Errorf("releasing lease for %q: %w", key, err)
	}
	return nil
}
