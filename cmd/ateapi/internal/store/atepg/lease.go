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
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storesql"
	"github.com/jackc/pgx/v5"
)

func (p *Persistence) AcquireLease(ctx context.Context, key string) (*store.Lease, error) {
	return storesql.LeaseSQL{
		Database: "PostgreSQL",
		Acquire:  p.acquireLease,
		Renew:    p.renewLease,
		Release:  p.releaseLease,
	}.AcquireLease(ctx, key, p.leaseTTL)
}

// cleanupExpiredLeases deletes expired lease rows and returns how many it
// removed. Acquisition reclaims an expired row for its own key by itself, so
// this only keeps rows for keys nobody asks for again from accumulating. It
// runs from the maintenance loop.
//
// Rows are taken in batches with SKIP LOCKED, so a pass never waits on a
// concurrent acquire reclaiming a row or on another replica's pass, and two
// replicas cleaning at once delete disjoint rows. A pass keeps going while
// batches come back full and stops on the first short one.
func (p *Persistence) cleanupExpiredLeases(ctx context.Context) (int64, error) {
	var deleted int64
	for {
		tag, err := p.watchPool.Exec(ctx, `
			DELETE FROM leases
			WHERE key IN (
				SELECT key FROM leases
				WHERE expires_at <= clock_timestamp()
				LIMIT $1
				FOR UPDATE SKIP LOCKED)`, storesql.LeaseCleanupBatch)
		if err != nil {
			return deleted, fmt.Errorf("deleting expired leases: %w", err)
		}
		deleted += tag.RowsAffected()
		if tag.RowsAffected() < storesql.LeaseCleanupBatch {
			return deleted, nil
		}
	}
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
