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
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storesql"
)

// Lease expiry is decided by the database clock. UTC_TIMESTAMP is fixed at
// the start of each statement, which for these single-statement writes is the
// moment PostgreSQL's clock_timestamp() would report.

func (p *Persistence) AcquireLease(ctx context.Context, key string) (*store.Lease, error) {
	return storesql.LeaseSQL{
		Database: "MySQL",
		Acquire:  p.acquireLease,
		Renew:    p.renewLease,
		Release:  p.releaseLease,
	}.AcquireLease(ctx, key, p.leaseTTL)
}

// acquireLease takes over an expired row, or inserts one if the key has none.
// A single INSERT ... ON DUPLICATE KEY UPDATE cannot report whether it took
// the row: with CLIENT_FOUND_ROWS an unchanged duplicate counts as affected,
// like an insert. So this is two statements; each is atomic and a racing
// acquirer loses on one of them. A concurrent takeover waits on
// the row lock and then finds the row unexpired. A concurrent insert hits the
// duplicate key, or, when the row was just released, a deadlock InnoDB
// resolves in favor of the other inserter.
func (p *Persistence) acquireLease(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	res, err := p.db.ExecContext(ctx, `
		UPDATE leases
		SET token = ?, expires_at = UTC_TIMESTAMP(6) + INTERVAL ? MICROSECOND
		WHERE lease_key = ? AND expires_at <= UTC_TIMESTAMP(6)`, token, ttl.Microseconds(), key)
	if err != nil {
		return false, fmt.Errorf("acquiring lease for %q: %w", key, err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return false, fmt.Errorf("acquiring lease for %q: %w", key, err)
	} else if n == 1 {
		return true, nil
	}
	if _, err := p.db.ExecContext(ctx, `
		INSERT INTO leases (lease_key, token, expires_at)
		VALUES (?, ?, UTC_TIMESTAMP(6) + INTERVAL ? MICROSECOND)`, key, token, ttl.Microseconds()); err != nil {
		if isUniqueViolation(err) || mysqlErrNumber(err) == errDeadlock {
			return false, nil
		}
		return false, fmt.Errorf("acquiring lease for %q: %w", key, err)
	}
	return true, nil
}

func (p *Persistence) renewLease(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	res, err := p.db.ExecContext(ctx, `
		UPDATE leases
		SET expires_at = UTC_TIMESTAMP(6) + INTERVAL ? MICROSECOND
		WHERE lease_key = ? AND token = ? AND expires_at > UTC_TIMESTAMP(6)`, ttl.Microseconds(), key, token)
	if err != nil {
		return false, fmt.Errorf("renewing lease for %q: %w", key, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("renewing lease for %q: %w", key, err)
	}
	return n == 1, nil
}

func (p *Persistence) releaseLease(ctx context.Context, key, token string) error {
	if _, err := p.db.ExecContext(ctx, `DELETE FROM leases WHERE lease_key = ? AND token = ?`, key, token); err != nil {
		return fmt.Errorf("releasing lease for %q: %w", key, err)
	}
	return nil
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
		var batch int64
		err := inTx(ctx, p.watchDB, func(tx *sql.Tx) error {
			rows, err := tx.QueryContext(ctx, `
				SELECT lease_key FROM leases
				WHERE expires_at <= UTC_TIMESTAMP(6)
				LIMIT ?
				FOR UPDATE SKIP LOCKED`, storesql.LeaseCleanupBatch)
			if err != nil {
				return err
			}
			var keys []any
			for rows.Next() {
				var key string
				if err := rows.Scan(&key); err != nil {
					rows.Close()
					return err
				}
				keys = append(keys, key)
			}
			rows.Close()
			if err := rows.Err(); err != nil || len(keys) == 0 {
				return err
			}
			res, err := tx.ExecContext(ctx, `DELETE FROM leases WHERE lease_key IN (?`+strings.Repeat(", ?", len(keys)-1)+`)`, keys...)
			if err != nil {
				return err
			}
			batch, err = res.RowsAffected()
			return err
		})
		if err != nil {
			return deleted, fmt.Errorf("deleting expired leases: %w", err)
		}
		deleted += batch
		if batch < storesql.LeaseCleanupBatch {
			return deleted, nil
		}
	}
}
