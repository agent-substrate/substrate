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

// The worker outbox: worker writes append one event row to the
// range-partitioned, UNLOGGED worker_outbox table in the same transaction
// (writeAndAppendEvent), per-replica watchers poll it with an xmin-fenced
// xid cursor (WatchWorkers), and a background loop pre-creates partitions
// and retires old ones by dropping them, recording a trim high-water mark
// that lets lagging watchers detect loss and resync (outboxMaintenance).

package atepg

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
)

// Outbox payload format: one event-type byte followed by the binary Worker proto.
// The tag byte is read by other replicas during rolling deploys, so
// store.WorkerEventType values must stay append-only stable and fit a byte.
func marshalWorkerEvent(eventType store.WorkerEventType, worker *ateapipb.Worker) ([]byte, error) {
	b, err := proto.Marshal(worker)
	if err != nil {
		return nil, fmt.Errorf("in proto.Marshal: %w", err)
	}
	return append([]byte{byte(eventType)}, b...), nil
}

func unmarshalWorkerEvent(payload []byte) (store.WorkerEvent, error) {
	if len(payload) == 0 {
		return store.WorkerEvent{}, fmt.Errorf("empty worker event payload")
	}
	// Assert invariants at the boundary. Corrupted payloads or unknown types
	// must fail here to trigger a loud resync, rather than falling through
	// downstream as silent no-ops.
	eventType := store.WorkerEventType(payload[0])
	switch eventType {
	case store.WorkerEventCreated, store.WorkerEventUpdated, store.WorkerEventDeleted:
	default:
		return store.WorkerEvent{}, fmt.Errorf("unknown worker event type byte %d", payload[0])
	}
	worker := &ateapipb.Worker{}
	if err := proto.Unmarshal(payload[1:], worker); err != nil {
		return store.WorkerEvent{}, fmt.Errorf("in proto.Unmarshal: %w", err)
	}
	if worker.GetMetadata().GetName() == "" {
		return store.WorkerEvent{}, fmt.Errorf("worker event payload has no worker name")
	}
	return store.WorkerEvent{Type: eventType, Worker: worker}, nil
}

// writeAndAppendEvent runs fn inside a transaction, then--only if fn
// reports a change worth publishing--appends the event to the worker_outbox
// table in the same transaction, so watchers see it if and only if the
// transaction commits.
func (p *Persistence) writeAndAppendEvent(ctx context.Context, eventType store.WorkerEventType, worker *ateapipb.Worker, fn func(ctx context.Context, tx pgx.Tx) (changed bool, err error)) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	changed, err := fn(ctx, tx)
	if err != nil {
		return err
	}

	if changed {
		payload, err := marshalWorkerEvent(eventType, worker)
		if err != nil {
			return fmt.Errorf("marshaling worker event: %w", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO worker_outbox (payload) VALUES ($1)`, payload); err != nil {
			return fmt.Errorf("appending worker outbox: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

const (
	// Bound worker-event delivery latency in the absence of an xmin stall.
	outboxPollInterval = 50 * time.Millisecond

	// Cap rows fetched per poll; a burst beyond it carries over to the next poll
	// (events are delayed, never dropped).
	outboxBatch = 1024

	// Minimum time retention keeps outbox rows.
	outboxRetentionAge = 15 * time.Minute

	// Paces partition maintenance.
	outboxMaintenanceInterval = time.Minute

	// The outbox partition range width.
	outboxPartitionInterval = 15 * time.Minute

	// Bounds a maintenance pass to prevent indefinite hangs (e.g., from lock waits)
	// which would permanently starve partition creation. Stalls abort and retry.
	outboxMaintenancePassTimeout = 5 * time.Minute

	// How many intervals ahead partitions are pre-created: creation must stall past
	// lead-1 intervals before any write detours into the DEFAULT partition backstop.
	outboxPartitionLead = 2
)

// Bounds stale-serving during polling outages: after this duration of
// uninterrupted failures, the watch closes and forces a full cache relist.
// Balances riding out transient blips vs. failing fast on real outages.
// Configured per-Persistence-instance to prevent data races in tests.
const outboxPollFailureCloseAfter = 30 * time.Second

// outboxNow returns the database's clock_timestamp — the clock rows route
// by, and therefore the one partition bounds and expiry must use.
func (p *Persistence) outboxNow(ctx context.Context) (time.Time, error) {
	var now time.Time
	if err := p.watchPool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("reading database clock: %w", err)
	}
	return now.UTC(), nil
}

// Maintains worker_outbox partitions on a fixed timer.
func (p *Persistence) outboxMaintenance(ctx context.Context) {
	ticker := time.NewTicker(outboxMaintenanceInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		passCtx, cancel := context.WithTimeout(ctx, outboxMaintenancePassTimeout)
		if err := p.maintainWorkerOutboxPartitions(passCtx); err != nil && ctx.Err() == nil {
			slog.WarnContext(ctx, "worker outbox maintenance failed", slog.Any("err", err))
		}
		cancel()
	}
}

// Database-scoped advisory lock used to elect a single replica to run the retention transaction (drops + trim).
const outboxMaintenanceLockKey = "atepg-outbox-maintenance"

// pollWorkerOutboxSQL is the watch's batch query. The xid::text cast MUST
// carry an alias so that ORDER BY xid sorts numerically by the table's xid8
// column instead of alphabetically by the string output.
const pollWorkerOutboxSQL = `
	SELECT xid::text AS xid_text, payload FROM worker_outbox
	WHERE xid > $1::xid8
	  AND xid < pg_snapshot_xmin(pg_current_snapshot())
	ORDER BY xid LIMIT $2`

// pollSafetySQL returns cheap safety scalars fetched on every poll:
// 1. A fell-behind check (trim mark is past both cursor and baseline).
// 2. The postmaster start time (to detect database restarts).
const pollSafetySQL = `
	SELECT EXISTS(
		SELECT 1 FROM worker_outbox_trim
		WHERE xid > $1::xid8 AND xid > $2::xid8),
	pg_postmaster_start_time()::text`

// maintainWorkerOutboxPartitions runs one maintenance pass. Partition creation
// is unelected. DEFAULT truncate and partition drops run in SEPARATE elected
// transactions to prevent AB/BA deadlocks against writers.
func (p *Persistence) maintainWorkerOutboxPartitions(ctx context.Context) error {
	// Must use the database's clock. App-sourced time would let a fast-clocked
	// replica accidentally drift partition bounds and shorten retention.
	now, err := p.outboxNow(ctx)
	if err != nil {
		return err
	}
	if err := p.createWorkerOutboxPartitions(ctx, outboxPartitionLeadTimes(now)...); err != nil {
		return err
	}

	if err := p.retireStrayedOutboxDefault(ctx); err != nil {
		return err
	}
	return p.dropExpiredOutboxRetention(ctx, now)
}

// retireStrayedOutboxDefault truncates a non-empty DEFAULT partition in its
// own elected transaction. Locks touched: DEFAULT child only — never the
// parent (see maintainWorkerOutboxPartitions on deadlock ordering).
func (p *Persistence) retireStrayedOutboxDefault(ctx context.Context) error {
	tx, err := p.watchPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning outbox stray-cleanup transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	var elected bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtext(current_database() || ':' || $1))`, outboxMaintenanceLockKey).Scan(&elected); err != nil {
		return fmt.Errorf("electing outbox maintenance: %w", err)
	}
	if !elected {
		return nil // another replica is maintaining; next tick retries
	}
	// A non-empty DEFAULT partition means partition creation stalled and writes
	// detoured here. Watchers that lose events will detect the trim mark and resync.
	var strays bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM worker_outbox_default)`).Scan(&strays); err != nil {
		return fmt.Errorf("checking outbox default partition: %w", err)
	}
	if !strays {
		// Nothing to clean; end the election transaction explicitly rather
		// than leaning on the deferred rollback for a read-only exit.
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("committing outbox stray-cleanup transaction: %w", err)
		}
		return nil
	}
	slog.WarnContext(ctx, "outbox DEFAULT partition is non-empty; partition creation has stalled and writes are detouring")
	if err := p.truncateWorkerOutboxDefault(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing outbox stray-cleanup transaction: %w", err)
	}
	return nil
}

// dropExpiredOutboxRetention drops aged-out partitions in its own elected
// transaction. Locks touched: parent (ACCESS EXCLUSIVE, blocking every
// worker write's outbox append) plus the dropped children — never the
// DEFAULT while waiting on the parent.
func (p *Persistence) dropExpiredOutboxRetention(ctx context.Context, now time.Time) error {
	tx, err := p.watchPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning outbox retention transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	var elected bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtext(current_database() || ':' || $1))`, outboxMaintenanceLockKey).Scan(&elected); err != nil {
		return fmt.Errorf("electing outbox maintenance: %w", err)
	}
	if !elected {
		return nil // another replica is maintaining; next tick retries
	}
	// Drops run last in the pass with commit immediately after, keeping the
	// writer-blocking window minimal.
	if err := p.dropExpiredWorkerOutboxPartitions(ctx, tx, now); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing outbox retention transaction: %w", err)
	}
	return nil
}

// workerOutboxPartitionName names the partition covering the given instant.
func workerOutboxPartitionName(at time.Time) string {
	// it truncates to the partition boundary itself so callers can pass any moment within the range.
	return "worker_outbox_p" + at.UTC().Truncate(outboxPartitionInterval).Format("200601021504")
}

// outboxPartitionLeadTimes lists instants covering now through the creation lead, one per partition interval.
func outboxPartitionLeadTimes(now time.Time) []time.Time {
	times := make([]time.Time, outboxPartitionLead+1)
	for i := range times {
		times[i] = now.UTC().Add(time.Duration(i) * outboxPartitionInterval)
	}
	return times
}

// createWorkerOutboxPartitions idempotently creates partitions.
// If writes spilled into DEFAULT, CREATE PARTITION fails (23514). We catch
// this, TRUNCATE the strays (triggering watcher resyncs), and run CREATE
// PARTITION inside the SAME transaction to safely un-wedge the system.
func (p *Persistence) createWorkerOutboxPartitions(ctx context.Context, instants ...time.Time) error {
	err := p.tryCreateWorkerOutboxPartitions(ctx, false, instants...)
	if err == nil || !isCheckViolation(err) {
		return err
	}
	slog.WarnContext(ctx, "outbox DEFAULT partition holds rows in a range being created; truncating strays to un-wedge partition creation",
		slog.Any("err", err))
	return p.tryCreateWorkerOutboxPartitions(ctx, true, instants...)
}

// isCheckViolation matches SQLSTATE 23514, which CREATE ... PARTITION OF
// raises when the DEFAULT partition holds rows inside the new range.
func isCheckViolation(err error) bool { return pgErrCode(err) == "23514" }

func (p *Persistence) tryCreateWorkerOutboxPartitions(ctx context.Context, truncateStrays bool, instants ...time.Time) error {
	tx, err := p.watchPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning outbox partition transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('agent-substrate-atepg-outbox-partitions'))`); err != nil {
		return fmt.Errorf("locking outbox partition DDL: %w", err)
	}
	if truncateStrays {
		// Parent BEFORE child: Writers lock parent-then-child, so locking DEFAULT
		// first (and later needing parent for CREATE PARTITION) causes deadlocks.
		// Locking the parent first matches writer order, and holding it through
		// the CREATEs prevents concurrent writes from re-seeding DEFAULT mid-rescue.
		if _, err := tx.Exec(ctx, `LOCK TABLE worker_outbox IN ACCESS EXCLUSIVE MODE`); err != nil {
			return fmt.Errorf("locking outbox parent for stray rescue: %w", err)
		}
		if err := p.truncateWorkerOutboxDefault(ctx, tx); err != nil {
			return err
		}
	}
	for _, at := range instants {
		start := at.UTC().Truncate(outboxPartitionInterval)
		// UNLOGGED: see schema comment for the durability trade-off.
		// autovacuum off: partitions are insert-only and discarded whole,
		// so autovacuum is unnecessary and its scans would cause latency spikes.
		stmt := fmt.Sprintf(`CREATE UNLOGGED TABLE IF NOT EXISTS %s PARTITION OF worker_outbox FOR VALUES FROM ('%s') TO ('%s') WITH (autovacuum_enabled = off)`,
			workerOutboxPartitionName(start), start.Format(time.RFC3339), start.Add(outboxPartitionInterval).Format(time.RFC3339))
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("creating outbox partition for %s: %w", start, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing outbox partition transaction: %w", err)
	}
	return nil
}

// dropExpiredWorkerOutboxPartitions drops every outbox partition whose
// entire range is older than retention.
func (p *Persistence) dropExpiredWorkerOutboxPartitions(ctx context.Context, q querier, now time.Time) error {
	rows, err := q.Query(ctx, `
		SELECT c.relname FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid
		JOIN pg_class parent ON parent.oid = i.inhparent
		WHERE parent.relname = 'worker_outbox'`)
	if err != nil {
		return fmt.Errorf("listing outbox partitions: %w", err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return fmt.Errorf("scanning outbox partition name: %w", err)
		}
		names = append(names, name)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("listing outbox partitions: %w", err)
	}

	for _, name := range names {
		// The DEFAULT partition (worker_outbox_default) doesn't match
		// the range prefix and is skipped here naturally.
		suffix, ok := strings.CutPrefix(name, "worker_outbox_p")
		if !ok {
			continue
		}
		start, err := time.Parse("200601021504", suffix)
		if err != nil {
			continue // not a partition this maintenance loop manages
		}
		if now.Sub(start.Add(outboxPartitionInterval)) < outboxRetentionAge {
			continue
		}
		if err := p.dropWorkerOutboxPartition(ctx, q, name); err != nil {
			return err
		}
	}
	return nil
}

// dropWorkerOutboxPartition records the trim mark and drops the
// partition on the caller's (elected, single) retention transaction.
func (p *Persistence) dropWorkerOutboxPartition(ctx context.Context, q querier, name string) error {
	ident := pgx.Identifier{name}.Sanitize()
	// The mark is the partition's greatest xid.
	if _, err := q.Exec(ctx, fmt.Sprintf(`
		INSERT INTO worker_outbox_trim (xid)
		SELECT xid FROM %s ORDER BY xid DESC LIMIT 1
		ON CONFLICT (id) DO UPDATE SET xid = EXCLUDED.xid
		WHERE EXCLUDED.xid > worker_outbox_trim.xid`, ident)); err != nil {
		return fmt.Errorf("recording trim mark for outbox partition %s: %w", name, err)
	}
	if _, err := q.Exec(ctx, `DROP TABLE `+ident); err != nil {
		return fmt.Errorf("dropping outbox partition %s: %w", name, err)
	}
	return nil
}

// truncateWorkerOutboxDefault discards the DEFAULT partition wholesale to
// un-stall partition creation.
func (p *Persistence) truncateWorkerOutboxDefault(ctx context.Context, q querier) error {
	// Lock BEFORE reading the trim mark to block concurrent writers. This ensures
	// our snapshot sees exactly what TRUNCATE will destroy, preventing silent data loss.
	if _, err := q.Exec(ctx, `LOCK TABLE worker_outbox_default IN ACCESS EXCLUSIVE MODE`); err != nil {
		return fmt.Errorf("locking outbox default partition: %w", err)
	}
	// Highest xid is recorded as a trim mark in the same transaction so lagging watchers detect the loss and resync.
	if _, err := q.Exec(ctx, `
		INSERT INTO worker_outbox_trim (xid)
		SELECT xid FROM worker_outbox_default ORDER BY xid DESC LIMIT 1
		ON CONFLICT (id) DO UPDATE SET xid = EXCLUDED.xid
		WHERE EXCLUDED.xid > worker_outbox_trim.xid`); err != nil {
		return fmt.Errorf("recording trim mark for outbox default partition: %w", err)
	}
	if _, err := q.Exec(ctx, `TRUNCATE worker_outbox_default`); err != nil {
		return fmt.Errorf("truncating outbox default partition: %w", err)
	}
	return nil
}

// WatchWorkers subscribes by polling the worker_outbox table using an xid cursor.
// It fences reads behind pg_snapshot_xmin (the oldest in-flight transaction)
// to guarantee gap-free delivery. Note that a long-running transaction anywhere
// in the database will stall delivery.
//
// Events are delivered in xid order, so consumers must reconcile worker versions.
// If the watcher detects missed events—either by lagging behind retention drops
// or if a database restart truncates the UNLOGGED partitions—it closes the channel
// to force the consumer to resync from the primary tables.
func (p *Persistence) WatchWorkers(ctx context.Context) (*store.WorkerWatch, error) {
	watchCtx, cancel := context.WithCancel(ctx)

	// cursor starts at xmin-1. baseline records pre-subscribe history (xid < xmin)
	// so past drops aren't mistaken for losses, while preventing artificially
	// inflated baselines from masking the future drop of slow, in-flight txs.
	var cursorXid, baselineXid, baselineStart string
	if err := p.watchPool.QueryRow(watchCtx, `
		SELECT (pg_snapshot_xmin(pg_current_snapshot())::text::numeric - 1)::text,
		       GREATEST(
		           COALESCE((SELECT max(xid) FROM worker_outbox
		                     WHERE xid < pg_snapshot_xmin(pg_current_snapshot())), '0'::xid8),
		           COALESCE((SELECT xid FROM worker_outbox_trim), '0'::xid8))::text,
		       pg_postmaster_start_time()::text`).Scan(&cursorXid, &baselineXid, &baselineStart); err != nil {
		cancel()
		return nil, fmt.Errorf("reading worker outbox cursor: %w", err)
	}

	ch := make(chan store.WorkerEvent, 128)
	go func() {
		defer close(ch)
		ticker := time.NewTicker(outboxPollInterval)
		defer ticker.Stop()
		// failingSince limits how long consumers serve stale state during an outage.
		// Past outboxPollFailureCloseAfter, the channel closes. Unlike the postmaster
		// restart check (which fires when connectivity returns), this surfaces the
		// actual outage in real-time.
		var failingSince time.Time
		for {
			select {
			case <-watchCtx.Done():
				return
			case <-ticker.C:
			}
			// Drain until a batch is partial. Sleeping between full batches would
			// cap throughput and cause unrecoverable lag during bursts.
			for {
				// Safety checks share the batch round trip but must remain separate
				// queries so we can detect gaps and restarts even when no rows match.
				b := &pgx.Batch{}
				b.Queue(pollWorkerOutboxSQL, cursorXid, outboxBatch)
				b.Queue(pollSafetySQL, cursorXid, baselineXid)
				br := p.watchPool.SendBatch(watchCtx, b)

				type feedRow struct {
					xid     string
					payload []byte
				}
				var batch []feedRow
				rows, err := br.Query()
				if err == nil {
					for rows.Next() {
						var r feedRow
						if err = rows.Scan(&r.xid, &r.payload); err != nil {
							batch = nil
							break
						}
						batch = append(batch, r)
					}
					rows.Close()
				}
				var fellBehind bool
				var pmStart string
				if err == nil {
					err = br.QueryRow().Scan(&fellBehind, &pmStart)
				}
				if closeErr := br.Close(); err == nil {
					err = closeErr
				}
				if err != nil {
					if watchCtx.Err() != nil {
						return
					}
					// Transient failure: retry next tick. Persistent failure (past the
					// deadline): close the watch to flip workercache not-ready, forcing
					// callers to fail fast instead of serving a frozen fleet view.
					if failingSince.IsZero() {
						failingSince = time.Now()
					} else if time.Since(failingSince) > p.pollFailureCloseAfter {
						slog.WarnContext(watchCtx, "worker outbox polling has failed persistently; closing watch",
							slog.Duration("failing_for", time.Since(failingSince)), slog.Any("err", err))
						return
					}
					slog.WarnContext(watchCtx, "worker outbox poll failed", slog.Any("err", err))
					break
				}
				failingSince = time.Time{}
				// A restarted postmaster truncated the UNLOGGED outbox:
				// committed-but-undelivered events may be gone, so close
				// before the cursor can skip past them; consumers resync
				// with a full relist.
				if pmStart != baselineStart {
					slog.WarnContext(watchCtx, "database restarted under the outbox; closing watch for resync",
						slog.String("was", baselineStart), slog.String("now", pmStart))
					return
				}
				// Retention safety: if retention's recorded trim high-water
				// mark is ahead of everything this watcher has seen, a row
				// it never consumed was discarded. Close before delivering
				// anything past the gap.
				if fellBehind {
					slog.WarnContext(watchCtx, "worker watch fell behind outbox retention; closing for resync",
						slog.String("cursor_xid", cursorXid))
					return
				}

				for _, r := range batch {
					event, err := unmarshalWorkerEvent(r.payload)
					if err != nil {
						// Close to force a relist. Skipping it would cause silent
						// data loss. The fresh watch starts at the current xmin,
						// naturally bypassing the corrupt row to prevent a boot loop.
						slog.ErrorContext(watchCtx, "worker event unmarshal failed; closing watch for resync",
							slog.String("xid", r.xid), slog.Any("err", err))
						return
					}
					select {
					case ch <- event:
						cursorXid = r.xid
					case <-watchCtx.Done():
						return
					}
				}
				if len(batch) < outboxBatch {
					break // caught up; wait for the next tick
				}
			}
		}
	}()
	return store.NewWorkerWatch(ch, cancel), nil
}
