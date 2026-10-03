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

// The worker outbox: every worker write appends one event row to
// worker_outbox in the same transaction (writeAndAppendEvent), per-replica
// watchers poll it with a seq cursor (WatchWorkers), and the maintenance loop
// deletes rows past retention and records how far it deleted, so lagging
// watchers detect the loss and resync (trimWorkerOutbox).
//
// PostgreSQL fences its xid cursor behind the oldest in-flight transaction.
// MySQL exposes no such horizon, so seq is handed out by a single counter
// row that each worker write locks from its append until it commits. Writes
// therefore commit in seq order, and a row a poller has not yet seen can
// never carry a seq below the cursor. The cost is that worker writes
// serialize for the duration of their final statement and commit.

package atemy

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storesql"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// writeAndAppendEvent runs fn inside a transaction, then--only if fn reports a
// worker worth publishing--appends the event to the worker_outbox table in the
// same transaction, so watchers see it if and only if the transaction commits.
// fn returns the worker the event carries, or nil to skip the event; it is
// returned from writeAndAppendEvent so callers get back what actually
// committed.
func (p *Persistence) writeAndAppendEvent(ctx context.Context, eventType store.WorkerEventType, fn func(ctx context.Context, tx *sql.Tx) (*ateapipb.Worker, error)) (*ateapipb.Worker, error) {
	var worker *ateapipb.Worker
	var payload []byte
	err := inTx(ctx, p.db, func(tx *sql.Tx) error {
		var err error
		worker, err = fn(ctx, tx)
		if err != nil || worker == nil {
			return err
		}
		payload, err = storesql.MarshalWorkerEvent(eventType, worker)
		if err != nil {
			return fmt.Errorf("marshaling worker event: %w", err)
		}
		// The counter row lock is taken last and held to commit; see the
		// file comment.
		if _, err := tx.ExecContext(ctx, `UPDATE worker_outbox_sequence SET seq = seq + 1 WHERE id = 1`); err != nil {
			return fmt.Errorf("advancing worker outbox sequence: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO worker_outbox (seq, created_at, payload)
			SELECT seq, UTC_TIMESTAMP(6), ? FROM worker_outbox_sequence WHERE id = 1`, payload); err != nil {
			return fmt.Errorf("appending worker outbox: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if payload != nil {
		p.watchers.Publish(ctx, payload)
	}
	return worker, nil
}

const (
	// Bound worker-event delivery latency.
	outboxPollInterval = 50 * time.Millisecond

	// Cap rows fetched per poll; a burst beyond it carries over to the next poll
	// (events are delayed, never dropped).
	outboxBatch = 1024

	// Minimum time retention keeps outbox rows.
	outboxRetentionAge = 15 * time.Minute

	// Rows one retention transaction deletes.
	outboxTrimBatch = 1000
)

// Bounds stale-serving during polling outages: after this duration of
// uninterrupted failures, the watch closes and forces a full cache relist.
const outboxPollFailureCloseAfter = 30 * time.Second

// trimWorkerOutbox deletes rows older than retention, oldest first.
func (p *Persistence) trimWorkerOutbox(ctx context.Context) error {
	return p.trimWorkerOutboxOlderThan(ctx, outboxRetentionAge)
}

// trimWorkerOutboxOlderThan deletes, in batches, the longest seq prefix of
// rows created more than age ago by the database clock. Each batch raises the
// trim mark to its greatest seq in the same transaction. Locking the trim row
// first serializes replicas running maintenance at once.
func (p *Persistence) trimWorkerOutboxOlderThan(ctx context.Context, age time.Duration) error {
	for {
		var deleted int
		err := inTx(ctx, p.watchDB, func(tx *sql.Tx) error {
			var mark uint64
			if err := tx.QueryRowContext(ctx, `SELECT seq FROM worker_outbox_trim WHERE id = 1 FOR UPDATE`).Scan(&mark); err != nil {
				return fmt.Errorf("locking outbox trim mark: %w", err)
			}
			rows, err := tx.QueryContext(ctx, `
				SELECT seq, created_at < UTC_TIMESTAMP(6) - INTERVAL ? MICROSECOND
				FROM worker_outbox ORDER BY seq LIMIT ?`, age.Microseconds(), outboxTrimBatch)
			if err != nil {
				return fmt.Errorf("reading expired outbox rows: %w", err)
			}
			var last uint64
			for rows.Next() {
				var seq uint64
				var expired bool
				if err := rows.Scan(&seq, &expired); err != nil {
					rows.Close()
					return fmt.Errorf("scanning outbox row: %w", err)
				}
				if !expired {
					break
				}
				last = seq
				deleted++
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return fmt.Errorf("reading expired outbox rows: %w", err)
			}
			if deleted == 0 {
				return nil
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM worker_outbox WHERE seq <= ?`, last); err != nil {
				return fmt.Errorf("deleting expired outbox rows: %w", err)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE worker_outbox_trim SET seq = GREATEST(seq, ?) WHERE id = 1`, last); err != nil {
				return fmt.Errorf("recording outbox trim mark: %w", err)
			}
			return nil
		})
		if err != nil || deleted < outboxTrimBatch {
			return err
		}
	}
}

// WatchWorkers subscribes by polling the worker_outbox table with a seq
// cursor. The cursor starts at the last committed seq, so the watch sees every
// write that commits after it subscribes and none from before.
//
// This process's own writes are published at commit, ahead of the poll, and
// again on the poll, so consumers must reconcile versions and tolerate
// duplicates.
//
// If the watcher falls behind retention, it closes the channel to force the
// consumer to resync from the primary tables. Unlike atepg's UNLOGGED outbox,
// worker_outbox is durable, so a database restart loses no events.
func (p *Persistence) WatchWorkers(ctx context.Context) (*store.WorkerWatch, error) {
	watchCtx, cancel := context.WithCancel(ctx)

	var cursor uint64
	if err := p.watchDB.QueryRowContext(watchCtx, `SELECT seq FROM worker_outbox_sequence WHERE id = 1`).Scan(&cursor); err != nil {
		cancel()
		return nil, fmt.Errorf("reading worker outbox cursor: %w", err)
	}

	ch := make(chan store.WorkerEvent, 128)
	// Committed writes in this process are published straight onto ch, ahead
	// of the poll that would carry them.
	p.watchers.Add(ch)
	go func() {
		defer func() {
			p.watchers.Remove(ch)
			close(ch)
		}()
		ticker := time.NewTicker(outboxPollInterval)
		defer ticker.Stop()
		// failingSince limits how long consumers serve stale state during an
		// outage. Past pollFailureCloseAfter, the channel closes.
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
				batch, fellBehind, err := p.pollWorkerOutbox(watchCtx, cursor)
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
				// Retention safety: if retention deleted past the cursor, a row
				// this watcher never consumed may be gone. Close before
				// delivering anything past the gap.
				if fellBehind {
					slog.WarnContext(watchCtx, "worker watch fell behind outbox retention; closing for resync",
						slog.Uint64("cursor_seq", cursor))
					return
				}
				for _, r := range batch {
					event, err := storesql.UnmarshalWorkerEvent(r.payload)
					if err != nil {
						// Close to force a relist. Skipping it would cause silent
						// data loss. The fresh watch starts at the current head,
						// naturally bypassing the corrupt row to prevent a boot loop.
						slog.ErrorContext(watchCtx, "worker event unmarshal failed; closing watch for resync",
							slog.Uint64("seq", r.seq), slog.Any("err", err))
						return
					}
					select {
					case ch <- event:
						cursor = r.seq
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

type outboxRow struct {
	seq     uint64
	payload []byte
}

// pollWorkerOutbox reads the rows after cursor, then whether retention has
// deleted past it. The trim mark is read second: a trim that commits between
// the two reads raises the mark, so a row missing from the batch is always
// detected.
func (p *Persistence) pollWorkerOutbox(ctx context.Context, cursor uint64) ([]outboxRow, bool, error) {
	rows, err := p.watchDB.QueryContext(ctx, `
		SELECT seq, payload FROM worker_outbox
		WHERE seq > ? ORDER BY seq LIMIT ?`, cursor, outboxBatch)
	if err != nil {
		return nil, false, err
	}
	var batch []outboxRow
	for rows.Next() {
		var r outboxRow
		if err := rows.Scan(&r.seq, &r.payload); err != nil {
			rows.Close()
			return nil, false, err
		}
		batch = append(batch, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	var mark uint64
	if err := p.watchDB.QueryRowContext(ctx, `SELECT seq FROM worker_outbox_trim WHERE id = 1`).Scan(&mark); err != nil {
		return nil, false, err
	}
	return batch, mark > cursor, nil
}
