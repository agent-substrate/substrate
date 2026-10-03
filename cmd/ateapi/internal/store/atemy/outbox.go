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
// seq is an AUTO_INCREMENT key taken by each write's last statement, so
// writes never wait on one another, as with atepg's xids. A seq is assigned
// before its transaction commits, though, and InnoDB exposes its oldest open
// transaction only through INNODB_TRX, which Vitess does not serve, so a
// poller can see seq N+1 before seq N commits.
// The poller delivers what it sees and remembers each skipped seq as
// pending, delivering it when it commits. A pending seq is dropped only once
// a locking probe shows no write holds it, which means it rolled back. That reorders events only
// across workers: two writes to one worker hold its row lock in turn, so the
// later write's seq is assigned after the earlier one commits.

package atemy

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
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
		payload = nil
		var err error
		worker, err = fn(ctx, tx)
		if err != nil || worker == nil {
			return err
		}
		payload, err = storesql.MarshalWorkerEvent(eventType, worker)
		if err != nil {
			return fmt.Errorf("marshaling worker event: %w", err)
		}
		// Last, so the seq is assigned as close to commit as possible: a seq
		// waiting on its commit is a gap pollers must track.
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO worker_outbox (created_at, payload)
			VALUES (UTC_TIMESTAMP(6), ?)`, payload); err != nil {
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

	// How long a skipped seq stays pending before a probe asks whether a write
	// still holds it. It covers the moment inside a write's INSERT between
	// taking the seq and locking the new row, when neither shows.
	outboxGapGrace = 2 * time.Second

	// How far back a new watch looks for seqs still committing. A write that
	// took its seq longer ago than this and commits after the watch starts is
	// not delivered to that watch.
	outboxSubscribeWindow = 5 * time.Minute

	// Caps the skipped seqs a watcher tracks; past it the watcher resyncs.
	outboxMaxPending = 4096
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
// trim mark to its greatest seq in the same transaction. The replica that
// locks the trim row runs the pass; the others skip it, as atepg's elected
// retention does.
func (p *Persistence) trimWorkerOutboxOlderThan(ctx context.Context, age time.Duration) error {
	for {
		var deleted int
		err := inTx(ctx, p.watchDB, func(tx *sql.Tx) error {
			deleted = 0
			var mark uint64
			err := tx.QueryRowContext(ctx, `SELECT seq FROM worker_outbox_trim WHERE id = 1 FOR UPDATE SKIP LOCKED`).Scan(&mark)
			if errors.Is(err, sql.ErrNoRows) {
				return nil // another replica is maintaining; next tick retries
			}
			if err != nil {
				return fmt.Errorf("electing outbox retention: %w", err)
			}
			rows, err := tx.QueryContext(ctx, `
				SELECT seq, created_at < UTC_TIMESTAMP(6) - INTERVAL ? MICROSECOND
				FROM worker_outbox ORDER BY seq LIMIT ?`, age.Microseconds(), outboxTrimBatch)
			if err != nil {
				return fmt.Errorf("reading expired outbox rows: %w", err)
			}
			var expiredSeqs []any
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
				expiredSeqs = append(expiredSeqs, seq)
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
			// Only the rows read, so a write still committing below last is
			// never waited on. A watcher waiting on such a row closes once the
			// trim mark passes it.
			if _, err := tx.ExecContext(ctx, `DELETE FROM worker_outbox WHERE seq IN (?`+strings.Repeat(", ?", len(expiredSeqs)-1)+`)`, expiredSeqs...); err != nil {
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

// outboxMarks are the values a poll checks against its cursor, read after
// the poll's rows.
type outboxMarks struct {
	// trim is the greatest seq retention has deleted.
	trim uint64
	// head is the greatest seq written: the larger of the greatest stored
	// seq and trim.
	head uint64
	// server identifies the MySQL server that answered (@@server_uuid). A
	// failover to another server can lose writes it never received.
	server string
}

func (p *Persistence) readOutboxMarks(ctx context.Context) (outboxMarks, error) {
	var m outboxMarks
	err := p.watchDB.QueryRowContext(ctx, `
		SELECT t.seq, GREATEST(t.seq, COALESCE((SELECT MAX(seq) FROM worker_outbox), 0)), @@server_uuid
		FROM worker_outbox_trim AS t WHERE t.id = 1`).Scan(&m.trim, &m.head, &m.server)
	return m, err
}

// outboxCursor is a watcher's position: every seq at or below seq was
// delivered, written before the watch, or is pending.
type outboxCursor struct {
	seq uint64
	// pending holds skipped seqs below seq, each with when it was skipped.
	pending map[uint64]time.Time
	server  string
}

// skip records the seqs between the cursor and next as pending.
func (c *outboxCursor) skip(next uint64, now time.Time) {
	c.wait(c.seq, next, now)
}

// wait records the seqs strictly between lo and hi as pending, stopping one
// past outboxMaxPending so closeReason reports the overflow.
func (c *outboxCursor) wait(lo, hi uint64, now time.Time) {
	for s := lo + 1; s < hi && len(c.pending) <= outboxMaxPending; s++ {
		c.pending[s] = now
	}
}

// Reasons a watch closes for a resync, logged as is.
const (
	resyncServerChanged  = "database server changed under the outbox; closing watch for resync"
	resyncLostWrites     = "worker outbox moved behind the watch cursor; closing watch for resync"
	resyncFellBehind     = "worker watch fell behind outbox retention; closing for resync"
	resyncTooManyPending = "worker watch is waiting on too many uncommitted outbox rows; closing for resync"
	resyncPendingTrimmed = "outbox retention deleted a row the worker watch was waiting for; closing for resync"
)

// closeReason reports why marks force a resync, or "" if they do not.
func (c *outboxCursor) closeReason(m outboxMarks) string {
	switch {
	case m.server != c.server:
		return resyncServerChanged
	case m.head < c.seq:
		return resyncLostWrites
	case m.trim > c.seq:
		return resyncFellBehind
	case len(c.pending) > outboxMaxPending:
		return resyncTooManyPending
	}
	for s := range c.pending {
		if s <= m.trim {
			return resyncPendingTrimmed
		}
	}
	return ""
}

// rolledBackCandidates returns the pending seqs skipped more than outboxGapGrace ago,
// the ones a probe may find rolled back.
func (c *outboxCursor) rolledBackCandidates(now time.Time) []any {
	var seqs []any
	for s, at := range c.pending {
		if now.Sub(at) > outboxGapGrace {
			seqs = append(seqs, s)
		}
	}
	return seqs
}

// WatchWorkers subscribes by polling the worker_outbox table with a seq
// cursor. The cursor starts at the greatest seq written, and the seqs below it
// still uncommitted start pending, so the watch sees every write that commits
// after it subscribes.
//
// This process's own writes are published at commit, ahead of the poll, and
// again on the poll, so consumers must reconcile versions and tolerate
// duplicates.
//
// The watch closes to force the consumer to resync from the primary tables
// when it may have missed an event: it fell behind retention, the server
// changed or lost writes, or it waits on too many uncommitted seqs. Unlike
// atepg's UNLOGGED outbox, worker_outbox is durable, so a restart of the same
// server loses no events.
func (p *Persistence) WatchWorkers(ctx context.Context) (*store.WorkerWatch, error) {
	watchCtx, cancel := context.WithCancel(ctx)

	cursor, err := p.subscribeWorkerOutbox(watchCtx)
	if err != nil {
		cancel()
		return nil, err
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
				batch, full, resync, err := p.pollWorkerOutbox(watchCtx, cursor)
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
				if resync {
					return // pollWorkerOutbox logged why
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
					case <-watchCtx.Done():
						return
					}
				}
				if !full {
					break // caught up; wait for the next tick
				}
			}
		}
	}()
	return store.NewWorkerWatch(ch, cancel), nil
}

// subscribeWorkerOutbox places a new watch at the greatest seq written. It
// walks back from there through the rows written in the last
// outboxSubscribeWindow.
// The seqs missing among them, and between the oldest of them and the row
// before it, may belong to writes still committing, so they start pending.
// Rows present were written before the watch and are not delivered.
func (p *Persistence) subscribeWorkerOutbox(ctx context.Context) (*outboxCursor, error) {
	marks, err := p.readOutboxMarks(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading worker outbox cursor: %w", err)
	}
	cursor := &outboxCursor{seq: marks.head, pending: map[uint64]time.Time{}, server: marks.server}
	now := time.Now()
	// above is the lowest recent row seen so far; the walk ends at the first
	// row older than the window, or at the trim mark.
	above, recent := marks.head+1, true
	for recent {
		rows, err := p.watchDB.QueryContext(ctx, `
			SELECT seq, created_at >= UTC_TIMESTAMP(6) - INTERVAL ? MICROSECOND
			FROM worker_outbox WHERE seq > ? AND seq < ?
			ORDER BY seq DESC LIMIT ?`, outboxSubscribeWindow.Microseconds(), marks.trim, above, outboxBatch)
		if err != nil {
			return nil, fmt.Errorf("reading recent worker outbox rows: %w", err)
		}
		n := 0
		for rows.Next() && recent {
			var seq uint64
			if err := rows.Scan(&seq, &recent); err != nil {
				rows.Close()
				return nil, fmt.Errorf("reading recent worker outbox rows: %w", err)
			}
			if above <= marks.head {
				cursor.wait(seq, above, now)
			}
			if recent {
				above = seq
			}
			n++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("reading recent worker outbox rows: %w", err)
		}
		if n < outboxBatch {
			break
		}
	}
	if recent && above <= marks.head {
		// Every row down to the trim mark is recent.
		cursor.wait(marks.trim, above, now)
	}
	if reason := cursor.closeReason(marks); reason != "" {
		return nil, fmt.Errorf("subscribing to the worker outbox: %s", reason)
	}
	return cursor, nil
}

type outboxRow struct {
	seq     uint64
	payload []byte
}

// pollWorkerOutbox returns the rows to deliver, in seq order: pending seqs
// that have committed, then up to outboxBatch rows past the cursor, and
// advances cursor past them. full reports that more rows may follow; resync
// reports, after logging why, that the watch must close instead.
//
// The marks are read after the rows. A trim that commits between the reads
// raises the trim mark, so a row missing from the batch is always detected.
// Pending rows are read after new rows, so any write to a worker that the
// new rows carry finds that worker's earlier pending write committed too.
func (p *Persistence) pollWorkerOutbox(ctx context.Context, cursor *outboxCursor) (batch []outboxRow, full, resync bool, err error) {
	fresh, err := p.queryOutboxRows(ctx, `
		SELECT seq, payload FROM worker_outbox
		WHERE seq > ? ORDER BY seq LIMIT ?`, cursor.seq, outboxBatch)
	if err != nil {
		return nil, false, false, err
	}
	var late []outboxRow
	if len(cursor.pending) > 0 {
		seqs := make([]any, 0, len(cursor.pending))
		for s := range cursor.pending {
			seqs = append(seqs, s)
		}
		late, err = p.queryOutboxRows(ctx, `
			SELECT seq, payload FROM worker_outbox
			WHERE seq IN (?`+strings.Repeat(", ?", len(seqs)-1)+`) ORDER BY seq`, seqs...)
		if err != nil {
			return nil, false, false, err
		}
	}
	marks, err := p.readOutboxMarks(ctx)
	if err != nil {
		return nil, false, false, err
	}

	now := time.Now()
	for _, r := range late {
		delete(cursor.pending, r.seq)
	}
	for _, r := range fresh {
		cursor.skip(r.seq, now)
		cursor.seq = r.seq
	}
	if reason := cursor.closeReason(marks); reason != "" {
		slog.WarnContext(ctx, reason,
			slog.Uint64("cursor_seq", cursor.seq),
			slog.Uint64("trim_seq", marks.trim),
			slog.Uint64("head_seq", marks.head),
			slog.String("was", cursor.server),
			slog.String("now", marks.server))
		return nil, false, true, nil
	}
	if err := p.dropRolledBack(ctx, cursor, now); err != nil {
		return nil, false, false, err
	}
	return slices.Concat(late, fresh), len(fresh) == outboxBatch, false, nil
}

// dropRolledBack stops waiting on pending seqs whose writes rolled back. A
// write that took a seq holds an implicit lock on its row until it commits or
// rolls back, so a NOWAIT locking read fails while any candidate is still
// committing. When it succeeds, a candidate it does not return has no row and
// no writer, and a candidate it does return committed after this poll read
// pending rows and is delivered by the next poll.
func (p *Persistence) dropRolledBack(ctx context.Context, cursor *outboxCursor, now time.Time) error {
	candidates := cursor.rolledBackCandidates(now)
	if len(candidates) == 0 {
		return nil
	}
	rows, err := p.watchDB.QueryContext(ctx, `
		SELECT seq FROM worker_outbox
		WHERE seq IN (?`+strings.Repeat(", ?", len(candidates)-1)+`)
		FOR SHARE NOWAIT`, candidates...)
	if mysqlErrNumber(err) == errLockNowait {
		return nil // a write is still committing; probe again next poll
	}
	if err != nil {
		return fmt.Errorf("probing pending worker outbox rows: %w", err)
	}
	defer rows.Close()
	committed := map[uint64]bool{}
	for rows.Next() {
		var seq uint64
		if err := rows.Scan(&seq); err != nil {
			return fmt.Errorf("probing pending worker outbox rows: %w", err)
		}
		committed[seq] = true
	}
	if err := rows.Err(); err != nil {
		if mysqlErrNumber(err) == errLockNowait {
			return nil
		}
		return fmt.Errorf("probing pending worker outbox rows: %w", err)
	}
	for _, c := range candidates {
		if s := c.(uint64); !committed[s] {
			delete(cursor.pending, s)
		}
	}
	return nil
}

func (p *Persistence) queryOutboxRows(ctx context.Context, query string, args ...any) ([]outboxRow, error) {
	rows, err := p.watchDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []outboxRow
	for rows.Next() {
		var r outboxRow
		if err := rows.Scan(&r.seq, &r.payload); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
