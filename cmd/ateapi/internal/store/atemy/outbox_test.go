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

// Tests for the worker outbox (outbox.go): transactional append, delivery of
// AUTO_INCREMENT seqs that commit out of order, retention and the trim mark,
// and the watch's close-for-resync signals.

package atemy

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storesql"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func workerPayload(t *testing.T, eventType store.WorkerEventType, worker *ateapipb.Worker) []byte {
	t.Helper()
	payload, err := storesql.MarshalWorkerEvent(eventType, worker)
	if err != nil {
		t.Fatalf("marshaling event for %q: %v", worker.GetMetadata().GetName(), err)
	}
	return payload
}

// insertOutboxRow appends payload on q, stamped age before now, the way
// writeAndAppendEvent does, and returns its seq. Inside a transaction, the
// seq stays uncommitted until the transaction ends.
func insertOutboxRow(t *testing.T, q querier, payload []byte, age time.Duration) uint64 {
	t.Helper()
	res, err := q.ExecContext(t.Context(), `
		INSERT INTO worker_outbox (created_at, payload)
		VALUES (UTC_TIMESTAMP(6) - INTERVAL ? MICROSECOND, ?)`, age.Microseconds(), payload)
	if err != nil {
		t.Fatalf("appending outbox row: %v", err)
	}
	seq, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("reading appended seq: %v", err)
	}
	return uint64(seq)
}

// beginOutboxWrite opens a transaction that appends an event for worker and
// stays open, as a worker write between its append and its commit does.
func beginOutboxWrite(t *testing.T, p *Persistence, eventType store.WorkerEventType, worker *ateapipb.Worker) (*sql.Tx, uint64) {
	t.Helper()
	tx, err := p.db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	t.Cleanup(func() { tx.Rollback() }) //nolint:errcheck // no-op once committed
	return tx, insertOutboxRow(t, tx, workerPayload(t, eventType, worker), 0)
}

// appendRawEvents commits n outbox rows carrying payload in one statement,
// stamped age before now, and returns the first and last seq.
func appendRawEvents(t *testing.T, p *Persistence, n int, payload []byte, age time.Duration) (first, last uint64) {
	t.Helper()
	ctx := t.Context()
	conn, err := p.db.Conn(ctx)
	if err != nil {
		t.Fatalf("acquiring connection: %v", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `SET SESSION cte_max_recursion_depth = ?`, n+1); err != nil {
		t.Fatalf("raising recursion depth: %v", err)
	}
	if _, err := conn.ExecContext(ctx, `
		INSERT INTO worker_outbox (created_at, payload)
		WITH RECURSIVE r (i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM r WHERE i < ?)
		SELECT UTC_TIMESTAMP(6) - INTERVAL ? MICROSECOND, ? FROM r`,
		n, age.Microseconds(), payload); err != nil {
		t.Fatalf("appending outbox rows: %v", err)
	}
	if err := conn.QueryRowContext(ctx, `SELECT LAST_INSERT_ID()`).Scan(&first); err != nil {
		t.Fatalf("reading first seq: %v", err)
	}
	return first, first + uint64(n) - 1
}

// outboxHead returns the greatest stored seq.
func outboxHead(t *testing.T, p *Persistence) uint64 {
	t.Helper()
	var seq uint64
	if err := p.db.QueryRowContext(t.Context(), `SELECT COALESCE(MAX(seq), 0) FROM worker_outbox`).Scan(&seq); err != nil {
		t.Fatalf("reading outbox head: %v", err)
	}
	return seq
}

func trimMark(t *testing.T, p *Persistence) uint64 {
	t.Helper()
	var seq uint64
	if err := p.db.QueryRowContext(t.Context(), `SELECT seq FROM worker_outbox_trim WHERE id = 1`).Scan(&seq); err != nil {
		t.Fatalf("reading trim mark: %v", err)
	}
	return seq
}

func outboxSeqs(t *testing.T, p *Persistence) []uint64 {
	t.Helper()
	rows, err := p.db.QueryContext(t.Context(), `SELECT seq FROM worker_outbox ORDER BY seq`)
	if err != nil {
		t.Fatalf("reading outbox: %v", err)
	}
	defer rows.Close()
	var seqs []uint64
	for rows.Next() {
		var seq uint64
		if err := rows.Scan(&seq); err != nil {
			t.Fatalf("scanning outbox row: %v", err)
		}
		seqs = append(seqs, seq)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("reading outbox: %v", err)
	}
	return seqs
}

func seqRange(first, last uint64) []uint64 {
	var seqs []uint64
	for seq := first; seq <= last; seq++ {
		seqs = append(seqs, seq)
	}
	return seqs
}

// receive returns the next event, failing if the watch closes or stays quiet.
func receive(t *testing.T, watch *store.WorkerWatch) store.WorkerEvent {
	t.Helper()
	select {
	case event, ok := <-watch.Events:
		if !ok {
			t.Fatal("watch closed, want an event")
		}
		return event
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for an event")
	}
	return store.WorkerEvent{}
}

// requireClosedNext fails unless the watch's next signal is its close.
func requireClosedNext(t *testing.T, watch *store.WorkerWatch, why string) {
	t.Helper()
	select {
	case event, ok := <-watch.Events:
		if ok {
			t.Fatalf("delivered event for %q, want the watch to close: %s", event.Worker.GetMetadata().GetName(), why)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("watch stayed open: %s", why)
	}
}

func watchWorkers(t *testing.T, p *Persistence) *store.WorkerWatch {
	t.Helper()
	watch, err := p.WatchWorkers(t.Context())
	if err != nil {
		t.Fatalf("WatchWorkers failed: %v", err)
	}
	t.Cleanup(watch.Close)
	return watch
}

// eventNames returns the worker names of the next n events.
func eventNames(t *testing.T, watch *store.WorkerWatch, n int) []string {
	t.Helper()
	var names []string
	for range n {
		names = append(names, receive(t, watch).Worker.GetMetadata().GetName())
	}
	return names
}

// TestWorkerEvent_OnlyAfterCommit proves a worker write's outbox row shares
// the write's transaction: a rolled-back write delivers nothing and the seq it
// took, a gap the watcher waits on, does not close the watch.
func TestWorkerEvent_OnlyAfterCommit(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	// A replica's watcher sees only polled rows, so a rolled-back row that
	// leaked would arrive from the outbox.
	watch := watchWorkers(t, newReplica(t, s))

	const workerName = "6e4d2f81-b3a9-4c05-8e72-1f9d4a0c7b63"
	worker := newTestWorker(workerName)
	protoBytes, err := proto.Marshal(worker)
	if err != nil {
		t.Fatalf("marshaling worker: %v", err)
	}
	tx, rolledBack := beginOutboxWrite(t, s, store.WorkerEventCreated, newTestWorker("rolled-back"))
	if _, err := tx.ExecContext(ctx, `INSERT INTO workers (name, uid, version, proto) VALUES (?, ?, ?, ?)`,
		workerName, "rolled-back-uid", int64(1), protoBytes); err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	created, err := s.CreateWorker(ctx, worker)
	if err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	if created.GetMetadata().GetName() == "" || outboxHead(t, s) <= rolledBack {
		t.Fatalf("committed write did not take a seq past the rolled-back %d", rolledBack)
	}
	event := receive(t, watch)
	if event.Type != store.WorkerEventCreated {
		t.Errorf("event type = %v, want WorkerEventCreated", event.Type)
	}
	if diff := cmp.Diff(created, event.Worker, protocmp.Transform()); diff != "" {
		t.Errorf("event worker mismatch (-want +got):\n%s", diff)
	}

	// The rolled-back seq stays pending without closing the watch.
	if _, err := s.CreateWorker(ctx, newTestWorker("after-rollback")); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	if got := receive(t, watch).Worker.GetMetadata().GetName(); got != "after-rollback" {
		t.Errorf("delivered %q, want after-rollback", got)
	}
}

// Worker writes take their seqs from AUTO_INCREMENT rather than a shared row,
// so a write to one worker commits while another worker's write is open
// after its append.
func TestWorkerWrites_DoNotSerialize(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	for _, name := range []string{"worker-a", "worker-b"} {
		if _, err := s.CreateWorker(ctx, newTestWorker(name)); err != nil {
			t.Fatalf("CreateWorker(%s) failed: %v", name, err)
		}
	}

	holder, heldSeq := beginOutboxWrite(t, s, store.WorkerEventUpdated, newTestWorker("worker-a"))
	if _, err := holder.ExecContext(ctx, `UPDATE workers SET version = version + 1 WHERE name = 'worker-a'`); err != nil {
		t.Fatalf("updating worker-a: %v", err)
	}

	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	stored, err := s.GetWorker(ctx, "worker-b")
	if err != nil {
		t.Fatalf("GetWorker failed: %v", err)
	}
	if _, err := s.UpdateWorker(writeCtx, "worker-b", store.PreconditionFrom(stored), func(w *ateapipb.Worker) error {
		w.Ips = []string{"10.0.0.2"}
		return nil
	}); err != nil {
		t.Fatalf("UpdateWorker(worker-b) while worker-a's write is open = %v, want it to commit", err)
	}
	if got := outboxHead(t, s); got <= heldSeq {
		t.Errorf("worker-b committed seq %d, want one past worker-a's uncommitted %d", got, heldSeq)
	}
	if err := holder.Commit(); err != nil {
		t.Fatalf("committing worker-a's write: %v", err)
	}
}

// TestWatchWorkers_OutOfOrderCommitNotSkipped commits seq N+1 while seq N is
// still open. The watcher delivers N+1, remembers N as pending, and delivers
// N late once it commits, without losing it or closing the watch.
func TestWatchWorkers_OutOfOrderCommitNotSkipped(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	watch := watchWorkers(t, newReplica(t, s))

	txA, seqA := beginOutboxWrite(t, s, store.WorkerEventCreated, newTestWorker("first-seq-late-commit"))
	if _, err := s.CreateWorker(ctx, newTestWorker("second-seq-early-commit")); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	if got := outboxHead(t, s); got <= seqA {
		t.Fatalf("early commit took seq %d, want one past the open %d", got, seqA)
	}
	if got := receive(t, watch).Worker.GetMetadata().GetName(); got != "second-seq-early-commit" {
		t.Fatalf("delivered %q, want second-seq-early-commit", got)
	}

	if err := txA.Commit(); err != nil {
		t.Fatalf("committing the first writer: %v", err)
	}
	if got := receive(t, watch).Worker.GetMetadata().GetName(); got != "first-seq-late-commit" {
		t.Fatalf("delivered %q, want the late first-seq-late-commit", got)
	}
	if _, err := s.CreateWorker(ctx, newTestWorker("after-late-commit")); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	if got := receive(t, watch).Worker.GetMetadata().GetName(); got != "after-late-commit" {
		t.Errorf("delivered %q, want after-late-commit", got)
	}
}

// Writes to one worker take its row lock in turn, so the later write's seq is
// assigned after the earlier one commits. Its events arrive in write order
// both when another worker's seq is pending and when its own earlier write
// is the pending one, delivered late in the same poll.
func TestWatchWorkers_KeepsPerWorkerOrder(t *testing.T) {
	updateIPs := func(t *testing.T, s *Persistence, name, ip string) {
		t.Helper()
		stored, err := s.GetWorker(t.Context(), name)
		if err != nil {
			t.Fatalf("GetWorker failed: %v", err)
		}
		if _, err := s.UpdateWorker(t.Context(), name, store.PreconditionFrom(stored), func(w *ateapipb.Worker) error {
			w.Ips = []string{ip}
			return nil
		}); err != nil {
			t.Errorf("UpdateWorker(%s, %s) failed: %v", name, ip, err)
		}
	}
	ipsOf := func(t *testing.T, watch *store.WorkerWatch, name string, n int) []string {
		t.Helper()
		var ips []string
		for len(ips) < n {
			event := receive(t, watch)
			if event.Worker.GetMetadata().GetName() == name {
				ips = append(ips, event.Worker.GetIps()...)
			}
		}
		return ips
	}

	t.Run("another worker's seq pending", func(t *testing.T) {
		s := setupMySQLPersistence(t)
		if _, err := s.CreateWorker(t.Context(), newTestWorker("ordered")); err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}
		watch := watchWorkers(t, newReplica(t, s))
		txOther, _ := beginOutboxWrite(t, s, store.WorkerEventCreated, newTestWorker("other"))

		updateIPs(t, s, "ordered", "10.0.0.1")
		updateIPs(t, s, "ordered", "10.0.0.2")
		if got, want := ipsOf(t, watch, "ordered", 2), []string{"10.0.0.1", "10.0.0.2"}; !slices.Equal(got, want) {
			t.Errorf("updates delivered in order %q, want %q", got, want)
		}
		if err := txOther.Commit(); err != nil {
			t.Fatalf("committing the other worker's write: %v", err)
		}
		if got := receive(t, watch).Worker.GetMetadata().GetName(); got != "other" {
			t.Errorf("delivered %q, want the late other", got)
		}
	})

	t.Run("its own earlier write pending", func(t *testing.T) {
		s := setupMySQLPersistence(t)
		ctx := t.Context()
		created, err := s.CreateWorker(ctx, newTestWorker("ordered"))
		if err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}
		watch := watchWorkers(t, newReplica(t, s))

		// The first write holds the worker's row lock past its append.
		first := proto.CloneOf(created)
		first.Ips = []string{"10.0.0.1"}
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			t.Fatalf("BeginTx failed: %v", err)
		}
		defer tx.Rollback() //nolint:errcheck // no-op once committed
		if _, err := tx.ExecContext(ctx, `UPDATE workers SET version = version WHERE name = 'ordered'`); err != nil {
			t.Fatalf("locking the worker: %v", err)
		}
		insertOutboxRow(t, tx, workerPayload(t, store.WorkerEventUpdated, first), 0)

		// Another worker commits a later seq, so the first write's seq is
		// pending.
		if _, err := s.CreateWorker(ctx, newTestWorker("other")); err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}
		if got := receive(t, watch).Worker.GetMetadata().GetName(); got != "other" {
			t.Fatalf("delivered %q, want other", got)
		}

		second := make(chan error, 1)
		go func() {
			stored, err := s.GetWorker(ctx, "ordered")
			if err == nil {
				_, err = s.UpdateWorker(ctx, "ordered", store.PreconditionFrom(stored), func(w *ateapipb.Worker) error {
					w.Ips = []string{"10.0.0.2"}
					return nil
				})
			}
			second <- err
		}()
		waitForLockWait(t, second)
		if err := tx.Commit(); err != nil {
			t.Fatalf("committing the first write: %v", err)
		}
		if err := <-second; err != nil {
			t.Fatalf("second UpdateWorker failed: %v", err)
		}
		if got, want := ipsOf(t, watch, "ordered", 2), []string{"10.0.0.1", "10.0.0.2"}; !slices.Equal(got, want) {
			t.Errorf("updates delivered in order %q, want %q", got, want)
		}
	})
}

// A write in flight when a watch subscribes has a seq below a write that
// committed before the watch, so the watch starts with it pending and
// delivers it once it commits. Rows already committed are not replayed.
func TestWatchWorkers_DeliversAWriteInFlightAtSubscribe(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	// An earlier row, outside the gap window, as on a quiet system.
	insertOutboxRow(t, s.db, workerPayload(t, store.WorkerEventCreated, newTestWorker("long-ago")), 2*outboxGapWait)

	tx, inFlight := beginOutboxWrite(t, s, store.WorkerEventCreated, newTestWorker("in-flight"))
	if _, err := s.CreateWorker(ctx, newTestWorker("committed-before-subscribe")); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	if got := outboxHead(t, s); got <= inFlight {
		t.Fatalf("committed write took seq %d, want one past the in-flight %d", got, inFlight)
	}
	watch := watchWorkers(t, newReplica(t, s))

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	if _, err := s.CreateWorker(ctx, newTestWorker("after-subscribe")); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	got := eventNames(t, watch, 2)
	if !slices.Contains(got, "in-flight") || !slices.Contains(got, "after-subscribe") {
		t.Errorf("delivered %q, want in-flight and after-subscribe, and nothing written before the watch", got)
	}
}

// TestWorkerEvents_OneRowPerWrite pins the invariant the seq cursor rests on:
// each committed worker write appends exactly one outbox row, and a write
// that announces nothing appends none.
func TestWorkerEvents_OneRowPerWrite(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()

	if _, err := s.CreateWorker(ctx, newTestWorker("one-row-worker")); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	for i := range 10 {
		stored, err := s.GetWorker(ctx, "one-row-worker")
		if err != nil {
			t.Fatalf("GetWorker failed: %v", err)
		}
		if _, err := s.UpdateWorker(ctx, "one-row-worker", store.PreconditionFrom(stored), func(*ateapipb.Worker) error {
			return nil
		}); err != nil {
			t.Fatalf("UpdateWorker %d failed: %v", i, err)
		}
	}
	if released, err := s.ReleaseActorFromWorker(ctx, "one-row-worker", "not-hosted"); err != nil || released != nil {
		t.Fatalf("ReleaseActorFromWorker of an unhosted actor = %v, %v; want nil, nil", released, err)
	}
	if _, err := s.DeleteWorker(ctx, "one-row-worker", store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteWorker failed: %v", err)
	}

	const writes = 12
	seqs := outboxSeqs(t, s)
	if len(seqs) != writes {
		t.Fatalf("%d outbox rows, want %d", len(seqs), writes)
	}
	if diff := cmp.Diff(seqRange(seqs[0], seqs[0]+writes-1), seqs); diff != "" {
		t.Errorf("outbox seqs are not contiguous (-want +got):\n%s", diff)
	}
}

// Retention deletes rows oldest seq first and stops at the first row still
// within retention, so the trim mark always bounds a contiguous deleted
// prefix, even if a later row carries an older timestamp.
func TestTrimWorkerOutbox_TrimsOnlyTheExpiredPrefix(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	payload := []byte("payload")
	insertOutboxRow(t, s.db, payload, time.Hour)
	secondOld := insertOutboxRow(t, s.db, payload, time.Hour)
	fresh := insertOutboxRow(t, s.db, payload, 0)
	lateOld := insertOutboxRow(t, s.db, payload, time.Hour)

	for range 2 {
		if err := s.trimWorkerOutboxOlderThan(ctx, 30*time.Minute); err != nil {
			t.Fatalf("trimWorkerOutboxOlderThan failed: %v", err)
		}
		if diff := cmp.Diff([]uint64{fresh, lateOld}, outboxSeqs(t, s)); diff != "" {
			t.Errorf("outbox seqs after trim (-want +got):\n%s", diff)
		}
		if got := trimMark(t, s); got != secondOld {
			t.Errorf("trim mark = %d, want %d", got, secondOld)
		}
	}

	// The production retention keeps rows younger than outboxRetentionAge.
	if err := s.trimWorkerOutbox(ctx); err != nil {
		t.Fatalf("trimWorkerOutbox failed: %v", err)
	}
	if diff := cmp.Diff([]uint64{fresh, lateOld}, outboxSeqs(t, s)); diff != "" {
		t.Errorf("outbox seqs after production retention (-want +got):\n%s", diff)
	}
}

// Retention of rows a watcher has already consumed leaves the trim mark at
// its cursor, so the watcher keeps delivering.
func TestWatchWorkers_SurvivesTrimOfConsumedRows(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	watch := watchWorkers(t, newReplica(t, s))

	if _, err := s.CreateWorker(ctx, newTestWorker("consumed")); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	if got := receive(t, watch).Worker.GetMetadata().GetName(); got != "consumed" {
		t.Fatalf("delivered %q, want consumed", got)
	}
	consumed := outboxHead(t, s)
	if err := s.trimWorkerOutboxOlderThan(ctx, 0); err != nil {
		t.Fatalf("trimWorkerOutboxOlderThan failed: %v", err)
	}
	if got := trimMark(t, s); got != consumed {
		t.Fatalf("trim mark = %d, want %d", got, consumed)
	}

	if _, err := s.CreateWorker(ctx, newTestWorker("after-trim")); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	if got := receive(t, watch).Worker.GetMetadata().GetName(); got != "after-trim" {
		t.Errorf("delivered %q, want after-trim", got)
	}
}

// A database that loses committed writes, as a restore from an older backup
// does, leaves the outbox behind a watcher's cursor. New writes would reuse
// seqs the cursor has passed, so the watcher must close rather than skip them.
func TestWatchWorkers_ClosesWhenTheOutboxMovesBehindTheCursor(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	watch := watchWorkers(t, newReplica(t, s))

	if _, err := s.CreateWorker(ctx, newTestWorker("lost-worker")); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	if got := receive(t, watch).Worker.GetMetadata().GetName(); got != "lost-worker" {
		t.Fatalf("delivered %q, want lost-worker", got)
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM worker_outbox`); err != nil {
		t.Fatalf("dropping the outbox rows: %v", err)
	}
	requireClosedNext(t, watch, "the outbox moved behind the cursor")
}

// TestWatchWorkers_ClosesOnCorruptPayload pins close-over-skip: a payload
// that fails to decode must close the watch rather than advance past it.
func TestWatchWorkers_ClosesOnCorruptPayload(t *testing.T) {
	s := setupMySQLPersistence(t)
	watch := watchWorkers(t, s)
	insertOutboxRow(t, s.db, []byte{0xff, 0xde, 0xad}, 0)
	requireClosedNext(t, watch, "a corrupt payload was skipped")
}

func TestOutboxCursor_Skip(t *testing.T) {
	now := time.Now()
	c := &outboxCursor{seq: 5, pending: map[uint64]time.Time{}}
	c.skip(9, now)
	if got := slices.Sorted(maps.Keys(c.pending)); !slices.Equal(got, []uint64{6, 7, 8}) {
		t.Errorf("pending after skipping to 9 = %v, want [6 7 8]", got)
	}
	c.seq = 9
	c.skip(10, now)
	if len(c.pending) != 3 {
		t.Errorf("skipping to the next seq added pending seqs: %v", c.pending)
	}

	// A huge gap stops one past the cap, enough for closeReason to see it.
	c = &outboxCursor{pending: map[uint64]time.Time{}}
	c.skip(outboxMaxPending*10, now)
	if got := len(c.pending); got != outboxMaxPending+1 {
		t.Errorf("pending after a huge gap = %d seqs, want %d", got, outboxMaxPending+1)
	}
}

func TestOutboxCursor_CloseReason(t *testing.T) {
	healthy := outboxMarks{trim: 5, head: 20, server: "a"}
	tooMany := map[uint64]time.Time{}
	for s := range uint64(outboxMaxPending + 1) {
		tooMany[100+s] = time.Time{}
	}
	for _, tc := range []struct {
		name    string
		pending map[uint64]time.Time
		marks   outboxMarks
		closes  bool
	}{
		{"healthy", map[uint64]time.Time{8: {}}, healthy, false},
		{"head at the cursor", nil, outboxMarks{trim: 5, head: 10, server: "a"}, false},
		{"trim at the cursor", nil, outboxMarks{trim: 10, head: 10, server: "a"}, false},
		{"server changed", nil, outboxMarks{trim: 5, head: 20, server: "b"}, true},
		{"head behind the cursor", nil, outboxMarks{trim: 5, head: 9, server: "a"}, true},
		{"trim past the cursor", nil, outboxMarks{trim: 11, head: 20, server: "a"}, true},
		{"too many pending", tooMany, healthy, true},
		{"pending at the trim mark", map[uint64]time.Time{5: {}}, healthy, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pending := tc.pending
			if pending == nil {
				pending = map[uint64]time.Time{}
			}
			c := &outboxCursor{seq: 10, pending: pending, server: "a"}
			if got := c.closeReason(tc.marks); (got != "") != tc.closes {
				t.Errorf("closeReason(%+v) = %q, want closing: %t", tc.marks, got, tc.closes)
			}
		})
	}
}

// A rolled-back write leaves a seq that never commits. The watcher stops
// waiting on it after outboxGapWait.
func TestOutboxCursor_Expire(t *testing.T) {
	now := time.Now()
	c := &outboxCursor{seq: 10, pending: map[uint64]time.Time{
		6: now.Add(-outboxGapWait - time.Second),
		7: now.Add(-outboxGapWait),
		8: now.Add(-time.Second),
	}}
	c.expire(now)
	if got := slices.Sorted(maps.Keys(c.pending)); !slices.Equal(got, []uint64{7, 8}) {
		t.Errorf("pending after expire = %v, want [7 8]", got)
	}
}

func TestWatchWorkers_StartsAtTheCommittedSeq(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	if _, err := s.CreateWorker(ctx, newTestWorker("before-subscribe")); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	watch := watchWorkers(t, newReplica(t, s))
	if _, err := s.CreateWorker(ctx, newTestWorker("after-subscribe")); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	// Polled rows arrive in seq order, so an earlier row would come first.
	if got := receive(t, watch).Worker.GetMetadata().GetName(); got != "after-subscribe" {
		t.Errorf("first delivered event is for %q, want after-subscribe", got)
	}
}

// Unlike atepg's xmin fence, the seq cursor waits only on writes that hold
// the sequence row, so an unrelated open transaction does not delay delivery.
func TestWatchWorkers_UnrelatedTransactionDoesNotDelayDelivery(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	watch := watchWorkers(t, newReplica(t, s))

	blocker, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer blocker.Rollback() //nolint:errcheck // never committed
	if _, err := blocker.ExecContext(ctx, `INSERT INTO atespaces (name, uid, version, proto) VALUES ('blocker', 'uid', 1, '')`); err != nil {
		t.Fatalf("blocker write failed: %v", err)
	}

	if _, err := s.CreateWorker(ctx, newTestWorker("unfenced")); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	if got := receive(t, watch).Worker.GetMetadata().GetName(); got != "unfenced" {
		t.Errorf("delivered %q, want unfenced", got)
	}
}

func TestTrimWorkerOutbox_DrainsAcrossBatches(t *testing.T) {
	s := setupMySQLPersistence(t)
	_, last := appendRawEvents(t, s, outboxTrimBatch*2+5, []byte("payload"), time.Hour)

	if err := s.trimWorkerOutboxOlderThan(t.Context(), time.Minute); err != nil {
		t.Fatalf("trimWorkerOutboxOlderThan failed: %v", err)
	}
	if seqs := outboxSeqs(t, s); len(seqs) != 0 {
		t.Errorf("%d rows left after a full pass, want 0", len(seqs))
	}
	if got := trimMark(t, s); got != last {
		t.Errorf("trim mark = %d, want %d", got, last)
	}
}

// Two replicas running retention at once serialize on the trim row: both
// succeed and the end state is the same as one pass.
func TestTrimWorkerOutbox_ConcurrentPassesAreHarmless(t *testing.T) {
	s := setupMySQLPersistence(t)
	replica := newReplica(t, s)
	_, last := appendRawEvents(t, s, outboxTrimBatch*2+5, []byte("payload"), time.Hour)

	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i, p := range []*Persistence{s, replica} {
		wg.Go(func() { errs[i] = p.trimWorkerOutboxOlderThan(t.Context(), time.Minute) })
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("concurrent pass %d returned error: %v", i, err)
		}
	}
	if seqs := outboxSeqs(t, s); len(seqs) != 0 {
		t.Errorf("%d rows left after both passes, want 0", len(seqs))
	}
	if got := trimMark(t, s); got != last {
		t.Errorf("trim mark = %d, want %d", got, last)
	}
}

// Retention and worker writes lock disjoint rows (the trim row and old outbox
// rows against the sequence row and new outbox rows), so running them at once
// must neither deadlock nor fail a write.
func TestTrimWorkerOutbox_ConcurrentWritersDoNotDeadlock(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()

	writerCtx, stopWriters := context.WithCancel(ctx)
	defer stopWriters()
	var wg sync.WaitGroup
	writerErrs := make(chan error, 4)
	for g := range 4 {
		wg.Go(func() {
			for i := 0; writerCtx.Err() == nil; i++ {
				if _, err := s.CreateWorker(writerCtx, newTestWorker(fmt.Sprintf("trim-writer-%d-%d", g, i))); err != nil && writerCtx.Err() == nil {
					writerErrs <- fmt.Errorf("writer %d iteration %d: %w", g, i, err)
					return
				}
			}
		})
	}
	for i := range 20 {
		if err := s.trimWorkerOutboxOlderThan(ctx, 0); err != nil {
			t.Errorf("trim pass %d failed under concurrent writers: %v", i, err)
		}
	}
	stopWriters()
	wg.Wait()
	close(writerErrs)
	for err := range writerErrs {
		t.Errorf("concurrent writer failed: %v", err)
	}
	mark := trimMark(t, s)
	if seqs := outboxSeqs(t, s); len(seqs) > 0 && seqs[0] <= mark {
		t.Errorf("row %d survived at or below the trim mark %d", seqs[0], mark)
	}
	if mark == 0 {
		t.Error("no trim pass deleted anything")
	}
}

// TestWatchWorkers_ClosesWhenTrimmedPastCursor lets a watcher fall behind for
// real: its consumer stops reading, the poller blocks with a full channel
// partway through its first batch, and retention deletes every row, including
// the ones after that batch. Once the consumer resumes, the watcher delivers
// the batch it already read and then closes, because rows it never read were
// deleted.
func TestWatchWorkers_ClosesWhenTrimmedPastCursor(t *testing.T) {
	s := setupMySQLPersistence(t)
	watch := watchWorkers(t, newReplica(t, s))

	appendRawEvents(t, s, outboxBatch+100, workerPayload(t, store.WorkerEventUpdated, newTestWorker("lagging-worker")), 0)
	deadline := time.Now().Add(10 * time.Second)
	for len(watch.Events) < cap(watch.Events) {
		if time.Now().After(deadline) {
			t.Fatalf("watch buffered %d of %d events, want it full", len(watch.Events), cap(watch.Events))
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := s.trimWorkerOutboxOlderThan(t.Context(), 0); err != nil {
		t.Fatalf("trimWorkerOutboxOlderThan failed: %v", err)
	}
	if seqs := outboxSeqs(t, s); len(seqs) != 0 {
		t.Fatalf("%d rows survived the trim, want 0", len(seqs))
	}

	delivered := 0
	for {
		select {
		case _, ok := <-watch.Events:
			if !ok {
				if delivered != outboxBatch {
					t.Errorf("delivered %d events before closing, want the %d of the batch read before the trim", delivered, outboxBatch)
				}
				return
			}
			delivered++
		case <-time.After(5 * time.Second):
			t.Fatalf("watch stayed open after %d events; rows it never read were trimmed", delivered)
		}
	}
}

// TestWatchWorkers_ClosesAfterPersistentPollFailure pins the loss signal for
// polling outages: a persistent failure must close the channel within
// pollFailureCloseAfter so consumers stop serving a frozen fleet view.
func TestWatchWorkers_ClosesAfterPersistentPollFailure(t *testing.T) {
	requireDB(t)
	ctx := t.Context()
	// Connect gives the watcher its own pool, so closing it simulates an
	// outage without touching the shared pool.
	p, err := Connect(ctx, ConnectConfig{ReadWriteDSN: containerDSN, OwnerDSN: containerDSN})
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer p.DB().Close()
	defer p.Close()
	p.pollFailureCloseAfter = 300 * time.Millisecond

	watch, err := p.WatchWorkers(ctx)
	if err != nil {
		t.Fatalf("WatchWorkers failed: %v", err)
	}
	defer watch.Close()
	p.watchDB.Close()

	requireClosedNext(t, watch, "polling failed persistently")
}

// TestLocalPublishReachesWatchers pins the local fast path: every worker
// event is on an active watcher's channel by the time the write call
// returns, carrying the committed state. Each read is non-blocking, so only
// the commit-time publish can satisfy it.
func TestLocalPublishReachesWatchers(t *testing.T) {
	p := setupMySQLPersistence(t)
	ctx := t.Context()
	watch := watchWorkers(t, p)

	// nextEvent takes the next buffered event of type want, skipping outbox
	// copies of earlier writes.
	nextEvent := func(what string, want store.WorkerEventType) store.WorkerEvent {
		t.Helper()
		for {
			select {
			case ev, ok := <-watch.Events:
				if !ok {
					t.Fatalf("after %s: watch closed", what)
				}
				if ev.Type == want {
					return ev
				}
			default:
				t.Fatalf("after %s: no %v on the watch channel; the commit-time publish did not reach the watcher", what, want)
				return store.WorkerEvent{}
			}
		}
	}

	created, err := p.CreateWorker(ctx, newTestWorker("local-publish-worker"))
	if err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	ev := nextEvent("create", store.WorkerEventCreated)
	if got, want := ev.Worker.GetMetadata().GetVersion(), created.GetMetadata().GetVersion(); got != want {
		t.Errorf("created event version = %d, want committed version %d", got, want)
	}

	updated, err := p.UpdateWorker(ctx, created.GetMetadata().GetName(), store.PreconditionFrom(created), func(toUpdate *ateapipb.Worker) error {
		toUpdate.Ips = []string{"10.0.0.9"}
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateWorker failed: %v", err)
	}
	ev = nextEvent("update", store.WorkerEventUpdated)
	if got, want := ev.Worker.GetMetadata().GetVersion(), updated.GetMetadata().GetVersion(); got != want {
		t.Errorf("updated event version = %d, want committed version %d", got, want)
	}
	if !slices.Equal(ev.Worker.GetIps(), []string{"10.0.0.9"}) {
		t.Errorf("updated event carries Ips %q, want the committed mutation", ev.Worker.GetIps())
	}
	if ev.Worker == updated {
		t.Error("locally published Worker aliases the caller's returned Worker; it must be a copy")
	}

	if _, err := p.DeleteWorker(ctx, created.GetMetadata().GetName(), store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteWorker failed: %v", err)
	}
	ev = nextEvent("delete", store.WorkerEventDeleted)
	if ev.Worker.GetMetadata().GetName() != created.GetMetadata().GetName() {
		t.Errorf("deleted event names worker %q, want %q", ev.Worker.GetMetadata().GetName(), created.GetMetadata().GetName())
	}
}

// TestLocalPublishSurvivesWatchClose covers the close race the watcher
// registry exists to prevent: a write publishing concurrently with a watch
// shutting down must not send on a closed channel.
func TestLocalPublishSurvivesWatchClose(t *testing.T) {
	p := setupMySQLPersistence(t)
	ctx := t.Context()
	for i := range 20 {
		watch, err := p.WatchWorkers(ctx)
		if err != nil {
			t.Fatalf("WatchWorkers failed: %v", err)
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			watch.Close()
		}()
		w, err := p.CreateWorker(ctx, newTestWorker(fmt.Sprintf("close-race-worker-%d", i)))
		if err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}
		if _, err := p.DeleteWorker(ctx, w.GetMetadata().GetName(), store.DeletePreconditions{}); err != nil {
			t.Fatalf("DeleteWorker failed: %v", err)
		}
		<-done
	}
}

// TestClose_StopsMaintenance pins that Close ends the background maintenance
// goroutine: Close blocks on the loop's done channel, so its return is the
// assertion.
func TestClose_StopsMaintenance(t *testing.T) {
	p, err := NewPersistence(t.Context(), requireDB(t))
	if err != nil {
		t.Fatalf("NewPersistence failed: %v", err)
	}
	closed := make(chan struct{})
	go func() {
		p.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not stop the maintenance loop")
	}
}
