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

// Tests for the worker outbox (outbox.go): transactional append, seq-ordered
// delivery behind the sequence row lock, retention and the trim mark, and the
// watch's close-for-resync signals.

package atemy

import (
	"context"
	"database/sql"
	"fmt"
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

func workerPayload(t *testing.T, eventType store.WorkerEventType, name string) []byte {
	t.Helper()
	payload, err := storesql.MarshalWorkerEvent(eventType, newTestWorker(name))
	if err != nil {
		t.Fatalf("marshaling event for %q: %v", name, err)
	}
	return payload
}

// appendRawEvent appends payload inside tx the way writeAndAppendEvent does,
// so tx holds the sequence row until it ends. It returns the row's seq.
func appendRawEvent(t *testing.T, tx *sql.Tx, payload []byte) uint64 {
	t.Helper()
	ctx := t.Context()
	if _, err := tx.ExecContext(ctx, `UPDATE worker_outbox_sequence SET seq = seq + 1 WHERE id = 1`); err != nil {
		t.Fatalf("advancing sequence: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO worker_outbox (seq, created_at, payload)
		SELECT seq, UTC_TIMESTAMP(6), ? FROM worker_outbox_sequence WHERE id = 1`, payload); err != nil {
		t.Fatalf("appending outbox row: %v", err)
	}
	var seq uint64
	if err := tx.QueryRowContext(ctx, `SELECT seq FROM worker_outbox_sequence WHERE id = 1`).Scan(&seq); err != nil {
		t.Fatalf("reading sequence: %v", err)
	}
	return seq
}

// appendRawEvents commits n outbox rows carrying payload in one transaction,
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
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatalf("beginning transaction: %v", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op once committed
	if _, err := tx.ExecContext(ctx, `UPDATE worker_outbox_sequence SET seq = seq + ? WHERE id = 1`, n); err != nil {
		t.Fatalf("advancing sequence: %v", err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT seq FROM worker_outbox_sequence WHERE id = 1`).Scan(&last); err != nil {
		t.Fatalf("reading sequence: %v", err)
	}
	first = last - uint64(n) + 1
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO worker_outbox (seq, created_at, payload)
		WITH RECURSIVE r (i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM r WHERE i < ?)
		SELECT ? + i, UTC_TIMESTAMP(6) - INTERVAL ? MICROSECOND, ? FROM r`,
		n-1, first, age.Microseconds(), payload); err != nil {
		t.Fatalf("appending outbox rows: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("committing outbox rows: %v", err)
	}
	return first, last
}

func currentSeq(t *testing.T, p *Persistence) uint64 {
	t.Helper()
	var seq uint64
	if err := p.db.QueryRowContext(t.Context(), `SELECT seq FROM worker_outbox_sequence WHERE id = 1`).Scan(&seq); err != nil {
		t.Fatalf("reading sequence: %v", err)
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

// TestWorkerEvent_OnlyAfterCommit proves a worker write's outbox row shares
// the write's transaction: a rolled-back write leaves neither an event nor a
// consumed seq, while a committed write always produces an event.
func TestWorkerEvent_OnlyAfterCommit(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	// A replica's watcher sees only polled rows, in seq order, so a
	// rolled-back row that leaked would arrive before the committed one.
	watch := watchWorkers(t, newReplica(t, s))
	before := currentSeq(t, s)

	const workerName = "6e4d2f81-b3a9-4c05-8e72-1f9d4a0c7b63"
	worker := newTestWorker(workerName)
	protoBytes, err := proto.Marshal(worker)
	if err != nil {
		t.Fatalf("marshaling worker: %v", err)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO workers (name, uid, version, proto) VALUES (?, ?, ?, ?)`,
		workerName, "rolled-back-uid", int64(1), protoBytes); err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	appendRawEvent(t, tx, workerPayload(t, store.WorkerEventCreated, "rolled-back"))
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}
	if got := currentSeq(t, s); got != before {
		t.Errorf("sequence = %d after a rolled-back write, want %d", got, before)
	}

	created, err := s.CreateWorker(ctx, worker)
	if err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	event := receive(t, watch)
	if event.Type != store.WorkerEventCreated {
		t.Errorf("event type = %v, want WorkerEventCreated", event.Type)
	}
	if diff := cmp.Diff(created, event.Worker, protocmp.Transform()); diff != "" {
		t.Errorf("event worker mismatch (-want +got):\n%s", diff)
	}
}

// TestWatchWorkers_OutOfOrderCommitNotSkipped holds the sequence row in an
// open transaction and starts a second writer, which blocks on that row. The
// second writer can only take the next seq after the first commits, so the
// watcher receives both, in seq order, and its cursor never passes a row that
// commits later.
func TestWatchWorkers_OutOfOrderCommitNotSkipped(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	watch := watchWorkers(t, newReplica(t, s))

	tx1, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer tx1.Rollback() //nolint:errcheck // no-op once committed
	firstSeq := appendRawEvent(t, tx1, workerPayload(t, store.WorkerEventCreated, "first-open-writer"))

	second := make(chan error, 1)
	go func() {
		_, err := s.CreateWorker(ctx, newTestWorker("second-blocked-writer"))
		second <- err
	}()
	waitForLockWait(t, second)
	select {
	case event := <-watch.Events:
		t.Fatalf("delivered %q while no outbox write had committed", event.Worker.GetMetadata().GetName())
	default:
	}

	if err := tx1.Commit(); err != nil {
		t.Fatalf("committing the first writer: %v", err)
	}
	select {
	case err := <-second:
		if err != nil {
			t.Fatalf("CreateWorker behind the open writer failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("CreateWorker stayed blocked after the first writer committed")
	}

	got := []string{receive(t, watch).Worker.GetMetadata().GetName(), receive(t, watch).Worker.GetMetadata().GetName()}
	if want := []string{"first-open-writer", "second-blocked-writer"}; !slices.Equal(got, want) {
		t.Errorf("delivered %q, want %q", got, want)
	}
	if diff := cmp.Diff([]uint64{firstSeq, firstSeq + 1}, outboxSeqs(t, s)); diff != "" {
		t.Errorf("outbox seqs (-want +got):\n%s", diff)
	}
}

// A write that holds the sequence row when a watch subscribes has not
// committed, so the watch's cursor starts below it and delivers it once it
// commits. Subscribing must not wait on that write.
func TestWatchWorkers_DeliversAWriteInFlightAtSubscribe(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	replica := newReplica(t, s)

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op once committed
	appendRawEvent(t, tx, workerPayload(t, store.WorkerEventCreated, "in-flight"))

	subscribed := make(chan *store.WorkerWatch, 1)
	go func() {
		watch, err := replica.WatchWorkers(ctx)
		if err != nil {
			t.Errorf("WatchWorkers failed: %v", err)
			close(subscribed)
			return
		}
		subscribed <- watch
	}()
	var watch *store.WorkerWatch
	select {
	case watch = <-subscribed:
		if watch == nil {
			t.FailNow()
		}
		t.Cleanup(watch.Close)
	case <-time.After(5 * time.Second):
		t.Fatal("WatchWorkers blocked behind an open worker write")
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	if got := receive(t, watch).Worker.GetMetadata().GetName(); got != "in-flight" {
		t.Errorf("delivered %q, want in-flight", got)
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

// TestWorkerEvents_OneRowPerWrite pins the invariant the seq cursor rests on:
// each committed worker write appends exactly one outbox row at the next seq,
// and a write that announces nothing consumes none.
func TestWorkerEvents_OneRowPerWrite(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	before := currentSeq(t, s)

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
	if diff := cmp.Diff(seqRange(before+1, before+writes), outboxSeqs(t, s)); diff != "" {
		t.Errorf("outbox seqs (-want +got):\n%s", diff)
	}
	if got := currentSeq(t, s); got != before+writes {
		t.Errorf("sequence = %d, want %d", got, before+writes)
	}
}

// Retention deletes rows oldest seq first and stops at the first row still
// within retention, so the trim mark always bounds a contiguous deleted
// prefix, even if a later row carries an older timestamp.
func TestTrimWorkerOutbox_TrimsOnlyTheExpiredPrefix(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	payload := []byte("payload")
	first, _ := appendRawEvents(t, s, 2, payload, time.Hour)
	fresh, _ := appendRawEvents(t, s, 1, payload, 0)
	lateOld, _ := appendRawEvents(t, s, 1, payload, time.Hour)

	for range 2 {
		if err := s.trimWorkerOutboxOlderThan(ctx, 30*time.Minute); err != nil {
			t.Fatalf("trimWorkerOutboxOlderThan failed: %v", err)
		}
		if diff := cmp.Diff([]uint64{fresh, lateOld}, outboxSeqs(t, s)); diff != "" {
			t.Errorf("outbox seqs after trim (-want +got):\n%s", diff)
		}
		if got := trimMark(t, s); got != first+1 {
			t.Errorf("trim mark = %d, want %d", got, first+1)
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

	appendRawEvents(t, s, outboxBatch+100, workerPayload(t, store.WorkerEventUpdated, "lagging-worker"), 0)
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

// Retention of rows a watcher has already consumed leaves the trim mark at
// or below its cursor, so the watcher keeps delivering.
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
	if err := s.trimWorkerOutboxOlderThan(ctx, 0); err != nil {
		t.Fatalf("trimWorkerOutboxOlderThan failed: %v", err)
	}
	if got, want := trimMark(t, s), currentSeq(t, s); got != want {
		t.Fatalf("trim mark = %d, want %d", got, want)
	}

	if _, err := s.CreateWorker(ctx, newTestWorker("after-trim")); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	if got := receive(t, watch).Worker.GetMetadata().GetName(); got != "after-trim" {
		t.Errorf("delivered %q, want after-trim", got)
	}
}

// A database that loses committed writes (a failover or a restore) moves the
// sequence behind a watcher's cursor. New writes would reuse seqs the cursor
// has passed, so the watcher must close rather than skip them.
func TestWatchWorkers_ClosesWhenTheSequenceMovesBehindTheCursor(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	watch := watchWorkers(t, newReplica(t, s))

	if _, err := s.CreateWorker(ctx, newTestWorker("lost-worker")); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	if got := receive(t, watch).Worker.GetMetadata().GetName(); got != "lost-worker" {
		t.Fatalf("delivered %q, want lost-worker", got)
	}
	head := currentSeq(t, s)
	if _, err := s.db.ExecContext(ctx, `DELETE FROM worker_outbox WHERE seq = ?`, head); err != nil {
		t.Fatalf("dropping the last outbox row: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE worker_outbox_sequence SET seq = seq - 1 WHERE id = 1`); err != nil {
		t.Fatalf("rewinding the sequence: %v", err)
	}
	requireClosedNext(t, watch, "the sequence moved behind the cursor")
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

// TestWatchWorkers_ClosesOnCorruptPayload pins close-over-skip: a payload
// that fails to decode must close the watch rather than advance past it.
func TestWatchWorkers_ClosesOnCorruptPayload(t *testing.T) {
	s := setupMySQLPersistence(t)
	watch := watchWorkers(t, s)

	tx, err := s.db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op once committed
	appendRawEvent(t, tx, []byte{0xff, 0xde, 0xad})
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	requireClosedNext(t, watch, "a corrupt payload was skipped")
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
