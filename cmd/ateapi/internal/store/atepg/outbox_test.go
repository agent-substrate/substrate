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

// Tests for the worker outbox (outbox.go): transactional append, watch
// delivery and its safety fences, partition maintenance/retention, and the
// dedicated watch pool. Shared container fixtures live in atepg_test.go.

package atepg

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// TestConnect_DedicatedWatchPool covers the dual-pool path only Connect
// takes (the rest of the suite uses NewPersistence, where feed traffic
// shares the caller's pool): the watch pool must be distinct and owned, and
// the watch must deliver through it end to end.
func TestConnect_DedicatedWatchPool(t *testing.T) {
	requirePool(t) // ensures the container is up and containerDSN is set
	ctx := context.Background()

	p, err := Connect(ctx, containerDSN, "public")
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer p.pool.Close()
	defer p.Close()

	if p.watchPool == p.pool {
		t.Fatal("Connect did not create a dedicated watch pool")
	}
	if !p.ownsWatchPool {
		t.Fatal("Connect must own the watch pool so Close releases it")
	}
	if got := p.watchPool.Config().MaxConns; got != watchPoolMaxConns {
		t.Fatalf("watch pool MaxConns = %d, want %d", got, watchPoolMaxConns)
	}

	clearAll(t, p)
	watch, err := p.WatchWorkers(ctx)
	if err != nil {
		t.Fatalf("WatchWorkers failed: %v", err)
	}
	defer watch.Close()
	worker := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "watchpool-worker"},
		WorkerNamespace: "ns",
		WorkerPool:      "pool",
		WorkerPod:       "watchpool-pod",
	}
	if _, err := p.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	select {
	case event := <-watch.Events:
		if event.Type != store.WorkerEventCreated || event.Worker.GetWorkerPod() != "watchpool-pod" {
			t.Fatalf("unexpected event %v %s", event.Type, event.Worker.GetWorkerPod())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for event through the watch pool")
	}
}

// TestWorkerEvent_OnlyAfterCommit proves the doc's atomicity claim: a
// worker write's outbox insert shares the write's transaction, so a
// rolled-back write never produces an event, while a committed write always
// does.
func TestWorkerEvent_OnlyAfterCommit(t *testing.T) {
	s := setupPostgresStore(t).(*Persistence)
	ctx := context.Background()

	watch, err := s.WatchWorkers(ctx)
	if err != nil {
		t.Fatalf("WatchWorkers failed: %v", err)
	}
	defer watch.Close()

	const workerName = "6e4d2f81-b3a9-4c05-8e72-1f9d4a0c7b63"
	worker := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: workerName},
		WorkerNamespace: "ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod",
		WorkerPodUid:    workerName,
	}
	protoBytes, err := proto.Marshal(worker)
	if err != nil {
		t.Fatalf("marshaling worker: %v", err)
	}

	// Write the row and roll back instead of committing: no event should
	// ever arrive, proving the outbox insert is undone with the rest of the
	// transaction.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO workers (name, uid, version, proto)
		VALUES ($1, $2, $3, $4)`,
		workerName, "rolled-back-uid", int64(1), protoBytes); err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO worker_outbox (payload) VALUES ($1)`, []byte("rolled-back-payload")); err != nil {
		t.Fatalf("outbox insert failed: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	select {
	case event := <-watch.Events:
		t.Fatalf("received event %+v from a rolled-back transaction; the outbox insert must be undone with the rest of the transaction", event)
	case <-time.After(500 * time.Millisecond):
		// Expected: nothing arrives.
	}

	// The equivalent committed write must produce an event.
	if _, err := s.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	select {
	case event := <-watch.Events:
		if event.Type != store.WorkerEventCreated {
			t.Errorf("expected WorkerEventCreated, got %v", event.Type)
		}
		// CreateWorker assigns the uid, version and timestamps server-side.
		want := proto.Clone(worker).(*ateapipb.Worker)
		want.Metadata.Version = 1
		if diff := cmp.Diff(want, event.Worker, protocmp.Transform(),
			protocmp.IgnoreFields(&ateapipb.ResourceMetadata{}, "uid", "create_time", "update_time")); diff != "" {
			t.Errorf("event worker mismatch (-want +got):\n%s", diff)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event from a committed write")
	}
}

// TestWatchWorkers_OutOfOrderCommitNotSkipped reproduces the commit-order
// gap: xids are assigned at a transaction's first write but rows appear at
// COMMIT, so a transaction holding a lower xid can commit after a
// higher-xid sibling. A watcher that advanced past every visible row would
// skip the in-flight one and lose its event permanently. The xmin fence
// must instead hold the committed sibling back until the older
// transaction resolves, then deliver both in order.
func TestWatchWorkers_OutOfOrderCommitNotSkipped(t *testing.T) {
	s := setupPostgresStore(t).(*Persistence)
	ctx := context.Background()

	watch, err := s.WatchWorkers(ctx)
	if err != nil {
		t.Fatalf("WatchWorkers failed: %v", err)
	}
	defer watch.Close()

	mkPayload := func(pod string) []byte {
		payload, err := marshalWorkerEvent(store.WorkerEventCreated,
			&ateapipb.Worker{
				Metadata:        &ateapipb.ResourceMetadata{Name: pod},
				WorkerNamespace: "ns", WorkerPool: "pool", WorkerPod: pod,
			})
		if err != nil {
			t.Fatalf("marshaling event for %q: %v", pod, err)
		}
		return payload
	}

	// tx1 appends first (lower xid) and stays open.
	tx1, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx1 failed: %v", err)
	}
	defer tx1.Rollback(ctx) //nolint:errcheck // no-op once committed
	if _, err := tx1.Exec(ctx, `INSERT INTO worker_outbox (payload) VALUES ($1)`, mkPayload("first-xid-late-commit")); err != nil {
		t.Fatalf("tx1 outbox insert failed: %v", err)
	}

	// tx2 appends second (higher xid) and commits immediately.
	tx2, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx2 failed: %v", err)
	}
	if _, err := tx2.Exec(ctx, `INSERT INTO worker_outbox (payload) VALUES ($1)`, mkPayload("second-xid-early-commit")); err != nil {
		t.Fatalf("tx2 outbox insert failed: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("tx2 Commit failed: %v", err)
	}

	// While tx1 is in flight, tx2's committed event must be held back by
	// the xmin fence — otherwise the cursor has already skipped tx1's row.
	select {
	case event := <-watch.Events:
		t.Fatalf("event %q delivered while an older feed transaction was still in flight; its sibling event is now unreachable", event.Worker.GetWorkerPod())
	case <-time.After(500 * time.Millisecond):
		// Expected: fence holds both events back.
	}

	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("tx1 Commit failed: %v", err)
	}

	var got []string
	for len(got) < 2 {
		select {
		case event := <-watch.Events:
			got = append(got, event.Worker.GetWorkerPod())
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for both events; delivered so far: %v", got)
		}
	}
	want := []string{"first-xid-late-commit", "second-xid-early-commit"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("event delivery order mismatch (-want +got):\n%s", diff)
	}
}

// TestWorkerOutboxPartitionRetention verifies partition-based
// retention: an hourly partition wholly past outboxRetentionAge is
// dropped (with its greatest xid recorded in worker_outbox_trim), fresh
// rows survive, and aged strays in the DEFAULT partition are trimmed by the
// row-wise fallback.
func TestWorkerOutboxPartitionRetention(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	// A partition two hours back, holding one aged event.
	stale := time.Now().UTC().Add(-2 * time.Hour)
	if err := s.createWorkerOutboxPartitions(ctx, stale); err != nil {
		t.Fatalf("creating stale partition failed: %v", err)
	}
	var staleXid string
	if err := s.pool.QueryRow(ctx, `INSERT INTO worker_outbox (payload, created_at) VALUES ($1, $2) RETURNING xid::text`,
		[]byte("old"), stale).Scan(&staleXid); err != nil {
		t.Fatalf("inserting aged row failed: %v", err)
	}
	// An aged stray in the DEFAULT partition (no hourly partition covers a
	// day ago), and a fresh row in the current partition.
	if _, err := s.pool.Exec(ctx, `INSERT INTO worker_outbox (payload, created_at) VALUES ($1, now() - interval '1 day')`, []byte("stray")); err != nil {
		t.Fatalf("inserting default-partition stray failed: %v", err)
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO worker_outbox (payload) VALUES ($1)`, []byte("fresh")); err != nil {
		t.Fatalf("inserting fresh row failed: %v", err)
	}

	if err := s.maintainWorkerOutboxPartitions(ctx); err != nil {
		t.Fatalf("maintainWorkerOutboxPartitions failed: %v", err)
	}

	var staleExists bool
	if err := s.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`,
		workerOutboxPartitionName(stale)).Scan(&staleExists); err != nil {
		t.Fatalf("checking stale partition failed: %v", err)
	}
	if staleExists {
		t.Errorf("stale partition %s still exists, want dropped", workerOutboxPartitionName(stale))
	}
	var remaining int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM worker_outbox`).Scan(&remaining); err != nil {
		t.Fatalf("counting remaining rows failed: %v", err)
	}
	if remaining != 1 {
		t.Errorf("%d rows remain, want 1 (the fresh row; aged partition row and default stray gone)", remaining)
	}
	var trim bool
	if err := s.pool.QueryRow(ctx, `SELECT (SELECT xid FROM worker_outbox_trim) >= $1::xid8`, staleXid).Scan(&trim); err != nil {
		t.Fatalf("reading trim mark failed: %v", err)
	}
	if !trim {
		t.Errorf("trim mark does not cover dropped partition's xid %s", staleXid)
	}
}

// TestOutboxMaintenance_SingleMaintainer verifies the retention
// election: while another replica holds the advisory lock, a pass skips
// retention cleanly (no error, nothing dropped); once released, the next
// pass does the work. (Partition creation is deliberately unelected.)
func TestOutboxMaintenance_SingleMaintainer(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	stale := time.Now().UTC().Add(-2 * time.Hour)
	if err := s.createWorkerOutboxPartitions(ctx, stale); err != nil {
		t.Fatalf("creating stale partition failed: %v", err)
	}

	// Another "replica" mid-pass: hold the advisory lock in an open
	// transaction of our own.
	holder, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin holder failed: %v", err)
	}
	defer holder.Rollback(ctx) //nolint:errcheck // released below
	if _, err := holder.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext(current_database() || ':' || $1))`, outboxMaintenanceLockKey); err != nil {
		t.Fatalf("taking maintenance lock failed: %v", err)
	}

	if err := s.maintainWorkerOutboxPartitions(ctx); err != nil {
		t.Fatalf("pass with lock held must skip cleanly, got: %v", err)
	}
	var staleExists bool
	if err := s.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, workerOutboxPartitionName(stale)).Scan(&staleExists); err != nil {
		t.Fatalf("checking stale partition failed: %v", err)
	}
	if !staleExists {
		t.Fatal("stale partition was dropped by a pass that lost the election")
	}

	if err := holder.Rollback(ctx); err != nil {
		t.Fatalf("releasing maintenance lock failed: %v", err)
	}
	if err := s.maintainWorkerOutboxPartitions(ctx); err != nil {
		t.Fatalf("pass after lock release failed: %v", err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, workerOutboxPartitionName(stale)).Scan(&staleExists); err != nil {
		t.Fatalf("re-checking stale partition failed: %v", err)
	}
	if staleExists {
		t.Error("stale partition survived a pass that held the election")
	}
}

// TestWorkerEvents_OneRowPerTransaction pins the invariant the xid-only
// watch cursor rests on: writeAndAppendEvent appends exactly one outbox row
// per transaction, so xids are distinct across the outbox and a poll batch
// can never split a same-xid group.
func TestWorkerEvents_OneRowPerTransaction(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	worker := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "one-row-worker"},
		WorkerNamespace: "ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod",
	}
	if _, err := s.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	for i := 0; i < 10; i++ {
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
	if _, err := s.DeleteWorker(ctx, "one-row-worker", store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteWorker failed: %v", err)
	}

	var total, distinct int
	if err := s.pool.QueryRow(ctx, `SELECT count(*), count(DISTINCT xid) FROM worker_outbox`).Scan(&total, &distinct); err != nil {
		t.Fatalf("counting outbox rows failed: %v", err)
	}
	if total == 0 || total != distinct {
		t.Errorf("feed has %d rows but %d distinct xids; the one-row-per-transaction invariant is broken", total, distinct)
	}
}

// TestWatchWorkers_DeliveryFencedByOldestTransaction documents the xmin
// fence's real bound: one old transaction anywhere holds back delivery of
// everything committed after it, for as long as it lives.
func TestWatchWorkers_DeliveryFencedByOldestTransaction(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	watch, err := s.WatchWorkers(ctx)
	if err != nil {
		t.Fatalf("WatchWorkers failed: %v", err)
	}
	defer watch.Close()

	// An unrelated transaction that merely holds an xid.
	blocker, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin blocker failed: %v", err)
	}
	defer blocker.Rollback(ctx) //nolint:errcheck // released below
	if _, err := blocker.Exec(ctx, `SELECT pg_current_xact_id()`); err != nil {
		t.Fatalf("assigning blocker xid failed: %v", err)
	}

	worker := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "fenced-worker"},
		WorkerNamespace: "ns",
		WorkerPool:      "pool",
		WorkerPod:       "fenced",
	}
	if _, err := s.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}

	select {
	case event := <-watch.Events:
		t.Fatalf("event %+v delivered through the fence while an older transaction was in flight", event)
	case <-time.After(600 * time.Millisecond):
		// Expected: committed but fenced behind the blocker's xid.
	}

	if err := blocker.Rollback(ctx); err != nil {
		t.Fatalf("ending blocker failed: %v", err)
	}
	select {
	case event := <-watch.Events:
		if got := event.Worker.GetWorkerPod(); got != "fenced" {
			t.Errorf("delivered %q, want %q", got, "fenced")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("event not delivered after the fencing transaction ended")
	}
}

// TestClose_StopsMaintenance pins that Close ends the background
// maintenance goroutine (main.go defers it for exactly this): Close blocks
// on the loop's done channel, so its return IS the assertion.
func TestClose_StopsMaintenance(t *testing.T) {
	ctx := context.Background()
	p, err := NewPersistence(ctx, requirePool(t))
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

// TestPollQueryPlanStaysOnIndex pins the poll's plan shape against the
// output-column shadowing bug: an unaliased xid::text captures the bare
// ORDER BY name, sorting xids as text — which both diverges from the
// cursor predicate's xid8 order (silently skipping events across digit
// boundaries) and forces full scans with a top-N sort. Behavioral tests
// cannot see this; the plan can.
func TestPollQueryPlanStaysOnIndex(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	// Seed enough rows (and stats) for the planner to have a real choice:
	// on empty partitions it costs bitmap scans plus an explicit Sort as
	// cheapest regardless of the index, which would make the Merge Append
	// assertion below vacuously unreachable.
	if _, err := s.pool.Exec(ctx, `INSERT INTO worker_outbox (payload) SELECT 'x'::bytea FROM generate_series(1, 3000)`); err != nil {
		t.Fatalf("seeding outbox rows failed: %v", err)
	}
	if _, err := s.pool.Exec(ctx, `ANALYZE worker_outbox`); err != nil {
		t.Fatalf("ANALYZE failed: %v", err)
	}

	rows, err := s.pool.Query(ctx, "EXPLAIN "+pollWorkerOutboxSQL, "100", outboxBatch)
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scanning plan line: %v", err)
		}
		plan.WriteString(line)
		plan.WriteString("\n")
	}
	got := plan.String()
	if strings.Contains(got, "::text") {
		t.Errorf("poll plan sorts by a text expression (output-column shadowing is back):\n%s", got)
	}
	if !strings.Contains(got, "Merge Append") {
		t.Errorf("poll plan is not an index-ordered Merge Append:\n%s", got)
	}
}

// TestOutboxMaintenance_ConcurrentPassesAreHarmless backs the doc's
// claim: two replicas racing a maintenance pass produce no errors and the
// correct end state (one wins the election, the loser skips).
func TestOutboxMaintenance_ConcurrentPassesAreHarmless(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	replica, err := NewPersistence(ctx, s.pool)
	if err != nil {
		t.Fatalf("second Persistence failed: %v", err)
	}
	t.Cleanup(replica.Close)

	stale := time.Now().UTC().Add(-2 * time.Hour)
	if err := s.createWorkerOutboxPartitions(ctx, stale); err != nil {
		t.Fatalf("creating stale partition failed: %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, p := range []*Persistence{s, replica} {
		wg.Add(1)
		go func(i int, p *Persistence) {
			defer wg.Done()
			errs[i] = p.maintainWorkerOutboxPartitions(ctx)
		}(i, p)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("concurrent pass %d returned error: %v", i, err)
		}
	}
	var staleExists bool
	if err := s.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, workerOutboxPartitionName(stale)).Scan(&staleExists); err != nil {
		t.Fatalf("checking stale partition failed: %v", err)
	}
	if staleExists {
		t.Error("stale partition survived both concurrent passes")
	}
}

// TestWorkerOutboxPartitionsAreUnlogged pins the maintenance profile the
// schema documents: every outbox partition must be UNLOGGED (relpersistence
// 'u') with autovacuum disabled (all are insert-only and discarded whole,
// by drop or truncate — an in-window insert-autovacuum is a measured p99
// spike); and worker_outbox_trim — the loss-detection high-water mark —
// must remain logged so it survives a crash.
func TestWorkerOutboxPartitionsAreUnlogged(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	rows, err := s.pool.Query(ctx, `
		SELECT c.relname, c.relpersistence, COALESCE(array_to_string(c.reloptions, ','), '') FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid
		JOIN pg_class parent ON parent.oid = i.inhparent
		WHERE parent.relname = 'worker_outbox'`)
	if err != nil {
		t.Fatalf("listing outbox partitions: %v", err)
	}
	defer rows.Close()
	checked := 0
	for rows.Next() {
		var name, persistence, options string
		if err := rows.Scan(&name, &persistence, &options); err != nil {
			t.Fatalf("scanning partition row: %v", err)
		}
		if persistence != "u" {
			t.Errorf("partition %s has relpersistence %q, want 'u' (unlogged)", name, persistence)
		}
		if !strings.Contains(options, "autovacuum_enabled=off") {
			t.Errorf("partition %s does not disable autovacuum (reloptions %q)", name, options)
		}
		checked++
	}
	if checked == 0 {
		t.Fatal("no outbox partitions found to check")
	}
	var trimPersistence string
	if err := s.pool.QueryRow(ctx, `SELECT relpersistence FROM pg_class WHERE relname = 'worker_outbox_trim'`).Scan(&trimPersistence); err != nil {
		t.Fatalf("checking worker_outbox_trim persistence: %v", err)
	}
	if trimPersistence != "p" {
		t.Errorf("worker_outbox_trim has relpersistence %q, want 'p' (logged) — the trim mark must survive a crash", trimPersistence)
	}
}

// The restart escape hatch (a changed pg_postmaster_start_time() closes
// the watch, because a restart truncates the UNLOGGED feed) has no e2e
// test here: restarting the testcontainer remaps its host port, severing
// the pool permanently — unlike production, where the database endpoint is
// stable across restarts. The comparison itself is four lines in
// WatchWorkers' poll loop; the trimmed-past-cursor test below covers the
// shared close-for-resync path.

// TestWatchWorkers_ClosesWhenTrimmedPastCursor verifies the retention
// escape hatch: when rows a watcher has not consumed are deleted out from
// under it (a retention trim on a badly lagging watcher), the watcher must
// close its channel — the signal consumers treat as resync-and-relist —
// rather than silently skip the gap.
func TestWatchWorkers_ClosesWhenTrimmedPastCursor(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	watch, err := s.WatchWorkers(ctx)
	if err != nil {
		t.Fatalf("WatchWorkers failed: %v", err)
	}
	defer watch.Close()

	// Deliver one event normally so the cursor is established.
	worker := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "trim-worker"},
		WorkerNamespace: "ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod",
	}
	if _, err := s.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	select {
	case <-watch.Events:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the first event")
	}

	// Atomically append three events and trim them away unconsumed —
	// the watcher never gets a chance to see them, exactly as if
	// retention took rows a lagging watcher had not reached.
	payload, err := marshalWorkerEvent(store.WorkerEventUpdated, worker)
	if err != nil {
		t.Fatalf("marshaling event: %v", err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed
	if _, err := tx.Exec(ctx, `INSERT INTO worker_outbox (payload) VALUES ($1), ($1), ($1)`, payload); err != nil {
		t.Fatalf("outbox inserts failed: %v", err)
	}
	// Mirrors trimWorkerChangesDefault's shape: the mark is the deleted
	// set's greatest xid. (The three rows above share one transaction —
	// fine here: this test only needs the recorded mark to land past the
	// watcher's cursor, and deletes everything the watcher has not seen.)
	if _, err := tx.Exec(ctx, `
		WITH doomed AS (
			DELETE FROM worker_outbox WHERE xid > (SELECT COALESCE((SELECT xid FROM worker_outbox_trim), '0'::xid8))
			RETURNING xid
		)
		INSERT INTO worker_outbox_trim (xid)
		SELECT xid FROM doomed ORDER BY xid DESC LIMIT 1
		ON CONFLICT (id) DO UPDATE SET xid = EXCLUDED.xid
		WHERE EXCLUDED.xid > worker_outbox_trim.xid`); err != nil {
		t.Fatalf("trim failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// The watcher must close the channel, not deliver past the gap.
	select {
	case event, ok := <-watch.Events:
		if ok {
			t.Fatalf("received event %+v past a trimmed gap; expected the channel to close for resync", event)
		}
		// Expected: channel closed.
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the watch channel to close after a trim past the cursor")
	}
}

// TestPartitionCreation_UnwedgesFromStrayedDefault reproduces the
// self-wedging order caught in review: creation stalls past the lead while
// writes continue, strays land in DEFAULT with current-range timestamps, and
// CREATE ... PARTITION OF for that range then fails outright — permanently,
// since maintenance returns before its strays cleanup and boot fails the
// same way. The fix retries creation with a same-transaction truncate of the
// strays; this test drills the exact scenario.
func TestPartitionCreation_UnwedgesFromStrayedDefault(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	// Stay clear of a partition boundary so the stray and the re-created
	// partition land in the same range.
	now := time.Now().UTC()
	if rem := now.Truncate(outboxPartitionInterval).Add(outboxPartitionInterval).Sub(now); rem < 5*time.Second {
		time.Sleep(rem + time.Second)
		now = time.Now().UTC()
	}

	// Simulate the stall: the current range's partition does not exist.
	if _, err := s.pool.Exec(ctx, `DROP TABLE `+workerOutboxPartitionName(now)); err != nil {
		t.Fatalf("dropping current partition: %v", err)
	}
	// A write during the stall detours into DEFAULT.
	worker := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "wedge-worker"},
		WorkerNamespace: "ns",
		WorkerPool:      "pool",
		WorkerPod:       "wedge-pod",
	}
	if _, err := s.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker during stall: %v", err)
	}
	var strays bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM worker_outbox_default)`).Scan(&strays); err != nil {
		t.Fatalf("checking DEFAULT: %v", err)
	}
	if !strays {
		t.Fatal("expected the stall-window write to land in the DEFAULT partition")
	}

	// The stall clears: creation must dig itself out (pre-fix this returned
	// SQLSTATE 23514 forever, and NewPersistence failed the same way).
	if err := s.createWorkerOutboxPartitions(ctx, outboxPartitionLeadTimes(now)...); err != nil {
		t.Fatalf("createWorkerOutboxPartitions did not un-wedge: %v", err)
	}

	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM worker_outbox_default)`).Scan(&strays); err != nil {
		t.Fatalf("re-checking DEFAULT: %v", err)
	}
	if strays {
		t.Fatal("DEFAULT partition still holds strays after the rescue")
	}
	var partitionExists bool
	if err := s.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, workerOutboxPartitionName(now)).Scan(&partitionExists); err != nil {
		t.Fatalf("checking recreated partition: %v", err)
	}
	if !partitionExists {
		t.Fatal("current-range partition was not recreated")
	}
	// The truncated stray was never trim-covered before the fix; the rescue
	// must have recorded it so lagging watchers resync rather than skip.
	var trimSet bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM worker_outbox_trim WHERE xid > '0'::xid8)`).Scan(&trimSet); err != nil {
		t.Fatalf("checking trim mark: %v", err)
	}
	if !trimSet {
		t.Fatal("rescue did not record a trim mark for the truncated strays")
	}
}

// TestOutboxMaintenance_NoDeadlockWithConcurrentWriters drills the measured
// child-vs-parent lock-order deadlock: writers route into DEFAULT exactly
// when the stray cleanup runs (that's the truncate path's precondition, not
// a coincidence), and writers lock parent-then-child while a single
// truncate+drops transaction locked child-then-parent. With retention split
// into two transactions and the wedge rescue locking the parent first, no
// interleaving can cycle. Pre-fix this test deadlocked (SQLSTATE 40P01)
// within an iteration or two.
func TestOutboxMaintenance_NoDeadlockWithConcurrentWriters(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	now := time.Now().UTC()
	if rem := now.Truncate(outboxPartitionInterval).Add(outboxPartitionInterval).Sub(now); rem < 10*time.Second {
		time.Sleep(rem + time.Second)
		now = time.Now().UTC()
	}

	// An aged-out partition with a row, so the drops leg has real work.
	old := now.Add(-2*outboxRetentionAge - 2*outboxPartitionInterval)
	if err := s.createWorkerOutboxPartitions(ctx, old); err != nil {
		t.Fatalf("creating expired partition: %v", err)
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO worker_outbox (created_at, payload) VALUES ($1, $2)`,
		old, []byte{byte(store.WorkerEventUpdated)}); err != nil {
		t.Fatalf("seeding expired partition: %v", err)
	}

	// Writers hammering the outbox for the whole test.
	writerCtx, stopWriters := context.WithCancel(ctx)
	defer stopWriters()
	var wg sync.WaitGroup
	writerErr := make(chan error, 4)
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; writerCtx.Err() == nil; i++ {
				w := &ateapipb.Worker{
					Metadata:        &ateapipb.ResourceMetadata{Name: fmt.Sprintf("ddl-worker-%d-%d", g, i)},
					WorkerNamespace: "ns", WorkerPool: "pool", WorkerPod: fmt.Sprintf("ddl-pod-%d-%d", g, i),
				}
				if _, err := s.CreateWorker(writerCtx, w); err != nil && writerCtx.Err() == nil {
					select {
					case writerErr <- fmt.Errorf("writer %d iteration %d: %w", g, i, err):
					default:
					}
					return
				}
			}
		}(g)
	}

	// Repeatedly re-enter the degraded state under live writers: drop the
	// current partition (parent-first, or this helper itself would ABBA),
	// let writes detour into DEFAULT, then run a full maintenance pass —
	// rescue-create, stray cleanup, and drops, all with writers in flight.
	for i := 0; i < 4; i++ {
		if _, err := s.pool.Exec(ctx,
			`DO $$ BEGIN LOCK TABLE worker_outbox IN ACCESS EXCLUSIVE MODE; EXECUTE 'DROP TABLE IF EXISTS `+workerOutboxPartitionName(time.Now().UTC())+`'; END $$`); err != nil {
			t.Fatalf("dropping current partition (iteration %d): %v", i, err)
		}
		time.Sleep(150 * time.Millisecond) // let writers detour into DEFAULT
		if err := s.maintainWorkerOutboxPartitions(ctx); err != nil {
			t.Fatalf("maintenance pass %d failed under concurrent writers: %v", i, err)
		}
	}

	stopWriters()
	wg.Wait()
	select {
	case err := <-writerErr:
		t.Fatalf("concurrent writer failed: %v", err)
	default:
	}

	var strays bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM worker_outbox_default)`).Scan(&strays); err != nil {
		t.Fatalf("checking DEFAULT: %v", err)
	}
	if strays {
		t.Fatal("DEFAULT partition still holds strays after maintenance")
	}
	var oldExists bool
	if err := s.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, workerOutboxPartitionName(old)).Scan(&oldExists); err != nil {
		t.Fatalf("checking expired partition: %v", err)
	}
	if oldExists {
		t.Fatal("expired partition survived the drops leg")
	}
}

// TestWatchWorkers_ClosesAfterPersistentPollFailure pins the watch
// contract's loss signal for polling outages: transient failures retry with
// the cursor kept (edge case 12), but a persistent failure must close the
// channel within outboxPollFailureCloseAfter so workercache flips not-ready
// and callers fail fast — instead of serving a frozen fleet view for as
// long as the outage lasts. (The postmaster-restart check cannot cover
// this: it fires only after connectivity returns.)
func TestWatchWorkers_ClosesAfterPersistentPollFailure(t *testing.T) {
	requirePool(t)
	ctx := context.Background()

	// Connect so the watcher has its own pool: killing it simulates a
	// persistent outage without touching the shared container pool.
	p, err := Connect(ctx, containerDSN, "public")
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	p.pollFailureCloseAfter = 300 * time.Millisecond // before WatchWorkers starts the poller
	defer p.pool.Close()
	defer p.Close()

	watch, err := p.WatchWorkers(ctx)
	if err != nil {
		t.Fatalf("WatchWorkers failed: %v", err)
	}
	defer watch.Close()

	p.watchPool.Close() // every subsequent poll now fails

	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-watch.Events:
			if !ok {
				return // closed: the loss signal fired
			}
		case <-deadline:
			t.Fatal("watch did not close after persistent poll failure")
		}
	}
}

// TestWatchWorkers_BaselineDoesNotMaskOwedTrims pins the subscribe-baseline
// corner from review: a transaction that took its xid BEFORE this watch
// subscribed but commits AFTER it is a legitimate owed event — yet with a
// baseline of "highest existing row xid", an already-committed HIGHER xid
// (out-of-order commit) raised the baseline above the owed xid, so when the
// owed row was trimmed while the xmin fence stalled, trim ≤ baseline and the
// fell-behind close never fired: silent loss. The baseline must only cover
// settled history below the cursor's own xmin snapshot.
func TestWatchWorkers_BaselineDoesNotMaskOwedTrims(t *testing.T) {
	s := setupPostgresStore(t).(*Persistence)
	ctx := context.Background()

	// L: an old in-flight transaction pinning xmin — the fence stall.
	lTx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin L: %v", err)
	}
	defer lTx.Rollback(ctx) //nolint:errcheck
	var xidL string
	if err := lTx.QueryRow(ctx, `SELECT pg_current_xact_id()::text`).Scan(&xidL); err != nil {
		t.Fatalf("assigning L's xid: %v", err)
	}

	// W: takes the next xid now, but commits only after the subscribe.
	wTx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin W: %v", err)
	}
	defer wTx.Rollback(ctx) //nolint:errcheck
	var xidW string
	if err := wTx.QueryRow(ctx, `SELECT pg_current_xact_id()::text`).Scan(&xidW); err != nil {
		t.Fatalf("assigning W's xid: %v", err)
	}

	// C: a LATER xid that commits BEFORE the subscribe — the baseline poison:
	// a visible outbox row whose xid exceeds W's.
	if _, err := s.CreateWorker(ctx, &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "baseline-poison"},
		WorkerNamespace: "ns", WorkerPool: "pool", WorkerPod: "baseline-pod",
	}); err != nil {
		t.Fatalf("CreateWorker (C): %v", err)
	}

	watch, err := s.WatchWorkers(ctx) // cursor = xidL-1; baseline must exclude C's xid (>= xmin)
	if err != nil {
		t.Fatalf("WatchWorkers: %v", err)
	}
	defer watch.Close()

	// W commits its owed event post-subscribe (fenced behind L, undeliverable).
	if _, err := wTx.Exec(ctx, `INSERT INTO worker_outbox (payload) VALUES ($1)`,
		[]byte{byte(store.WorkerEventUpdated)}); err != nil {
		t.Fatalf("W's outbox insert: %v", err)
	}
	if err := wTx.Commit(ctx); err != nil {
		t.Fatalf("committing W: %v", err)
	}

	// Retention drops W's partition while the fence still stalls: record the
	// trim exactly as dropWorkerOutboxPartition would.
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO worker_outbox_trim (xid) VALUES ($1::xid8)
		ON CONFLICT (id) DO UPDATE SET xid = EXCLUDED.xid
		WHERE EXCLUDED.xid > worker_outbox_trim.xid`, xidW); err != nil {
		t.Fatalf("recording trim of W's partition: %v", err)
	}

	// The fell-behind close must fire: trim (xidW) > cursor (xidL-1), and the
	// baseline may not be raised by C's higher-but-already-visible xid.
	// Nothing can be delivered first — everything above xidL is fenced.
	select {
	case event, ok := <-watch.Events:
		if ok {
			t.Fatalf("delivered event %+v through the fence", event)
		}
		// Closed: loss surfaced.
	case <-time.After(5 * time.Second):
		t.Fatal("watch stayed open: the trimmed owed event was silently lost (baseline masked the trim)")
	}
}

// TestUnmarshalWorkerEvent_BoundaryAssertions pins the write-side invariants
// asserted at the read boundary: known event-type byte and a keyable worker.
func TestUnmarshalWorkerEvent_BoundaryAssertions(t *testing.T) {
	valid, err := marshalWorkerEvent(store.WorkerEventUpdated, &ateapipb.Worker{
		Metadata: &ateapipb.ResourceMetadata{Name: "w1"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for name, tc := range map[string]struct {
		payload []byte
		wantErr bool
	}{
		"valid":             {valid, false},
		"empty":             {nil, true},
		"unknown type byte": {[]byte{0xff, 0x00}, true},
		"type byte only":    {[]byte{byte(store.WorkerEventCreated)}, true}, // empty proto = nameless
		"garbage proto":     {append([]byte{byte(store.WorkerEventCreated)}, 0xde, 0xad, 0xbe), true},
		"nameless worker":   {func() []byte { b, _ := marshalWorkerEvent(store.WorkerEventDeleted, &ateapipb.Worker{}); return b }(), true},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := unmarshalWorkerEvent(tc.payload)
			if (err != nil) != tc.wantErr {
				t.Fatalf("unmarshalWorkerEvent() err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

// TestWatchWorkers_ClosesOnCorruptPayload pins close-over-skip: a payload
// that fails the boundary checks must close the watch (loss surfaced,
// relist repairs) rather than advance the cursor past it silently.
func TestWatchWorkers_ClosesOnCorruptPayload(t *testing.T) {
	s := setupPostgresStore(t).(*Persistence)
	ctx := context.Background()

	watch, err := s.WatchWorkers(ctx)
	if err != nil {
		t.Fatalf("WatchWorkers: %v", err)
	}
	defer watch.Close()

	if _, err := s.pool.Exec(ctx, `INSERT INTO worker_outbox (payload) VALUES ($1)`,
		[]byte{0xff, 0xde, 0xad}); err != nil {
		t.Fatalf("inserting corrupt payload: %v", err)
	}

	select {
	case event, ok := <-watch.Events:
		if ok {
			t.Fatalf("delivered event %+v from a corrupt payload", event)
		}
		// Closed: the loss signal fired.
	case <-time.After(5 * time.Second):
		t.Fatal("watch stayed open past a corrupt payload (silent skip)")
	}
}
