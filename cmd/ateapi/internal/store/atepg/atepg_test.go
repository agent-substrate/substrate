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
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// One Postgres container serves every test in this package; each test gets
// isolation via DebugClearAll rather than a fresh container, which would be
// far slower. Tests in this package are not safe to run with -parallel.
var (
	containerOnce sync.Once
	containerPool *pgxpool.Pool
	containerDSN  string
	containerPG   *postgres.PostgresContainer
	containerErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if containerPool != nil {
		containerPool.Close()
	}
	if containerPG != nil {
		if err := containerPG.Terminate(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "terminating PostgreSQL testcontainer: %v\n", err)
			if code == 0 {
				code = 1
			}
		}
	}
	os.Exit(code)
}

func requirePool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	containerOnce.Do(func() {
		ctx := context.Background()
		pgContainer, err := postgres.Run(ctx, "postgres:18-alpine",
			postgres.WithDatabase("atepg"),
			postgres.WithUsername("atepg"),
			postgres.WithPassword("atepg"),
		)
		if err != nil {
			containerErr = err
			return
		}
		containerPG = pgContainer
		dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			containerErr = err
			return
		}
		containerDSN = dsn
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			containerErr = err
			return
		}
		// The official postgres image restarts its server process once after
		// initdb; the port accepts (and briefly resets) connections during
		// that window, so ping with retries rather than failing on the first
		// attempt.
		var pingErr error
		for i := 0; i < 30; i++ {
			pingErr = pool.Ping(ctx)
			if pingErr == nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		if pingErr != nil {
			containerErr = fmt.Errorf("pinging PostgreSQL testcontainer after retries: %w", pingErr)
			return
		}
		containerPool = pool
	})
	if containerErr != nil {
		t.Skipf("PostgreSQL testcontainer unavailable (requires Docker): %v", containerErr)
	}
	return containerPool
}

func setupPostgresPersistence(t *testing.T) *Persistence {
	t.Helper()
	ctx := context.Background()
	p, err := NewPersistence(ctx, requirePool(t))
	if err != nil {
		t.Fatalf("NewPersistence failed: %v", err)
	}
	t.Cleanup(p.Close)
	if err := p.DebugClearAll(ctx); err != nil {
		t.Fatalf("DebugClearAll failed: %v", err)
	}
	return p
}

func setupPostgresStore(t *testing.T) store.Interface {
	t.Helper()
	return setupPostgresPersistence(t)
}

// TestConnect_DedicatedWatchPool covers the dual-pool path only Connect
// takes (the rest of the suite uses NewPersistence, where feed traffic
// shares the caller's pool): the watch pool must be distinct and owned, and
// the watch must deliver through it end to end.
func TestConnect_DedicatedWatchPool(t *testing.T) {
	requirePool(t) // ensures the container is up and containerDSN is set
	ctx := context.Background()

	p, err := Connect(ctx, containerDSN)
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

	if err := p.DebugClearAll(ctx); err != nil {
		t.Fatalf("DebugClearAll failed: %v", err)
	}
	watch, err := p.WatchWorkers(ctx)
	if err != nil {
		t.Fatalf("WatchWorkers failed: %v", err)
	}
	defer watch.Close()
	worker := &ateapipb.Worker{WorkerNamespace: "ns", WorkerPool: "pool", WorkerPod: "watchpool-pod"}
	if err := p.CreateWorker(ctx, worker); err != nil {
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

func newTestAtespace(name string) *ateapipb.Atespace {
	return &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: name}}
}

// TestCreateActor_MissingAtespace_FailedPrecondition exercises the
// foreign-key race the doc calls out: CreateActor rejects an actor whose
// atespace doesn't exist (including a concurrently-deleted one), closing the
// TOCTOU window ateredis's separate existence check leaves open.
func TestCreateActor_MissingAtespace_FailedPrecondition(t *testing.T) {
	s := setupPostgresStore(t).(*Persistence)
	ctx := context.Background()

	actor := &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Name: "id1", Atespace: "no-such-atespace"},
		ActorTemplateNamespace: "ns1",
		ActorTemplateName:      "tmpl1",
		Status:                 &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	}
	if _, err := s.CreateActor(ctx, actor); !errors.Is(err, store.ErrFailedPrecondition) {
		t.Errorf("CreateActor with missing atespace = %v, want ErrFailedPrecondition", err)
	}
}

// TestWorkerEvent_OnlyAfterCommit proves the doc's atomicity claim: a
// worker write's change-feed insert shares the write's transaction, so a
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
	// ever arrive, proving the feed insert is undone with the rest of the
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
	if _, err := tx.Exec(ctx, `INSERT INTO worker_changes (payload) VALUES ($1)`, []byte("rolled-back-payload")); err != nil {
		t.Fatalf("feed insert failed: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	select {
	case event := <-watch.Events:
		t.Fatalf("received event %+v from a rolled-back transaction; the feed insert must be undone with the rest of the transaction", event)
	case <-time.After(500 * time.Millisecond):
		// Expected: nothing arrives.
	}

	// The equivalent committed write must produce an event.
	if err := s.CreateWorker(ctx, worker); err != nil {
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
			&ateapipb.Worker{WorkerNamespace: "ns", WorkerPool: "pool", WorkerPod: pod})
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
	if _, err := tx1.Exec(ctx, `INSERT INTO worker_changes (payload) VALUES ($1)`, mkPayload("first-xid-late-commit")); err != nil {
		t.Fatalf("tx1 feed insert failed: %v", err)
	}

	// tx2 appends second (higher xid) and commits immediately.
	tx2, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx2 failed: %v", err)
	}
	if _, err := tx2.Exec(ctx, `INSERT INTO worker_changes (payload) VALUES ($1)`, mkPayload("second-xid-early-commit")); err != nil {
		t.Fatalf("tx2 feed insert failed: %v", err)
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

// TestWorkerChangesPartitionRetention verifies partition-based
// retention: an hourly partition wholly past changeFeedRetentionAge is
// dropped (with its greatest xid recorded in worker_changes_trim), fresh
// rows survive, and aged strays in the DEFAULT partition are trimmed by the
// row-wise fallback.
func TestWorkerChangesPartitionRetention(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	// A partition two hours back, holding one aged event.
	stale := time.Now().UTC().Add(-2 * time.Hour)
	if err := s.createWorkerChangesPartitions(ctx, stale); err != nil {
		t.Fatalf("creating stale partition failed: %v", err)
	}
	var staleXid string
	if err := s.pool.QueryRow(ctx, `INSERT INTO worker_changes (payload, created_at) VALUES ($1, $2) RETURNING xid::text`,
		[]byte("old"), stale).Scan(&staleXid); err != nil {
		t.Fatalf("inserting aged row failed: %v", err)
	}
	// An aged stray in the DEFAULT partition (no hourly partition covers a
	// day ago), and a fresh row in the current partition.
	if _, err := s.pool.Exec(ctx, `INSERT INTO worker_changes (payload, created_at) VALUES ($1, now() - interval '1 day')`, []byte("stray")); err != nil {
		t.Fatalf("inserting default-partition stray failed: %v", err)
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO worker_changes (payload) VALUES ($1)`, []byte("fresh")); err != nil {
		t.Fatalf("inserting fresh row failed: %v", err)
	}

	if err := s.maintainWorkerChangesPartitions(ctx); err != nil {
		t.Fatalf("maintainWorkerChangesPartitions failed: %v", err)
	}

	var staleExists bool
	if err := s.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`,
		workerChangesPartitionName(stale)).Scan(&staleExists); err != nil {
		t.Fatalf("checking stale partition failed: %v", err)
	}
	if staleExists {
		t.Errorf("stale partition %s still exists, want dropped", workerChangesPartitionName(stale))
	}
	var remaining int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM worker_changes`).Scan(&remaining); err != nil {
		t.Fatalf("counting remaining rows failed: %v", err)
	}
	if remaining != 1 {
		t.Errorf("%d rows remain, want 1 (the fresh row; aged partition row and default stray gone)", remaining)
	}
	var trim bool
	if err := s.pool.QueryRow(ctx, `SELECT (SELECT xid FROM worker_changes_trim) >= $1::xid8`, staleXid).Scan(&trim); err != nil {
		t.Fatalf("reading trim mark failed: %v", err)
	}
	if !trim {
		t.Errorf("trim mark does not cover dropped partition's xid %s", staleXid)
	}
}

// TestChangeFeedMaintenance_SingleMaintainer verifies the retention
// election: while another replica holds the advisory lock, a pass skips
// retention cleanly (no error, nothing dropped); once released, the next
// pass does the work. (Partition creation is deliberately unelected.)
func TestChangeFeedMaintenance_SingleMaintainer(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	stale := time.Now().UTC().Add(-2 * time.Hour)
	if err := s.createWorkerChangesPartitions(ctx, stale); err != nil {
		t.Fatalf("creating stale partition failed: %v", err)
	}

	// Another "replica" mid-pass: hold the advisory lock in an open
	// transaction of our own.
	holder, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin holder failed: %v", err)
	}
	defer holder.Rollback(ctx) //nolint:errcheck // released below
	if _, err := holder.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext(current_database() || ':' || $1))`, changeFeedMaintenanceLockKey); err != nil {
		t.Fatalf("taking maintenance lock failed: %v", err)
	}

	if err := s.maintainWorkerChangesPartitions(ctx); err != nil {
		t.Fatalf("pass with lock held must skip cleanly, got: %v", err)
	}
	var staleExists bool
	if err := s.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, workerChangesPartitionName(stale)).Scan(&staleExists); err != nil {
		t.Fatalf("checking stale partition failed: %v", err)
	}
	if !staleExists {
		t.Fatal("stale partition was dropped by a pass that lost the election")
	}

	if err := holder.Rollback(ctx); err != nil {
		t.Fatalf("releasing maintenance lock failed: %v", err)
	}
	if err := s.maintainWorkerChangesPartitions(ctx); err != nil {
		t.Fatalf("pass after lock release failed: %v", err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, workerChangesPartitionName(stale)).Scan(&staleExists); err != nil {
		t.Fatalf("re-checking stale partition failed: %v", err)
	}
	if staleExists {
		t.Error("stale partition survived a pass that held the election")
	}
}

// TestWorkerEvents_OneRowPerTransaction pins the invariant the xid-only
// watch cursor rests on: writeAndAppendChange appends exactly one feed row
// per transaction, so xids are distinct across the feed and a poll batch
// can never split a same-xid group.
func TestWorkerEvents_OneRowPerTransaction(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	worker := &ateapipb.Worker{WorkerNamespace: "ns", WorkerPool: "pool", WorkerPod: "pod"}
	if err := s.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	for i := 0; i < 10; i++ {
		stored, err := s.GetWorker(ctx, "ns", "pool", "pod")
		if err != nil {
			t.Fatalf("GetWorker failed: %v", err)
		}
		if err := s.UpdateWorker(ctx, stored, stored.GetVersion()); err != nil {
			t.Fatalf("UpdateWorker %d failed: %v", i, err)
		}
	}
	if err := s.DeleteWorker(ctx, "ns", "pool", "pod"); err != nil {
		t.Fatalf("DeleteWorker failed: %v", err)
	}

	var total, distinct int
	if err := s.pool.QueryRow(ctx, `SELECT count(*), count(DISTINCT xid) FROM worker_changes`).Scan(&total, &distinct); err != nil {
		t.Fatalf("counting feed rows failed: %v", err)
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

	worker := &ateapipb.Worker{WorkerNamespace: "ns", WorkerPool: "pool", WorkerPod: "fenced"}
	if err := s.CreateWorker(ctx, worker); err != nil {
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
// boundaries) and forces full scans with a top-N sort. Behavioural tests
// cannot see this; the plan can.
func TestPollQueryPlanStaysOnIndex(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	// Seed enough rows (and stats) for the planner to have a real choice:
	// on empty partitions it costs bitmap scans plus an explicit Sort as
	// cheapest regardless of the index, which would make the Merge Append
	// assertion below vacuously unreachable.
	if _, err := s.pool.Exec(ctx, `INSERT INTO worker_changes (payload) SELECT 'x'::bytea FROM generate_series(1, 3000)`); err != nil {
		t.Fatalf("seeding feed rows failed: %v", err)
	}
	if _, err := s.pool.Exec(ctx, `ANALYZE worker_changes`); err != nil {
		t.Fatalf("ANALYZE failed: %v", err)
	}

	rows, err := s.pool.Query(ctx, "EXPLAIN "+pollWorkerChangesSQL, "100", changeFeedBatch)
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

// TestChangeFeedMaintenance_ConcurrentPassesAreHarmless backs the doc's
// claim: two replicas racing a maintenance pass produce no errors and the
// correct end state (one wins the election, the loser skips).
func TestChangeFeedMaintenance_ConcurrentPassesAreHarmless(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	replica, err := NewPersistence(ctx, s.pool)
	if err != nil {
		t.Fatalf("second Persistence failed: %v", err)
	}
	t.Cleanup(replica.Close)

	stale := time.Now().UTC().Add(-2 * time.Hour)
	if err := s.createWorkerChangesPartitions(ctx, stale); err != nil {
		t.Fatalf("creating stale partition failed: %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, p := range []*Persistence{s, replica} {
		wg.Add(1)
		go func(i int, p *Persistence) {
			defer wg.Done()
			errs[i] = p.maintainWorkerChangesPartitions(ctx)
		}(i, p)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("concurrent pass %d returned error: %v", i, err)
		}
	}
	var staleExists bool
	if err := s.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, workerChangesPartitionName(stale)).Scan(&staleExists); err != nil {
		t.Fatalf("checking stale partition failed: %v", err)
	}
	if staleExists {
		t.Error("stale partition survived both concurrent passes")
	}
}

// TestWorkerChangesPartitionsAreUnlogged pins the maintenance profile the
// schema documents: every feed partition must be UNLOGGED (relpersistence
// 'u') with autovacuum disabled (all are insert-only and discarded whole,
// by drop or truncate — an in-window insert-autovacuum is a measured p99
// spike); and worker_changes_trim — the loss-detection high-water mark —
// must remain logged so it survives a crash.
func TestWorkerChangesPartitionsAreUnlogged(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	rows, err := s.pool.Query(ctx, `
		SELECT c.relname, c.relpersistence, COALESCE(array_to_string(c.reloptions, ','), '') FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid
		JOIN pg_class parent ON parent.oid = i.inhparent
		WHERE parent.relname = 'worker_changes'`)
	if err != nil {
		t.Fatalf("listing feed partitions: %v", err)
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
		t.Fatal("no feed partitions found to check")
	}
	var trimPersistence string
	if err := s.pool.QueryRow(ctx, `SELECT relpersistence FROM pg_class WHERE relname = 'worker_changes_trim'`).Scan(&trimPersistence); err != nil {
		t.Fatalf("checking worker_changes_trim persistence: %v", err)
	}
	if trimPersistence != "p" {
		t.Errorf("worker_changes_trim has relpersistence %q, want 'p' (logged) — the trim mark must survive a crash", trimPersistence)
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
	worker := &ateapipb.Worker{WorkerNamespace: "ns", WorkerPool: "pool", WorkerPod: "pod"}
	if err := s.CreateWorker(ctx, worker); err != nil {
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
	if _, err := tx.Exec(ctx, `INSERT INTO worker_changes (payload) VALUES ($1), ($1), ($1)`, payload); err != nil {
		t.Fatalf("feed inserts failed: %v", err)
	}
	// Mirrors trimWorkerChangesDefault's shape: the mark is the deleted
	// set's greatest xid. (The three rows above share one transaction —
	// fine here: this test only needs the recorded mark to land past the
	// watcher's cursor, and deletes everything the watcher has not seen.)
	if _, err := tx.Exec(ctx, `
		WITH doomed AS (
			DELETE FROM worker_changes WHERE xid > (SELECT COALESCE((SELECT xid FROM worker_changes_trim), '0'::xid8))
			RETURNING xid
		)
		INSERT INTO worker_changes_trim (xid)
		SELECT xid FROM doomed ORDER BY xid DESC LIMIT 1
		ON CONFLICT (id) DO UPDATE SET xid = EXCLUDED.xid
		WHERE EXCLUDED.xid > worker_changes_trim.xid`); err != nil {
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

func TestListActors_InvalidPageToken(t *testing.T) {
	s := setupPostgresStore(t).(*Persistence)
	ctx := context.Background()

	if _, err := s.ListActors(ctx, "", store.ListOptions{PageSize: 10, PageToken: "not-valid-base64!!"}); err == nil {
		t.Errorf("ListActors with malformed page token = nil error, want an error")
	}
}

func TestDecodePageTokenRejectsWrongKeyShape(t *testing.T) {
	token := encodePageToken(kindActor, "", []string{"only-an-atespace"})
	if _, err := decodePageToken(token, kindActor, "", 2); err == nil {
		t.Fatal("decodePageToken() accepted a global actor token with only one key part")
	}
}

func TestListActors_CrossScopePageToken(t *testing.T) {
	s := setupPostgresStore(t).(*Persistence)
	ctx := context.Background()

	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("CreateAtespace(team-a) failed: %v", err)
	}
	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-b")); err != nil {
		t.Fatalf("CreateAtespace(team-b) failed: %v", err)
	}
	for _, name := range []string{"a1", "a2"} {
		if _, err := s.CreateActor(ctx, &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: name, Atespace: "team-a"}, Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED}}); err != nil {
			t.Fatalf("CreateActor failed: %v", err)
		}
	}

	page, err := s.ListActors(ctx, "team-a", store.ListOptions{PageSize: 1})
	if err != nil {
		t.Fatalf("ListActors(team-a) failed: %v", err)
	}
	if page.NextPageToken == "" {
		t.Fatalf("expected a next page token")
	}

	// A token minted for team-a must be rejected when replayed against team-b
	// or against the unscoped (global) listing.
	if _, err := s.ListActors(ctx, "team-b", store.ListOptions{PageSize: 1, PageToken: page.NextPageToken}); err == nil {
		t.Errorf("ListActors(team-b) with team-a's token = nil error, want an error")
	}
	if _, err := s.ListActors(ctx, "", store.ListOptions{PageSize: 1, PageToken: page.NextPageToken}); err == nil {
		t.Errorf("ListActors(all) with team-a's token = nil error, want an error")
	}

	// A worker-list token must be rejected by ListAtespaces (different kind).
	workerPage, err := s.ListWorkers(ctx, store.ListOptions{PageSize: 1})
	if err != nil {
		t.Fatalf("ListWorkers failed: %v", err)
	}
	if workerPage.NextPageToken != "" {
		if _, err := s.ListAtespaces(ctx, store.ListOptions{PageSize: 1, PageToken: workerPage.NextPageToken}); err == nil {
			t.Errorf("ListAtespaces with a worker page token = nil error, want an error")
		}
	}
}

func TestAcquireLock_ExpiresAfterHolderStops(t *testing.T) {
	s := setupPostgresPersistence(t)
	s.lockTTL = 200 * time.Millisecond
	holderCtx, cancelHolder := context.WithCancel(context.Background())
	lock, err := s.AcquireLock(holderCtx, "test-lock")
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}
	cancelHolder()
	select {
	case <-lock.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("lock context was not cancelled with its holder")
	}

	// Canceling the holder stops renewal without calling Close, modeling a
	// process that disappeared and left its lease to expire.
	time.Sleep(s.lockTTL + 500*time.Millisecond)

	newLock, err := s.AcquireLock(context.Background(), "test-lock")
	if err != nil {
		t.Fatalf("AcquireLock after lease expiration failed: %v", err)
	}
	newLock.Close()
}

// TestAcquireLock_ConcurrentTakeover races many goroutines to acquire an
// already-expired lease against the real database, and asserts exactly one
// wins -- the property the doc's conditional-upsert SQL is meant to
// guarantee under real concurrency, which a single-connection unit test
// can't exercise.
func TestAcquireLock_ConcurrentTakeover(t *testing.T) {
	s := setupPostgresPersistence(t)
	s.lockTTL = time.Millisecond
	holderCtx, cancelHolder := context.WithCancel(context.Background())
	initial, err := s.AcquireLock(holderCtx, "contested-lock")
	if err != nil {
		t.Fatalf("seeding initial lease failed: %v", err)
	}
	cancelHolder()
	<-initial.Context().Done()
	time.Sleep(50 * time.Millisecond) // let the 1ms lease expire.
	s.lockTTL = 10 * time.Second

	const numRacers = 20
	winners := make(chan *store.Lock, numRacers)
	var wg sync.WaitGroup
	for i := 0; i < numRacers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			lock, err := s.AcquireLock(context.Background(), "contested-lock")
			if err != nil {
				if !errors.Is(err, store.ErrLockConflict) {
					t.Errorf("AcquireLock racer %d failed: %v", i, err)
				}
				return
			}
			// Keep the winning lease held until every racer has attempted
			// acquisition. Releasing it here would let later racers win
			// sequentially rather than testing concurrent takeover.
			winners <- lock
		}(i)
	}
	wg.Wait()
	close(winners)

	if got := len(winners); got != 1 {
		t.Errorf("expected exactly 1 racer to win the expired lease, got %d", got)
	}
	for lock := range winners {
		lock.Close()
	}
}
