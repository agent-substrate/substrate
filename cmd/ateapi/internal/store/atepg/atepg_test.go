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
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/dockerenv"
	"github.com/agent-substrate/substrate/internal/resources"
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
		if err := dockerenv.Configure(ctx); err != nil {
			containerErr = err
			return
		}
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

func newTestAtespace(name string) *ateapipb.Atespace {
	return &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: name}}
}

func createTestAtespace(t *testing.T, s *Persistence, name string) {
	t.Helper()
	if _, err := s.CreateAtespace(context.Background(), newTestAtespace(name)); err != nil {
		t.Fatalf("CreateAtespace(%q) failed: %v", name, err)
	}
}

func createTestActorTemplate(t *testing.T, s *Persistence, atespace, name string) {
	t.Helper()
	if _, err := s.CreateActorTemplate(context.Background(), &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
	}); err != nil {
		t.Fatalf("CreateActorTemplate(%q/%q) failed: %v", atespace, name, err)
	}
}

func TestUpdateActor_ConcurrentWriteReturnsConflict(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	createTestAtespace(t, s, "team-a")
	created, err := s.CreateActor(ctx, &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-a"},
		ActorTemplateNamespace: "default",
		ActorTemplateName:      "template-a",
		Status:                 &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	actorRef := resources.ActorRefFromActor(created)

	mutations := 0
	_, err = s.UpdateActor(ctx, actorRef, store.PreconditionFrom(created), func(toUpdate *ateapipb.Actor) error {
		mutations++
		if _, err := s.UpdateActor(ctx, actorRef, store.PreconditionFrom(created), func(concurrent *ateapipb.Actor) error {
			concurrent.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}
			return nil
		}); err != nil {
			return fmt.Errorf("concurrent actor update: %w", err)
		}
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
		return nil
	})
	if !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("UpdateActor error = %v, want ErrVersionConflict", err)
	}
	if mutations != 1 {
		t.Errorf("mutation ran %d times, want 1", mutations)
	}
	stored, err := s.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if stored.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("state = %v, want SUSPENDED: losing update was persisted", stored.GetStatus().GetState())
	}
	if got := stored.GetWorkerSelector().GetMatchLabels()["tier"]; got != "paid" {
		t.Errorf("worker selector tier = %q, want paid", got)
	}
	if got, want := stored.GetMetadata().GetVersion(), created.GetMetadata().GetVersion()+1; got != want {
		t.Errorf("version = %d, want %d", got, want)
	}
}

func TestUpdateActorTemplate_ConcurrentWriteReturnsConflict(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	createTestAtespace(t, s, "team-a")
	createTestActorTemplate(t, s, "team-a", "template-a")
	templateRef := resources.ActorTemplateRef{Atespace: "team-a", Name: "template-a"}
	created, err := s.GetActorTemplate(ctx, templateRef)
	if err != nil {
		t.Fatalf("GetActorTemplate failed: %v", err)
	}

	mutations := 0
	_, err = s.UpdateActorTemplate(ctx, templateRef, store.PreconditionFrom(created), func(toUpdate *ateapipb.ActorTemplate) error {
		mutations++
		if _, err := s.UpdateActorTemplate(ctx, templateRef, store.PreconditionFrom(created), func(concurrent *ateapipb.ActorTemplate) error {
			concurrent.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}
			return nil
		}); err != nil {
			return fmt.Errorf("concurrent actor template update: %w", err)
		}
		toUpdate.Status = &ateapipb.ActorTemplateStatus{GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{
			ErrorMessage: "LosingUpdate",
		}}
		return nil
	})
	if !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("UpdateActorTemplate error = %v, want ErrVersionConflict", err)
	}
	if mutations != 1 {
		t.Errorf("mutation ran %d times, want 1", mutations)
	}
	stored, err := s.GetActorTemplate(ctx, templateRef)
	if err != nil {
		t.Fatalf("GetActorTemplate failed: %v", err)
	}
	if got := stored.GetWorkerSelector().GetMatchLabels()["tier"]; got != "paid" {
		t.Errorf("worker selector tier = %q, want paid", got)
	}
	if got := stored.GetStatus().GetGoldenSnapshotStatus().GetErrorMessage(); got != "" {
		t.Errorf("status error message = %q, want empty: losing update was persisted", got)
	}
}

func TestUpdateActorSnapshotTag_CASPreventsDeleteRecreateABA(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	createTestAtespace(t, s, "team-a")
	for _, name := range []string{"snapshot-a", "snapshot-b"} {
		if _, err := s.CreateActorSnapshot(ctx, &ateapipb.ActorSnapshot{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: name},
			Status:   &ateapipb.ActorSnapshotStatus{SnapshotUri: "gs://bucket/" + name},
		}); err != nil {
			t.Fatalf("CreateActorSnapshot(%q) failed: %v", name, err)
		}
	}
	original, err := s.CreateActorSnapshotTag(ctx, resources.ActorSnapshotRef{Atespace: "team-a", Name: "snapshot-a"}, &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "tag-a"},
		Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
	})
	if err != nil {
		t.Fatalf("CreateActorSnapshotTag failed: %v", err)
	}

	mutations := 0
	var recreated *ateapipb.ActorSnapshotTag
	_, err = s.UpdateActorSnapshotTag(ctx, resources.ActorSnapshotTagRef{Atespace: "team-a", Name: "tag-a"}, store.PreconditionFrom(original), func(toUpdate *ateapipb.ActorSnapshotTag) error {
		mutations++
		if _, err := s.DeleteActorSnapshotTag(ctx, resources.ActorSnapshotTagRef{Atespace: "team-a", Name: "tag-a"}); err != nil {
			return fmt.Errorf("deleting original tag: %w", err)
		}
		recreated, err = s.CreateActorSnapshotTag(ctx, resources.ActorSnapshotRef{Atespace: "team-a", Name: "snapshot-b"}, &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "tag-a"},
			Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
		})
		if err != nil {
			return fmt.Errorf("recreating tag: %w", err)
		}
		toUpdate.Scope = ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED
		return nil
	})
	if !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("UpdateActorSnapshotTag error = %v, want ErrVersionConflict", err)
	}
	if mutations != 1 {
		t.Errorf("guarded mutation ran %d times, want 1", mutations)
	}
	stored, err := s.GetActorSnapshotTag(ctx, resources.ActorSnapshotTagRef{Atespace: "team-a", Name: "tag-a"})
	if err != nil {
		t.Fatalf("GetActorSnapshotTag failed: %v", err)
	}
	if diff := cmp.Diff(recreated, stored, protocmp.Transform()); diff != "" {
		t.Errorf("recreated tag was overwritten (-want +got):\n%s", diff)
	}
}

func TestCreateActorSnapshotTag_ForeignKeyErrors(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	createTestAtespace(t, s, "team-a")
	tag := func() *ateapipb.ActorSnapshotTag {
		return &ateapipb.ActorSnapshotTag{Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "latest"}}
	}

	if _, err := s.CreateActorSnapshotTag(ctx, resources.ActorSnapshotRef{Atespace: "team-a", Name: "missing"}, tag()); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("missing snapshot error = %v, want ErrNotFound", err)
	}
	if _, err := s.CreateActorSnapshot(ctx, &ateapipb.ActorSnapshot{Metadata: &ateapipb.ResourceMetadata{Atespace: "gone", Name: "snapshot"}}); err != nil {
		t.Fatalf("CreateActorSnapshot: %v", err)
	}
	tagWithoutAtespace := tag()
	tagWithoutAtespace.Metadata.Atespace = "gone"
	if _, err := s.CreateActorSnapshotTag(ctx, resources.ActorSnapshotRef{Atespace: "gone", Name: "snapshot"}, tagWithoutAtespace); !errors.Is(err, store.ErrFailedPrecondition) {
		t.Errorf("missing tag atespace error = %v, want ErrFailedPrecondition", err)
	}
}

// TestAcquireLease_DoesNotSweepOtherKeys keeps garbage collection off the
// acquisition path: acquiring one key must touch only that key's row, never
// scan or delete rows belonging to other keys. Reclaiming those is the
// background maintenance sweep's job.
func TestAcquireLease_DoesNotSweepOtherKeys(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO leases (key, token, expires_at) VALUES
		('expired', 'old', clock_timestamp() - interval '1 minute'),
		('active', 'live', clock_timestamp() + interval '1 hour')`); err != nil {
		t.Fatalf("seeding leases: %v", err)
	}
	lease, err := s.AcquireLease(ctx, "new")
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	defer lease.Close()

	if _, ok := leaseToken(t, s, "expired"); !ok {
		t.Error("AcquireLease deleted an unrelated expired lease; the sweep belongs on the maintenance tick")
	}
	if _, ok := leaseToken(t, s, "active"); !ok {
		t.Error("AcquireLease deleted an unrelated live lease")
	}
}

// TestCreateActor_MissingAtespace_FailedPrecondition exercises the
// foreign-key race the doc calls out: CreateActor rejects an actor whose
// atespace doesn't exist (including a concurrently-deleted one), with the
// foreign key closing the TOCTOU window around a separate existence check.
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

func TestAcquireLease_ExpiresAfterHolderStops(t *testing.T) {
	s := setupPostgresPersistence(t)
	s.leaseTTL = 200 * time.Millisecond
	holderCtx, cancelHolder := context.WithCancel(context.Background())
	lease, err := s.AcquireLease(holderCtx, "test-lease")
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}
	cancelHolder()
	select {
	case <-lease.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("lease context was not cancelled with its holder")
	}

	// Canceling the holder stops renewal without calling Close, modeling a
	// process that disappeared and left its lease to expire.
	time.Sleep(s.leaseTTL + 500*time.Millisecond)

	newLease, err := s.AcquireLease(context.Background(), "test-lease")
	if err != nil {
		t.Fatalf("AcquireLease after lease expiration failed: %v", err)
	}
	newLease.Close()
}

// TestAcquireLease_TakesOverExpiredRowWithoutSweep pins the property that lets
// the expired-lease sweep live on the background maintenance tick instead of
// on AcquireLease, and so on the actor resume path: an expired row that is
// still present must be taken over by the acquire statement itself.
func TestAcquireLease_TakesOverExpiredRowWithoutSweep(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	s.leaseTTL = 200 * time.Millisecond

	holderCtx, cancelHolder := context.WithCancel(ctx)
	lease, err := s.AcquireLease(holderCtx, "stale-lease")
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}
	// Canceling without Close stops renewal and skips the release delete, so
	// the row is left behind to expire, as a vanished process leaves it.
	cancelHolder()
	<-lease.Context().Done()
	time.Sleep(s.leaseTTL + 300*time.Millisecond)

	firstToken, ok := leaseToken(t, s, "stale-lease")
	if !ok {
		t.Fatal("expired lease row is gone; the test no longer exercises takeover of an existing row")
	}

	newLease, err := s.AcquireLease(ctx, "stale-lease")
	if err != nil {
		t.Fatalf("AcquireLease over an unswept expired row failed: %v", err)
	}
	defer newLease.Close()

	secondToken, ok := leaseToken(t, s, "stale-lease")
	if !ok {
		t.Fatal("lease row missing after a successful acquisition")
	}
	if secondToken == firstToken {
		t.Errorf("lease token after takeover = %q, want a new token (the expired row was not overwritten)", secondToken)
	}
}

// TestSweepExpiredLeases checks the maintenance sweep discards expired rows
// and leaves a live lease alone -- deleting a live row would let a second
// holder acquire a key its owner still holds.
func TestSweepExpiredLeases(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	if _, err := s.pool.Exec(ctx, `
		INSERT INTO leases (key, token, expires_at) VALUES
			('expired-a', 'token-a', clock_timestamp() - interval '1 hour'),
			('expired-b', 'token-b', clock_timestamp() - interval '1 second'),
			('live',      'token-c', clock_timestamp() + interval '1 hour')`); err != nil {
		t.Fatalf("seeding leases failed: %v", err)
	}

	if err := s.sweepExpiredLeases(ctx); err != nil {
		t.Fatalf("sweepExpiredLeases failed: %v", err)
	}

	for _, key := range []string{"expired-a", "expired-b"} {
		if _, ok := leaseToken(t, s, key); ok {
			t.Errorf("lease %q survived the sweep, want it deleted", key)
		}
	}
	token, ok := leaseToken(t, s, "live")
	if !ok {
		t.Error("the sweep deleted an unexpired lease")
	} else if token != "token-c" {
		t.Errorf("live lease token = %q, want %q", token, "token-c")
	}

	// A pass over a table with nothing left to collect is a no-op.
	if err := s.sweepExpiredLeases(ctx); err != nil {
		t.Fatalf("sweepExpiredLeases on an already-swept table failed: %v", err)
	}
	if _, ok := leaseToken(t, s, "live"); !ok {
		t.Error("a second sweep deleted the unexpired lease")
	}
}

// TestSweepExpiredLeases_DrainsPastOneBatch checks a backlog larger than
// leaseSweepBatch -- what an ateapi replica dying mid-flight leaves behind --
// is reclaimed by a single pass rather than one batch per tick.
func TestSweepExpiredLeases_DrainsPastOneBatch(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	backlog := leaseSweepBatch + leaseSweepBatch/2
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO leases (key, token, expires_at)
		SELECT 'expired-' || i, 'token-' || i, clock_timestamp() - interval '1 hour'
		FROM generate_series(1, $1) AS i`, backlog); err != nil {
		t.Fatalf("seeding expired leases failed: %v", err)
	}

	if err := s.sweepExpiredLeases(ctx); err != nil {
		t.Fatalf("sweepExpiredLeases failed: %v", err)
	}

	var remaining int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM leases`).Scan(&remaining); err != nil {
		t.Fatalf("counting leases failed: %v", err)
	}
	if remaining != 0 {
		t.Errorf("leases remaining after one sweep pass = %d, want 0", remaining)
	}
}

// TestSweepExpiredLeases_ConcurrentTakeover pins why the sweep's DELETE keeps
// its expiry predicate on the leases table itself and not only in the
// bounding subquery: a sweep racing a takeover of the same expired key must
// never remove the row that takeover commits. Deleting it would let a third
// caller acquire a key whose new holder still believes it holds it.
func TestSweepExpiredLeases_ConcurrentTakeover(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	if _, err := s.pool.Exec(ctx, `
		INSERT INTO leases (key, token, expires_at)
		VALUES ('contended', 'stale', clock_timestamp() - interval '1 hour')`); err != nil {
		t.Fatalf("seeding expired lease failed: %v", err)
	}

	// Take the row over in a transaction left open. Its row lock is what the
	// sweep must either skip or, having blocked on it, re-check against the
	// committed tuple.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("beginning takeover transaction failed: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var takenKey string
	if err := tx.QueryRow(ctx, acquireLeaseSQL, "contended", "fresh", 3600.0).Scan(&takenKey); err != nil {
		t.Fatalf("taking over the expired lease failed: %v", err)
	}

	sweepDone := make(chan error, 1)
	go func() { sweepDone <- s.sweepExpiredLeases(ctx) }()

	// Give the sweep time to reach the row and skip or block on its lock.
	time.Sleep(500 * time.Millisecond)
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("committing the takeover failed: %v", err)
	}
	if err := <-sweepDone; err != nil {
		t.Fatalf("sweepExpiredLeases failed: %v", err)
	}

	token, ok := leaseToken(t, s, "contended")
	if !ok {
		t.Fatal("the sweep deleted a lease that was taken over while the sweep ran; two callers can now hold it")
	}
	if token != "fresh" {
		t.Errorf("lease token = %q, want %q", token, "fresh")
	}
}

// leaseToken reports the token stored for key, and whether the row exists.
func leaseToken(t *testing.T, s *Persistence, key string) (string, bool) {
	t.Helper()
	var token string
	err := s.pool.QueryRow(context.Background(), `SELECT token FROM leases WHERE key = $1`, key).Scan(&token)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false
	}
	if err != nil {
		t.Fatalf("reading lease %q failed: %v", key, err)
	}
	return token, true
}

// TestAcquireLease_ConcurrentTakeover races many goroutines to acquire an
// already-expired lease against the real database, and asserts exactly one
// wins -- the property the doc's conditional-upsert SQL is meant to
// guarantee under real concurrency, which a single-connection unit test
// can't exercise.
func TestAcquireLease_ConcurrentTakeover(t *testing.T) {
	s := setupPostgresPersistence(t)
	s.leaseTTL = time.Millisecond
	holderCtx, cancelHolder := context.WithCancel(context.Background())
	initial, err := s.AcquireLease(holderCtx, "contested-lease")
	if err != nil {
		t.Fatalf("seeding initial lease failed: %v", err)
	}
	cancelHolder()
	<-initial.Context().Done()
	time.Sleep(50 * time.Millisecond) // let the 1ms lease expire.
	s.leaseTTL = 10 * time.Second

	const numRacers = 20
	winners := make(chan *store.Lease, numRacers)
	var wg sync.WaitGroup
	for i := 0; i < numRacers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			lease, err := s.AcquireLease(context.Background(), "contested-lease")
			if err != nil {
				if !errors.Is(err, store.ErrLeaseConflict) {
					t.Errorf("AcquireLease racer %d failed: %v", i, err)
				}
				return
			}
			// Keep the winning lease held until every racer has attempted
			// acquisition. Releasing it here would let later racers win
			// sequentially rather than testing concurrent takeover.
			winners <- lease
		}(i)
	}
	wg.Wait()
	close(winners)

	if got := len(winners); got != 1 {
		t.Errorf("expected exactly 1 racer to win the expired lease, got %d", got)
	}
	for lease := range winners {
		lease.Close()
	}
}
