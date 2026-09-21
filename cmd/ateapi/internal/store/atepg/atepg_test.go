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
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/dockerenv"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// testSnapshotOwnerUID stands in for the Actor UID an external snapshot's
// prefix is keyed on. The store keeps a snapshot URI opaque, so the tests only
// need URIs of the right shape, not ones an actual actor wrote.
const testSnapshotOwnerUID = "6b1f9d0c-4a2e-4d38-9c77-5e0a1b2c3d4e"

// One Postgres container serves every test in this package; each test gets
// isolation via clearAll rather than a fresh container, which would be
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

// clearAll truncates every table so the next test starts from an empty store
// without paying for a fresh database. Nothing in production mass-deletes
// state, so the statement lives here rather than on Persistence.
func clearAll(t *testing.T, p *Persistence) {
	t.Helper()
	if _, err := p.pool.Exec(context.Background(), `TRUNCATE atespaces, actors, actor_egress_policies, actor_templates, tags, workers, worker_assignments, leases, worker_outbox, worker_outbox_trim`); err != nil {
		t.Fatalf("truncating tables: %v", err)
	}
}

func setupPostgresPersistence(t *testing.T) *Persistence {
	t.Helper()
	ctx := context.Background()
	p, err := NewPersistence(ctx, requirePool(t))
	if err != nil {
		t.Fatalf("NewPersistence failed: %v", err)
	}
	t.Cleanup(p.Close)
	clearAll(t, p)
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

// createTestSuspendedActor seeds an actor holding an external snapshot, which
// is what CreateTag tags.
func createTestSuspendedActor(t *testing.T, s *Persistence, atespace, name string) *ateapipb.Actor {
	t.Helper()
	created, err := s.CreateActor(context.Background(), &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "default", Name: "template-a"},
		Status: &ateapipb.ActorStatus{
			State:            ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			ExternalSnapshot: &ateapipb.ExternalSnapshot{SnapshotUri: "gs://bucket/atespaces/" + atespace + "/actors/" + testSnapshotOwnerUID + "/snapshots/" + name, ContentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL},
		},
	})
	if err != nil {
		t.Fatalf("CreateActor(%s/%s) failed: %v", atespace, name, err)
	}
	return created
}

// createTestTag creates tagName over its own copy of actor's
// external snapshot, already finalized.
func createTestTag(t *testing.T, s *Persistence, actor *ateapipb.Actor, tagAtespace, tagName string) *ateapipb.Tag {
	t.Helper()
	tag, err := s.CreateTag(context.Background(), &ateapipb.Tag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: tagAtespace, Name: tagName},
		Scope:    ateapipb.TagScope_TAG_SCOPE_ATESPACE,
		Status: &ateapipb.TagStatus{
			Snapshot:       &ateapipb.ExternalSnapshot{SnapshotUri: "gs://bucket/atespaces/" + tagAtespace + "/tags/" + tagName},
			SourceActorUid: actor.GetMetadata().GetUid(),
		},
	})
	if err != nil {
		t.Fatalf("CreateTag(%s/%s) failed: %v", tagAtespace, tagName, err)
	}
	return tag
}

func TestUpdateTag_CASPreventsDeleteRecreateABA(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	createTestAtespace(t, s, "team-a")
	actorA := createTestSuspendedActor(t, s, "team-a", "actor-a")
	actorB := createTestSuspendedActor(t, s, "team-a", "actor-b")
	original := createTestTag(t, s, actorA, "team-a", "tag-a")

	mutations := 0
	var recreated *ateapipb.Tag
	_, err := s.UpdateTag(ctx, resources.TagRef{Atespace: "team-a", Name: "tag-a"}, store.PreconditionFrom(original), func(toUpdate *ateapipb.Tag) error {
		mutations++
		if _, err := s.DeleteTag(ctx, resources.TagRef{Atespace: "team-a", Name: "tag-a"}); err != nil {
			return fmt.Errorf("deleting original tag: %w", err)
		}
		recreated = createTestTag(t, s, actorB, "team-a", "tag-a")
		toUpdate.Scope = ateapipb.TagScope_TAG_SCOPE_PUBLISHED
		return nil
	})
	if !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("UpdateTag error = %v, want ErrVersionConflict", err)
	}
	if mutations != 1 {
		t.Errorf("guarded mutation ran %d times, want 1", mutations)
	}
	stored, err := s.GetTag(ctx, resources.TagRef{Atespace: "team-a", Name: "tag-a"})
	if err != nil {
		t.Fatalf("GetTag failed: %v", err)
	}
	if diff := cmp.Diff(recreated, stored, protocmp.Transform()); diff != "" {
		t.Errorf("recreated tag was overwritten (-want +got):\n%s", diff)
	}
}

func TestCreateTag_TagForeignKeyErrors(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	createTestAtespace(t, s, "team-a")
	actor := createTestSuspendedActor(t, s, "team-a", "actor-a")

	// A tag in an atespace that does not exist trips the tag's atespace FK.
	_, err := s.CreateTag(ctx, &ateapipb.Tag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "gone", Name: "latest"},
		Status: &ateapipb.TagStatus{
			StorageLocation: "gs://bucket",
			SourceActorUid:  actor.GetMetadata().GetUid(),
		},
	})
	if !errors.Is(err, store.ErrFailedPrecondition) {
		t.Errorf("missing tag atespace error = %v, want ErrFailedPrecondition", err)
	}
}

func TestAcquireLease_CleansExpiredLeases(t *testing.T) {
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

	var expired, active int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM leases WHERE key = 'expired'`).Scan(&expired); err != nil {
		t.Fatalf("counting expired lease: %v", err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM leases WHERE key = 'active'`).Scan(&active); err != nil {
		t.Fatalf("counting active lease: %v", err)
	}
	if expired != 0 || active != 1 {
		t.Errorf("lease counts = expired:%d active:%d, want 0 and 1", expired, active)
	}
}

func TestDecodePageTokenRejectsWrongKeyShape(t *testing.T) {
	token := encodePageToken(kindActor, "", []string{"only-an-atespace"})
	if _, err := decodePageToken(token, kindActor, "", 2); err == nil {
		t.Fatal("decodePageToken() accepted a global actor token with only one key part")
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

// TestSaveWorker_RejectsAStaleWrite proves the precondition saveWorker states
// on top of the row lock its callers hold: a Worker read before someone else
// wrote it cannot overwrite that write.
func TestSaveWorker_RejectsAStaleWrite(t *testing.T) {
	requirePool(t)
	ctx := context.Background()

	p, err := Connect(ctx, containerDSN, "public")
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer p.pool.Close()
	defer p.Close()
	clearAll(t, p)

	created, err := p.CreateWorker(ctx, &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "stale-write-worker"},
		WorkerNamespace: "ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod",
	})
	if err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}

	// Move the stored Worker on, so the copy above is a version behind.
	if _, err := p.UpdateWorker(ctx, created.GetMetadata().GetName(), store.PreconditionFrom(created), func(toUpdate *ateapipb.Worker) error {
		toUpdate.Ip = "10.0.0.1"
		return nil
	}); err != nil {
		t.Fatalf("UpdateWorker failed: %v", err)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)
	if err := saveWorker(ctx, tx, created); !errors.Is(err, store.ErrVersionConflict) {
		t.Errorf("saveWorker() with a stale Worker = %v, want ErrVersionConflict", err)
	}
}

// TestSaveWorker_RejectsAVanishedWorker keeps a deleted row from being an
// update of nothing.
func TestSaveWorker_RejectsAVanishedWorker(t *testing.T) {
	requirePool(t)
	ctx := context.Background()

	p, err := Connect(ctx, containerDSN, "public")
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer p.pool.Close()
	defer p.Close()
	clearAll(t, p)

	created, err := p.CreateWorker(ctx, &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "vanished-worker"},
		WorkerNamespace: "ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod",
	})
	if err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	if _, err := p.DeleteWorker(ctx, created.GetMetadata().GetName(), store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteWorker failed: %v", err)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)
	if err := saveWorker(ctx, tx, created); !errors.Is(err, store.ErrVersionConflict) {
		t.Errorf("saveWorker() on a deleted Worker = %v, want ErrVersionConflict", err)
	}
}
