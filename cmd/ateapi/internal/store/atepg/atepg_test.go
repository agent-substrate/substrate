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

	"github.com/golang-migrate/migrate/v4"
	migratedatabase "github.com/golang-migrate/migrate/v4/database"
	migratepgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
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

func TestMigrationsConcurrentStartup(t *testing.T) {
	pool := requirePool(t)
	ctx := t.Context()
	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS "concurrent-startup" CASCADE`); err != nil {
		t.Fatalf("resetting PostgreSQL schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS "concurrent-startup" CASCADE`)
	})

	errs := make(chan error, 2)
	for range 2 {
		go func() {
			p, err := Connect(ctx, containerDSN, "concurrent-startup")
			if p != nil {
				p.Close()
				p.pool.Close()
			}
			errs <- err
		}()
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("Connect failed: %v", err)
		}
	}

	var version int
	var dirty bool
	if err := pool.QueryRow(ctx, `SELECT version, dirty FROM "concurrent-startup".schema_migrations`).Scan(&version, &dirty); err != nil {
		t.Fatalf("reading migration version: %v", err)
	}
	if version != 1 || dirty {
		t.Fatalf("migration state = (%d, %t), want (1, false)", version, dirty)
	}
}

func TestMigrationsWaitForInProgressMigration(t *testing.T) {
	pool := requirePool(t)
	ctx := t.Context()
	const schema = "migration-lock-wait"
	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS "migration-lock-wait" CASCADE`); err != nil {
		t.Fatalf("resetting schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS "migration-lock-wait" CASCADE`)
	})

	p, err := Connect(ctx, containerDSN, schema)
	if err != nil {
		t.Fatalf("creating current schema: %v", err)
	}
	p.Close()
	p.pool.Close()

	migrationPool, err := pgxpool.New(ctx, containerDSN+"&search_path="+schema)
	if err != nil {
		t.Fatalf("opening migration pool: %v", err)
	}
	t.Cleanup(migrationPool.Close)
	migrator, _, err := openMigrator(migrationPool)
	if err != nil {
		t.Fatalf("opening migrator: %v", err)
	}
	t.Cleanup(func() {
		sourceErr, databaseErr := migrator.Close()
		if err := errors.Join(sourceErr, databaseErr); err != nil {
			t.Errorf("closing migrator: %v", err)
		}
	})

	lockConn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquiring migration lock connection: %v", err)
	}
	var databaseName string
	if err := lockConn.QueryRow(ctx, `SELECT current_database()`).Scan(&databaseName); err != nil {
		lockConn.Release()
		t.Fatalf("reading database name: %v", err)
	}
	lockID, err := migratedatabase.GenerateAdvisoryLockId(databaseName, schema, migratepgx.DefaultMigrationsTable)
	if err != nil {
		lockConn.Release()
		t.Fatalf("generating migration lock ID: %v", err)
	}
	locked := true
	t.Cleanup(func() {
		if locked {
			_, _ = lockConn.Exec(context.Background(), `SELECT pg_advisory_unlock($1::text::bigint)`, lockID)
		}
		lockConn.Release()
	})
	if _, err := lockConn.Exec(ctx, `SELECT pg_advisory_lock($1::text::bigint)`, lockID); err != nil {
		t.Fatalf("locking migrations: %v", err)
	}
	if _, err := lockConn.Exec(ctx, `UPDATE "migration-lock-wait".schema_migrations SET dirty = true`); err != nil {
		t.Fatalf("marking migration in progress: %v", err)
	}

	result := make(chan error, 1)
	go func() { result <- migrateToLatest(ctx, migrator, 1) }()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case err := <-result:
			t.Fatalf("migration returned before the advisory lock was released: %v", err)
		case <-ticker.C:
			var waiting bool
			if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted)`).Scan(&waiting); err != nil {
				t.Fatalf("checking migration lock wait: %v", err)
			}
			if waiting {
				goto release
			}
		case <-timeout.C:
			t.Fatal("migration did not wait for the advisory lock")
		}
	}

release:
	if _, err := lockConn.Exec(ctx, `UPDATE "migration-lock-wait".schema_migrations SET dirty = false`); err != nil {
		t.Fatalf("marking migration complete: %v", err)
	}
	if _, err := lockConn.Exec(ctx, `SELECT pg_advisory_unlock($1::text::bigint)`, lockID); err != nil {
		t.Fatalf("unlocking migrations: %v", err)
	}
	locked = false
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("migration failed after the advisory lock was released: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("migration did not finish after the advisory lock was released")
	}
}

func TestConnectUsesConfiguredSchema(t *testing.T) {
	pool := requirePool(t)
	ctx := t.Context()
	const schema = "substrate-test"
	if _, err := pool.Exec(ctx, `
		DROP SCHEMA IF EXISTS "substrate-test" CASCADE;
		DROP SCHEMA IF EXISTS "substrate-other-test" CASCADE;
		CREATE SCHEMA "substrate-other-test";
		CREATE TABLE "substrate-other-test".worker_outbox (
			created_at timestamptz NOT NULL
		) PARTITION BY RANGE (created_at);
		CREATE TABLE "substrate-other-test".worker_outbox_p200001010000
			PARTITION OF "substrate-other-test".worker_outbox
			FOR VALUES FROM ('2000-01-01 00:00:00+00') TO ('2000-01-01 00:05:00+00');
		CREATE TABLE IF NOT EXISTS public.substrate_schema_test_marker (id integer)`); err != nil {
		t.Fatalf("preparing schema test: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `
			DROP SCHEMA IF EXISTS "substrate-test" CASCADE;
			DROP SCHEMA IF EXISTS "substrate-other-test" CASCADE;
			DROP TABLE IF EXISTS public.substrate_schema_test_marker`)
	})

	dsn, err := containerPG.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("getting PostgreSQL connection string: %v", err)
	}
	persistence, err := Connect(ctx, dsn+"&search_path=public", schema)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer persistence.pool.Close()
	defer persistence.Close()

	if _, err := persistence.CreateAtespace(ctx, newTestAtespace("schema-test")); err != nil {
		t.Fatalf("creating atespace in configured schema: %v", err)
	}

	for _, table := range []string{"atespaces", "schema_migrations"} {
		var exists bool
		if err := persistence.pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.tables
				WHERE table_schema = $1 AND table_name = $2
			)`, schema, table).Scan(&exists); err != nil {
			t.Fatalf("checking %s.%s: %v", schema, table, err)
		}
		if !exists {
			t.Errorf("expected %s.%s to exist", schema, table)
		}
	}

	var markerExists bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('public.substrate_schema_test_marker') IS NOT NULL`).Scan(&markerExists); err != nil {
		t.Fatalf("checking unrelated table: %v", err)
	}
	if !markerExists {
		t.Error("migration removed an unrelated table")
	}

	if err := persistence.dropExpiredWorkerOutboxPartitions(ctx, persistence.watchPool, time.Now()); err != nil {
		t.Fatalf("dropping expired partitions in the configured schema: %v", err)
	}
	var unrelatedPartitionExists bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('"substrate-other-test".worker_outbox_p200001010000') IS NOT NULL`).Scan(&unrelatedPartitionExists); err != nil {
		t.Fatalf("checking unrelated outbox partition: %v", err)
	}
	if !unrelatedPartitionExists {
		t.Error("outbox maintenance removed a partition from another schema")
	}
}

func TestMigrationSchemaStates(t *testing.T) {
	pool := requirePool(t)
	ctx := t.Context()

	t.Run("ahead and clean", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS "migration-ahead" CASCADE`); err != nil {
			t.Fatalf("resetting schema: %v", err)
		}
		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS "migration-ahead" CASCADE`)
		})

		p, err := Connect(ctx, containerDSN, "migration-ahead")
		if err != nil {
			t.Fatalf("creating current schema: %v", err)
		}
		p.Close()
		p.pool.Close()
		if _, err := pool.Exec(ctx, `UPDATE "migration-ahead".schema_migrations SET version = 2, dirty = false`); err != nil {
			t.Fatalf("setting ahead migration state: %v", err)
		}

		p, err = Connect(ctx, containerDSN, "migration-ahead")
		if err != nil {
			t.Fatalf("Connect with an ahead clean schema failed: %v", err)
		}
		p.Close()
		p.pool.Close()
	})

	t.Run("dirty", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS "migration-dirty" CASCADE`); err != nil {
			t.Fatalf("resetting schema: %v", err)
		}
		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS "migration-dirty" CASCADE`)
		})

		p, err := Connect(ctx, containerDSN, "migration-dirty")
		if err != nil {
			t.Fatalf("creating current schema: %v", err)
		}
		p.Close()
		p.pool.Close()
		if _, err := pool.Exec(ctx, `UPDATE "migration-dirty".schema_migrations SET dirty = true`); err != nil {
			t.Fatalf("setting dirty migration state: %v", err)
		}

		_, err = Connect(ctx, containerDSN, "migration-dirty")
		var dirtyErr migrate.ErrDirty
		if !errors.As(err, &dirtyErr) {
			t.Fatalf("Connect error = %v, want migrate.ErrDirty", err)
		}
	})

	t.Run("tables without metadata", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `
			DROP SCHEMA IF EXISTS "migration-legacy" CASCADE;
			CREATE SCHEMA "migration-legacy";
			CREATE TABLE "migration-legacy".atespaces (id integer)`); err != nil {
			t.Fatalf("creating legacy schema: %v", err)
		}
		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS "migration-legacy" CASCADE`)
		})

		_, err := Connect(ctx, containerDSN, "migration-legacy")
		if err == nil || !strings.Contains(err.Error(), "Substrate tables exist without migration metadata") {
			t.Fatalf("Connect error = %v, want unsupported schema error", err)
		}
		var metadataExists bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass('"migration-legacy".schema_migrations') IS NOT NULL`).Scan(&metadataExists); err != nil {
			t.Fatalf("checking migration metadata: %v", err)
		}
		if metadataExists {
			t.Error("Connect created migration metadata for an unsupported schema")
		}
	})
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
		toUpdate.Status = &ateapipb.ActorTemplateStatus{Message: "ready"}
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
	if got := stored.GetStatus().GetMessage(); got != "" {
		t.Errorf("status message = %q, want empty: losing update was persisted", got)
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
	original, err := s.CreateActorSnapshotTag(ctx, "team-a", "snapshot-a", &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "tag-a"},
		Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
	})
	if err != nil {
		t.Fatalf("CreateActorSnapshotTag failed: %v", err)
	}

	mutations := 0
	var recreated *ateapipb.ActorSnapshotTag
	_, err = s.UpdateActorSnapshotTag(ctx, "team-a", "tag-a", store.PreconditionFrom(original), func(toUpdate *ateapipb.ActorSnapshotTag) error {
		mutations++
		if _, err := s.DeleteActorSnapshotTag(ctx, "team-a", "tag-a"); err != nil {
			return fmt.Errorf("deleting original tag: %w", err)
		}
		recreated, err = s.CreateActorSnapshotTag(ctx, "team-a", "snapshot-b", &ateapipb.ActorSnapshotTag{
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
	stored, err := s.GetActorSnapshotTag(ctx, "team-a", "tag-a")
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

	if _, err := s.CreateActorSnapshotTag(ctx, "team-a", "missing", tag()); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("missing snapshot error = %v, want ErrNotFound", err)
	}
	if _, err := s.CreateActorSnapshot(ctx, &ateapipb.ActorSnapshot{Metadata: &ateapipb.ResourceMetadata{Atespace: "gone", Name: "snapshot"}}); err != nil {
		t.Fatalf("CreateActorSnapshot: %v", err)
	}
	tagWithoutAtespace := tag()
	tagWithoutAtespace.Metadata.Atespace = "gone"
	if _, err := s.CreateActorSnapshotTag(ctx, "gone", "snapshot", tagWithoutAtespace); !errors.Is(err, store.ErrFailedPrecondition) {
		t.Errorf("missing tag atespace error = %v, want ErrFailedPrecondition", err)
	}
}

func TestAcquireLock_CleansExpiredLeases(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO leases (key, token, expires_at) VALUES
		('expired', 'old', clock_timestamp() - interval '1 minute'),
		('active', 'live', clock_timestamp() + interval '1 hour')`); err != nil {
		t.Fatalf("seeding leases: %v", err)
	}
	lock, err := s.AcquireLock(ctx, "new")
	if err != nil {
		t.Fatalf("AcquireLock: %v", err)
	}
	defer lock.Close()

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
