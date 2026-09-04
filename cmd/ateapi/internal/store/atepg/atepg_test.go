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
	"io/fs"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/dockerenv"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

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

	// Every migration applied once and no more: two racing starts must not each
	// record the same version.
	want, err := fs.Glob(migrationFiles, "migrations/*.sql")
	if err != nil {
		t.Fatalf("listing migrations: %v", err)
	}
	var applied int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM "concurrent-startup".schema_migrations WHERE version_id > 0 AND is_applied`).Scan(&applied); err != nil {
		t.Fatalf("reading applied migrations: %v", err)
	}
	if applied != len(want) {
		t.Fatalf("applied migration rows = %d, want %d", applied, len(want))
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

	if _, err := pool.Exec(ctx, `CREATE SCHEMA "migration-lock-wait"`); err != nil {
		t.Fatalf("creating migration schema: %v", err)
	}
	migrationPool, err := pgxpool.New(ctx, containerDSN+"&search_path="+schema)
	if err != nil {
		t.Fatalf("opening migration pool: %v", err)
	}
	t.Cleanup(migrationPool.Close)
	lockID, err := migrationLockID(ctx, migrationPool)
	if err != nil {
		t.Fatalf("generating migration lock ID: %v", err)
	}
	lockConn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquiring migration lock connection: %v", err)
	}
	locked := true
	t.Cleanup(func() {
		if locked {
			_, _ = lockConn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, lockID)
		}
		lockConn.Release()
	})
	if _, err := lockConn.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockID); err != nil {
		t.Fatalf("locking migrations: %v", err)
	}

	result := make(chan error, 1)
	go func() { result <- applyMigrations(ctx, migrationPool) }()
	select {
	case err := <-result:
		t.Fatalf("migration returned while its advisory lock was held: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	if _, err := lockConn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, lockID); err != nil {
		t.Fatalf("unlocking migrations: %v", err)
	}
	locked = false
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("migration failed after the advisory lock was released: %v", err)
		}
	case <-time.After(3 * time.Second):
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

	t.Run("ahead", func(t *testing.T) {
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
		if _, err := pool.Exec(ctx, `INSERT INTO "migration-ahead".schema_migrations (version_id, is_applied) VALUES (2, true)`); err != nil {
			t.Fatalf("setting ahead migration state: %v", err)
		}

		p, err = Connect(ctx, containerDSN, "migration-ahead")
		if err != nil {
			t.Fatalf("Connect with an ahead clean schema failed: %v", err)
		}
		p.Close()
		p.pool.Close()
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
		if err == nil || !strings.Contains(err.Error(), "Substrate tables exist without a migration ledger") {
			t.Fatalf("Connect error = %v, want unsupported schema error", err)
		}
		var metadataExists bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass('"migration-legacy".schema_migrations') IS NOT NULL`).Scan(&metadataExists); err != nil {
			t.Fatalf("checking migration metadata: %v", err)
		}
		if metadataExists {
			t.Error("Connect created a migration ledger for an unsupported schema")
		}
	})
}

func TestMigrationFailureLeavesCompletedPrefixAndResumes(t *testing.T) {
	pool := requirePool(t)
	ctx := t.Context()
	const schema = "migration-resume"
	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS "migration-resume" CASCADE; CREATE SCHEMA "migration-resume"`); err != nil {
		t.Fatalf("resetting schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS "migration-resume" CASCADE`)
	})

	migrationPool, err := pgxpool.New(ctx, containerDSN+"&search_path="+schema)
	if err != nil {
		t.Fatalf("opening migration pool: %v", err)
	}
	t.Cleanup(migrationPool.Close)
	files := fstest.MapFS{
		"000001_create.sql": {Data: []byte("-- +goose Up\nCREATE TABLE resume_test (id integer PRIMARY KEY);")},
		"000002_add_value.sql": {Data: []byte("-- +goose Up\n" +
			"ALTER TABLE resume_test ADD COLUMN value text;")},
		"000003_fail.sql": {Data: []byte("-- +goose Up\n" +
			"CREATE TABLE failed_transaction (id integer);\n" +
			"ALTER TABLE missing_table ADD COLUMN value text;")},
	}
	provider, err := openMigrationProvider(ctx, migrationPool, files)
	if err != nil {
		t.Fatalf("creating migration provider: %v", err)
	}
	if _, err := provider.UpTo(ctx, 1); err != nil {
		t.Fatalf("applying pre-run migration: %v", err)
	}
	migrationErr := migrateToLatest(ctx, provider)
	if migrationErr == nil {
		t.Fatal("migration succeeded, want version 3 failure")
	}
	var partial *goose.PartialError
	if !errors.As(migrationErr, &partial) {
		t.Fatalf("migration error = %v, want goose.PartialError", migrationErr)
	}
	if got := partial.Failed.Source.Version; got != 3 {
		t.Fatalf("failed migration version = %d, want 3", got)
	}
	if len(partial.Applied) != 1 || partial.Applied[0].Source.Version != 2 {
		t.Fatalf("migrations completed before failure = %#v, want version 2", partial.Applied)
	}
	if err := provider.Close(); err != nil {
		t.Fatalf("closing failed migration provider: %v", err)
	}

	var valueColumnExists bool
	if err := migrationPool.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = 'resume_test' AND column_name = 'value'
	)`, schema).Scan(&valueColumnExists); err != nil {
		t.Fatalf("checking completed migration: %v", err)
	}
	if !valueColumnExists {
		t.Error("migration 2 was not left applied")
	}
	var failedSQLApplied bool
	if err := migrationPool.QueryRow(ctx, `SELECT to_regclass('failed_transaction') IS NOT NULL`).Scan(&failedSQLApplied); err != nil {
		t.Fatalf("checking failed migration transaction: %v", err)
	}
	if failedSQLApplied {
		t.Error("SQL from failed migration 3 was committed")
	}
	if diff := cmp.Diff([]int64{1, 2}, appliedMigrationVersions(t, migrationPool)); diff != "" {
		t.Fatalf("applied migration versions after failure (-want +got):\n%s", diff)
	}

	files["000003_fail.sql"] = &fstest.MapFile{Data: []byte("-- +goose Up\nCREATE TABLE resumed_migration (id integer);")}
	resumedProvider, err := openMigrationProvider(ctx, migrationPool, files)
	if err != nil {
		t.Fatalf("creating resumed migration provider: %v", err)
	}
	t.Cleanup(func() {
		if err := resumedProvider.Close(); err != nil {
			t.Errorf("closing resumed migration provider: %v", err)
		}
	})
	if err := migrateToLatest(ctx, resumedProvider); err != nil {
		t.Fatalf("resuming migrations: %v", err)
	}
	if diff := cmp.Diff([]int64{1, 2, 3}, appliedMigrationVersions(t, migrationPool)); diff != "" {
		t.Fatalf("applied migration versions after resume (-want +got):\n%s", diff)
	}
	var resumed bool
	if err := migrationPool.QueryRow(ctx, `SELECT to_regclass('resumed_migration') IS NOT NULL`).Scan(&resumed); err != nil {
		t.Fatalf("checking resumed migration: %v", err)
	}
	if !resumed {
		t.Error("migration 3 was not applied after restart")
	}
}

func appliedMigrationVersions(t *testing.T, pool *pgxpool.Pool) []int64 {
	t.Helper()
	var versions []int64
	if err := pool.QueryRow(t.Context(), `
		SELECT COALESCE(array_agg(version_id ORDER BY version_id), '{}'::bigint[])
		FROM schema_migrations
		WHERE version_id > 0 AND is_applied`).Scan(&versions); err != nil {
		t.Fatalf("reading applied migration versions: %v", err)
	}
	return versions
}

// clearAll truncates every table so the next test starts from an empty store
// without paying for a fresh database. Nothing in production mass-deletes
// state, so the statement lives here rather than on Persistence.
func clearAll(t *testing.T, p *Persistence) {
	t.Helper()
	if _, err := p.pool.Exec(context.Background(), `TRUNCATE atespaces, actors, actor_egress_policies, actor_templates, actor_snapshots, actor_snapshot_tags, workers, worker_assignments, leases, worker_outbox, worker_outbox_trim`); err != nil {
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

func TestUpdateActor_ConcurrentWriteReturnsConflict(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	createTestAtespace(t, s, "team-a")
	created, err := s.CreateActor(ctx, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-a"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "default", Name: "template-a"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
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

// TestCreateActor_MissingAtespace_FailedPrecondition exercises the
// foreign-key race the doc calls out: CreateActor rejects an actor whose
// atespace doesn't exist (including a concurrently-deleted one), with the
// foreign key closing the TOCTOU window around a separate existence check.
func TestCreateActor_MissingAtespace_FailedPrecondition(t *testing.T) {
	s := setupPostgresStore(t).(*Persistence)
	ctx := context.Background()

	actor := &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Name: "id1", Atespace: "no-such-atespace"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns1", Name: "tmpl1"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
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
