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

package authz

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/server"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/grpc/codes"

	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/atepg/pgfga"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/dockerenv"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/principal"
)

func configureDockerEnv(ctx context.Context) error {
	if os.Getenv("DOCKER_HOST") != "" {
		return nil
	}
	output, err := exec.CommandContext(ctx, "docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}").Output()
	if err != nil {
		return err
	}
	host := strings.TrimSpace(string(output))
	if host == "" {
		return nil
	}
	_ = os.Setenv("DOCKER_HOST", host)
	if os.Getenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE") == "" {
		socket := host
		if runtime.GOOS == "darwin" {
			socket = "/var/run/docker.sock"
		}
		_ = os.Setenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", socket)
	}
	return nil
}

func startPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	// Without Docker the tests skip locally, but must fail in CI, where a
	// skip would pass the package without running it.
	unavailable := t.Skipf
	if dockerenv.Required() {
		unavailable = t.Fatalf
	}
	if err := configureDockerEnv(ctx); err != nil {
		unavailable("docker is unavailable: %v", err)
	}

	pgContainer, err := postgres.Run(ctx,
		"postgres:18-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		unavailable("failed to start postgres container: %v", err)
	}
	t.Cleanup(func() {
		_ = testcontainers.TerminateContainer(pgContainer)
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("getting connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("creating pgxpool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
	})

	var pingErr error
	for i := 0; i < 30; i++ {
		pingErr = pool.Ping(ctx)
		if pingErr == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if pingErr != nil {
		t.Fatalf("timed out waiting for postgres ping: %v", pingErr)
	}

	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()
	provider, err := goose.NewProvider(goose.DialectPostgres, db, os.DirFS("../store/atepg/migrations"))
	if err != nil {
		t.Fatalf("create goose provider: %v", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		t.Fatalf("run Substrate and OpenFGA migrations: %v", err)
	}
	return pool
}

// testLocker is an InitLocker whose leases exclude each other within one
// process.
type testLocker struct{ mu sync.Mutex }

func (l *testLocker) AcquireLease(ctx context.Context, _ string) (*store.Lease, error) {
	if !l.mu.TryLock() {
		return nil, store.ErrLeaseConflict
	}
	ctx, cancel := context.WithCancel(ctx)
	return store.NewLease(ctx, func() {
		cancel()
		l.mu.Unlock()
	}), nil
}

// newTestFGAServer returns an OpenFGA server on the pgfga datastore over pool.
func newTestFGAServer(t *testing.T, pool *pgxpool.Pool) *server.Server {
	t.Helper()
	ds, err := pgfga.New(pool)
	if err != nil {
		t.Fatalf("pgfga.New failed: %v", err)
	}
	fgaSrv, err := NewOpenFGAServer(ds)
	if err != nil {
		t.Fatalf("NewOpenFGAServer failed: %v", err)
	}
	t.Cleanup(fgaSrv.Close)
	return fgaSrv
}

func newTestAuthz(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (*server.Server, string, string) {
	t.Helper()
	fgaSrv := newTestFGAServer(t, pool)
	storeID, modelID, err := EnsureStoreAndModel(ctx, &testLocker{}, fgaSrv)
	if err != nil {
		t.Fatalf("EnsureStoreAndModel failed: %v", err)
	}
	return fgaSrv, storeID, modelID
}

func TestEnsureStoreAndModel_NilArgs(t *testing.T) {
	if _, err := NewOpenFGAServer(nil); err == nil {
		t.Fatal("expected error when datastore is nil in NewOpenFGAServer")
	}
	if _, _, err := EnsureStoreAndModel(context.Background(), nil, nil); err == nil {
		t.Fatal("expected error when initLock is nil in EnsureStoreAndModel")
	}
}

func TestAcquireInitLock_WaitsForHolder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		locker := &testLocker{}
		held, err := locker.AcquireLease(t.Context(), initLeaseKey)
		if err != nil {
			t.Fatalf("AcquireLease failed: %v", err)
		}
		done := make(chan error, 1)
		go func() {
			lease, err := acquireInitLock(t.Context(), locker)
			if err == nil {
				lease.Close()
			}
			done <- err
		}()
		time.Sleep(3 * initLeaseRetryPeriod)
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("acquireInitLock returned %v while the lease was held", err)
		default:
		}
		held.Close()
		if err := <-done; err != nil {
			t.Fatalf("acquireInitLock after release failed: %v", err)
		}
	})
}

func TestAcquireInitLock_StopsWithContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		locker := &testLocker{}
		held, err := locker.AcquireLease(t.Context(), initLeaseKey)
		if err != nil {
			t.Fatalf("AcquireLease failed: %v", err)
		}
		defer held.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 3*initLeaseRetryPeriod)
		defer cancel()
		if _, err := acquireInitLock(ctx, locker); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("acquireInitLock = %v, want %v", err, context.DeadlineExceeded)
		}
	})
}

func TestEnsureStoreAndModel_InitializeAndCheck(t *testing.T) {
	pool := startPostgres(t)
	ctx := context.Background()

	fgaSrv, storeID, modelID := newTestAuthz(t, ctx, pool)

	if storeID == "" {
		t.Fatal("expected non-empty storeID")
	}
	if modelID == "" {
		t.Fatal("expected non-empty modelID")
	}

	// Write relationship tuples inside a transaction and verify authorization checks against the model.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("pool.Begin failed: %v", err)
	}
	_, err = fgaSrv.Write(pgfga.ContextWithTx(ctx, tx), &openfgav1.WriteRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				{
					User:     "user:alice",
					Relation: "owner",
					Object:   "global:root",
				},
				{
					User:     "global:root",
					Relation: "parent_global",
					Object:   "atespace:space-1",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Write tuples failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("tx.Commit failed: %v", err)
	}

	checkResp, err := fgaSrv.Check(ctx, &openfgav1.CheckRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		TupleKey: &openfgav1.CheckRequestTupleKey{
			User:     "user:alice",
			Relation: "can_update_access_policy",
			Object:   "atespace:space-1",
		},
	})
	if err != nil {
		t.Fatalf("Check alice can_update_access_policy failed: %v", err)
	}
	if !checkResp.GetAllowed() {
		t.Errorf("expected alice to be allowed can_update_access_policy on atespace:space-1 via global owner inheritance")
	}

	checkBob, err := fgaSrv.Check(ctx, &openfgav1.CheckRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		TupleKey: &openfgav1.CheckRequestTupleKey{
			User:     "user:bob",
			Relation: "can_update_access_policy",
			Object:   "atespace:space-1",
		},
	})
	if err != nil {
		t.Fatalf("Check bob can_update_access_policy failed: %v", err)
	}
	if checkBob.GetAllowed() {
		t.Errorf("expected bob to be denied can_update_access_policy on atespace:space-1")
	}

	// Verify idempotent re-initialization on the same shared pool reuses the existing store and model.
	_, storeID2, modelID2 := newTestAuthz(t, ctx, pool)

	if storeID2 != storeID {
		t.Errorf("expected same storeID %q on re-init, got %q", storeID, storeID2)
	}
	if modelID2 != modelID {
		t.Errorf("expected same modelID %q on re-init, got %q", modelID, modelID2)
	}
}

func writeTestTuple(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pm *PolicyManager, user, relation, object string) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("pool.Begin failed: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := pm.fgaServer.Write(pgfga.ContextWithTx(ctx, tx), &openfgav1.WriteRequest{
		StoreId:              pm.storeID,
		AuthorizationModelId: pm.modelID,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				{
					User:     formatUser(user),
					Relation: relation,
					Object:   object,
				},
			},
			OnDuplicate: "ignore",
		},
	}); err != nil {
		t.Fatalf("writeTestTuple(%s, %s, %s) failed: %v", user, relation, object, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("tx.Commit failed: %v", err)
	}
}

func TestAuthorizerAndPolicyManager_RuntimeChecks(t *testing.T) {
	pool := startPostgres(t)
	ctx := context.Background()

	fgaSrv := newTestFGAServer(t, pool)

	authorizer, policyManager, err := New(ctx, &testLocker{}, fgaSrv, nil, testSystemPrincipals())
	if err != nil {
		t.Fatalf("authz.New failed: %v", err)
	}

	// 1. Seed global owners (including a Kubernetes ServiceAccount ID with colons).
	writeTestTuple(t, ctx, pool, policyManager, "alice", "owner", GlobalRootObject)
	writeTestTuple(t, ctx, pool, policyManager, "system:serviceaccount:default:default", "owner", GlobalRootObject)

	// 1b. A nil Authorizer fails closed with codes.Internal unless explicitly bypassed.
	var nilAuthorizer *Authorizer
	if err := nilAuthorizer.Check(ctx, RelationCanCreateAtespace, GlobalRootObject); apierror.Code(err) != codes.Internal {
		t.Errorf("expected Internal for nil Authorizer, got %v", err)
	}
	if err := nilAuthorizer.Check(WithBypass(ctx), RelationCanCreateAtespace, GlobalRootObject); err != nil {
		t.Errorf("expected bypass context on nil Authorizer to succeed, got %v", err)
	}

	// 2. Unauthenticated request (no PrincipalInfo) fails with Unauthenticated unless bypassed.
	if err := authorizer.Check(ctx, RelationCanCreateAtespace, GlobalRootObject); apierror.Code(err) != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated for empty context, got %v", err)
	}
	if err := authorizer.Check(WithBypass(ctx), RelationCanCreateAtespace, GlobalRootObject); err != nil {
		t.Errorf("expected bypass context to succeed, got %v", err)
	}

	aliceCtx := principal.InjectContext(ctx, principal.PrincipalInfo{ID: "alice", Kind: principal.KindJWT})
	saCtx := principal.InjectContext(ctx, principal.PrincipalInfo{ID: "system:serviceaccount:default:default", Kind: principal.KindJWT})
	bobCtx := principal.InjectContext(ctx, principal.PrincipalInfo{ID: "bob", Kind: principal.KindJWT})

	// 3. Alice and SA (global owners) can create and list atespaces; Bob cannot.
	for _, c := range []context.Context{aliceCtx, saCtx} {
		if err := authorizer.Check(c, RelationCanCreateAtespace, GlobalRootObject); err != nil {
			t.Errorf("expected global owner to be allowed can_create_atespace, got %v", err)
		}
		if err := authorizer.Check(c, RelationCanListAtespaces, GlobalRootObject); err != nil {
			t.Errorf("expected global owner to be allowed can_list_atespaces, got %v", err)
		}
	}
	if err := authorizer.Check(bobCtx, RelationCanCreateAtespace, GlobalRootObject); apierror.Code(err) != codes.PermissionDenied {
		t.Errorf("expected bob to be denied can_create_atespace, got %v", err)
	}
	if err := authorizer.Check(bobCtx, RelationCanListAtespaces, GlobalRootObject); apierror.Code(err) != codes.PermissionDenied {
		t.Errorf("expected bob to be denied can_list_atespaces, got %v", err)
	}

	// 4. ContextualTuples links parent_global dynamically at Check time:
	// - Global owner (alice) inherits can_get, can_delete, can_create_actor on atespace:team-x.
	// - Unprivileged user (bob) is denied on all of them.
	for _, rel := range []string{RelationCanGet, RelationCanDelete, "can_create_actor"} {
		if err := authorizer.Check(aliceCtx, rel, AtespaceObject("team-x")); err != nil {
			t.Errorf("expected global owner alice allowed %s on team-x via contextual tuples, got %v", rel, err)
		}
		if err := authorizer.Check(bobCtx, rel, AtespaceObject("team-x")); apierror.Code(err) != codes.PermissionDenied {
			t.Errorf("expected bob denied %s on team-x, got %v", rel, err)
		}
	}

	// 5. Grant bob direct editor access on atespace:team-x.
	writeTestTuple(t, ctx, pool, policyManager, "bob", "editor", AtespaceObject("team-x"))
	if err := authorizer.Check(bobCtx, RelationCanGet, AtespaceObject("team-x")); err != nil {
		t.Errorf("expected bob allowed can_get on team-x, got %v", err)
	}

	// 6. PolicyManager.DeleteAtespacePolicies requires a transaction in ctx,
	// and removes all tuples on team-x when committed.
	if err := policyManager.DeleteAtespacePolicies(ctx, "team-x"); err == nil {
		t.Fatal("DeleteAtespacePolicies without a transaction in ctx succeeded")
	}
	txDel, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("pool.Begin failed: %v", err)
	}
	if err := policyManager.DeleteAtespacePolicies(pgfga.ContextWithTx(ctx, txDel), "team-x"); err != nil {
		t.Fatalf("DeleteAtespacePolicies failed: %v", err)
	}
	if err := txDel.Commit(ctx); err != nil {
		t.Fatalf("txDel.Commit failed: %v", err)
	}
	if err := authorizer.Check(bobCtx, RelationCanGet, AtespaceObject("team-x")); apierror.Code(err) != codes.PermissionDenied {
		t.Errorf("expected bob's direct tuple removed after DeleteAtespacePolicies, got %v", err)
	}

	// 7. Every OpenFGA error, including a cancelled or expired context and a
	// model/tuple validation failure, reaches the caller as Internal.
	canceledCtx, cancel := context.WithCancel(aliceCtx)
	cancel()
	if err := authorizer.Check(canceledCtx, RelationCanGet, AtespaceObject("team-x")); apierror.Code(err) != codes.Internal {
		t.Errorf("expected Internal for canceled context, got %v (%v)", apierror.Code(err), err)
	}
	expiredCtx, cancelDeadline := context.WithDeadline(aliceCtx, time.Now().Add(-time.Second))
	defer cancelDeadline()
	if err := authorizer.Check(expiredCtx, RelationCanGet, AtespaceObject("team-x")); apierror.Code(err) != codes.Internal {
		t.Errorf("expected Internal for expired context, got %v (%v)", apierror.Code(err), err)
	}
	if err := authorizer.Check(aliceCtx, "unknown_relation", GlobalRootObject); apierror.Code(err) != codes.Internal {
		t.Errorf("expected Internal for unknown relation, got %v (%v)", apierror.Code(err), err)
	}
}

func TestFormatUser_NoCollision(t *testing.T) {
	u1 := formatUser("alice")
	u2 := formatUser("user:alice")
	if u1 == u2 {
		t.Fatalf("formatUser(\"alice\") and formatUser(\"user:alice\") collided on %q", u1)
	}
}

func TestAtespacedObjects_NoCollision(t *testing.T) {
	// '/' separates the atespace from the name, so a '/' inside either must
	// not produce another resource's ID.
	for _, format := range []func(atespace, name string) string{ActorObject, ActorTemplateObject} {
		if o1, o2 := format("a/b", "c"), format("a", "b/c"); o1 == o2 {
			t.Errorf("(%q, %q) and (%q, %q) collided on %q", "a/b", "c", "a", "b/c", o1)
		}
	}
	if o1, o2 := ActorObject("a", "b"), ActorTemplateObject("a", "b"); o1 == o2 {
		t.Errorf("ActorObject and ActorTemplateObject collided on %q", o1)
	}
}
