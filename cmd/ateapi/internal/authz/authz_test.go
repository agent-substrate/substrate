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
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/server"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/principal"
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
	if err := configureDockerEnv(ctx); err != nil {
		t.Skipf("skipping test; docker is unavailable: %v", err)
	}

	pgContainer, err := postgres.Run(ctx,
		"postgres:18-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Skipf("skipping test; failed to start postgres container: %v", err)
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

func newTestAuthz(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (*server.Server, string, string) {
	t.Helper()
	fgaSrv, err := NewOpenFGAServer(pool)
	if err != nil {
		t.Fatalf("NewOpenFGAServer failed: %v", err)
	}
	t.Cleanup(fgaSrv.Close)
	storeID, modelID, err := EnsureStoreAndModel(ctx, pool, fgaSrv)
	if err != nil {
		t.Fatalf("EnsureStoreAndModel failed: %v", err)
	}
	return fgaSrv, storeID, modelID
}

func TestEnsureStoreAndModel_NilArgs(t *testing.T) {
	if _, err := NewOpenFGAServer(nil); err == nil {
		t.Fatal("expected error when pool is nil in NewOpenFGAServer")
	}
	if _, _, err := EnsureStoreAndModel(context.Background(), nil, nil); err == nil {
		t.Fatal("expected error when pool is nil in EnsureStoreAndModel")
	}
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
	_, err = fgaSrv.Write(ContextWithTx(ctx, tx), &openfgav1.WriteRequest{
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
			Relation: "can_set_policy",
			Object:   "atespace:space-1",
		},
	})
	if err != nil {
		t.Fatalf("Check alice can_set_policy failed: %v", err)
	}
	if !checkResp.GetAllowed() {
		t.Errorf("expected alice to be allowed can_set_policy on atespace:space-1 via global owner inheritance")
	}

	checkBob, err := fgaSrv.Check(ctx, &openfgav1.CheckRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		TupleKey: &openfgav1.CheckRequestTupleKey{
			User:     "user:bob",
			Relation: "can_set_policy",
			Object:   "atespace:space-1",
		},
	})
	if err != nil {
		t.Fatalf("Check bob can_set_policy failed: %v", err)
	}
	if checkBob.GetAllowed() {
		t.Errorf("expected bob to be denied can_set_policy on atespace:space-1")
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

func TestTransactionalDatastore_RollbackAndCommit(t *testing.T) {
	ctx := context.Background()
	pool := startPostgres(t)

	fgaSrv, storeID, modelID := newTestAuthz(t, ctx, pool)

	// Calling Write or Read (ReadPage) without an active pgx.Tx in ctx must fail loudly.
	if _, err := fgaSrv.Write(ctx, &openfgav1.WriteRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				{User: "user:bob", Relation: "editor", Object: "atespace:team-tx"},
			},
		},
	}); err == nil {
		t.Fatal("expected fgaSrv.Write without ContextWithTx to fail, got nil")
	}
	if _, err := fgaSrv.Read(ctx, &openfgav1.ReadRequest{
		StoreId:  storeID,
		TupleKey: &openfgav1.ReadRequestTupleKey{Object: "atespace:team-tx"},
	}); err == nil {
		t.Fatal("expected fgaSrv.Read without ContextWithTx to fail, got nil")
	}

	writeTuple := func(c context.Context) {
		t.Helper()
		_, err := fgaSrv.Write(c, &openfgav1.WriteRequest{
			StoreId:              storeID,
			AuthorizationModelId: modelID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					{
						User:     "user:bob",
						Relation: "editor",
						Object:   "atespace:team-tx",
					},
				},
			},
		})
		if err != nil {
			t.Fatalf("fgaSrv.Write failed: %v", err)
		}
	}

	checkAllowed := func() bool {
		t.Helper()
		resp, err := fgaSrv.Check(ctx, &openfgav1.CheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: modelID,
			TupleKey: &openfgav1.CheckRequestTupleKey{
				User:     "user:bob",
				Relation: "can_get",
				Object:   "atespace:team-tx",
			},
		})
		if err != nil {
			t.Fatalf("fgaSrv.Check failed: %v", err)
		}
		return resp.GetAllowed()
	}

	atespaceExists := func() bool {
		t.Helper()
		var exists bool
		if err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM atespaces WHERE name = 'team-tx')").Scan(&exists); err != nil {
			t.Fatalf("checking atespaces row failed: %v", err)
		}
		return exists
	}

	// Write both a Substrate atespaces row and an OpenFGA tuple inside a transaction
	// that rolls back -> neither the atespaces row nor the tuple may persist.
	txRollback, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("pool.Begin failed: %v", err)
	}
	if _, err := txRollback.Exec(ctx, "INSERT INTO atespaces (name, uid, version, proto) VALUES ('team-tx', 'uid-1', 1, $1)", []byte{}); err != nil {
		t.Fatalf("txRollback insert atespaces failed: %v", err)
	}
	writeTuple(ContextWithTx(ctx, txRollback))
	if err := txRollback.Rollback(ctx); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}
	if atespaceExists() {
		t.Fatalf("expected atespaces row 'team-tx' to be rolled back")
	}
	if checkAllowed() {
		t.Fatalf("expected bob denied after rolled-back write")
	}

	// Write both the Substrate atespaces row and the OpenFGA tuple in a committed
	// transaction -> both persist atomically.
	txCommit, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("pool.Begin failed: %v", err)
	}
	if _, err := txCommit.Exec(ctx, "INSERT INTO atespaces (name, uid, version, proto) VALUES ('team-tx', 'uid-1', 1, $1)", []byte{}); err != nil {
		t.Fatalf("txCommit insert atespaces failed: %v", err)
	}
	writeTuple(ContextWithTx(ctx, txCommit))
	if err := txCommit.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	if !atespaceExists() {
		t.Fatalf("expected atespaces row 'team-tx' to persist after commit")
	}
	if !checkAllowed() {
		t.Fatalf("expected bob allowed after committed write")
	}

	// Closing fgaServer must not close the shared pool.
	fgaSrv2, err := NewOpenFGAServer(pool)
	if err != nil {
		t.Fatalf("NewOpenFGAServer failed: %v", err)
	}
	fgaSrv2.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("expected shared pool to remain open after fgaServer.Close(), got %v", err)
	}

	// Verify fgaServer.Read and fgaServer.Write inside ContextWithTx do not check out
	// a second connection from a pool with MaxConns=1 (preventing pool starvation deadlock).
	singleConnCfg := pool.Config()
	singleConnCfg.MaxConns = 1
	singleConnCfg.MinConns = 0
	singlePool, err := pgxpool.NewWithConfig(ctx, singleConnCfg)
	if err != nil {
		t.Fatalf("creating single-conn pool: %v", err)
	}
	t.Cleanup(singlePool.Close)

	singleFGASrv, err := NewOpenFGAServer(singlePool)
	if err != nil {
		t.Fatalf("NewOpenFGAServer(singlePool) failed: %v", err)
	}
	t.Cleanup(singleFGASrv.Close)

	txCtxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	singleTx, err := singlePool.Begin(txCtxTimeout)
	if err != nil {
		t.Fatalf("singlePool.Begin failed: %v", err)
	}
	txCtx := ContextWithTx(txCtxTimeout, singleTx)

	readResp, err := singleFGASrv.Read(txCtx, &openfgav1.ReadRequest{
		StoreId:  storeID,
		TupleKey: &openfgav1.ReadRequestTupleKey{Object: "atespace:team-tx"},
	})
	if err != nil || len(readResp.GetTuples()) != 1 {
		t.Fatalf("singleFGASrv.Read on single-connection pool failed: resp=%+v, err=%v", readResp, err)
	}

	_, err = singleFGASrv.Write(txCtx, &openfgav1.WriteRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Deletes: &openfgav1.WriteRequestDeletes{
			TupleKeys: []*openfgav1.TupleKeyWithoutCondition{
				{User: "user:bob", Relation: "editor", Object: "atespace:team-tx"},
			},
		},
	})
	if err != nil {
		t.Fatalf("singleFGASrv.Write delete on single-connection pool failed: %v", err)
	}
	if err := singleTx.Commit(txCtxTimeout); err != nil {
		t.Fatalf("singleTx.Commit failed: %v", err)
	}
	if checkAllowed() {
		t.Fatalf("expected bob denied after committed delete on single-connection pool")
	}
}

func writeTestTuple(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pm *PolicyManager, user, relation, object string) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("pool.Begin failed: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := pm.fgaServer.Write(ContextWithTx(ctx, tx), &openfgav1.WriteRequest{
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

	fgaSrv, err := NewOpenFGAServer(pool)
	if err != nil {
		t.Fatalf("NewOpenFGAServer failed: %v", err)
	}
	t.Cleanup(fgaSrv.Close)

	authorizer, policyManager, err := New(ctx, pool, fgaSrv)
	if err != nil {
		t.Fatalf("authz.New failed: %v", err)
	}

	// 1. Seed global owners (including a Kubernetes ServiceAccount ID with colons).
	writeTestTuple(t, ctx, pool, policyManager, "alice", "owner", GlobalRootObject)
	writeTestTuple(t, ctx, pool, policyManager, "system:serviceaccount:default:default", "owner", GlobalRootObject)

	// 1b. A nil Authorizer fails closed with codes.Internal unless explicitly bypassed.
	var nilAuthorizer *Authorizer
	if err := nilAuthorizer.Check(ctx, RelationCanCreateAtespace, GlobalRootObject); status.Code(err) != codes.Internal {
		t.Errorf("expected Internal for nil Authorizer, got %v", err)
	}
	if err := nilAuthorizer.Check(WithBypass(ctx), RelationCanCreateAtespace, GlobalRootObject); err != nil {
		t.Errorf("expected bypass context on nil Authorizer to succeed, got %v", err)
	}

	// 2. Unauthenticated request (no PrincipalInfo) fails with Unauthenticated unless bypassed.
	if err := authorizer.Check(ctx, RelationCanCreateAtespace, GlobalRootObject); status.Code(err) != codes.Unauthenticated {
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
	if err := authorizer.Check(bobCtx, RelationCanCreateAtespace, GlobalRootObject); status.Code(err) != codes.PermissionDenied {
		t.Errorf("expected bob to be denied can_create_atespace, got %v", err)
	}
	if err := authorizer.Check(bobCtx, RelationCanListAtespaces, GlobalRootObject); status.Code(err) != codes.PermissionDenied {
		t.Errorf("expected bob to be denied can_list_atespaces, got %v", err)
	}

	// 4. ContextualTuples links parent_global dynamically at Check time:
	// - Global owner (alice) inherits can_get, can_delete, can_create_actor on atespace:team-x.
	// - Unprivileged user (bob) is denied on all of them.
	for _, rel := range []string{RelationCanGet, RelationCanDelete, "can_create_actor"} {
		if err := authorizer.Check(aliceCtx, rel, AtespaceObject("team-x")); err != nil {
			t.Errorf("expected global owner alice allowed %s on team-x via contextual tuples, got %v", rel, err)
		}
		if err := authorizer.Check(bobCtx, rel, AtespaceObject("team-x")); status.Code(err) != codes.PermissionDenied {
			t.Errorf("expected bob denied %s on team-x, got %v", rel, err)
		}
	}

	// 5. Grant bob direct editor access on atespace:team-x.
	writeTestTuple(t, ctx, pool, policyManager, "bob", "editor", AtespaceObject("team-x"))
	if err := authorizer.Check(bobCtx, RelationCanGet, AtespaceObject("team-x")); err != nil {
		t.Errorf("expected bob allowed can_get on team-x, got %v", err)
	}

	// 6. PolicyManager.DeleteAtespacePolicies requires an active transaction in ctx,
	// and removes all tuples on team-x when committed.
	if err := policyManager.DeleteAtespacePolicies(ctx, "team-x"); err == nil {
		t.Fatalf("expected DeleteAtespacePolicies without ContextWithTx to fail, got nil")
	}
	txDel, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("pool.Begin failed: %v", err)
	}
	if err := policyManager.DeleteAtespacePolicies(ContextWithTx(ctx, txDel), "team-x"); err != nil {
		t.Fatalf("DeleteAtespacePolicies failed: %v", err)
	}
	if err := txDel.Commit(ctx); err != nil {
		t.Fatalf("txDel.Commit failed: %v", err)
	}
	if err := authorizer.Check(bobCtx, RelationCanGet, AtespaceObject("team-x")); status.Code(err) != codes.PermissionDenied {
		t.Errorf("expected bob's direct tuple removed after DeleteAtespacePolicies, got %v", err)
	}
}

func TestFormatUser_NoCollision(t *testing.T) {
	u1 := formatUser("alice")
	u2 := formatUser("user:alice")
	if u1 == u2 {
		t.Fatalf("formatUser(\"alice\") and formatUser(\"user:alice\") collided on %q", u1)
	}
}
