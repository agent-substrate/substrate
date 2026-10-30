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

package pgfga_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/server"

	"go.ate.dev/substrate/cmd/ateapi/internal/authz"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/atepg/pgfga"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
)

func TestMain(m *testing.M) {
	storetest.RunTests(m)
}

func newFGAServer(t *testing.T, pool *pgxpool.Pool) *server.Server {
	t.Helper()
	ds, err := pgfga.New(pool)
	if err != nil {
		t.Fatalf("pgfga.New failed: %v", err)
	}
	fgaSrv, err := authz.NewOpenFGAServer(ds)
	if err != nil {
		t.Fatalf("NewOpenFGAServer failed: %v", err)
	}
	t.Cleanup(fgaSrv.Close)
	return fgaSrv
}

func TestNewRejectsNilPool(t *testing.T) {
	if _, err := pgfga.New(nil); err == nil {
		t.Fatal("expected error when pool is nil")
	}
}

func TestTransactionalDatastore_RollbackAndCommit(t *testing.T) {
	ctx := context.Background()
	p := storetest.SetupPostgresPersistence(t)
	pool := p.Pool()

	fgaSrv := newFGAServer(t, pool)
	storeID, modelID, err := authz.EnsureStoreAndModel(ctx, p, fgaSrv)
	if err != nil {
		t.Fatalf("EnsureStoreAndModel failed: %v", err)
	}

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
	writeTuple(pgfga.ContextWithTx(ctx, txRollback))
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
	writeTuple(pgfga.ContextWithTx(ctx, txCommit))
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
	ds2, err := pgfga.New(pool)
	if err != nil {
		t.Fatalf("pgfga.New failed: %v", err)
	}
	fgaSrv2, err := authz.NewOpenFGAServer(ds2)
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

	singleFGASrv := newFGAServer(t, singlePool)

	txCtxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	singleTx, err := singlePool.Begin(txCtxTimeout)
	if err != nil {
		t.Fatalf("singlePool.Begin failed: %v", err)
	}
	txCtx := pgfga.ContextWithTx(txCtxTimeout, singleTx)

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
