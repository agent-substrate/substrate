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
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/server"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/dockerenv"
	"github.com/agent-substrate/substrate/internal/principal"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// One container per backend serves every test in this package. Each test gets
// a fresh database in it, so OpenFGA stores and tuples never leak between tests.
var (
	databaseSeq atomic.Int64

	postgresOnce      sync.Once
	postgresContainer *tcpostgres.PostgresContainer
	postgresAdmin     *pgxpool.Pool
	postgresErr       error

	mysqlOnce      sync.Once
	mysqlContainer *tcmysql.MySQLContainer
	mysqlAdmin     *sql.DB
	mysqlAdminDSN  string
	mysqlErr       error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if postgresAdmin != nil {
		postgresAdmin.Close()
	}
	if mysqlAdmin != nil {
		_ = mysqlAdmin.Close()
	}
	var containers []testcontainers.Container
	if postgresContainer != nil {
		containers = append(containers, postgresContainer)
	}
	if mysqlContainer != nil {
		containers = append(containers, mysqlContainer)
	}
	for _, ctr := range containers {
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			fmt.Fprintf(os.Stderr, "terminating testcontainer: %v\n", err)
			if code == 0 {
				code = 1
			}
		}
	}
	os.Exit(code)
}

func requireContainer(t *testing.T, name string, err error) {
	t.Helper()
	if err == nil {
		return
	}
	if dockerenv.Required() {
		t.Fatalf("%s testcontainer unavailable and required (CI or REQUIRE_DOCKER is set): %v", name, err)
	}
	t.Skipf("%s testcontainer unavailable (requires Docker): %v", name, err)
}

// testDB is one migrated store database and the backend-specific operations
// the tests need on it.
type testDB struct {
	name    string
	backend Backend
	// sqlDB runs queries on the backend's pool.
	sqlDB *sql.DB
	// singleConnBackend returns a second backend on the same database whose
	// pool holds at most one connection.
	singleConnBackend func(t *testing.T) Backend
}

type testTx struct {
	tx       Tx
	exec     func(ctx context.Context, query string) error
	commit   func(ctx context.Context) error
	rollback func(ctx context.Context) error
}

// forEachBackend runs fn against a fresh PostgreSQL and a fresh MySQL store
// database.
func forEachBackend(t *testing.T, fn func(t *testing.T, db testDB)) {
	t.Run("postgres", func(t *testing.T) { fn(t, startPostgres(t)) })
	t.Run("mysql", func(t *testing.T) { fn(t, startMySQL(t)) })
}

func startPostgres(t *testing.T) testDB {
	t.Helper()
	ctx := context.Background()
	postgresOnce.Do(func() {
		if err := dockerenv.Configure(ctx); err != nil {
			postgresErr = err
			return
		}
		ctr, err := tcpostgres.Run(ctx,
			"postgres:18-alpine",
			tcpostgres.WithDatabase("testdb"),
			tcpostgres.WithUsername("postgres"),
			tcpostgres.WithPassword("postgres"),
			tcpostgres.BasicWaitStrategies(),
		)
		if ctr != nil {
			postgresContainer = ctr
		}
		if err != nil {
			postgresErr = err
			return
		}
		connStr, err := ctr.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			postgresErr = err
			return
		}
		pool, err := pgxpool.New(ctx, connStr)
		if err != nil {
			postgresErr = err
			return
		}
		postgresAdmin = pool
		postgresErr = pingWithRetries(ctx, pool.Ping)
	})
	requireContainer(t, "PostgreSQL", postgresErr)

	dbName := fmt.Sprintf("authz_%d", databaseSeq.Add(1))
	if _, err := postgresAdmin.Exec(ctx, "CREATE DATABASE "+dbName); err != nil {
		t.Fatalf("creating database %s: %v", dbName, err)
	}
	cfg := postgresAdmin.Config().Copy()
	cfg.ConnConfig.Database = dbName
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("creating pgxpool: %v", err)
	}
	t.Cleanup(pool.Close)

	sqlDB := stdlib.OpenDBFromPool(pool)
	t.Cleanup(func() { _ = sqlDB.Close() })
	applyMigrations(t, ctx, goose.DialectPostgres, sqlDB, "../store/atepg/migrations")

	return testDB{
		name:    "postgres",
		backend: PostgresBackend(pool),
		sqlDB:   sqlDB,
		singleConnBackend: func(t *testing.T) Backend {
			t.Helper()
			singleCfg := pool.Config()
			singleCfg.MaxConns = 1
			singleCfg.MinConns = 0
			single, err := pgxpool.NewWithConfig(context.Background(), singleCfg)
			if err != nil {
				t.Fatalf("creating single-conn pool: %v", err)
			}
			t.Cleanup(single.Close)
			return PostgresBackend(single)
		},
	}
}

func startMySQL(t *testing.T) testDB {
	t.Helper()
	ctx := context.Background()
	mysqlOnce.Do(func() {
		if err := dockerenv.Configure(ctx); err != nil {
			mysqlErr = err
			return
		}
		ctr, err := tcmysql.Run(ctx,
			"mysql:8.4",
			tcmysql.WithDatabase("authz"),
			tcmysql.WithUsername("root"),
			tcmysql.WithPassword("root"),
		)
		if ctr != nil {
			mysqlContainer = ctr
		}
		if err != nil {
			mysqlErr = err
			return
		}
		// The session settings atemy.Open applies; importing atemy here
		// would be an import cycle.
		dsn, err := ctr.ConnectionString(ctx, "parseTime=true", "loc=UTC", "clientFoundRows=true", "interpolateParams=true", "transaction_isolation=%27READ-COMMITTED%27")
		if err != nil {
			mysqlErr = err
			return
		}
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			mysqlErr = err
			return
		}
		mysqlAdmin = db
		mysqlAdminDSN = dsn
		mysqlErr = pingWithRetries(ctx, db.PingContext)
	})
	requireContainer(t, "MySQL", mysqlErr)

	dbName := fmt.Sprintf("authz_%d", databaseSeq.Add(1))
	if _, err := mysqlAdmin.ExecContext(ctx, "CREATE DATABASE "+dbName); err != nil {
		t.Fatalf("creating database %s: %v", dbName, err)
	}
	cfg, err := gomysql.ParseDSN(mysqlAdminDSN)
	if err != nil {
		t.Fatalf("parsing MySQL DSN: %v", err)
	}
	cfg.DBName = dbName
	dsn := cfg.FormatDSN()
	openDB := func(t *testing.T) *sql.DB {
		t.Helper()
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			t.Fatalf("opening MySQL database: %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	db := openDB(t)
	applyMigrations(t, ctx, goose.DialectMySQL, db, "../store/atemy/migrations")

	return testDB{
		name:    "mysql",
		backend: MySQLBackend(db),
		sqlDB:   db,
		singleConnBackend: func(t *testing.T) Backend {
			t.Helper()
			single := openDB(t)
			single.SetMaxOpenConns(1)
			return MySQLBackend(single)
		},
	}
}

func pingWithRetries(ctx context.Context, ping func(context.Context) error) error {
	var err error
	for i := 0; i < 30; i++ {
		if err = ping(ctx); err == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("pinging database after retries: %w", err)
}

func applyMigrations(t *testing.T, ctx context.Context, dialect goose.Dialect, db *sql.DB, dir string) {
	t.Helper()
	provider, err := goose.NewProvider(dialect, db, os.DirFS(dir))
	if err != nil {
		t.Fatalf("create goose provider: %v", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		t.Fatalf("run Substrate and OpenFGA migrations: %v", err)
	}
}

func newTestAuthz(t *testing.T, ctx context.Context, backend Backend) (*server.Server, string, string) {
	t.Helper()
	fgaSrv, err := NewOpenFGAServer(backend)
	if err != nil {
		t.Fatalf("NewOpenFGAServer failed: %v", err)
	}
	t.Cleanup(fgaSrv.Close)
	storeID, modelID, err := EnsureStoreAndModel(ctx, backend, fgaSrv)
	if err != nil {
		t.Fatalf("EnsureStoreAndModel failed: %v", err)
	}
	return fgaSrv, storeID, modelID
}

func TestEnsureStoreAndModel_NilArgs(t *testing.T) {
	if _, err := NewOpenFGAServer(Backend{}); err == nil {
		t.Fatal("expected error when the backend is empty in NewOpenFGAServer")
	}
	if _, _, err := EnsureStoreAndModel(context.Background(), Backend{}, nil); err == nil {
		t.Fatal("expected error when the backend is empty in EnsureStoreAndModel")
	}
}

func TestEnsureStoreAndModel_InitializeAndCheck(t *testing.T) {
	forEachBackend(t, testEnsureStoreAndModelInitializeAndCheck)
}

func testEnsureStoreAndModelInitializeAndCheck(t *testing.T, db testDB) {
	ctx := context.Background()

	fgaSrv, storeID, modelID := newTestAuthz(t, ctx, db.backend)

	if storeID == "" {
		t.Fatal("expected non-empty storeID")
	}
	if modelID == "" {
		t.Fatal("expected non-empty modelID")
	}

	// Write relationship tuples inside a transaction and verify authorization checks against the model.
	tx := db.begin(t, ctx)
	_, err := fgaSrv.Write(ContextWithTx(ctx, tx.tx), &openfgav1.WriteRequest{
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
	if err := tx.commit(ctx); err != nil {
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
	_, storeID2, modelID2 := newTestAuthz(t, ctx, db.backend)

	if storeID2 != storeID {
		t.Errorf("expected same storeID %q on re-init, got %q", storeID, storeID2)
	}
	if modelID2 != modelID {
		t.Errorf("expected same modelID %q on re-init, got %q", modelID, modelID2)
	}
}

func TestAcquireInitLock_CanceledWhileHeld(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db testDB) {
		unlock, err := acquireInitLock(t.Context(), db.backend)
		if err != nil {
			t.Fatalf("acquireInitLock failed: %v", err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
		defer cancel()
		_, err = acquireInitLock(ctx, db.backend)
		unlock()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("acquireInitLock while held = %v, want context.DeadlineExceeded", err)
		}
		ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		unlock, err = acquireInitLock(ctx, db.backend)
		if err != nil {
			t.Fatalf("acquireInitLock after unlock failed: %v", err)
		}
		unlock()
	})
}

func TestTransactionalDatastore_RollbackAndCommit(t *testing.T) {
	forEachBackend(t, testTransactionalDatastoreRollbackAndCommit)
}

func testTransactionalDatastoreRollbackAndCommit(t *testing.T, db testDB) {
	ctx := context.Background()

	fgaSrv, storeID, modelID := newTestAuthz(t, ctx, db.backend)

	// Calling Write or Read (ReadPage) without an active transaction in ctx must fail loudly.
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
		if err := db.sqlDB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM atespaces WHERE name = 'team-tx')").Scan(&exists); err != nil {
			t.Fatalf("checking atespaces row failed: %v", err)
		}
		return exists
	}

	const insertAtespace = "INSERT INTO atespaces (name, uid, version, proto) VALUES ('team-tx', 'uid-1', 1, '')"

	// Write both a Substrate atespaces row and an OpenFGA tuple inside a transaction
	// that rolls back -> neither the atespaces row nor the tuple may persist.
	txRollback := db.begin(t, ctx)
	if err := txRollback.exec(ctx, insertAtespace); err != nil {
		t.Fatalf("txRollback insert atespaces failed: %v", err)
	}
	writeTuple(ContextWithTx(ctx, txRollback.tx))
	if err := txRollback.rollback(ctx); err != nil {
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
	txCommit := db.begin(t, ctx)
	if err := txCommit.exec(ctx, insertAtespace); err != nil {
		t.Fatalf("txCommit insert atespaces failed: %v", err)
	}
	writeTuple(ContextWithTx(ctx, txCommit.tx))
	if err := txCommit.commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	if !atespaceExists() {
		t.Fatalf("expected atespaces row 'team-tx' to persist after commit")
	}
	if !checkAllowed() {
		t.Fatalf("expected bob allowed after committed write")
	}

	// Closing fgaServer must not close the shared pool.
	fgaSrv2, err := NewOpenFGAServer(db.backend)
	if err != nil {
		t.Fatalf("NewOpenFGAServer failed: %v", err)
	}
	fgaSrv2.Close()
	if err := db.sqlDB.PingContext(ctx); err != nil {
		t.Fatalf("expected shared pool to remain open after fgaServer.Close(), got %v", err)
	}

	// Verify fgaServer.Read and fgaServer.Write inside ContextWithTx do not check out
	// a second connection from a single-connection pool (preventing pool starvation deadlock).
	singleBackend := db.singleConnBackend(t)
	singleFGASrv, err := NewOpenFGAServer(singleBackend)
	if err != nil {
		t.Fatalf("NewOpenFGAServer(singleBackend) failed: %v", err)
	}
	t.Cleanup(singleFGASrv.Close)
	if singleBackend.db != nil {
		if got := singleBackend.db.Stats().MaxOpenConnections; got != 1 {
			t.Fatalf("MaxOpenConnections after NewOpenFGAServer = %d, want 1", got)
		}
	}

	txCtxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	singleTx := beginOn(t, txCtxTimeout, singleBackend)
	txCtx := ContextWithTx(txCtxTimeout, singleTx.tx)

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
	if err := singleTx.commit(txCtxTimeout); err != nil {
		t.Fatalf("singleTx.Commit failed: %v", err)
	}
	if checkAllowed() {
		t.Fatalf("expected bob denied after committed delete on single-connection pool")
	}
}

func (db testDB) begin(t *testing.T, ctx context.Context) testTx {
	t.Helper()
	return beginOn(t, ctx, db.backend)
}

// beginOn opens a transaction on backend's own pool, which may differ from
// the testDB pool (such as a single-connection pool).
func beginOn(t *testing.T, ctx context.Context, backend Backend) testTx {
	t.Helper()
	if backend.pool != nil {
		tx, err := backend.pool.Begin(ctx)
		if err != nil {
			t.Fatalf("pool.Begin failed: %v", err)
		}
		t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
		return testTx{
			tx: PgxTx(tx),
			exec: func(ctx context.Context, query string) error {
				_, err := tx.Exec(ctx, query)
				return err
			},
			commit:   tx.Commit,
			rollback: tx.Rollback,
		}
	}
	tx, err := backend.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("db.BeginTx failed: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return testTx{
		tx: SQLTx(tx),
		exec: func(ctx context.Context, query string) error {
			_, err := tx.ExecContext(ctx, query)
			return err
		},
		commit:   func(context.Context) error { return tx.Commit() },
		rollback: func(context.Context) error { return tx.Rollback() },
	}
}

func writeTestTuple(t *testing.T, ctx context.Context, db testDB, pm *PolicyManager, user, relation, object string) {
	t.Helper()
	tx := db.begin(t, ctx)
	if _, err := pm.fgaServer.Write(ContextWithTx(ctx, tx.tx), &openfgav1.WriteRequest{
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
	if err := tx.commit(ctx); err != nil {
		t.Fatalf("tx.Commit failed: %v", err)
	}
}

func TestAuthorizerAndPolicyManager_RuntimeChecks(t *testing.T) {
	forEachBackend(t, testAuthorizerAndPolicyManagerRuntimeChecks)
}

func testAuthorizerAndPolicyManagerRuntimeChecks(t *testing.T, db testDB) {
	ctx := context.Background()

	fgaSrv, err := NewOpenFGAServer(db.backend)
	if err != nil {
		t.Fatalf("NewOpenFGAServer failed: %v", err)
	}
	t.Cleanup(fgaSrv.Close)

	authorizer, policyManager, err := New(ctx, db.backend, fgaSrv, nil)
	if err != nil {
		t.Fatalf("authz.New failed: %v", err)
	}

	// 1. Seed global owners (including a Kubernetes ServiceAccount ID with colons).
	writeTestTuple(t, ctx, db, policyManager, "alice", "owner", GlobalRootObject)
	writeTestTuple(t, ctx, db, policyManager, "system:serviceaccount:default:default", "owner", GlobalRootObject)

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
	writeTestTuple(t, ctx, db, policyManager, "bob", "editor", AtespaceObject("team-x"))
	if err := authorizer.Check(bobCtx, RelationCanGet, AtespaceObject("team-x")); err != nil {
		t.Errorf("expected bob allowed can_get on team-x, got %v", err)
	}

	// 6. PolicyManager.DeleteAtespacePolicies requires a transaction, and
	// removes all tuples on team-x when committed.
	if err := policyManager.DeleteAtespacePolicies(ctx, Tx{}, "team-x"); !errors.Is(err, ErrNilTransaction) {
		t.Fatalf("DeleteAtespacePolicies with nil tx = %v, want ErrNilTransaction", err)
	}
	txDel := db.begin(t, ctx)
	if err := policyManager.DeleteAtespacePolicies(ctx, txDel.tx, "team-x"); err != nil {
		t.Fatalf("DeleteAtespacePolicies failed: %v", err)
	}
	if err := txDel.commit(ctx); err != nil {
		t.Fatalf("txDel.Commit failed: %v", err)
	}
	if err := authorizer.Check(bobCtx, RelationCanGet, AtespaceObject("team-x")); status.Code(err) != codes.PermissionDenied {
		t.Errorf("expected bob's direct tuple removed after DeleteAtespacePolicies, got %v", err)
	}

	// 7. Check preserves Canceled and DeadlineExceeded and maps server-side
	// OpenFGA errors (such as model/tuple validation failures) to Internal.
	canceledCtx, cancel := context.WithCancel(aliceCtx)
	cancel()
	if err := authorizer.Check(canceledCtx, RelationCanGet, AtespaceObject("team-x")); status.Code(err) != codes.Canceled {
		t.Errorf("expected Canceled for canceled context, got %v (%v)", status.Code(err), err)
	}
	expiredCtx, cancelDeadline := context.WithDeadline(aliceCtx, time.Now().Add(-time.Second))
	defer cancelDeadline()
	if err := authorizer.Check(expiredCtx, RelationCanGet, AtespaceObject("team-x")); status.Code(err) != codes.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded for expired context, got %v (%v)", status.Code(err), err)
	}
	if err := authorizer.Check(aliceCtx, "unknown_relation", GlobalRootObject); status.Code(err) != codes.Internal {
		t.Errorf("expected Internal for unknown relation, got %v (%v)", status.Code(err), err)
	}
}

func TestStatusFromFGAError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
	}{
		{"context.Canceled", context.Canceled, codes.Canceled},
		{"OpenFGA ErrRequestCancelled", serverErrors.ErrRequestCancelled, codes.Canceled},
		{"context.DeadlineExceeded", context.DeadlineExceeded, codes.DeadlineExceeded},
		{"OpenFGA ErrRequestDeadlineExceeded", serverErrors.ErrRequestDeadlineExceeded, codes.DeadlineExceeded},
		{"gRPC InvalidArgument from OpenFGA", status.Error(codes.InvalidArgument, "bad tuple"), codes.Internal},
		{"OpenFGA validation error code", serverErrors.ValidationError(errors.New("bad relation")), codes.Internal},
		{"OpenFGA internal error code", serverErrors.NewInternalError("", errors.New("db down")), codes.Internal},
		{"untyped error", errors.New("boom"), codes.Internal},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := status.Code(statusFromFGAError(tc.err)); got != tc.wantCode {
				t.Errorf("status.Code(statusFromFGAError(%v)) = %v, want %v", tc.err, got, tc.wantCode)
			}
		})
	}
}

func TestFormatUser_NoCollision(t *testing.T) {
	u1 := formatUser("alice")
	u2 := formatUser("user:alice")
	if u1 == u2 {
		t.Fatalf("formatUser(\"alice\") and formatUser(\"user:alice\") collided on %q", u1)
	}
}

// TestPolicyManager_MemberLengthPerBackend pins the one difference between
// the backends: OpenFGA's MySQL schema holds a tuple user in 256 characters,
// so only the MySQL store refuses longer members, as InvalidArgument.
func TestPolicyManager_MemberLengthPerBackend(t *testing.T) {
	forEachBackend(t, func(t *testing.T, db testDB) {
		ctx := t.Context()
		fgaSrv, err := NewOpenFGAServer(db.backend)
		if err != nil {
			t.Fatalf("NewOpenFGAServer failed: %v", err)
		}
		t.Cleanup(fgaSrv.Close)
		_, policyManager, err := New(ctx, db.backend, fgaSrv, nil)
		if err != nil {
			t.Fatalf("New failed: %v", err)
		}
		reconcile := func(member string) error {
			t.Helper()
			tx := db.begin(t, ctx)
			defer tx.rollback(ctx) //nolint:errcheck // the test only inspects the reconcile error
			return policyManager.ReconcileGlobalBindings(ctx, tx.tx, []*ateapipb.Binding{{Role: "owner", Members: []string{member}}})
		}

		if err := reconcile("user:" + strings.Repeat("a", 251)); err != nil {
			t.Errorf("a 256-character member failed: %v", err)
		}
		err = reconcile("user:" + strings.Repeat("a", 252))
		if db.name == "postgres" {
			if err != nil {
				t.Errorf("a 257-character member failed on PostgreSQL: %v", err)
			}
			return
		}
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("a 257-character member on MySQL = %v, want InvalidArgument", err)
		}
		if err := reconcile("user:" + strings.Repeat(":", 84)); status.Code(err) != codes.InvalidArgument {
			t.Errorf("a member past the limit once encoded on MySQL = %v, want InvalidArgument", err)
		}
	})
}
