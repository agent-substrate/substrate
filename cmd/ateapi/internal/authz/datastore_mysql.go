// Copyright 2026 Google LLC and The OpenFGA Authors
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
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storesql"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/mysql"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
)

// mysqlTransactionalDatastore is the MySQL counterpart of
// transactionalDatastore; the NOTE in datastore.go applies to both. It wraps
// github.com/openfga/openfga/pkg/storage/mysql/mysql.go, whose schema atemy
// migration 000002 pins.
type mysqlTransactionalDatastore struct {
	*mysql.Datastore
}

// newMySQLTransactionalDatastore wraps an upstream MySQL datastore on db.
// mysql.NewWithDB (mysql.go:78-86) calls db.SetMaxOpenConns,
// SetConnMaxIdleTime, and SetConnMaxLifetime unconditionally, so the config
// carries db's open-connection limit forward and restates the lifetimes
// atemy.Connect sets. database/sql exposes no getter for the lifetimes.
func newMySQLTransactionalDatastore(db *sql.DB) (*mysqlTransactionalDatastore, error) {
	cfg := sqlcommon.NewConfig()
	cfg.MaxOpenConns = db.Stats().MaxOpenConnections
	cfg.ConnMaxLifetime = storesql.ConnMaxLifetime
	cfg.ConnMaxIdleTime = storesql.ConnMaxIdleTime
	ds, err := mysql.NewWithDB(db, cfg)
	if err != nil {
		return nil, fmt.Errorf("creating OpenFGA mysql adapter: %w", err)
	}
	return &mysqlTransactionalDatastore{Datastore: ds}, nil
}

// Close is a no-op because the underlying *sql.DB is owned and closed by the
// caller (cmd/ateapi/main.go), and OpenFGA's server.Close() calls datastore.Close().
// Upstream equivalent: (*mysql.Datastore).Close (mysql.go:129-134).
func (d *mysqlTransactionalDatastore) Close() {}

// ReadAuthorizationModel queries the authorization_model table on the *sql.Tx
// in ctx when present, and otherwise delegates to the pool datastore.
//
// 1:1 with (*mysql.Datastore).ReadAuthorizationModel (mysql.go:410-415).
func (d *mysqlTransactionalDatastore) ReadAuthorizationModel(ctx context.Context, store string, modelID string) (*openfgav1.AuthorizationModel, error) {
	tx, ok := sqlTxFromContext(ctx)
	if !ok {
		return d.Datastore.ReadAuthorizationModel(ctx, store, modelID)
	}
	return readAuthorizationModelOnTx(ctx, sqlTxStatements(tx), store, modelID)
}

// ReadPage requires an active *sql.Tx on ctx via ContextWithTx and executes the
// paginated tuple query on that transaction.
//
// 1:1 with (*mysql.Datastore).ReadPage (mysql.go:150-161).
func (d *mysqlTransactionalDatastore) ReadPage(
	ctx context.Context,
	store string,
	filter storage.ReadFilter,
	options storage.ReadPageOptions,
) ([]*openfgav1.Tuple, string, error) {
	tx, ok := sqlTxFromContext(ctx)
	if !ok {
		return nil, "", ErrNoTransactionInContext
	}
	return readPageOnTx(ctx, sqlTxStatements(tx), store, filter, options)
}

// Write requires an active *sql.Tx on ctx via ContextWithTx and executes the
// write on that transaction without calling BeginTx or Commit.
//
// 1:1 with (*mysql.Datastore).Write (mysql.go:221-238).
func (d *mysqlTransactionalDatastore) Write(
	ctx context.Context,
	store string,
	deletes storage.Deletes,
	writes storage.Writes,
	opts ...storage.TupleWriteOption,
) error {
	tx, ok := sqlTxFromContext(ctx)
	if !ok {
		return ErrNoTransactionInContext
	}
	return writeOnTx(ctx, sqlTxStatements(tx), store, deletes, writes, storage.NewTupleWriteOptions(opts...), time.Now().UTC())
}

func sqlTxStatements(tx *sql.Tx) txStatements {
	return txStatements{
		stbl:      sq.StatementBuilder.PlaceholderFormat(sq.Question),
		connector: sqlcommon.NewTxConnector(tx),
		exec: func(ctx context.Context, stmt string, args ...any) (int64, error) {
			res, err := tx.ExecContext(ctx, stmt, args...)
			if err != nil {
				return 0, err
			}
			return res.RowsAffected()
		},
		handleError: mysql.HandleSQLError,
	}
}

// mysqlInitLockName names the user-level lock that serializes OpenFGA store
// provisioning across replicas. GET_LOCK names are server-wide, so the name
// carries the database, as a PostgreSQL advisory lock is scoped to one. MySQL
// caps names at 64 characters, so the database name is hashed.
func mysqlInitLockName(database string) string {
	sum := sha256.Sum256([]byte(database))
	return "atefga-init:" + hex.EncodeToString(sum[:16])
}

// acquireMySQLInitLock is the MySQL counterpart of acquireInitLock, and like
// it waits until the lock is free or ctx ends. GET_LOCK belongs to the
// session, so it pins one connection until unlock. It uses SELECT GET_LOCK
// without FROM, the only form PlanetScale Vitess accepts.
func acquireMySQLInitLock(ctx context.Context, db *sql.DB) (func(), error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquiring connection for OpenFGA init lock: %w", err)
	}
	var database string
	if err := conn.QueryRowContext(ctx, `SELECT DATABASE()`).Scan(&database); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("acquiring OpenFGA init lock: %w", err)
	}
	name := mysqlInitLockName(database)
	var acquired sql.NullInt64
	// A negative timeout waits indefinitely; canceling ctx closes the
	// connection, which ends the wait.
	if err := conn.QueryRowContext(ctx, `SELECT GET_LOCK(?, -1)`, name).Scan(&acquired); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("acquiring OpenFGA init lock: %w", err)
	}
	if !acquired.Valid || acquired.Int64 != 1 {
		_ = conn.Close()
		return nil, fmt.Errorf("acquiring OpenFGA init lock: GET_LOCK did not grant %q", name)
	}
	return func() {
		_, _ = conn.ExecContext(context.Background(), `SELECT RELEASE_LOCK(?)`, name)
		// Discard the session rather than pool it. That frees the lock even if
		// RELEASE_LOCK failed, and Vitess keeps a session that took a lock on
		// a reserved connection until the client disconnects.
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		_ = conn.Close()
	}, nil
}
