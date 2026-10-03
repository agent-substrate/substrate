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
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
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
// carries db's current open-connection limit forward. database/sql exposes no
// getter for the two lifetimes; NewConfig leaves them at zero, which is the
// database/sql default.
func newMySQLTransactionalDatastore(db *sql.DB) (*mysqlTransactionalDatastore, error) {
	cfg := sqlcommon.NewConfig()
	cfg.MaxOpenConns = db.Stats().MaxOpenConnections
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

// ateFGAInitLockName names the MySQL user-level lock that serializes OpenFGA
// store provisioning across replicas. Unlike a PostgreSQL advisory lock,
// GET_LOCK is scoped to the server rather than the database, so deployments
// sharing a server also serialize with each other, which is harmless here.
const ateFGAInitLockName = "atefga-init"

// mysqlInitLockTimeout bounds one GET_LOCK wait. Provisioning holds the lock
// for milliseconds, so expiry means a stuck holder.
const mysqlInitLockTimeout = 5 * time.Minute

// acquireMySQLInitLock is the MySQL counterpart of acquireInitLock. GET_LOCK
// belongs to the session, so it pins one connection until unlock. It uses
// SELECT GET_LOCK without FROM, the only form PlanetScale Vitess accepts.
func acquireMySQLInitLock(ctx context.Context, db *sql.DB) (func(), error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquiring connection for OpenFGA init lock: %w", err)
	}
	var acquired sql.NullInt64
	if err := conn.QueryRowContext(ctx, `SELECT GET_LOCK(?, ?)`, ateFGAInitLockName, int(mysqlInitLockTimeout.Seconds())).Scan(&acquired); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("acquiring OpenFGA init lock: %w", err)
	}
	if !acquired.Valid || acquired.Int64 != 1 {
		_ = conn.Close()
		return nil, fmt.Errorf("acquiring OpenFGA init lock: GET_LOCK did not grant %q within %s", ateFGAInitLockName, mysqlInitLockTimeout)
	}
	return func() {
		var released sql.NullInt64
		err := conn.QueryRowContext(context.Background(), `SELECT RELEASE_LOCK(?)`, ateFGAInitLockName).Scan(&released)
		if err != nil || released.Int64 != 1 {
			// Close would return the session to the pool still holding the
			// lock; ErrBadConn makes database/sql discard it, which ends the
			// session and frees the lock.
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		_ = conn.Close()
	}, nil
}
