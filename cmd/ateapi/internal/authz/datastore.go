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
	"errors"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/postgres"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
)

// ErrNoTransactionInContext is returned by ReadPage and Write when called
// without an active pgx.Tx injected via ContextWithTx.
var ErrNoTransactionInContext = errors.New("authz datastore: active pgx.Tx required in context")

// NOTE: The SQL query and tuple write/delete/changelog execution helpers in this
// file are 1:1 adaptations of unexported methods and package-private types in
// github.com/openfga/openfga/pkg/storage/postgres/postgres.go (pinned to
// PostgreSQL migration version 6 via TestOpenFGAMigrationVersionGuard in
// cmd/ateapi/internal/store/atepg/migrations_test.go).
//
// Why this wrapper is necessary:
//   1. Upstream (*postgres.Datastore).Write (postgres.go:584-649) always opens
//      its own transaction via s.primaryDB.BeginTx(ctx, ...) and commits it
//      before returning, making it impossible to atomically commit or roll back
//      OpenFGA tuple mutations alongside Substrate table mutations.
//   2. Upstream (*postgres.Datastore).ReadPage / ReadAuthorizationModel
//      always query s.getPgxPool(...) instead of an active pgx.Tx, which both
//      escapes the caller's transaction snapshot (e.g. RepeatableRead / Serializable)
//      and can deadlock a pool when all connections are held by active write transactions.
//   3. Upstream (*postgres.Datastore).Close (postgres.go:307-314) closes the
//      underlying *pgxpool.Pool, which would close Substrate's shared pool when
//      fgaServer.Close() is called.
//
// Transaction enforcement rules:
//   - Write and ReadPage (called by fgaServer.Write and fgaServer.Read) require
//     an active pgx.Tx in ctx via ContextWithTx and fail fast with
//     ErrNoTransactionInContext if absent.
//   - ReadAuthorizationModel uses the active pgx.Tx when present in ctx (e.g.
//     when fgaServer.Write validates tuples against the model), and falls back
//     to the connection pool when called outside a transaction (e.g. during
//     fgaServer.Check or EnsureStoreAndModel).
//   - All other datastore methods (e.g. Read, ReadUserTuple, ReadUsersetTuples)
//     are inherited from the embedded *postgres.Datastore and run on the pool.

// transactionalDatastore wraps OpenFGA's postgres.Datastore to allow ReadPage and Write
// operations to participate in an existing external transaction passed via ContextWithTx.
type transactionalDatastore struct {
	*postgres.Datastore
}

// newTransactionalDatastore wraps ds with transaction-awareness.
func newTransactionalDatastore(ds *postgres.Datastore) *transactionalDatastore {
	return &transactionalDatastore{Datastore: ds}
}

// Close is a no-op because the underlying *pgxpool.Pool is owned and closed by
// the caller (cmd/ateapi/main.go), and OpenFGA's server.Close() calls datastore.Close().
// Upstream equivalent: (*postgres.Datastore).Close (postgres.go:307-314).
func (d *transactionalDatastore) Close() {}

// ReadAuthorizationModel checks for an active pgx.Tx on ctx. If present (e.g.
// during fgaServer.Write tuple validation), it queries the authorization_model
// table on that transaction so Write never needs to check out a second connection
// from the pool. When no pgx.Tx is in ctx (e.g. during fgaServer.Check or
// EnsureStoreAndModel), it delegates to the underlying pool datastore.
//
// 1:1 with (*postgres.Datastore).ReadAuthorizationModel (postgres.go:831-858),
// except rows are queried via tx.Query instead of db.Query when tx is present.
func (d *transactionalDatastore) ReadAuthorizationModel(ctx context.Context, store string, modelID string) (*openfgav1.AuthorizationModel, error) {
	tx, ok := TxFromContext(ctx)
	if !ok {
		return d.Datastore.ReadAuthorizationModel(ctx, store, modelID)
	}
	stmt, args, err := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).
		Select("authorization_model_id", "schema_version", "type", "type_definition", "serialized_protobuf").
		From("authorization_model").
		Where(sq.Eq{
			"store":                  store,
			"authorization_model_id": modelID,
		}).ToSql()
	if err != nil {
		return nil, postgres.HandleSQLError(err)
	}
	rows, err := tx.Query(ctx, stmt, args...)
	if err != nil {
		return nil, postgres.HandleSQLError(err)
	}
	defer rows.Close()
	ret, err := sqlcommon.ConstructAuthorizationModelFromSQLRows(&pgxRowsWrapper{rows: rows})
	if err != nil {
		return nil, postgres.HandleSQLError(err)
	}
	return ret, nil
}

// ReadPage requires an active pgx.Tx on ctx via ContextWithTx and executes the
// paginated tuple query on that transaction. If no transaction is present in ctx,
// it fails fast with ErrNoTransactionInContext.
//
// 1:1 with (*postgres.Datastore).ReadPage (postgres.go:349-361).
func (d *transactionalDatastore) ReadPage(
	ctx context.Context,
	store string,
	filter storage.ReadFilter,
	options storage.ReadPageOptions,
) ([]*openfgav1.Tuple, string, error) {
	tx, ok := TxFromContext(ctx)
	if !ok {
		return nil, "", ErrNoTransactionInContext
	}
	iter, err := readOnTx(tx, store, filter, options)
	if err != nil {
		return nil, "", err
	}
	defer iter.Stop()
	return iter.ToArray(ctx, options.Pagination)
}

// readOnTx is a 1:1 copy of (*postgres.Datastore).read (postgres.go:363-418),
// replacing (*pgxPoolConnector)(db) with &pgxTxConnector{tx: tx}.
func readOnTx(
	tx pgx.Tx,
	store string,
	filter storage.ReadFilter,
	pageOpts storage.ReadPageOptions,
) (*sqlcommon.SQLTupleIterator, error) {
	stbl := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	sb := stbl.
		Select(sqlcommon.SQLIteratorColumns()...).
		From("tuple").
		Where(sq.Eq{"store": store}).
		OrderBy("ulid")
	objectType, objectID := tupleUtils.SplitObject(filter.Object)
	if objectType != "" {
		sb = sb.Where(sq.Eq{"object_type": objectType})
	}
	if objectID != "" {
		sb = sb.Where(sq.Eq{"object_id": objectID})
	}
	if filter.Relation != "" {
		sb = sb.Where(sq.Eq{"relation": filter.Relation})
	}
	if filter.User != "" {
		userType, userID, _ := tupleUtils.ToUserParts(filter.User)
		if userID != "" {
			sb = sb.Where(sq.Eq{"_user": filter.User})
		} else {
			sb = sb.Where(sq.Like{"_user": userType + ":%"})
		}
	}
	if len(filter.Conditions) > 0 {
		sb = sb.Where(sq.Eq{"COALESCE(condition_name, '')": filter.Conditions})
	}
	if pageOpts.Pagination.From != "" {
		sb = sb.Where(sq.GtOrEq{"ulid": pageOpts.Pagination.From})
	}
	if pageOpts.Pagination.PageSize != 0 {
		sb = sb.Limit(uint64(pageOpts.Pagination.PageSize + 1))
	}
	rowGetter, err := sqlcommon.NewRowGetter(&pgxTxConnector{tx: tx}, sb)
	if err != nil {
		return nil, postgres.HandleSQLError(err)
	}
	return sqlcommon.NewSQLTupleIterator(rowGetter, postgres.HandleSQLError), nil
}

// Write requires an active pgx.Tx on ctx via ContextWithTx and executes the
// write on that transaction without calling BeginTx or Commit. If no transaction
// is present in ctx, it fails fast with ErrNoTransactionInContext.
//
// 1:1 with (*postgres.Datastore).Write (postgres.go:421-431).
func (d *transactionalDatastore) Write(
	ctx context.Context,
	store string,
	deletes storage.Deletes,
	writes storage.Writes,
	opts ...storage.TupleWriteOption,
) error {
	tx, ok := TxFromContext(ctx)
	if !ok {
		return ErrNoTransactionInContext
	}
	return d.writeOnTx(ctx, tx, store, deletes, writes, storage.NewTupleWriteOptions(opts...), time.Now().UTC())
}

// writeOnTx is a 1:1 copy of (*postgres.Datastore).write (postgres.go:584-649)
// and selectAllExistingRowsForUpdate (postgres.go:435-455), except that it uses
// the caller-supplied tx instead of calling s.primaryDB.BeginTx / txn.Rollback / txn.Commit.
func (d *transactionalDatastore) writeOnTx(
	ctx context.Context,
	tx pgx.Tx,
	store string,
	deletes storage.Deletes,
	writes storage.Writes,
	opts storage.TupleWriteOptions,
	now time.Time,
) error {
	lockKeys := sqlcommon.MakeTupleLockKeys(deletes, writes)
	if len(lockKeys) == 0 {
		return nil
	}

	connector := &pgxTxConnector{tx: tx}
	stbl := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	existing := make(map[string]*openfgav1.Tuple, len(lockKeys))

	for start := 0; start < len(lockKeys); start += storage.DefaultMaxTuplesPerWrite {
		end := start + storage.DefaultMaxTuplesPerWrite
		if end > len(lockKeys) {
			end = len(lockKeys)
		}
		if err := selectExistingRowsForWrite(ctx, stbl, connector, store, lockKeys[start:end], existing); err != nil {
			return err
		}
	}

	deleteConditions, writeItems, changeLogItems, err := sqlcommon.GetDeleteWriteChangelogItems(store, existing,
		sqlcommon.WriteData{
			Deletes: deletes,
			Writes:  writes,
			Opts:    opts,
			Now:     now,
		})
	if err != nil {
		return err
	}

	if err := executeDeleteTuples(ctx, tx, store, deleteConditions); err != nil {
		return err
	}
	if err := executeWriteTuples(ctx, tx, writeItems); err != nil {
		return err
	}
	if err := executeInsertChanges(ctx, tx, changeLogItems); err != nil {
		return err
	}
	return nil
}

// selectExistingRowsForWrite is a 1:1 copy of upstream selectExistingRowsForWrite
// in github.com/openfga/openfga/pkg/storage/postgres/postgres.go.
func selectExistingRowsForWrite(
	ctx context.Context,
	stbl sq.StatementBuilderType,
	connector sqlcommon.Connector,
	store string,
	keys []sqlcommon.TupleLockKey,
	existing map[string]*openfgav1.Tuple,
) error {
	inExpr, args := sqlcommon.BuildRowConstructorIN(keys)
	sb := stbl.
		Select(sqlcommon.SQLIteratorColumns()...).
		From("tuple").
		Where(sq.Eq{"store": store}).
		Where(sq.Expr("(object_type, object_id, relation, _user, user_type) IN "+inExpr, args...)).
		Suffix("FOR UPDATE")

	poolGetRows, err := sqlcommon.NewRowGetter(connector, sb)
	if err != nil {
		return postgres.HandleSQLError(err)
	}

	iter := sqlcommon.NewSQLTupleIterator(poolGetRows, postgres.HandleSQLError)
	defer iter.Stop()

	items, _, err := iter.ToArray(ctx, storage.PaginationOptions{PageSize: len(keys)})
	if err != nil {
		return err
	}
	for _, tuple := range items {
		existing[tupleUtils.TupleKeyToString(tuple.GetKey())] = tuple
	}
	return nil
}

// executeDeleteTuples is a 1:1 copy of upstream executeDeleteTuples (postgres.go:458-488).
func executeDeleteTuples(ctx context.Context, tx pgx.Tx, store string, deleteConditions sq.Or) error {
	stbl := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	for start, totalDeletes := 0, len(deleteConditions); start < totalDeletes; start += storage.DefaultMaxTuplesPerWrite {
		end := start + storage.DefaultMaxTuplesPerWrite
		if end > totalDeletes {
			end = totalDeletes
		}

		deleteConditionsBatch := deleteConditions[start:end]
		stmt, args, err := stbl.Delete("tuple").
			Where(sq.Eq{"store": store}).
			Where(deleteConditionsBatch).
			ToSql()
		if err != nil {
			return postgres.HandleSQLError(err)
		}

		res, err := tx.Exec(ctx, stmt, args...)
		if err != nil {
			return postgres.HandleSQLError(err)
		}
		if res.RowsAffected() != int64(len(deleteConditionsBatch)) {
			return storage.ErrWriteConflictOnDelete
		}
	}
	return nil
}

// executeWriteTuples is a 1:1 copy of upstream executeWriteTuples (postgres.go:491-538).
func executeWriteTuples(ctx context.Context, tx pgx.Tx, writeItems [][]interface{}) error {
	stbl := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	for start, totalWrites := 0, len(writeItems); start < totalWrites; start += storage.DefaultMaxTuplesPerWrite {
		end := start + storage.DefaultMaxTuplesPerWrite
		if end > totalWrites {
			end = totalWrites
		}

		writesBatch := writeItems[start:end]
		insertBuilder := stbl.
			Insert("tuple").
			Columns(
				"store",
				"object_type",
				"object_id",
				"relation",
				"_user",
				"user_type",
				"condition_name",
				"condition_context",
				"ulid",
				"inserted_at",
			)

		for _, item := range writesBatch {
			insertBuilder = insertBuilder.Values(item...)
		}

		stmt, args, err := insertBuilder.ToSql()
		if err != nil {
			return postgres.HandleSQLError(err)
		}

		_, err = tx.Exec(ctx, stmt, args...)
		if err != nil {
			dberr := postgres.HandleSQLError(err)
			if errors.Is(dberr, storage.ErrCollision) {
				return storage.ErrWriteConflictOnInsert
			}
			return dberr
		}
	}
	return nil
}

// executeInsertChanges is a 1:1 copy of upstream executeInsertChanges (postgres.go:540-582).
func executeInsertChanges(ctx context.Context, tx pgx.Tx, changeLogItems [][]interface{}) error {
	stbl := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	for start, totalItems := 0, len(changeLogItems); start < totalItems; start += storage.DefaultMaxTuplesPerWrite {
		end := start + storage.DefaultMaxTuplesPerWrite
		if end > totalItems {
			end = totalItems
		}

		changeLogBatch := changeLogItems[start:end]
		changelogBuilder := stbl.
			Insert("changelog").
			Columns(
				"store",
				"object_type",
				"object_id",
				"relation",
				"_user",
				"condition_name",
				"condition_context",
				"operation",
				"ulid",
				"inserted_at",
			)

		for _, item := range changeLogBatch {
			changelogBuilder = changelogBuilder.Values(item...)
		}

		stmt, args, err := changelogBuilder.ToSql()
		if err != nil {
			return postgres.HandleSQLError(err)
		}

		_, err = tx.Exec(ctx, stmt, args...)
		if err != nil {
			return postgres.HandleSQLError(err)
		}
	}
	return nil
}

// pgxTxConnector, pgxTxConnection, and pgxRowsWrapper are 1:1 copies of the
// unexported adapter types in github.com/openfga/openfga/pkg/storage/postgres/postgres.go:101-190.
type pgxTxConnector struct {
	tx pgx.Tx
}

var _ sqlcommon.Connector = (*pgxTxConnector)(nil)

func (c *pgxTxConnector) Connect(ctx context.Context) (sqlcommon.Connection, error) {
	return &pgxTxConnection{tx: c.tx}, nil
}

type pgxTxConnection struct {
	tx pgx.Tx
}

var _ sqlcommon.Connection = (*pgxTxConnection)(nil)

func (c *pgxTxConnection) Query(ctx context.Context, sql string, args ...any) (sqlcommon.Rows, error) {
	rows, err := c.tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &pgxRowsWrapper{rows: rows}, nil
}

func (c *pgxTxConnection) Close() error {
	return nil
}

type pgxRowsWrapper struct {
	rows pgx.Rows
}

var _ sqlcommon.Rows = (*pgxRowsWrapper)(nil)

func (r *pgxRowsWrapper) Err() error {
	return r.rows.Err()
}

func (r *pgxRowsWrapper) Next() bool {
	return r.rows.Next()
}

func (r *pgxRowsWrapper) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

func (r *pgxRowsWrapper) Close() error {
	r.rows.Close()
	return nil
}
