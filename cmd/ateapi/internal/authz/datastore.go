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
// without an active store transaction injected via ContextWithTx.
var ErrNoTransactionInContext = errors.New("authz datastore: active store transaction required in context")

// NOTE: The SQL query and tuple write/delete/changelog execution helpers in this
// file are 1:1 adaptations of unexported methods and package-private types in
// github.com/openfga/openfga/pkg/storage/postgres/postgres.go (pinned to
// PostgreSQL migration version 6 via TestOpenFGAMigrationVersionGuard in
// cmd/ateapi/internal/store/atepg/migrations_test.go) and of sqlcommon.Write in
// github.com/openfga/openfga/pkg/storage/sqlcommon/sqlcommon.go, which the
// upstream MySQL datastore uses. Both upstream paths run the same statements;
// they differ only in placeholder format, driver, and error mapping, which
// txStatements carries.
//
// Why this wrapper is necessary:
//   1. Upstream (*postgres.Datastore).Write (postgres.go:584-649) and
//      sqlcommon.Write (sqlcommon.go:987-1139) always open their own
//      transaction and commit it before returning, making it impossible to
//      atomically commit or roll back OpenFGA tuple mutations alongside
//      Substrate table mutations.
//   2. Upstream ReadPage / ReadAuthorizationModel always query the pool
//      instead of an active transaction, which both escapes the caller's
//      transaction snapshot (e.g. RepeatableRead / Serializable) and can
//      deadlock a pool when all connections are held by active write transactions.
//   3. Upstream Close (postgres.go:307-314, mysql.go:129-134) closes the
//      underlying pool, which would close Substrate's shared pool when
//      fgaServer.Close() is called.
//
// Transaction enforcement rules:
//   - Write and ReadPage (called by fgaServer.Write and fgaServer.Read) require
//     an active transaction for the datastore's backend in ctx via
//     ContextWithTx and fail fast with ErrNoTransactionInContext if absent.
//   - ReadAuthorizationModel uses the active transaction when present in ctx
//     (e.g. when fgaServer.Write validates tuples against the model), and falls
//     back to the connection pool when called outside a transaction (e.g.
//     during fgaServer.Check or EnsureStoreAndModel).
//   - All other datastore methods (e.g. Read, ReadUserTuple, ReadUsersetTuples)
//     are inherited from the embedded upstream datastore and run on the pool.

// transactionalDatastore wraps an upstream OpenFGA datastore so ReadPage and
// Write join the store transaction passed via ContextWithTx.
type transactionalDatastore struct {
	storage.OpenFGADatastore
	mysql bool
}

// txFromContext returns the statements of the store transaction in ctx if it
// belongs to d's backend.
func (d *transactionalDatastore) txFromContext(ctx context.Context) (*txStatements, bool) {
	tx, ok := TxFromContext(ctx)
	if !ok || tx.q.mysql != d.mysql {
		return nil, false
	}
	return tx.q, true
}

// Close is a no-op because the caller (cmd/ateapi/main.go) owns the pool, and
// OpenFGA's server.Close() calls datastore.Close().
func (d *transactionalDatastore) Close() {}

// ReadAuthorizationModel runs on the store transaction in ctx when present,
// and otherwise on the upstream pool datastore.
func (d *transactionalDatastore) ReadAuthorizationModel(ctx context.Context, store string, modelID string) (*openfgav1.AuthorizationModel, error) {
	q, ok := d.txFromContext(ctx)
	if !ok {
		return d.OpenFGADatastore.ReadAuthorizationModel(ctx, store, modelID)
	}
	return readAuthorizationModelOnTx(ctx, q, store, modelID)
}

// ReadPage runs the paginated tuple query on the store transaction in ctx.
func (d *transactionalDatastore) ReadPage(
	ctx context.Context,
	store string,
	filter storage.ReadFilter,
	options storage.ReadPageOptions,
) ([]*openfgav1.Tuple, string, error) {
	q, ok := d.txFromContext(ctx)
	if !ok {
		return nil, "", ErrNoTransactionInContext
	}
	return readPageOnTx(ctx, q, store, filter, options)
}

// Write runs the write on the store transaction in ctx without calling
// BeginTx or Commit.
func (d *transactionalDatastore) Write(
	ctx context.Context,
	store string,
	deletes storage.Deletes,
	writes storage.Writes,
	opts ...storage.TupleWriteOption,
) error {
	q, ok := d.txFromContext(ctx)
	if !ok {
		return ErrNoTransactionInContext
	}
	return writeOnTx(ctx, q, store, deletes, writes, storage.NewTupleWriteOptions(opts...), time.Now().UTC())
}

// txStatements runs OpenFGA statements on one caller-owned transaction. It
// holds what differs between the upstream PostgreSQL and MySQL datastores.
type txStatements struct {
	stbl        sq.StatementBuilderType
	connector   sqlcommon.Connector
	exec        func(ctx context.Context, stmt string, args ...any) (rowsAffected int64, err error)
	handleError func(err error, args ...any) error
	mysql       bool
}

func pgxTxStatements(tx pgx.Tx) *txStatements {
	return &txStatements{
		stbl:      sq.StatementBuilder.PlaceholderFormat(sq.Dollar),
		connector: &pgxTxConnector{tx: tx},
		exec: func(ctx context.Context, stmt string, args ...any) (int64, error) {
			tag, err := tx.Exec(ctx, stmt, args...)
			if err != nil {
				return 0, err
			}
			return tag.RowsAffected(), nil
		},
		handleError: postgres.HandleSQLError,
	}
}

// readAuthorizationModelOnTx is a 1:1 copy of the query in
// (*postgres.Datastore).ReadAuthorizationModel (postgres.go:831-858) and
// sqlcommon.ReadAuthorizationModel (sqlcommon.go:1267-1290), run on q's
// transaction.
func readAuthorizationModelOnTx(ctx context.Context, q *txStatements, store, modelID string) (*openfgav1.AuthorizationModel, error) {
	stmt, args, err := q.stbl.
		Select("authorization_model_id", "schema_version", "type", "type_definition", "serialized_protobuf").
		From("authorization_model").
		Where(sq.Eq{
			"store":                  store,
			"authorization_model_id": modelID,
		}).ToSql()
	if err != nil {
		return nil, q.handleError(err)
	}
	conn, err := q.connector.Connect(ctx)
	if err != nil {
		return nil, q.handleError(err)
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, stmt, args...)
	if err != nil {
		return nil, q.handleError(err)
	}
	defer rows.Close()
	ret, err := sqlcommon.ConstructAuthorizationModelFromSQLRows(rows)
	if err != nil {
		return nil, q.handleError(err)
	}
	return ret, nil
}

// readPageOnTx is a 1:1 copy of (*postgres.Datastore).ReadPage
// (postgres.go:349-361) and (*mysql.Datastore).ReadPage (mysql.go:150-161).
func readPageOnTx(
	ctx context.Context,
	q *txStatements,
	store string,
	filter storage.ReadFilter,
	options storage.ReadPageOptions,
) ([]*openfgav1.Tuple, string, error) {
	iter, err := readOnTx(q, store, filter, options)
	if err != nil {
		return nil, "", err
	}
	defer iter.Stop()
	return iter.ToArray(ctx, options.Pagination)
}

// readOnTx is a 1:1 copy of (*postgres.Datastore).read (postgres.go:363-418)
// and (*mysql.Datastore).read (mysql.go:163-218), replacing the pool
// connector with q's transaction connector.
func readOnTx(
	q *txStatements,
	store string,
	filter storage.ReadFilter,
	pageOpts storage.ReadPageOptions,
) (*sqlcommon.SQLTupleIterator, error) {
	sb := q.stbl.
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
	rowGetter, err := sqlcommon.NewRowGetter(q.connector, sb)
	if err != nil {
		return nil, q.handleError(err)
	}
	return sqlcommon.NewSQLTupleIterator(rowGetter, q.handleError), nil
}

// writeOnTx is a 1:1 copy of (*postgres.Datastore).write (postgres.go:584-649)
// and sqlcommon.Write (sqlcommon.go:987-1139), except that it uses the
// caller-supplied transaction instead of calling BeginTx / Rollback / Commit.
func writeOnTx(
	ctx context.Context,
	q *txStatements,
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

	existing := make(map[string]*openfgav1.Tuple, len(lockKeys))

	for start := 0; start < len(lockKeys); start += storage.DefaultMaxTuplesPerWrite {
		end := start + storage.DefaultMaxTuplesPerWrite
		if end > len(lockKeys) {
			end = len(lockKeys)
		}
		if err := selectExistingRowsForWrite(ctx, q, store, lockKeys[start:end], existing); err != nil {
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

	if err := executeDeleteTuples(ctx, q, store, deleteConditions); err != nil {
		return err
	}
	if err := executeWriteTuples(ctx, q, writeItems); err != nil {
		return err
	}
	if err := executeInsertChanges(ctx, q, changeLogItems); err != nil {
		return err
	}
	return nil
}

// selectExistingRowsForWrite is a 1:1 copy of upstream selectExistingRowsForWrite
// in postgres.go:1401 and sqlcommon.go:819-844.
func selectExistingRowsForWrite(
	ctx context.Context,
	q *txStatements,
	store string,
	keys []sqlcommon.TupleLockKey,
	existing map[string]*openfgav1.Tuple,
) error {
	inExpr, args := sqlcommon.BuildRowConstructorIN(keys)
	sb := q.stbl.
		Select(sqlcommon.SQLIteratorColumns()...).
		From("tuple").
		Where(sq.Eq{"store": store}).
		Where(sq.Expr("(object_type, object_id, relation, _user, user_type) IN "+inExpr, args...)).
		Suffix("FOR UPDATE")

	rowGetter, err := sqlcommon.NewRowGetter(q.connector, sb)
	if err != nil {
		return q.handleError(err)
	}

	iter := sqlcommon.NewSQLTupleIterator(rowGetter, q.handleError)
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

// executeDeleteTuples is a 1:1 copy of upstream executeDeleteTuples
// (postgres.go:458-488) and the delete loop in sqlcommon.Write
// (sqlcommon.go:1032-1057).
func executeDeleteTuples(ctx context.Context, q *txStatements, store string, deleteConditions sq.Or) error {
	for start, totalDeletes := 0, len(deleteConditions); start < totalDeletes; start += storage.DefaultMaxTuplesPerWrite {
		end := start + storage.DefaultMaxTuplesPerWrite
		if end > totalDeletes {
			end = totalDeletes
		}

		deleteConditionsBatch := deleteConditions[start:end]
		stmt, args, err := q.stbl.Delete("tuple").
			Where(sq.Eq{"store": store}).
			Where(deleteConditionsBatch).
			ToSql()
		if err != nil {
			return q.handleError(err)
		}

		rowsAffected, err := q.exec(ctx, stmt, args...)
		if err != nil {
			return q.handleError(err)
		}
		if rowsAffected != int64(len(deleteConditionsBatch)) {
			return storage.ErrWriteConflictOnDelete
		}
	}
	return nil
}

// executeWriteTuples is a 1:1 copy of upstream executeWriteTuples
// (postgres.go:491-538) and the insert loop in sqlcommon.Write
// (sqlcommon.go:1059-1097).
func executeWriteTuples(ctx context.Context, q *txStatements, writeItems [][]interface{}) error {
	for start, totalWrites := 0, len(writeItems); start < totalWrites; start += storage.DefaultMaxTuplesPerWrite {
		end := start + storage.DefaultMaxTuplesPerWrite
		if end > totalWrites {
			end = totalWrites
		}

		writesBatch := writeItems[start:end]
		insertBuilder := q.stbl.
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
			return q.handleError(err)
		}

		if _, err := q.exec(ctx, stmt, args...); err != nil {
			dberr := q.handleError(err)
			if errors.Is(dberr, storage.ErrCollision) {
				return storage.ErrWriteConflictOnInsert
			}
			return dberr
		}
	}
	return nil
}

// executeInsertChanges is a 1:1 copy of upstream executeInsertChanges
// (postgres.go:540-582) and the changelog loop in sqlcommon.Write
// (sqlcommon.go:1100-1131).
func executeInsertChanges(ctx context.Context, q *txStatements, changeLogItems [][]interface{}) error {
	for start, totalItems := 0, len(changeLogItems); start < totalItems; start += storage.DefaultMaxTuplesPerWrite {
		end := start + storage.DefaultMaxTuplesPerWrite
		if end > totalItems {
			end = totalItems
		}

		changeLogBatch := changeLogItems[start:end]
		changelogBuilder := q.stbl.
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
			return q.handleError(err)
		}

		if _, err := q.exec(ctx, stmt, args...); err != nil {
			return q.handleError(err)
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
