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

	"github.com/jackc/pgx/v5"
)

// Tx is the store transaction that OpenFGA tuple reads and writes join. Build
// it with PgxTx for the PostgreSQL store or SQLTx for the MySQL store; the
// zero value carries no transaction.
type Tx struct {
	q *txStatements
}

// PgxTx wraps a PostgreSQL store transaction.
func PgxTx(tx pgx.Tx) Tx {
	if tx == nil {
		return Tx{}
	}
	return Tx{q: pgxTxStatements(tx)}
}

// SQLTx wraps a MySQL store transaction.
func SQLTx(tx *sql.Tx) Tx {
	if tx == nil {
		return Tx{}
	}
	return Tx{q: sqlTxStatements(tx)}
}

func (t Tx) isZero() bool { return t.q == nil }

type txContextKey struct{}

// ContextWithTx injects an active store transaction into ctx.
func ContextWithTx(ctx context.Context, tx Tx) context.Context {
	if tx.isZero() {
		return ctx
	}
	return context.WithValue(ctx, txContextKey{}, tx)
}

// TxFromContext retrieves the active store transaction from ctx, if present.
func TxFromContext(ctx context.Context) (Tx, bool) {
	tx, ok := ctx.Value(txContextKey{}).(Tx)
	return tx, ok && !tx.isZero()
}
