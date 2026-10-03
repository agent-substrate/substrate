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

package atemy

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// The helpers below stand in for the foreign keys atepg declares. A child
// insert takes a shared lock on its parent row, so the parent cannot be
// deleted until the insert commits. A parent delete takes an exclusive lock
// first, so it waits for in-flight child inserts and then sees their rows.

// lockAtespace takes a shared lock on an atespace for the rest of tx. Returns
// ErrFailedPrecondition if the atespace does not exist.
func lockAtespace(ctx context.Context, tx *sql.Tx, name string) error {
	return lockParent(ctx, tx, `SELECT 1 FROM atespaces WHERE name = ? FOR SHARE`, name)
}

// lockActor takes a shared lock on an actor for the rest of tx. Returns
// ErrFailedPrecondition if the actor does not exist.
func lockActor(ctx context.Context, tx *sql.Tx, atespace, name string) error {
	return lockParent(ctx, tx, `SELECT 1 FROM actors WHERE atespace = ? AND name = ? FOR SHARE`, atespace, name)
}

func lockParent(ctx context.Context, tx *sql.Tx, query string, args ...any) error {
	var found int
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&found); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return store.ErrFailedPrecondition
		}
		return fmt.Errorf("locking parent row: %w", err)
	}
	return nil
}

// insertChild inserts a row whose parent lock must hold until commit,
// reporting a taken key as ErrAlreadyExists. what names the row in errors.
func (p *Persistence) insertChild(ctx context.Context, what string, lock func(tx *sql.Tx) error, insert string, args ...any) error {
	return inTx(ctx, p.db, func(tx *sql.Tx) error {
		if err := lock(tx); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, insert, args...); err != nil {
			if isUniqueViolation(err) {
				return store.ErrAlreadyExists
			}
			return fmt.Errorf("inserting %s: %w", what, err)
		}
		return nil
	})
}

// deleteLockedRow locks the row of table that where selects, checks
// precondition against it, deletes it, and returns its columns. Returns ErrNotFound if no
// row matches. table and where are trusted SQL fragments.
func deleteLockedRow(ctx context.Context, tx *sql.Tx, table, where string, precondition store.DeletePreconditions, args ...any) (uid string, version int64, protoBytes []byte, err error) {
	if err := tx.QueryRowContext(ctx, `SELECT uid, version, proto FROM `+table+` WHERE `+where+` FOR UPDATE`, args...).Scan(&uid, &version, &protoBytes); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", 0, nil, store.ErrNotFound
		}
		return "", 0, nil, fmt.Errorf("locking %s row for delete: %w", table, err)
	}
	if err := precondition.Check(&ateapipb.ResourceMetadata{Uid: uid, Version: version}); err != nil {
		return "", 0, nil, err
	}
	res, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE `+where, args...)
	if err != nil {
		return "", 0, nil, fmt.Errorf("deleting %s row: %w", table, err)
	}
	if err := requireOneRow(res, "deleting "+table+" row"); err != nil {
		return "", 0, nil, err
	}
	return uid, version, protoBytes, nil
}

// updateGuarded writes a new version of a row only if it still carries the
// uid and version the caller read, reporting a lost race as
// ErrVersionConflict. what names the row in errors.
func updateGuarded(ctx context.Context, q querier, what, update string, args ...any) error {
	res, err := q.ExecContext(ctx, update, args...)
	if err != nil {
		return fmt.Errorf("updating %s: %w", what, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("updating %s: %w", what, err)
	}
	switch n {
	case 0:
		return store.ErrVersionConflict
	case 1:
		return nil
	default:
		return fmt.Errorf("updating %s affected %d rows, want 1", what, n)
	}
}
