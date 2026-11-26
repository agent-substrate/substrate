// Copyright 2026 The Agent Substrate Authors
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

package atepg

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// atespaceLockStripes shards shared advisory locks across 64 2-arg lock
	// tags so thousands of concurrent CreateActor/CreateTag calls in one
	// atespace never dirty the atespaces heap page or allocate MultiXacts,
	// while DeleteAtespace locks all 64 stripes exclusively in ascending order.
	atespaceLockStripes         = 64
	atespaceLockNamespacePrefix = "atespace:"

	atespaceHasActorsSQL = `SELECT EXISTS (SELECT 1 FROM actors WHERE atespace = $1)`
)

func atespaceLockStripe() int32 {
	return rand.Int32N(atespaceLockStripes)
}

// insertIntoAtespace sends the striped shared advisory lock and the guarded
// INSERT as two separate statements inside one pgx.Batch on a single
// transaction. Under READ COMMITTED, PostgreSQL acquires the INSERT statement's
// MVCC snapshot only AFTER pg_advisory_xact_lock_shared returns, so a
// concurrent DeleteAtespace that held the exclusive lock and committed while
// this transaction waited is guaranteed to be visible to the EXISTS check.
func (p *Persistence) insertIntoAtespace(ctx context.Context, atespace, insertSQL string, args ...any) (pgconn.CommandTag, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	var b pgx.Batch
	b.Queue(`SELECT pg_advisory_xact_lock_shared(hashtext($1 || $2), $3::int4)`,
		atespaceLockNamespacePrefix, atespace, atespaceLockStripe())
	b.Queue(insertSQL, args...)
	br := tx.SendBatch(ctx, &b)
	_, lockErr := br.Exec()
	var tag pgconn.CommandTag
	var insErr error
	if lockErr == nil {
		tag, insErr = br.Exec()
	}
	closeErr := br.Close()
	if err := errors.Join(lockErr, insErr, closeErr); err != nil {
		return pgconn.CommandTag{}, err
	}
	if tag.RowsAffected() == 0 {
		return tag, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return pgconn.CommandTag{}, err
	}
	return tag, nil
}

func (p *Persistence) CreateAtespace(ctx context.Context, atespace *ateapi.Atespace) (*ateapi.Atespace, error) {
	name := atespace.GetMetadata().GetName()

	dbAtespace := proto.Clone(atespace).(*ateapi.Atespace)
	setCreateMetadata(dbAtespace.Metadata)

	protoBytes, err := proto.Marshal(dbAtespace)
	if err != nil {
		return nil, fmt.Errorf("marshaling atespace: %w", err)
	}

	_, err = p.pool.Exec(ctx, `
		INSERT INTO atespaces (name, uid, version, proto)
		VALUES ($1, $2, $3, $4)`,
		name, dbAtespace.GetMetadata().GetUid(), dbAtespace.GetMetadata().GetVersion(), protoBytes)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, store.ErrAlreadyExists
		}
		return nil, fmt.Errorf("inserting atespace %q: %w", name, err)
	}
	return dbAtespace, nil
}

func (p *Persistence) GetAtespace(ctx context.Context, name string) (*ateapi.Atespace, error) {
	var protoBytes []byte
	err := p.pool.QueryRow(ctx, `SELECT proto FROM atespaces WHERE name = $1`, name).Scan(&protoBytes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting atespace %q: %w", name, err)
	}
	out := &ateapi.Atespace{}
	if err := unmarshalStored(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling atespace: %w", err)
	}
	return out, nil
}

func (p *Persistence) ListAtespaces(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapi.Atespace], error) {
	opts, err := store.NormalizeListOptions(opts)
	if err != nil {
		return store.ListResponse[*ateapi.Atespace]{}, err
	}
	pageSize, pageTokenStr := opts.PageSize, opts.PageToken
	token, err := decodePageToken(pageTokenStr, kindAtespace, "", 1)
	if err != nil {
		return store.ListResponse[*ateapi.Atespace]{}, err
	}
	var last *string
	if len(token.Last) > 0 {
		last = &token.Last[0]
	}

	rows, err := p.pool.Query(ctx, `
		SELECT name, proto FROM atespaces
		WHERE $1::text IS NULL OR name > $1
		ORDER BY name
		LIMIT $2`, last, int64(pageSize)+1)
	if err != nil {
		return store.ListResponse[*ateapi.Atespace]{}, fmt.Errorf("listing atespaces: %w", err)
	}
	defer rows.Close()

	var names []string
	var result []*ateapi.Atespace
	for rows.Next() {
		var name string
		var protoBytes []byte
		if err := rows.Scan(&name, &protoBytes); err != nil {
			return store.ListResponse[*ateapi.Atespace]{}, fmt.Errorf("scanning atespace row: %w", err)
		}
		a := &ateapi.Atespace{}
		if err := unmarshalRow(protoBytes, a, "atespace", name); err != nil {
			return store.ListResponse[*ateapi.Atespace]{}, err
		}
		result = append(result, a)
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return store.ListResponse[*ateapi.Atespace]{}, fmt.Errorf("listing atespaces: %w", err)
	}

	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		nextToken = encodePageToken(kindAtespace, "", []string{names[pageSize-1]})
	}
	return store.ListResponse[*ateapi.Atespace]{Items: result, NextPageToken: nextToken}, nil
}

func (p *Persistence) DeleteAtespace(ctx context.Context, name string, precondition store.Precondition) (*ateapi.Atespace, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning atespace delete: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	// Lock all 64 stripes in ascending order so in-flight CreateActor/CreateTag
	// statements on this atespace finish before the emptiness checks below, and
	// new ones wait until this delete commits or rolls back.
	if _, err := tx.Exec(ctx, `
		SELECT pg_advisory_xact_lock(hashtext($1 || $2), s)
		FROM generate_series(0, $3::int4 - 1) AS s`,
		atespaceLockNamespacePrefix, name, atespaceLockStripes); err != nil {
		return nil, fmt.Errorf("locking atespace %q for delete: %w", name, err)
	}

	var protoBytes []byte
	err = tx.QueryRow(ctx, `
		DELETE FROM atespaces
		WHERE name = $1
		  AND ($2::text = '' OR uid = $2::text)
		  AND ($3::bigint = 0 OR version = $3::bigint)
		RETURNING proto`, name, precondition.UID, precondition.Version).Scan(&protoBytes)
	if err != nil {
		if isForeignKeyViolation(err) {
			return nil, store.ErrFailedPrecondition
		}
		if errors.Is(err, pgx.ErrNoRows) {
			var uid string
			var version int64
			err := tx.QueryRow(ctx, `SELECT uid, version FROM atespaces WHERE name = $1`, name).Scan(&uid, &version)
			return nil, mapDeleteError(err, uid, version, precondition)
		}
		return nil, fmt.Errorf("deleting atespace %q: %w", name, err)
	}
	var hasActors bool
	if err := tx.QueryRow(ctx, atespaceHasActorsSQL, name).Scan(&hasActors); err != nil {
		return nil, fmt.Errorf("checking actors in atespace %q: %w", name, err)
	}
	if hasActors {
		return nil, store.ErrFailedPrecondition
	}
	var hasTags bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM tags WHERE atespace = $1)`, name).Scan(&hasTags); err != nil {
		return nil, fmt.Errorf("checking tags in atespace %q: %w", name, err)
	}
	if hasTags {
		return nil, store.ErrFailedPrecondition
	}
	if err := p.policyManager.DeleteAtespacePolicies(ctx, tx, name); err != nil {
		return nil, fmt.Errorf("deleting authorization tuples for atespace %q: %w", name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing atespace delete %q: %w", name, err)
	}
	out := &ateapi.Atespace{}
	if err := unmarshalStored(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling deleted atespace: %w", err)
	}
	out.Metadata.DeleteTime = timestamppb.Now()
	return out, nil
}
