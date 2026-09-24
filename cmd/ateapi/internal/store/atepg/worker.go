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

package atepg

import (
	"context"
	"errors"
	"fmt"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
)

func (p *Persistence) CreateWorker(ctx context.Context, worker *ateapipb.Worker) (*ateapipb.Worker, error) {
	dbWorker := proto.Clone(worker).(*ateapipb.Worker)
	if dbWorker.Metadata == nil {
		dbWorker.Metadata = &ateapipb.ResourceMetadata{}
	}
	setCreateMetadata(dbWorker.Metadata)

	protoBytes, err := proto.Marshal(dbWorker)
	if err != nil {
		return nil, fmt.Errorf("marshaling worker: %w", err)
	}

	created, err := p.writeAndAppendEvent(ctx, store.WorkerEventCreated, func(ctx context.Context, tx pgx.Tx) (*ateapipb.Worker, error) {
		_, err := tx.Exec(ctx, `
			INSERT INTO workers (name, uid, version, proto)
			VALUES ($1, $2, $3, $4)`,
			dbWorker.GetMetadata().GetName(), dbWorker.GetMetadata().GetUid(), dbWorker.GetMetadata().GetVersion(), protoBytes)
		if err != nil {
			return nil, err
		}
		return dbWorker, nil
	})
	if err != nil {
		if isUniqueViolation(err) {
			return nil, store.ErrAlreadyExists
		}
		return nil, fmt.Errorf("creating worker: %w", err)
	}
	return created, nil
}

func getWorkerRow(ctx context.Context, q querier, name string) (*ateapipb.Worker, error) {
	var protoBytes []byte
	err := q.QueryRow(ctx, `SELECT proto FROM workers WHERE name = $1`, name).Scan(&protoBytes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting worker %s: %w", name, err)
	}
	out := &ateapipb.Worker{}
	if err := unmarshalStored(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling worker: %w", err)
	}
	return out, nil
}

func (p *Persistence) GetWorker(ctx context.Context, name string) (*ateapipb.Worker, error) {
	return getWorkerRow(ctx, p.pool, name)
}

// getWorkerRowForUpdate reads the worker and holds its row lock for the rest of
// tx, so nothing else can write the row between this read and the write that
// follows it.
func getWorkerRowForUpdate(ctx context.Context, tx pgx.Tx, name string) (*ateapipb.Worker, error) {
	var protoBytes []byte
	if err := tx.QueryRow(ctx, `SELECT proto FROM workers WHERE name = $1 FOR UPDATE`, name).Scan(&protoBytes); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("locking worker %s for update: %w", name, err)
	}
	out := &ateapipb.Worker{}
	if err := unmarshalStored(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling worker: %w", err)
	}
	return out, nil
}

// UpdateWorker runs mutate against the worker read FOR UPDATE inside the write
// transaction, so a concurrent writer blocks on the row lock rather than
// interleaving. That is what makes an occupancy test inside mutate a
// compare-and-set. The predicate cannot be pushed into SQL: the row stores an
// opaque marshaled proto, so assignment is not addressable in a WHERE clause.
func (p *Persistence) UpdateWorker(ctx context.Context, name string, precondition store.Precondition, mutate func(*ateapipb.Worker) error) (*ateapipb.Worker, error) {
	if err := precondition.Validate(); err != nil {
		return nil, err
	}
	return p.writeAndAppendEvent(ctx, store.WorkerEventUpdated, func(ctx context.Context, tx pgx.Tx) (*ateapipb.Worker, error) {
		dbWorker, err := getWorkerRowForUpdate(ctx, tx, name)
		if err != nil {
			return nil, err
		}
		if err := precondition.Check(dbWorker.GetMetadata()); err != nil {
			return nil, err
		}

		// Snapshot the stored metadata before handing the worker to mutate.
		// mutate is free to edit anything it is given; immutable fields are
		// the service layer's to enforce, via declarative validation.
		oldMeta := proto.CloneOf(dbWorker.GetMetadata())
		if err := mutate(dbWorker); err != nil {
			return nil, err
		}
		// Stored metadata is authoritative; discard any metadata edits made by
		// the closure and derive the next revision from the row we locked.
		setUpdateMetadata(dbWorker.Metadata, oldMeta)

		protoBytes, err := proto.Marshal(dbWorker)
		if err != nil {
			return nil, fmt.Errorf("marshaling worker: %w", err)
		}

		commandTag, err := tx.Exec(ctx, `
			UPDATE workers
			SET version = $1, proto = $2
			WHERE name = $3`,
			dbWorker.GetMetadata().GetVersion(), protoBytes, name)
		if err != nil {
			return nil, fmt.Errorf("updating worker %s: %w", name, err)
		}
		if commandTag.RowsAffected() != 1 {
			return nil, fmt.Errorf("updating worker %s affected %d rows, want 1", name, commandTag.RowsAffected())
		}
		return dbWorker, nil
	})
}

func (p *Persistence) DeleteWorker(ctx context.Context, name string, precondition store.DeletePreconditions) (*ateapipb.Worker, error) {
	return p.writeAndAppendEvent(ctx, store.WorkerEventDeleted, func(ctx context.Context, tx pgx.Tx) (*ateapipb.Worker, error) {
		// Locked rather than plainly read so the incarnation precondition was evaluated
		// against is the one the DELETE removes.
		deleted, err := getWorkerRowForUpdate(ctx, tx, name)
		if err != nil {
			return nil, err
		}
		if err := precondition.Check(deleted.GetMetadata()); err != nil {
			return nil, err
		}
		commandTag, err := tx.Exec(ctx, `DELETE FROM workers WHERE name = $1`, name)
		if err != nil {
			return nil, fmt.Errorf("deleting worker %s: %w", name, err)
		}
		if commandTag.RowsAffected() != 1 {
			return nil, fmt.Errorf("deleting worker %s affected %d rows, want 1", name, commandTag.RowsAffected())
		}
		return deleted, nil
	})
}

func (p *Persistence) ListWorkers(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapipb.Worker], error) {
	opts, err := store.NormalizeListOptions(opts)
	if err != nil {
		return store.ListResponse[*ateapipb.Worker]{}, err
	}
	pageSize, pageTokenStr := opts.PageSize, opts.PageToken
	token, err := decodePageToken(pageTokenStr, kindWorker, "", 1)
	if err != nil {
		return store.ListResponse[*ateapipb.Worker]{}, err
	}
	var last *string
	if len(token.Last) > 0 {
		last = &token.Last[0]
	}

	rows, err := p.pool.Query(ctx, `
		SELECT name, proto FROM workers
		WHERE $1::text IS NULL OR name > $1
		ORDER BY name
		LIMIT $2`, last, int64(pageSize)+1)
	if err != nil {
		return store.ListResponse[*ateapipb.Worker]{}, fmt.Errorf("listing workers: %w", err)
	}
	defer rows.Close()

	var names []string
	var result []*ateapipb.Worker
	for rows.Next() {
		var name string
		var protoBytes []byte
		if err := rows.Scan(&name, &protoBytes); err != nil {
			return store.ListResponse[*ateapipb.Worker]{}, fmt.Errorf("scanning worker row: %w", err)
		}
		w := &ateapipb.Worker{}
		if err := unmarshalStored(protoBytes, w); err != nil {
			return store.ListResponse[*ateapipb.Worker]{}, fmt.Errorf("unmarshaling worker: %w", err)
		}
		result = append(result, w)
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return store.ListResponse[*ateapipb.Worker]{}, fmt.Errorf("listing workers: %w", err)
	}

	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		nextToken = encodePageToken(kindWorker, "", []string{names[pageSize-1]})
	}
	return store.ListResponse[*ateapipb.Worker]{Items: result, NextPageToken: nextToken}, nil
}
