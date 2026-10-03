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
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storesql"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
)

func (p *Persistence) CreateWorker(ctx context.Context, worker *ateapipb.Worker) (*ateapipb.Worker, error) {
	dbWorker := proto.Clone(worker).(*ateapipb.Worker)
	if dbWorker.Metadata == nil {
		dbWorker.Metadata = &ateapipb.ResourceMetadata{}
	}
	storesql.SetCreateMetadata(dbWorker.Metadata)

	protoBytes, err := proto.Marshal(dbWorker)
	if err != nil {
		return nil, fmt.Errorf("marshaling worker: %w", err)
	}

	created, err := p.writeAndAppendEvent(ctx, store.WorkerEventCreated, func(ctx context.Context, tx *sql.Tx) (*ateapipb.Worker, error) {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO workers (name, uid, version, proto)
			VALUES (?, ?, ?, ?)`,
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
	err := q.QueryRowContext(ctx, `SELECT proto FROM workers WHERE name = ?`, name).Scan(&protoBytes)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting worker %s: %w", name, err)
	}
	out := &ateapipb.Worker{}
	if err := storesql.UnmarshalStored(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling worker: %w", err)
	}
	return out, nil
}

func (p *Persistence) GetWorker(ctx context.Context, name string) (*ateapipb.Worker, error) {
	return getWorkerRow(ctx, p.db, name)
}

func getWorkerRowForUpdate(ctx context.Context, tx *sql.Tx, name string) (*ateapipb.Worker, error) {
	var protoBytes []byte
	if err := tx.QueryRowContext(ctx, `SELECT proto FROM workers WHERE name = ? FOR UPDATE`, name).Scan(&protoBytes); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("locking worker %s for update: %w", name, err)
	}
	out := &ateapipb.Worker{}
	if err := storesql.UnmarshalStored(protoBytes, out); err != nil {
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
	return p.writeAndAppendEvent(ctx, store.WorkerEventUpdated, func(ctx context.Context, tx *sql.Tx) (*ateapipb.Worker, error) {
		dbWorker, err := getWorkerRowForUpdate(ctx, tx, name)
		if err != nil {
			return nil, err
		}
		if err := precondition.Check(dbWorker.GetMetadata()); err != nil {
			return nil, err
		}

		oldMeta := proto.CloneOf(dbWorker.GetMetadata())
		if err := mutate(dbWorker); err != nil {
			return nil, err
		}
		// Stored metadata is authoritative; discard any metadata edits made by
		// the closure and derive the next revision from the row we locked.
		storesql.SetUpdateMetadata(dbWorker.Metadata, oldMeta)

		protoBytes, err := proto.Marshal(dbWorker)
		if err != nil {
			return nil, fmt.Errorf("marshaling worker: %w", err)
		}
		res, err := tx.ExecContext(ctx, `
			UPDATE workers
			SET version = ?, proto = ?
			WHERE name = ?`,
			dbWorker.GetMetadata().GetVersion(), protoBytes, name)
		if err != nil {
			return nil, fmt.Errorf("updating worker %s: %w", name, err)
		}
		if err := requireOneRow(res, "updating worker "+name); err != nil {
			return nil, err
		}
		return dbWorker, nil
	})
}

// DeleteWorker removes the worker's assignments with it.
func (p *Persistence) DeleteWorker(ctx context.Context, name string, precondition store.DeletePreconditions) (*ateapipb.Worker, error) {
	return p.writeAndAppendEvent(ctx, store.WorkerEventDeleted, func(ctx context.Context, tx *sql.Tx) (*ateapipb.Worker, error) {
		// Locked rather than plainly read so the incarnation precondition was
		// evaluated against is the one the DELETE removes.
		deleted, err := getWorkerRowForUpdate(ctx, tx, name)
		if err != nil {
			return nil, err
		}
		if err := precondition.Check(deleted.GetMetadata()); err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM worker_assignments WHERE worker_name = ?`, name); err != nil {
			return nil, fmt.Errorf("deleting assignments of worker %s: %w", name, err)
		}
		res, err := tx.ExecContext(ctx, `DELETE FROM workers WHERE name = ?`, name)
		if err != nil {
			return nil, fmt.Errorf("deleting worker %s: %w", name, err)
		}
		if err := requireOneRow(res, "deleting worker "+name); err != nil {
			return nil, err
		}
		return deleted, nil
	})
}

func (p *Persistence) ListWorkers(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapipb.Worker], error) {
	opts, err := store.NormalizeListOptions(opts)
	if err != nil {
		return store.ListResponse[*ateapipb.Worker]{}, err
	}
	token, err := storesql.DecodePageToken(opts.PageToken, storesql.KindWorker, "", 1)
	if err != nil {
		return store.ListResponse[*ateapipb.Worker]{}, err
	}
	query, args := `SELECT name, proto FROM workers ORDER BY name LIMIT ?`, []any{int64(opts.PageSize) + 1}
	if len(token.Last) > 0 {
		query, args = `SELECT name, proto FROM workers WHERE name > ? ORDER BY name LIMIT ?`, []any{token.Last[0], int64(opts.PageSize) + 1}
	}
	items, next, err := listPage(ctx, p.db, storesql.KindWorker, "", opts.PageSize, func(b []byte, keys []string) (*ateapipb.Worker, error) {
		w := &ateapipb.Worker{}
		return w, storesql.UnmarshalRow(b, w, "worker", keys...)
	}, query, args...)
	if err != nil {
		return store.ListResponse[*ateapipb.Worker]{}, fmt.Errorf("listing workers: %w", err)
	}
	return store.ListResponse[*ateapipb.Worker]{Items: items, NextPageToken: next}, nil
}
