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
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
)

// getWorkerForUpdate reads a Worker and holds its row until the caller's
// transaction commits, which is what serializes updates to its allocation. The
// caller supplies the transaction; this only takes the lock.
func getWorkerForUpdate(ctx context.Context, tx *sql.Tx, name string) (*ateapipb.Worker, error) {
	var protoBytes []byte
	err := tx.QueryRowContext(ctx, `SELECT proto FROM workers WHERE name = ? FOR UPDATE`, name).Scan(&protoBytes)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("locking worker %s: %w", name, err)
	}
	worker := &ateapipb.Worker{}
	if err := proto.Unmarshal(protoBytes, worker); err != nil {
		return nil, fmt.Errorf("unmarshaling worker: %w", err)
	}
	return worker, nil
}

// saveWorker writes back a Worker whose allocation just moved, at the next
// version. This assumes the caller holds the row lock getWorkerForUpdate took.
func saveWorker(ctx context.Context, tx *sql.Tx, worker *ateapipb.Worker) error {
	read := proto.CloneOf(worker.GetMetadata())
	storesql.SetUpdateMetadata(worker.GetMetadata(), worker.GetMetadata())
	protoBytes, err := proto.Marshal(worker)
	if err != nil {
		return fmt.Errorf("marshaling worker: %w", err)
	}
	// Callers hold the row lock getWorkerForUpdate took, so this matches the
	// row they read. It is stated anyway so a caller that skipped the lock
	// fails loudly instead of overwriting a newer Worker, and so a row that is
	// gone is an error rather than an update of nothing.
	res, err := tx.ExecContext(ctx, `UPDATE workers SET version = ?, proto = ? WHERE name = ? AND uid = ? AND version = ?`,
		worker.GetMetadata().GetVersion(), protoBytes, read.GetName(), read.GetUid(), read.GetVersion())
	if err != nil {
		return fmt.Errorf("updating worker %s: %w", read.GetName(), err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("updating worker %s: %w", read.GetName(), err)
	} else if n != 1 {
		return store.ErrVersionConflict
	}
	return nil
}

func (p *Persistence) BindActorToWorker(ctx context.Context, workerName string, assignment *ateapipb.ActorAssignment, admit func(*ateapipb.Worker) error) error {
	actorUID := assignment.GetActorUid()
	if actorUID == "" {
		return fmt.Errorf("binding an assignment with no actor_uid to worker %s", workerName)
	}
	// The store assigns identity. atespace is empty because Workers are
	// global-scoped; the name is the Actor's UID, which is also the row key.
	// This is the identity a first bind gets; a rebind keeps the recorded one.
	createMeta := &ateapipb.ResourceMetadata{Name: actorUID}
	storesql.SetCreateMetadata(createMeta)

	_, err := p.writeAndAppendEvent(ctx, store.WorkerEventUpdated, func(ctx context.Context, tx *sql.Tx) (*ateapipb.Worker, error) {
		// A retried transaction starts again from the first bind's identity.
		assignment.Metadata = proto.CloneOf(createMeta)
		worker, err := getWorkerForUpdate(ctx, tx, workerName)
		if err != nil {
			return nil, err
		}
		if worker.Status == nil {
			worker.Status = &ateapipb.WorkerStatus{}
		}

		// Read under the row lock, so an epoch raised concurrently is either
		// seen here or raised after this bind commits.
		assignment.WorkerEpoch = worker.GetEpoch()
		assignmentBytes, err := proto.Marshal(assignment)
		if err != nil {
			return nil, fmt.Errorf("marshaling assignment: %w", err)
		}

		// Insert first and let the duplicate key say whether the Actor was
		// already bound. Checking with a read instead would miss a claim that
		// commits after it, and both claims would believe they were first. A
		// failed statement does not abort a MySQL transaction, so the rebind
		// path below runs in the same transaction.
		_, err = tx.ExecContext(ctx, `
			INSERT INTO worker_assignments (actor_uid, worker_name, proto)
			VALUES (?, ?, ?)`,
			actorUID, workerName, assignmentBytes)
		if err != nil && !isUniqueViolation(err) {
			return nil, fmt.Errorf("binding actor %s to worker %s: %w", actorUID, workerName, err)
		}
		if err == nil {
			// A new binding needs room. The row lock holds the answer until
			// commit, and a refusal rolls the insert back.
			if admit != nil {
				if err := admit(worker); err != nil {
					return nil, err
				}
			}
			allocated, err := resources.AddToAllocated(worker.Status.Allocated, assignment, +1)
			if err != nil {
				return nil, err
			}
			worker.Status.Allocated = allocated
			if err := saveWorker(ctx, tx, worker); err != nil {
				return nil, err
			}
			return worker, nil
		}

		// Already bound. Only this path, the retried claim, pays for the read.
		previous, previousWorker, err := getAssignmentRow(ctx, tx, actorUID)
		if err != nil {
			return nil, err
		}
		if previousWorker != workerName {
			return nil, fmt.Errorf("actor %s is already hosted by worker %s", actorUID, previousWorker)
		}

		// Subtract before adding: the Actor is already counted, and its
		// declared size may have changed.
		allocated, err := resources.AddToAllocated(worker.Status.Allocated, previous, -1)
		if err != nil {
			return nil, err
		}
		worker.Status.Allocated = allocated

		// Admit against the Worker without the old reservation. An
		// ActorTemplate is mutable, so a replacement can be larger than what
		// it replaces.
		if admit != nil {
			if err := admit(worker); err != nil {
				return nil, err
			}
		}

		if allocated, err = resources.AddToAllocated(allocated, assignment, +1); err != nil {
			return nil, err
		}
		worker.Status.Allocated = allocated

		// A rebind updates the assignment already recorded, so re-stamping it
		// as a create would make a retried claim look like a new subresource.
		storesql.SetUpdateMetadata(assignment.Metadata, previous.GetMetadata())
		rebindBytes, err := proto.Marshal(assignment)
		if err != nil {
			return nil, fmt.Errorf("marshaling rebound assignment: %w", err)
		}

		// Guarded on worker_name so a claim that moved the Actor elsewhere is
		// refused rather than overwritten.
		rebind, err := tx.ExecContext(ctx, `
			UPDATE worker_assignments SET proto = ?
			WHERE actor_uid = ? AND worker_name = ?`,
			rebindBytes, actorUID, workerName)
		if err != nil {
			return nil, fmt.Errorf("rebinding actor %s on worker %s: %w", actorUID, workerName, err)
		}
		if n, err := rebind.RowsAffected(); err != nil {
			return nil, fmt.Errorf("rebinding actor %s on worker %s: %w", actorUID, workerName, err)
		} else if n != 1 {
			return nil, fmt.Errorf("%w: actor %s left worker %s while it was being rebound", store.ErrVersionConflict, actorUID, workerName)
		}
		if err := saveWorker(ctx, tx, worker); err != nil {
			return nil, err
		}
		return worker, nil
	})
	return err
}

func (p *Persistence) ReleaseActorFromWorker(ctx context.Context, workerName string, actorUID string) (*ateapipb.Worker, error) {
	var released *ateapipb.Worker
	_, err := p.writeAndAppendEvent(ctx, store.WorkerEventUpdated, func(ctx context.Context, tx *sql.Tx) (*ateapipb.Worker, error) {
		released = nil
		worker, err := getWorkerForUpdate(ctx, tx, workerName)
		if err != nil {
			return nil, err
		}

		var protoBytes []byte
		err = tx.QueryRowContext(ctx, `
			SELECT proto FROM worker_assignments
			WHERE actor_uid = ? AND worker_name = ?
			FOR UPDATE`, actorUID, workerName).Scan(&protoBytes)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil // nothing to release, and so nothing to announce
		}
		if err != nil {
			return nil, fmt.Errorf("releasing actor %s from worker %s: %w", actorUID, workerName, err)
		}
		res, err := tx.ExecContext(ctx, `DELETE FROM worker_assignments WHERE actor_uid = ? AND worker_name = ?`, actorUID, workerName)
		if err != nil {
			return nil, fmt.Errorf("releasing actor %s from worker %s: %w", actorUID, workerName, err)
		}
		if err := requireOneRow(res, "releasing actor "+actorUID+" from worker "+workerName); err != nil {
			return nil, err
		}
		assignment := &ateapipb.ActorAssignment{}
		if err := proto.Unmarshal(protoBytes, assignment); err != nil {
			return nil, fmt.Errorf("unmarshaling released assignment: %w", err)
		}

		if worker.Status == nil {
			worker.Status = &ateapipb.WorkerStatus{}
		}
		allocated, err := resources.AddToAllocated(worker.Status.Allocated, assignment, -1)
		if err != nil {
			return nil, err
		}
		worker.Status.Allocated = allocated
		if err := saveWorker(ctx, tx, worker); err != nil {
			return nil, err
		}
		released = worker
		return worker, nil
	})
	if err != nil {
		return nil, err
	}
	return released, nil
}

// getAssignmentRow reads the assignment for actorUID and names the worker
// holding it.
func getAssignmentRow(ctx context.Context, q querier, actorUID string) (*ateapipb.ActorAssignment, string, error) {
	var (
		protoBytes []byte
		workerName string
	)
	err := q.QueryRowContext(ctx, `SELECT proto, worker_name FROM worker_assignments WHERE actor_uid = ?`, actorUID).
		Scan(&protoBytes, &workerName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, "", store.ErrNotFound
		}
		return nil, "", fmt.Errorf("getting assignment for actor %s: %w", actorUID, err)
	}
	assignment := &ateapipb.ActorAssignment{}
	if err := proto.Unmarshal(protoBytes, assignment); err != nil {
		return nil, "", fmt.Errorf("unmarshaling assignment: %w", err)
	}
	return assignment, workerName, nil
}

func (p *Persistence) GetWorkerAssignment(ctx context.Context, workerName, actorUID string) (*ateapipb.ActorAssignment, error) {
	assignment, holder, err := getAssignmentRow(ctx, p.db, actorUID)
	if err != nil {
		return nil, err
	}
	if holder != workerName {
		return nil, store.ErrNotFound
	}
	return assignment, nil
}

func (p *Persistence) ListWorkerAssignments(ctx context.Context, workerName string, opts store.ListOptions) (store.ListResponse[*ateapipb.ActorAssignment], error) {
	opts, err := store.NormalizeListOptions(opts)
	if err != nil {
		return store.ListResponse[*ateapipb.ActorAssignment]{}, err
	}
	// The token is scoped to the Worker, so one cannot be replayed against
	// another Worker's assignments.
	token, err := storesql.DecodePageToken(opts.PageToken, storesql.KindWorkerAssign, workerName, 1)
	if err != nil {
		return store.ListResponse[*ateapipb.ActorAssignment]{}, err
	}
	limit := int64(opts.PageSize) + 1
	query, args := `
		SELECT actor_uid, proto FROM worker_assignments
		WHERE worker_name = ?
		ORDER BY actor_uid LIMIT ?`, []any{workerName, limit}
	if len(token.Last) > 0 {
		query, args = `
			SELECT actor_uid, proto FROM worker_assignments
			WHERE worker_name = ? AND actor_uid > ?
			ORDER BY actor_uid LIMIT ?`, []any{workerName, token.Last[0], limit}
	}
	items, next, err := listPage(ctx, p.db, storesql.KindWorkerAssign, workerName, opts.PageSize, func(b []byte, keys []string) (*ateapipb.ActorAssignment, error) {
		assignment := &ateapipb.ActorAssignment{}
		if err := proto.Unmarshal(b, assignment); err != nil {
			return nil, fmt.Errorf("unmarshaling assignment of actor %s: %w", keys[0], err)
		}
		return assignment, nil
	}, query, args...)
	if err != nil {
		if errors.Is(err, store.ErrInvalidPageToken) {
			return store.ListResponse[*ateapipb.ActorAssignment]{}, err
		}
		return store.ListResponse[*ateapipb.ActorAssignment]{}, fmt.Errorf("listing assignments of worker %s: %w", workerName, err)
	}
	return store.ListResponse[*ateapipb.ActorAssignment]{Items: items, NextPageToken: next}, nil
}

func (p *Persistence) FindWorkerHostingActor(ctx context.Context, actorUID string) (string, error) {
	var workerName string
	err := p.db.QueryRowContext(ctx, `SELECT worker_name FROM worker_assignments WHERE actor_uid = ?`, actorUID).Scan(&workerName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", store.ErrNotFound
		}
		return "", fmt.Errorf("finding the worker hosting actor %s: %w", actorUID, err)
	}
	return workerName, nil
}
