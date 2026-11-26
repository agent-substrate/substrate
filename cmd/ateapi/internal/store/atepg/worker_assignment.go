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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/protobuf/proto"
)

const (
	workerForUpdateSQL = `SELECT proto FROM workers WHERE name = $1 FOR UPDATE`
	// workerOfActorForUpdateSQL locks the Worker holding actorUID's assignment
	// in one statement. The assignment row names the Worker and the Worker is
	// locked before its assignment row is touched, matching BindActorToWorker's
	// lock order.
	workerOfActorForUpdateSQL = `
		SELECT proto FROM workers
		WHERE name = (SELECT worker_name FROM worker_assignments WHERE actor_uid = $1)
		FOR UPDATE`
	saveWorkerSQL = `UPDATE workers SET version = $1, proto = $2 WHERE name = $3 AND uid = $4 AND version = $5`
)

// scanWorkerForUpdate decodes a row of workerForUpdateSQL or
// workerOfActorForUpdateSQL; the row stays locked until the caller's
// transaction commits, which is what serializes updates to its allocation.
func scanWorkerForUpdate(row pgx.Row, name string) (*ateapi.Worker, error) {
	var protoBytes []byte
	err := row.Scan(&protoBytes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("locking worker %s: %w", name, err)
	}
	worker := &ateapi.Worker{}
	if err := unmarshalStored(protoBytes, worker); err != nil {
		return nil, fmt.Errorf("unmarshaling worker: %w", err)
	}
	return worker, nil
}

// queueSaveWorker queues the write of a Worker whose allocation just moved, at
// the next version. This assumes the caller's transaction holds the row lock
// workerForUpdateSQL or workerOfActorForUpdateSQL took.
func queueSaveWorker(w *writes, worker *ateapi.Worker) error {
	currentUID := worker.GetMetadata().GetUid()
	currentVersion := worker.GetMetadata().GetVersion()
	worker.Metadata = setUpdateMetadata(worker.Metadata)
	protoBytes, err := proto.Marshal(worker)
	if err != nil {
		return fmt.Errorf("marshaling worker: %w", err)
	}
	meta := worker.GetMetadata()
	// Callers hold the row lock for this worker, so this matches the row they
	// read. It is stated anyway so a caller that skipped the lock fails loudly
	// instead of overwriting a newer Worker, and so a row that is gone is an
	// error rather than an update of nothing.
	w.exec(saveWorkerSQL, []any{meta.GetVersion(), protoBytes, meta.GetName(), currentUID, currentVersion}, func(tag pgconn.CommandTag) error {
		if tag.RowsAffected() != 1 {
			return store.ErrVersionConflict
		}
		return nil
	})
	return nil
}

// BindActorToWorker runs its transaction in three round trips: one batch that
// locks the Worker and reads the Actor, one batch with every write, and the
// commit. The Worker row is held for all three.
func (p *Persistence) BindActorToWorker(ctx context.Context, workerName string, assignment *ateapi.ActorAssignment, admit func(*ateapi.Worker) error, actorRef resources.ActorRef, precondition store.Precondition, mutate func(*ateapi.Actor) error) (*ateapi.Actor, error) {
	actorUID := assignment.GetActorUid()
	if actorUID == "" {
		return nil, fmt.Errorf("binding an assignment with no actor_uid to worker %s", workerName)
	}
	if err := precondition.ValidateRequired(); err != nil {
		return nil, err
	}
	if precondition.UID != actorUID {
		return nil, fmt.Errorf("binding actor %s to worker %s under a precondition for actor %s", actorUID, workerName, precondition.UID)
	}
	// The store assigns identity. atespace is empty because Workers are
	// global-scoped; the name is the Actor's UID, which is also the row key.
	assignment.Metadata = &ateapi.ResourceMetadata{Name: actorUID}
	setCreateMetadata(assignment.Metadata)
	assignmentBytes, err := proto.Marshal(assignment)
	if err != nil {
		return nil, fmt.Errorf("marshaling assignment: %w", err)
	}

	var updated *ateapi.Actor
	_, err = p.writeAndAppendEventBatched(ctx, store.WorkerEventUpdated, func(ctx context.Context, tx pgx.Tx, w *writes) (*ateapi.Worker, error) {
		reads := &pgx.Batch{}
		reads.Queue(workerForUpdateSQL, workerName)
		reads.Queue(actorForUpdateSQL, actorRef.Atespace, actorRef.Name)
		results := tx.SendBatch(ctx, reads)
		worker, err := scanWorkerForUpdate(results.QueryRow(), workerName)
		if errors.Is(err, store.ErrNotFound) {
			err = store.ErrWorkerNotFound
		}
		var current readActor
		if err == nil {
			current, err = readActorForUpdate(results.QueryRow(), actorRef)
		}
		if closeErr := results.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("reading worker %s and actor %s: %w", workerName, actorRef, closeErr)
		}
		if err != nil {
			return nil, err
		}
		if worker.Status == nil {
			worker.Status = &ateapi.WorkerStatus{}
		}

		// The row lock holds the answer until commit.
		if admit != nil {
			if err := admit(worker); err != nil {
				return nil, err
			}
		}
		// The Actor's version check settles concurrent binds of it: every bind
		// updates the Actor, so all but the first fail it.
		next, err := current.apply(actorRef, precondition, mutate, false)
		if err != nil {
			return nil, err
		}
		w.exec(updateActorSQL, next.args(actorRef), func(tag pgconn.CommandTag) error { return next.check(actorRef, tag) })
		w.exec(`
			INSERT INTO worker_assignments (actor_uid, worker_name, proto)
			VALUES ($1, $2, $3)`, []any{actorUID, workerName, assignmentBytes}, nil)
		worker.Status.Allocated = resources.AddToAllocated(worker.Status.Allocated, assignment, +1)
		if err := queueSaveWorker(w, worker); err != nil {
			return nil, err
		}
		updated = next.actor
		return worker, nil
	})
	if err != nil {
		if isUniqueViolation(err) {
			return nil, fmt.Errorf("binding actor %s to worker %s: %w", actorUID, workerName, err)
		}
		return nil, err
	}
	return updated, nil
}

// UpdateActorAndRelease runs its transaction in three round trips: one batch
// that locks the Worker, drops the assignment and reads the Actor, one batch
// with the writes, and the commit.
func (p *Persistence) UpdateActorAndRelease(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition, mutate func(*ateapi.Actor) error) (*ateapi.Actor, error) {
	if err := precondition.ValidateRequired(); err != nil {
		return nil, err
	}
	actorUID := precondition.UID
	var updated *ateapi.Actor
	_, err := p.writeAndAppendEventBatched(ctx, store.WorkerEventUpdated, func(ctx context.Context, tx pgx.Tx, w *writes) (*ateapi.Worker, error) {
		reads := &pgx.Batch{}
		reads.Queue(workerOfActorForUpdateSQL, actorUID)
		reads.Queue(`
			DELETE FROM worker_assignments
			WHERE actor_uid = $1
			RETURNING worker_name, proto`, actorUID)
		reads.Queue(actorForUpdateSQL, actorRef.Atespace, actorRef.Name)
		results := tx.SendBatch(ctx, reads)
		released, err := releaseActor(results, actorUID)
		var current readActor
		if err == nil {
			current, err = readActorForUpdate(results.QueryRow(), actorRef)
		}
		if closeErr := results.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("releasing actor %s: %w", actorUID, closeErr)
		}
		if err != nil {
			return nil, err
		}
		if released != nil {
			if err := queueSaveWorker(w, released); err != nil {
				return nil, err
			}
		}
		// A row released since the caller read the Actor also moved the
		// Actor on, so its version check fails here.
		next, err := current.apply(actorRef, precondition, mutate, false)
		if err != nil {
			return nil, err
		}
		w.exec(updateActorSQL, next.args(actorRef), func(tag pgconn.CommandTag) error { return next.check(actorRef, tag) })
		updated = next.actor
		return released, nil
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

// releaseActor reads the first two results of UpdateActorAndRelease's batch:
// the locked Worker that held actorUID's assignment and the assignment row
// just deleted. It returns the Worker with the assignment no longer booked,
// or nil if no Worker held it. If an assignment appeared or moved to another
// Worker between the two statements, ErrVersionConflict aborts the transaction.
func releaseActor(results pgx.BatchResults, actorUID string) (*ateapi.Worker, error) {
	worker, err := scanWorkerForUpdate(results.QueryRow(), "of actor "+actorUID)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	var (
		deletedWorker string
		protoBytes    []byte
	)
	deleteErr := results.QueryRow().Scan(&deletedWorker, &protoBytes)
	if worker == nil {
		if deleteErr == nil {
			return nil, store.ErrVersionConflict
		}
		if !errors.Is(deleteErr, pgx.ErrNoRows) {
			return nil, fmt.Errorf("releasing actor %s: %w", actorUID, deleteErr)
		}
		return nil, nil
	}
	if errors.Is(deleteErr, pgx.ErrNoRows) {
		return nil, nil
	}
	if deleteErr != nil {
		return nil, fmt.Errorf("releasing actor %s from worker %s: %w", actorUID, worker.GetMetadata().GetName(), deleteErr)
	}
	if deletedWorker != worker.GetMetadata().GetName() {
		return nil, store.ErrVersionConflict
	}
	assignment := &ateapi.ActorAssignment{}
	if err := unmarshalStored(protoBytes, assignment); err != nil {
		return nil, fmt.Errorf("unmarshaling released assignment: %w", err)
	}
	if worker.Status == nil {
		worker.Status = &ateapi.WorkerStatus{}
	}
	worker.Status.Allocated = resources.AddToAllocated(worker.Status.Allocated, assignment, -1)
	return worker, nil
}

// getAssignmentRow reads the assignment for actorUID and names the worker
// holding it.
func getAssignmentRow(ctx context.Context, q querier, actorUID string) (*ateapi.ActorAssignment, string, error) {
	var (
		protoBytes []byte
		workerName string
	)
	err := q.QueryRow(ctx, `SELECT proto, worker_name FROM worker_assignments WHERE actor_uid = $1`, actorUID).
		Scan(&protoBytes, &workerName)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, "", store.ErrNotFound
		}
		return nil, "", fmt.Errorf("getting assignment for actor %s: %w", actorUID, err)
	}
	assignment := &ateapi.ActorAssignment{}
	if err := unmarshalStored(protoBytes, assignment); err != nil {
		return nil, "", fmt.Errorf("unmarshaling assignment: %w", err)
	}
	return assignment, workerName, nil
}

func (p *Persistence) GetWorkerAssignment(ctx context.Context, workerName, actorUID string) (*ateapi.ActorAssignment, error) {
	assignment, holder, err := getAssignmentRow(ctx, p.pool, actorUID)
	if err != nil {
		return nil, err
	}
	if holder != workerName {
		return nil, store.ErrNotFound
	}
	return assignment, nil
}

func (p *Persistence) ListWorkerAssignments(ctx context.Context, workerName string, opts store.ListOptions) (store.ListResponse[*ateapi.ActorAssignment], error) {
	opts, err := store.NormalizeListOptions(opts)
	if err != nil {
		return store.ListResponse[*ateapi.ActorAssignment]{}, err
	}
	pageSize := opts.PageSize
	// The token is scoped to the Worker, so one cannot be replayed against
	// another Worker's assignments.
	token, err := decodePageToken(opts.PageToken, kindWorkerAssign, workerName, 1)
	if err != nil {
		return store.ListResponse[*ateapi.ActorAssignment]{}, err
	}
	var last *string
	if len(token.Last) > 0 {
		last = &token.Last[0]
	}

	rows, err := p.pool.Query(ctx, `
		SELECT actor_uid, proto FROM worker_assignments
		WHERE worker_name = $1 AND ($2::text IS NULL OR actor_uid > $2)
		ORDER BY actor_uid
		LIMIT $3`, workerName, last, int64(pageSize)+1)
	if err != nil {
		return store.ListResponse[*ateapi.ActorAssignment]{}, fmt.Errorf("listing assignments of worker %s: %w", workerName, err)
	}
	defer rows.Close()

	var uids []string
	var result []*ateapi.ActorAssignment
	for rows.Next() {
		var actorUID string
		var protoBytes []byte
		if err := rows.Scan(&actorUID, &protoBytes); err != nil {
			return store.ListResponse[*ateapi.ActorAssignment]{}, fmt.Errorf("scanning assignment of worker %s: %w", workerName, err)
		}
		assignment := &ateapi.ActorAssignment{}
		if err := unmarshalStored(protoBytes, assignment); err != nil {
			return store.ListResponse[*ateapi.ActorAssignment]{}, fmt.Errorf("unmarshaling assignment of actor %s: %w", actorUID, err)
		}
		result = append(result, assignment)
		uids = append(uids, actorUID)
	}
	if err := rows.Err(); err != nil {
		return store.ListResponse[*ateapi.ActorAssignment]{}, fmt.Errorf("listing assignments of worker %s: %w", workerName, err)
	}

	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		nextToken = encodePageToken(kindWorkerAssign, workerName, []string{uids[pageSize-1]})
	}
	return store.ListResponse[*ateapi.ActorAssignment]{Items: result, NextPageToken: nextToken}, nil
}
