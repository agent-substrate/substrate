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
	"log/slog"

	"github.com/jackc/pgx/v5"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/protobuf/proto"
)

// getWorkerForUpdate reads a Worker and holds its row until the caller's
// transaction commits, which is what serializes updates to its allocation. The
// caller supplies the transaction; this only takes the lock.
func getWorkerForUpdate(ctx context.Context, tx pgx.Tx, name string) (*ateapi.Worker, error) {
	var protoBytes []byte
	err := tx.QueryRow(ctx, `SELECT proto FROM workers WHERE name = $1 FOR UPDATE`, name).Scan(&protoBytes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("locking worker %s: %w", name, err)
	}
	worker := &ateapi.Worker{}
	if err := proto.Unmarshal(protoBytes, worker); err != nil {
		return nil, fmt.Errorf("unmarshaling worker: %w", err)
	}
	return worker, nil
}

// saveWorker writes back a Worker whose allocation just moved, at the next
// version. This assumes the caller holds the row lock getWorkerForUpdate took.
func saveWorker(ctx context.Context, tx pgx.Tx, worker *ateapi.Worker) error {
	read := proto.CloneOf(worker.GetMetadata())
	setUpdateMetadata(worker.GetMetadata(), worker.GetMetadata())
	protoBytes, err := proto.Marshal(worker)
	if err != nil {
		return fmt.Errorf("marshaling worker: %w", err)
	}
	// Callers hold the row lock getWorkerForUpdate took, so this matches the
	// row they read. It is stated anyway so a caller that skipped the lock
	// fails loudly instead of overwriting a newer Worker, and so a row that is
	// gone is an error rather than an update of nothing.
	tag, err := tx.Exec(ctx, `UPDATE workers SET version = $1, proto = $2 WHERE name = $3 AND uid = $4 AND version = $5`,
		worker.GetMetadata().GetVersion(), protoBytes, read.GetName(), read.GetUid(), read.GetVersion())
	if err != nil {
		return fmt.Errorf("updating worker %s: %w", read.GetName(), err)
	}
	if tag.RowsAffected() != 1 {
		return store.ErrVersionConflict
	}
	return nil
}

func (p *Persistence) BindActorToWorker(ctx context.Context, workerName string, assignment *ateapi.ActorAssignment, admit func(*ateapi.Worker) error, actorRef resources.ActorRef, precondition store.Precondition, mutate func(*ateapi.Actor) error) (*ateapi.Actor, error) {
	actorUID := assignment.GetActorUid()
	if actorUID == "" {
		return nil, fmt.Errorf("binding an assignment with no actor_uid to worker %s", workerName)
	}
	if err := precondition.Validate(); err != nil {
		return nil, err
	}
	if precondition.UID != actorUID {
		return nil, fmt.Errorf("binding actor %s to worker %s under a precondition for actor %s", actorUID, workerName, precondition.UID)
	}
	// The store assigns identity. atespace is empty because Workers are
	// global-scoped; the name is the Actor's UID, which is also the row key.
	assignment.Metadata = &ateapi.ResourceMetadata{Name: actorUID}
	setCreateMetadata(assignment.Metadata)
	assignment.State = ateapi.ActorAssignmentState_ACTOR_ASSIGNMENT_STATE_RUNNING
	assignmentBytes, err := proto.Marshal(assignment)
	if err != nil {
		return nil, fmt.Errorf("marshaling assignment: %w", err)
	}

	var updated *ateapi.Actor
	var movedFrom string
	_, err = p.writeAndAppendEvent(ctx, store.WorkerEventUpdated, func(ctx context.Context, tx pgx.Tx) (*ateapi.Worker, error) {
		// A suspended Actor still bound to a node keeps its assignment. Only
		// finds where it is: the Worker holding it is locked below.
		previous, holder, err := getAssignmentRow(ctx, tx, actorUID)
		switch {
		case errors.Is(err, store.ErrNotFound):
			previous = nil
		case err != nil:
			return nil, err
		case resources.AssignmentRunning(previous):
			return nil, fmt.Errorf("binding actor %s to worker %s: it is already running on worker %s", actorUID, workerName, holder)
		}

		worker, err := getWorkerForUpdate(ctx, tx, workerName)
		if errors.Is(err, store.ErrNotFound) {
			return nil, store.ErrWorkerNotFound
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
		if updated, err = updateActor(ctx, tx, actorRef, store.DeletePreconditions(precondition), mutate, false); err != nil {
			return nil, err
		}

		resumed := false
		if previous != nil && holder != workerName {
			// Resumed on another Worker of the node: the assignment moves.
			if _, err := dropAssignment(ctx, tx, actorUID, holder); err != nil {
				return nil, err
			}
			movedFrom = holder
		} else if previous != nil {
			tag, err := tx.Exec(ctx, `UPDATE worker_assignments SET proto = $1 WHERE actor_uid = $2 AND worker_name = $3`,
				assignmentBytes, actorUID, workerName)
			if err != nil {
				return nil, fmt.Errorf("resuming the assignment of actor %s on worker %s: %w", actorUID, workerName, err)
			}
			resumed = tag.RowsAffected() == 1
		}
		if !resumed {
			if _, err := tx.Exec(ctx, `
				INSERT INTO worker_assignments (actor_uid, worker_name, proto)
				VALUES ($1, $2, $3)`,
				actorUID, workerName, assignmentBytes); err != nil {
				return nil, fmt.Errorf("binding actor %s to worker %s: %w", actorUID, workerName, err)
			}
			addAssignmentTotal(worker, +1)
		}
		worker.Status.Allocated = resources.AddToAllocated(worker.Status.Allocated, assignment, +1)
		if err := saveWorker(ctx, tx, worker); err != nil {
			return nil, err
		}
		return worker, nil
	})
	if err != nil {
		return nil, err
	}
	if movedFrom != "" {
		// One transaction appends one event, for the Worker bound to, so the
		// Worker the assignment left is published on its own.
		p.publishWorker(ctx, movedFrom)
	}
	return updated, nil
}

func (p *Persistence) UpdateActorAndRelease(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition, mutate func(*ateapi.Actor) error) (*ateapi.Actor, error) {
	if err := precondition.Validate(); err != nil {
		return nil, err
	}
	actorUID := precondition.UID
	var updated *ateapi.Actor
	_, err := p.writeAndAppendEvent(ctx, store.WorkerEventUpdated, func(ctx context.Context, tx pgx.Tx) (*ateapi.Worker, error) {
		// The Worker is locked before the Actor, as BindActorToWorker locks
		// them, so the first read only finds which Worker to lock.
		worker, err := lockAssignedWorker(ctx, tx, actorUID)
		if err != nil {
			return nil, err
		}
		// A row released since the caller read the Actor also moved the
		// Actor on, so its version check fails here.
		if updated, err = updateActor(ctx, tx, actorRef, store.DeletePreconditions(precondition), mutate, false); err != nil {
			return nil, err
		}
		if worker == nil {
			return nil, nil
		}
		if keepsSuspendedAssignment(updated) && updated.GetStatus().GetAssignedNode() == worker.GetNodeName() {
			return suspendAssignment(ctx, tx, worker, actorUID)
		}
		return releaseAssignment(ctx, tx, worker, actorUID)
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

// keepsSuspendedAssignment reports whether a stored Actor keeps its assignment
// in the suspended state: it is suspended, names no Worker, and is still bound
// to a node.
func keepsSuspendedAssignment(actor *ateapi.Actor) bool {
	status := actor.GetStatus()
	return status.GetState() == ateapi.ActorState_ACTOR_STATE_SUSPENDED &&
		status.GetWorkerAssignment() == nil &&
		status.GetAssignedNode() != ""
}

// lockAssignedWorker locks and returns the Worker holding actorUID's
// assignment, or nil if no Worker holds one.
func lockAssignedWorker(ctx context.Context, tx pgx.Tx, actorUID string) (*ateapi.Worker, error) {
	_, workerName, err := getAssignmentRow(ctx, tx, actorUID)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	worker, err := getWorkerForUpdate(ctx, tx, workerName)
	if errors.Is(err, store.ErrNotFound) {
		// The Worker and its assignments went after the first read.
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if worker.Status == nil {
		worker.Status = &ateapi.WorkerStatus{}
	}
	return worker, nil
}

// releaseAssignment deletes actorUID's assignment from worker, which the caller
// has locked, and saves the Worker without it. It returns the saved Worker, or
// nil if the row went after the caller found it.
func releaseAssignment(ctx context.Context, tx pgx.Tx, worker *ateapi.Worker, actorUID string) (*ateapi.Worker, error) {
	workerName := worker.GetMetadata().GetName()
	var protoBytes []byte
	err := tx.QueryRow(ctx, `
		DELETE FROM worker_assignments
		WHERE actor_uid = $1 AND worker_name = $2
		RETURNING proto`, actorUID, workerName).Scan(&protoBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("releasing actor %s from worker %s: %w", actorUID, workerName, err)
	}
	assignment := &ateapi.ActorAssignment{}
	if err := unmarshalStored(protoBytes, assignment); err != nil {
		return nil, fmt.Errorf("unmarshaling released assignment: %w", err)
	}
	if resources.AssignmentRunning(assignment) {
		worker.Status.Allocated = resources.AddToAllocated(worker.Status.Allocated, assignment, -1)
	}
	addAssignmentTotal(worker, -1)
	if err := saveWorker(ctx, tx, worker); err != nil {
		return nil, err
	}
	return worker, nil
}

// suspendAssignment marks actorUID's assignment on worker, which the caller has
// locked, suspended, and saves the Worker with its resources no longer
// allocated. The assignment stays counted in the Worker's total. It returns
// the saved Worker, or nil if the row went after the caller found it.
func suspendAssignment(ctx context.Context, tx pgx.Tx, worker *ateapi.Worker, actorUID string) (*ateapi.Worker, error) {
	workerName := worker.GetMetadata().GetName()
	var protoBytes []byte
	err := tx.QueryRow(ctx, `SELECT proto FROM worker_assignments WHERE actor_uid = $1 AND worker_name = $2 FOR UPDATE`,
		actorUID, workerName).Scan(&protoBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting the assignment of actor %s on worker %s: %w", actorUID, workerName, err)
	}
	assignment := &ateapi.ActorAssignment{}
	if err := unmarshalStored(protoBytes, assignment); err != nil {
		return nil, fmt.Errorf("unmarshaling assignment: %w", err)
	}
	if !resources.AssignmentRunning(assignment) {
		return nil, nil
	}
	worker.Status.Allocated = resources.AddToAllocated(worker.Status.Allocated, assignment, -1)
	assignment.State = ateapi.ActorAssignmentState_ACTOR_ASSIGNMENT_STATE_SUSPENDED
	setUpdateMetadata(assignment.Metadata, proto.CloneOf(assignment.Metadata))
	if protoBytes, err = proto.Marshal(assignment); err != nil {
		return nil, fmt.Errorf("marshaling assignment: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE worker_assignments SET proto = $1 WHERE actor_uid = $2 AND worker_name = $3`,
		protoBytes, actorUID, workerName); err != nil {
		return nil, fmt.Errorf("suspending the assignment of actor %s on worker %s: %w", actorUID, workerName, err)
	}
	if err := saveWorker(ctx, tx, worker); err != nil {
		return nil, err
	}
	return worker, nil
}

// dropAssignment locks the Worker named workerName and deletes actorUID's
// assignment from it. It returns the saved Worker, or nil if either is gone.
// Callers already hold the Actor's row, and some another Worker's, so this can
// lock in the reverse order of a concurrent BindActorToWorker. Postgres aborts
// one side of such a deadlock, and writeAndAppendEvent reports it as
// ErrVersionConflict.
func dropAssignment(ctx context.Context, tx pgx.Tx, actorUID, workerName string) (*ateapi.Worker, error) {
	worker, err := getWorkerForUpdate(ctx, tx, workerName)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if worker.Status == nil {
		worker.Status = &ateapi.WorkerStatus{}
	}
	return releaseAssignment(ctx, tx, worker, actorUID)
}

// dropSuspendedAssignment deletes the suspended assignment of an Actor that is
// no longer bound to a node, if it has one, and returns the Worker it left.
func dropSuspendedAssignment(ctx context.Context, tx pgx.Tx, actorUID string) (*ateapi.Worker, error) {
	_, workerName, err := getAssignmentRow(ctx, tx, actorUID)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return dropAssignment(ctx, tx, actorUID, workerName)
}

// publishWorker appends an event carrying the Worker named name as stored, for
// a change made by a transaction that appended an event for another Worker.
// Best effort: the worker cache's relist picks up what this misses.
func (p *Persistence) publishWorker(ctx context.Context, name string) {
	_, err := p.writeAndAppendEvent(ctx, store.WorkerEventUpdated, func(ctx context.Context, tx pgx.Tx) (*ateapi.Worker, error) {
		worker, err := getWorkerForUpdate(ctx, tx, name)
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil
		}
		return worker, err
	})
	if err != nil {
		slog.WarnContext(ctx, "Failed to publish a worker an assignment moved off", slog.String("worker", name), slog.Any("err", err))
	}
}

// addAssignmentTotal adjusts the count of the Worker's assignments by n.
func addAssignmentTotal(worker *ateapi.Worker, n int64) {
	total := worker.GetStatus().GetAssignments().GetTotal() + n
	if total == 0 {
		worker.Status.Assignments = nil
		return
	}
	worker.Status.Assignments = &ateapi.ActorAssignmentsStatus{Total: total}
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
