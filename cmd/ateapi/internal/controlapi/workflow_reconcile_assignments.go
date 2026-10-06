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

package controlapi

import (
	"context"
	"errors"
	"fmt"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// needsAssignmentReconcile reports whether worker's epoch has risen past the
// last one its earlier Actors were released for.
func needsAssignmentReconcile(worker *ateapipb.Worker) bool {
	return worker.GetEpoch() > worker.GetStatus().GetObservedEpoch()
}

// ReconcileAssignments brings a Worker's status.observed_epoch up to its epoch.
// A raised epoch means its ateom restarted, taking with it the sandboxes of the
// Actors placed during an earlier epoch, so those Actors are crashed and their
// assignments released before observed_epoch records it. A failure leaves
// observed_epoch where it was, so the next pass redoes the release.
//
// Nothing to do, including a Worker that is gone, is success.
func (w *WorkerWorkflow) ReconcileAssignments(ctx context.Context, name string) (err error) {
	ctx, done := stepSpan(ctx, "ReconcileAssignments")
	defer func() { err = done(err) }()

	worker, err := w.store.GetWorker(ctx, name)
	if errors.Is(err, store.ErrNotFound) {
		markSkipped(ctx, "worker not found")
		return nil
	}
	if err != nil {
		return fmt.Errorf("while fetching worker %s: %w", name, err)
	}
	if !needsAssignmentReconcile(worker) {
		markSkipped(ctx, "epoch already observed")
		return nil
	}
	epoch := worker.GetEpoch()

	// A Worker whose observed_epoch is 0 was registered before epochs were
	// reported, so its current epoch is recorded without releasing anything:
	// nothing says which of its Actors predate it.
	if worker.GetStatus().GetObservedEpoch() != 0 {
		if err := w.releaseAssignmentsBefore(ctx, worker, epoch); err != nil {
			return err
		}
	}
	return w.recordObservedEpoch(ctx, name, worker.GetMetadata().GetUid(), epoch)
}

// recordObservedEpochAttempts bounds how often recordObservedEpoch rereads a
// Worker that other writes keep moving on.
const recordObservedEpochAttempts = 5

// recordObservedEpoch raises observed_epoch to epoch on the Worker incarnation
// uid names. A Worker that is gone, or replaced by a new incarnation, is left
// alone.
//
// A write that lands on the Worker after the sweep does not make the record
// wrong, so a version conflict is retried against a fresh read. Every
// assignment is stamped with the Worker's epoch under its row lock, the same
// lock that raises the epoch, so a row stamped before epoch was committed
// before epoch was read and the sweep found it. Later binds are stamped epoch
// or higher.
func (w *WorkerWorkflow) recordObservedEpoch(ctx context.Context, name, uid string, epoch int64) error {
	for attempt := 1; ; attempt++ {
		worker, err := w.store.GetWorker(ctx, name)
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("while re-fetching worker %s: %w", name, err)
		}
		_, err = w.store.UpdateWorker(ctx, name, store.Precondition{UID: uid, Version: worker.GetMetadata().GetVersion()}, func(toUpdate *ateapipb.Worker) error {
			toUpdate.Status.ObservedEpoch = max(toUpdate.GetStatus().GetObservedEpoch(), epoch)
			return nil
		})
		switch {
		case err == nil, errors.Is(err, store.ErrNotFound), errors.Is(err, store.ErrUIDConflict):
			return nil
		case errors.Is(err, store.ErrVersionConflict) && attempt < recordObservedEpochAttempts:
			continue
		default:
			return fmt.Errorf("while recording observed epoch %d on worker %s: %w", epoch, name, err)
		}
	}
}

// errActorBusy reports an assignment skipped because an operation holds its
// Actor's lease.
var errActorBusy = errors.New("actor has an operation in progress")

// releaseAssignmentsBefore releases every assignment on worker made during an
// epoch before epoch. An assignment whose Actor is busy is skipped until the
// rest are released, then reported, so the pass is retried.
func (w *WorkerWorkflow) releaseAssignmentsBefore(ctx context.Context, worker *ateapipb.Worker, epoch int64) error {
	name := worker.GetMetadata().GetName()
	busy := 0
	var node nodeReclaim
	// Releasing deletes rows mid-scan, which paging tolerates: the cursor is
	// the last actor UID listed, not an offset.
	for token := ""; ; {
		page, err := w.store.ListWorkerAssignments(ctx, name, store.ListOptions{PageToken: token})
		if err != nil {
			return fmt.Errorf("while listing the assignments of worker %s: %w", name, err)
		}
		for _, assignment := range page.Items {
			if assignment.GetWorkerEpoch() >= epoch {
				continue
			}
			err := w.releaseEarlierAssignment(ctx, worker, assignment, epoch, &node)
			if errors.Is(err, errActorBusy) {
				busy++
				continue
			}
			if err != nil {
				return err
			}
		}
		if !page.HasNextPage() {
			break
		}
		token = page.NextPageToken
	}
	if busy > 0 {
		return fmt.Errorf("%d assignments on worker %s left for a retry: %w", busy, name, errActorBusy)
	}
	return nil
}

// releaseEarlierAssignment clears one assignment made during an epoch of the
// Worker before epoch, under its Actor's lease so no operation on the Actor is
// midway. The Actor, read under the lease, may have been bound here again in
// the current epoch since the list; its assignment then carries that epoch, as
// the rebound row does, and is left alone. An Actor still running on it has
// its node state reclaimed, is crashed, and its row released after, so a
// failure in between leaves the row for a retry to find, and the Actor already
// CRASHED. A row whose Actor does not point here is left over from an
// operation that failed partway, and is released.
func (w *WorkerWorkflow) releaseEarlierAssignment(ctx context.Context, worker *ateapipb.Worker, assignment *ateapipb.ActorAssignment, epoch int64, node *nodeReclaim) error {
	name := worker.GetMetadata().GetName()
	release := func() error {
		if _, err := w.store.ReleaseActorFromWorker(ctx, name, assignment.GetActorUid()); err != nil {
			return fmt.Errorf("while releasing actor %s from worker %s: %w", assignment.GetActorUid(), name, err)
		}
		return nil
	}

	if assignment.GetActor() == nil {
		return release()
	}
	actorRef := resources.ActorRefFromObjectRef(assignment.GetActor())
	lease, err := w.store.AcquireLease(ctx, actorLeaseKey(actorRef))
	if errors.Is(err, store.ErrLeaseConflict) {
		return errActorBusy
	}
	if err != nil {
		return fmt.Errorf("while acquiring the lease of actor %s: %w", actorRef, err)
	}
	defer lease.Close()
	ctx = lease.Context()

	actor, err := w.store.GetActor(ctx, actorRef)
	if errors.Is(err, store.ErrNotFound) {
		return release()
	}
	if err != nil {
		return fmt.Errorf("while getting actor to release from worker %s: %w", name, err)
	}
	if actor.GetMetadata().GetUid() != assignment.GetActorUid() {
		return release()
	}

	if actor.GetStatus().GetWorkerAssignment().GetWorker().GetName() != name {
		return release()
	}
	if actor.GetStatus().GetWorkerAssignment().GetWorkerEpoch() >= epoch {
		return nil
	}
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		// The restart took the sandbox but not the actor's directory, and no
		// RPC told atelet, which still registers the actor, so its sweep
		// would keep the directory until atelet restarts.
		w.reclaimActorStateOnNode(ctx, worker, actor, node)
		if err := w.crashBoundActor(ctx, worker, actorRef, actor, "Releasing actor from a worker whose ateom restarted", crashMessageAteomRestarted); err != nil {
			return err
		}
	}
	return release()
}
