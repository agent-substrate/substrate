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
	"log/slog"
	"slices"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/objectstore"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// revertableStates are the states a revert is accepted from.
var revertableStates = []ateapipb.ActorState{
	ateapipb.ActorState_ACTOR_STATE_RUNNING,
	ateapipb.ActorState_ACTOR_STATE_PAUSED,
	ateapipb.ActorState_ACTOR_STATE_CRASHED,
}

// RevertActor executes the workflow to discard an actor's current execution and
// return it to SUSPENDED, leaving its external snapshot untouched for a later
// resume to restore from. It is the recovery verb: it accepts a CRASHED actor,
// and needs no live worker to succeed.
//
// Idempotent: each step keys on a persisted field rather than on the state the
// actor was reverted from, so a re-entered workflow fast-forwards to wherever
// the previous attempt stopped.
func (w *ActorWorkflow) RevertActor(ctx context.Context, actorRef resources.ActorRef) (_ *ateapipb.Actor, err error) {
	start := time.Now()
	var actor *ateapipb.Actor
	var actorTemplate *ateapipb.ActorTemplate
	// Set just before finalize; nil until then, so earlier exits label
	// themselves from the record they hold.
	var finalAttrs []attribute.KeyValue

	defer func() {
		attrs := finalAttrs
		if attrs == nil {
			attrs = lifecycleOpAttrs(actor, actorTemplate, "", "")
		}
		w.instruments.recordLifecycleOp(ctx, ateattr.OperationRevert, start, err, attrs...)
	}()

	leaseCtx, lease, err := w.acquireActorLease(ctx, actorRef)
	if err != nil {
		return nil, err
	}
	defer lease.Close()

	actor, actorTemplate, err = w.loadActorForRevert(leaseCtx, actorRef)
	if err != nil {
		return nil, err
	}

	var marked *ateapipb.Actor
	if marked, err = w.ensureMarkedReverting(leaseCtx, actorRef, actor); err != nil {
		return nil, err
	}
	actor = marked

	if err = w.ensureWorkerDiscarded(leaseCtx, actorRef, actor, actorTemplate); err != nil {
		return nil, err
	}

	if err = w.ensureInProgressSnapshotDiscarded(leaseCtx, actor); err != nil {
		return nil, err
	}

	// TODO: Check if local checkpoints need to be pruned #641
	// w.ensureLocalCheckpointsPruned(leaseCtx, actor)

	// FinalizeReverted clears the WorkerAssignment the labels read, so snapshot
	// them here, as suspend and crash.go do.
	finalAttrs = lifecycleOpAttrs(actor, actorTemplate, "", "")
	var finalized *ateapipb.Actor
	if finalized, err = w.ensureRevertedFinalized(leaseCtx, actorRef); err != nil {
		return nil, err
	}
	return finalized, nil
}

// loadActorForRevert fetches the current actor record and its template.
func (w *ActorWorkflow) loadActorForRevert(ctx context.Context, actorRef resources.ActorRef) (_ *ateapipb.Actor, _ *ateapipb.ActorTemplate, err error) {
	ctx, done := stepSpan(ctx, "LoadActorForRevert")
	defer func() { err = done(err) }()

	actor, err := w.store.GetActor(ctx, actorRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil, status.Errorf(codes.NotFound, "Actor %s not found", actorRef)
		}
		return nil, nil, fmt.Errorf("while fetching actor: %w", err)
	}

	actorTemplate, err := resolveActorTemplate(ctx, w.store, actor)
	if errors.Is(err, errActorTemplateNotFound) {
		slog.WarnContext(ctx, "Reverting an actor whose template no longer resolves",
			slog.Any("actor", actorRef),
			slog.String("templateAtespace", actor.GetActorTemplate().GetAtespace()),
			slog.String("templateName", actor.GetActorTemplate().GetName()))
		return actor, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	return actor, actorTemplate, nil
}

// ensureMarkedReverting transitions the actor to REVERTING and persists the
// change. Skips when a previous attempt already marked it, which is what makes
// the rest of the workflow re-enterable.
func (w *ActorWorkflow) ensureMarkedReverting(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor) (_ *ateapipb.Actor, err error) {
	ctx, done := stepSpan(ctx, "MarkReverting")
	defer func() { err = done(err) }()

	st := actor.GetStatus().GetState()
	if st == ateapipb.ActorState_ACTOR_STATE_REVERTING {
		markSkipped(ctx, "actor already REVERTING")
		return actor, nil
	}
	if !slices.Contains(revertableStates, st) {
		return nil, status.Errorf(codes.FailedPrecondition, "Actor %s is not in a revertable state (got: %v, want one of %v)", actorRef, st, revertableStates)
	}

	storedActor, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_REVERTING
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while setting actor state to REVERTING: %w", err)
	}
	logActorStateChanged(ctx, storedActor, ateattr.OperationRevert)
	return storedActor, nil
}

// ensureWorkerDiscarded tears down whatever the actor is running on and frees
// the worker it booked — including one the actor does not name.
//
// The assignment-backed path mirrors delete's ordering: terminate, detach, and
// only then release, so a worker is never handed back while a sandbox or a
// mount may still be live on it. The assignment itself is left on the record
// for FinalizeReverted to clear, so an interrupted release is rediscoverable.
//
// The sweep afterwards runs on every origin, not just when the record names no
// worker. An assignment commits before the actor is updated to point at it, so
// a crash in between leaves a row nothing references — and a CRASHED actor is
// the likeliest holder of one. Absence is the ordinary case, not an error.
func (w *ActorWorkflow) ensureWorkerDiscarded(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor, actorTemplate *ateapipb.ActorTemplate) (err error) {
	ctx, done := stepSpan(ctx, "DiscardWorker")
	defer func() { err = done(err) }()

	if assignment := actor.GetStatus().GetWorkerAssignment(); assignment != nil {
		hosted, err := workerHostsActor(ctx, w.store, assignment.GetWorker().GetName(), actor.GetMetadata().GetUid())
		if err != nil {
			return err
		}
		if hosted {
			if terr := w.ensureAteletTerminated(ctx, actorRef, actor, actorTemplate); terr != nil {
				// A failed terminate lands the actor in CRASHED, which the user
				// can revert again — that retry needs no live worker.
				return crashActorOnError(ctx, w.store, actorRef, terr, ateattr.OperationRevert)
			}
			if err := w.ensureVolumesDetached(ctx, actor, actorTemplate, "DetachVolumesForRevert", ateattr.OperationRevert); err != nil {
				return err
			}
			if _, _, err := releaseWorker(ctx, w.store, actor); err != nil {
				return fmt.Errorf("while releasing worker: %w", err)
			}
		}
	}

	return w.releaseAssignmentWithoutBacklink(ctx, actor)
}

// ensureInProgressSnapshotDiscarded deletes the objects a suspend was partway
// through writing when the actor was reverted.
func (w *ActorWorkflow) ensureInProgressSnapshotDiscarded(ctx context.Context, actor *ateapipb.Actor) (err error) {
	ctx, done := stepSpan(ctx, "DiscardInProgressSnapshot")
	defer func() { err = done(err) }()

	inProgress := actor.GetStatus().GetInProgressSnapshotUri()
	switch {
	case w.objectStore == nil:
		markSkipped(ctx, "no object store configured")
		return nil
	case inProgress == "":
		markSkipped(ctx, "no in-progress snapshot recorded")
		return nil
	}

	uri, err := resources.ParseSnapshotURI(inProgress)
	if err != nil {
		return fmt.Errorf("while parsing the in-progress snapshot %q: %w", inProgress, err)
	}
	// A suspend records the in-progress URI under the actor's own prefix
	// before atelet writes the first object, so a URI owned by anything else
	// is a corrupted record.
	owner := actorSnapshotOwner(actor)
	if !uri.OwnedBy(owner) {
		return fmt.Errorf("the in-progress snapshot %q is not owned by actor %s", inProgress, owner)
	}
	// Only the abandoned snapshot goes, not the actor's whole prefix: the
	// external snapshot the revert returns the actor to lives under it too.
	return objectstore.DeletePrefix(ctx, w.objectStore, uri.Prefix())
}

// ensureRevertedFinalized commits SUSPENDED and drops every pointer to the
// execution that was discarded, in a single update.
// It re-reads the actor first so an out-of-band transition is not overwritten,
// and so the version precondition guards what was actually observed.
func (w *ActorWorkflow) ensureRevertedFinalized(ctx context.Context, actorRef resources.ActorRef) (_ *ateapipb.Actor, err error) {
	ctx, done := stepSpan(ctx, "FinalizeReverted")
	defer func() { err = done(err) }()

	latestActor, err := w.store.GetActor(ctx, actorRef)
	if err != nil {
		return nil, err
	}
	if got := latestActor.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_REVERTING {
		return nil, status.Errorf(codes.FailedPrecondition, "FinalizeReverted prerequisite not met for Actor: %s (got: %v, want %s)", actorRef, got, ateapipb.ActorState_ACTOR_STATE_REVERTING)
	}

	storedActor, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(latestActor), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
		toUpdate.Status.WorkerAssignment = nil
		toUpdate.Status.InProgressSnapshotUri = ""
		toUpdate.Status.InProgressLocalSnapshotName = ""
		toUpdate.Status.LocalSnapshotInfo = nil
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return nil, err
	}
	logActorStateChanged(ctx, storedActor, ateattr.OperationRevert)
	return storedActor, nil
}
