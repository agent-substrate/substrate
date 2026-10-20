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
	"time"

	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// defaultWorkerReclaimTimeout bounds each Terminate a worker delete sends to
// reclaim a released actor's node state, and how long a node whose atelet did
// not answer is skipped by subsequent reclaims.
const defaultWorkerReclaimTimeout = 30 * time.Second

// nodeGaveUp reports whether an earlier reclaim on nodeName timed out or could
// not reach atelet within w.reclaimTimeout, pruning any expired entries.
// Remembering the node across DeleteWorker calls keeps a hung atelet from
// stalling workersync on every worker pod deleted from that node.
func (w *WorkerWorkflow) nodeGaveUp(nodeName string) bool {
	now := w.now()
	w.mu.Lock()
	defer w.mu.Unlock()
	for node, until := range w.gaveUp {
		if !now.Before(until) {
			delete(w.gaveUp, node)
		}
	}
	_, ok := w.gaveUp[nodeName]
	return ok
}

// giveUpOnNode records that nodeName's atelet could not be reached or did not
// answer, so subsequent reclaims on that node are skipped for w.reclaimTimeout.
func (w *WorkerWorkflow) giveUpOnNode(nodeName string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.gaveUp[nodeName] = w.now().Add(w.reclaimTimeout)
}

// DeleteWorker executes the workflow to deregister a Worker. The caller reaches
// here because the Worker's pod is gone, so the Actor bound to it — if any — has
// lost its sandbox and is released before the record is removed.
//
// Re-drivable in the sense DeleteActor is: a failed attempt leaves the Worker
// record in place, and a retry fast-forwards past whatever the previous attempt
// already did. An absent Worker is NOT_FOUND rather than success; idempotency
// belongs to the caller, which knows whether that is the state it wanted.
func (w *WorkerWorkflow) DeleteWorker(ctx context.Context, name string, precondition store.DeletePreconditions) (*ateapipb.Worker, error) {
	ctx = preview.ContextWithOptIn(ctx)
	worker, err := w.ensureMarkedForDeletion(ctx, name, precondition)
	if err != nil {
		return nil, err
	}

	// Order matters: the delete is what erases the Actor's pointer at the
	// Worker, so a failed release has to leave the record in place for the
	// caller to rediscover and retry.
	if err := w.ensureBoundActorsReleased(ctx, worker); err != nil {
		return nil, err
	}

	return w.finalizeDeleted(ctx, worker)
}

// ensureMarkedForDeletion sets the Worker's delete_time, checking
// precondition, and drains it, returning the stored copy. Skips when a
// previous attempt already marked the Worker.
func (w *WorkerWorkflow) ensureMarkedForDeletion(ctx context.Context, name string, precondition store.DeletePreconditions) (_ *ateapipb.Worker, err error) {
	ctx, done := stepSpan(ctx, "MarkWorkerForDeletion")
	defer func() { err = done(err) }()

	var alreadyDeleting *ateapipb.Worker
	marked, err := w.store.MarkWorkerForDeletion(ctx, name, precondition, func(toUpdate *ateapipb.Worker) error {
		if toUpdate.GetMetadata().GetDeleteTime() != nil {
			alreadyDeleting = toUpdate
			return errAlreadyMarked
		}
		// Scheduling only places on ACTIVE Workers, so draining first stops a
		// concurrent resume from binding to a page the sweep has already passed.
		// The delete would cascade that assignment away and leave the Actor
		// pointing at a Worker that is gone.
		if toUpdate.Status == nil {
			toUpdate.Status = &ateapipb.WorkerStatus{}
		}
		toUpdate.Status.State = ateapipb.WorkerState_WORKER_STATE_DRAINING
		return nil
	})
	if errors.Is(err, errAlreadyMarked) {
		markSkipped(ctx, "worker already marked for deletion")
		return alreadyDeleting, nil
	}
	if err != nil {
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, apierror.Aborted("Worker %s does not have uid %s", name, precondition.UID)
		}
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Worker %s not found", name)
		}
		return nil, fmt.Errorf("while marking worker for deletion: %w", err)
	}
	return marked, nil
}

// ensureBoundActorsReleased resets every Actor bound to the Worker.
//
// A single failure stops the sweep, leaving the Worker record in place with the
// Actors that have not been released still bound to it, so a retry picks up
// where this left off. An Actor with an operation in progress is skipped
// instead, so the others are still released, and the call then fails as
// ABORTED for the caller to retry once that operation is done.
func (w *WorkerWorkflow) ensureBoundActorsReleased(ctx context.Context, worker *ateapipb.Worker) (err error) {
	ctx, done := stepSpan(ctx, "ReleaseBoundActors")
	defer func() { err = done(err) }()

	// Every page: a Worker can hold thousands of Actors and a page holds at
	// most a thousand, so stopping at the first would leave the rest bound.
	var released, busy int
	for token := ""; ; {
		page, err := w.store.ListWorkerAssignments(ctx, worker.GetMetadata().GetName(), store.ListOptions{PageToken: token})
		if err != nil {
			return fmt.Errorf("while listing the assignments of worker %s: %w", worker.GetMetadata().GetName(), err)
		}
		for _, assignment := range page.Items {
			err := w.releaseBoundActor(ctx, worker, assignment)
			if errors.Is(err, errActorBusy) {
				busy++
				continue
			}
			if err != nil {
				return err
			}
			released++
		}
		if !page.HasNextPage() {
			break
		}
		token = page.NextPageToken
	}
	if busy > 0 {
		return apierror.Aborted("%d actors on worker %s have an operation in progress; left for a retry", busy, worker.GetMetadata().GetName())
	}
	if released == 0 {
		markSkipped(ctx, "worker has no actors assigned")
	}
	return nil
}

// releaseBoundActor resets one Actor bound to the Worker. An Actor that already
// reached ACTOR_STATE_SUSPENDED saved its state cleanly during graceful
// termination, so it is left untouched and remains resumable. An Actor that was
// still running when the pod disappeared is moved to ACTOR_STATE_CRASHED and its
// pod pointers are cleared.
//
// Nothing to release is the common case and reports success: a superseded
// assignment and an Actor that has since moved elsewhere both leave no Actor
// pointing at this Worker, which is the state this is driving towards.
//
// It runs under the Actor's lease, as releaseEarlierAssignment does, so no
// operation on the Actor is midway: a suspend cannot finalize a snapshot while
// the reclaim below runs, nor lose a checkpoint it is still uploading. An Actor
// whose lease is held is left for a retry (errActorBusy). A write that still
// races this one fails the version check as ABORTED.
func (w *WorkerWorkflow) releaseBoundActor(ctx context.Context, worker *ateapipb.Worker, assignment *ateapipb.ActorAssignment) error {
	if assignment.GetActor() == nil {
		markSkipped(ctx, "assignment names no actor")
		return nil
	}
	name := worker.GetMetadata().GetName()
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
		markSkipped(ctx, "assigned actor no longer exists")
		return nil
	}
	if err != nil {
		return fmt.Errorf("while getting actor to release from worker %s: %w", name, err)
	}
	if actor.GetMetadata().GetUid() != assignment.GetActorUid() {
		markSkipped(ctx, "assignment names a superseded actor incarnation")
		return nil
	}
	// Skip if a concurrent SuspendActor already cleared the pointer, or if the
	// actor has since been placed on a different worker.
	if actor.GetStatus().GetWorkerAssignment().GetWorker().GetName() != name {
		markSkipped(ctx, "actor no longer points at this worker")
		return nil
	}
	// If the actor is suspended, it's already been released.
	if actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		markSkipped(ctx, "actor suspended cleanly before the pod went away")
		return nil
	}

	// Reclaim the actor's runtime state on the node before crashing it. A
	// crash keeps the actor's assigned node, so if this best-effort reclaim is
	// skipped or fails, deleting or reverting the actor reclaims the state
	// there later.
	w.reclaimActorStateOnNode(ctx, worker, actor)

	return w.crashBoundActor(ctx, worker, actorRef, actor, "Releasing actor from a worker whose pod is gone", crashMessageWorkerPodGone)
}

// crashBoundActor moves an Actor that lost its sandbox on the Worker to
// ACTOR_STATE_CRASHED and clears its pointers at the Worker. crashMsg is
// recorded in the Actor's status. An Actor in ACTOR_STATE_DELETING is left
// untouched. The Actor's assignment row in the worker is left for the caller to
// release.
// A concurrent write to the Actor fails this as ABORTED.
func (w *WorkerWorkflow) crashBoundActor(ctx context.Context, worker *ateapipb.Worker, actorRef resources.ActorRef, actor *ateapipb.Actor, logMsg, crashMsg string) error {
	// DELETING is final. A CRASHED actor can be reverted and resumed, and the
	// delete may already have collected its volumes and snapshots. The delete's
	// retry clears the actor's pointers.
	if actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_DELETING {
		markSkipped(ctx, "actor is DELETING")
		return nil
	}

	name := worker.GetMetadata().GetName()
	opName := ateattr.OperationUnknown
	switch actor.GetStatus().GetState() {
	case ateapipb.ActorState_ACTOR_STATE_RESUMING:
		opName = ateattr.OperationResume
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDING:
		opName = ateattr.OperationSuspend
	case ateapipb.ActorState_ACTOR_STATE_REVERTING:
		opName = ateattr.OperationRevert
	}

	wasAlreadyCrashed := actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_CRASHED

	// Snapshot crash attributes before pod and pool pointers are cleared on actor.
	sandboxClass := actor.GetStatus().GetWorkerAssignment().GetSandboxRuntime().GetSandboxClass()
	crashAttrs := ateattr.ActorMetricAttributes(actor, sandboxClass, opName)

	slog.LogAttrs(ctx, slog.LevelInfo, logMsg,
		append(ateattr.ActorLogAttrs(resources.ActorAttributionFromActor(actor)),
			slog.String("worker", name))...)
	_, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
		if !wasAlreadyCrashed {
			toUpdate.Status.Crash = newActorCrash(opName, crashMsg)
		}
		toUpdate.Status.WorkerAssignment = nil
		return nil
	})
	switch {
	case err == nil:
	case errors.Is(err, store.ErrNotFound):
		// The actor was deleted out from under us; nothing points here anymore.
		return nil
	case errors.Is(err, store.ErrUIDConflict), errors.Is(err, store.ErrVersionConflict):
		return apierror.Aborted("concurrent update conflict, please retry")
	default:
		return fmt.Errorf("while releasing actor from worker %s: %w", name, err)
	}

	if !wasAlreadyCrashed {
		logActorCrashed(ctx, actor, opName)
		recordActorCrash(ctx, crashAttrs)
	}
	return nil
}

// reclaimActorStateOnNode asks the atelet on the worker's node to terminate the
// actor, removing /var/lib/ate/actors/<uid> and /var/lib/ate/snapshots/<uid>
// (durable dir, checkpoint and restore images, and local snapshots) and
// unmounting its external volumes.
//
// The worker's pod and process are gone by the time this runs, while the
// node's atelet remains and reclaims the directories without a live worker.
//
// This call is best-effort so an unreachable node cannot wedge worker
// deregistration. If it fails, the actor's revert or delete reclaims the state
// on its assigned node. Each Terminate gets w.reclaimTimeout so a hung unmount,
// such as against a dead NFS server, cannot run out the delete's own deadline
// and fail the release after it.
func (w *WorkerWorkflow) reclaimActorStateOnNode(ctx context.Context, worker *ateapipb.Worker, actor *ateapipb.Actor) {
	ctx, done := stepSpan(ctx, "ReclaimActorStateOnNode")
	defer func() { _ = done(nil) }()

	nodeName := worker.GetNodeName()
	if w.nodeGaveUp(nodeName) {
		markSkipped(ctx, "an earlier reclaim on the node timed out or could not reach atelet")
		return
	}

	actorRef := resources.ActorRefFromActor(actor)
	logAttrs := []any{
		slog.Any("actor", actorRef),
		slog.String("actor_uid", actor.GetMetadata().GetUid()),
		slog.String("worker", worker.GetMetadata().GetName()),
		slog.String("node", nodeName),
	}

	conn, err := w.dialer.DialForAteletOnNode(nodeName)
	if err != nil {
		// Includes ErrNoAteletOnNode: the atelet is restarting, or the node
		// itself is gone. Nothing to reclaim against right now.
		w.giveUpOnNode(nodeName)
		slog.WarnContext(ctx, "Could not reach the atelet holding a released actor's state; leaving it, and other actors' state on the node, for their revert or delete",
			append(logAttrs, slog.Any("err", err))...)
		return
	}

	slog.InfoContext(ctx, "Reclaiming the node state of an actor released from a worker whose pod is gone", logAttrs...)
	// The template is not resolvable from this workflow, and a delete can
	// outlive it. The fallback spec carries the external volumes to unmount.
	callCtx, cancel := context.WithTimeout(ctx, w.reclaimTimeout)
	defer cancel()
	_, err = ateletpb.NewAteletClient(conn).Terminate(callCtx, &ateletpb.TerminateRequest{
		WorkerPodUid:          worker.GetWorkerPodUid(),
		Atespace:              actor.GetMetadata().GetAtespace(),
		ActorName:             actor.GetMetadata().GetName(),
		ActorUid:              actor.GetMetadata().GetUid(),
		ActorTemplateAtespace: actor.GetActorTemplate().GetAtespace(),
		ActorTemplateName:     actor.GetActorTemplate().GetName(),
		Spec:                  fallbackWorkloadSpec(actor),
	})
	switch status.Code(err) {
	case codes.OK:
	case codes.NotFound:
		slog.InfoContext(ctx, "Actor already terminated on its node", logAttrs...)
	case codes.DeadlineExceeded, codes.Unavailable:
		w.giveUpOnNode(nodeName)
		slog.WarnContext(ctx, "The atelet holding a released actor's state did not answer; leaving it, and other actors' state on the node, for their revert or delete",
			append(logAttrs, slog.Any("err", err))...)
	default:
		slog.WarnContext(ctx, "Failed to reclaim a released actor's node state; leaving it for the actor's revert or delete",
			append(logAttrs, slog.Any("err", err))...)
	}
}

// finalizeDeleted removes the worker from the store and returns the deleted
// record.
func (w *WorkerWorkflow) finalizeDeleted(ctx context.Context, worker *ateapipb.Worker) (_ *ateapipb.Worker, err error) {
	ctx, done := stepSpan(ctx, "FinalizeDeleted")
	defer func() { err = done(err) }()

	name := worker.GetMetadata().GetName()
	// Only the uid guards the delete. Each actor the sweep releases updates the
	// Worker's allocation and moves its version. The uid still refuses a Worker
	// replaced by a new incarnation.
	precondition := store.DeletePreconditions{UID: worker.GetMetadata().GetUid()}
	deleted, err := w.store.DeleteWorker(ctx, name, precondition)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			return nil, apierror.NotFound("Worker %s not found", name)
		case errors.Is(err, store.ErrUIDConflict):
			return nil, apierror.Aborted("Worker %s does not have uid %s", name, precondition.UID)
		case errors.Is(err, store.ErrVersionConflict):
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while deleting worker from DB: %w", err)
	}
	return deleted, nil
}
