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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// workerReclaimTimeout bounds each Terminate a worker delete sends to reclaim a
// released actor's node state.
var workerReclaimTimeout = 30 * time.Second

// nodeReclaim carries, across the actors one DeleteWorker releases, whether
// their node's atelet is still worth asking. A worker lives on one node, so once
// a reclaim there times out or cannot connect, the rest would only wait out the
// same timeout; they are left for the orphan sweep.
type nodeReclaim struct {
	gaveUp bool
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
	worker, err := w.loadWorkerForDelete(ctx, name)
	if err != nil {
		return nil, err
	}

	// Checked against the Worker the caller observed, before the drain below
	// moves the version.
	if err := precondition.Check(worker.GetMetadata()); err != nil {
		switch {
		case errors.Is(err, store.ErrUIDConflict):
			return nil, apierror.Aborted("Worker %s does not have uid %s", name, precondition.UID)
		case errors.Is(err, store.ErrVersionConflict):
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		return nil, err
	}

	// Scheduling only places on ACTIVE Workers, so draining first stops a
	// concurrent resume from binding to a page the sweep has already passed.
	// The delete would cascade that assignment away and leave the Actor
	// pointing at a Worker that is gone.
	worker, err = w.ensureDraining(ctx, worker)
	if err != nil {
		return nil, err
	}

	// The drain moved the version, so only the uid guard still means anything:
	// a Worker replaced by a new incarnation mid-delete is still refused.
	precondition.Version = 0

	// Order matters: the delete is what erases the Actor's pointer at the
	// Worker, so a failed release has to leave the record in place for the
	// caller to rediscover and retry.
	if err := w.ensureBoundActorsReleased(ctx, worker); err != nil {
		return nil, err
	}

	return w.finalizeDeleted(ctx, name, precondition)
}

// loadWorkerForDelete fetches the current worker record. Reading before any of
// the release runs is also what reports an absent Worker as such.
func (w *WorkerWorkflow) loadWorkerForDelete(ctx context.Context, name string) (_ *ateapipb.Worker, err error) {
	ctx, done := stepSpan(ctx, "LoadWorkerForDelete")
	defer func() { err = done(err) }()

	worker, err := w.store.GetWorker(ctx, name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Worker %s not found", name)
		}
		return nil, fmt.Errorf("while fetching worker: %w", err)
	}
	return worker, nil
}

// ensureDraining moves the Worker out of the state scheduling will place on,
// and is a no-op for one already draining.
func (w *WorkerWorkflow) ensureDraining(ctx context.Context, worker *ateapipb.Worker) (_ *ateapipb.Worker, err error) {
	ctx, done := stepSpan(ctx, "DrainWorkerForDelete")
	defer func() { err = done(err) }()

	if worker.GetStatus().GetState() == ateapipb.WorkerState_WORKER_STATE_DRAINING {
		return worker, nil
	}
	drained, err := w.store.UpdateWorker(ctx, worker.GetMetadata().GetName(), store.PreconditionFrom(worker),
		func(toUpdate *ateapipb.Worker) error {
			if toUpdate.Status == nil {
				toUpdate.Status = &ateapipb.WorkerStatus{}
			}
			toUpdate.Status.State = ateapipb.WorkerState_WORKER_STATE_DRAINING
			return nil
		})
	if err != nil {
		return nil, fmt.Errorf("while draining worker for delete: %w", err)
	}
	return drained, nil
}

// ensureBoundActorsReleased resets every Actor bound to the Worker.
//
// A single failure stops the sweep, leaving the Worker record in place with the
// Actors that have not been released still bound to it, so a retry picks up
// where this left off.
func (w *WorkerWorkflow) ensureBoundActorsReleased(ctx context.Context, worker *ateapipb.Worker) (err error) {
	ctx, done := stepSpan(ctx, "ReleaseBoundActors")
	defer func() { err = done(err) }()

	// Every page: a Worker can hold thousands of Actors and a page holds at
	// most a thousand, so stopping at the first would leave the rest bound.
	var released int
	var node nodeReclaim
	for token := ""; ; {
		page, err := w.store.ListWorkerAssignments(ctx, worker.GetMetadata().GetName(), store.ListOptions{PageToken: token})
		if err != nil {
			return fmt.Errorf("while listing the assignments of worker %s: %w", worker.GetMetadata().GetName(), err)
		}
		for _, assignment := range page.Items {
			if err := w.releaseBoundActor(ctx, worker, assignment, &node); err != nil {
				return err
			}
			released++
		}
		if !page.HasNextPage() {
			break
		}
		token = page.NextPageToken
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
// A concurrent SuspendActor or ResumeActor wins the optimistic version check;
// this attempt fails as ABORTED so the caller retries against the newer state.
func (w *WorkerWorkflow) releaseBoundActor(ctx context.Context, worker *ateapipb.Worker, assignment *ateapipb.ActorAssignment, node *nodeReclaim) error {
	if assignment.GetActor() == nil {
		markSkipped(ctx, "assignment names no actor")
		return nil
	}
	name := worker.GetMetadata().GetName()
	actorRef := resources.ActorRefFromObjectRef(assignment.GetActor())
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
	// Reclaim before the state checks below: the disk the actor left on the
	// node has to go whether it suspended cleanly or crashed. This is the
	// earliest point to reclaim it, not the only one: a crash keeps the
	// actor's assigned node, so deleting or reverting the actor reclaims the
	// state there, and the node's orphan sweep collects it once atelet no
	// longer hosts the actor.
	w.reclaimActorStateOnNode(ctx, worker, actor, node)

	// If the actor is suspended, it's already been released.
	if actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		markSkipped(ctx, "actor suspended cleanly before the pod went away")
		return nil
	}
	return w.crashBoundActor(ctx, worker, actorRef, actor, "Releasing actor from a worker whose pod is gone", crashMessageWorkerPodGone)
}

// crashBoundActor moves an Actor that lost its sandbox on the Worker to
// ACTOR_STATE_CRASHED and clears its pointers at the Worker. crashMsg is
// recorded in the Actor's status. The Actor's assignment row in the worker is
// left for the caller to release.
// A concurrent write to the Actor fails this as ABORTED.
func (w *WorkerWorkflow) crashBoundActor(ctx context.Context, worker *ateapipb.Worker, actorRef resources.ActorRef, actor *ateapipb.Actor, logMsg, crashMsg string) error {
	name := worker.GetMetadata().GetName()
	opName := ateattr.OperationUnknown
	switch actor.GetStatus().GetState() {
	case ateapipb.ActorState_ACTOR_STATE_RESUMING:
		opName = ateattr.OperationResume
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDING:
		opName = ateattr.OperationSuspend
	case ateapipb.ActorState_ACTOR_STATE_PAUSING:
		opName = ateattr.OperationPause
	case ateapipb.ActorState_ACTOR_STATE_REVERTING:
		opName = ateattr.OperationRevert
	}

	wasAlreadyCrashed := actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_CRASHED

	// Snapshot crash attributes before pod and pool pointers are cleared on actor.
	crashAttrs := ateattr.ActorMetricAttributes(actor, worker.GetSandboxClass(), opName)

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
// actor, which is what reclaims the actor's state directory
// (/var/lib/ate/actors/<uid>: durable dir, checkpoint and restore
// images — gigabytes for a durdir actor) and unmounts its external volumes.
//
// The worker's pod, and the ateom with it, is gone by the time this runs; the
// node's atelet is not. atelet tolerates the missing ateom and reclaims the
// directories anyway.
//
// Best-effort by construction: a node that cannot be reached must not wedge the
// deregistration of its workers. Nothing retries this call, but what it misses
// is not lost: the actor's delete or revert reclaims it on its assigned node,
// and the orphan sweep collects it once atelet no longer hosts the actor. The
// Terminate gets workerReclaimTimeout: an unmount that hangs, such as one
// against a dead NFS server, would otherwise run out the delete's own deadline
// and fail the release after it.
func (w *WorkerWorkflow) reclaimActorStateOnNode(ctx context.Context, worker *ateapipb.Worker, actor *ateapipb.Actor, node *nodeReclaim) {
	ctx, done := stepSpan(ctx, "ReclaimActorStateOnNode")
	defer func() { _ = done(nil) }()

	if w.dialer == nil {
		markSkipped(ctx, "no atelet dialer configured")
		return
	}
	nodeName := worker.GetNodeName()
	if nodeName == "" {
		// Defensive: node_name is required and immutable, and the syncer
		// registers only Ready pods, which are scheduled by definition.
		markSkipped(ctx, "worker records no node")
		return
	}
	// A local snapshot is state this node holds deliberately, and Terminate
	// prunes local checkpoints. Leaving it is the conservative choice: a
	// wrongly-kept snapshot costs disk until the actor's delete reclaims it on
	// its assigned node, a wrongly-deleted one cannot be recovered at all.
	if _, local := findLatestSnapshotStorage(actor.GetStatus(), ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED); local != nil {
		markSkipped(ctx, "actor has a local snapshot pinned to the node")
		return
	}
	if node.gaveUp {
		markSkipped(ctx, "an earlier reclaim on the node timed out or could not connect")
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
		slog.WarnContext(ctx, "Could not reach the atelet holding a released actor's state; leaving it for the orphan sweep",
			append(logAttrs, slog.Any("err", err))...)
		return
	}

	slog.InfoContext(ctx, "Reclaiming the node state of an actor released from a worker whose pod is gone", logAttrs...)
	// The template is not resolvable from this workflow — and would be the
	// wrong thing to block on if it were, since a delete can outlive it. The
	// fallback spec carries what the teardown acts on: the external volumes to
	// unmount.
	callCtx, cancel := context.WithTimeout(ctx, workerReclaimTimeout)
	defer cancel()
	_, err = ateletpb.NewAteomHerderClient(conn).Terminate(callCtx, &ateletpb.TerminateRequest{
		TargetAteomUid:        worker.GetWorkerPodUid(),
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
		node.gaveUp = true
		slog.WarnContext(ctx, "The atelet holding a released actor's state did not answer; leaving it, and the worker's other actors' state, for the orphan sweep",
			append(logAttrs, slog.Any("err", err))...)
	default:
		slog.WarnContext(ctx, "Failed to reclaim a released actor's node state; leaving it for the orphan sweep",
			append(logAttrs, slog.Any("err", err))...)
	}
}

// finalizeDeleted removes the worker from the store and returns the deleted
// record. The request's guards are carried down as delete preconditions, so a
// worker that moved on since the caller read it is reported as a conflict rather
// than removed.
func (w *WorkerWorkflow) finalizeDeleted(ctx context.Context, name string, precondition store.DeletePreconditions) (_ *ateapipb.Worker, err error) {
	ctx, done := stepSpan(ctx, "FinalizeDeleted")
	defer func() { err = done(err) }()

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
