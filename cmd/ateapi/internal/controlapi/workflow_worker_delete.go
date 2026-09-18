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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DeleteWorker executes the workflow to deregister a Worker. The caller reaches
// here because the Worker's pod is gone, so the Actor bound to it — if any — has
// lost its sandbox and is released before the record is removed.
//
// Re-drivable in the sense DeleteActor is: a failed attempt leaves the Worker
// record in place, and a retry fast-forwards past whatever the previous attempt
// already did. An absent Worker is NOT_FOUND rather than success; idempotency
// belongs to the caller, which knows whether that is the state it wanted.
func (w *WorkerWorkflow) DeleteWorker(ctx context.Context, name string, pre store.DeletePreconditions) (*ateapipb.Worker, error) {
	worker, err := w.loadWorkerForDelete(ctx, name)
	if err != nil {
		return nil, err
	}

	// Checked against the Worker the caller observed, before the drain below
	// moves the version.
	if err := pre.Check(worker.GetMetadata()); err != nil {
		switch {
		case errors.Is(err, store.ErrUIDConflict):
			return nil, status.Errorf(codes.Aborted, "Worker %s does not have uid %s", name, pre.UID)
		case errors.Is(err, store.ErrVersionConflict):
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
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
	pre.Version = 0

	// Order matters: the delete is what erases the Actor's pointer at the
	// Worker, so a failed release has to leave the record in place for the
	// caller to rediscover and retry.
	if err := w.ensureBoundActorsReleased(ctx, worker); err != nil {
		return nil, err
	}

	return w.finalizeDeleted(ctx, name, pre)
}

// loadWorkerForDelete fetches the current worker record. Reading before any of
// the release runs is also what reports an absent Worker as such.
func (w *WorkerWorkflow) loadWorkerForDelete(ctx context.Context, name string) (_ *ateapipb.Worker, err error) {
	ctx, done := stepSpan(ctx, "LoadWorkerForDelete")
	defer func() { err = done(err) }()

	worker, err := w.store.GetWorker(ctx, name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Worker %s not found", name)
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
	for token := ""; ; {
		page, err := w.store.ListWorkerAssignments(ctx, worker.GetMetadata().GetName(), store.ListOptions{PageToken: token})
		if err != nil {
			return fmt.Errorf("while listing the assignments of worker %s: %w", worker.GetMetadata().GetName(), err)
		}
		for _, assignment := range page.Items {
			if err := w.releaseBoundActor(ctx, worker, assignment); err != nil {
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
func (w *WorkerWorkflow) releaseBoundActor(ctx context.Context, worker *ateapipb.Worker, assignment *ateapipb.ActorAssignment) error {
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
	// node has to go whether it suspended cleanly or crashed, and this is the
	// last moment anything still knows which node that is. Once the record is
	// released, the actor names no worker, no worker names a node, and nothing
	// ever revisits the actor's UID — the directories are orphaned for the life
	// of the node.
	w.reclaimActorStateOnNode(ctx, worker, actor)

	// If the actor is suspended, it's already been released.
	if actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		markSkipped(ctx, "actor suspended cleanly before the pod went away")
		return nil
	}
	opName := ateattr.OperationUnknown
	switch actor.GetStatus().GetState() {
	case ateapipb.ActorState_ACTOR_STATE_RESUMING:
		opName = ateattr.OperationResume
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDING:
		opName = ateattr.OperationSuspend
	case ateapipb.ActorState_ACTOR_STATE_PAUSING:
		opName = ateattr.OperationPause
	}

	wasAlreadyCrashed := actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_CRASHED

	// Snapshot crash attributes before pod and pool pointers are cleared on actor.
	crashAttrs := ateattr.ActorMetricAttributes(actor, worker.GetSandboxClass(), opName, ateattr.ReasonWorkerPodGone)

	slog.LogAttrs(ctx, slog.LevelInfo, "Releasing actor from a worker whose pod is gone",
		append(ateattr.ActorLogAttrs(resources.ActorAttributionFromActor(actor)),
			slog.String("worker", name))...)
	_, err = w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
		toUpdate.Status.WorkerAssignment = nil
		// Local in-progress checkpoint dies with the worker: it lived on the node
		// that went away. The external in-progress checkpoint is kept. It'll be deleted
		// with the actor when the actor is deleted (only possible outcome from CRASHED
		// state).
		toUpdate.Status.InProgressLocalSnapshotName = ""
		return nil
	})
	switch {
	case err == nil:
	case errors.Is(err, store.ErrNotFound):
		// The actor was deleted out from under us; nothing points here anymore.
		return nil
	case errors.Is(err, store.ErrUIDConflict), errors.Is(err, store.ErrVersionConflict):
		return status.Error(codes.Aborted, "concurrent update conflict, please retry")
	default:
		return fmt.Errorf("while releasing actor from worker %s: %w", name, err)
	}

	if !wasAlreadyCrashed {
		logActorCrashed(ctx, actor, opName, ateattr.ReasonWorkerPodGone)
		recordActorCrash(ctx, crashAttrs)
	}
	return nil
}

// reclaimActorStateOnNode asks the atelet on the worker's node to terminate the
// actor, which is what reclaims the actor's state directory
// (/var/lib/ateom-gvisor/actors/<uid>: durable dir, checkpoint and restore
// images — gigabytes for a durdir actor) and unmounts its external volumes.
//
// The worker's pod is gone by the time this runs, so the atelet is reached by
// node: the by-pod lookup the actor workflows use resolves through the pod that
// has just disappeared, and returning "pod not found" there is exactly how this
// state came to be orphaned. atelet tolerates the ateom being gone and reclaims
// the directories anyway.
//
// Best-effort by construction: a node that cannot be reached must not wedge the
// deregistration of its workers, and the reclaim has no record of its own to
// retry from once the worker is deleted. What it misses — an unreachable atelet,
// a node that never comes back, an abrupt eviction — is the orphan sweep's to
// collect.
func (w *WorkerWorkflow) reclaimActorStateOnNode(ctx context.Context, worker *ateapipb.Worker, actor *ateapipb.Actor) {
	ctx, done := stepSpan(ctx, "ReclaimActorStateOnNode")
	defer func() { _ = done(nil) }()

	if w.dialer == nil {
		markSkipped(ctx, "no atelet dialer configured")
		return
	}
	nodeName := worker.GetNodeName()
	if nodeName == "" {
		// Pre-dates the field, or a Worker registered before its pod was
		// scheduled. Nothing names the node holding the state.
		markSkipped(ctx, "worker records no node")
		return
	}
	// A local snapshot is state this node holds deliberately, and Terminate
	// prunes local checkpoints. Leaving it is the conservative choice: a
	// wrongly-kept snapshot costs disk the sweep can still reclaim later, a
	// wrongly-deleted one cannot be recovered at all.
	if actor.GetStatus().GetLocalSnapshotInfo() != nil {
		markSkipped(ctx, "actor has a local snapshot pinned to the node")
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
	_, err = ateletpb.NewAteomHerderClient(conn).Terminate(ctx, &ateletpb.TerminateRequest{
		TargetAteomUid:        worker.GetWorkerPodUid(),
		Atespace:              actor.GetMetadata().GetAtespace(),
		ActorName:             actor.GetMetadata().GetName(),
		ActorUid:              actor.GetMetadata().GetUid(),
		ActorTemplateAtespace: actor.GetActorTemplate().GetAtespace(),
		ActorTemplateName:     actor.GetActorTemplate().GetName(),
		Spec:                  fallbackWorkloadSpec(actor),
	})
	switch {
	case err == nil:
	case status.Code(err) == codes.NotFound:
		slog.InfoContext(ctx, "Actor already terminated on its node", logAttrs...)
	default:
		slog.WarnContext(ctx, "Failed to reclaim a released actor's node state; leaving it for the orphan sweep",
			append(logAttrs, slog.Any("err", err))...)
	}
}

// finalizeDeleted removes the worker from the store and returns the deleted
// record. The request's guards are carried down as delete preconditions, so a
// worker that moved on since the caller read it is reported as a conflict rather
// than removed.
func (w *WorkerWorkflow) finalizeDeleted(ctx context.Context, name string, pre store.DeletePreconditions) (_ *ateapipb.Worker, err error) {
	ctx, done := stepSpan(ctx, "FinalizeDeleted")
	defer func() { err = done(err) }()

	deleted, err := w.store.DeleteWorker(ctx, name, pre)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			return nil, status.Errorf(codes.NotFound, "Worker %s not found", name)
		case errors.Is(err, store.ErrUIDConflict):
			return nil, status.Errorf(codes.Aborted, "Worker %s does not have uid %s", name, pre.UID)
		case errors.Is(err, store.ErrVersionConflict):
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while deleting worker from DB: %w", err)
	}
	return deleted, nil
}
