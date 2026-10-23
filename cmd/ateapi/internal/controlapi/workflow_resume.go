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

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/scheduling"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
)

// restoreTelemetry labels the restore operation for the resume lifecycle
// metric. WireFidelity describes the restore requested, not the stored
// snapshot's scope: a MEMORY snapshot restored under a replaced template goes
// out as data.
type restoreTelemetry struct {
	SnapshotKind string
	WireFidelity string
}

// ResumeActor executes the workflow to resume a suspended actor. Idempotent:
// a re-entered workflow fast-forwards past the steps a previous attempt
// completed, deriving progress from the persisted actor alone.
func (w *ActorWorkflow) ResumeActor(ctx context.Context, actorRef resources.ActorRef) (_ *ateapi.Actor, resumed bool, err error) {
	ctx = preview.ContextWithOptIn(ctx)
	start := time.Now()
	var actor *ateapi.Actor
	var actorTemplate *ateapi.ActorTemplate
	var tele restoreTelemetry
	var wasRunning bool

	// Recorded before the lease so lease contention still counts as an attempt.
	// Clean already-running no-ops are skipped: the router resumes per routed
	// request, and recording those would sample at router QPS and bury
	// cold-resume latency.
	defer func() {
		if err == nil && wasRunning {
			return
		}
		w.instruments.recordLifecycleOp(ctx, ateattr.OperationResume, start, err,
			lifecycleOpAttrs(actor, tele.SnapshotKind, tele.WireFidelity)...)
	}()

	// Routed requests call ResumeActor even when the actor is already running.
	// Read before taking the distributed lease so that hot-path checks do not
	// upsert and delete a PostgreSQL lease row. Any state that needs work is read
	// again under the lease below.
	actor, err = w.store.GetActor(ctx, actorRef)
	if err != nil {
		return nil, false, err
	}
	if wasRunning = actor.GetStatus().GetState() == ateapi.ActorState_ACTOR_STATE_RUNNING; wasRunning {
		return actor, false, nil
	}

	leaseCtx, lease, err := w.acquireActorLease(ctx, actorRef)
	if err != nil {
		return nil, false, err
	}
	defer lease.Close()

	actor, actorTemplate, err = w.loadActorForResume(leaseCtx, actorRef)
	if err != nil {
		return nil, false, err
	}
	if wasRunning = actor.GetStatus().GetState() == ateapi.ActorState_ACTOR_STATE_RUNNING; wasRunning {
		return actor, false, nil
	}
	var created *ateapi.Actor
	if created, err = w.ensureVolumesCreated(leaseCtx, actorRef, actor, actorTemplate); err != nil {
		return nil, false, err
	}
	actor = created
	var worker *ateapi.Worker
	var assigned *ateapi.Actor
	assigned, worker, err = w.ensureWorkerAssigned(leaseCtx, actorRef, actor, actorTemplate)
	if err != nil && shouldDetachFromNode(actor, err) {
		var detached *ateapi.Actor
		if detached, err = w.detachFromFullNode(leaseCtx, actorRef, actor, actorTemplate, err); err != nil {
			return nil, false, err
		}
		actor = detached
		assigned, worker, err = w.ensureWorkerAssigned(leaseCtx, actorRef, actor, actorTemplate)
	}
	if err != nil {
		return nil, false, err
	}
	actor = assigned
	volumePublishContexts, err := w.ensureVolumesAttached(leaseCtx, actor, worker, actorTemplate)
	if err != nil {
		return nil, false, err
	}
	var workerEpoch int64
	if tele, workerEpoch, err = w.ensureAteletRestored(leaseCtx, actorRef, actor, actorTemplate, volumePublishContexts); err != nil {
		return nil, false, err
	}
	var running *ateapi.Actor
	if running, err = w.finalizeRunning(leaseCtx, actorRef, workerEpoch); err != nil {
		return nil, false, err
	}
	actor = running
	return actor, true, nil
}

// loadActorForResume fetches the current actor record and its template.
func (w *ActorWorkflow) loadActorForResume(ctx context.Context, actorRef resources.ActorRef) (_ *ateapi.Actor, _ *ateapi.ActorTemplate, err error) {
	ctx, done := stepSpan(ctx, "LoadActorForResume")
	defer func() { err = done(err) }()

	actor, err := w.store.GetActor(ctx, actorRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil, apierror.NotFound("Actor %s not found", actorRef)
		}
		return nil, nil, fmt.Errorf("while getting actor from DB: %w", err)
	}

	// If the actor is already running, there is no pending restore to prepare
	// for. Short-circuit immediately to avoid an unnecessary template read on
	// the hot resume path.
	if actor.GetStatus().GetState() == ateapi.ActorState_ACTOR_STATE_RUNNING {
		return actor, nil, nil
	}

	actorTemplate, err := resolveActorTemplate(ctx, w.store, actor)
	if err != nil {
		return nil, nil, err
	}
	return actor, actorTemplate, nil
}

// checkSnapshotUpload fails with DataLoss when snapshotToResume records no
// upload. A local restore also names the snapshot's upload.
func checkSnapshotUpload(actorRef resources.ActorRef, actor *ateapi.Actor) error {
	if snap := snapshotToResume(actor.GetStatus()); snap != nil && snap.GetDurableSnapshot() == nil {
		return apierror.DataLoss("Actor %s snapshot %s records no upload", actorRef, snap.GetUuid())
	}
	return nil
}

// templateReplaced reports whether snap was captured under a different
// ActorTemplate than tmpl. An unstamped snapshot is treated as matching.
func templateReplaced(snap *ateapi.Snapshot, tmpl *ateapi.ActorTemplate) bool {
	capturedUnder := snap.GetActorTemplateUid()
	return capturedUnder != "" && capturedUnder != tmpl.GetMetadata().GetUid()
}

// snapshotSharing is what a restore declares about a snapshot with the
// given owner. A tag's snapshot is immutable once published and read by
// every actor created from it, so atelet may serve its files from the
// node-local cache; the actor's own is replaced by its next suspend, so
// never, and so is one whose owner is not recorded.
func snapshotSharing(owner ateapi.SnapshotOwner) ateletpb.SnapshotSharing {
	if owner == ateapi.SnapshotOwner_SNAPSHOT_OWNER_TAG {
		return ateletpb.SnapshotSharing_SNAPSHOT_SHARING_SHARED
	}
	return ateletpb.SnapshotSharing_SNAPSHOT_SHARING_PRIVATE
}

// ensureVolumesCreated provisions any actor volumes that have no storage
// volume ID yet, persisting the resulting volume state (even when creation
// partially failed, so progress is not lost) and returning the stored copy.
// An actor being deleted gets no new volumes.
func (w *ActorWorkflow) ensureVolumesCreated(ctx context.Context, actorRef resources.ActorRef, actor *ateapi.Actor, actorTemplate *ateapi.ActorTemplate) (_ *ateapi.Actor, err error) {
	ctx, done := stepSpan(ctx, "CreateVolumes")
	defer func() { err = done(err) }()

	pending := slices.ContainsFunc(actor.GetStatus().GetExternalVolumes().GetVolumes(), func(vol *ateapi.ExternalVolumeStatus) bool {
		return vol.GetStorageVolumeId() == ""
	})
	if !pending {
		markSkipped(ctx, "no volumes awaiting creation")
		return actor, nil
	}
	if st := actor.GetStatus().GetState(); st == ateapi.ActorState_ACTOR_STATE_DELETING {
		return nil, apierror.FailedPrecondition("cannot create volumes for Actor %s in state %s", actorRef, st)
	}

	volumes, createErr := createActorVolumes(ctx, w.pluginRegistry, w.storageClassLister, actor.GetMetadata().GetUid(), actorTemplate, actor.GetStatus().GetExternalVolumes().GetVolumes())
	// createActorVolumes reports the state it got to even when it fails, so both
	// paths persist the same field.
	updatePrecondition := store.PreconditionFrom(actor)
	persistVolumes := func(toUpdate *ateapi.Actor) error {
		toUpdate.Status.ExternalVolumes = externalVolumesStatus(volumes)
		return nil
	}
	if createErr != nil {
		// Even if volume creation failed, we still want to persist any updated volume state.
		if _, updateErr := w.store.UpdateActor(ctx, actorRef, updatePrecondition, persistVolumes); updateErr != nil {
			slog.ErrorContext(ctx, "failed to update actor volumes on volume creation failure in resume", slog.Any("error", updateErr))
		}
		return nil, createErr
	}
	storedActor, updateErr := w.store.UpdateActor(ctx, actorRef, updatePrecondition, persistVolumes)
	if updateErr != nil {
		if errors.Is(updateErr, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while updating actor after volume creation: %w", updateErr)
	}
	return storedActor, nil
}

// ensureWorkerAssigned assigns a worker and leaves the actor RESUMING.
// Re-entry validates the recorded assignment before reuse.
// Version conflicts re-read the actor before retrying.
func (w *ActorWorkflow) ensureWorkerAssigned(ctx context.Context, actorRef resources.ActorRef, actor *ateapi.Actor, actorTemplate *ateapi.ActorTemplate) (_ *ateapi.Actor, _ *ateapi.Worker, err error) {
	ctx, done := stepSpan(ctx, "AssignWorker")
	defer func() { err = done(err) }()

	switch actor.GetStatus().GetState() {
	case ateapi.ActorState_ACTOR_STATE_RESUMING:
		worker, err := w.validateAssignedWorker(ctx, actorRef, actor, actorTemplate)
		if err != nil {
			return nil, nil, err
		}
		markSkipped(ctx, "actor already RESUMING with a valid worker assignment")
		return actor, worker, nil
	case ateapi.ActorState_ACTOR_STATE_SUSPENDED:
	default:
		return nil, nil, apierror.FailedPrecondition("AssignWorker prerequisite not met for Actor: %s (got: %v, want %s)", actorRef, actor.GetStatus().GetState(), ateapi.ActorState_ACTOR_STATE_SUSPENDED)
	}

	var assignedActor *ateapi.Actor
	var assignedWorker *ateapi.Worker
	// ctxErr marks the loop ending between attempts, where no attempt ran and
	// so none recorded.
	var ctxErr error
	// lastAttempt times only the last attempt that ran, as every other record
	// on the histogram does. The backoff can sleep once more after the last
	// attempt before it gives up, so the time since that attempt began is too
	// long. attempted is false if the context ended before the first attempt.
	var lastAttempt time.Duration
	attempted := false
	err = wait.ExponentialBackoff(assignmentBackoff, func() (bool, error) {
		if err := ctx.Err(); err != nil {
			ctxErr = err
			return false, err
		}
		attemptStart := time.Now()
		attemptActor, attemptWorker, attemptErr := w.assignWorkerAttempt(ctx, actorRef, actor, actorTemplate)
		lastAttempt = time.Since(attemptStart)
		attempted = true
		if attemptErr == nil {
			assignedActor, assignedWorker = attemptActor, attemptWorker
			return true, nil
		}
		if errors.Is(attemptErr, store.ErrVersionConflict) || errors.Is(attemptErr, errWorkerFilledUp) {
			if attemptActor != nil {
				actor = attemptActor // retry with the refreshed actor
			}
			return false, nil
		}
		return false, attemptErr
	})
	// The retried attempts hid their own records, so the resume would leave no
	// trace on the histogram without one here.
	//
	// wait.Interrupted also matches a context error, which an attempt returns
	// when the context ends inside it; that attempt recorded itself. Only the
	// timeout of the backoff means that the retries ran out.
	exhausted := wait.Interrupted(err) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
	if exhausted && ctx.Err() != nil {
		// The context ended in the sleep after the last attempt.
		ctxErr = ctx.Err()
		err = ctxErr
	}
	switch {
	case err == nil:
		return assignedActor, assignedWorker, nil
	case ctxErr != nil:
		if attempted {
			w.recordExhaustedAssignment(ctx, lastAttempt, actor, ctxErr)
		}
		return nil, nil, err
	case exhausted:
		err = apierror.Aborted("concurrent update conflict, please retry: %w", store.ErrVersionConflict)
		w.recordExhaustedAssignment(ctx, lastAttempt, actor, err)
		return nil, nil, err
	default:
		// assignWorkerAttempt recorded this one itself.
		return nil, nil, err
	}
}

// assignmentBackoff bounds the retries of an assignment attempt that loses a
// race for a worker. The delay reaches Cap after 5 attempts, which ends the
// retries before Steps runs out, so the loop takes about 0.5 to 1 s.
var assignmentBackoff = wait.Backoff{
	Steps:    12,
	Duration: 15 * time.Millisecond,
	Factor:   2.0,
	Jitter:   1.0,
	Cap:      250 * time.Millisecond,
}

// recordExhaustedAssignment writes the one record of an assignment loop that
// ended without assigning. No worker was assigned, so no pool is named.
func (w *ActorWorkflow) recordExhaustedAssignment(ctx context.Context, elapsed time.Duration, actor *ateapi.Actor, err error) {
	w.instruments.recordSchedulerAssignment(ctx, elapsed, ateattr.SchedulerOutcomeError, "", "", snapshotSandboxClass(actor), err)
}

// validateAssignedWorker checks a RESUMING actor's persisted assignment
// against the current worker record. Every invalid outcome crashes the actor:
// a RESUMING actor whose worker vanished, drained, was reassigned, or is no
// longer eligible can never make progress on its own. A worker that is only
// unavailable is reported as UNAVAILABLE for the caller to retry.
func (w *ActorWorkflow) validateAssignedWorker(ctx context.Context, actorRef resources.ActorRef, actor *ateapi.Actor, actorTemplate *ateapi.ActorTemplate) (*ateapi.Worker, error) {
	assignment := actor.GetStatus().GetWorkerAssignment()
	if assignment == nil {
		slog.ErrorContext(ctx, "expected a worker assignment on a RESUMING actor, found none")

		// Crash the actor if its worker assignment is missing. We should never be in this state.
		if cerr := crashActor(ctx, w.store, actorRef, ateattr.OperationResume, crashMessageWorkerAssignmentMissing); cerr != nil {
			return nil, cerr
		}
		return nil, apierror.Aborted("actor %s crashed", actorRef)
	}

	worker, err := w.store.GetWorker(ctx, assignment.GetWorker().GetName())
	if err != nil {
		// Crash the actor if it was assigned to a deleted pod.
		if errors.Is(err, store.ErrNotFound) {
			if cerr := crashActor(ctx, w.store, actorRef, ateattr.OperationResume, crashMessageWorkerGone); cerr != nil {
				return nil, cerr
			}
			return nil, apierror.Aborted("actor %s crashed", actorRef)
		}
		return nil, fmt.Errorf("failed to get already assigned worker for actor %w", err)
	}
	if worker.GetStatus().GetState() == ateapi.WorkerState_WORKER_STATE_DRAINING {
		slog.InfoContext(ctx, "Assigned worker is draining; crashing actor",
			slog.String("actor", actorRef.String()),
			slog.String("worker", worker.GetWorkerNamespace()+"/"+worker.GetWorkerPod()))
		if cerr := crashActor(ctx, w.store, actorRef, ateattr.OperationResume, crashMessageWorkerDraining); cerr != nil {
			return nil, cerr
		}
		return nil, apierror.Aborted("actor %s crashed", actorRef.String())
	}
	// An unavailable worker may come back with its Actors intact. If its worker
	// process restarted instead, a retried resume restores this Actor on the
	// new process.
	if worker.GetStatus().GetState() == ateapi.WorkerState_WORKER_STATE_UNAVAILABLE {
		return nil, apierror.Unavailable("worker %s assigned to actor %s is unavailable", worker.GetMetadata().GetName(), actorRef)
	}
	if !scheduling.Registered(worker) {
		return nil, apierror.Unavailable("worker %s assigned to actor %s has not finished registering", worker.GetMetadata().GetName(), actorRef)
	}
	// Verify the worker is still hosting this Actor.
	hosted, err := workerHostsActor(ctx, w.store, worker.GetMetadata().GetName(), actor.GetMetadata().GetUid())
	if err != nil {
		return nil, err
	}
	if !hosted {
		slog.ErrorContext(ctx, "crashing actor because its assigned worker no longer hosts it",
			slog.String("worker", worker.GetWorkerPod()))
		if cerr := crashActor(ctx, w.store, actorRef, ateattr.OperationResume, crashMessageWorkerReassigned); cerr != nil {
			return nil, fmt.Errorf("while crashing actor: %w", cerr)
		}
		return nil, apierror.Aborted("actor %s crashed", actorRef)
	}
	constraints := schedulingConstraints(actor, actorTemplate)
	if !w.appliesWithFallback(worker, constraints, canRelaxRuntime(actor, constraints)) {
		slog.ErrorContext(ctx, "crashing actor because previously assigned worker is not eligible anymore")
		// If that worker's pool is no longer eligible (e.g. the actor's
		// worker_selector was updated after the failed attempt), release it back
		// to the free pool instead of leaving it claimed forever — nothing else
		// reclaims a healthy worker whose actor moved on to a different pool.
		if _, err := w.store.ReleaseActorFromWorker(ctx, worker.GetMetadata().GetName(), actor.GetMetadata().GetUid()); err != nil {
			return nil, fmt.Errorf("while releasing stale worker assignment: %w", err)
		}
		if cerr := crashActor(ctx, w.store, actorRef, ateattr.OperationResume, crashMessageWorkerIneligible); cerr != nil {
			return nil, fmt.Errorf("while crashing actor: %w", cerr)
		}
		return nil, apierror.Aborted("actor %s crashed", actorRef)
	}
	return worker, nil
}

// admittedResources is what an assignment books against its worker, or nil
// when the actor declared no limits and so reserves nothing.
func admittedResources(constraints scheduling.Constraints) *ateapi.Resources {
	return constraints.Limits
}

// workerHoldingStaleClaim recovers a claim written before the Actor update.
// It releases the claim if the Worker is no longer eligible.
func (w *ActorWorkflow) workerHoldingStaleClaim(ctx context.Context, actor *ateapi.Actor, constraints scheduling.Constraints, relaxable bool) (*ateapi.Worker, error) {
	actorUID := actor.GetMetadata().GetUid()
	workerName, err := w.store.FindWorkerHostingActor(ctx, actorUID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("while looking for a worker already hosting actor %q: %w", actorUID, err)
	}
	worker, err := w.store.GetWorker(ctx, workerName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil // the worker went away with its claim
		}
		return nil, fmt.Errorf("while reading worker %q holding a stale claim: %w", workerName, err)
	}

	// The Actor's allocation makes HasRoom unsuitable for an existing claim.
	if worker.GetNodeName() != "" && w.appliesWithFallback(worker, constraints, relaxable) {
		return worker, nil
	}

	_, err = w.store.ReleaseActorFromWorker(ctx, workerName, actorUID)
	if err != nil {
		return nil, fmt.Errorf("while releasing stale claim on worker %q: %w", workerName, err)
	}
	return nil, nil
}

// schedulerRecordable excludes the two errors the assignment loop retries
// transparently. Counting a retried attempt would inflate the error rate and
// double-count the eventual success; ensureWorkerAssigned records a loop that
// never succeeds.
func schedulerRecordable(err error) bool {
	return !errors.Is(err, store.ErrVersionConflict) && !errors.Is(err, errWorkerFilledUp)
}

// assignWorkerAttempt makes one attempt at claiming a worker for the actor
// and persisting RESUMING with the assignment. On a version conflict it
// re-reads the actor: if the fresh copy can still be resumed the refreshed
// actor is returned along with the conflict so the caller retries with clean
// inputs; any other status aborts the resume.
func (w *ActorWorkflow) assignWorkerAttempt(ctx context.Context, actorRef resources.ActorRef, actor *ateapi.Actor, actorTemplate *ateapi.ActorTemplate) (_ *ateapi.Actor, _ *ateapi.Worker, err error) {
	// Checked before a worker is claimed.
	if err := checkSnapshotUpload(actorRef, actor); err != nil {
		return nil, nil, err
	}
	start := time.Now()
	outcome := ateattr.SchedulerOutcomeError
	poolNamespace := ""
	pool := ""
	class := ""
	defer func() {
		if schedulerRecordable(err) {
			w.instruments.recordSchedulerAssignment(ctx, time.Since(start), outcome, poolNamespace, pool, class, err)
		}
	}()

	constraints := schedulingConstraints(actor, actorTemplate)
	snapRuntime := constraints.RequiredRuntime
	class = constraints.SandboxClass
	relaxable := canRelaxRuntime(actor, constraints)

	assignedWorker, err := w.workerHoldingStaleClaim(ctx, actor, constraints, relaxable)
	if err != nil {
		return nil, nil, err
	}
	if assignedWorker == nil {
		pickedWorker, err := w.scheduler.Schedule(ctx, constraints)
		if errors.Is(err, scheduling.ErrNoCapacity) && relaxable {
			constraints.RequiredRuntime = hardware.Requirement{}
			pickedWorker, err = w.scheduler.Schedule(ctx, constraints)
		}
		if err != nil {
			if errors.Is(err, scheduling.ErrNoCapacity) {
				outcome = ateattr.SchedulerOutcomeNoCapacity
				return nil, nil, apierror.ResourceExhausted("%w", errNoWorkerRoom)
			}
			return nil, nil, err
		}

		assignedWorker = pickedWorker
		slog.InfoContext(ctx, "Picked worker", slog.Any("worker", pickedWorker.String()))
	} else if relaxable && !snapRuntime.WorkerMatches(assignedWorker.GetStatus()) {
		// The stale claim only passed the relaxed check, so bind under it:
		// admit asks the same question again under the row lock.
		constraints.RequiredRuntime = hardware.Requirement{}
	}

	if assignedWorker.GetNodeName() == "" {
		return nil, nil, apierror.FailedPrecondition("worker %s has no node name", assignedWorker.GetMetadata().GetName())
	}

	assignment := &ateapi.ActorAssignment{
		Actor: &ateapi.ObjectRef{
			Atespace: actor.GetMetadata().GetAtespace(),
			Name:     actor.GetMetadata().GetName(),
		},
		ActorUid: actor.GetMetadata().GetUid(),
		// Record what this claim reserves so release returns the same amount.
		Resources: admittedResources(constraints),
	}

	// The candidate came from a watch-fed cache, so it may already be full or no
	// longer eligible. The store re-asks under the Worker's row lock, where the
	// answer holds until the bind commits, so nothing here needs a fresh read
	// and two claims for the last place cannot both be admitted.
	// The bind evaluates the fresh row; record the assignment from the same view.
	bound := assignedWorker
	admit := func(fresh *ateapi.Worker) error {
		if !w.scheduler.Applies(fresh, constraints) || !w.scheduler.HasRoom(fresh, constraints) {
			return errWorkerFilledUp
		}
		bound = fresh
		return nil
	}
	if err := w.store.BindActorToWorker(ctx, assignedWorker.GetMetadata().GetName(), assignment, admit); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			w.workerCache.Forget(assignedWorker.GetMetadata().GetName())
			return nil, nil, fmt.Errorf("selected worker disappeared before claim: %w", store.ErrVersionConflict)
		}
		// errWorkerFilledUp leaves the cache alone: it reports no room for
		// this actor's size, so forgetting the worker would hide it from
		// every smaller actor until the next watch event or relist.
		return nil, nil, err
	}

	newAssignment := workerAssignmentFrom(bound, snapRuntime)
	storedActor, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapi.Actor) error {
		toUpdate.Status.State = ateapi.ActorState_ACTOR_STATE_RESUMING
		toUpdate.Status.WorkerAssignment = newAssignment
		toUpdate.Status.AssignedNode = newAssignment.GetNodeName()
		return nil
	})
	if err != nil {
		if !errors.Is(err, store.ErrVersionConflict) {
			return nil, nil, err
		}
		// refresh the version of actor to avoid always failure in rest retries.
		fresh, gerr := w.store.GetActor(ctx, actorRef)
		if gerr != nil {
			slog.WarnContext(ctx, "Failed to refresh actor after assignment conflict", slog.Any("err", gerr))
			return nil, nil, err
		}
		switch fresh.GetStatus().GetState() {
		case ateapi.ActorState_ACTOR_STATE_SUSPENDED:
			slog.InfoContext(ctx, "Retrying assignment due to actor version conflict", slog.Any("actor", actorRef))
			return fresh, nil, err
		default:
			return nil, nil, apierror.Aborted("actor %s is %s and can no longer be resumed", actorRef, fresh.GetStatus().GetState())
		}
	}
	poolNamespace = bound.GetWorkerNamespace()
	pool = bound.GetWorkerPool()
	class = hardware.SandboxClass(bound.GetStatus())
	outcome = ateattr.SchedulerOutcomeAssigned
	logActorStateChanged(ctx, storedActor, ateattr.OperationResume)
	return storedActor, bound, nil
}

// errNoWorkerRoom reports that no worker has room for the Actor under its
// constraints.
var errNoWorkerRoom = errors.New("no worker has room for the actor")

// errWorkerFilledUp reports that the Worker the scheduler picked would not take
// the Actor once the store asked under its row lock. Retryable: the next
// attempt re-runs scheduling.
var errWorkerFilledUp = errors.New("picked worker no longer has room")

func workerAssignmentFrom(w *ateapi.Worker, snapRuntime hardware.Requirement) *ateapi.WorkerAssignment {
	return &ateapi.WorkerAssignment{
		Worker:          &ateapi.ObjectRef{Name: w.GetMetadata().GetName()},
		WorkerNamespace: w.GetWorkerNamespace(),
		WorkerPool:      w.GetWorkerPool(),
		WorkerPod:       w.GetWorkerPod(),
		WorkerPodUid:    w.GetWorkerPodUid(),
		WorkerPodIps:    slices.Clone(w.GetStatus().GetIps()),
		NodeName:        w.GetNodeName(),
		WorkerSpiffeId:  w.GetWorkerSpiffeId(),
		// The runtime the actor is started or restored under. Snapshots taken
		// during this assignment are stamped with it, so a worker that later
		// changes its default does not relabel state captured on the old one.
		SandboxRuntime: proto.CloneOf(snapRuntime.RuntimeFor(w.GetStatus())),
	}
}

// toAteletSandboxRuntime converts the control plane's ActorSandboxRuntime to
// atelet's, which it mirrors field for field.
func toAteletSandboxRuntime(in *ateapi.ActorSandboxRuntime) *ateletpb.ActorSandboxRuntime {
	if in == nil {
		return nil
	}
	out := &ateletpb.ActorSandboxRuntime{SandboxClass: in.GetSandboxClass()}
	if v := in.GetVersion(); v != nil {
		out.Version = &ateletpb.VersionedSandboxCompat{SchemaVersion: v.GetSchemaVersion()}
		for _, a := range v.GetAttributes() {
			out.Version.Attributes = append(out.Version.Attributes, &ateletpb.AttributeEntry{Key: a.GetKey(), Value: a.GetValue()})
		}
	}
	return out
}

func schedulingConstraints(actor *ateapi.Actor, tmpl *ateapi.ActorTemplate) scheduling.Constraints {
	snap := snapshotToResume(actor.GetStatus())
	var requiredNodes []string
	if node := actor.GetStatus().GetAssignedNode(); node != "" {
		requiredNodes = []string{node}
	}
	c := scheduling.Constraints{
		SandboxClass:    snap.GetSandboxRuntime().GetSandboxClass(),
		RequiredRuntime: hardware.NewRequirement(requiredSnapshotRuntime(snap, tmpl)),
		ActorSelector:   labels.SelectorFromSet(labels.Set(actor.GetWorkerSelector().GetMatchLabels())),
		RequiredNodes:   requiredNodes,
		Limits:          tmpl.GetResources().GetLimits(),
	}
	if sel := tmpl.GetWorkerSelector(); sel != nil {
		c.TemplateSelector = labels.SelectorFromSet(labels.Set(sel.GetMatchLabels()))
	}
	return c
}

// snapshotToResume returns the snapshot ResumeActor restores from: the latest
// one that is local or has a completed durable copy (a tag borrowed at
// creation included), or nil when the actor has none and cold-boots.
func snapshotToResume(status *ateapi.ActorStatus) *ateapi.Snapshot {
	return latestSnapshot(status, func(s *ateapi.Snapshot) bool {
		return s.GetLocality() != "" || s.GetDurableSnapshot().GetStatus() == ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
	})
}

// snapshotSandboxClass returns the sandbox class of snapshotToResume, or ""
// when the actor has none and cold-boots. Unlike requiredSnapshotRuntime it
// does not depend on fidelity: a worker of another class cannot read the
// snapshot's files even for a volumes-only restore.
func snapshotSandboxClass(actor *ateapi.Actor) string {
	return snapshotToResume(actor.GetStatus()).GetSandboxRuntime().GetSandboxClass()
}

// requiredSnapshotRuntime returns the ActorSandboxRuntime stamped on snap, the
// snapshotToResume, when the resume restores its memory state, or nil when it
// does not (cold boot, ROOTFS or VOLUMES fidelity, or replaced template).
func requiredSnapshotRuntime(snap *ateapi.Snapshot, tmpl *ateapi.ActorTemplate) *ateapi.ActorSandboxRuntime {
	if snap.GetFidelity() != ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY || templateReplaced(snap, tmpl) {
		return nil
	}
	return snap.GetSandboxRuntime()
}

// canRelaxRuntime reports whether scheduling may drop its runtime requirement.
func canRelaxRuntime(actor *ateapi.Actor, constraints scheduling.Constraints) bool {
	if constraints.RequiredRuntime.Runtime() == nil {
		return false
	}
	status := actor.GetStatus()
	// A pinned actor detaches first, so a compatible worker elsewhere can keep
	// its memory state.
	if status.GetState() == ateapi.ActorState_ACTOR_STATE_SUSPENDED && status.GetAssignedNode() != "" {
		return false
	}
	snap := snapshotToResume(status)
	if snap.GetLocality() == "" {
		return true
	}
	// Its node was deleted, so it restores from its upload, as a durable
	// snapshot does.
	if localElsewhere(snap, status.GetAssignedNode()) {
		return true
	}
	return snap.GetDurableSnapshot().GetStatus() == ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
}

// appliesWithFallback reports whether worker satisfies constraints, or — when
// relaxable — satisfies constraints with RequiredRuntime relaxed.
func (w *ActorWorkflow) appliesWithFallback(worker *ateapi.Worker, constraints scheduling.Constraints, relaxable bool) bool {
	relaxed := constraints
	relaxed.RequiredRuntime = hardware.Requirement{}
	if !w.scheduler.Applies(worker, relaxed) {
		return false
	}
	return relaxable || constraints.RequiredRuntime.WorkerMatches(worker.GetStatus())
}

// ensureVolumesAttached attaches the actor's mounted external volumes to the
// assigned worker's node and returns each driver's attachment metadata by
// volume name, for the Restore call that follows. Every volume must be
// provisioned with a storage volume ID; otherwise nothing is attached.
// Attachment is idempotent, so a re-entered workflow safely runs it again.
// TODO replace re-execution with a proper check on the volumes' attach state.
func (w *ActorWorkflow) ensureVolumesAttached(ctx context.Context, actor *ateapi.Actor, worker *ateapi.Worker, actorTemplate *ateapi.ActorTemplate) (_ map[string]map[string]string, err error) {
	ctx, done := stepSpan(ctx, "AttachVolumes")
	defer func() { err = done(err) }()

	if len(actor.GetStatus().GetExternalVolumes().GetVolumes()) > 0 && !preview.IsEnabled(preview.GateExternalVolumes) {
		return nil, fmt.Errorf("external volumes are not supported unless the ExternalVolumes preview gate is enabled")
	}

	node := worker.GetNodeName()
	if node == "" {
		return nil, fmt.Errorf("assigned worker has no node name")
	}

	ref := &ateapi.ObjectRef{Atespace: actor.GetMetadata().GetAtespace(), Name: actor.GetMetadata().GetName()}
	volumes := getMountedActorVolumes(ctx, ref, actor.GetStatus().GetExternalVolumes().GetVolumes(), actorTemplate)
	// Check every volume before attaching any, so a bad one cannot leave the
	// ones before it published.
	if err := checkVolumesAttachable(volumes); err != nil {
		return nil, err
	}
	volumePublishContexts := make(map[string]map[string]string)
	for _, vol := range volumes {
		slog.InfoContext(ctx, "Attaching volume to node", slog.String("volume_id", vol.GetStorageVolumeId()), slog.String("node", node))
		plugin, err := w.pluginRegistry.GetPlugin(ctx, vol.GetVolumeType())
		if err != nil {
			return nil, fmt.Errorf("failed to get volume plugin for %q: %w", vol.GetVolumeType(), err)
		}
		resp, err := plugin.AttachVolume(ctx, volume.AttachVolumeRequest{VolumeID: vol.GetStorageVolumeId(), Node: node})
		if err != nil {
			return nil, fmt.Errorf("failed to attach volume %q to node %q: %w", vol.GetStorageVolumeId(), node, err)
		}
		if len(resp.PublishContext) > 0 {
			volumePublishContexts[vol.GetName()] = resp.PublishContext
		}
	}
	return volumePublishContexts, nil
}

// checkVolumesAttachable returns FailedPrecondition unless every volume has a
// storage volume ID.
func checkVolumesAttachable(volumes []*ateapi.ExternalVolumeStatus) error {
	for _, vol := range volumes {
		if vol.GetStorageVolumeId() == "" {
			return apierror.FailedPrecondition("volume %q has no storage volume ID to attach", vol.GetName())
		}
	}
	return nil
}

// ensureAteletRestored brings the workload up on the assigned worker:
// restoring snapshotToResume, or cold-booting from the template spec when the
// actor has no snapshot. This is the atelet reentrancy seam (#372): the
// request is keyed by the actor UID and
// the worker pod UID, so a re-entered workflow re-sends the same semantic
// request; once atelet's Restore/Run are idempotent on those keys this step
// becomes fully reentrant with no changes here.
func (w *ActorWorkflow) ensureAteletRestored(ctx context.Context, actorRef resources.ActorRef, actor *ateapi.Actor, actorTemplate *ateapi.ActorTemplate, volumePublishContexts map[string]map[string]string) (tele restoreTelemetry, workerEpoch int64, err error) {
	ctx, done := stepSpan(ctx, "CallAteletRestore")
	defer func() { err = done(err) }()

	// A re-entered resume skips assignWorkerAttempt, which checks this too.
	if err := checkSnapshotUpload(actorRef, actor); err != nil {
		return tele, 0, err
	}

	assignment := actor.GetStatus().GetWorkerAssignment()
	ateletConn, err := w.dialer.DialForAteletOnNode(assignment.GetNodeName())
	if err != nil {
		return tele, 0, err
	}
	client := ateletpb.NewAteletClient(ateletConn)

	workloadSpec, err := workloadSpecFromActorTemplate(actorTemplate, actor, volumePublishContexts)
	if err != nil {
		return tele, 0, err
	}
	egressGateway := w.egressGateway()

	// The actor's declared limits ride the RPC down to the sandbox so it is sized
	// to the actor (replacing the worker-pod downward-API approach).
	actorResources := toAteletResources(actorTemplate.GetResources())

	snap := snapshotToResume(actor.GetStatus())

	// The assignment is what the next checkpoint is stamped from, so it is also
	// what decides how this restore runs.
	sandboxRuntime := toAteletSandboxRuntime(assignment.GetSandboxRuntime())

	// New actor without any existing snapshots to restore from.
	if snap == nil {
		slog.InfoContext(ctx, "Actor has no snapshot; Booting from ActorTemplate spec")
		tele.SnapshotKind = ateattr.SnapshotKindBoot

		req := &ateletpb.RunRequest{
			WorkerPodUid:          assignment.GetWorkerPodUid(),
			Atespace:              actor.GetMetadata().GetAtespace(),
			ActorName:             actor.GetMetadata().GetName(),
			ActorTemplateAtespace: actor.GetActorTemplate().GetAtespace(),
			ActorTemplateName:     actor.GetActorTemplate().GetName(),
			SandboxRuntime:        sandboxRuntime,
			Spec:                  workloadSpec,
			ActorUid:              actor.GetMetadata().Uid,
			EgressGateway:         egressGateway,
			Resources:             actorResources,
		}
		resp, err := client.Run(ctx, req)
		if err != nil {
			return tele, 0, handleAteletError(ctx, w.store, actorRef, ateattr.OperationResume, "Run", false, err)
		}
		return tele, resp.GetWorkerEpoch(), nil
	}

	slog.InfoContext(ctx, "Actor has snapshot; Restoring from snapshot", slog.String("snapshot_uuid", snap.GetUuid()))
	tele.SnapshotKind = ateattr.SnapshotKindLatest
	if snap.GetLocality() != "" && snap.GetLocality() == assignment.GetNodeName() {
		tele.SnapshotKind = ateattr.SnapshotKindLocal
	}

	// Exclusive by construction: requiredSnapshotRuntime returns nil when the
	// template changed, so runtimeIncompatible is false when templateReplaced
	// is true.
	requiredRuntime := requiredSnapshotRuntime(snap, actorTemplate)
	runtimeIncompatible := requiredRuntime != nil && !hardware.Matches(assignment.GetSandboxRuntime(), requiredRuntime)

	fidelity := fidelityToAtelet(snap.GetFidelity())
	switch {
	case templateReplaced(snap, actorTemplate):
		fidelity = ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
	case runtimeIncompatible:
		logRuntimeFallback(ctx, actorRef, assignment, requiredRuntime)
		fidelity = ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
	}
	tele.WireFidelity = ateattr.SnapshotFidelityValue(fidelity)
	req := &ateletpb.RestoreRequest{
		WorkerPodUid:          assignment.GetWorkerPodUid(),
		Atespace:              actor.GetMetadata().GetAtespace(),
		ActorName:             actor.GetMetadata().GetName(),
		ActorTemplateAtespace: actor.GetActorTemplate().GetAtespace(),
		ActorTemplateName:     actor.GetActorTemplate().GetName(),
		Spec:                  workloadSpec,
		Snapshot: &ateletpb.Snapshot{
			SnapshotUuid: snap.GetUuid(),
			Generation:   snap.GetGeneration(),
			Fidelity:     fidelity,
			// A local snapshot's upload is named even while PENDING: atelet
			// may have uploaded the snapshot and detached the actor before
			// its report of the upload lands. atelet reads it only when it
			// holds no usable local copy, and the manifest, uploaded last,
			// tells a complete upload from a partial one.
			Object: &ateletpb.ObjectStorage{
				SnapshotUri: snap.GetDurableSnapshot().GetObject().GetSnapshotUri(),
				Sharing:     snapshotSharing(snap.GetOwner()),
			},
		},
		SandboxRuntime: sandboxRuntime,
		ActorUid:       actor.GetMetadata().Uid,
		EgressGateway:  egressGateway,
		Resources:      actorResources,
	}
	resp, err := client.Restore(ctx, req)
	if err != nil {
		// The snapshot's node is gone, and atelet found no upload of it.
		if ctx.Err() == nil && status.Code(err) == codes.NotFound && localElsewhere(snap, assignment.GetNodeName()) {
			slog.LogAttrs(ctx, slog.LevelError, "Setting Actor to crashed due to Atelet error",
				append(ateattr.ActorRefLogAttrs(actorRef), slog.Any("err", err))...)
			message := fmt.Sprintf("%s: %s", crashMessageLocalSnapshotNodeGone, ateletCrashMessage("Restore", err))
			return tele, 0, crashActorAndFail(ctx, w.store, actorRef, ateattr.OperationResume, message)
		}
		return tele, 0, handleAteletError(ctx, w.store, actorRef, ateattr.OperationResume, "Restore", false, err)
	}
	return tele, resp.GetWorkerEpoch(), nil
}

// logRuntimeFallback records a resume that restores without memory state
// because the assigned worker does not run the runtime the snapshot's memory
// state was captured under.
func logRuntimeFallback(ctx context.Context, actorRef resources.ActorRef, assignment *ateapi.WorkerAssignment, required *ateapi.ActorSandboxRuntime) {
	slog.WarnContext(ctx, "Assigned worker cannot restore the snapshot's runtime; restoring without memory state",
		slog.String("actor", actorRef.String()),
		slog.String("worker", assignment.GetWorkerPod()),
		slog.Any("required_runtime", required),
		slog.Any("assigned_runtime", assignment.GetSandboxRuntime()))
}

func (w *ActorWorkflow) egressGateway() *ateletpb.EgressGateway {
	if w.egressGatewayAddress == "" {
		return nil
	}
	return &ateletpb.EgressGateway{Address: w.egressGatewayAddress}
}

// finalizeRunning re-reads the actor for a fresh version and commits RUNNING,
// stamped with workerEpoch, the epoch of the worker process that ran it.
//
// The worker process registered that epoch, with its pod IPs, before it took
// the workload. If it is still the Worker's registered epoch, the Actor is
// stamped with it and with those IPs, which a resume bound before the process
// started did not carry. If a later epoch is registered, that process has exited and taken
// the sandbox with it, so RUNNING is not committed: the error is Unavailable,
// and the retried resume restores the Actor on the current process.
func (w *ActorWorkflow) finalizeRunning(ctx context.Context, actorRef resources.ActorRef, workerEpoch int64) (_ *ateapi.Actor, err error) {
	ctx, done := stepSpan(ctx, "FinalizeRunning")
	defer func() { err = done(err) }()

	latestActor, err := w.store.GetActor(ctx, actorRef)
	if err != nil {
		return nil, err
	}

	workerName := latestActor.GetStatus().GetWorkerAssignment().GetWorker().GetName()
	worker, err := w.store.GetWorker(ctx, workerName)
	if errors.Is(err, store.ErrNotFound) {
		// Deleting the Worker crashes the Actors bound to it.
		return nil, apierror.Aborted("worker %s of actor %s is gone", workerName, actorRef)
	}
	if err != nil {
		return nil, fmt.Errorf("while getting worker %s: %w", workerName, err)
	}
	switch registered := worker.GetStatus().GetRegisteredEpoch(); {
	case workerEpoch < registered:
		slog.WarnContext(ctx, "Worker process that ran the actor has been replaced; restoring it again",
			slog.String("worker", workerName),
			slog.Int64("worker_epoch", workerEpoch),
			slog.Int64("registered_epoch", registered))
		return nil, apierror.Unavailable("worker %s restarted while actor %s was being restored, please retry", workerName, actorRef)
	case workerEpoch > registered:
		return nil, apierror.Internal("worker %s reported epoch %d, above its registered epoch %d", workerName, workerEpoch, registered)
	}

	storedActor, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(latestActor), func(toUpdate *ateapi.Actor) error {
		toUpdate.Status.State = ateapi.ActorState_ACTOR_STATE_RUNNING
		toUpdate.Status.WorkerAssignment.WorkerEpoch = workerEpoch
		toUpdate.Status.WorkerAssignment.WorkerPodIps = slices.Clone(worker.GetStatus().GetIps())
		// The workload already runs, so a record that can't take the upload
		// doesn't hold the actor in RESUMING. The next suspend prunes the entry.
		if err := recordRestoredUpload(ctx, toUpdate.Status); err != nil {
			slog.WarnContext(ctx, "Restored a snapshot from its upload but could not record the upload",
				slog.Any("actor", actorRef), slog.Any("err", err))
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		return nil, err
	}
	logActorStateChanged(ctx, storedActor, ateattr.OperationResume)
	return storedActor, nil
}

// recordRestoredUpload records the upload of a snapshot that resume restored
// on another node after its own node was deleted. The restore proved the
// upload complete, and the deleted node no longer holds a copy. On an error
// it leaves status unchanged.
func recordRestoredUpload(ctx context.Context, status *ateapi.ActorStatus) error {
	snap := snapshotToResume(status)
	if !localElsewhere(snap, status.GetAssignedNode()) {
		return nil
	}
	node := snap.GetLocality()
	if err := applyLatestDurable(ctx, status, &ateapi.SnapshotReport{
		Uuid:       snap.GetUuid(),
		Generation: snap.GetGeneration(),
		Fidelity:   snap.GetFidelity(),
		Object:     snap.GetDurableSnapshot().GetObject(),
	}); err != nil {
		return err
	}
	return applyDetach(status, node, snap.GetGeneration())
}
