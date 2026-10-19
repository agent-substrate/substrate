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
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// suspendMode selects which of the two suspends the suspend workflow runs.
// Both take the same steps; the mode only decides the shape of the snapshot
// they take.
type suspendMode int

const (
	// suspendSync makes the snapshot durable before the workflow returns:
	// the actor ends SUSPENDED holding a durable snapshot.
	suspendSync suspendMode = iota
	// suspendAsync leaves the snapshot on the worker's node: the actor ends
	// SUSPENDED holding a node-local snapshot whose durable copy is still
	// pending, pinned to that node until atelet uploads it in the background
	// and the detach report unpins it.
	suspendAsync
)

// suspendModeFromOptions maps a SuspendActor request's options to the
// workflow mode. An unset mode runs an async suspend.
func suspendModeFromOptions(opts *ateapipb.SuspendOptions) suspendMode {
	if opts.GetMode() == ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS {
		return suspendSync
	}
	return suspendAsync
}

// uploadMode is when atelet makes the checkpoint durable.
func (m suspendMode) uploadMode() ateletpb.UploadMode {
	if m == suspendAsync {
		return ateletpb.UploadMode_UPLOAD_MODE_DEFERRED
	}
	return ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE
}

// SuspendActor executes the workflow to suspend an actor. A running actor is
// checkpointed on its worker: a sync suspend uploads the snapshot before
// returning, an async suspend keeps it on that node. A SUSPENDED actor is
// returned as it is, whatever the mode. Idempotent: a re-entered workflow
// fast-forwards past the steps a previous attempt completed, deriving
// progress from the persisted actor alone.
func (w *ActorWorkflow) SuspendActor(ctx context.Context, actorRef resources.ActorRef, mode suspendMode) (_ *ateapipb.Actor, err error) {
	ctx = preview.ContextWithOptIn(ctx)
	start := time.Now()
	var actor *ateapipb.Actor
	var actorTemplate *ateapipb.ActorTemplate
	var wireFidelity string
	// ateletState is the state atelet returned from Checkpoint, which
	// finalize applies.
	var ateletState *ateletpb.ActorSnapshotState
	// Set just before finalize; nil until then, so earlier exits label
	// themselves from the record they hold.
	var finalAttrs []attribute.KeyValue

	defer func() {
		attrs := finalAttrs
		if attrs == nil {
			attrs = lifecycleOpAttrs(actor, "", wireFidelity)
		}
		w.instruments.recordLifecycleOp(ctx, ateattr.OperationSuspend, start, err, attrs...)
	}()

	leaseCtx, lease, err := w.acquireActorLease(ctx, actorRef)
	if err != nil {
		return nil, err
	}
	defer lease.Close()

	actor, actorTemplate, err = w.loadActorForSuspend(leaseCtx, actorRef)
	if err != nil {
		return nil, err
	}
	// A SUSPENDED actor has nothing left to do, whatever the mode and wherever
	// its snapshot is: atelet uploads a node-local snapshot in the background.
	// These successes report no pool, and cannot: the previous attempt
	// released the worker, so the record names none (#957).
	if actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		return actor, nil
	}
	var marked *ateapipb.Actor
	if marked, err = w.ensureMarkedSuspending(leaseCtx, actorRef, actor, actorTemplate, mode); err != nil {
		return nil, err
	}
	actor = marked
	wireFidelity, ateletState, err = w.ensureAteletSuspended(leaseCtx, actorRef, actor, actorTemplate, mode)
	if err != nil {
		return nil, err
	}
	// TODO(#2734): An async suspend should skip detaching here, since the actor can resume on the same node.
	if err = w.ensureVolumesDetached(leaseCtx, actor, actorTemplate, "DetachVolumes", ateattr.OperationSuspend); err != nil {
		return nil, err
	}
	// FinalizeSuspended clears the WorkerAssignment the labels read, so snapshot
	// them here, as crash.go does for the crash counter. The fidelity label is
	// the one requested.
	finalAttrs = lifecycleOpAttrs(actor, "", wireFidelity)
	var finalized *ateapipb.Actor
	if finalized, err = w.ensureSuspendedFinalized(leaseCtx, actorRef, mode, ateletState); err != nil {
		return nil, err
	}
	actor = finalized
	return actor, nil
}

// loadActorForSuspend fetches the current actor record and its template.
func (w *ActorWorkflow) loadActorForSuspend(ctx context.Context, actorRef resources.ActorRef) (_ *ateapipb.Actor, _ *ateapipb.ActorTemplate, err error) {
	ctx, done := stepSpan(ctx, "LoadActorForSuspend")
	defer func() { err = done(err) }()

	actor, err := w.store.GetActor(ctx, actorRef)
	if err != nil {
		return nil, nil, err
	}
	actorTemplate, err := resolveActorTemplate(ctx, w.store, actor)
	if err != nil {
		return nil, nil, err
	}
	return actor, actorTemplate, nil
}

// ensureMarkedSuspending transitions a RUNNING actor to SUSPENDING, minting
// the in-progress snapshot and recording the actor version the snapshot will
// capture. Either mode mints a snapshot with a pending durable copy. Skips
// when a previous attempt already marked the actor, in either mode; the
// persisted snapshot and source version then stay authoritative for the rest
// of the workflow.
func (w *ActorWorkflow) ensureMarkedSuspending(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor, actorTemplate *ateapipb.ActorTemplate, mode suspendMode) (_ *ateapipb.Actor, err error) {
	ctx, done := stepSpan(ctx, "MarkSuspending")
	defer func() { err = done(err) }()

	state := actor.GetStatus().GetState()
	if state != ateapipb.ActorState_ACTOR_STATE_RUNNING && state != ateapipb.ActorState_ACTOR_STATE_SUSPENDING {
		return nil, apierror.FailedPrecondition("MarkSuspending prerequisite not met for Actor: %s (got: %v, want %s)", actorRef, state, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	}
	if state == ateapipb.ActorState_ACTOR_STATE_SUSPENDING {
		markSkipped(ctx, "actor already SUSPENDING")
		return actor, nil
	}
	snapshotName := resources.NewSnapshotName()

	// Fail here rather than at checkpoint time if the template's location
	// cannot produce a usable URI: nothing has been written yet.
	uri, err := w.newInProgressSnapshotURI(actorTemplate, actor, snapshotName)
	if err != nil {
		return nil, err
	}
	storedActor, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDING
		// Increment last_assigned_generation for the new Suspend request and create
		// a snapshot for that generation with a pending durable copy. Both modes
		// record where the snapshot is uploaded, so the mode can change on a retry.
		gen := toUpdate.Status.LastAssignedGeneration + 1
		toUpdate.Status.LastAssignedGeneration = gen

		snap := newDurableSnapshot(
			gen,
			ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR,
			actorTemplate.GetSnapshotConfig().GetPreferredFidelity(),
			actorTemplate.GetMetadata().GetUid(),
			uri.Name(),
			uri.String(),
			ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING,
		)
		snap.SandboxRuntime = proto.CloneOf(toUpdate.GetStatus().GetWorkerAssignment().GetSandboxRuntime())
		toUpdate.Status.Snapshots = append(toUpdate.Status.Snapshots, snap)
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		return nil, err
	}
	logActorStateChanged(ctx, storedActor, ateattr.OperationSuspend)
	return storedActor, nil
}

// ensureAteletSuspended checkpoints the workload to the actor's persisted
// in-progress snapshot location. A sync suspend has atelet upload it before
// returning; an async suspend has atelet keep it on the node. It returns the
// snapshot state atelet reports once the checkpoint is taken, which finalize
// applies.
// A re-entered workflow re-sends the same snapshot, which
// ensureMarkedSuspending minted once, and atelet answers a Checkpoint of a
// snapshot it already committed without checkpointing again, so a retry after
// a lost response succeeds.
func (w *ActorWorkflow) ensureAteletSuspended(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor, actorTemplate *ateapipb.ActorTemplate, mode suspendMode) (wireFidelity string, state *ateletpb.ActorSnapshotState, err error) {
	ctx, done := stepSpan(ctx, "CallAteletSuspend")
	defer func() { err = done(err) }()

	assignment := actor.GetStatus().GetWorkerAssignment()
	if assignment == nil {
		// Missing active worker pod reference in SUSPENDING state indicates corrupted store state.
		return "", nil, crashActorAndFail(ctx, w.store, actorRef, ateattr.OperationSuspend, crashMessageWorkerAssignmentMissing)
	}

	ateletConn, err := w.dialer.DialForAteletOnNode(assignment.GetNodeName())
	if err != nil {
		return "", nil, fmt.Errorf("while getting atelet conn for node %q: %w", assignment.GetNodeName(), err)
	}
	client := ateletpb.NewAteletClient(ateletConn)

	workloadSpec, err := workloadSpecFromActorTemplate(actorTemplate, actor, nil)
	if err != nil {
		return "", nil, err
	}

	// Checkpoint does not carry the sandbox config: atelet uses the version the
	// actor is currently running (recorded on-node at Run/Restore) and pins it
	// into the snapshot manifest.
	inProgressSnap := snapshotAtLatestGeneration(actor.GetStatus())
	fidelity := fidelityToAtelet(actorTemplate.GetSnapshotConfig().GetPreferredFidelity())
	req := &ateletpb.CheckpointRequest{
		WorkerPodUid:          assignment.GetWorkerPodUid(),
		Atespace:              actor.GetMetadata().GetAtespace(),
		ActorName:             actor.GetMetadata().GetName(),
		ActorTemplateAtespace: actor.GetActorTemplate().GetAtespace(),
		ActorTemplateName:     actor.GetActorTemplate().GetName(),
		Spec:                  workloadSpec,
		Snapshot: &ateletpb.Snapshot{
			SnapshotUuid: inProgressSnap.GetUuid(),
			Generation:   inProgressSnap.GetGeneration(),
			Fidelity:     fidelity,
			Object: &ateletpb.ObjectStorage{
				SnapshotUri: inProgressSnap.GetDurableSnapshot().GetObject().GetSnapshotUri(),
			},
		},
		UploadConfig: &ateletpb.UploadConfig{
			Mode: mode.uploadMode(),
		},
		ActorUid: actor.GetMetadata().Uid,
	}
	wireFidelity = ateattr.SnapshotFidelityValue(fidelity)

	resp, err := client.Checkpoint(ctx, req)
	if err != nil {
		// A snapshot conflict leaves the actor and its assignment intact for a retry.
		if ctx.Err() == nil && status.Code(err) == codes.Aborted {
			return wireFidelity, nil, apierror.Aborted("while calling atelet Checkpoint: %w", err)
		}
		return wireFidelity, nil, handleAteletError(ctx, w.store, actorRef, ateattr.OperationSuspend, "Checkpoint", false, err)
	}
	return wireFidelity, resp.GetState(), nil
}

// newInProgressSnapshotURI is where the snapshot an actor is currently taking is
// written: under the actor's own prefix, so the objects name their owner.
func (w *ActorWorkflow) newInProgressSnapshotURI(actorTemplate *ateapipb.ActorTemplate, actor *ateapipb.Actor, snapshotName string) (resources.SnapshotURI, error) {
	location, err := w.snapshotLocation(actorTemplate)
	if err != nil {
		return resources.SnapshotURI{}, err
	}
	atespace := actor.GetMetadata().GetAtespace()
	uri, err := resources.NewActorSnapshotURI(location, atespace, actor.GetMetadata().GetUid(), snapshotName)
	if err != nil {
		return resources.SnapshotURI{}, fmt.Errorf("while building the snapshot URI for actor %s/%s: %w", atespace, actor.GetMetadata().GetName(), err)
	}
	return uri, nil
}

// ensureVolumesDetached detaches the actor's mounted external volumes from
// its worker node. Detachment is idempotent, so a re-entered workflow safely
// runs it again. spanName distinguishes the suspend and revert steps in
// traces; op labels the volume metrics.
// TODO replace re-execution with a proper check on the volumes' attach state.
func (w *ActorWorkflow) ensureVolumesDetached(ctx context.Context, actor *ateapipb.Actor, actorTemplate *ateapipb.ActorTemplate, spanName, op string) (err error) {
	ctx, done := stepSpan(ctx, spanName)
	defer func() { err = done(err) }()

	// Given that external volumes has been in the codebase for a while and
	// its preview-ness is solely around the API, and that the impact of
	// disabling the gate while in use would be an orphaned volume
	// attachment, we are NOT checking the ExternalVolumes gate in the
	// delete/suspend path.
	return detachActorVolumes(ctx, w.pluginRegistry, actor, actorTemplate, op)
}

// ensureSuspendedFinalized releases the actor's worker (only when it is still
// owned by this actor), records the in-progress snapshot as the actor's
// durable snapshot (sync, or async when atelet keeps only the durable copy) or
// node-local snapshot with its PENDING durable copy (async), and commits
// SUSPENDED with the assignment cleared in a single update, then releases the
// durable snapshot that update replaced.
// When the latest snapshot cannot be recorded, it commits CRASHED instead and
// fails the step.
// It re-reads the actor first so an out-of-band transition (e.g. the syncer
// crashing the actor after its worker died) is not overwritten: an actor that
// is no longer SUSPENDING is left as it is and the step fails with
// FailedPrecondition.
// ateletState is the state atelet returned from Checkpoint, applied in the
// same update. A state that conflicts with the record crashes the actor.
func (w *ActorWorkflow) ensureSuspendedFinalized(ctx context.Context, actorRef resources.ActorRef, mode suspendMode, ateletState *ateletpb.ActorSnapshotState) (_ *ateapipb.Actor, err error) {
	ctx, done := stepSpan(ctx, "FinalizeSuspended")
	defer func() { err = done(err) }()

	// The step is a chain of store round-trips with no spans of its own, so
	// log a per-call breakdown to attribute its latency. Deferred so a call
	// that stalls and then fails still reports where the time went; steps not
	// reached (or skipped) log zero.
	start := time.Now()
	var dGetActor, dReleaseWorker, dUpdateActor, dReleaseSnapshot time.Duration
	defer func() {
		slog.InfoContext(ctx, "FinalizeSuspended store call durations",
			slog.Any("actor", actorRef),
			slog.Duration("total", time.Since(start)),
			slog.Duration("get_actor", dGetActor),
			slog.Duration("release_worker", dReleaseWorker),
			slog.Duration("update_actor", dUpdateActor),
			slog.Duration("release_snapshot", dReleaseSnapshot))
	}()

	t := time.Now()
	latestActor, err := w.store.GetActor(ctx, actorRef)
	dGetActor = time.Since(t)
	if err != nil {
		return nil, err
	}
	if state := latestActor.GetStatus().GetState(); state != ateapipb.ActorState_ACTOR_STATE_SUSPENDING {
		return nil, apierror.FailedPrecondition("FinalizeSuspended prerequisite not met for Actor: %s (got: %v, want %s)", actorRef, state, ateapipb.ActorState_ACTOR_STATE_SUSPENDING)
	}
	if mode == suspendAsync {
		// A local snapshot on an unknown node can never be found by the scheduler.
		if latestActor.GetStatus().GetAssignedNode() == "" {
			return nil, crashActorAndFail(ctx, w.store, actorRef, ateattr.OperationSuspend, crashMessageLocalSnapshotNodeUnknown)
		}
		if snapshotAtLatestGeneration(latestActor.GetStatus()) == nil {
			return nil, crashActorAndFail(ctx, w.store, actorRef, ateattr.OperationSuspend, crashMessageLocalSnapshotMissing)
		}
	}
	// Both modes mark the durable copy PENDING up front. Without it a sync
	// suspend has nothing to mark COMPLETED, and atelet's background upload of
	// a pause's snapshot could never be recorded.
	if snapshotAtLatestGeneration(latestActor.GetStatus()).GetDurableSnapshot() == nil {
		return nil, crashActorAndFail(ctx, w.store, actorRef, ateattr.OperationSuspend, crashMessageDurableSnapshotMissing)
	}

	// A state that conflicts with the record crashes the actor. Atelet has
	// already destroyed the workload, so a retried Checkpoint can't succeed.
	applied := proto.CloneOf(latestActor.GetStatus())
	if err := applyAteletResponseState(ctx, applied, ateletState); err != nil {
		slog.WarnContext(ctx, "Snapshot state atelet returned conflicts with the record",
			slog.Any("actor", actorRef),
			slog.Any("err", err))
		return nil, crashActorAndFail(ctx, w.store, actorRef, ateattr.OperationSuspend, crashMessageSnapshotStateConflict)
	}

	// 1. Free the worker (if it hasn't been freed yet)
	if latestActor.GetStatus().GetWorkerAssignment() != nil {
		t = time.Now()
		_, err := releaseWorker(ctx, w.store, latestActor)
		dReleaseWorker = time.Since(t)
		if err != nil {
			return nil, err
		}
	}

	// 2. Finalize the actor: record its new snapshot and mark it SUSPENDED. This
	// must run even with no worker assignment (nothing to free), or the actor
	// would be left SUSPENDING forever with the workflow reporting success.
	// The precondition pins the version checked above, so an out-of-band write
	// since then fails the update instead of being overwritten.
	t = time.Now()
	storedActor, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(latestActor), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
		// The precondition makes toUpdate the version applied was built from.
		toUpdate.Status.Snapshots = applied.GetSnapshots()
		snap := snapshotAtLatestGeneration(toUpdate.Status)
		if mode == suspendSync {
			snap.GetDurableSnapshot().Status = ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
		}
		durable := snap.GetDurableSnapshot().GetStatus() == ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
		// Keep the actor on the node that holds its snapshot. atelet holds an
		// async suspend's snapshot until it detaches it, which it does only
		// once the snapshot is durable.
		if ateletKeptLocal(ateletState, snap) || !durable {
			snap.Locality = toUpdate.GetStatus().GetAssignedNode()
		} else {
			toUpdate.Status.AssignedNode = ""
		}
		pruneSnapshots(toUpdate.Status, snap)
		toUpdate.Status.WorkerAssignment = nil
		return nil
	})
	dUpdateActor = time.Since(t)
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		return nil, err
	}
	logActorStateChanged(ctx, storedActor, ateattr.OperationSuspend)

	// 3. Release the external snapshots this suspend replaced. Best-effort: the
	// new snapshot is committed and the worker is gone, so failing the
	// workflow here would only strand the actor.
	t = time.Now()
	w.releaseDroppedSnapshots(ctx, latestActor, storedActor)
	dReleaseSnapshot = time.Since(t)
	return storedActor, nil
}

// ateletKeptLocal reports whether atelet retained this checkpoint.
func ateletKeptLocal(state *ateletpb.ActorSnapshotState, snap *ateapipb.Snapshot) bool {
	local := state.GetLatestLocal().GetSnapshot()
	uri := snap.GetDurableSnapshot().GetObject().GetSnapshotUri()
	return local != nil && uri != "" &&
		local.GetSnapshotUuid() == snap.GetUuid() &&
		local.GetGeneration() == snap.GetGeneration() &&
		local.GetObject().GetSnapshotUri() == uri
}

// releaseDroppedSnapshots releases each external snapshot that before records
// as COMPLETED and after doesn't. A failure is only logged, and leaves the
// snapshot's objects in storage until the actor is deleted, which removes its
// whole prefix. An upload atelet reports can make a newer snapshot COMPLETED
// outside any workflow, so the next one can drop more than one.
func (w *ActorWorkflow) releaseDroppedSnapshots(ctx context.Context, before, after *ateapipb.Actor) {
	completed := ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
	kept := map[string]bool{}
	for _, snap := range after.GetStatus().GetSnapshots() {
		kept[snap.GetDurableSnapshot().GetObject().GetSnapshotUri()] = true
	}
	for _, snap := range before.GetStatus().GetSnapshots() {
		dropped := snap.GetDurableSnapshot().GetObject().GetSnapshotUri()
		if snap.GetDurableSnapshot().GetStatus() != completed || kept[dropped] {
			continue
		}
		if err := w.releaseReplacedSnapshot(ctx, before, dropped); err != nil {
			slog.WarnContext(ctx, "Failed to release an external snapshot the actor no longer records; its objects are left in storage until the actor is deleted",
				slog.Any("actor", resources.ActorRefFromActor(before)),
				slog.String("snapshot_uri", dropped),
				slog.String("err", err.Error()))
		}
	}
}

// releaseReplacedSnapshot releases previous, the URI of an external snapshot
// the actor held before a suspend or revert dropped it. actor is the record as
// it was before that workflow committed.
// An actor that borrowed its current snapshot from a tag releases nothing —
// the snapshot lives under the tag's prefix, and the tag outlives the actor.
func (w *ActorWorkflow) releaseReplacedSnapshot(ctx context.Context, actor *ateapipb.Actor, previous string) (err error) {
	ctx, done := stepSpan(ctx, "ReleaseReplacedSnapshot")
	defer func() { err = done(err) }()

	switch {
	case w.objectStorePlugin == nil:
		markSkipped(ctx, "no object store configured")
		return nil
	case previous == "":
		markSkipped(ctx, "the actor held no external snapshot")
		return nil
	}
	uri, err := resources.ParseSnapshotURI(previous)
	if err != nil {
		return fmt.Errorf("while parsing the replaced external snapshot %q: %w", previous, err)
	}
	if !uri.OwnedBy(actorSnapshotOwner(actor)) {
		markSkipped(ctx, "the replaced external snapshot is owned by another resource")
		return nil
	}
	return w.cleanupSnapshot(ctx, uri.Prefix())
}

func actorSnapshotOwner(actor *ateapipb.Actor) resources.SnapshotOwner {
	return resources.ActorSnapshotOwner(actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetUid())
}
