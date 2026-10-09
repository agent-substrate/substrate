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
	"go.opentelemetry.io/otel/attribute"
)

// SuspendActor executes the workflow to suspend a running or paused actor:
// a running actor is checkpointed on its worker, a paused actor's node-local
// snapshot is uploaded. Idempotent: a re-entered workflow fast-forwards past
// the steps a previous attempt completed, deriving progress from the
// persisted actor alone.
func (w *ActorWorkflow) SuspendActor(ctx context.Context, actorRef resources.ActorRef) (_ *ateapipb.Actor, err error) {
	start := time.Now()
	var actor *ateapipb.Actor
	var actorTemplate *ateapipb.ActorTemplate
	var wireFidelity string
	// Set just before finalize; nil until then, so earlier exits label
	// themselves from the record they hold.
	var finalAttrs []attribute.KeyValue

	defer func() {
		attrs := finalAttrs
		if attrs == nil {
			attrs = lifecycleOpAttrs(actor, actorTemplate, "", wireFidelity)
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
	if actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		// Fully suspended already: FinalizeSuspended commits SUSPENDED and the
		// cleared worker assignment in a single update, so there is nothing
		// left to do. This success reports no pool, and cannot: the previous
		// attempt released the worker, so the record names none (#957).
		return actor, nil
	}
	// Decided before marking: once SUSPENDING is committed, the loaded status
	// alone can no longer tell the two origins apart.
	fromPaused := isPausedOriginSuspend(actor)
	var marked *ateapipb.Actor
	if marked, err = w.ensureMarkedSuspending(leaseCtx, actorRef, actor, actorTemplate); err != nil {
		return nil, err
	}
	actor = marked
	if fromPaused {
		wireFidelity, err = w.ensurePausedSnapshotUploaded(leaseCtx, actorRef, actor, actorTemplate)
	} else {
		wireFidelity, err = w.ensureAteletSuspended(leaseCtx, actorRef, actor, actorTemplate)
	}
	if err != nil {
		return nil, err
	}
	if err = w.ensureVolumesDetached(leaseCtx, actor, actorTemplate, "DetachVolumes", ateattr.OperationSuspend); err != nil {
		return nil, err
	}
	// FinalizeSuspended clears the WorkerAssignment the labels read, so snapshot
	// them here, as crash.go does for the crash counter.
	finalAttrs = lifecycleOpAttrs(actor, actorTemplate, "", wireFidelity)
	var finalized *ateapipb.Actor
	if finalized, err = w.ensureSuspendedFinalized(leaseCtx, actorRef); err != nil {
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

// ensureMarkedSuspending transitions a RUNNING or PAUSED actor to
// SUSPENDING, minting the in-progress snapshot location and recording the
// actor version the snapshot will capture. Skips when a previous attempt
// already marked the actor; the persisted location and source version then
// stay authoritative for the rest of the workflow.
func (w *ActorWorkflow) ensureMarkedSuspending(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor, actorTemplate *ateapipb.ActorTemplate) (_ *ateapipb.Actor, err error) {
	ctx, done := stepSpan(ctx, "MarkSuspending")
	defer func() { err = done(err) }()

	if actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_SUSPENDING {
		markSkipped(ctx, "actor already SUSPENDING")
		return actor, nil
	}
	if got := actor.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_RUNNING && got != ateapipb.ActorState_ACTOR_STATE_PAUSED {
		return nil, apierror.FailedPrecondition("MarkSuspending prerequisite not met for Actor: %s (got: %v, want %s or %s)", actorRef, got, ateapipb.ActorState_ACTOR_STATE_RUNNING, ateapipb.ActorState_ACTOR_STATE_PAUSED)
	}
	if actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_PAUSED {
		localSnap, _ := findLatestSnapshotStorage(actor.GetStatus(), ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
		if localSnap == nil {
			if err := crashActor(ctx, w.store, actorRef, ateattr.OperationSuspend, crashMessageLocalSnapshotNodeUnknown); err != nil {
				slog.ErrorContext(ctx, "Failed to crash actor", slog.String("err", err.Error()))
			}
			return nil, fmt.Errorf("actor is CRASHED because it was in PAUSED state with no completed local snapshot")
		}
	}

	// Fail here rather than at checkpoint time if the template's location
	// cannot produce a usable URI: nothing has been written yet.
	uri, err := newInProgressSnapshotURI(actorTemplate, actor)
	if err != nil {
		return nil, err
	}
	storedActor, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapipb.Actor) error {
		wasPaused := toUpdate.Status.State == ateapipb.ActorState_ACTOR_STATE_PAUSED
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDING
		if wasPaused {
			// Add a new in progress durable snapshot to the current generation
			snap, _ := findLatestSnapshotStorage(toUpdate.Status, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
			if snap == nil {
				return fmt.Errorf("actor %s is PAUSED but has no completed local snapshot", actorRef)
			}
			snap.ActorTemplateUid = actorTemplate.GetMetadata().GetUid()
			durableInProgress := &ateapipb.SnapshotStorage{
				Durability: ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE,
				Status:     ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS,
				Fidelity:   preferredFidelity(actorRef.Atespace, actorTemplate),
				Object:     &ateapipb.ObjectSnapshot{SnapshotUri: uri.String()},
			}
			setSnapshotStorage(snap, durableInProgress)
			return nil
		}
		// Increment last_assigned_generation for the new Suspend request and create
		// an in-progress snapshot for that generation.
		gen := toUpdate.Status.LastAssignedGeneration + 1
		toUpdate.Status.LastAssignedGeneration = gen
		toUpdate.Status.Snapshots = append(toUpdate.Status.Snapshots,
			newDurableSnapshot(
				gen,
				ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR,
				preferredFidelity(actorRef.Atespace, actorTemplate),
				actorTemplate.GetMetadata().GetUid(),
				uri.String(),
				ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS,
			))
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

// preferredFidelity returns the fidelity a suspend snapshot is taken with.
// Golden actors always commit MEMORY regardless of the template's
// preferredFidelity: new actors borrow the golden snapshot and resume it
// with memory, so it must carry the guest memory and filesystem.
func preferredFidelity(atespace string, tmpl *ateapipb.ActorTemplate) ateapipb.SnapshotFidelity {
	if atespace == resources.GoldenActorAtespace {
		return ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	}
	return tmpl.GetSnapshotConfig().GetPreferredFidelity()
}

// isPausedOriginSuspend reports whether the suspend must upload a PAUSED
// actor's node-local snapshot instead of checkpointing a running workload.
// A LocalSnapshot alone does not mean paused-origin: resume never clears
// it, so a RUNNING actor resumed from pause still carries a stale one. The
// nil worker assignment disambiguates — running-origin suspends keep their
// assignment until finalize, paused actors never have one.
func isPausedOriginSuspend(actor *ateapipb.Actor) bool {
	if actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_PAUSED {
		return true
	}
	if actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_SUSPENDING &&
		actor.GetStatus().GetWorkerAssignment() == nil {
		_, localSt := findLatestSnapshotStorage(actor.GetStatus(), ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
		return localSt != nil
	}
	return false
}

// ensureAteletSuspended checkpoints the workload to the actor's persisted
// in-progress snapshot location. This is the atelet reentrancy seam (#372):
// the request is keyed by the actor UID, the worker pod UID, and the
// once-minted snapshot location, so a re-entered workflow re-sends the same
// semantic request; once atelet's Checkpoint is idempotent on those keys this
// step becomes fully reentrant with no changes here.
func (w *ActorWorkflow) ensureAteletSuspended(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor, actorTemplate *ateapipb.ActorTemplate) (wireFidelity string, err error) {
	ctx, done := stepSpan(ctx, "CallAteletSuspend")
	defer func() { err = done(err) }()

	assignment := actor.GetStatus().GetWorkerAssignment()
	if assignment == nil {
		// Missing active worker pod reference in SUSPENDING state indicates corrupted store state.
		if err := crashActor(ctx, w.store, actorRef, ateattr.OperationSuspend, crashMessageWorkerAssignmentMissing); err != nil {
			slog.ErrorContext(ctx, "Failed to crash actor", slog.String("err", err.Error()))
		}
		return "", fmt.Errorf("actor is CRASHED because it was in SUSPENDING state but has no active worker")
	}

	ateletConn, err := w.dialer.DialForAteletOnNode(assignment.GetNodeName())
	if err != nil {
		return "", fmt.Errorf("while getting atelet conn for node %q: %w", assignment.GetNodeName(), err)
	}
	client := ateletpb.NewAteomHerderClient(ateletConn)

	workloadSpec, err := workloadSpecFromActorTemplate(actorTemplate, actor, nil)
	if err != nil {
		return "", err
	}

	// Checkpoint does not carry the sandbox config: atelet uses the version the
	// actor is currently running (recorded on-node at Run/Restore) and pins it
	// into the snapshot manifest.
	_, inProgressDurable := findLatestSnapshotStorage(actor.GetStatus(), ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS)
	req := &ateletpb.CheckpointRequest{
		TargetAteomUid:        assignment.GetWorkerPodUid(),
		Atespace:              actor.GetMetadata().GetAtespace(),
		ActorName:             actor.GetMetadata().GetName(),
		ActorTemplateAtespace: actor.GetActorTemplate().GetAtespace(),
		ActorTemplateName:     actor.GetActorTemplate().GetName(),
		Spec:                  workloadSpec,
		Type:                  ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
		ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{
			SnapshotUri: inProgressDurable.GetObject().GetSnapshotUri(),
		},
		Fidelity: fidelityToAtelet(preferredFidelity(actor.GetMetadata().GetAtespace(), actorTemplate)),
		ActorUid: actor.GetMetadata().Uid,
	}
	wireFidelity = ateattr.SnapshotFidelityValue(req.Fidelity)

	if _, err = client.Checkpoint(ctx, req); err != nil {
		return wireFidelity, handleAteletError(ctx, w.store, actorRef, ateattr.OperationSuspend, "Checkpoint", false, err)
	}
	return wireFidelity, nil
}

// ensurePausedSnapshotUploaded suspends a PAUSED actor by telling the atelet
// on the node holding the local pause snapshot to upload it to the actor's
// persisted in-progress snapshot location; no workload runs, so there is no
// ateom to checkpoint. Retries re-send the same semantic request: the
// destination is minted once and the upload overwrites deterministic object
// names, with the remote manifest as the commit marker.
func (w *ActorWorkflow) ensurePausedSnapshotUploaded(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor, actorTemplate *ateapipb.ActorTemplate) (wireFidelity string, err error) {
	ctx, done := stepSpan(ctx, "UploadPausedCheckpoint")
	defer func() { err = done(err) }()

	_, localSt := findLatestSnapshotStorage(actor.GetStatus(), ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	nodeName := actor.GetStatus().GetAssignedNode()
	if localSt == nil || nodeName == "" {
		// Without the node the snapshot can never be found (mirrors
		// FinalizePaused, which crashes rather than record an unknown node).
		if err := crashActor(ctx, w.store, actorRef, ateattr.OperationSuspend, crashMessageLocalSnapshotNodeUnknown); err != nil {
			slog.ErrorContext(ctx, "Failed to crash actor", slog.String("err", err.Error()))
		}
		return "", fmt.Errorf("actor is CRASHED because it was suspending a paused snapshot with no node recorded")
	}

	ateletConn, err := w.dialer.DialForAteletOnNode(nodeName)
	if err != nil {
		// No atelet on the node is indistinguishable from an atelet restart or
		// informer lag, and the snapshot bytes may still be on its disk: stay
		// retryable rather than crash.
		return "", fmt.Errorf("while getting atelet conn for node %q: %w", nodeName, err)
	}
	client := ateletpb.NewAteomHerderClient(ateletConn)

	_, inProgressDurable := findLatestSnapshotStorage(actor.GetStatus(), ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS)
	req := &ateletpb.UploadPausedCheckpointRequest{
		Atespace:               actor.GetMetadata().GetAtespace(),
		ActorName:              actor.GetMetadata().GetName(),
		ActorUid:               actor.GetMetadata().GetUid(),
		ActorTemplateAtespace:  actor.GetActorTemplate().GetAtespace(),
		ActorTemplateName:      actor.GetActorTemplate().GetName(),
		LocalSnapshotName:      localSt.GetLocal().GetSnapshotName(),
		DestinationSnapshotUri: inProgressDurable.GetObject().GetSnapshotUri(),
		// The commit scope, like a running-origin suspend; atelet converts
		// from the captured scope in the snapshot's manifest where possible.
		DesiredFidelity: fidelityToAtelet(preferredFidelity(actor.GetMetadata().GetAtespace(), actorTemplate)),
	}
	wireFidelity = ateattr.SnapshotFidelityValue(req.DesiredFidelity)

	if _, err = client.UploadPausedCheckpoint(ctx, req); err != nil {
		return wireFidelity, handleAteletError(ctx, w.store, actorRef, ateattr.OperationSuspend, "UploadPausedCheckpoint", false, err)
	}
	return wireFidelity, nil
}

// newInProgressSnapshotURI is where the snapshot an actor is currently taking is
// written: under the actor's own prefix, so the objects name their owner.
func newInProgressSnapshotURI(actorTemplate *ateapipb.ActorTemplate, actor *ateapipb.Actor) (resources.SnapshotURI, error) {
	atespace := actor.GetMetadata().GetAtespace()
	uri, err := resources.NewActorSnapshotURI(actorTemplate.GetSnapshotConfig().GetStorageLocation(), atespace, actor.GetMetadata().GetUid(), resources.NewSnapshotName())
	if err != nil {
		return resources.SnapshotURI{}, fmt.Errorf("while building the snapshot URI for actor %s/%s: %w", atespace, actor.GetMetadata().GetName(), err)
	}
	return uri, nil
}

// ensureVolumesDetached detaches the actor's mounted external volumes from
// its worker node. Detachment is idempotent, so a re-entered workflow safely
// runs it again. spanName distinguishes the suspend and pause steps in
// traces; op labels the volume metrics.
// TODO replace re-execution with a proper check on the volumes' attach state.
func (w *ActorWorkflow) ensureVolumesDetached(ctx context.Context, actor *ateapipb.Actor, actorTemplate *ateapipb.ActorTemplate, spanName, op string) (err error) {
	ctx, done := stepSpan(ctx, spanName)
	defer func() { err = done(err) }()

	return detachActorVolumes(ctx, w.pluginRegistry, actor, actorTemplate, op)
}

// ensureSuspendedFinalized releases the actor's worker (only when it is still
// owned by this actor), records the in-progress snapshot as the actor's
// durable snapshot, and commits SUSPENDED with the assignment cleared in a
// single update, then releases the durable snapshot that update replaced.
// It re-reads the actor first so an out-of-band transition (e.g. the syncer
// crashing the actor after its worker died) is not overwritten: with no
// assignment left there is nothing to finalize.
func (w *ActorWorkflow) ensureSuspendedFinalized(ctx context.Context, actorRef resources.ActorRef) (_ *ateapipb.Actor, err error) {
	ctx, done := stepSpan(ctx, "FinalizeSuspended")
	defer func() { err = done(err) }()

	// The step is a chain of store round-trips with no spans of its own, so
	// log a per-call breakdown to attribute its latency. Deferred so a call
	// that stalls and then fails still reports where the time went; steps not
	// reached (or skipped) log zero.
	start := time.Now()
	var dGetActor, dReleaseWorker, dRefetchActor, dUpdateActor, dReleaseSnapshot time.Duration
	defer func() {
		slog.InfoContext(ctx, "FinalizeSuspended store call durations",
			slog.Any("actor", actorRef),
			slog.Duration("total", time.Since(start)),
			slog.Duration("get_actor", dGetActor),
			slog.Duration("release_worker", dReleaseWorker),
			slog.Duration("refetch_actor", dRefetchActor),
			slog.Duration("update_actor", dUpdateActor),
			slog.Duration("release_snapshot", dReleaseSnapshot))
	}()

	t := time.Now()
	latestActor, err := w.store.GetActor(ctx, actorRef)
	dGetActor = time.Since(t)
	if err != nil {
		return nil, err
	}

	// 1. Free the worker (if it hasn't been freed yet)
	if latestActor.GetStatus().GetWorkerAssignment() != nil {
		t = time.Now()
		_, _, err := releaseWorker(ctx, w.store, latestActor)
		dReleaseWorker = time.Since(t)
		if err != nil {
			return nil, err
		}

		// Re-fetch the actor now that the worker is freed.
		t = time.Now()
		latestActor, err = w.store.GetActor(ctx, actorRef)
		dRefetchActor = time.Since(t)
		if err != nil {
			return nil, err
		}
	}

	// 2. Finalize the actor: record its new durable snapshot and mark it SUSPENDED. This
	// must run even with no worker assignment (nothing to free), or the actor
	// would be left SUSPENDING forever with the workflow reporting success.
	_, prevSt := findLatestSnapshotStorage(latestActor.GetStatus(), ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)

	// 3. Commit the actor.
	t = time.Now()
	storedActor, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(latestActor), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
		snap := snapshotAtLatestGeneration(toUpdate.Status)
		if snap == nil {
			return fmt.Errorf("actor %s has no latest snapshot", actorRef)
		}
		if inProgressSt := findSnapshotStorage(snap, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE); inProgressSt.GetObject().GetSnapshotUri() != "" {
			inProgressSt.Status = ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
		}
		// Remove all local snapshots since they are superseded by the new durable snapshot.
		// TODO: This will change when we merge pause and suspend and allow for both local
		// and durable snapshots to be kept.
		removeSnapshotStorageEntries(toUpdate.Status, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL, nil)
		// Remove all durable snapshots older than the new durable snapshot.
		removeOlderSnapshotStorageEntries(toUpdate.Status, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE, new(ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED), snap.GetGeneration())
		toUpdate.Status.WorkerAssignment = nil
		toUpdate.Status.AssignedNode = ""
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

	// 4. Release the external snapshot this suspend replaced. Best-effort: the
	// new snapshot is committed and the worker is gone, so failing the
	// workflow here would only strand the actor. A failure leaves the old
	// snapshot's objects in storage until the actor is deleted, which removes
	// its whole prefix.
	t = time.Now()
	_, nextSt := findLatestSnapshotStorage(storedActor.GetStatus(), ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	releaseErr := w.releaseReplacedSnapshot(ctx, latestActor, nextSt)
	dReleaseSnapshot = time.Since(t)
	if releaseErr != nil {
		slog.WarnContext(ctx, "Failed to release the external snapshot a suspend replaced; its objects are left in storage until the actor is deleted",
			slog.Any("actor", actorRef),
			slog.String("snapshot_uri", prevSt.GetObject().GetSnapshotUri()),
			slog.String("err", releaseErr.Error()))
	}
	return storedActor, nil
}

// releaseReplacedSnapshot releases the external snapshot the actor held before
// this suspend. actor is the record as it was before the suspend committed.
// An actor that borrowed its current snapshot from a tag releases nothing —
// the snapshot lives under the tag's prefix, and the tag outlives the actor.
func (w *ActorWorkflow) releaseReplacedSnapshot(ctx context.Context, actor *ateapipb.Actor, nextSnapshot *ateapipb.SnapshotStorage) (err error) {
	ctx, done := stepSpan(ctx, "ReleaseReplacedSnapshot")
	defer func() { err = done(err) }()

	_, prevSt := findLatestSnapshotStorage(actor.GetStatus(), ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	previous := prevSt.GetObject().GetSnapshotUri()
	switch {
	case w.snapshotPlugin == nil:
		markSkipped(ctx, "no object store configured")
		return nil
	case previous == "":
		markSkipped(ctx, "the actor held no external snapshot")
		return nil
	case previous == nextSnapshot.GetObject().GetSnapshotUri():
		markSkipped(ctx, "the actor's external snapshot is unchanged")
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
