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
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/objectstore"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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
	var wireSnapshotScope string
	// Set just before finalize; nil until then, so earlier exits label
	// themselves from the record they hold.
	var finalAttrs []attribute.KeyValue

	defer func() {
		attrs := finalAttrs
		if attrs == nil {
			attrs = lifecycleOpAttrs(actor, actorTemplate, "", wireSnapshotScope)
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
		wireSnapshotScope, err = w.ensurePausedSnapshotUploaded(leaseCtx, actorRef, actor, actorTemplate)
	} else {
		wireSnapshotScope, err = w.ensureAteletSuspended(leaseCtx, actorRef, actor, actorTemplate)
	}
	if err != nil {
		return nil, err
	}
	if err = w.ensureVolumesDetached(leaseCtx, actor, actorTemplate, "DetachVolumes", ateattr.OperationSuspend); err != nil {
		return nil, err
	}
	// FinalizeSuspended clears the WorkerAssignment the labels read, so snapshot
	// them here, as crash.go does for the crash counter.
	finalAttrs = lifecycleOpAttrs(actor, actorTemplate, "", wireSnapshotScope)
	var finalized *ateapipb.Actor
	if finalized, err = w.ensureSuspendedFinalized(leaseCtx, actorRef, actorTemplate); err != nil {
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
		return nil, status.Errorf(codes.FailedPrecondition, "MarkSuspending prerequisite not met for Actor: %s (got: %v, want %s or %s)", actorRef, got, ateapipb.ActorState_ACTOR_STATE_RUNNING, ateapipb.ActorState_ACTOR_STATE_PAUSED)
	}
	// A paused-origin suspend uploads what the pause captured; it cannot
	// fabricate the memory a Full commit needs from a Data-only capture.
	// Reject before leaving PAUSED so the actor stays resumable.
	if actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_PAUSED &&
		pausedContentScope(actor.GetStatus().GetLocalSnapshotInfo(), actorTemplate) == ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA &&
		commitSnapshotScope(actorRef.Atespace, actorTemplate) == ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL {
		return nil, status.Errorf(codes.FailedPrecondition, "actor %s paused with a Data snapshot; the template commits Full, which a paused-origin suspend cannot produce", actorRef)
	}

	name := resources.NewSnapshotName()
	// Fail here rather than at checkpoint time if the template's location
	// cannot produce a usable URI: nothing has been written yet.
	if _, err := inProgressSnapshotURI(actorTemplate, actor, name); err != nil {
		return nil, err
	}
	storedActor, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDING
		toUpdate.Status.InProgressSnapshotName = name
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return nil, err
	}
	return storedActor, nil
}

// commitSnapshotScope returns the scope a commit (suspend) snapshot is taken
// with. Golden actors always commit Full regardless of the template's
// onCommit: the golden snapshot is the base an OnGolden data resume is
// combined with at restore, so it must carry the guest memory and filesystem
// — a data-only golden would leave nothing to restore the guest from.
func commitSnapshotScope(atespace string, tmpl *ateapipb.ActorTemplate) ateapipb.SnapshotContentScope {
	if atespace == resources.GoldenActorAtespace {
		return ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL
	}
	return effectiveContentScope(tmpl.GetSnapshotsConfig().GetOnCommit())
}

// pausedContentScope returns the scope a paused actor's local snapshot was
// captured with: the value recorded at pause finalization, or — for actors
// paused before content_scope existed — the template's onPause, the same
// derivation resume uses for local snapshots.
func pausedContentScope(local *ateapipb.LocalSnapshotInfo, tmpl *ateapipb.ActorTemplate) ateapipb.SnapshotContentScope {
	if scope := local.GetContentScope(); scope != ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED {
		return scope
	}
	return effectiveContentScope(tmpl.GetSnapshotsConfig().GetOnPause())
}

// isPausedOriginSuspend reports whether the suspend must upload a PAUSED
// actor's node-local snapshot instead of checkpointing a running workload.
// A LocalSnapshotInfo alone does not mean paused-origin: resume never clears
// it, so a RUNNING actor resumed from pause still carries a stale one. The
// nil worker assignment disambiguates — running-origin suspends keep their
// assignment until finalize, paused actors never have one.
func isPausedOriginSuspend(actor *ateapipb.Actor) bool {
	return actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_PAUSED ||
		(actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_SUSPENDING &&
			actor.GetStatus().GetWorkerAssignment() == nil &&
			actor.GetStatus().GetLocalSnapshotInfo() != nil)
}

// ensureAteletSuspended checkpoints the workload to the actor's persisted
// in-progress snapshot location. This is the atelet reentrancy seam (#372):
// the request is keyed by the actor UID, the worker pod UID, and the
// once-minted snapshot location, so a re-entered workflow re-sends the same
// semantic request; once atelet's Checkpoint is idempotent on those keys this
// step becomes fully reentrant with no changes here.
func (w *ActorWorkflow) ensureAteletSuspended(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor, actorTemplate *ateapipb.ActorTemplate) (wireSnapshotScope string, err error) {
	ctx, done := stepSpan(ctx, "CallAteletSuspend")
	defer func() { err = done(err) }()

	assignment := actor.GetStatus().GetWorkerAssignment()
	if assignment == nil {
		// Missing active worker pod reference in SUSPENDING state indicates corrupted store state.
		if err := crashActor(ctx, w.store, actorRef, ateattr.OperationSuspend, ateattr.ReasonCorruptedAssignment); err != nil {
			slog.ErrorContext(ctx, "Failed to crash actor", slog.String("err", err.Error()))
		}
		return "", fmt.Errorf("actor is CRASHED because it was in SUSPENDING state but has no active worker")
	}

	ateletConn, err := w.dialer.DialForWorker(assignment.GetWorkerNamespace(), assignment.GetWorkerPod())
	if err != nil {
		if errors.Is(err, ErrWorkerPodNotFound) {
			slog.ErrorContext(ctx, "Worker pod gone before checkpoint, crashing actor", "namespace", assignment.GetWorkerNamespace(), "pod", assignment.GetWorkerPod(), "in_progress_snapshot_name", actor.GetStatus().GetInProgressSnapshotName())
			if err := crashActor(ctx, w.store, actorRef, ateattr.OperationSuspend, ateattr.ReasonWorkerPodGone); err != nil {
				slog.ErrorContext(ctx, "Failed to crash actor", slog.String("err", err.Error()))
			}
			return "", fmt.Errorf("actor is CRASHED because its worker pod is gone and no snapshot was written")
		}
		return "", fmt.Errorf("while getting atelet conn for worker pod: %w", err)
	}
	client := ateletpb.NewAteomHerderClient(ateletConn)

	workloadSpec, err := workloadSpecFromActorTemplate(actorTemplate, actor)
	if err != nil {
		return "", err
	}

	snapshotURI, err := inProgressSnapshotURI(actorTemplate, actor, actor.GetStatus().GetInProgressSnapshotName())
	if err != nil {
		return "", err
	}

	// Checkpoint does not carry the sandbox config: atelet uses the version the
	// actor is currently running (recorded on-node at Run/Restore) and pins it
	// into the snapshot manifest.
	req := &ateletpb.CheckpointRequest{
		TargetAteomUid:        assignment.GetWorkerPodUid(),
		Atespace:              actor.GetMetadata().GetAtespace(),
		ActorName:             actor.GetMetadata().GetName(),
		ActorTemplateAtespace: actor.GetActorTemplate().GetAtespace(),
		ActorTemplateName:     actor.GetActorTemplate().GetName(),
		Spec:                  workloadSpec,
		Type:                  ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
		Config: &ateletpb.CheckpointRequest_ExternalConfig{
			ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{
				SnapshotUri: snapshotURI.String(),
			},
		},
		Scope:    actorSnapshotContentScopeToAtelet(commitSnapshotScope(actor.GetMetadata().GetAtespace(), actorTemplate)),
		ActorUid: actor.GetMetadata().Uid,
	}
	wireSnapshotScope = ateattr.SnapshotScopeValue(req.Scope)

	_, err = client.Checkpoint(ctx, req)
	return wireSnapshotScope, maybeCrashActor(ctx, w.store, actorRef, err, "while checkpointing workload", ateattr.OperationSuspend)
}

// ensurePausedSnapshotUploaded suspends a PAUSED actor by telling the atelet
// on the node holding the local pause snapshot to upload it to the actor's
// persisted in-progress snapshot location; no workload runs, so there is no
// ateom to checkpoint. Retries re-send the same semantic request: the
// destination is minted once and the upload overwrites deterministic object
// names, with the remote manifest as the commit marker.
func (w *ActorWorkflow) ensurePausedSnapshotUploaded(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor, actorTemplate *ateapipb.ActorTemplate) (wireSnapshotScope string, err error) {
	ctx, done := stepSpan(ctx, "UploadPausedCheckpoint")
	defer func() { err = done(err) }()

	local := actor.GetStatus().GetLocalSnapshotInfo()
	if len(local.GetNodeVmsWithLocalSnapshots()) == 0 {
		// Without the node the snapshot can never be found (mirrors
		// FinalizePaused, which crashes rather than record an unknown node).
		if err := crashActor(ctx, w.store, actorRef, ateattr.OperationSuspend, ateattr.ReasonCorruptedAssignment); err != nil {
			slog.ErrorContext(ctx, "Failed to crash actor", slog.String("err", err.Error()))
		}
		return "", fmt.Errorf("actor is CRASHED because it was suspending a paused snapshot with no node recorded")
	}

	ateletConn, err := w.dialer.DialForAteletOnNode(local.GetNodeVmsWithLocalSnapshots()[0])
	if err != nil {
		// No atelet on the node is indistinguishable from an atelet restart or
		// informer lag, and the snapshot bytes may still be on its disk: stay
		// retryable rather than crash.
		return "", fmt.Errorf("while getting atelet conn for node %q: %w", local.GetNodeVmsWithLocalSnapshots()[0], err)
	}
	client := ateletpb.NewAteomHerderClient(ateletConn)

	snapshotURI, err := inProgressSnapshotURI(actorTemplate, actor, actor.GetStatus().GetInProgressSnapshotName())
	if err != nil {
		return "", err
	}

	req := &ateletpb.UploadPausedCheckpointRequest{
		Atespace:               actor.GetMetadata().GetAtespace(),
		ActorName:              actor.GetMetadata().GetName(),
		ActorUid:               actor.GetMetadata().GetUid(),
		ActorTemplateAtespace:  actor.GetActorTemplate().GetAtespace(),
		ActorTemplateName:      actor.GetActorTemplate().GetName(),
		LocalSnapshotName:      local.GetSnapshotName(),
		DestinationSnapshotUri: snapshotURI.String(),
		// The commit scope, like a running-origin suspend; atelet converts
		// from the captured scope in the snapshot's manifest where possible.
		DesiredScope: actorSnapshotContentScopeToAtelet(commitSnapshotScope(actor.GetMetadata().GetAtespace(), actorTemplate)),
	}
	wireSnapshotScope = ateattr.SnapshotScopeValue(req.DesiredScope)

	_, err = client.UploadPausedCheckpoint(ctx, req)
	return wireSnapshotScope, maybeCrashActor(ctx, w.store, actorRef, err, "while uploading paused snapshot", ateattr.OperationSuspend)
}

// inProgressSnapshotURI is where the snapshot an actor is currently taking is
// written: under the actor's own prefix, so the objects name their owner.
func inProgressSnapshotURI(actorTemplate *ateapipb.ActorTemplate, actor *ateapipb.Actor, name string) (resources.SnapshotURI, error) {
	atespace := actor.GetMetadata().GetAtespace()
	uri, err := resources.NewActorSnapshotURI(actorTemplate.GetSnapshotsConfig().GetStorageLocation(), atespace, actor.GetMetadata().GetUid(), name)
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

	return detachActorVolumes(ctx, w.store, w.pluginRegistry, actor, actorTemplate, op)
}

// ensureSuspendedFinalized releases the actor's worker (only when it is still
// owned by this actor), records the in-progress snapshot as the actor's
// external snapshot, and commits SUSPENDED with the assignment cleared in a
// single update. It only finalizes SUSPENDING before and after releasing the
// worker, so an out-of-band transition (e.g. worker deletion crashing the
// actor) is not overwritten.
func (w *ActorWorkflow) ensureSuspendedFinalized(ctx context.Context, actorRef resources.ActorRef, actorTemplate *ateapipb.ActorTemplate) (_ *ateapipb.Actor, err error) {
	ctx, done := stepSpan(ctx, "FinalizeSuspended")
	defer func() { err = done(err) }()

	// The step is a chain of store round-trips with no spans of its own, so
	// log a per-call breakdown to attribute its latency. Deferred so a call
	// that stalls and then fails still reports where the time went; steps not
	// reached (or skipped) log zero.
	start := time.Now()
	var dGetActor, dReleaseWorker, dRefetchActor, dReleaseSnapshot, dUpdateActor time.Duration
	defer func() {
		slog.InfoContext(ctx, "FinalizeSuspended store call durations",
			slog.Any("actor", actorRef),
			slog.Duration("total", time.Since(start)),
			slog.Duration("get_actor", dGetActor),
			slog.Duration("release_worker", dReleaseWorker),
			slog.Duration("refetch_actor", dRefetchActor),
			slog.Duration("release_snapshot", dReleaseSnapshot),
			slog.Duration("update_actor", dUpdateActor))
	}()

	t := time.Now()
	latestActor, err := w.store.GetActor(ctx, actorRef)
	dGetActor = time.Since(t)
	if err != nil {
		return nil, err
	}
	if got := latestActor.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDING {
		return nil, status.Errorf(codes.FailedPrecondition, "FinalizeSuspended prerequisite not met for Actor: %s (got: %v, want %s)", actorRef, got, ateapipb.ActorState_ACTOR_STATE_SUSPENDING)
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
		if got := latestActor.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDING {
			return nil, status.Errorf(codes.FailedPrecondition, "FinalizeSuspended prerequisite not met for Actor: %s (got: %v, want %s)", actorRef, got, ateapipb.ActorState_ACTOR_STATE_SUSPENDING)
		}
	}

	// 2. Finalize the actor: record its new external snapshot and mark it SUSPENDED. This
	// must run even with no worker assignment (nothing to free), or the actor
	// would be left SUSPENDING forever with the workflow reporting success.
	snapshotName := latestActor.GetStatus().GetInProgressSnapshotName()
	externalSnapshot := latestActor.GetStatus().GetExternalSnapshot()
	if snapshotName != "" {
		// The same inputs CallAteletSuspend used, so the recorded URI is
		// where the external snapshot was actually written.
		uri, err := inProgressSnapshotURI(actorTemplate, latestActor, snapshotName)
		if err != nil {
			return nil, err
		}
		externalSnapshot = &ateapipb.ExternalSnapshot{
			SnapshotUri:  uri.String(),
			ContentScope: commitSnapshotScope(actorRef.Atespace, actorTemplate),
		}
	}

	// 3. Release the external snapshot this suspend replaces (latestActor.externalSnapshot)
	// before it's overwritten by externalSnapshot in the commit phase below.
	t = time.Now()
	err = w.releaseReplacedSnapshot(ctx, latestActor, externalSnapshot)
	dReleaseSnapshot = time.Since(t)
	if err != nil {
		return nil, err
	}

	// 4. Commit the actor.
	t = time.Now()
	storedActor, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(latestActor), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
		if snapshotName != "" {
			// The recorded URI is under the actor's own prefix, so the actor now
			// owns its external snapshot rather than borrowing the tag's it may
			// have been created from.
			toUpdate.Status.ExternalSnapshot = proto.CloneOf(externalSnapshot)
			toUpdate.Status.InProgressSnapshotName = ""
		}
		toUpdate.Status.WorkerAssignment = nil
		toUpdate.Status.LocalSnapshotInfo = nil
		return nil
	})
	dUpdateActor = time.Since(t)
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return nil, err
	}
	return storedActor, nil
}

// releaseReplacedSnapshot releases the external snapshot the actor held before
// this suspend.
// It runs while the actor record still points at the old snapshot, so an
// interrupted release is rediscoverable: the retry deletes whatever is left.
// An actor that borrowed its current snapshot from a tag releases nothing —
// the snapshot lives under the tag's prefix, and the tag outlives the actor.
func (w *ActorWorkflow) releaseReplacedSnapshot(ctx context.Context, actor *ateapipb.Actor, nextSnapshot *ateapipb.ExternalSnapshot) (err error) {
	ctx, done := stepSpan(ctx, "ReleaseReplacedSnapshot")
	defer func() { err = done(err) }()

	previous := actor.GetStatus().GetExternalSnapshot().GetSnapshotUri()
	switch {
	case w.objectStore == nil:
		markSkipped(ctx, "no object store configured")
		return nil
	case previous == "":
		markSkipped(ctx, "the actor held no external snapshot")
		return nil
	case previous == nextSnapshot.GetSnapshotUri():
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
	return objectstore.DeletePrefix(ctx, w.objectStore, uri.Prefix())
}

func actorSnapshotOwner(actor *ateapipb.Actor) resources.SnapshotOwner {
	return resources.ActorSnapshotOwner(actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetUid())
}
