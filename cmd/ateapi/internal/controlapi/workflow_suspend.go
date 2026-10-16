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
	"google.golang.org/protobuf/proto"
)

// suspendMode selects which of the two suspends the suspend workflow runs.
// Both take the same steps; the mode only decides the shape of the snapshot
// they take.
type suspendMode int

const (
	// suspendSync makes the snapshot durable before the workflow returns:
	// the actor ends SUSPENDED holding a durable snapshot (SuspendActor).
	suspendSync suspendMode = iota
	// suspendAsync leaves the snapshot on the worker's node, to be uploaded
	// later: the actor ends SUSPENDED holding a node-local snapshot and
	// pinned to that node (PauseActor). Nothing uploads it yet.
	suspendAsync
)

// uploadMode is when atelet makes the checkpoint durable.
func (m suspendMode) uploadMode() ateletpb.UploadMode {
	if m == suspendAsync {
		return ateletpb.UploadMode_UPLOAD_MODE_DEFERRED
	}
	return ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE
}

// SuspendActor executes the workflow to suspend a running or paused actor:
// a running actor is checkpointed on its worker, a paused actor's node-local
// snapshot is uploaded. Idempotent: a re-entered workflow fast-forwards past
// the steps a previous attempt completed, deriving progress from the
// persisted actor alone.
func (w *ActorWorkflow) SuspendActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error) {
	ctx = preview.ContextWithOptIn(ctx)
	return w.suspendActor(ctx, actorRef, suspendSync)
}

// PauseActor executes the workflow to pause a running actor: it is
// checkpointed on its worker and the snapshot is kept on that node.
// Idempotent: a re-entered workflow fast-forwards past the steps a previous
// attempt completed, deriving progress from the persisted actor alone.
func (w *ActorWorkflow) PauseActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error) {
	ctx = preview.ContextWithOptIn(ctx)
	return w.suspendActor(ctx, actorRef, suspendAsync)
}

// suspendActor runs the suspend workflow behind SuspendActor and PauseActor.
func (w *ActorWorkflow) suspendActor(ctx context.Context, actorRef resources.ActorRef, mode suspendMode) (_ *ateapipb.Actor, err error) {
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
	// FinalizeSuspended commits SUSPENDED and the cleared worker assignment in
	// a single update, so an actor already holding the snapshot this mode
	// leaves behind has nothing left to do. These successes report no pool,
	// and cannot: the previous attempt released the worker, so the record
	// names none (#957).
	if actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		if mode == suspendAsync && latestSnapshotIsLocalOnly(actor.GetStatus()) {
			return actor, nil // fully paused
		}
		if mode == suspendSync && !latestSnapshotIsLocalOnly(actor.GetStatus()) {
			return actor, nil // fully suspended
		}
		// A sync suspend of a paused actor goes on to upload its node-local
		// snapshot. An async suspend of a suspended actor is rejected by MarkSuspending.
	}
	// Decided before marking: once SUSPENDING is committed, the loaded status
	// alone can no longer tell the two origins apart.
	// TODO(paused-suspend): remove the paused -> suspended transition before GA.
	fromPaused := mode == suspendSync && isPausedOriginSuspend(actor)
	var marked *ateapipb.Actor
	if marked, err = w.ensureMarkedSuspending(leaseCtx, actorRef, actor, actorTemplate, mode); err != nil {
		return nil, err
	}
	actor = marked
	if fromPaused {
		wireFidelity, err = w.ensurePausedSnapshotUploaded(leaseCtx, actorRef, actor, actorTemplate)
	} else {
		wireFidelity, err = w.ensureAteletSuspended(leaseCtx, actorRef, actor, actorTemplate, mode)
	}
	if err != nil {
		return nil, err
	}
	// TODO(#2734): An async suspend should skip detaching here, since the actor can resume on the same node.
	if err = w.ensureVolumesDetached(leaseCtx, actor, actorTemplate, "DetachVolumes", ateattr.OperationSuspend); err != nil {
		return nil, err
	}
	// FinalizeSuspended clears the WorkerAssignment the labels read, so snapshot
	// them here, as crash.go does for the crash counter.
	finalAttrs = lifecycleOpAttrs(actor, actorTemplate, "", wireFidelity)
	var finalized *ateapipb.Actor
	if finalized, err = w.ensureSuspendedFinalized(leaseCtx, actorRef, mode); err != nil {
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

// ensureMarkedSuspending transitions a RUNNING or paused actor to
// SUSPENDING, minting the in-progress snapshot and recording the actor
// version the snapshot will capture. A sync suspend mints a pending durable
// snapshot; an async one mints a node-local snapshot with no locality yet.
// Skips when a previous attempt of the same mode already marked the actor;
// the persisted snapshot and source version then stay authoritative for the
// rest of the workflow. A mark left by the other mode is rejected: that
// workflow owns the actor until it finishes.
func (w *ActorWorkflow) ensureMarkedSuspending(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor, actorTemplate *ateapipb.ActorTemplate, mode suspendMode) (_ *ateapipb.Actor, err error) {
	ctx, done := stepSpan(ctx, "MarkSuspending")
	defer func() { err = done(err) }()

	if actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_SUSPENDING {
		// An async mark's latest snapshot is node-local with no locality yet,
		// a sync mark's has a pending durable copy.
		if latestSnapshotIsPendingLocal(actor.GetStatus()) != (mode == suspendAsync) {
			inProgress := "Pause"
			if mode == suspendAsync {
				inProgress = "Suspend"
			}
			return nil, apierror.FailedPrecondition("MarkSuspending prerequisite not met for Actor: %s (a previous %s is in progress)", actorRef, inProgress)
		}
		markSkipped(ctx, "actor already SUSPENDING")
		return actor, nil
	}
	state := actor.GetStatus().GetState()
	// Only a sync suspend takes a paused actor.
	paused := mode == suspendSync && isPausedOriginSuspend(actor) // TODO(paused-suspend)
	if state != ateapipb.ActorState_ACTOR_STATE_RUNNING && !paused {
		if mode == suspendAsync {
			return nil, apierror.FailedPrecondition("MarkSuspending prerequisite not met for Actor: %s (got: %v, want %s)", actorRef, state, ateapipb.ActorState_ACTOR_STATE_RUNNING)
		}
		return nil, apierror.FailedPrecondition("MarkSuspending prerequisite not met for Actor: %s (got: %v, want %s or %s with a node-local snapshot)", actorRef, state, ateapipb.ActorState_ACTOR_STATE_RUNNING, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	}
	// Golden actors are only suspended, never paused.
	// TODO: Remove this check: the suspend workflow should not special-case golden actors.
	if mode == suspendAsync && actorRef.Atespace == resources.GoldenActorAtespace {
		return nil, apierror.FailedPrecondition("actors in atespace %q are golden actors, which cannot be paused", actorRef.Atespace)
	}
	// A paused actor's durable snapshot is an upload of its local checkpoint,
	// so it keeps that checkpoint's name; a running actor's is a new one.
	var snapshotName string
	if paused {
		// paused implies a snapshot with a locality, so this is never nil.
		snapshotName = findLatestLocalSnapshot(actor.GetStatus()).GetUuid()
	} else {
		snapshotName = resources.NewSnapshotName()
	}

	// Fail here rather than at checkpoint time if the template's location
	// cannot produce a usable URI: nothing has been written yet.
	uri, err := newInProgressSnapshotURI(actorTemplate, actor, snapshotName)
	if err != nil {
		return nil, err
	}
	storedActor, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDING
		if paused {
			// Promote the current generation's local snapshot to a pending durable snapshot.
			snap := findLatestLocalSnapshot(toUpdate.Status)
			if snap == nil {
				return fmt.Errorf("actor %s is paused but has no completed local snapshot", actorRef)
			}
			snap.ActorTemplateUid = actorTemplate.GetMetadata().GetUid()
			snap.Fidelity = preferredFidelity(actorRef.Atespace, actorTemplate)
			snap.DurableSnapshot = &ateapipb.SnapshotStorage{
				Status: ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING,
				Object: &ateapipb.ObjectSnapshot{SnapshotUri: uri.String()},
			}
			return nil
		}
		// Increment last_assigned_generation for the new Suspend request and create
		// a pending snapshot for that generation: durable for a sync suspend,
		// node-local with no locality until finalize for an async one.
		gen := toUpdate.Status.LastAssignedGeneration + 1
		toUpdate.Status.LastAssignedGeneration = gen

		fidelity := preferredFidelity(actorRef.Atespace, actorTemplate)
		templateUID := actorTemplate.GetMetadata().GetUid()
		snap := newLocalSnapshot(gen, fidelity, templateUID, snapshotName, "")
		if mode == suspendSync {
			snap = newDurableSnapshot(
				gen,
				ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR,
				fidelity,
				templateUID,
				uri.Name(),
				uri.String(),
				ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING,
			)
		}
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

// TODO(paused-suspend): remove with the paused -> suspended transition.
// isPausedOriginSuspend reports whether the suspend must upload a paused
// actor's node-local snapshot instead of checkpointing a running workload.
// A local snapshot alone does not mean paused-origin: resume restores from it
// without deleting its files, so a RUNNING actor resumed from pause still
// carries one until its next checkpoint. The nil worker assignment
// disambiguates — running-origin suspends keep their assignment until
// finalize, paused actors never have one.
func isPausedOriginSuspend(actor *ateapipb.Actor) bool {
	if actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_SUSPENDED && latestSnapshotIsLocalOnly(actor.GetStatus()) {
		return true
	}
	if actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_SUSPENDING &&
		actor.GetStatus().GetWorkerAssignment() == nil {
		return findLatestLocalSnapshot(actor.GetStatus()) != nil
	}
	return false
}

// ensureAteletSuspended checkpoints the workload to the actor's persisted
// in-progress snapshot location. A sync suspend has atelet upload it before
// returning; an async suspend has atelet keep it on the node.
// This is the atelet reentrancy seam (#372):
// the request is keyed by the actor UID, the worker pod UID, and the
// once-minted snapshot location, so a re-entered workflow re-sends the same
// semantic request; once atelet's Checkpoint is idempotent on those keys this
// step becomes fully reentrant with no changes here.
func (w *ActorWorkflow) ensureAteletSuspended(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor, actorTemplate *ateapipb.ActorTemplate, mode suspendMode) (wireFidelity string, err error) {
	ctx, done := stepSpan(ctx, "CallAteletSuspend")
	defer func() { err = done(err) }()

	assignment := actor.GetStatus().GetWorkerAssignment()
	if assignment == nil {
		// Missing active worker pod reference in SUSPENDING state indicates corrupted store state.
		return "", crashActorAndFail(ctx, w.store, actorRef, ateattr.OperationSuspend, crashMessageWorkerAssignmentMissing)
	}

	ateletConn, err := w.dialer.DialForAteletOnNode(assignment.GetNodeName())
	if err != nil {
		return "", fmt.Errorf("while getting atelet conn for node %q: %w", assignment.GetNodeName(), err)
	}
	client := ateletpb.NewAteletClient(ateletConn)

	workloadSpec, err := workloadSpecFromActorTemplate(actorTemplate, actor, nil)
	if err != nil {
		return "", err
	}

	// Checkpoint does not carry the sandbox config: atelet uses the version the
	// actor is currently running (recorded on-node at Run/Restore) and pins it
	// into the snapshot manifest.
	inProgressSnap := snapshotAtLatestGeneration(actor.GetStatus())
	fidelity := fidelityToAtelet(preferredFidelity(actor.GetMetadata().GetAtespace(), actorTemplate))
	// A sync suspend records its URI on the in-progress durable snapshot;
	// an async one has no durable snapshot record yet, so derive the URI
	// from the persisted snapshot UUID.
	snapshotURI := inProgressSnap.GetDurableSnapshot().GetObject().GetSnapshotUri()
	if mode == suspendAsync {
		uri, err := newInProgressSnapshotURI(actorTemplate, actor, inProgressSnap.GetUuid())
		if err != nil {
			return "", err
		}
		snapshotURI = uri.String()
	}
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
				SnapshotUri: snapshotURI,
			},
		},
		UploadConfig: &ateletpb.UploadConfig{
			Mode: mode.uploadMode(),
		},
		ActorUid: actor.GetMetadata().Uid,
	}
	wireFidelity = ateattr.SnapshotFidelityValue(fidelity)

	if _, err = client.Checkpoint(ctx, req); err != nil {
		hErr := handleAteletError(ctx, w.store, actorRef, ateattr.OperationSuspend, "Checkpoint", false, err)
		if isWorkerReachabilityError(err) {
			workerName := assignment.GetWorker().GetName()
			worker, werr := w.store.GetWorker(ctx, workerName)
			if (werr == nil && worker.GetStatus().GetState() == ateapipb.WorkerState_WORKER_STATE_UNAVAILABLE) || errors.Is(werr, store.ErrNotFound) {
				slog.WarnContext(ctx, "crashing actor because worker is unavailable during suspend checkpoint",
					slog.String("actor", actorRef.String()),
					slog.String("worker", workerName))
				if cerr := crashActor(ctx, w.store, actorRef, ateattr.OperationSuspend, ateletCrashMessage("Checkpoint", err)); cerr != nil {
					return wireFidelity, cerr
				}
				return wireFidelity, fmt.Errorf("actor %s crashed: %w", actorRef, err)
			}
		}
		return wireFidelity, hErr
	}
	return wireFidelity, nil
}

// TODO(paused-suspend): remove with the paused -> suspended transition.
// ensurePausedSnapshotUploaded suspends a paused actor by telling the atelet
// on the node holding the local pause snapshot to upload it to the actor's
// persisted in-progress snapshot location; no workload runs, so there is no
// worker to checkpoint. Retries re-send the same semantic request: the
// destination is minted once and the upload overwrites deterministic object
// names, with the remote manifest as the commit marker.
func (w *ActorWorkflow) ensurePausedSnapshotUploaded(ctx context.Context, actorRef resources.ActorRef, actor *ateapipb.Actor, actorTemplate *ateapipb.ActorTemplate) (wireFidelity string, err error) {
	ctx, done := stepSpan(ctx, "PromoteSnapshot")
	defer func() { err = done(err) }()

	localSnap := findLatestLocalSnapshot(actor.GetStatus())
	if localSnap == nil {
		return "", crashActorAndFail(ctx, w.store, actorRef, ateattr.OperationSuspend, crashMessageLocalSnapshotMissing)
	}
	nodeName := actor.GetStatus().GetAssignedNode()
	if nodeName == "" {
		// Without the node the snapshot can never be found (mirrors an async
		// FinalizeSuspended, which crashes rather than record an unknown node).
		return "", crashActorAndFail(ctx, w.store, actorRef, ateattr.OperationSuspend, crashMessageLocalSnapshotNodeUnknown)
	}

	ateletConn, err := w.dialer.DialForAteletOnNode(nodeName)
	if err != nil {
		// No atelet on the node is indistinguishable from an atelet restart or
		// informer lag, and the snapshot bytes may still be on its disk: stay
		// retryable rather than crash.
		return "", fmt.Errorf("while getting atelet conn for node %q: %w", nodeName, err)
	}
	client := ateletpb.NewAteletClient(ateletConn)

	// The template's preferred fidelity, as a running-origin suspend commits.
	fidelity := fidelityToAtelet(preferredFidelity(actor.GetMetadata().GetAtespace(), actorTemplate))
	req := &ateletpb.PromoteSnapshotRequest{
		Atespace:              actor.GetMetadata().GetAtespace(),
		ActorName:             actor.GetMetadata().GetName(),
		ActorUid:              actor.GetMetadata().GetUid(),
		ActorTemplateAtespace: actor.GetActorTemplate().GetAtespace(),
		ActorTemplateName:     actor.GetActorTemplate().GetName(),
		Snapshot: &ateletpb.Snapshot{
			SnapshotUuid: localSnap.GetUuid(),
			Generation:   localSnap.GetGeneration(),
			Fidelity:     fidelity,
			Object: &ateletpb.ObjectStorage{
				SnapshotUri: snapshotAtLatestGeneration(actor.GetStatus()).GetDurableSnapshot().GetObject().GetSnapshotUri(),
			},
		},
	}
	wireFidelity = ateattr.SnapshotFidelityValue(fidelity)

	if _, err = client.PromoteSnapshot(ctx, req); err != nil {
		return wireFidelity, handleAteletError(ctx, w.store, actorRef, ateattr.OperationSuspend, "PromoteSnapshot", false, err)
	}
	return wireFidelity, nil
}

// newInProgressSnapshotURI is where the snapshot an actor is currently taking is
// written: under the actor's own prefix, so the objects name their owner.
func newInProgressSnapshotURI(actorTemplate *ateapipb.ActorTemplate, actor *ateapipb.Actor, snapshotName string) (resources.SnapshotURI, error) {
	atespace := actor.GetMetadata().GetAtespace()
	uri, err := resources.NewActorSnapshotURI(actorTemplate.GetSnapshotConfig().GetStorageLocation(), atespace, actor.GetMetadata().GetUid(), snapshotName)
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
// durable snapshot (sync) or node-local snapshot (async), and commits
// SUSPENDED with the assignment cleared in a single update, then releases the
// durable snapshot that update replaced.
// When the latest snapshot cannot be recorded, it commits CRASHED instead and
// fails the step.
// It re-reads the actor first so an out-of-band transition (e.g. the syncer
// crashing the actor after its worker died) is not overwritten: an actor that
// is no longer SUSPENDING is left as it is and the step fails with
// FailedPrecondition.
func (w *ActorWorkflow) ensureSuspendedFinalized(ctx context.Context, actorRef resources.ActorRef, mode suspendMode) (_ *ateapipb.Actor, err error) {
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
	switch mode {
	case suspendSync:
		if snapshotAtLatestGeneration(latestActor.GetStatus()).GetDurableSnapshot() == nil {
			return nil, crashActorAndFail(ctx, w.store, actorRef, ateattr.OperationSuspend, crashMessageDurableSnapshotMissing)
		}
	case suspendAsync:
		// A local snapshot on an unknown node can never be found by the scheduler.
		if latestActor.GetStatus().GetAssignedNode() == "" {
			return nil, crashActorAndFail(ctx, w.store, actorRef, ateattr.OperationSuspend, crashMessageLocalSnapshotNodeUnknown)
		}
		if snapshotAtLatestGeneration(latestActor.GetStatus()) == nil {
			return nil, crashActorAndFail(ctx, w.store, actorRef, ateattr.OperationSuspend, crashMessageLocalSnapshotMissing)
		}
	}

	// 1. Free the worker (if it hasn't been freed yet)
	if latestActor.GetStatus().GetWorkerAssignment() != nil {
		t = time.Now()
		_, _, err := releaseWorker(ctx, w.store, latestActor)
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
		snap := snapshotAtLatestGeneration(toUpdate.Status)
		switch mode {
		case suspendSync:
			snap.GetDurableSnapshot().Status = ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
			// Remove all local snapshots since they are superseded by the new durable snapshot.
			// TODO: This will change when an async suspend uploads its snapshot
			// and both local and durable snapshots are kept.
			pruneSnapshots(toUpdate.Status, nil)
			toUpdate.Status.AssignedNode = ""
		case suspendAsync:
			// Record the node locality of the local checkpoint and clear older local checkpoints.
			// AssignedNode is kept: a paused actor resumes on that node.
			snap.Locality = toUpdate.GetStatus().GetAssignedNode()
			pruneSnapshots(toUpdate.Status, snap)
		}
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

	// 3. Release the external snapshot this suspend replaced. Best-effort: the
	// new snapshot is committed and the worker is gone, so failing the
	// workflow here would only strand the actor. A failure leaves the old
	// snapshot's objects in storage until the actor is deleted, which removes
	// its whole prefix.
	t = time.Now()
	replaced := findLatestDurableSnapshot(latestActor.GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED).GetDurableSnapshot().GetObject().GetSnapshotUri()
	current := findLatestDurableSnapshot(storedActor.GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED).GetDurableSnapshot().GetObject().GetSnapshotUri()
	releaseErr := w.releaseReplacedSnapshot(ctx, latestActor, replaced, current)
	dReleaseSnapshot = time.Since(t)
	if releaseErr != nil {
		slog.WarnContext(ctx, "Failed to release the external snapshot a suspend replaced; its objects are left in storage until the actor is deleted",
			slog.Any("actor", actorRef),
			slog.String("snapshot_uri", replaced),
			slog.String("err", releaseErr.Error()))
	}
	return storedActor, nil
}

// releaseReplacedSnapshot releases previous, the URI of the external snapshot
// the actor held before this suspend, unless it is still current. actor is
// the record as it was before the suspend committed.
// An actor that borrowed its current snapshot from a tag releases nothing —
// the snapshot lives under the tag's prefix, and the tag outlives the actor.
func (w *ActorWorkflow) releaseReplacedSnapshot(ctx context.Context, actor *ateapipb.Actor, previous, current string) (err error) {
	ctx, done := stepSpan(ctx, "ReleaseReplacedSnapshot")
	defer func() { err = done(err) }()

	switch {
	case w.objectStorePlugin == nil:
		markSkipped(ctx, "no object store configured")
		return nil
	case previous == "":
		markSkipped(ctx, "the actor held no external snapshot")
		return nil
	case previous == current:
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
