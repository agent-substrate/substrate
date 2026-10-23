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
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"go.opentelemetry.io/otel/attribute"
)

// revertableStates are the states a revert is accepted from. Snapshot locality
// does not change which lifecycle operations an actor accepts.
var revertableStates = []ateapi.ActorState{
	ateapi.ActorState_ACTOR_STATE_RUNNING,
	ateapi.ActorState_ACTOR_STATE_CRASHED,
}

// isRevertable reports whether a revert is accepted from the actor's current state.
func isRevertable(actor *ateapi.Actor) bool {
	state := actor.GetStatus().GetState()
	return slices.Contains(revertableStates, state)
}

// RevertActor executes the workflow to discard an actor's current execution and
// return it to SUSPENDED, leaving its latest committed snapshot untouched for a later
// resume to restore from. It is the recovery verb: it accepts a CRASHED actor,
// and needs no live worker to succeed.
//
// Idempotent: each step keys on a persisted field rather than on the state the
// actor was reverted from, so a re-entered workflow fast-forwards to wherever
// the previous attempt stopped.
func (w *ActorWorkflow) RevertActor(ctx context.Context, actorRef resources.ActorRef) (_ *ateapi.Actor, err error) {
	ctx = preview.ContextWithOptIn(ctx)
	start := time.Now()
	var actor *ateapi.Actor
	var actorTemplate *ateapi.ActorTemplate
	// Set just before finalize; nil until then, so earlier exits label
	// themselves from the record they hold.
	var finalAttrs []attribute.KeyValue

	defer func() {
		attrs := finalAttrs
		if attrs == nil {
			attrs = lifecycleOpAttrs(actor, "", "")
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

	var marked *ateapi.Actor
	if marked, err = w.ensureMarkedReverting(leaseCtx, actorRef, actor); err != nil {
		return nil, err
	}
	actor = marked

	if err = w.ensureWorkerDiscarded(leaseCtx, actorRef, actor, actorTemplate); err != nil {
		return nil, err
	}
	// Termination may have discovered a deleted node and cleared its local copies.
	if actor, err = w.store.GetActor(leaseCtx, actorRef); err != nil {
		return nil, err
	}

	if err = w.ensureInProgressSnapshotDiscarded(leaseCtx, actor); err != nil {
		return nil, err
	}

	// FinalizeReverted clears the WorkerAssignment the labels read, so snapshot
	// them here, as suspend and crash.go do.
	finalAttrs = lifecycleOpAttrs(actor, "", "")
	var finalized *ateapi.Actor
	if finalized, err = w.ensureRevertedFinalized(leaseCtx, actorRef); err != nil {
		return nil, err
	}
	return finalized, nil
}

// loadActorForRevert fetches the current actor record and its template.
func (w *ActorWorkflow) loadActorForRevert(ctx context.Context, actorRef resources.ActorRef) (_ *ateapi.Actor, _ *ateapi.ActorTemplate, err error) {
	ctx, done := stepSpan(ctx, "LoadActorForRevert")
	defer func() { err = done(err) }()

	actor, err := w.store.GetActor(ctx, actorRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil, apierror.NotFound("Actor %s not found", actorRef)
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
func (w *ActorWorkflow) ensureMarkedReverting(ctx context.Context, actorRef resources.ActorRef, actor *ateapi.Actor) (_ *ateapi.Actor, err error) {
	ctx, done := stepSpan(ctx, "MarkReverting")
	defer func() { err = done(err) }()

	st := actor.GetStatus().GetState()
	if st == ateapi.ActorState_ACTOR_STATE_REVERTING {
		markSkipped(ctx, "actor already REVERTING")
		return actor, nil
	}
	if !isRevertable(actor) {
		return nil, apierror.FailedPrecondition("Actor %s is not in a revertable state (got: %v, want one of %v)", actorRef, st, revertableStates)
	}

	storedActor, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(toUpdate *ateapi.Actor) error {
		toUpdate.Status.State = ateapi.ActorState_ACTOR_STATE_REVERTING
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
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
func (w *ActorWorkflow) ensureWorkerDiscarded(ctx context.Context, actorRef resources.ActorRef, actor *ateapi.Actor, actorTemplate *ateapi.ActorTemplate) (err error) {
	ctx, done := stepSpan(ctx, "DiscardWorker")
	defer func() { err = done(err) }()

	// A failed restore on another node can leave the snapshot's locality on
	// the deleted source node. The current node's Terminate cannot clear it.
	checkedNodes := map[string]bool{"": true, actor.GetStatus().GetAssignedNode(): true}
	goneNodes := map[string]bool{}
	for _, snap := range actor.GetStatus().GetSnapshots() {
		node := snap.GetLocality()
		if checkedNodes[node] {
			continue
		}
		checkedNodes[node] = true
		gone, err := w.dialer.NodeGone(ctx, node)
		if err != nil {
			return fmt.Errorf("while checking snapshot node %q: %w", node, err)
		}
		if gone {
			goneNodes[node] = true
		}
	}
	if len(goneNodes) > 0 {
		actor, err = w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(a *ateapi.Actor) error {
			for _, snapshot := range a.GetStatus().GetSnapshots() {
				if goneNodes[snapshot.GetLocality()] {
					snapshot.Locality = ""
				}
			}
			return nil
		})
		if errors.Is(err, store.ErrVersionConflict) {
			return apierror.Aborted("concurrent update conflict, please retry")
		}
		if err != nil {
			return fmt.Errorf("while clearing snapshot locality on deleted nodes: %w", err)
		}
	}

	if actor.GetStatus().GetAssignedNode() != "" {
		forgetNode := false
		snapshotState, nodeGone, err := w.ensureAteletTerminated(ctx, actorRef, actor, actorTemplate, ateattr.OperationRevert, true)
		if err != nil {
			if !canDetachWithoutAtelet(ctx, actor, err) {
				return err
			}
			forgetNode = true
		}
		if err := w.ensureVolumesDetached(ctx, actor, actorTemplate, "DetachVolumesForRevert", ateattr.OperationRevert); err != nil {
			return err
		}
		if forgetNode {
			if _, err := w.recordDetach(ctx, actorRef, actor, actor.GetStatus().GetAssignedNode(), nil); err != nil {
				return err
			}
		}
		if nodeGone || snapshotState != nil {
			// A checkpoint may commit before Suspend fails, or detach while the
			// actor is crashed. Reconcile before deleting any pending objects.
			if _, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(a *ateapi.Actor) error {
				if nodeGone {
					for _, snapshot := range a.GetStatus().GetSnapshots() {
						if snapshot.GetLocality() == actor.GetStatus().GetAssignedNode() {
							snapshot.Locality = ""
						}
					}
					return nil
				}
				return applyRevertedSnapshotState(ctx, a.Status, actor.GetStatus().GetAssignedNode(), snapshotState)
			}); err != nil {
				return err
			}
		}
		if actor.GetStatus().GetWorkerAssignment() != nil {
			if _, err := releaseWorker(ctx, w.store, actor); err != nil {
				return fmt.Errorf("while releasing worker: %w", err)
			}
		}
	}

	return w.releaseAssignmentWithoutBacklink(ctx, actor)
}

// applyRevertedSnapshotState reconciles the checkpoints atelet actually kept
// after teardown, including a checkpoint whose Suspend failed after committing.
func applyRevertedSnapshotState(ctx context.Context, status *ateapi.ActorStatus, node string, state *ateletpb.ActorSnapshotState) error {
	knownLocal := latestSnapshot(status, func(s *ateapi.Snapshot) bool { return s.GetLocality() == node })
	durable := findLatestDurableSnapshot(status, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	committed := max(state.GetLatestLocal().GetSnapshot().GetGeneration(), state.GetLatestDurable().GetSnapshot().GetGeneration(), durable.GetGeneration())
	if knownLocal.GetGeneration() > committed {
		// Tracker state can be lost after a failed save and an atelet restart.
		// Absence alone does not prove a pending local checkpoint was detached.
		return apierror.FailedPrecondition("atelet on node %s cannot account for local snapshot generation %d", node, knownLocal.GetGeneration())
	}
	if err := applyAteletResponseState(ctx, status, state); err != nil {
		return err
	}
	for _, snap := range status.GetSnapshots() {
		if snap.GetLocality() == node {
			snap.Locality = ""
		}
	}
	for i, report := range []*ateletpb.SnapshotReport{state.GetLatestLocal(), state.GetUploading()} {
		if report == nil {
			continue
		}
		reported := report.GetSnapshot()
		snap := findSnapshotByGeneration(status.GetSnapshots(), reported.GetGeneration())
		// Suspend can drop an older upload's record while atelet finishes it.
		if snap == nil && i == 1 {
			continue
		}
		if snap == nil || snap.GetUuid() != reported.GetSnapshotUuid() ||
			snap.GetDurableSnapshot().GetObject().GetSnapshotUri() != reported.GetObject().GetSnapshotUri() {
			return apierror.FailedPrecondition("atelet on node %s retained an unrecognized snapshot: %v", node, reported)
		}
		if got := fidelityFromAtelet(reported.GetFidelity()); got == ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED || got != snap.GetFidelity() {
			return apierror.FailedPrecondition("atelet on node %s retained snapshot %s with unexpected fidelity", node, snap.GetUuid())
		}
		snap.Locality = node
	}
	return nil
}

// ensureInProgressSnapshotDiscarded removes uploads of checkpoints that never
// committed locally. A committed local snapshot's upload must continue, even
// while its durable status is still PENDING.
func (w *ActorWorkflow) ensureInProgressSnapshotDiscarded(ctx context.Context, actor *ateapi.Actor) (err error) {
	ctx, done := stepSpan(ctx, "DiscardInProgressSnapshot")
	defer func() { err = done(err) }()

	var inProgress []string
	for _, snap := range actor.GetStatus().GetSnapshots() {
		if snap.GetLocality() != "" {
			continue
		}
		storage := snap.GetDurableSnapshot()
		if storage.GetStatus() == ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING && storage.GetObject().GetSnapshotUri() != "" {
			inProgress = append(inProgress, storage.GetObject().GetSnapshotUri())
		}
	}
	switch {
	case w.objectStorePlugin == nil:
		markSkipped(ctx, "no object store configured")
		return nil
	case len(inProgress) == 0:
		markSkipped(ctx, "no in-progress snapshot recorded")
		return nil
	}

	// Check every URI before deleting any, so a corrupted record deletes nothing.
	owner := actorSnapshotOwner(actor)
	prefixes := make([]resources.StoragePrefix, 0, len(inProgress))
	for _, raw := range inProgress {
		uri, err := resources.ParseSnapshotURI(raw)
		if err != nil {
			return fmt.Errorf("while parsing the in-progress snapshot %q: %w", raw, err)
		}
		// A suspend records the in-progress URI under the actor's own prefix
		// before atelet writes the first object, so a URI owned by anything
		// else is a corrupted record.
		if !uri.OwnedBy(owner) {
			return fmt.Errorf("the in-progress snapshot %q is not owned by actor %s", raw, owner)
		}
		prefixes = append(prefixes, uri.Prefix())
	}
	// Only the abandoned snapshots go, not the actor's whole prefix: the
	// external snapshot the revert returns the actor to lives under it too.
	for _, prefix := range prefixes {
		if err := w.cleanupSnapshot(ctx, prefix); err != nil {
			return err
		}
	}
	return nil
}

// ensureRevertedFinalized commits SUSPENDED and drops every pointer to the
// execution that was discarded, in a single update, then releases the
// external snapshots the update dropped.
// It re-reads the actor first so an out-of-band transition is not overwritten,
// and so the version precondition guards what was actually observed.
func (w *ActorWorkflow) ensureRevertedFinalized(ctx context.Context, actorRef resources.ActorRef) (_ *ateapi.Actor, err error) {
	ctx, done := stepSpan(ctx, "FinalizeReverted")
	defer func() { err = done(err) }()

	latestActor, err := w.store.GetActor(ctx, actorRef)
	if err != nil {
		return nil, err
	}
	if got := latestActor.GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_REVERTING {
		return nil, apierror.FailedPrecondition("FinalizeReverted prerequisite not met for Actor: %s (got: %v, want %s)", actorRef, got, ateapi.ActorState_ACTOR_STATE_REVERTING)
	}

	storedActor, err := w.store.UpdateActor(ctx, actorRef, store.PreconditionFrom(latestActor), func(toUpdate *ateapi.Actor) error {
		toUpdate.Status.State = ateapi.ActorState_ACTOR_STATE_SUSPENDED
		toUpdate.Status.WorkerAssignment = nil
		local := findLatestLocalSnapshot(toUpdate.Status)
		if durable := findLatestDurableSnapshot(toUpdate.Status, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED); durable.GetGeneration() > local.GetGeneration() {
			local = nil
		}
		toUpdate.Status.AssignedNode = local.GetLocality()
		// Keep the last successful checkpoint, not a newer incomplete suspend.
		// LastAssignedGeneration stays monotonic even when that checkpoint is older.
		pruneSnapshots(toUpdate.Status, local)
		toUpdate.Status.Crash = nil
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		return nil, err
	}
	logActorStateChanged(ctx, storedActor, ateattr.OperationRevert)
	w.releaseDroppedSnapshots(ctx, latestActor, storedActor)
	return storedActor, nil
}
