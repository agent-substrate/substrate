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

package snapshottracker

import (
	"context"
	"time"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/proto/ateletpb"
)

// CommitLocal records snap, just stored in the actor's local checkpoint
// directory by a Checkpoint in either upload mode, as the actor's latest local
// snapshot, schedules the actor's detach and, if snap needs an upload (see
// uploadDue), its upload, and returns the actor's state. A commit of a
// snapshot already recorded or superseded by a newer committed snapshot leaves
// the state as it is. An older snapshot's upload in progress runs on, but an
// Uploading that keepUploading no longer keeps is dropped. The Checkpoint then
// prunes the local snapshots LocalSnapshotsToKeep no longer names.
func (t *Tracker) CommitLocal(actorUID string, snap SnapshotRef) *ateletpb.ActorSnapshotState {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a := t.findOrAddLocked(actorUID)
	if !a.localNeeded(snap) {
		if a.reportDue() {
			t.clearReportDueLocked(a)
		} else {
			t.requeueLocked(a)
		}
		return t.stateLocked(a)
	}
	t.setLatestLocalLocked(a, snap)
	a.Started = false
	if a.UndurableSince.IsZero() && a.uploadNeeded(snap) {
		a.UndurableSince = snap.CommitTime
	}
	// Drop a failed upload that snap replaces in this save rather than leave
	// it for a worker: keepUploading tells it apart by uploadingFailed, which
	// the state file does not keep, so an atelet restart would resume it from
	// a local copy the Checkpoint's prune deletes.
	a.clearSupersededUploading()
	// Start the backoff over unless Uploading is still kept and snap is not
	// the next upload target: Uploading's retry then still waits it out.
	if !a.keepUploading() || sameUUID(a.nextUploadTarget(), &snap) {
		t.resetBackoffLocked(a)
	}
	t.cancelReportLocked(a)
	t.saveLocked(a)
	t.requeueLocked(a)
	return t.stateLocked(a)
}

// CommitDurable records snap, just uploaded by a Checkpoint under
// UPLOAD_MODE_IMMEDIATE, as the actor's latest durable snapshot, and returns
// the actor's state. A commit of a snapshot already durable or older than
// LatestDurable leaves the state as it is.
//
// It drops an older LatestLocal, which snap supersedes, so the Checkpoint's
// prune deletes it even if no local copy of snap is kept. The Checkpoint then
// records its local copy with CommitLocal, if it manages to keep one.
//
// Once it records snap, it returns only after any upload of an older snapshot
// has stopped (see durableStateLocked).
func (t *Tracker) CommitDurable(actorUID string, snap SnapshotRef) *ateletpb.ActorSnapshotState {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a := t.findOrAddLocked(actorUID)
	if !a.uploadNeeded(snap) {
		if a.reportDue() {
			t.clearReportDueLocked(a)
		} else {
			t.requeueLocked(a)
		}
		return t.stateLocked(a)
	}
	a.LatestDurable = snap.clone()
	a.Started = false
	if a.LatestLocal != nil && a.LatestLocal.generation() < snap.generation() {
		a.clearLatestLocal()
	}
	t.cancelReportLocked(a)
	t.durableCommittedLocked(a)
	return t.durableStateLocked(a)
}

// CommittedCheckpointState returns the actor's state for a Checkpoint whose
// snapshot atelet committed already, and cancels any pending background upload
// report while a local snapshot is kept. Once LatestLocal is nil, a pending
// detach report stays due so the actor's state file and directory are removed
// once the detach report is acknowledged.
func (t *Tracker) CommittedCheckpointState(actorUID string) *ateletpb.ActorSnapshotState {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a := t.findLocked(actorUID)
	if a == nil {
		return &ateletpb.ActorSnapshotState{}
	}
	if a.LatestLocal != nil && a.reportDue() {
		t.clearReportDueLocked(a)
	}
	return t.stateLocked(a)
}

// LocalSnapshotsToKeep returns the actor's local snapshots atelet still needs:
// LatestLocal, and Uploading while a background upload is reading it or will
// retry it. A Checkpoint prunes every other local snapshot once it has
// committed its own. Until the next commit, a background upload only reads one
// of these, so the prune never deletes a snapshot an upload needs.
func (t *Tracker) LocalSnapshotsToKeep(actorUID string) []string {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a := t.findLocked(actorUID)
	if a == nil {
		return nil
	}
	var keep []string
	if a.LatestLocal != nil {
		keep = append(keep, a.LatestLocal.uuid())
	}
	if a.keepUploading() && !sameUUID(a.Uploading, a.LatestLocal) {
		keep = append(keep, a.Uploading.uuid())
	}
	return keep
}

// CommitPromoted records snap, a local snapshot PromoteSnapshot made durable or
// found durable already, as the actor's latest durable snapshot, and returns
// the actor's state. A promote of a snapshot already durable, or older than
// LatestDurable, keeps what was recorded.
//
// It keeps snap's local copy, the time its checkpoint committed, and any
// UploadTime on snap. It leaves DetachAt as it is.
//
// Once it records snap, it returns only after any upload of an older snapshot
// has stopped (see durableStateLocked).
func (t *Tracker) CommitPromoted(actorUID string, snap SnapshotRef) *ateletpb.ActorSnapshotState {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a := t.findOrAddLocked(actorUID)
	// Skip when snap is already LatestDurable (so a retried promote keeps the
	// original CommitTime and UploadTime) or older than LatestDurable.
	if !a.uploadNeeded(snap) {
		if a.LatestLocal != nil && a.reportDue() {
			t.clearReportDueLocked(a)
		} else {
			t.requeueLocked(a)
		}
		return t.stateLocked(a)
	}
	// Carry over the checkpoint commit time when promoting the tracked
	// local snapshot. snap.UploadTime is already set by the caller when
	// it uploaded the snapshot.
	if sameUUID(a.LatestLocal, &snap) {
		snap.CommitTime = a.LatestLocal.CommitTime
	}
	a.LatestDurable = snap.clone()
	// A promote with a local snapshot still on the node returns the latest
	// state to the control plane. Once LatestLocal is nil, a pending detach
	// report stays due, since the control plane does not read a promote's
	// response as a detach.
	if a.LatestLocal != nil {
		t.cancelReportLocked(a)
	}
	t.durableCommittedLocked(a)
	return t.durableStateLocked(a)
}

// BeginStart records that a Run or Restore is starting the actor on this node,
// once any background detach of the actor in progress has finished, and
// returns the func to call when the start ends, whether or not it succeeded.
//
// It first records the start (see markStarted) and saves it, so even if
// atelet restarts or the start fails partway:
//   - atelet does not detach the actor while a workload restored from its
//     local snapshot may use it.
//   - A retried Checkpoint does not reset the new activation's directories
//     and volumes.
//
// A failed Run or Restore leaves the detach unscheduled, since atelet cannot
// tell whether the workload is running on the worker. The local snapshot stays
// until the next checkpoint or Terminate on this node.
//
// When it cannot save the start, it fails with Unavailable, and the caller
// must not start the actor.
func (t *Tracker) BeginStart(ctx context.Context, actorUID string) (end func(), err error) {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a, err := t.waitForDetachLocked(ctx, actorUID, nil)
	if err != nil {
		return nil, err
	}
	if a != nil {
		// markStarted sets Started and clears DetachAt, and reports whether
		// either changed: only then is there a change to save. If the save
		// fails, put them and unsaved back as they were, matching the state
		// file, so the detach stays scheduled.
		if detachAt, started, unsaved := a.DetachAt, a.Started, a.unsaved; a.markStarted() {
			if err := t.writeStateLocked(a); err != nil {
				a.DetachAt, a.Started, a.unsaved = detachAt, started, unsaved
				return nil, apierror.Unavailable("while recording the start of actor %s: %v", actorUID, err)
			}
		}
		// No detach is scheduled now, so failed detaches have nothing left
		// to retry. Keep the backoff when an upload is still due.
		if _, ok := t.uploadDueLocked(a); !ok {
			t.resetBackoffLocked(a)
		}
	}
	return t.holdLocked(actorUID, a, holdStart), nil
}

// BeginCheckpoint records that a Checkpoint of snap under mode is in progress
// on this node, once any background detach in progress has finished, and
// returns the func to call when the checkpoint ends, whether or not it
// succeeded.
//
// atelet does not detach the actor in between, so a detach cannot delete the
// local snapshot the checkpoint writes. Under UPLOAD_MODE_IMMEDIATE, atelet
// starts no background upload in between either, since CommitDurable would
// only cancel it; one already running goes on.
//
// It reports whether snap is committed already, as the actor's latest local
// or durable snapshot. The Checkpoint is then a retry of one that committed
// snap and deleted the workload.
//
// It fails with:
//   - Aborted when snap has a stale generation for the actor: below the
//     actor's newest snapshot on this node, or equal to it under another
//     snapshot_uuid.
//   - FailedPrecondition when snap is committed under its snapshot_uuid but
//     named differently.
func (t *Tracker) BeginCheckpoint(ctx context.Context, actorUID string, snap *ateletpb.Snapshot, mode ateletpb.UploadMode) (committed bool, end func(), err error) {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a, err := t.waitForDetachLocked(ctx, actorUID, func(a *actorSnapshots) error { return a.checkGeneration(snap) })
	if err != nil {
		return false, nil, err
	}
	if a != nil {
		if committed, err = a.checkpointCommitted(snap); err != nil {
			return false, nil, err
		}
	}
	kind := holdCheckpoint
	if mode == ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE {
		kind = holdDurable
	}
	// Unlike BeginStart, leave DetachAt as it is. Either it is already
	// zero, because the actor's Run or Restore on this node unscheduled the
	// detach, or snap is committed already, and its detach stays scheduled.
	return committed, t.holdLocked(actorUID, a, kind), nil
}

// NotStartedSince reports whether snap is committed, as LatestLocal or
// LatestDurable, and no Run or Restore of the actor has begun on this node
// since the latest Checkpoint commit (see Started).
func (t *Tracker) NotStartedSince(actorUID string, snap *ateletpb.Snapshot) bool {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a := t.findLocked(actorUID)
	if a == nil || a.Started {
		return false
	}
	committed, err := a.checkpointCommitted(snap)
	return err == nil && committed
}

// BeginPromote records that a PromoteSnapshot of snap is in progress on this
// node, once any background detach in progress and any background upload of
// snap have finished. A stale generation fails with Aborted. A conflicting
// description of LatestDurable fails with FailedPrecondition. It reports
// whether snap is already the actor's latest durable snapshot, and returns the
// func to call when the promote ends. atelet does not start a detach or a
// background upload of the actor in between.
func (t *Tracker) BeginPromote(ctx context.Context, actorUID string, snap SnapshotRef) (alreadyDurable bool, end func(), err error) {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a, err := t.waitForDetachOrSnapshotUploadLocked(ctx, actorUID, snap)
	if err != nil {
		return false, nil, err
	}
	if a != nil {
		alreadyDurable, err = a.isLatestDurable(snap)
		if err != nil {
			return false, nil, err
		}
	}
	return alreadyDurable, t.holdLocked(actorUID, a, holdDurable), nil
}

// BeginTerminate records that a Terminate of the actor is in progress on this
// node, and returns the func to call when the terminate ends, whether or not it
// succeeded. Until then, atelet starts no background work on the actor; Forget
// cancels an upload or detach already in progress. If the terminate fails
// before Forget, its end lets the actor's background work resume.
func (t *Tracker) BeginTerminate(actorUID string) (end func()) {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	return t.holdLocked(actorUID, t.findLocked(actorUID), holdTerminate)
}

// holdLocked records a Run, Restore, Checkpoint, PromoteSnapshot, or Terminate
// of the actor in progress on its entry a, which holds detaches and state
// reports of the actor off (and background uploads when kind.blocksUploads),
// and returns the func that ends it and queues the actor again, unless Forget
// has dropped the actor meanwhile. An actor atelet tracks nothing of yet (a
// nil a) is held too, so no detach or report can begin on a snapshot its first
// Checkpoint commits before it ends.
func (t *Tracker) holdLocked(actorUID string, a *actorSnapshots, kind holdKind) (end func()) {
	if a == nil {
		a = &actorSnapshots{actorUID: actorUID}
	}
	t.addLocked(a)
	prev := a.hold
	a.hold = kind
	return func() {
		t.lock(actorUID)
		defer t.unlock(actorUID)
		if t.findLocked(actorUID) != a {
			// Forget dropped the actor, as a Terminate does while it holds
			// the actor.
			return
		}
		a.hold = prev
		if a.hold != holdNone {
			// The outer Checkpoint's end queues the actor.
			return
		}
		if a.reportDeferred {
			t.queueReportLocked(a)
		}
		t.requeueLocked(a)
	}
}

// Forget drops the actor's state, in memory and on disk. It first cancels any
// background upload or detach of the actor in progress and waits for it to
// end, releasing the actor's lock meanwhile.
func (t *Tracker) Forget(actorUID string) error {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	if a := t.findLocked(actorUID); a != nil {
		t.addLocked(a)
		a.hold = holdTerminate
		for a.op != nil {
			op := a.op
			op.cancel()
			t.unlock(actorUID)
			<-op.done
			t.lock(actorUID)
		}
	}
	t.retries.Forget(actorUID)
	t.reporting.retries.Forget(actorUID)
	t.dropLocked(actorUID)
	t.undurableActorsTally.forget(actorUID)
	return removeSnapshotState(actorUID)
}

// setLatestLocalLocked records snap as the actor's latest local snapshot and
// schedules the actor's detach a detach delay from now.
func (t *Tracker) setLatestLocalLocked(a *actorSnapshots, snap SnapshotRef) {
	a.setLatestLocal(snap, t.clock.Now().Add(t.detachDelay))
}

// clearLatestLocalLocked records that atelet holds no local snapshot of the
// actor, as after Detach pruned its local checkpoints, zeroes UndurableSince,
// and lifts the backoff of its failed background work, which has nothing left
// to retry.
func (t *Tracker) clearLatestLocalLocked(a *actorSnapshots) {
	a.Uploading = nil
	a.uploadingFailed = false
	a.UndurableSince = time.Time{}
	a.clearLatestLocal()
	t.resetBackoffLocked(a)
}

// durableCommittedLocked finishes CommitDurable or CommitPromoted once
// LatestDurable is updated. When LatestLocal is now durable too (or nil), it
// clears UndurableSince and lifts any retry backoff; when a newer LatestLocal
// still needs uploading, its UndurableSince and backoff stay in place.
func (t *Tracker) durableCommittedLocked(a *actorSnapshots) {
	if a.LatestLocal == nil || !a.uploadNeeded(*a.LatestLocal) {
		a.UndurableSince = time.Time{}
		t.resetBackoffLocked(a)
	}
	t.saveLocked(a)
	t.requeueLocked(a)
}

// durableStateLocked returns the actor's state for CommitDurable or
// CommitPromoted to return once they record a new LatestDurable. Once the
// control plane learns of it, it may delete the objects of older snapshots, so
// this first cancels any upload of an older snapshot in progress and waits for
// it to end, releasing the actor's lock meanwhile. The object-store plugin may
// still finish an object that upload had under way; deleting the actor
// collects it.
func (t *Tracker) durableStateLocked(a *actorSnapshots) *ateletpb.ActorSnapshotState {
	for op := a.supersededUpload(); op != nil; op = a.supersededUpload() {
		op.cancel()
		t.unlock(a.actorUID)
		// The upload ends by updating a, the entry it began on.
		<-op.done
		t.lock(a.actorUID)
	}
	return t.stateLocked(a)
}

// succeededLocked finishes a change that leaves the actor in a successful end
// state, such as a background upload or detach: it starts the actor's backoff
// over, saves the actor's state, and queues the actor for its next background
// work.
func (t *Tracker) succeededLocked(a *actorSnapshots) {
	t.resetBackoffLocked(a)
	t.saveLocked(a)
	t.requeueLocked(a)
}
