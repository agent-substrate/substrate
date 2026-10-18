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
	"slices"
	"time"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"google.golang.org/protobuf/proto"
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
// LatestDurable leaves the state as it is. It drops an older LatestLocal,
// which snap supersedes, so the Checkpoint's prune deletes it even if no local
// copy of snap is kept. The Checkpoint then records its local copy with
// CommitLocal, if it manages to keep one. An older snapshot's upload in
// progress runs on, and removes that snapshot's local copy when it ends (see
// removeSupersededLocalCopy).
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
	if a.LatestLocal != nil && a.LatestLocal.generation() < snap.generation() {
		a.clearLatestLocal()
	}
	t.cancelReportLocked(a)
	t.durableCommittedLocked(a)
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
// found durable already, as the actor's latest durable snapshot, keeping its
// local copy, the time its checkpoint committed, and any UploadTime on snap.
// It leaves DetachAt as it is. A promote of a snapshot already durable, or
// older than LatestDurable, keeps what was recorded. It returns the actor's
// state.
func (t *Tracker) CommitPromoted(actorUID string, snap SnapshotRef) *ateletpb.ActorSnapshotState {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a := t.findOrAddLocked(actorUID)
	// Skip when snap is already LatestDurable (so a retried promote keeps the
	// original CommitTime and UploadTime) or older than LatestDurable (a
	// newer checkpoint became durable while PromoteSnapshot ran).
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
	return t.stateLocked(a)
}

// BeginStart records that a Run or Restore is starting the actor on this node,
// once any detach in progress has finished, and returns the func to call when
// the start ends, whether or not it succeeded. It first unschedules the
// actor's detach (see markStarted) and saves that, so atelet does not detach
// the actor while a workload restored from its local snapshot may use it, even
// if atelet restarts or the start fails partway. A failed Run or Restore
// leaves the detach unscheduled, since atelet cannot tell whether the workload
// is running on the worker; the local snapshot stays until the next checkpoint
// or Terminate on this node. When it cannot save that, it fails with
// Unavailable, and the caller must not start the actor.
func (t *Tracker) BeginStart(ctx context.Context, actorUID string) (end func(), err error) {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a, err := t.waitForDetachLocked(ctx, actorUID, nil)
	if err != nil {
		return nil, err
	}
	if a != nil {
		// markStarted clears DetachAt, and reports whether it was set:
		// only then is there a change to save. If the save fails, put
		// DetachAt and unsaved back as they were, matching the state file,
		// so the detach stays scheduled.
		if detachAt, unsaved := a.DetachAt, a.unsaved; a.markStarted() {
			if err := t.writeStateLocked(a); err != nil {
				a.DetachAt, a.unsaved = detachAt, unsaved
				return nil, apierror.Unavailable("while unscheduling the detach of actor %s before starting it: %v", actorUID, err)
			}
		}
		// No detach is scheduled now, so failed detaches have nothing left
		// to retry. Keep the backoff when an upload is still due.
		if _, ok := t.uploadDueLocked(a); !ok {
			t.resetBackoffLocked(a)
		}
	}
	return t.holdLocked(actorUID, a, nil, false), nil
}

// BeginCheckpoint records that a Checkpoint of snap is in progress on this
// node, once any detach in progress has finished, and returns the func to call
// when the checkpoint ends, whether or not it succeeded. It fails with
// FailedPrecondition when snap has a stale generation for the actor: below the
// actor's newest snapshot on this node, or equal to it under another
// snapshot_uuid. atelet does not detach the actor in between, so a detach
// cannot delete the local snapshot the checkpoint writes.
func (t *Tracker) BeginCheckpoint(ctx context.Context, actorUID string, snap *ateletpb.Snapshot) (end func(), err error) {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a, err := t.waitForDetachLocked(ctx, actorUID, func(a *actorSnapshots) error {
		return a.checkGeneration(snap)
	})
	if err != nil {
		return nil, err
	}
	// Unlike BeginStart, leave DetachAt as it is. Either it is already
	// zero, because the actor's Run or Restore on this node unscheduled the
	// detach, or this is a retry of a Checkpoint that already committed its
	// snapshot, whose detach stays scheduled.
	return t.holdLocked(actorUID, a, &SnapshotRef{Snapshot: proto.CloneOf(snap)}, false), nil
}

// BeginPromote records that a PromoteSnapshot of snap is in progress on this
// node, once any detach in progress and any background upload of snap have
// finished. It fails with FailedPrecondition when snap has a stale generation
// for the actor or conflicts with LatestDurable. It reports whether snap is
// already the actor's latest durable snapshot, and returns the func to call
// when the promote ends. atelet does not start a detach or a background upload
// of the actor in between.
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
	return alreadyDurable, t.holdLocked(actorUID, a, snap.clone(), true), nil
}

// holdLocked counts a Run, Restore, Checkpoint, or PromoteSnapshot of the
// actor in progress on its entry a, which holds detaches and state reports of
// the actor off (and background uploads when promote is true), and returns the
// func that ends it and queues the actor again. When snap is non-nil
// (Checkpoint and PromoteSnapshot), it stays in a.inFlight until end runs, so
// newest includes it. An actor atelet tracks nothing of yet (a nil a) is held
// too, so no detach or report can begin on a snapshot its first Checkpoint
// commits before it ends.
func (t *Tracker) holdLocked(actorUID string, a *actorSnapshots, snap *SnapshotRef, promote bool) (end func()) {
	if a == nil {
		a = &actorSnapshots{actorUID: actorUID}
	}
	t.addLocked(a)
	a.holds++
	if snap != nil {
		a.inFlight = append(a.inFlight, snap)
	}
	if promote {
		a.promotes++
	}
	return func() {
		t.lock(actorUID)
		defer t.unlock(actorUID)
		a.holds--
		if snap != nil {
			a.inFlight = slices.DeleteFunc(a.inFlight, func(r *SnapshotRef) bool { return r == snap })
		}
		if promote {
			a.promotes--
		}
		if !t.tracksLocked(a) {
			// Forget (via Terminate) dropped the actor while the Run, Restore,
			// Checkpoint, or PromoteSnapshot was in progress.
			return
		}
		if a.holds == 0 && a.reportDeferred {
			t.queueReportLocked(a)
		}
		t.requeueLocked(a)
	}
}

// Forget drops the actor's state, in memory and on disk. It does not wait for
// the actor's operation in progress, an upload or a detach: it cancels it, and
// the operation finds itself abandoned when it ends.
func (t *Tracker) Forget(actorUID string) error {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	t.abandonOpLocked(actorUID)
	// Start the actor's backoff over; its entry, and any hold, goes below.
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

// succeededLocked finishes a change that leaves the actor in a successful end
// state, such as a background upload or detach: it starts the actor's backoff
// over, saves the actor's state, and queues the actor for its next background
// work.
func (t *Tracker) succeededLocked(a *actorSnapshots) {
	t.resetBackoffLocked(a)
	t.saveLocked(a)
	t.requeueLocked(a)
}
