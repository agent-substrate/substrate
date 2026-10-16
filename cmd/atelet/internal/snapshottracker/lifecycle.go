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

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/proto/ateletpb"
)

// CommitLocal records snap, just stored in the actor's local checkpoint
// directory by a Checkpoint in either upload mode, as the actor's latest local
// snapshot, schedules the actor's detach, and returns the actor's state.
func (t *Tracker) CommitLocal(actorUID string, snap SnapshotRef) *ateletpb.ActorSnapshotState {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a := t.findOrAddLocked(actorUID)
	t.setLatestLocalLocked(a, snap)
	// TODO: Use succeededLocked here once UPLOAD_MODE_DEFERRED uploads are
	// implemented: a deferred checkpoint is then a successful end state, which
	// starts the actor's backoff over.
	t.saveLocked(a)
	t.requeueLocked(a)
	return t.stateLocked(a)
}

// CommitDurable records snap, just uploaded by a Checkpoint under
// UPLOAD_MODE_IMMEDIATE, as the actor's latest durable snapshot, and returns
// the actor's state. The Checkpoint then records its local copy with
// CommitLocal, if it manages to keep one.
func (t *Tracker) CommitDurable(actorUID string, snap SnapshotRef) *ateletpb.ActorSnapshotState {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a := t.findOrAddLocked(actorUID)
	a.LatestDurable = snap.clone()
	t.succeededLocked(a)
	return t.stateLocked(a)
}

// ForgetLocal drops the actor's latest local snapshot. Call it before deleting
// the actor's local checkpoints, so the state never names a deleted snapshot,
// even if the caller then fails.
func (t *Tracker) ForgetLocal(actorUID string) {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a := t.findLocked(actorUID)
	if a == nil || a.LatestLocal == nil {
		return
	}
	t.clearLatestLocalLocked(a)
	t.addLocked(a)
	t.saveLocked(a)
	t.requeueLocked(a)
}

// IsDurable reports whether snap is recorded as the actor's latest durable
// snapshot. It fails with FailedPrecondition when LatestDurable has snap's
// UUID with a different generation or URI.
func (t *Tracker) IsDurable(actorUID string, snap SnapshotRef) (bool, error) {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a := t.findLocked(actorUID)
	if a == nil {
		return false, nil
	}
	return a.isLatestDurable(snap)
}

// CommitPromoted records snap, a local snapshot PromoteSnapshot made durable or
// found durable already, as the actor's latest durable snapshot, keeping its
// local copy and the time its checkpoint committed. It leaves DetachAt as it
// is. A retried promote of a snapshot already recorded keeps what was
// recorded. It returns the actor's state.
func (t *Tracker) CommitPromoted(actorUID string, snap SnapshotRef) *ateletpb.ActorSnapshotState {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a := t.findOrAddLocked(actorUID)
	uuid := snap.Snapshot.GetSnapshotUuid()
	if d := a.LatestDurable; d == nil || d.Snapshot.GetSnapshotUuid() != uuid {
		if l := a.LatestLocal; l != nil && l.Snapshot.GetSnapshotUuid() == uuid {
			snap.CommitTime = l.CommitTime
			// The local copy takes the promoted Snapshot, which names where
			// it is durable.
			a.setLatestLocal(snap, a.DetachAt)
		}
		a.LatestDurable = snap.clone()
	}
	t.succeededLocked(a)
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
	// Every operation is a detach.
	a, err := t.waitIdleLocked(ctx, actorUID, nil)
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
		// No detach is scheduled now, so the actor's failed detaches have
		// nothing left to retry.
		t.resetBackoffLocked(a)
	}
	return t.holdLocked(actorUID, a), nil
}

// BeginCheckpoint records that a Checkpoint of the actor is in progress on this
// node, once any detach or earlier hold in progress has finished, and returns
// the func to call when the checkpoint ends, whether or not it succeeded.
// atelet does not detach the actor or run a second checkpoint of it in between,
// so neither can delete or overwrite the local snapshot the checkpoint writes.
func (t *Tracker) BeginCheckpoint(ctx context.Context, actorUID string) (end func(), err error) {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	for {
		// Every operation is a detach.
		a, err := t.waitIdleLocked(ctx, actorUID, nil)
		if err != nil {
			return nil, err
		}
		if a == nil || a.holds == 0 {
			// Unlike BeginStart, leave DetachAt as it is. Either it is already
			// zero, because the actor's Run or Restore on this node unscheduled
			// the detach, or this is a retry of a Checkpoint that already
			// committed its snapshot, whose detach stays scheduled.
			return t.holdLocked(actorUID, a), nil
		}
		if err := t.waitOpLocked(ctx, actorUID, a.holdDone); err != nil {
			return nil, err
		}
	}
}

// holdLocked counts a Run, Restore, or Checkpoint of the actor in progress on
// its entry a, which holds detaches of the actor off, and returns the func that
// ends it and queues the actor again, for a detach that came due meanwhile. An
// actor atelet tracks nothing of yet (a nil a) is held too, so no detach can
// begin on a snapshot its first Checkpoint commits before it ends.
func (t *Tracker) holdLocked(actorUID string, a *actorSnapshots) (end func()) {
	if a == nil {
		a = &actorSnapshots{actorUID: actorUID}
	}
	t.addLocked(a)
	if a.holds == 0 {
		a.holdDone = make(chan struct{})
	}
	a.holds++
	return func() {
		t.lock(actorUID)
		defer t.unlock(actorUID)
		a.holds--
		if a.holds == 0 {
			close(a.holdDone)
			a.holdDone = nil
		}
		if !t.tracksLocked(a) {
			// Forget (via Terminate) dropped the actor while the Run, Restore,
			// or Checkpoint was in progress.
			return
		}
		t.requeueLocked(a)
	}
}

// Forget drops the actor's state, in memory and on disk. It does not wait for
// the actor's detach in progress: it cancels it, and the detach finds itself
// abandoned when it ends.
func (t *Tracker) Forget(actorUID string) error {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	t.abandonOpLocked(actorUID)
	// Start the actor's backoff over; its entry, and any hold, goes below.
	t.retries.Forget(actorUID)
	t.dropLocked(actorUID)
	return removeSnapshotState(actorUID)
}

// setLatestLocalLocked records snap as the actor's latest local snapshot and
// schedules the actor's detach a detach delay from now.
func (t *Tracker) setLatestLocalLocked(a *actorSnapshots, snap SnapshotRef) {
	a.setLatestLocal(snap, t.clock.Now().Add(t.detachDelay))
}

// clearLatestLocalLocked records that atelet holds no local snapshot of the
// actor, and lifts the backoff of its failed detaches, which have nothing left
// to retry.
func (t *Tracker) clearLatestLocalLocked(a *actorSnapshots) {
	a.clearLatestLocal()
	t.resetBackoffLocked(a)
}

// succeededLocked finishes a change that leaves the actor in a successful end
// state, such as its latest snapshot being durable: it starts the actor's
// backoff over, saves the actor's state, and queues the actor for its next
// background work.
func (t *Tracker) succeededLocked(a *actorSnapshots) {
	t.resetBackoffLocked(a)
	t.saveLocked(a)
	t.requeueLocked(a)
}
