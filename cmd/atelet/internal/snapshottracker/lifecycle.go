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
	"go.ate.dev/substrate/internal/proto/ateletpb"
)

// CommitLocal records snap, just stored locally under UPLOAD_MODE_DEFERRED, as
// the actor's latest local snapshot, and returns the actor's state.
func (t *Tracker) CommitLocal(actorUID string, snap SnapshotRef) *ateletpb.ActorSnapshotState {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a := t.findOrAddLocked(actorUID)
	a.LatestLocal = snap.clone()
	// TODO: Use succeededLocked here once UPLOAD_MODE_DEFERRED uploads are
	// implemented: a deferred checkpoint is then a successful end state, which
	// starts the actor's backoff over.
	t.saveLocked(a)
	t.requeueLocked(a)
	return t.stateLocked(a)
}

// CommitDurable records snap, uploaded during Checkpoint under
// UPLOAD_MODE_IMMEDIATE, as the actor's latest snapshot. The actor keeps no
// local snapshot. It returns the actor's state.
func (t *Tracker) CommitDurable(actorUID string, snap SnapshotRef) *ateletpb.ActorSnapshotState {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a := t.findOrAddLocked(actorUID)
	a.LatestDurable = snap.clone()
	// TODO: change this when we add detach.
	a.LatestLocal = nil
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
	a.LatestLocal = nil
	t.addLocked(a)
	t.saveLocked(a)
	t.requeueLocked(a)
}

// CommitPromoted records snap, the local snapshot PromoteSnapshot just
// uploaded, as the actor's latest snapshot, keeping the time its checkpoint
// committed. PromoteSnapshot then deletes the local copy, so the actor keeps
// no local snapshot. A retried promote of a snapshot already recorded keeps
// what was recorded. It returns the actor's state.
func (t *Tracker) CommitPromoted(actorUID string, snap SnapshotRef) *ateletpb.ActorSnapshotState {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a := t.findOrAddLocked(actorUID)
	uuid := snap.Snapshot.GetSnapshotUuid()
	if d := a.LatestDurable; d == nil || d.Snapshot.GetSnapshotUuid() != uuid {
		if l := a.LatestLocal; l != nil && l.Snapshot.GetSnapshotUuid() == uuid {
			snap.CommitTime = l.CommitTime
		}
		a.LatestDurable = snap.clone()
	}
	// TODO: change this when PromoteSnapshot keeps the local copy.
	a.LatestLocal = nil
	t.succeededLocked(a)
	return t.stateLocked(a)
}

// Forget drops the actor's state, in memory and on disk. It does not wait for
// the actor's background operation in progress: it cancels it, and the
// operation finds itself abandoned when it ends.
func (t *Tracker) Forget(actorUID string) error {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	t.abandonOpLocked(actorUID)
	t.resetBackoffLocked(actorUID)
	t.dropLocked(actorUID)
	return removeSnapshotState(actorUID)
}

// succeededLocked finishes a change that leaves the actor in a successful end
// state, such as its latest snapshot being durable: it starts the actor's
// backoff over, saves the actor's state, and queues the actor for its next
// background work.
func (t *Tracker) succeededLocked(a *actorSnapshots) {
	t.resetBackoffLocked(a.actorUID)
	t.saveLocked(a)
	t.requeueLocked(a)
}
