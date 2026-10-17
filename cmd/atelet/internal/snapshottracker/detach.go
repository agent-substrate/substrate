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

// beginDetachLocked begins a detach of the actor's latest local snapshot,
// under a context derived from ctx, and returns the func that finishes it
// once the caller releases the actor's lock. It begins nothing, and fails,
// when no detach can make the snapshot durable (see detachTarget).
func (t *Tracker) beginDetachLocked(ctx context.Context, a *actorSnapshots, background bool) (func() (*ateletpb.ActorSnapshotState, error), error) {
	target, alreadyDurable, err := a.detachTarget()
	if err != nil {
		return nil, err
	}
	opCtx, op := t.beginOpLocked(ctx, a, opDetach, target)
	return func() (*ateletpb.ActorSnapshotState, error) {
		return t.finishDetach(opCtx, a.actorUID, op, alreadyDurable, background)
	}, nil
}

// Detach makes the actor's latest local snapshot durable, then deletes the
// actor's local snapshots. It is a no-op when atelet holds no local snapshot
// for the actor. It fails with Aborted while a Run, Restore, Checkpoint, or
// PromoteSnapshot of the actor is in progress on this node, or once a Run or
// Restore of the actor has begun since its latest local snapshot, and with
// FailedPrecondition when no detach can make the snapshot durable. It runs in
// the calling goroutine rather than through the background queue, once any
// operation in progress on the actor has ended.
func (t *Tracker) Detach(ctx context.Context, actorUID string) (*ateletpb.ActorSnapshotState, error) {
	if t.store == nil {
		return nil, apierror.Unimplemented("atelet has no snapshot store to detach actors with")
	}
	t.lock(actorUID)
	// Wait for any operation in progress (from a background worker or
	// another RPC call) to end first: a detach may leave nothing to detach,
	// and an upload of the latest local snapshot nothing to upload.
	a, err := t.waitIdleLocked(ctx, actorUID, checkNotHeld)
	if err != nil {
		t.unlock(actorUID)
		return nil, err
	}
	if a == nil || a.LatestLocal == nil {
		// No local snapshot is stored on this node, so there is no work to be
		// done.
		st := &ateletpb.ActorSnapshotState{}
		if a != nil {
			if a.reportDue() {
				t.clearReportDueLocked(a)
			}
			st = t.stateLocked(a)
		}
		t.unlock(actorUID)
		return st, nil
	}
	if !a.detachScheduled() {
		// A Run or Restore of the actor has begun since its latest local
		// snapshot, which a workload restored from it may still use.
		t.unlock(actorUID)
		return nil, apierror.Aborted("actor %s is in use on this node", actorUID)
	}
	finish, err := t.beginDetachLocked(ctx, a, false)
	t.unlock(actorUID)
	if err != nil {
		return nil, err
	}
	return finish()
}

// checkNotHeld fails with Aborted while a Run, Restore, Checkpoint, or
// PromoteSnapshot of the actor is in progress on this node.
func checkNotHeld(a *actorSnapshots) error {
	if a.holds > 0 {
		return apierror.Aborted("actor %s is in use on this node", a.actorUID)
	}
	return nil
}

// finishDetach runs op, a detach of the actor's latest local snapshot: it
// uploads the snapshot unless already durable, then deletes the actor's local
// snapshots. background reports whether a worker runs it, rather than a Detach
// call; a worker's detach makes a report of what it changed due, while a
// Detach call returns the new state and cancels any pending report. A detach
// Forget abandons leaves the actor alone, and fails.
func (t *Tracker) finishDetach(ctx context.Context, actorUID string, op *actorOp, alreadyDurable, background bool) (*ateletpb.ActorSnapshotState, error) {
	defer close(op.done)
	uploaded := false
	target := op.target
	detachStart := t.clock.Now()
	var err error
	if !alreadyDurable {
		err = ctx.Err()
		if err == nil {
			uploaded, _, err = t.uploadSnapshot(ctx, actorUID, target, background)
		}
		if err == nil {
			err = t.updateOp(actorUID, op, nil, func(a *actorSnapshots) {
				if t.recordUploadLocked(a, target, uploaded) && background {
					t.markReportDueLocked(a)
				}
			})
		}
	}

	// The upload is recorded before anything is deleted, and LatestLocal is
	// cleared only once its files are gone. After a crash or a failed prune in
	// between, the detach is retried, finds the snapshot durable, and only
	// prunes. Until then LatestLocal can name a half-deleted copy, which is
	// safe because the snapshot is durable: Restore reads snapshot.object
	// when the local copy is unusable. The prune runs with op in progress
	// (before op.done is closed), so Run, Restore, and Checkpoint wait for it.
	// TODO: Notify ateapi of the upcoming detach before deleting the actor's
	// local snapshots, so it stops sending Restores that rely on them.
	recordDetach := err == nil
	if recordDetach {
		detachStart = t.clock.Now()
		// Forget or the caller may have ended ctx as the upload finished.
		err = ctx.Err()
	}
	if err == nil {
		err = PruneLocalCheckpoints(ctx, actorUID)
	}
	if abErr := t.updateOp(actorUID, op, err, func(a *actorSnapshots) {
		switch {
		case err == nil:
			t.clearLatestLocalLocked(a)
			if background {
				t.markReportDueLocked(a)
			} else {
				t.cancelReportLocked(a)
			}
		case ctx.Err() == nil:
			// Detach again once the retry delay has passed, and no sooner
			// than the detach was due.
			a.DetachAt = later(a.DetachAt, t.backOffLocked(a))
		}
	}); abErr != nil {
		err = abErr
	}

	// Log and record before endOp, which ends ctx.
	elapsed := t.clock.Since(detachStart)
	logOp(ctx, actorUID, op.kind, target, uploaded, background, elapsed, err)
	if background && recordDetach {
		t.instruments.recordDetach(ctx, target, elapsed, err)
	}
	return t.endOp(actorUID, op, err)
}
