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

	"go.ate.dev/substrate/cmd/atelet/internal/actordirs"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/proto/ateletpb"
)

// beginDetachLocked begins a detach of the actor's latest local snapshot,
// under a context derived from ctx, and returns its op and the func that runs
// it once the caller releases the actor's lock. It begins nothing, and fails,
// when no detach can make the snapshot durable (see detachTarget).
func (t *Tracker) beginDetachLocked(ctx context.Context, a *actorSnapshots, background bool) (*actorOp, func(), error) {
	target, alreadyDurable, err := a.detachTarget()
	if err != nil {
		return nil, nil, err
	}
	opCtx, op := t.beginOpLocked(ctx, a, opDetach, target)
	return op, func() {
		defer close(op.done)
		op.state, op.err = t.finishDetach(opCtx, a.actorUID, op, alreadyDurable, background)
	}, nil
}

// Detach makes the actor's latest local snapshot durable, then deletes the
// actor's local snapshots and its files on this node (keeping
// snapshot-state.json until the detach is reported). It is a no-op when atelet
// holds no local snapshot for the actor. It fails with Aborted once a Run or
// Restore of the actor has begun since its latest local snapshot, and with
// FailedPrecondition when no detach can make the snapshot durable. It starts
// once the actor's operation in progress ends, and runs on the tracker's
// context, not ctx. If ctx ends first, Detach returns ctx's error and the
// detach runs on, recorded by its report. After Wait begins, Detach fails with
// Unavailable. Callers serialize it with the actor's other lifecycle RPCs (see
// Tracker).
func (t *Tracker) Detach(ctx context.Context, actorUID string) (*ateletpb.ActorSnapshotState, error) {
	if t.store == nil {
		return nil, apierror.Unimplemented("atelet has no snapshot store to detach actors with")
	}
	t.lock(actorUID)
	// Wait for any operation in progress (from a background worker or
	// another RPC call) to end first: a detach may leave nothing to detach,
	// and an upload of the latest local snapshot nothing to upload.
	a, err := t.waitIdleLocked(ctx, actorUID)
	if err != nil {
		t.unlock(actorUID)
		return nil, err
	}
	if a == nil || a.LatestLocal == nil {
		// No local snapshot is stored on this node, so there is no work to be
		// done. A report already due stays due, in case the caller doesn't
		// record this state, and so a retried DetachActor call still sees
		// LatestDurable until the report is acknowledged. When the actor has
		// not started and no report is due (as after a report gave up or was
		// refused with an error other than NotFound), make a report due again
		// so the next acknowledgement removes snapshot-state.json and the
		// actor directory.
		st := &ateletpb.ActorSnapshotState{}
		if a != nil {
			st = t.stateLocked(a)
			if !a.Started && !a.reportDue() && a.LatestDurable != nil {
				t.addLocked(a)
				t.markReportDueLocked(a)
				t.saveLocked(a)
				t.queueReportLocked(a)
				t.requeueLocked(a)
			}
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
	// Under foregroundMu, so no detach joins workersDone after Wait begins.
	t.foregroundMu.Lock()
	if t.waiting || t.ctx.Err() != nil {
		t.foregroundMu.Unlock()
		t.unlock(actorUID)
		return nil, apierror.Unavailable("atelet is shutting down")
	}
	op, run, err := t.beginDetachLocked(t.ctx, a, false)
	if err != nil {
		t.foregroundMu.Unlock()
		t.unlock(actorUID)
		return nil, err
	}
	t.workersDone.Go(run)
	t.foregroundMu.Unlock()
	t.unlock(actorUID)

	select {
	case <-op.done:
		return op.state, t.detachCallError(ctx, op.err)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// detachCallError returns err, or Unavailable if the tracker's shutdown ended
// the detach, so the caller doesn't mistake it for its own cancellation.
func (t *Tracker) detachCallError(ctx context.Context, err error) error {
	if err != nil && t.ctx.Err() != nil && ctx.Err() == nil {
		return apierror.Unavailable("atelet stopped during the detach: %v", err)
	}
	return err
}

// finishDetach runs op, a detach of the actor's latest local snapshot: it
// uploads the snapshot unless already durable, then deletes the actor's local
// snapshots and its other files on this node except snapshot-state.json.
// background reports whether a worker runs it, rather than a Detach call.
// Either way it makes a report of what it changed due, so snapshot-state.json
// and the actor's directory are removed once the control plane acknowledges
// the detach (or refuses it with NotFound).
func (t *Tracker) finishDetach(ctx context.Context, actorUID string, op *actorOp, alreadyDurable, background bool) (*ateletpb.ActorSnapshotState, error) {
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
			t.updateOp(actorUID, func(a *actorSnapshots) {
				if t.recordUploadLocked(a, target, uploaded) {
					t.markReportDueLocked(a)
				}
			})
		}
	}

	// The upload is recorded before anything is deleted, and LatestLocal is
	// cleared only once its files and the actor's other files are gone. After
	// a crash or a failed cleanup in between, the detach is retried, finds the
	// snapshot durable, and only cleans up. Until then LatestLocal can name a
	// half-deleted copy, which is safe because the snapshot is durable:
	// Restore reads snapshot.object when the local copy is unusable. The
	// cleanup runs with op in progress (before op.done is closed), so Run,
	// Restore, and Checkpoint wait for it.
	recordDetach := err == nil
	if recordDetach {
		detachStart = t.clock.Now()
		// Forget or the tracker's shutdown may have ended ctx as the upload
		// finished.
		err = ctx.Err()
	}
	if err == nil {
		err = actordirs.RemoveActorFilesExceptState(actorUID)
	}
	t.updateOp(actorUID, func(a *actorSnapshots) {
		switch {
		case err == nil:
			t.clearLatestLocalLocked(a)
			t.markReportDueLocked(a)
		case ctx.Err() == nil:
			// Detach again once the retry delay has passed, and no sooner
			// than the detach was due.
			a.DetachAt = later(a.DetachAt, t.backOffLocked(a))
		}
	})

	// Log and record before endOp, which ends ctx.
	elapsed := t.clock.Since(detachStart)
	logOp(ctx, actorUID, op.kind, target, uploaded, background, elapsed, err)
	if background && recordDetach {
		t.instruments.recordDetach(ctx, target, elapsed, err)
	}
	return t.endOp(actorUID, op, err)
}
