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
	"log/slog"
	"time"
)

// beginUploadLocked begins a background upload of the actor's nextUploadTarget,
// under a context derived from ctx, and returns the func that runs it once the
// caller releases the actor's lock. Any superseded snapshot in a.Uploading is
// replaced in the same state-file save, and its local copy removed (see
// removeSupersededLocalCopy) before the upload runs.
func (t *Tracker) beginUploadLocked(ctx context.Context, a *actorSnapshots) func() {
	superseded, _ := a.clearSupersededUploading()
	target := *a.nextUploadTarget()
	wasUploading := sameUUID(a.Uploading, &target)
	opCtx, op := t.beginOpLocked(ctx, a, opUpload, target)
	// beginOpLocked records target in a.Uploading. Save only when target was
	// not already in a.Uploading, as it already is on disk when retrying or
	// resuming an earlier upload.
	if !wasUploading {
		t.saveLocked(a)
	}
	actorUID := a.actorUID
	return func() {
		if superseded != nil {
			t.removeSupersededLocalCopy(actorUID, *superseded)
		}
		t.backgroundUpload(opCtx, actorUID, op)
	}
}

// cleanupSupersededUploadingLocked clears a.Uploading when no upload is
// running and a newer commit has superseded it, so backgroundUpload will never
// run for it. It saves the change and returns a func that removes the local
// copy kept for that upload (see removeSupersededLocalCopy), or nil.
func (t *Tracker) cleanupSupersededUploadingLocked(a *actorSnapshots) func() {
	superseded, cleared := a.clearSupersededUploading()
	if !cleared {
		return nil
	}
	t.saveLocked(a)
	if superseded == nil {
		return nil
	}
	actorUID := a.actorUID
	return func() {
		t.removeSupersededLocalCopy(actorUID, *superseded)
	}
}

// uploadSnapshot uploads target from its local copy to object storage and,
// when background is true, records the background upload metric.
func (t *Tracker) uploadSnapshot(ctx context.Context, actorUID string, target SnapshotRef, background bool) (uploaded bool, elapsed time.Duration, err error) {
	start := t.clock.Now()
	uploaded, err = t.store.UploadLocalSnapshot(ctx, actorUID, target)
	elapsed = t.clock.Since(start)
	if background {
		t.instruments.recordUpload(ctx, target, elapsed, uploaded, err)
	}
	return uploaded, elapsed, err
}

// backgroundUpload runs op, a background upload of the actor's nextUploadTarget
// when it began, on a worker. Once the upload succeeds or a newer snapshot
// replaces target, it removes target's local copy if LatestLocal moved on (see
// removeSupersededLocalCopy); when the upload fails and LatestLocal cannot
// replace target, it keeps target in Uploading to retry. An upload Forget
// abandoned leaves the actor alone.
func (t *Tracker) backgroundUpload(ctx context.Context, actorUID string, op *actorOp) {
	defer close(op.done)
	target := op.target
	uploaded, elapsed, err := t.uploadSnapshot(ctx, actorUID, target, true)
	logOp(ctx, actorUID, op.kind, target, uploaded, true, elapsed, err)

	t.lock(actorUID)
	a := t.endOpLocked(actorUID, op)
	if a == nil {
		t.unlock(actorUID)
		return
	}
	removeOld := false
	switch {
	case err == nil:
		if t.recordUploadLocked(a, target, uploaded) {
			t.markReportDueLocked(a)
		}
		a.Uploading = nil
		a.uploadingFailed = false
		t.succeededLocked(a)
		t.queueReportLocked(a)
		removeOld = a.localSuperseded(&target)
	case !a.uploadNeeded(target):
		// A newer snapshot became durable while the upload ran, as an
		// immediate checkpoint or a promote makes one, so the failure costs
		// nothing.
		a.Uploading = nil
		a.uploadingFailed = false
		t.saveLocked(a)
		t.requeueLocked(a)
		removeOld = a.localSuperseded(&target)
	default:
		if t.ctx.Err() == nil {
			a.uploadingFailed = true
			if superseded, cleared := a.clearSupersededUploading(); cleared {
				t.saveLocked(a)
				removeOld = superseded != nil
			}
			if _, ok := t.nextDueLocked(a); ok {
				t.backOffLocked(a)
			}
		}
		t.requeueLocked(a)
	}
	t.unlock(actorUID)
	if removeOld {
		t.removeSupersededLocalCopy(actorUID, target)
	}
}

// recordUploadLocked records that an upload of target succeeded, and reports
// whether target is now LatestDurable, which it is not when that generation or
// a newer snapshot was already durable. uploaded reports whether the upload
// wrote the manifest rather than finding it committed already.
func (t *Tracker) recordUploadLocked(a *actorSnapshots, target SnapshotRef, uploaded bool) bool {
	now := t.clock.Now()
	d := target.clone()
	if uploaded {
		d.UploadTime = now
	}
	if !a.recordDurable(d) {
		return false
	}
	a.uploadSucceeded(now)
	return true
}

// removeSupersededLocalCopy deletes the local copy of target, an upload's
// snapshot that a newer snapshot superseded, once no upload needs it: the copy
// was kept only for the upload. It leaves the copy while a Run, Restore,
// Checkpoint, or PromoteSnapshot of the actor is in progress, since a Restore
// or PromoteSnapshot may be reading it, and deletes it under the actor's lock
// so that none begins reading it meanwhile. Best-effort: the actor's next
// checkpoint, detach, or terminate prunes a copy left behind.
func (t *Tracker) removeSupersededLocalCopy(actorUID string, target SnapshotRef) {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	if t.heldLocked(actorUID) {
		return
	}
	if err := removeLocalCheckpoint(t.ctx, actorUID, target.uuid()); err != nil {
		slog.WarnContext(t.ctx, "Failed to delete a superseded local checkpoint", slog.String("actorUID", actorUID), slog.Any("err", err))
	}
}
