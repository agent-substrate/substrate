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

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/proto/ateletpb"
)

// Background work settings. All but the Default ones, which atelet's
// --snapshot-upload-delay, --snapshot-detach-delay, --snapshot-upload-workers,
// and --snapshot-detach-workers override, are fixed until they become flags or
// per-request settings on UploadConfig.
const (
	// DefaultUploadDelay is how long by default after the oldest
	// not-yet-durable snapshot of an actor was taken atelet uploads the
	// actor's latest local snapshot, under UPLOAD_MODE_DEFERRED.
	DefaultUploadDelay = 5 * time.Minute
	// DefaultDetachDelay is how long an actor stays suspended on this node by
	// default, its latest snapshot kept locally, before atelet detaches it.
	DefaultDetachDelay = 30 * time.Minute
	// snapshotRetryDelay is how long atelet holds an actor's background work
	// back after a failure. Each further consecutive failure doubles it, up to
	// maxSnapshotRetryDelay.
	snapshotRetryDelay    = time.Minute
	maxSnapshotRetryDelay = 10 * time.Minute
	// DefaultUploadWorkers and DefaultDetachWorkers are how many background
	// uploads and detaches run at once on a node by default. Each kind has a
	// queue and workers of its own, so neither waits behind the other.
	DefaultUploadWorkers = 2
	DefaultDetachWorkers = 2
)

// opKind is what an operation on an actor's snapshots does.
type opKind int

const (
	// opUpload makes a snapshot durable: a background upload.
	opUpload opKind = iota
	// opDetach makes the latest local snapshot durable, then deletes the
	// actor's local snapshots: a background detach, or a Detach call.
	opDetach
	// numOpKinds is how many kinds of operation there are.
	numOpKinds
)

func (k opKind) String() string {
	switch k {
	case opUpload:
		return "upload"
	case opDetach:
		return "detach"
	default:
		return "unknown"
	}
}

// logOp logs how an upload or detach ended. Foreground failures are returned
// to the RPC caller, so only background failures are logged here: as a
// warning, or at info level once ctx has ended, as when Terminate or an atelet
// shutdown cancels the operation.
func logOp(ctx context.Context, actorUID string, kind opKind, target SnapshotRef, uploaded, background bool, elapsed time.Duration, err error) {
	attrs := []any{
		slog.String("op", kind.String()),
		slog.String("actorUID", actorUID),
		slog.String("snapshot", target.uuid()),
		slog.Int("generation", int(target.generation())),
		slog.Bool("background", background),
		slog.Duration("elapsed", elapsed),
	}
	switch {
	case err == nil:
		slog.InfoContext(ctx, "Snapshot operation finished", append(attrs, slog.Bool("uploaded", uploaded))...)
	case !background:
	case ctx.Err() != nil:
		slog.InfoContext(ctx, "Snapshot operation canceled", append(attrs, slog.Any("err", err))...)
	default:
		slog.WarnContext(ctx, "Snapshot operation failed", append(attrs, slog.Any("err", err))...)
	}
}

// actorOp is an actor's operation in progress: an upload or a detach. An
// actor has at most one, of any kind.
//
// A worker runs it in the background once it is due. An atelet RPC such as
// Detach runs it in the RPC's goroutine instead, without going through the
// background queue.
//
// The operation holds the actor's lock only while it begins and ends, not
// while it runs:
//   - Terminate never waits for it.
//   - Checkpoint never waits for an upload.
//   - Run, Restore, and Checkpoint wait for a detach to end, so it never
//     deletes the actor's local snapshots underneath them.
type actorOp struct {
	kind opKind
	// target is the snapshot the operation acts on.
	target SnapshotRef
	// cancel ends the context the operation runs under.
	cancel context.CancelFunc
	// done is closed once the operation has ended, and has finished any change
	// it makes to the actor's local snapshots.
	done chan struct{}
}

// Start reads each actor's snapshot state file on this node, queues the actor
// for when its next background upload or detach is due and for any report due,
// so its background work resumes after an atelet restart, and starts each
// queue's workers and the report workers. With instruments, it also adds each
// actor's undurable local snapshot to their tally before any worker runs.
func (t *Tracker) Start() {
	if t.store != nil || t.reporting.reporter != nil || t.undurableActorsTally != nil {
		actorUIDs, err := savedActorUIDs()
		if err != nil {
			// Best-effort: each actor's next commit queues it again.
			slog.Warn("Failed to find the actors with snapshot state to queue", slog.Any("err", err))
		}
		for _, actorUID := range actorUIDs {
			if !t.locks.Lock(t.ctx, actorUID) {
				break
			}
			if a := t.findLocked(actorUID); a != nil {
				t.undurableActorsTally.observe(a)
				t.enqueueSavedReportLocked(a)
				t.requeueLocked(a)
			}
			t.unlock(actorUID)
		}
	}
	for kind, n := range t.workers {
		for range n {
			t.workersDone.Go(func() { t.runWorker(opKind(kind)) })
		}
	}
	if t.reporting.reporter != nil {
		for range t.reporting.workers {
			t.workersDone.Go(t.runReporter)
		}
	}
}

// Wait blocks until the workers have stopped, which they do once the tracker's
// context ends, or until ctx ends. It returns ctx's error if ctx ended first.
func (t *Tracker) Wait(ctx context.Context) error {
	stopped := make(chan struct{})
	go func() {
		t.workersDone.Wait()
		close(stopped)
	}()
	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// runWorker runs the background operations of kind of the actors it takes
// from kind's queue, until the queue shuts down.
func (t *Tracker) runWorker(kind opKind) {
	q := t.queues[kind]
	for {
		actorUID, shutdown := q.Get()
		if shutdown {
			return
		}
		t.syncActor(actorUID, kind)
		q.Done(actorUID)
	}
}

// syncActor runs the actor's background operation of kind if beginDueWork
// begins one.
func (t *Tracker) syncActor(actorUID string, kind opKind) {
	if run := t.beginDueWork(actorUID, kind); run != nil {
		run()
	}
}

// beginDueWork begins the actor's background operation of kind if one is due,
// and returns the func that runs it. Otherwise it returns nil, and queues the
// actor for when its next operations are due, unless an operation, Run,
// Restore, Checkpoint, or PromoteSnapshot in progress on the actor queues it
// again when it ends.
func (t *Tracker) beginDueWork(actorUID string, kind opKind) func() {
	if !t.locks.Lock(t.ctx, actorUID) {
		// atelet is shutting down.
		return nil
	}
	defer t.unlock(actorUID)
	if t.ctx.Err() != nil {
		// atelet is shutting down.
		return nil
	}
	a := t.findLocked(actorUID)
	if a == nil || a.op != nil {
		// Forgotten, or an operation is already in progress on the actor: one
		// a worker of the other queue began, or one an atelet RPC such as
		// Detach runs in the RPC's goroutine. When that operation ends,
		// requeueLocked queues the actor again.
		return nil
	}
	now := t.clock.Now()
	var cleanup func()
	switch kind {
	case opDetach:
		if due, ok := t.detachDueLocked(a); ok && !due.After(now) {
			if a.holds > 0 {
				// A Run, Restore, Checkpoint, or PromoteSnapshot of the actor
				// is in progress, and queues the actor again when it ends
				// (see holdLocked).
				return nil
			}
			if finish, err := t.beginDetachLocked(t.ctx, a, true); err == nil {
				t.instruments.recordDetachOverdue(t.ctx, now.Sub(due))
				return func() { _, _ = finish() }
			}
		}
	case opUpload:
		if due, ok := t.uploadDueLocked(a); ok && !due.After(now) {
			if a.promotes > 0 {
				// A PromoteSnapshot of the actor is in progress, and queues
				// the actor again when it ends (see holdLocked).
				return nil
			}
			run := t.beginUploadLocked(t.ctx, a)
			t.instruments.recordUploadOverdue(t.ctx, now.Sub(due))
			return run
		}
		// beginUploadLocked clears any superseded Uploading snapshot before
		// starting the next upload. When no upload runs (for example after an
		// immediate checkpoint or promote made a newer snapshot durable,
		// leaving no upload due), clear a superseded Uploading snapshot here.
		cleanup = t.cleanupSupersededUploadingLocked(a)
	}
	t.requeueLocked(a)
	return cleanup
}

// nextDueLocked returns when the actor's next background detach or upload is
// due, if either is.
func (t *Tracker) nextDueLocked(a *actorSnapshots) (time.Time, bool) {
	due, ok := t.detachDueLocked(a)
	if upload, uploadOK := t.uploadDueLocked(a); uploadOK && (!ok || upload.Before(due)) {
		due, ok = upload, true
	}
	return due, ok
}

// requeueLocked queues the actor on each queue for when its next background
// operation of that kind is due, if it has one, then drops the actor's entry
// from memory unless the entry has an operation, or a Run, Restore,
// Checkpoint, or PromoteSnapshot, in progress, a retry hold, a failed upload
// kept to retry, or a change that failed to save. An entry whose Uploading is
// superseded is queued at once on the upload queue and kept in memory until an
// upload worker cleans it up. A save that finds the actor's directory gone
// leaves nothing unsaved. requeueLocked runs after a change to the actor is
// saved, so it must not change what the snapshot state file keeps.
func (t *Tracker) requeueLocked(a *actorSnapshots) {
	now := t.clock.Now()
	// CommitDurable and CommitPromoted leave a superseded Uploading snapshot
	// for an upload worker to clear from the state file and delete from disk,
	// rather than deleting its files on the RPC path. Queue the actor on the
	// upload queue now so a worker cleans it up even when no upload is due.
	supersededUploading := t.store != nil && a.Uploading != nil && !a.keepUploading()
	if supersededUploading {
		t.queues[opUpload].Add(a.actorUID)
	} else if due, ok := t.uploadDueLocked(a); ok {
		t.queues[opUpload].AddAfter(a.actorUID, due.Sub(now))
	}
	if due, ok := t.detachDueLocked(a); ok {
		t.queues[opDetach].AddAfter(a.actorUID, due.Sub(now))
	}
	if !supersededUploading && a.op == nil && a.holds == 0 && a.retryAt.IsZero() && !a.uploadingFailed && !a.unsaved {
		t.dropLocked(a.actorUID)
	}
}

// beginOpLocked records an operation of kind on target as the actor's
// operation in progress, and returns it with the context it runs under: one
// derived from ctx, which Forget cancels.
//
// The operation and its context are nil when the actor already has an
// operation in progress, started by a background worker or an atelet RPC.
//
// The operation keeps the actor's entry in memory until it ends. The caller
// releases the actor's lock while the operation runs, and closes the
// operation's done channel once it has ended.
func (t *Tracker) beginOpLocked(ctx context.Context, a *actorSnapshots, kind opKind, target SnapshotRef) (context.Context, *actorOp) {
	if a.op != nil {
		return nil, nil
	}
	ctx, cancel := context.WithCancel(ctx)
	cloned := target.clone()
	if kind == opUpload {
		a.Uploading = cloned
		a.uploadingFailed = false
	}
	a.op = &actorOp{kind: kind, target: *cloned, cancel: cancel, done: make(chan struct{})}
	t.addLocked(a)
	return ctx, a.op
}

// opEntryLocked returns the actor's entry while op is still its operation in
// progress, or nil once Forget has abandoned op.
func (t *Tracker) opEntryLocked(actorUID string, op *actorOp) *actorSnapshots {
	a := t.findLocked(actorUID)
	if a == nil || a.op != op {
		return nil
	}
	return a
}

// abandonedError returns what an operation on the actor fails with once Forget
// has abandoned it: opErr if the operation failed already, and otherwise
// Aborted.
func abandonedError(actorUID string, opErr error) error {
	if opErr != nil {
		return opErr
	}
	return apierror.Aborted("actor %s was forgotten during the operation", actorUID)
}

// updateOp runs fn under the actor's lock and saves the actor's state while op
// is still the actor's operation in progress. Once Forget has abandoned op, it
// leaves the actor alone and fails with abandonedError.
func (t *Tracker) updateOp(actorUID string, op *actorOp, opErr error, fn func(a *actorSnapshots)) error {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a := t.opEntryLocked(actorUID, op)
	if a == nil {
		return abandonedError(actorUID, opErr)
	}
	fn(a)
	t.saveLocked(a)
	return nil
}

// endOpLocked ends op and returns the actor's entry, or nil once Forget has
// abandoned op.
func (t *Tracker) endOpLocked(actorUID string, op *actorOp) *actorSnapshots {
	op.cancel()
	a := t.opEntryLocked(actorUID, op)
	if a != nil {
		a.op = nil
	}
	return a
}

// endOp ends op, queues the actor for its next background operation and any
// report due, and returns the actor's state. Once Forget has abandoned op, it
// leaves the actor alone and fails with abandonedError; otherwise it fails
// with opErr if non-nil.
func (t *Tracker) endOp(actorUID string, op *actorOp, opErr error) (*ateletpb.ActorSnapshotState, error) {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a := t.endOpLocked(actorUID, op)
	if a == nil {
		return nil, abandonedError(actorUID, opErr)
	}
	t.requeueLocked(a)
	t.queueReportLocked(a)
	if opErr != nil {
		return nil, opErr
	}
	return t.stateLocked(a), nil
}

// waitIdleLocked waits, as long as ctx allows, until the actor has no
// operation in progress (whether started by a background worker or an atelet
// RPC), and returns the actor's entry, which may be nil. It releases the
// actor's lock while it waits, so the actor can change meanwhile, and holds
// it again when it returns. check, if not nil, vets the entry on every pass:
// an error from it is returned at once, without waiting further.
func (t *Tracker) waitIdleLocked(ctx context.Context, actorUID string, check func(*actorSnapshots) error) (*actorSnapshots, error) {
	for {
		a := t.findLocked(actorUID)
		if a != nil && check != nil {
			if err := check(a); err != nil {
				return nil, err
			}
		}
		if a == nil || a.op == nil {
			return a, nil
		}
		if err := t.waitOpLocked(ctx, actorUID, a.op); err != nil {
			return nil, err
		}
	}
}

// waitForDetachLocked waits, as long as ctx allows, until no detach of the
// actor is in progress, and returns the actor's entry, which may be nil. It
// releases the actor's lock while it waits, and holds it again when it
// returns. check, if not nil, vets the entry on every pass: an error from it
// is returned at once, without waiting further. Checkpoint, Run, and Restore
// call this first so they never race with a detach pruning the actor's local
// directory, without waiting behind an upload.
func (t *Tracker) waitForDetachLocked(ctx context.Context, actorUID string, check func(*actorSnapshots) error) (*actorSnapshots, error) {
	for {
		a := t.findLocked(actorUID)
		if a != nil && check != nil {
			if err := check(a); err != nil {
				return nil, err
			}
		}
		if a == nil || a.runningDetach() == nil {
			return a, nil
		}
		if err := t.waitOpLocked(ctx, actorUID, a.op); err != nil {
			return nil, err
		}
	}
}

// waitForDetachOrSnapshotUploadLocked waits, as long as ctx allows, until no
// detach of the actor and no background upload of snap is in progress, and
// returns the actor's entry, which may be nil. It checks snap's generation on
// every pass, so a stale generation fails at once without waiting, and one
// superseded while waiting fails as soon as the wait ends. PromoteSnapshot
// calls this first so it never races with a detach or uploads the same
// snapshot twice.
func (t *Tracker) waitForDetachOrSnapshotUploadLocked(ctx context.Context, actorUID string, snap SnapshotRef) (*actorSnapshots, error) {
	for {
		a := t.findLocked(actorUID)
		if a != nil {
			if err := a.checkGeneration(snap.Snapshot); err != nil {
				return nil, err
			}
		}
		if a == nil || a.op == nil {
			return a, nil
		}
		switch a.op.kind {
		case opDetach:
		case opUpload:
			if !sameUUID(&a.op.target, &snap) {
				return a, nil
			}
		default:
			return a, nil
		}
		if err := t.waitOpLocked(ctx, actorUID, a.op); err != nil {
			return nil, err
		}
	}
}

// waitOpLocked waits, releasing the actor's lock meanwhile, until op has
// ended or ctx does.
func (t *Tracker) waitOpLocked(ctx context.Context, actorUID string, op *actorOp) error {
	t.unlock(actorUID)
	defer t.lock(actorUID)
	select {
	case <-op.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// abandonOpLocked cancels the actor's operation in progress, if any, without
// waiting for it to end.
func (t *Tracker) abandonOpLocked(actorUID string) {
	// An entry with an operation in progress is always in memory.
	t.mu.Lock()
	a := t.actors[actorUID]
	t.mu.Unlock()
	if a != nil && a.op != nil {
		a.op.cancel()
		a.op = nil
	}
}

// backOffLocked records a failure of the actor's background work, and holds
// that work back until the time it returns: later with each consecutive
// failure.
func (t *Tracker) backOffLocked(a *actorSnapshots) time.Time {
	a.retryAt = t.clock.Now().Add(t.retries.When(a.actorUID))
	return a.retryAt
}

// resetBackoffLocked lifts the hold backOffLocked put on the actor's
// background work, and forgets its consecutive failures, so its next failure
// holds it back for snapshotRetryDelay again.
func (t *Tracker) resetBackoffLocked(a *actorSnapshots) {
	a.retryAt = time.Time{}
	t.retries.Forget(a.actorUID)
}
