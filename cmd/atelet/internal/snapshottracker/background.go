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

// Background work settings. All but DefaultDetachDelay, which
// --snapshot-detach-delay overrides, are fixed until they become flags or
// per-request settings on UploadConfig.
const (
	// DefaultDetachDelay is how long an actor stays suspended on this node by
	// default, its latest snapshot kept locally, before atelet detaches it.
	DefaultDetachDelay = 30 * time.Minute
	// snapshotRetryDelay is how long atelet holds an actor's background work
	// back after a failure. Each further consecutive failure doubles it, up to
	// maxSnapshotRetryDelay.
	snapshotRetryDelay    = time.Minute
	maxSnapshotRetryDelay = 10 * time.Minute
	// snapshotWorkers is how many background workers run at once on a node.
	snapshotWorkers = 4
)

// actorOp is an actor's operation in progress, such as a detach. A worker
// runs it when due in the background, whereas an atelet RPC such as Detach
// runs it in the RPC's goroutine without going through the background queue.
// An actor has at most one, of any kind. The operation holds the actor's lock
// only while it begins and ends, not while it runs, so Terminate never waits
// for it. Run, Restore, and Checkpoint wait for it to end, so it never
// changes the actor's local snapshots underneath them.
type actorOp struct {
	// target is the snapshot the operation acts on.
	target SnapshotRef
	// cancel ends the context the operation runs under.
	cancel context.CancelFunc
	// done is closed once the operation has ended, and has finished any change
	// it makes to the actor's local snapshots.
	done chan struct{}
}

// Start queues every actor with a snapshot state file on this node, so its
// background work resumes after an atelet restart, and starts the workers
// that run actors' background work. A worker reads each actor's file and
// queues the actor for when its next background work is due.
func (t *Tracker) Start() {
	actorUIDs, err := savedActorUIDs()
	if err != nil {
		// Best-effort: each actor's next commit queues it again.
		slog.Warn("Failed to find the actors with snapshot state to queue", slog.Any("err", err))
	}
	for _, actorUID := range actorUIDs {
		t.queue.Add(actorUID)
	}
	for range t.workers {
		t.workersDone.Go(t.runWorker)
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

// runWorker runs the background work of the actors it takes from the queue,
// until the queue shuts down.
func (t *Tracker) runWorker() {
	for {
		actorUID, shutdown := t.queue.Get()
		if shutdown {
			return
		}
		t.syncActor(actorUID)
		t.queue.Done(actorUID)
	}
}

// syncActor runs the actor's background operation if beginDueWork begins one.
func (t *Tracker) syncActor(actorUID string) {
	if run := t.beginDueWork(actorUID); run != nil {
		run()
	}
}

// beginDueWork begins the actor's background operation if one is due, and
// returns the func that runs it. Otherwise it returns nil, and queues the
// actor for when the next one is due, unless an operation, Run, Restore, or
// Checkpoint in progress on the actor queues it again when it ends.
func (t *Tracker) beginDueWork(actorUID string) func() {
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
		// Forgotten, or an atelet RPC such as Detach is already running an
		// operation on the actor in the RPC's goroutine rather than through
		// the background queue. When that operation ends, requeueLocked
		// queues the actor again.
		return nil
	}
	now := t.clock.Now()
	if due, ok := t.detachDueLocked(a); ok && !due.After(now) {
		if a.holds > 0 {
			// A Run, Restore, or Checkpoint of the actor is in progress, and
			// queues the actor again when it ends (see holdLocked).
			return nil
		}
		if finish, err := t.beginDetachLocked(t.ctx, a, true); err == nil {
			return func() { t.backgroundDetach(actorUID, finish) }
		}
	}
	t.requeueLocked(a)
	return nil
}

// requeueLocked queues the actor for when its next background operation is
// due, if it has one, then drops the actor's entry from memory unless the
// entry has an operation in progress, a Run, Restore, or Checkpoint in
// progress, a retry hold, or a change that failed to save. A save that finds
// the actor's directory gone leaves nothing unsaved. requeueLocked runs after
// a change to the actor is saved, so it must not change what the snapshot
// state file keeps.
func (t *Tracker) requeueLocked(a *actorSnapshots) {
	if due, ok := t.detachDueLocked(a); ok {
		t.queue.AddAfter(a.actorUID, due.Sub(t.clock.Now()))
	}
	if a.op == nil && a.holds == 0 && a.retryAt.IsZero() && !a.unsaved {
		t.dropLocked(a.actorUID)
	}
}

// beginOpLocked records an operation on target as the actor's operation in
// progress, which keeps the actor's entry in memory until the operation ends,
// and returns it with the context it runs under: one derived from ctx, which
// Forget cancels. The operation and its context are nil when the actor
// already has an operation in progress (started by a background worker or an
// atelet RPC). The caller releases the actor's lock while the operation runs,
// and closes the operation's done channel once it has ended.
func (t *Tracker) beginOpLocked(ctx context.Context, a *actorSnapshots, target SnapshotRef) (context.Context, *actorOp) {
	if a.op != nil {
		return nil, nil
	}
	ctx, cancel := context.WithCancel(ctx)
	a.op = &actorOp{target: *target.clone(), cancel: cancel, done: make(chan struct{})}
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

// endOp ends op, queues the actor for its next background operation, and
// returns the actor's state. Once Forget has abandoned op, it leaves the actor
// alone and fails with abandonedError; otherwise it fails with opErr if
// non-nil.
func (t *Tracker) endOp(actorUID string, op *actorOp, opErr error) (*ateletpb.ActorSnapshotState, error) {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a := t.endOpLocked(actorUID, op)
	if a == nil {
		return nil, abandonedError(actorUID, opErr)
	}
	t.requeueLocked(a)
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
