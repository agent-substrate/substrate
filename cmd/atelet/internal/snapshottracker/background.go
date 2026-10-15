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

// Fixed until they become flags or per-request settings on UploadConfig.
const (
	// snapshotRetryDelay is how long atelet holds an actor's background work
	// back after a failure. Each further consecutive failure doubles it, up to
	// maxSnapshotRetryDelay.
	snapshotRetryDelay    = time.Minute
	maxSnapshotRetryDelay = 10 * time.Minute
	// snapshotWorkers is how many background workers run at once on a node.
	snapshotWorkers = 4
)

// actorOp is an actor's background operation in progress. An actor has at
// most one. The operation holds the actor's lock only while it begins and
// ends, not while it runs, so Checkpoint and Terminate never wait for it.
type actorOp struct {
	// target is the snapshot the operation acts on.
	target SnapshotRef
	// cancel ends the context the operation runs under.
	cancel context.CancelFunc
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

// syncActor runs the actor's due background work, and otherwise queues the
// actor for when its next background work is due.
func (t *Tracker) syncActor(actorUID string) {
	if !t.locks.Lock(t.ctx, actorUID) {
		// atelet is shutting down.
		return
	}
	defer t.unlock(actorUID)
	if t.ctx.Err() != nil {
		// atelet is shutting down.
		return
	}
	a := t.findLocked(actorUID)
	if a == nil || a.op != nil {
		// Forgotten, or an operation is in progress, whose caller queues the
		// actor again when it ends.
		return
	}
	// TODO: Run the actor's due background work here, such as uploading its
	// latest local snapshot. Begin it with beginOpLocked and release the
	// actor's lock while it runs, so Checkpoint and Terminate never wait for
	// it. Then retake the lock and end it with endOpLocked.
	t.requeueLocked(a)
}

// requeueLocked queues the actor for when its next background work is due,
// then drops the actor's entry from memory unless the entry has an operation
// in progress or a change that failed to save. An actor whose directory is
// gone has nothing to save, so its entry is dropped. requeueLocked runs after
// a change to the actor is saved, so it must not change what the snapshot
// state file keeps. No background work is defined, so it queues nothing.
func (t *Tracker) requeueLocked(a *actorSnapshots) {
	if a.op == nil && !a.unsaved {
		t.dropLocked(a.actorUID)
	}
}

// beginOpLocked records an operation on target as the actor's background
// operation in progress, which keeps the actor's entry in memory until the
// operation ends, and returns it with the context it runs under. The
// operation and its context are nil when the actor already has an operation
// in progress. The caller releases the actor's lock while the operation runs.
func (t *Tracker) beginOpLocked(a *actorSnapshots, target SnapshotRef) (context.Context, *actorOp) {
	if a.op != nil {
		return nil, nil
	}
	ctx, cancel := context.WithCancel(t.ctx)
	a.op = &actorOp{target: *target.clone(), cancel: cancel}
	t.addLocked(a)
	return ctx, a.op
}

// endOpLocked ends op and returns the actor's entry, or nil once Forget has
// abandoned op: the caller must then leave the actor's state and local
// snapshots alone. Otherwise the caller records the operation's outcome on
// the entry, saves the entry if that changed it, and calls requeueLocked.
func (t *Tracker) endOpLocked(actorUID string, op *actorOp) *actorSnapshots {
	op.cancel()
	a := t.findLocked(actorUID)
	if a == nil || a.op != op {
		return nil
	}
	a.op = nil
	return a
}

// abandonOpLocked cancels the actor's background operation in progress, if
// any, without waiting for it to end.
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

// backOffLocked records a failure of the actor's background work, and returns
// when the actor may try again: later with each consecutive failure.
func (t *Tracker) backOffLocked(actorUID string) time.Time {
	return t.clock.Now().Add(t.retries.When(actorUID))
}

// resetBackoffLocked forgets the actor's consecutive failures, so its next
// failure holds it back for snapshotRetryDelay again.
func (t *Tracker) resetBackoffLocked(actorUID string) {
	t.retries.Forget(actorUID)
}
