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
	"errors"
	"log/slog"
	"slices"

	"k8s.io/client-go/util/workqueue"
)

// pruneWorkers is how many prunes run at once on a node.
const pruneWorkers = 2

// pruneQueue holds the queue and workers for deleting the local snapshots that
// a commit superseded (see actorSnapshotsRecord.Prune). Prunes touch only the
// node's disk, so they have a queue and workers of their own, apart from
// uploads and detaches. A prune that fails has no retry timer: the snapshot
// stays listed, and the actor's next state change, Checkpoint, Detach, or
// Terminate tries again.
type pruneQueue struct {
	// workers is how many prune workers drain queue.
	workers int
	// queue holds the UIDs of actors with a prune due.
	queue workqueue.TypedInterface[string]
}

func newPruneQueue() pruneQueue {
	return pruneQueue{
		workers: pruneWorkers,
		queue:   workqueue.NewTyped[string](),
	}
}

// pruneOp is a prune in progress on an actor's local snapshots.
type pruneOp struct {
	// uuids names the local snapshots the prune deletes.
	uuids []string
	// cancel ends the prune's context; Forget calls it.
	cancel context.CancelFunc
	// done is closed once the prune has ended.
	done chan struct{}
}

// listPruneLocked adds uuid to the actor's snapshots to prune, and reports
// whether it was not listed yet. The caller saves the actor's state in the
// same save that records the commit superseding the snapshot, so the state
// file never lists a snapshot a commit it lacks records. A listed snapshot is
// never committed again (see Prune), so nothing unlists one.
func (a *actorSnapshots) listPruneLocked(uuid string) bool {
	if uuid == "" || slices.Contains(a.Prune, uuid) {
		return false
	}
	a.Prune = append(a.Prune, uuid)
	return true
}

// pruneTargets returns the listed snapshots a prune deletes now: those that
// localSnapshotsToKeep does not name. A listed snapshot it still names, such
// as one an upload reads, waits until it no longer does.
func (a *actorSnapshots) pruneTargets() []string {
	keep := localSnapshotsToKeep(a)
	var targets []string
	for _, uuid := range a.Prune {
		if !slices.Contains(keep, uuid) {
			targets = append(targets, uuid)
		}
	}
	return targets
}

// pruneDue reports whether the actor has a snapshot to prune now.
func (a *actorSnapshots) pruneDue() bool {
	return len(a.pruneTargets()) > 0
}

// queuePruneLocked queues the actor's prune, if one is due and can run: no
// Run, Restore, Checkpoint, PromoteSnapshot, or Terminate holds the actor,
// and no prune or detach of it is in progress. Each of those queues the actor
// again when it ends.
func (t *Tracker) queuePruneLocked(a *actorSnapshots) {
	if a.hold != holdNone || a.pruning != nil || a.runningDetach() != nil || !a.pruneDue() {
		return
	}
	t.pruning.queue.Add(a.actorUID)
}

// beginPruneLocked begins a prune of the actor's listed local snapshots,
// under a context derived from ctx, and returns the func that runs it once
// the caller releases the actor's lock. The caller checks that a prune is due
// and can run (see queuePruneLocked). The prune deletes the targets of when it
// began: a snapshot listed while it runs waits for the next prune.
func (t *Tracker) beginPruneLocked(ctx context.Context, a *actorSnapshots) func() error {
	ctx, cancel := context.WithCancel(ctx)
	op := &pruneOp{uuids: a.pruneTargets(), cancel: cancel, done: make(chan struct{})}
	a.pruning = op
	t.addLocked(a)
	actorUID := a.actorUID
	return func() error { return t.finishPrune(ctx, actorUID, op) }
}

// finishPrune runs op: it deletes each of op.uuids, and drops the ones deleted
// from the actor's list. A snapshot it could not delete stays listed for a
// later attempt: the actor's next state change, Checkpoint, Detach, or
// Terminate, not a retry at once, which would spin on a disk that keeps
// failing. A snapshot that became deletable meanwhile is queued now. A
// snapshot that is missing already counts as deleted. pruneTargets left out a
// listed snapshot an upload still reads, so op.uuids never names one.
func (t *Tracker) finishPrune(ctx context.Context, actorUID string, op *pruneOp) error {
	defer close(op.done)
	defer op.cancel()
	start := t.clock.Now()
	var deleted []string
	var errs []error
	for _, uuid := range op.uuids {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		if err := t.removeLocalSnapshot(ctx, actorUID, uuid); err != nil {
			// Every target is attempted: one undeletable snapshot must not
			// strand the others on disk.
			errs = append(errs, err)
			continue
		}
		deleted = append(deleted, uuid)
	}
	err := errors.Join(errs...)

	// The entry stays in memory while the prune runs, and Forget waits for
	// the prune to end before dropping it.
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a := t.findLocked(actorUID)
	a.pruning = nil
	if len(deleted) > 0 {
		a.Prune = slices.DeleteFunc(a.Prune, func(uuid string) bool { return slices.Contains(deleted, uuid) })
		t.saveLocked(a)
	}
	attrs := []any{slog.String("actorUID", actorUID), slog.Any("deleted", deleted), slog.Duration("took", t.clock.Since(start))}
	switch {
	case err == nil:
		slog.InfoContext(ctx, "Pruned the actor's superseded local snapshots", attrs...)
	case ctx.Err() != nil:
		// Terminate or an atelet shutdown canceled the prune.
		slog.InfoContext(ctx, "Prune of the actor's superseded local snapshots canceled", append(attrs, slog.Any("err", err))...)
	default:
		slog.WarnContext(ctx, "Failed to prune the actor's superseded local snapshots", append(attrs, slog.Any("err", err))...)
		if !slices.ContainsFunc(a.pruneTargets(), func(uuid string) bool { return !slices.Contains(op.uuids, uuid) }) {
			t.requeueOpsLocked(a)
			return err
		}
	}
	t.requeueLocked(a)
	return err
}

// runPruner runs the prunes of the actors it takes from the prune queue, until
// the queue shuts down.
func (t *Tracker) runPruner() {
	for {
		actorUID, shutdown := t.pruning.queue.Get()
		if shutdown {
			return
		}
		t.prune(actorUID)
		t.pruning.queue.Done(actorUID)
	}
}

// prune runs the actor's prune if one is due and can run (see
// queuePruneLocked), in the worker's goroutine.
func (t *Tracker) prune(actorUID string) {
	if !t.locks.Lock(t.ctx, actorUID) {
		// atelet is shutting down.
		return
	}
	a := t.findLocked(actorUID)
	if a == nil || !a.pruneDue() || a.hold != holdNone || a.pruning != nil || a.runningDetach() != nil {
		t.unlock(actorUID)
		return
	}
	run := t.beginPruneLocked(t.ctx, a)
	t.unlock(actorUID)
	_ = run()
}

// waitForPruneLocked waits, releasing the actor's lock meanwhile, until the
// actor's prune in progress has ended or ctx does.
func (t *Tracker) waitForPruneLocked(ctx context.Context, actorUID string, op *pruneOp) error {
	t.unlock(actorUID)
	defer t.lock(actorUID)
	select {
	case <-op.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
