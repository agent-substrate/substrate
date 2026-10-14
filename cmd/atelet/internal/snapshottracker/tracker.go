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

// Package snapshottracker tracks each actor's snapshots on an atelet node, and
// detaches them in the background.
package snapshottracker

import (
	"context"
	"sync"
	"time"

	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
)

// Store moves an actor's snapshots between this node and object storage.
type Store interface {
	// UploadLocalSnapshot makes ref durable at its snapshot's URI from its
	// local copy. With the local copy gone, it succeeds without uploading if a
	// manifest is already committed there. It reports whether it uploaded.
	UploadLocalSnapshot(ctx context.Context, actorUID string, ref SnapshotRef) (bool, error)
}

// Tracker tracks each actor's snapshots on this node: the latest one stored
// locally, and the latest one confirmed in object storage. State reports them.
// It detaches suspended actors from this node in the background.
//
// Background work runs off a delaying workqueue of actor UIDs drained by a
// fixed pool of workers. Each change to an actor's state queues the actor for
// when its next detach is due:
//  1. A checkpoint (CommitLocal or CommitDurable) records the new snapshot as
//     LatestLocal, and schedules the actor's detach detachDelay after the
//     checkpoint.
//  2. A worker that takes the actor from the queue detaches it if the detach
//     is due then. When it is not due yet, the worker queues the actor again
//     for when it is: the queue holds an actor until the earliest time it was
//     queued for, so a due time that moves later only costs a worker that
//     check.
//  3. The queue hands an actor to one worker at a time, and the number of
//     workers bounds how many background detaches run at once.
//  4. A detach, in the background or via the Detach RPC, makes LatestLocal
//     durable and then deletes all the actor's local snapshots.
//  5. A Run or Restore unschedules the actor's detach as it begins
//     (BeginStart), and saves that before the workload can start, so atelet
//     does not detach an actor whose start has begun, even after a restart or
//     a failed start; only the actor's next checkpoint schedules a detach
//     again. A background detach that comes due while a Run, Restore, or
//     Checkpoint is in progress (BeginStart or BeginCheckpoint) waits for it
//     to end, which queues the actor again.
//
// The Detach RPC runs in the calling goroutine rather than on a worker, so an
// explicit control-plane call does not wait behind other actors' background
// work. It does wait for the actor's own detach in progress, as do Run,
// Restore, and Checkpoint. A worker leaves an actor with a detach in progress
// to that detach's end, which queues the actor again.
//
// TODO: Commits record the snapshot they are given without comparing
// Snapshot.generation. Checkpoint and PromoteSnapshot should reject a stale
// generation before doing any work, as Snapshot.generation describes, so that
// no commit replaces a snapshot with an older one.
type Tracker struct {
	// ctx bounds background work, and shuts the queue down when it ends.
	ctx context.Context
	// store uploads the snapshots of actors being detached. A tracker
	// without one schedules no background work.
	store Store
	// clock is what due times are measured on, and the queue waits on.
	clock clock.WithTicker
	// detachDelay is how long a suspended actor keeps its local snapshot on
	// this node before atelet detaches it: DefaultDetachDelay unless
	// WithDetachDelay sets it.
	detachDelay time.Duration
	// retries says how long an actor's background work is held back after a
	// failure: longer with each consecutive failure for the actor.
	retries workqueue.TypedRateLimiter[string]
	// workers is how many background workers run at once.
	workers int
	// queue holds the UIDs of actors with background work scheduled.
	queue workqueue.TypedDelayingInterface[string]
	// workersDone counts the workers still running.
	workersDone sync.WaitGroup

	// locks holds each actor's lock. Work on an actor, including its snapshot
	// state file I/O, holds that actor's lock, so it never holds up another
	// actor.
	locks *actorlock.Locks

	// mu guards the actors map, and is never held during file I/O.
	mu sync.Mutex
	// actors maps actorUID to the entry of an actor with an operation, a Run,
	// Restore, or Checkpoint, or a retry hold in progress, or whose state its
	// snapshot state file lacks because the save failed. requeueLocked drops
	// any other entry, and findLocked then reads the state from the file.
	// Read and change it through findLocked, findOrAddLocked, addLocked,
	// dropLocked, and tracksLocked.
	actors map[string]*actorSnapshots
}

// Option configures a Tracker.
type Option func(*Tracker)

// WithDetachDelay sets how long an actor stays suspended on this node, its
// latest snapshot kept locally, before atelet detaches it.
func WithDetachDelay(d time.Duration) Option {
	return func(t *Tracker) {
		t.detachDelay = d
	}
}

// New returns a tracker over store that tracks no actor yet and measures time
// on clk. Its workers run once Start is called, until ctx ends. A nil store
// schedules no detaches.
func New(ctx context.Context, store Store, clk clock.WithTicker, opts ...Option) *Tracker {
	t := &Tracker{
		ctx:         ctx,
		store:       store,
		clock:       clk,
		detachDelay: DefaultDetachDelay,
		retries:     workqueue.NewTypedItemExponentialFailureRateLimiter[string](snapshotRetryDelay, maxSnapshotRetryDelay),
		workers:     snapshotWorkers,
		queue:       workqueue.NewTypedDelayingQueueWithConfig(workqueue.TypedDelayingQueueConfig[string]{Clock: clk}),
		locks:       actorlock.New(),
		actors:      map[string]*actorSnapshots{},
	}
	for _, opt := range opts {
		opt(t)
	}
	context.AfterFunc(ctx, t.queue.ShutDown)
	return t
}

// lock takes the actor's lock, which the methods named *Locked require.
func (t *Tracker) lock(actorUID string) {
	// Lock gives up only when its context is done, which Background never is.
	t.locks.Lock(context.Background(), actorUID)
}

// unlock releases the actor's lock.
func (t *Tracker) unlock(actorUID string) {
	t.locks.Unlock(actorUID)
}

// findLocked returns the actor's entry from memory, or reads it from disk when
// not in memory. It returns nil when atelet tracks nothing of the actor.
func (t *Tracker) findLocked(actorUID string) *actorSnapshots {
	t.mu.Lock()
	a, ok := t.actors[actorUID]
	t.mu.Unlock()
	if ok {
		return a
	}
	return readSnapshotState(actorUID)
}

// findOrAddLocked returns the actor's entry, adding an empty one when there is
// none, so it never returns nil.
func (t *Tracker) findOrAddLocked(actorUID string) *actorSnapshots {
	a := t.findLocked(actorUID)
	if a == nil {
		a = &actorSnapshots{actorUID: actorUID}
	}
	t.addLocked(a)
	return a
}

// addLocked makes a its actor's entry.
func (t *Tracker) addLocked(a *actorSnapshots) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.actors[a.actorUID] = a
}

// dropLocked removes the actor's entry.
func (t *Tracker) dropLocked(actorUID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.actors, actorUID)
}

// tracksLocked reports whether a is still its actor's entry in memory: false
// once Forget has dropped the actor, even if a new entry has been added since.
func (t *Tracker) tracksLocked(a *actorSnapshots) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.actors[a.actorUID] == a
}

// detachDueLocked returns when atelet next checks whether to detach the actor,
// as actorSnapshots.detachDue does. A tracker without a store detaches
// nothing.
func (t *Tracker) detachDueLocked(a *actorSnapshots) (time.Time, bool) {
	if t.store == nil {
		return time.Time{}, false
	}
	return a.detachDue()
}

// stateLocked returns what atelet holds for the actor.
func (t *Tracker) stateLocked(a *actorSnapshots) *ateletpb.ActorSnapshotState {
	return &ateletpb.ActorSnapshotState{
		LatestLocal:   a.LatestLocal.report(),
		LatestDurable: a.LatestDurable.report(),
	}
}

// State returns what atelet holds for the actor.
func (t *Tracker) State(actorUID string) *ateletpb.ActorSnapshotState {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	if a := t.findLocked(actorUID); a != nil {
		return t.stateLocked(a)
	}
	return &ateletpb.ActorSnapshotState{}
}
