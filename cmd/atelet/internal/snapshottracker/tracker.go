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
// runs background work on them.
package snapshottracker

import (
	"context"
	"sync"

	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
)

// Tracker tracks each actor's snapshots on this node: the latest one stored
// locally, and the latest one confirmed in object storage. State reports them.
//
// Background work runs off a delaying workqueue of actor UIDs drained by a
// fixed pool of workers. The queue hands an actor to one worker at a time, and
// the number of workers bounds how much background work runs at once.
//
// TODO: Commits record the snapshot they are given without comparing
// Snapshot.generation. Checkpoint and PromoteSnapshot should reject a stale
// generation before doing any work, as Snapshot.generation describes, so that
// no commit replaces a snapshot with an older one.
type Tracker struct {
	// ctx bounds background work, and shuts the queue down when it ends.
	ctx context.Context
	// clock is what due times are measured on, and the queue waits on.
	clock clock.WithTicker
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
	// actors maps actorUID to the entry of an actor with a background
	// operation in progress, or whose state its snapshot state file lacks
	// because the save failed. requeueLocked drops any other entry, and
	// findLocked then reads the state from the file. Read and change it
	// through findLocked, findOrAddLocked, addLocked, and dropLocked.
	actors map[string]*actorSnapshots
}

// New returns a tracker that tracks no actor yet and measures time on clk.
// Its workers run once Start is called, until ctx ends.
func New(ctx context.Context, clk clock.WithTicker) *Tracker {
	t := &Tracker{
		ctx:     ctx,
		clock:   clk,
		retries: workqueue.NewTypedItemExponentialFailureRateLimiter[string](snapshotRetryDelay, maxSnapshotRetryDelay),
		workers: snapshotWorkers,
		queue:   workqueue.NewTypedDelayingQueueWithConfig(workqueue.TypedDelayingQueueConfig[string]{Clock: clk}),
		locks:   actorlock.New(),
		actors:  map[string]*actorSnapshots{},
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
