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

// Package snapshottracker tracks each actor's snapshots on an atelet node,
// uploads and detaches them in the background, and reports what those change
// to the control plane.
package snapshottracker

import (
	"context"
	"sync"
	"time"

	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
)

// Store moves an actor's snapshots between this node and object storage.
type Store interface {
	// UploadLocalSnapshot makes ref durable at its snapshot's URI from its
	// local copy, acting for ref.Actor. With the local copy gone, it succeeds
	// without uploading if a manifest is already committed there. It reports
	// whether it uploaded.
	UploadLocalSnapshot(ctx context.Context, actorUID string, ref SnapshotRef) (bool, error)
}

// Tracker tracks each actor's snapshots on this node: the latest one stored
// locally, and the latest one confirmed in object storage. State reports them,
// BeginCheckpoint and BeginPromote reject a stale generation before Checkpoint
// or PromoteSnapshot does any work, and commits never replace a snapshot with
// an older generation. It uploads and detaches them in the background.
//
// Background work runs off two delaying workqueues of actor UIDs, one for
// uploads and one for detaches, each drained by a fixed pool of workers of its
// own, so slow uploads cannot hold up a due detach, nor detaches an upload.
// Each change to an actor's state queues the actor on each queue for when its
// next upload or detach is due:
//  1. A checkpoint records the new snapshot as LatestLocal with CommitLocal,
//     and schedules the actor's detach detachDelay after the checkpoint. An
//     immediate checkpoint records its upload with CommitDurable first. A
//     promote of LatestLocal (CommitPromoted) leaves the schedule as it is.
//     If LatestLocal needs an upload (see uploadDue), its upload is due
//     uploadDelay after UndurableSince.
//  2. A worker that takes the actor from its queue runs the actor's upload,
//     or detach, if it is due then. When it is not due yet, the worker queues
//     the actor again for when it is: a queue holds an actor until the
//     earliest time it was queued for, so a due time that moves later only
//     costs a worker that check. When both are due, whichever worker begins
//     first runs; an upload leaves the detach only its prune.
//  3. The worker uploads the LatestLocal of when it runs, so snapshots
//     superseded while the actor waits for its upload are never uploaded.
//     An upload that has begun (Uploading) goes on across newer checkpoints
//     and atelet restarts, though, until it succeeds, a newer snapshot is
//     durable, or it fails while a newer local snapshot can replace it.
//  4. Each queue hands an actor to one of its workers at a time, and its
//     number of workers bounds how many background uploads, or detaches, run
//     at once. An actor can be on both queues at once, but has at most one
//     operation in progress (see actorOp): a worker that finds one leaves the
//     actor to that operation's end, which queues it again.
//  5. An upload leaves LatestLocal on disk so a same-node Restore still
//     reads it. A detach, in the background or via the Detach RPC, makes
//     LatestLocal durable and then deletes all the actor's local snapshots.
//  6. A Run or Restore unschedules the actor's detach as it begins
//     (BeginStart), and saves that before the workload can start, so atelet
//     does not detach an actor whose start has begun, even after a restart or
//     a failed start; only the actor's next checkpoint schedules a detach
//     again. A background detach that comes due while a Run, Restore,
//     Checkpoint, or PromoteSnapshot is in progress (BeginStart,
//     BeginCheckpoint, or BeginPromote) waits for it to end, which queues the
//     actor again.
//  7. A background upload that records a new LatestDurable, or a background
//     detach that records one or clears LatestLocal, makes a report of the
//     actor's state due (ReportDueSince), saved with the change so that it
//     survives a restart. A Checkpoint, a PromoteSnapshot while a local
//     snapshot is kept, or a Detach call returns the latest state to the
//     control plane, so it cancels any pending report. Report workers, apart
//     from the upload and detach workers, spread saved reports across a
//     startup window, send the actor's whole state through the Reporter, and
//     retry with per-actor and
//     queue-wide backoff until the control plane acknowledges or refuses it,
//     a call cancels it, or reporting.giveUpAfter passes. A report waits while
//     a Run, Restore, Checkpoint, or PromoteSnapshot is in progress, and a
//     background change made while a report ran is reported again.
//
// The Detach RPC runs in the calling goroutine rather than on a worker, so an
// explicit control-plane call does not wait behind other actors' background
// work. It does wait for the actor's own operation in progress, even an
// upload of an older snapshot; Run, Restore, and Checkpoint wait for a detach
// in progress; and PromoteSnapshot waits for a detach or a background upload
// of the same snapshot. A worker leaves an actor with an operation in
// progress to that operation's end, which queues the actor again.
type Tracker struct {
	// ctx bounds background work, and shuts the queues down when it ends.
	ctx context.Context
	// store uploads actors' snapshots. A tracker without one schedules no
	// background work.
	store Store
	// clock is what due times are measured on, and the queues wait on.
	clock clock.WithTicker
	// uploadDelay is how long after UndurableSince the actor's latest local
	// snapshot becomes due for background upload: DefaultUploadDelay unless
	// WithUploadDelay sets it.
	uploadDelay time.Duration
	// detachDelay is how long a suspended actor keeps its local snapshot on
	// this node before atelet detaches it: DefaultDetachDelay unless
	// WithDetachDelay sets it.
	detachDelay time.Duration
	// instruments records the background uploads and detaches (how late they
	// start, how long they take, and how they end) and reports
	// undurableActorsTally. Nil without WithInstruments.
	instruments *Instruments
	// undurableActorsTally tallies the actors that hold an undurable local
	// snapshot on this node, for instruments to report. Nil without
	// instruments.
	undurableActorsTally *undurableActorsTally
	// retries says how long an actor's background work is held back after a
	// failure: longer with each consecutive failure for the actor.
	retries workqueue.TypedRateLimiter[string]
	// workers is how many workers drain each kind's queue: DefaultUploadWorkers
	// and DefaultDetachWorkers unless WithUploadWorkers and WithDetachWorkers
	// set them.
	workers [numOpKinds]int
	// queues hold the UIDs of actors with a background operation of each kind
	// scheduled.
	queues [numOpKinds]workqueue.TypedDelayingInterface[string]
	// reporting holds the queue and settings for reporting snapshot state
	// changes to the control plane.
	reporting reportQueue
	// workersDone counts the workers still running.
	workersDone sync.WaitGroup

	// locks holds each actor's lock. Work on an actor, including its snapshot
	// state file I/O, holds that actor's lock, so it never holds up another
	// actor.
	locks *actorlock.Locks

	// mu guards the actors map, and is never held during file I/O.
	mu sync.Mutex
	// actors maps actorUID to the entries requeueLocked keeps in memory (see
	// requeueLocked). requeueLocked drops any other entry, and findLocked then
	// reads the state from the file. Read and change it through findLocked,
	// findOrAddLocked, addLocked, dropLocked, tracksLocked, and heldLocked.
	actors map[string]*actorSnapshots
}

// Option configures a Tracker.
type Option func(*Tracker)

// WithUploadDelay sets how long after the oldest snapshot of an actor not yet
// durable was taken atelet uploads the actor's latest local snapshot.
func WithUploadDelay(d time.Duration) Option {
	return func(t *Tracker) {
		t.uploadDelay = d
	}
}

// WithDetachDelay sets how long an actor stays suspended on this node, its
// latest snapshot kept locally, before atelet detaches it.
func WithDetachDelay(d time.Duration) Option {
	return func(t *Tracker) {
		t.detachDelay = d
	}
}

// WithUploadWorkers sets how many background uploads run at once on this node.
func WithUploadWorkers(n int) Option {
	return func(t *Tracker) {
		t.workers[opUpload] = n
	}
}

// WithDetachWorkers sets how many background detaches run at once on this
// node.
func WithDetachWorkers(n int) Option {
	return func(t *Tracker) {
		t.workers[opDetach] = n
	}
}

// WithInstruments attaches the instruments that record the tracker's
// background uploads and detaches, and report the undurable local snapshots on
// this node. A nil *Instruments is a no-op.
func WithInstruments(inst *Instruments) Option {
	return func(t *Tracker) {
		t.instruments = inst
	}
}

// WithReporter sets what reports an actor's snapshot state to the control
// plane after a background upload or detach.
func WithReporter(r Reporter) Option {
	return func(t *Tracker) {
		t.reporting.reporter = r
	}
}

// WithReportWorkers sets how many background reports run at once on this node.
// Zero disables background reporting.
func WithReportWorkers(n int) Option {
	return func(t *Tracker) {
		t.reporting.workers = n
	}
}

// WithReportGiveUpAfter sets how long atelet keeps retrying a failed report
// since it became due before giving up on it.
func WithReportGiveUpAfter(d time.Duration) Option {
	return func(t *Tracker) {
		t.reporting.giveUpAfter = d
	}
}

// New returns a tracker over store that tracks no actor yet and measures time
// on clk. Its workers run once Start is called, until ctx ends. A nil store
// schedules no uploads or detaches.
func New(ctx context.Context, store Store, clk clock.WithTicker, opts ...Option) *Tracker {
	t := &Tracker{
		ctx:         ctx,
		store:       store,
		clock:       clk,
		uploadDelay: DefaultUploadDelay,
		detachDelay: DefaultDetachDelay,
		retries:     workqueue.NewTypedItemExponentialFailureRateLimiter[string](snapshotRetryDelay, maxSnapshotRetryDelay),
		workers:     [numOpKinds]int{opUpload: DefaultUploadWorkers, opDetach: DefaultDetachWorkers},
		reporting:   newReportQueue(clk),
		locks:       actorlock.New(),
		actors:      map[string]*actorSnapshots{},
	}
	for k := range t.queues {
		t.queues[k] = workqueue.NewTypedDelayingQueueWithConfig(workqueue.TypedDelayingQueueConfig[string]{Clock: clk})
	}
	for _, opt := range opts {
		opt(t)
	}
	if t.reporting.workers <= 0 {
		t.reporting.reporter = nil
	}
	if t.instruments != nil {
		t.undurableActorsTally = newUndurableActorsTally(clk, t.uploadDelay, t.store != nil)
		t.instruments.watch(t.undurableActorsTally)
	}
	context.AfterFunc(ctx, func() {
		for _, q := range t.queues {
			q.ShutDown()
		}
		t.reporting.queue.ShutDown()
	})
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

// heldLocked reports whether a Run, Restore, Checkpoint, or PromoteSnapshot of
// the actor is in progress. An entry with one stays in memory, so this never
// reads the snapshot state file.
func (t *Tracker) heldLocked(actorUID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	a := t.actors[actorUID]
	return a != nil && a.holds > 0
}

// uploadDueLocked returns when the actor's background upload is due, as
// actorSnapshots.uploadDue does. A tracker without a store uploads nothing.
func (t *Tracker) uploadDueLocked(a *actorSnapshots) (time.Time, bool) {
	if t.store == nil {
		return time.Time{}, false
	}
	return a.uploadDue(t.uploadDelay)
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
	var uploading *ateletpb.SnapshotReport
	if a.keepUploading() {
		uploading = a.Uploading.report()
	}
	st := &ateletpb.ActorSnapshotState{
		LatestLocal:   a.LatestLocal.report(),
		Uploading:     uploading,
		LatestDurable: a.LatestDurable.report(),
	}
	if due, ok := t.uploadDueLocked(a); ok {
		st.UploadDueTime = timestamppb.New(due)
	}
	return st
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
