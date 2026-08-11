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

// Package store contains common types for the persistence layer.
package store

import (
	"context"
	"errors"
	"sync"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

var (
	// ErrNotFound indicates that the given object is not present in the DB.
	ErrNotFound = errors.New("persistence: not found")

	// ErrAlreadyExists indicates that the object already exists in the DB.
	ErrAlreadyExists = errors.New("persistence: already exists")

	// ErrVersionConflict indicates a write lost to a concurrent one: either a
	// precondition pinned a version the stored object is no longer at, or the
	// store's own retry budget was exhausted losing the same race.
	ErrVersionConflict = errors.New("persistence: version conflict")

	// ErrFailedPrecondition indicates the object is not in the required state for the operation.
	ErrFailedPrecondition = errors.New("persistence: failed precondition")

	// ErrLockConflict indicates that a distributed lock is already held by another client.
	ErrLockConflict = errors.New("persistence: lock conflict")

	// ErrUIDConflict indicates a precondition pinned a uid the stored object does
	// not carry, meaning the name now addresses a different incarnation. Retrying
	// can never resolve it.
	ErrUIDConflict = errors.New("persistence: uid conflict")
)

// Interface defines the contract for the persistence layer storing actor state.
type Interface interface {
	// Fetches an actor by reference. Returns ErrNotFound if missing.
	GetActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error)

	// Stores a new actor in suspended state and returns the stored resource with
	// server-assigned metadata (uid, version, timestamps). The input is not
	// mutated. Returns ErrAlreadyExists if key is taken.
	CreateActor(ctx context.Context, actor *ateapipb.Actor) (*ateapipb.Actor, error)

	// UpdateActor performs a transactional read-modify-write and returns the updated
	// actor with advanced metadata (version, update_time).
	//
	// mutate receives the stored actor and edits it in place. The mutated actor is
	// written iff mutate returns nil. A mutate that must only land on the actor the
	// caller observed guards itself with CheckActorPrecondition.
	//
	// mutate may run more than once, because the store retries when a concurrent
	// write invalidates the transaction.
	//
	// Returns ErrNotFound if missing, ErrVersionConflict if the retry budget is
	// exhausted, or the mutate's error verbatim otherwise.
	UpdateActor(ctx context.Context, actorRef resources.ActorRef, mutate func(dbActor *ateapipb.Actor) error) (*ateapipb.Actor, error)

	// Removes an actor and returns the deleted resource. Returns ErrNotFound if
	// missing, or ErrFailedPrecondition if not suspended.
	DeleteActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error)

	// Lists actors in the given atespace (scoped scan), or across ALL atespaces if atespace is
	// empty. Returns a page of actors and a next page token.
	ListActors(ctx context.Context, atespace string, pageSize int32, pageToken string) ([]*ateapipb.Actor, string, error)

	// Creates an immutable ActorSnapshot. The caller sets snapshot_uri; the
	// store keeps no location of its own.
	CreateActorSnapshot(ctx context.Context, snapshot *ateapipb.ActorSnapshot) (*ateapipb.ActorSnapshot, error)

	// Fetches an ActorSnapshot.
	GetActorSnapshot(ctx context.Context, atespace, name string) (*ateapipb.ActorSnapshot, error)

	// Resolves an Atespace-owned tag to an ActorSnapshot in constant time.
	GetActorSnapshotByTag(ctx context.Context, atespace, name string) (*ateapipb.ActorSnapshot, *ateapipb.ActorSnapshotTag, error)

	// Lists ActorSnapshots in one atespace, or all atespaces when empty.
	ListActorSnapshots(ctx context.Context, atespace string, pageSize int32, pageToken string) ([]*ateapipb.ActorSnapshot, string, error)

	// Adds an immutable Atespace-owned tag to an ActorSnapshot.
	TagActorSnapshot(ctx context.Context, atespace, name string, tag *ateapipb.ActorSnapshotTag) (*ateapipb.ActorSnapshotTag, error)

	// Updates a tag's reuse scope.
	UpdateActorSnapshotTag(ctx context.Context, atespace, name string, scope ateapipb.ActorSnapshotTagScope, expectedVersion int64) (*ateapipb.ActorSnapshotTag, error)

	// Deletes and returns a tag.
	DeleteActorSnapshotTag(ctx context.Context, atespace, name string) (*ateapipb.ActorSnapshotTag, error)

	// Stores a new atespace and returns the stored resource with server-assigned
	// metadata (uid, version, timestamps). The input is not mutated. Returns
	// ErrAlreadyExists if the name is taken.
	CreateAtespace(ctx context.Context, atespace *ateapipb.Atespace) (*ateapipb.Atespace, error)

	// Fetches an atespace by name. Returns ErrNotFound if missing.
	GetAtespace(ctx context.Context, name string) (*ateapipb.Atespace, error)

	// Lists atespaces. Returns a page of atespaces and a next page token.
	ListAtespaces(ctx context.Context, pageSize int32, pageToken string) ([]*ateapipb.Atespace, string, error)

	// AtespaceExists reports whether the atespace object exists.
	AtespaceExists(ctx context.Context, name string) (bool, error)

	// Removes an empty atespace and returns the deleted resource. Returns
	// ErrNotFound if missing, or ErrFailedPrecondition if the atespace is not empty
	// (e.g. there are actors in it).
	DeleteAtespace(ctx context.Context, name string) (*ateapipb.Atespace, error)

	// Fetches worker state by namespace, pool, and pod name. Returns ErrNotFound if missing.
	GetWorker(ctx context.Context, namespace, pool, pod string) (*ateapipb.Worker, error)

	// Registers a new idle worker. Returns ErrAlreadyExists if already registered.
	CreateWorker(ctx context.Context, worker *ateapipb.Worker) error

	// Updates worker state with optimistic concurrency check. Returns ErrNotFound if missing, or ErrVersionConflict on version mismatch.
	UpdateWorker(ctx context.Context, worker *ateapipb.Worker, expectedVersion int64) error

	// Removes a worker. Idempotent: does nothing if worker is not found.
	DeleteWorker(ctx context.Context, namespace, pool, pod string) error

	// Lists workers. Returns a page of workers and a next page token.
	ListWorkers(ctx context.Context, pageSize int32, pageToken string) ([]*ateapipb.Worker, string, error)

	// WatchWorkers returns an active subscription to track worker state changes.
	// The watch's Events channel is closed when the caller calls Close, the
	// context is cancelled, or the underlying notification system is lost.
	// Callers should treat a closed channel as a signal to re-subscribe, and
	// must Close the watch to release its subscription.
	WatchWorkers(ctx context.Context) (*WorkerWatch, error)

	// AcquireLock attempts to acquire a distributed lock for key. The lock is
	// held and renewed automatically until the returned Lock is closed.
	// Returns ErrLockConflict if the lock is already held by another client.
	AcquireLock(ctx context.Context, key string) (*Lock, error)

	// DebugClearAll drop all data from the database. Useful for debugging / local testing/
	DebugClearAll(ctx context.Context) error
}

const (
	// AnyUID accepts whichever actor holds the atespace and name at write time.
	AnyUID = ""
	// AnyVersion accepts whatever revision the store is at.
	AnyVersion int64 = 0
)

// CheckActorPrecondition reports whether dbActor is still the actor the caller
// observed, pinned on the uid and version it read, each waivable with AnyUID or
// AnyVersion. Version guards against concurrent writes, uid against actor
// atespace/name re-use across actor lifecycles.
//
// Call it at the top of an UpdateActor mutation so the write is conditional on
// the stored actor the transaction actually read, not on one read earlier
// outside of it. Returns ErrUIDConflict or ErrVersionConflict, which UpdateActor
// surfaces verbatim.
func CheckActorPrecondition(dbActor *ateapipb.Actor, uid string, version int64) error {
	md := dbActor.GetMetadata()
	if uid != AnyUID && uid != md.GetUid() {
		return ErrUIDConflict
	}
	if version != AnyVersion && version != md.GetVersion() {
		return ErrVersionConflict
	}
	return nil
}

// WorkerEventType indicates the type of change to a Worker.
type WorkerEventType int

const (
	WorkerEventCreated WorkerEventType = iota
	WorkerEventUpdated
	WorkerEventDeleted
)

// WorkerEvent carries a single worker state change notification.
type WorkerEvent struct {
	Type   WorkerEventType
	Worker *ateapipb.Worker
}

// WorkerWatch is an active subscription to worker state changes. The caller
// must call Close when done to release the underlying subscription. Events is
// closed when Close is called, the originating context is cancelled, or the
// underlying notification system is lost.
type WorkerWatch struct {
	// Events delivers worker state changes until the watch is torn down.
	Events <-chan WorkerEvent
	// stop releases the subscription backing Events. It is a context.CancelFunc,
	// so it is safe to call multiple times.
	stop context.CancelFunc
}

// NewWorkerWatch builds a WorkerWatch from an events channel and the cancel
// func that tears down its subscription.
func NewWorkerWatch(events <-chan WorkerEvent, stop context.CancelFunc) *WorkerWatch {
	return &WorkerWatch{Events: events, stop: stop}
}

// Close releases the subscription. Safe to call multiple times.
func (w *WorkerWatch) Close() { w.stop() }

// Lock represents a held distributed lock that is renewed automatically until
// Close is called. If renewal cannot keep the lease alive, the context
// returned by Context is cancelled so the caller can detect it may no
// longer have exclusive access.
type Lock struct {
	ctx     context.Context
	closeFn func()
	once    sync.Once
}

// NewLock builds a Lock from its lease context (cancelled on loss or Close)
// and the func that stops lease renewal and releases the lock.
func NewLock(ctx context.Context, closeFn func()) *Lock {
	return &Lock{ctx: ctx, closeFn: closeFn}
}

// Context returns a context derived from the context AcquireLock was called
// with. It is cancelled when Close is called, or earlier if the lease is
// lost.
func (l *Lock) Context() context.Context { return l.ctx }

// Close stops lease renewal and releases the lock. Safe to call multiple
// times.
func (l *Lock) Close() { l.once.Do(l.closeFn) }
