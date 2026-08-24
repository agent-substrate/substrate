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
	"fmt"
	"sync"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrNotFound indicates that the given object is not present in the DB.
	ErrNotFound = errors.New("persistence: not found")

	// ErrAlreadyExists indicates that the object already exists in the DB.
	ErrAlreadyExists = errors.New("persistence: already exists")

	// ErrVersionConflict indicates a write lost to a concurrent one: either the
	// write was guarded on a version the stored object is no longer at, or the
	// store's own retry budget was exhausted losing the same race.
	ErrVersionConflict = errors.New("persistence: version conflict")

	// ErrFailedPrecondition indicates the object is not in the required state for the operation.
	ErrFailedPrecondition = errors.New("persistence: failed precondition")

	// ErrLockConflict indicates that a distributed lock is already held by another client.
	ErrLockConflict = errors.New("persistence: lock conflict")

	// ErrInvalidPageToken indicates that a list page token is malformed or was
	// issued for a different list operation or scope.
	ErrInvalidPageToken = errors.New("persistence: invalid page token")

	// ErrInvalidPageSize indicates that a negative page size was supplied.
	ErrInvalidPageSize = errors.New("persistence: invalid page size")

	// ErrUIDConflict indicates a write was guarded on a uid the stored object does
	// not carry, meaning the name now addresses a different incarnation. Retrying
	// can never resolve it.
	ErrUIDConflict = errors.New("persistence: uid conflict")

	// ErrPreconditionRequired indicates an update was called with a precondition
	// missing either guard (uid or version). Blind writes are not accepted.
	ErrPreconditionRequired = errors.New("persistence: precondition required")

	// ErrImmutableField indicates an update's mutation changed a field that is
	// immutable for the lifetime of the stored object.
	ErrImmutableField = errors.New("persistence: immutable field")
)

// Interface defines the contract for the persistence layer storing actor state.
type Interface interface {
	// Stores a new actor in suspended state and returns the stored resource with
	// server-assigned metadata (uid, version, timestamps). The input is not
	// mutated. Returns ErrAlreadyExists if key is taken, or
	// ErrFailedPrecondition if the actor's atespace does not exist.
	CreateActor(ctx context.Context, actor *ateapipb.Actor) (*ateapipb.Actor, error)

	// Fetches an actor by reference. Returns ErrNotFound if missing.
	GetActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error)

	// Lists actors in the given atespace (scoped scan), or across ALL atespaces if atespace is
	// empty.
	ListActors(ctx context.Context, atespace string, opts ListOptions) (ListResponse[*ateapipb.Actor], error)

	// UpdateActor performs a transactional read-modify-write and returns the stored
	// actor with advanced metadata (version, update_time).
	//
	// precondition guards the write against landing on unexpected state: it is
	// checked against the stored actor before mutate runs. Both the uid and
	// version guards are required.
	//
	// mutate receives the stored actor and edits it in place. The mutated actor is
	// written iff mutate returns nil.
	//
	// mutate may run more than once, because the store retries when a concurrent
	// write invalidates the transaction.
	//
	// Returns ErrPreconditionRequired if the precondition omits either guard,
	// ErrNotFound if missing, ErrUIDConflict or ErrVersionConflict if the
	// precondition no longer holds, ErrVersionConflict if the retry budget is
	// exhausted, ErrImmutableField if the mutated actor changed a field that is
	// immutable for its lifetime, or the mutate's error verbatim otherwise.
	UpdateActor(ctx context.Context, actorRef resources.ActorRef, precondition Precondition, mutate func(toUpdate *ateapipb.Actor) error) (*ateapipb.Actor, error)

	// Removes an actor and returns the deleted resource. Returns ErrNotFound if
	// missing, or ErrFailedPrecondition if not already deleting.
	DeleteActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error)

	// Creates an immutable ActorSnapshot. The caller sets snapshot_uri; the
	// store keeps no location of its own.
	CreateActorSnapshot(ctx context.Context, snapshot *ateapipb.ActorSnapshot) (*ateapipb.ActorSnapshot, error)

	// Fetches an ActorSnapshot by reference. Returns ErrNotFound if missing.
	GetActorSnapshot(ctx context.Context, snapshotRef resources.ActorSnapshotRef) (*ateapipb.ActorSnapshot, error)

	// Lists ActorSnapshots in one atespace, or all atespaces when empty.
	ListActorSnapshots(ctx context.Context, atespace string, opts ListOptions) (ListResponse[*ateapipb.ActorSnapshot], error)

	// Adds an immutable Atespace-owned tag to the ActorSnapshot addressed by
	// snapshotRef. Returns ErrNotFound if the snapshot does not exist, or
	// ErrFailedPrecondition if the tag's atespace does not exist.
	CreateActorSnapshotTag(ctx context.Context, snapshotRef resources.ActorSnapshotRef, tag *ateapipb.ActorSnapshotTag) (*ateapipb.ActorSnapshotTag, error)

	// Fetches an Atespace-owned tag by reference. Returns ErrNotFound if
	// missing. The tag's snapshot field names the ActorSnapshot it resolves
	// to; fetch it with GetActorSnapshot if needed.
	GetActorSnapshotTag(ctx context.Context, tagRef resources.ActorSnapshotTagRef) (*ateapipb.ActorSnapshotTag, error)

	// UpdateActorSnapshotTag performs a transactional read-modify-write on the tag
	// addressed by tagRef, and returns the stored ActorSnapshotTag with
	// advanced metadata (version, update_time).
	//
	// precondition guards the write against landing on unexpected state: it is
	// checked against the stored tag before mutate runs. Both the uid and version
	// guards are required.
	//
	// mutate receives the stored tag and edits it in place. The mutated tag is
	// written iff mutate returns nil.
	//
	// mutate may run more than once, because the store retries when a concurrent
	// write invalidates the transaction.
	//
	// Returns ErrPreconditionRequired if the precondition omits either guard,
	// ErrNotFound if missing, ErrUIDConflict or ErrVersionConflict if the
	// precondition no longer holds, ErrVersionConflict if the retry budget is
	// exhausted, ErrImmutableField if the mutated tag changed a field that is
	// immutable for its lifetime, or the mutate's error verbatim otherwise.
	UpdateActorSnapshotTag(ctx context.Context, tagRef resources.ActorSnapshotTagRef, precondition Precondition, mutate func(toUpdate *ateapipb.ActorSnapshotTag) error) (*ateapipb.ActorSnapshotTag, error)

	// Deletes and returns a tag.
	DeleteActorSnapshotTag(ctx context.Context, tagRef resources.ActorSnapshotTagRef) (*ateapipb.ActorSnapshotTag, error)

	// Stores a new atespace and returns the stored resource with server-assigned
	// metadata (uid, version, timestamps). The input is not mutated. Returns
	// ErrAlreadyExists if the name is taken.
	CreateAtespace(ctx context.Context, atespace *ateapipb.Atespace) (*ateapipb.Atespace, error)

	// Fetches an atespace by name. Returns ErrNotFound if missing.
	GetAtespace(ctx context.Context, name string) (*ateapipb.Atespace, error)

	// Lists atespaces.
	ListAtespaces(ctx context.Context, opts ListOptions) (ListResponse[*ateapipb.Atespace], error)

	// Removes an empty atespace and returns the deleted resource. Returns
	// ErrNotFound if missing, or ErrFailedPrecondition if the atespace is not empty
	// (e.g. there are actors in it).
	DeleteAtespace(ctx context.Context, name string) (*ateapipb.Atespace, error)

	// Stores a new ActorTemplate and returns the stored resource with
	// server-assigned metadata (uid, version, timestamps). The input is not
	// mutated. Returns ErrAlreadyExists if the (atespace, name) is taken.
	CreateActorTemplate(ctx context.Context, template *ateapipb.ActorTemplate) (*ateapipb.ActorTemplate, error)

	// Fetches an ActorTemplate by reference. Returns ErrNotFound if missing.
	GetActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef) (*ateapipb.ActorTemplate, error)

	// Lists ActorTemplates in an atespace, or across all atespaces when
	// atespace is empty.
	ListActorTemplates(ctx context.Context, atespace string, opts ListOptions) (ListResponse[*ateapipb.ActorTemplate], error)

	// UpdateActorTemplate performs a transactional read-modify-write and returns
	// the updated template with advanced metadata (version, update_time).
	//
	// precondition guards the write against landing on unexpected state: it is
	// checked against the stored template before mutate runs. Both the uid and
	// version guards are required.
	UpdateActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef, precondition Precondition, mutate func(dbTemplate *ateapipb.ActorTemplate) error) (*ateapipb.ActorTemplate, error)

	// Removes an ActorTemplate and returns the deleted resource. Returns
	// ErrNotFound if missing.
	DeleteActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef) (*ateapipb.ActorTemplate, error)

	// Registers a new idle worker and returns the stored resource with
	// server-assigned metadata (uid, version, timestamps). The input is not
	// mutated. Returns ErrAlreadyExists if already registered.
	CreateWorker(ctx context.Context, worker *ateapipb.Worker) (*ateapipb.Worker, error)

	// Fetches worker state by name. Returns ErrNotFound if missing.
	GetWorker(ctx context.Context, name string) (*ateapipb.Worker, error)

	// Lists workers.
	ListWorkers(ctx context.Context, opts ListOptions) (ListResponse[*ateapipb.Worker], error)

	// UpdateWorker performs a transactional read-modify-write and returns the
	// stored worker with advanced metadata (version, update_time).
	//
	// precondition guards the write against landing on unexpected state: it is
	// checked against the stored worker before mutate runs. Both the uid and
	// version guards are required.
	//
	// Returns ErrPreconditionRequired if the precondition omits either guard,
	// ErrNotFound if missing, ErrUIDConflict or ErrVersionConflict if the
	// precondition no longer holds, ErrVersionConflict if the retry budget is
	// exhausted, or the mutate's error verbatim otherwise.
	UpdateWorker(ctx context.Context, name string, precondition Precondition, mutate func(toUpdate *ateapipb.Worker) error) (*ateapipb.Worker, error)

	// Removes a worker by name and returns the deleted resource. Returns
	// ErrNotFound if missing, or ErrUIDConflict/ErrVersionConflict if pre does
	// not describe the worker the caller observed.
	DeleteWorker(ctx context.Context, name string, pre DeletePreconditions) (*ateapipb.Worker, error)

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

// Precondition guards an update with the uid and version the caller observed:
// the write lands only if the stored object still matches both. Both fields are
// required.
type Precondition struct {
	// UID is the incarnation the write is for.
	UID string
	// Version is the revision the write is against.
	Version int64
}

// DeletePreconditions pins the object incarnation a delete may act on. Unlike
// Precondition, whose guards an update requires, each guard here is
// independently waivable: the zero value pins nothing, which is what an
// unguarded delete wants.
type DeletePreconditions struct {
	// UID accepts only the object carrying it; empty accepts whichever object
	// holds the name at delete time.
	UID string
	// Version accepts only that revision; zero accepts whatever revision the
	// store is at.
	Version int64
}

// Check reports whether md still describes the object the caller observed.
// A waived guard is not checked. The uid is reported first: a new incarnation
// makes the version meaningless.
//
// Returns ErrUIDConflict or ErrVersionConflict, which the delete surfaces
// verbatim.
func (p DeletePreconditions) Check(md *ateapipb.ResourceMetadata) error {
	if p.UID != "" && p.UID != md.GetUid() {
		return ErrUIDConflict
	}
	if p.Version != 0 && p.Version != md.GetVersion() {
		return ErrVersionConflict
	}
	return nil
}

// CheckWorkerMutation reports whether an UpdateWorker mutation left the
// worker's immutable identity fields alone. A backend calls it between running
// the mutation and writing the result. It lives here, above any one backend,
// so the rule is stated once and a second backend inherits it rather than
// restating it.
//
// metadata is not checked: a backend re-stamps it from the object it read, so
// whatever the mutation made of it is discarded either way.
//
// capacity is checked along with the rest because UpdateWorker replaces the
// worker rather than patching it: a request that omits capacity is asking to
// clear it, and silently losing a worker's compute capacity is worse than
// rejecting the write. A future pod resize has to relax this rule first.
//
// A rejection wraps ErrImmutableField, so a backend can return it as-is and
// callers still get the sentinel they map to INVALID_ARGUMENT.
func CheckWorkerMutation(stored, mutated *ateapipb.Worker) error {
	for _, f := range []struct {
		name    string
		stored  string
		mutated string
	}{
		{"worker_namespace", stored.GetWorkerNamespace(), mutated.GetWorkerNamespace()},
		{"worker_pool", stored.GetWorkerPool(), mutated.GetWorkerPool()},
		{"worker_pod", stored.GetWorkerPod(), mutated.GetWorkerPod()},
		{"worker_pod_uid", stored.GetWorkerPodUid(), mutated.GetWorkerPodUid()},
		{"node_name", stored.GetNodeName(), mutated.GetNodeName()},
		{"ip", stored.GetIp(), mutated.GetIp()},
	} {
		if f.stored != f.mutated {
			return fmt.Errorf("%w: %s changed from %q to %q", ErrImmutableField, f.name, f.stored, f.mutated)
		}
	}
	if !proto.Equal(stored.GetCapacity(), mutated.GetCapacity()) {
		return fmt.Errorf("%w: capacity changed from %v to %v", ErrImmutableField, stored.GetCapacity(), mutated.GetCapacity())
	}
	return nil
}

// hasResourceMetadata is an object the store addresses by atespace and name,
// and whose identity a caller can guard with a Precondition.
type hasResourceMetadata interface {
	GetMetadata() *ateapipb.ResourceMetadata
}

// PreconditionFrom builds the guards from the object the caller observed: its
// uid and version.
func PreconditionFrom[T hasResourceMetadata](observed T) Precondition {
	md := observed.GetMetadata()
	return Precondition{UID: md.GetUid(), Version: md.GetVersion()}
}

// Validate reports whether the precondition carries both required guards (uid and version)
//
// Returns ErrPreconditionRequired, which the update surfaces verbatim.
func (p Precondition) Validate() error {
	if p.UID == "" {
		return fmt.Errorf("%w: uid", ErrPreconditionRequired)
	}
	if p.Version == 0 {
		return fmt.Errorf("%w: version", ErrPreconditionRequired)
	}
	return nil
}

// Check reports whether md still matches the guards p carries. The uid is reported
// first: a new incarnation makes the version meaningless.
func (p Precondition) Check(md *ateapipb.ResourceMetadata) error {
	if p.UID != md.GetUid() {
		return ErrUIDConflict
	}
	if p.Version != md.GetVersion() {
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

// ListOptions carries the pagination parameters common to every List method.
type ListOptions struct {
	// PageSize caps how many items a single call returns.
	PageSize int32
	// PageToken resumes a listing after the page it was issued for. Empty
	// starts from the first page.
	PageToken string
}

// DefaultPageSize is used by store implementations when PageSize is unset.
const DefaultPageSize int32 = 1000

// NormalizeListOptions applies the store default and rejects invalid sizes.
// RPC handlers validate user input separately, but store callers also need a
// safe contract because list implementations use PageSize in slice indexes.
func NormalizeListOptions(opts ListOptions) (ListOptions, error) {
	if opts.PageSize < 0 {
		return ListOptions{}, ErrInvalidPageSize
	}
	if opts.PageSize == 0 {
		opts.PageSize = DefaultPageSize
	}
	return opts, nil
}

// ListResponse is the return value of a List method: the page of items it
// addressed, plus the token to fetch the next page. NextPageToken is empty
// once the listing has reached its last page.
type ListResponse[T any] struct {
	Items         []T
	NextPageToken string
}

// HasNextPage reports whether another page follows this one.
func (r ListResponse[T]) HasNextPage() bool {
	return r.NextPageToken != ""
}
