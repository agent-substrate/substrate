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
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

var (
	// ErrNotFound indicates that the given object is not present in the DB.
	ErrNotFound = errors.New("persistence: not found")

	// ErrAlreadyExists indicates that the object already exists in the DB.
	ErrAlreadyExists = errors.New("persistence: already exists")

	// ErrPersistenceRetry is the error returned when the persistence layer needs to retry.
	ErrPersistenceRetry = errors.New("persistence: retry status")

	// ErrFailedPrecondition indicates the object is not in the required state for the operation.
	ErrFailedPrecondition = errors.New("persistence: failed precondition")
)

// Interface defines the contract for the persistence layer storing actor state.
type Interface interface {
	// Fetches an actor by id. Returns ErrNotFound if missing.
	GetActor(ctx context.Context, id string) (*ateapipb.Actor, error)

	// Stores a new actor in suspended state. Returns ErrAlreadyExists if key is taken.
	CreateActor(ctx context.Context, actor *ateapipb.Actor) error

	// Updates actor state with optimistic concurrency check. Returns ErrNotFound if missing, or ErrPersistenceRetry on version mismatch.
	UpdateActor(ctx context.Context, actor *ateapipb.Actor, expectedVersion int64) error

	// Removes an actor. Returns ErrNotFound if missing, or ErrFailedPrecondition if not suspended.
	DeleteActor(ctx context.Context, id string) error

	// Lists all known actors. Returns a page of actors and a next page token.
	ListActors(ctx context.Context, pageSize int32, pageToken string) ([]*ateapipb.Actor, string, error)

	// Fetches worker state by namespace, pool, and pod name. Returns ErrNotFound if missing.
	GetWorker(ctx context.Context, namespace, pool, pod string) (*ateapipb.Worker, error)

	// Registers a new idle worker. Returns ErrAlreadyExists if already registered.
	CreateWorker(ctx context.Context, worker *ateapipb.Worker) error

	// Updates worker state with optimistic concurrency check. Returns ErrNotFound if missing, or ErrPersistenceRetry on version mismatch.
	UpdateWorker(ctx context.Context, worker *ateapipb.Worker, expectedVersion int64) error

	// Removes a worker. Idempotent: does nothing if worker is not found.
	DeleteWorker(ctx context.Context, namespace, pool, pod string) error

	// Lists all known workers. Returns nil if none found.
	ListWorkers(ctx context.Context) ([]*ateapipb.Worker, error)

	// WatchWorkers returns an active subscription to track worker state changes.
	// The watch's Events channel is closed when the caller calls Close, the
	// context is cancelled, or the underlying notification system is lost.
	// Callers should treat a closed channel as a signal to re-subscribe, and
	// must Close the watch to release its subscription.
	WatchWorkers(ctx context.Context) (*WorkerWatch, error)

	// AcquireLock attempts to acquire a distributed lock with a TTL.
	// Returns true if the lock was successfully acquired.
	// Returns false if the lock is already held by another client (conflict).
	// Returns an error only on database failure.
	// The value must be a unique token (e.g., UUID) to ensure safe release.
	AcquireLock(ctx context.Context, key string, value string, ttl time.Duration) (bool, error)

	// ReleaseLock releases a distributed lock if the stored value matches the passed value.
	// Returns nil if the lock was successfully released or if the lock was not held by this value.
	// Returns an error only on database failure.
	ReleaseLock(ctx context.Context, key string, value string) error

	// DebugClearAll drop all data from the database. Useful for debugging / local testing/
	DebugClearAll(ctx context.Context) error
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
