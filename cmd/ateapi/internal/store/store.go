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

	// Lists all known actors. Returns nil if none found.
	ListActors(ctx context.Context) ([]*ateapipb.Actor, error)

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
