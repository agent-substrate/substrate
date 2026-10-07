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

package admission

import (
	"context"
	"fmt"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/apivalidation"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/defaults"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// CreateWorker validates the input worker, initializes its server-owned status,
// validates the final object, and stores it.
//
// Returns ErrInvalid if inWorker fails declarative create validation, or
// store.ErrAlreadyExists if a worker with the same name is already registered.
func (a *Admission) CreateWorker(ctx context.Context, inWorker *ateapipb.Worker) (*ateapipb.Worker, error) {
	specWorker := proto.CloneOf(inWorker)
	specWorker.Status = nil
	defaults.Apply(specWorker)

	fldPath := field.NewPath("worker")
	if errs := apivalidation.ValidateWorkerCreate(ctx, fldPath, specWorker); len(errs) > 0 {
		return nil, invalidf("%v", errs.ToAggregate())
	}

	// A Worker is registered only once its pod is Ready and has an IP, which
	// makes ACTIVE the only state it can be born in.
	outWorker := proto.CloneOf(specWorker)
	// A new Worker hosts no Actors, so none are left from an earlier epoch.
	outWorker.Status = &ateapipb.WorkerStatus{
		State:         ateapipb.WorkerState_WORKER_STATE_ACTIVE,
		ObservedEpoch: specWorker.GetEpoch(),
	}

	// Capacity is left unset: a Worker holds nothing until its own ateom says
	// what it has, through WorkerService.SetWorkerCapacity. Nothing is placed
	// on it in the meantime, which is the point -- the alternative is guessing
	// on the Worker's behalf and placing against the guess.

	if errs := apivalidation.ValidateWorkerUpdate(ctx, fldPath, outWorker, specWorker, true); len(errs) > 0 {
		return nil, fmt.Errorf("%v", errs.ToAggregate())
	}

	return a.store.CreateWorker(ctx, outWorker)
}

// GetWorker retrieves a Worker by name.
//
// Returns store.ErrNotFound if the worker does not exist.
func (a *Admission) GetWorker(ctx context.Context, name string) (*ateapipb.Worker, error) {
	return a.store.GetWorker(ctx, name)
}

// ListWorkers lists all registered Workers.
//
// Returns store.ErrInvalidPageSize or store.ErrInvalidPageToken if pagination
// options are invalid.
func (a *Admission) ListWorkers(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapipb.Worker], error) {
	return a.store.ListWorkers(ctx, opts)
}

// UpdateWorkerSpec replaces the caller-mutable specification fields of a
// Worker, preserving server-owned metadata and status, and enforcing
// immutability and declarative validation.
//
// Returns store.ErrPreconditionRequired if metadata uid or version is unset,
// store.ErrNotFound if the worker does not exist, store.ErrUIDConflict or
// store.ErrVersionConflict if the precondition does not match the stored
// worker, or ErrInvalid if the updated spec fails declarative validation
// (including immutable field mutations).
func (a *Admission) UpdateWorkerSpec(ctx context.Context, inWorker *ateapipb.Worker) (*ateapipb.Worker, error) {
	name := inWorker.GetMetadata().GetName()
	return a.store.UpdateWorker(ctx, name, store.PreconditionFrom(inWorker), func(toUpdate *ateapipb.Worker) error {
		oldVal := proto.CloneOf(toUpdate)

		// Status and metadata are server-owned fields.
		status, metadata := toUpdate.GetStatus(), toUpdate.GetMetadata()
		proto.Reset(toUpdate)
		proto.Merge(toUpdate, inWorker)
		toUpdate.Status = status
		toUpdate.Metadata = metadata
		// Defaults are re-applied to the merged object, so a defaulted field
		// the request left unset is defaulted again rather than cleared.
		defaults.Apply(toUpdate)
		newVal := toUpdate

		// Validate the mutated value before doing any further work. This is
		// what enforces the immutable fields, since only the stored worker
		// gives declarative validation an old value to compare against.
		if errs := apivalidation.ValidateWorkerUpdate(ctx, field.NewPath("worker"), newVal, oldVal, false); len(errs) > 0 {
			return invalidf("%v", errs.ToAggregate())
		}

		// Validate the final value before storing it.
		if errs := apivalidation.ValidateWorkerUpdate(ctx, field.NewPath("worker"), newVal, oldVal, true); len(errs) > 0 {
			return fmt.Errorf("%v", errs.ToAggregate())
		}

		return nil
	})
}

// UpdateWorkerStatus updates the server-owned status of a Worker and validates
// the resulting resource before storing it.
//
// Returns store.ErrPreconditionRequired if precondition omits uid or version,
// store.ErrNotFound if the worker does not exist, store.ErrUIDConflict or
// store.ErrVersionConflict if precondition does not match the stored worker, or
// the error returned by mutate verbatim.
func (a *Admission) UpdateWorkerStatus(ctx context.Context, name string, precondition store.Precondition, mutate func(*ateapipb.WorkerStatus) error) (*ateapipb.Worker, error) {
	return a.store.UpdateWorker(ctx, name, precondition, func(toUpdate *ateapipb.Worker) error {
		oldVal := proto.CloneOf(toUpdate)
		if err := mutate(toUpdate.Status); err != nil {
			return err
		}
		if errs := apivalidation.ValidateWorkerUpdate(ctx, field.NewPath("worker"), toUpdate, oldVal, true); len(errs) > 0 {
			return fmt.Errorf("%v", errs.ToAggregate())
		}
		return nil
	})
}

// DeleteWorker deletes a Worker by name.
//
// Returns store.ErrNotFound if the worker does not exist, or
// store.ErrUIDConflict or store.ErrVersionConflict if precondition does not
// match the stored worker.
func (a *Admission) DeleteWorker(ctx context.Context, name string, precondition store.DeletePreconditions) (*ateapipb.Worker, error) {
	return a.store.DeleteWorker(ctx, name, precondition)
}

// BindActorToWorker binds an ActorAssignment to a Worker under the Worker's row lock.
//
// Returns store.ErrNotFound if the worker does not exist,
// store.ErrVersionConflict on concurrent modification, or the error returned by
// admit verbatim.
func (a *Admission) BindActorToWorker(ctx context.Context, workerName string, assignment *ateapipb.ActorAssignment, admit func(*ateapipb.Worker) error) error {
	return a.store.BindActorToWorker(ctx, workerName, assignment, admit)
}

// ReleaseActorFromWorker releases an ActorAssignment from a Worker. It returns
// a nil Worker and nil error if the assignment was already absent.
//
// Returns store.ErrNotFound if the worker does not exist, or
// store.ErrVersionConflict on concurrent modification.
func (a *Admission) ReleaseActorFromWorker(ctx context.Context, workerName string, actorUID string) (*ateapipb.Worker, error) {
	return a.store.ReleaseActorFromWorker(ctx, workerName, actorUID)
}

// GetWorkerAssignment retrieves a single ActorAssignment on a Worker.
//
// Returns store.ErrNotFound if the worker is not hosting actorUID.
func (a *Admission) GetWorkerAssignment(ctx context.Context, workerName, actorUID string) (*ateapipb.ActorAssignment, error) {
	return a.store.GetWorkerAssignment(ctx, workerName, actorUID)
}

// ListWorkerAssignments lists the ActorAssignments hosted by a Worker.
//
// Returns store.ErrInvalidPageSize or store.ErrInvalidPageToken if pagination
// options are invalid.
func (a *Admission) ListWorkerAssignments(ctx context.Context, workerName string, opts store.ListOptions) (store.ListResponse[*ateapipb.ActorAssignment], error) {
	return a.store.ListWorkerAssignments(ctx, workerName, opts)
}

// FindWorkerHostingActor returns the name of the Worker hosting actorUID.
//
// Returns store.ErrNotFound if no worker holds an assignment for actorUID.
func (a *Admission) FindWorkerHostingActor(ctx context.Context, actorUID string) (string, error) {
	return a.store.FindWorkerHostingActor(ctx, actorUID)
}

// WatchWorkers opens a watch stream of Worker changes.
func (a *Admission) WatchWorkers(ctx context.Context) (*store.WorkerWatch, error) {
	return a.store.WatchWorkers(ctx)
}
