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

package controlapi

import (
	"context"
	"errors"
	"fmt"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/admission"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/apivalidation"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/defaults"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// ListWorkerActorAssignments lists the Actors a Worker hosts. The assignments are a
// subresource rather than a field on Worker, so this is the only way to read
// them and neither GetWorker nor ListWorkers grows with occupancy.
func (s *RPCService) ListWorkerActorAssignments(ctx context.Context, req *ateapipb.ListWorkerActorAssignmentsRequest) (*ateapipb.ListWorkerActorAssignmentsResponse, error) {
	if errs := apivalidation.ValidateListWorkerActorAssignmentsRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	name := req.GetWorker().GetName()

	// The Worker is read first so a listing against one that does not exist is
	// NOT_FOUND rather than an empty page, which a caller cannot tell from a
	// Worker hosting nothing.
	if _, err := s.admission.GetWorker(ctx, name); err != nil {
		// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Worker %s not found", name)
		}
		return nil, fmt.Errorf("while fetching worker %s: %w", name, err)
	}

	page, err := s.admission.ListWorkerAssignments(ctx, name,
		store.ListOptions{PageSize: effectivePageSize(req.GetPageSize()), PageToken: req.GetPageToken()})
	if err != nil {
		return nil, mapListError(fmt.Errorf("while listing the assignments of worker %s: %w", name, err))
	}
	return &ateapipb.ListWorkerActorAssignmentsResponse{
		ActorAssignments: page.Items,
		NextPageToken:    page.NextPageToken,
	}, nil
}

func (s *RPCService) ListWorkers(ctx context.Context, req *ateapipb.ListWorkersRequest) (*ateapipb.ListWorkersResponse, error) {
	if errs := apivalidation.ValidateListWorkersRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	page, err := s.admission.ListWorkers(ctx, store.ListOptions{PageSize: effectivePageSize(req.GetPageSize()), PageToken: req.GetPageToken()})
	if err != nil {
		return nil, mapListError(fmt.Errorf("while listing workers in db: %w", err))
	}
	return &ateapipb.ListWorkersResponse{
		Workers:       page.Items,
		NextPageToken: page.NextPageToken,
	}, nil
}

func (s *RPCService) GetWorker(ctx context.Context, req *ateapipb.GetWorkerRequest) (*ateapipb.Worker, error) {
	if errs := apivalidation.ValidateGetWorkerRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	name := req.GetWorker().GetName()

	worker, err := s.admission.GetWorker(ctx, name)
	// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
	if errors.Is(err, store.ErrNotFound) {
		return nil, apierror.NotFound("Worker %s not found", name)
	}
	if err != nil {
		return nil, fmt.Errorf("while getting worker: %w", err)
	}
	return worker, nil
}

func (s *RPCService) CreateWorker(ctx context.Context, req *ateapipb.CreateWorkerRequest) (*ateapipb.Worker, error) {
	// First scrub any fields that callers are not allowed to set. status is
	// output-only, so whatever the request carried there is replaced rather
	// than rejected.
	inWorker := req.Worker
	if inWorker != nil { // otherwise validation will flag it
		scrubResourceMetadataForCreate(inWorker.Metadata)
		inWorker.Status = nil
		defaults.Apply(inWorker)
	}

	// Validate the request, including the object within it.
	if errs := apivalidation.ValidateCreateWorkerRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	created, err := s.admission.CreateWorker(ctx, inWorker)
	if err != nil {
		// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
		switch {
		case errors.Is(err, store.ErrAlreadyExists):
			return nil, apierror.AlreadyExists("Worker %s already exists", inWorker.GetMetadata().GetName())
		case errors.Is(err, admission.ErrInvalid):
			return nil, apierror.InvalidArgument("%w", err)
		default:
			return nil, fmt.Errorf("while creating worker: %w", err)
		}
	}
	return created, nil
}

// UpdateWorker replaces the stored Worker with the one the request carries.
// Only labels and epoch are the caller's to change; a request that alters an
// immutable field — including by leaving it unset, which would clear it — is
// rejected.
func (s *RPCService) UpdateWorker(ctx context.Context, req *ateapipb.UpdateWorkerRequest) (*ateapipb.Worker, error) {
	// First scrub any fields that callers are not allowed to set.
	inWorker := req.Worker
	if inWorker != nil { // otherwise validation will flag it
		scrubResourceMetadataForUpdate(inWorker.Metadata)
		inWorker.Status = nil
	}

	// Validate the request.
	if errs := apivalidation.ValidateUpdateWorkerRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	worker, err := s.admission.UpdateWorkerSpec(ctx, inWorker)
	return mapWorkerUpdate(inWorker.GetMetadata().GetName(), worker, err)
}

func (s *RPCService) DeleteWorker(ctx context.Context, req *ateapipb.DeleteWorkerRequest) (*ateapipb.Worker, error) {
	if errs := apivalidation.ValidateDeleteWorkerRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	// The delete releases the Actor bound to this Worker before removing the
	// record, so it is a workflow rather than a single store call.
	return s.workerWorkflow.DeleteWorker(ctx, req.GetWorker().GetName(), toDeletePreconditions(req.GetOptions()))
}

func (s *RPCService) DrainWorker(ctx context.Context, req *ateapipb.DrainWorkerRequest) (*ateapipb.Worker, error) {
	if errs := apivalidation.ValidateDrainWorkerRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	name := req.GetWorker().GetName()

	// A DrainWorkerRequest names a worker and carries no guards, so the ones
	// the store requires come from a read here rather than from the client. A
	// write that lands in between is reported as a conflict for the caller to
	// retry, the same as any other guarded update.
	observed, err := s.admission.GetWorker(ctx, name)
	// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
	if errors.Is(err, store.ErrNotFound) {
		return nil, apierror.NotFound("Worker %s not found", name)
	}
	if err != nil {
		return nil, fmt.Errorf("while getting worker to drain: %w", err)
	}

	worker, err := s.admission.UpdateWorkerStatus(ctx, name, store.PreconditionFrom(observed), func(status *ateapipb.WorkerStatus) error {
		if status.GetState() == ateapipb.WorkerState_WORKER_STATE_DRAINING {
			// already draining, do nothing
			return errWorkerUnchanged
		}
		status.State = ateapipb.WorkerState_WORKER_STATE_DRAINING
		// The assignments are left alone: a draining Worker keeps its Actors
		// until something releases them. Draining only stops new placements.
		return nil
	})
	if errors.Is(err, errWorkerUnchanged) {
		return observed, nil
	}
	return mapWorkerUpdate(name, worker, err)
}

func mapWorkerUpdate(name string, worker *ateapipb.Worker, err error) (*ateapipb.Worker, error) {
	if err == nil {
		return worker, nil
	}
	// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
	switch {
	case errors.Is(err, store.ErrNotFound):
		return nil, apierror.NotFound("Worker %s not found", name)
	case errors.Is(err, store.ErrUIDConflict):
		return nil, apierror.Aborted("Worker %s is not the one the request describes", name)
	case errors.Is(err, store.ErrVersionConflict):
		return nil, apierror.Aborted("concurrent update conflict, please retry")
	case errors.Is(err, store.ErrPreconditionRequired):
		return nil, apierror.InvalidArgument("while updating worker %s: %v", name, err)
	case errors.Is(err, admission.ErrInvalid):
		return nil, apierror.InvalidArgument("%w", err)
	default:
		return nil, fmt.Errorf("while updating worker: %w", err)
	}
}

// errWorkerUnchanged ends an UpdateWorkerStatus mutation that found its work
// already done. The store hands a mutation's error straight back and leaves
// the Worker — and its version — untouched, which is what lets DrainWorker be
// idempotent: a call with nothing left to do costs no version bump.
var errWorkerUnchanged = errors.New("worker is already in the requested state")
