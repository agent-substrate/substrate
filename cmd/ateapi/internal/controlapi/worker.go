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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/validate"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *RPCService) ListWorkers(ctx context.Context, req *ateapipb.ListWorkersRequest) (*ateapipb.ListWorkersResponse, error) {
	if errs := validateListWorkersRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	page, err := s.impl.ListWorkers(ctx, store.ListOptions{PageSize: effectivePageSize(req.GetPageSize()), PageToken: req.GetPageToken()})
	if err != nil {
		return nil, mapListError(fmt.Errorf("while listing workers in db: %w", err))
	}
	return &ateapipb.ListWorkersResponse{
		Workers:       page.Items,
		NextPageToken: page.NextPageToken,
	}, nil
}

func (s *ServiceImpl) ListWorkers(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapipb.Worker], error) {
	return s.store.ListWorkers(ctx, opts)
}

func validateListWorkersRequest(ctx context.Context, req *ateapipb.ListWorkersRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_ListWorkersRequest(ctx, op, nil, req, nil)
}

func (s *RPCService) GetWorker(ctx context.Context, req *ateapipb.GetWorkerRequest) (*ateapipb.Worker, error) {
	if errs := validateGetWorkerRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	name := req.GetWorker().GetName()

	worker, err := s.impl.GetWorker(ctx, name)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "Worker %s not found", name)
	}
	if err != nil {
		return nil, fmt.Errorf("while getting worker: %w", err)
	}
	return worker, nil
}

func (s *ServiceImpl) GetWorker(ctx context.Context, name string) (*ateapipb.Worker, error) {
	return s.store.GetWorker(ctx, name)
}

func validateGetWorkerRequest(ctx context.Context, req *ateapipb.GetWorkerRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_GetWorkerRequest(ctx, op, nil, req, nil)
}

func (s *RPCService) CreateWorker(ctx context.Context, req *ateapipb.CreateWorkerRequest) (*ateapipb.Worker, error) {
	// First scrub any fields that callers are not allowed to set. status is
	// output-only, so whatever the request carried there is replaced rather
	// than rejected.
	inWorker := req.Worker
	if inWorker != nil { // otherwise validation will flag it
		scrubResourceMetadataForCreate(inWorker.Metadata)
		inWorker.Status = nil
	}

	// Validate the request, including the object within it.
	if errs := validateCreateWorkerRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	// Handle the creation, including validation of the final stored object.
	return s.impl.CreateWorker(ctx, inWorker)
}

func (s *ServiceImpl) CreateWorker(ctx context.Context, inWorker *ateapipb.Worker) (*ateapipb.Worker, error) {
	// A Worker is registered only once its pod is Ready and has an IP, which
	// makes ACTIVE the only state it can be born in.
	outWorker := proto.CloneOf(inWorker)
	outWorker.Status = &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE}

	// Verify that the result is properly valid before storing it.
	if errs := validateWorkerUpdate(ctx, field.NewPath("worker"), outWorker, inWorker, true); len(errs) > 0 {
		return nil, toGRPCInternalError(errs)
	}

	// Save the data in the storage layer.
	created, err := s.store.CreateWorker(ctx, outWorker)
	if err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "Worker %s already exists", inWorker.GetMetadata().GetName())
		}
		return nil, fmt.Errorf("while creating worker: %w", err)
	}
	return created, nil
}

func validateCreateWorkerRequest(ctx context.Context, req *ateapipb.CreateWorkerRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_CreateWorkerRequest(ctx, op, nil, req, nil)
}

// UpdateWorker replaces the stored Worker with the one the request carries.
// Only sandbox_class and labels are the caller's to change; a request that
// alters an immutable field — including by leaving it unset, which would clear
// it — is rejected. The service layer enforces that with declarative
// validation against the stored worker inside the update transaction.
func (s *RPCService) UpdateWorker(ctx context.Context, req *ateapipb.UpdateWorkerRequest) (*ateapipb.Worker, error) {
	// First scrub any fields that callers are not allowed to set.
	inWorker := req.Worker
	if inWorker != nil { // otherwise validation will flag it
		scrubResourceMetadataForUpdate(inWorker.Metadata)
		inWorker.Status = nil
	}

	// Validate the request.
	if errs := validateUpdateWorkerRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	return s.mutateWorker(ctx, inWorker.GetMetadata().GetName(), store.PreconditionFrom(inWorker), func(toUpdate *ateapipb.Worker) error {
		// Status and metadata are server-owned fields.
		status, metadata := toUpdate.GetStatus(), toUpdate.GetMetadata()
		// Reset + merge from the input worker.
		proto.Reset(toUpdate)
		proto.Merge(toUpdate, inWorker)
		// Restore status and metadata from the server.
		toUpdate.Status = status
		toUpdate.Metadata = metadata
		return nil
	})
}

func (s *ServiceImpl) UpdateWorker(ctx context.Context, name string, precondition store.Precondition, mutate func(toUpdate *ateapipb.Worker) error) (*ateapipb.Worker, error) {
	return s.store.UpdateWorker(ctx, name, precondition, func(toUpdate *ateapipb.Worker) error {
		// Apply the mutation function to the stored value.
		oldVal := proto.CloneOf(toUpdate)
		if err := mutate(toUpdate); err != nil {
			return err
		}
		newVal := toUpdate

		// Validate the mutated value before doing any further work. This is
		// what enforces the immutable fields, since only the stored worker
		// gives declarative validation an old value to compare against.
		if errs := validateWorkerUpdate(ctx, field.NewPath("worker"), newVal, oldVal, false); len(errs) > 0 {
			return toGRPCStatusError(errs)
		}

		// Do any further work on the resource.

		// Validate the final value before storing it.
		if errs := validateWorkerUpdate(ctx, field.NewPath("worker"), newVal, oldVal, true); len(errs) > 0 {
			return toGRPCInternalError(errs)
		}

		return nil
	})
}

func validateUpdateWorkerRequest(ctx context.Context, req *ateapipb.UpdateWorkerRequest) field.ErrorList {
	// Call the generated validation.
	// We model this as a create rather than an update because updates assume
	// the existence of a "current" value, which we do not have yet.  This is
	// validating the request itself. The result will be validated later, after
	// we have a current value to compare against.
	op := operation.Operation{Type: operation.Create}
	return Validate_UpdateWorkerRequest(ctx, op, nil, req, nil)
}

func (s *RPCService) DeleteWorker(ctx context.Context, req *ateapipb.DeleteWorkerRequest) (*ateapipb.Worker, error) {
	if errs := validateDeleteWorkerRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	// The delete releases the Actor bound to this Worker before removing the
	// record, so it is a workflow rather than a single store call.
	return s.workerWorkflow.DeleteWorker(ctx, req.GetWorker().GetName(), store.DeletePreconditions{
		UID:     req.GetOptions().GetUid(),
		Version: req.GetOptions().GetVersion(),
	})
}

func (s *ServiceImpl) DeleteWorker(ctx context.Context, name string, pre store.DeletePreconditions) (*ateapipb.Worker, error) {
	return s.store.DeleteWorker(ctx, name, pre)
}

func validateDeleteWorkerRequest(ctx context.Context, req *ateapipb.DeleteWorkerRequest) field.ErrorList {
	// Call the generated validation. The preconditions in options are each
	// optional: a zero value waives that guard, so only non-zero values are
	// checked for shape.
	op := operation.Operation{Type: operation.Create}
	return Validate_DeleteWorkerRequest(ctx, op, nil, req, nil)
}

func (s *RPCService) DrainWorker(ctx context.Context, req *ateapipb.DrainWorkerRequest) (*ateapipb.Worker, error) {
	if errs := validateDrainWorkerRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	name := req.GetWorker().GetName()

	// A DrainWorkerRequest names a worker and carries no guards, so the ones
	// the store requires come from a read here rather than from the client. A
	// write that lands in between is reported as a conflict for the caller to
	// retry, the same as any other guarded update.
	observed, err := s.impl.GetWorker(ctx, name)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "Worker %s not found", name)
	}
	if err != nil {
		return nil, fmt.Errorf("while getting worker to drain: %w", err)
	}

	return s.mutateWorker(ctx, name, store.PreconditionFrom(observed), func(toUpdate *ateapipb.Worker) error {
		if toUpdate.GetStatus().GetState() == ateapipb.WorkerState_WORKER_STATE_DRAINING {
			// already draining, do nothing
			return &workerUnchanged{worker: proto.Clone(toUpdate).(*ateapipb.Worker)}
		}
		toUpdate.Status.State = ateapipb.WorkerState_WORKER_STATE_DRAINING
		// status.assignment is deliberately left alone: a draining Worker keeps
		// hosting the Actor bound to it until something releases it. Draining
		// only stops the scheduler routing new Actors here.
		return nil
	})
}

func validateDrainWorkerRequest(ctx context.Context, req *ateapipb.DrainWorkerRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_DrainWorkerRequest(ctx, op, nil, req, nil)
}

// mutateWorker runs mutate against the named Worker and translates what comes
// back into the RPC's result. A mutation that found nothing to do reports the
// Worker it saw; anything else is a store error.
func (s *RPCService) mutateWorker(ctx context.Context, name string, precondition store.Precondition, mutate func(toUpdate *ateapipb.Worker) error) (*ateapipb.Worker, error) {
	worker, err := s.impl.UpdateWorker(ctx, name, precondition, mutate)
	if err == nil {
		return worker, nil
	}

	var unchanged *workerUnchanged
	if errors.As(err, &unchanged) {
		return unchanged.worker, nil
	}
	switch {
	case errors.Is(err, store.ErrNotFound):
		return nil, status.Errorf(codes.NotFound, "Worker %s not found", name)
	case errors.Is(err, store.ErrUIDConflict):
		return nil, status.Errorf(codes.Aborted, "Worker %s is not the one the request describes", name)
	case errors.Is(err, store.ErrVersionConflict):
		return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
	case errors.Is(err, store.ErrPreconditionRequired):
		return nil, status.Errorf(codes.InvalidArgument, "while updating worker %s: %v", name, err)
	}
	return nil, fmt.Errorf("while updating worker: %w", err)
}

// workerUnchanged ends an UpdateWorker mutation that found its work already
// done. The store hands a mutation's error straight back and leaves the Worker —
// and its version — untouched, which is what lets DrainWorker be idempotent: a
// call with nothing left to do costs no version bump. worker is a copy, because
// the store is free to reuse or discard the message once mutate returns.
type workerUnchanged struct {
	worker *ateapipb.Worker
}

func (u *workerUnchanged) Error() string { return "worker is already in the requested state" }

// validateWorkerUpdate validates a Worker against the previous stored value.
// It is what enforces the immutable fields, which need an old value to compare
// against.
func validateWorkerUpdate(ctx context.Context, fldPath *field.Path, newVal, oldVal *ateapipb.Worker, requireStatus bool) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Update}
	errs := Validate_Worker(ctx, op, fldPath, newVal, oldVal)
	if requireStatus {
		// Status is optional in the schema, but is actually required to be set
		// by the server.  If it was specified, it was already validated above,
		// but if it was not specified we need to flag that as an error.
		errs = append(errs, validate.RequiredPointer(ctx, op, fldPath.Child("status"), newVal.GetStatus(), nil)...)
	}
	return errs
}

func (s *ServiceImpl) WatchWorkers(ctx context.Context) (*store.WorkerWatch, error) {
	return s.store.WatchWorkers(ctx)
}

// This is needed because DV doesn't have a standard format for IP addresses yet.
func ValidateCustom_Worker_Ip(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return validation.IsValidIP(fldPath, *value)
}

// This exists only because nested subfield tags are not supported yet.
func ValidateCustom_UpdateWorkerRequest_Worker(ctx context.Context, op operation.Operation, fldPath *field.Path, worker, _ *ateapipb.Worker) field.ErrorList {
	if worker == nil || worker.Metadata == nil {
		return nil // handled by DV
	}

	// Updates are validated in 2 steps: first the update request and then the
	// resource itself. DV for the request doesn't descend into the resource
	// metadata.  Once DV supports nested subfield tags, this can be changed to
	// something like:
	//   +k8s:subfield(metadata)=+k8s:subfield(atespace)=+k8s:forbidden
	// Workers are global-scoped, so metadata.atespace must be empty.
	errs := Validate_ResourceMetadata(ctx, op, fldPath.Child("metadata"), worker.Metadata, nil)
	errs = append(errs, validate.ForbiddenValue(ctx, op, fldPath.Child("metadata", "atespace"), &worker.Metadata.Atespace, nil)...)
	return errs
}
