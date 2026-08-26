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
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/validate/content"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *RPCService) ListWorkers(ctx context.Context, req *ateapipb.ListWorkersRequest) (*ateapipb.ListWorkersResponse, error) {
	if errs := validateListWorkersRequest(req); len(errs) > 0 {
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
	// TODO: implement this
	return s.store.ListWorkers(ctx, opts)
}

func validateListWorkersRequest(req *ateapipb.ListWorkersRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	if val, fldPath := req.PageSize, fldPath.Child("page_size"); val < 0 {
		errs = append(errs, field.Invalid(fldPath, val, "must be greater than or equal to 0"))
	}

	return errs
}

func (s *RPCService) GetWorker(ctx context.Context, req *ateapipb.GetWorkerRequest) (*ateapipb.Worker, error) {
	if errs := validateGetWorkerRequest(req); len(errs) > 0 {
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
	// TODO: implement this
	return s.store.GetWorker(ctx, name)
}

func validateGetWorkerRequest(req *ateapipb.GetWorkerRequest) field.ErrorList {
	var fldPath *field.Path
	return resources.ValidateGlobalObjectRef(req.GetWorker(), fldPath.Child("worker"))
}

func (s *RPCService) CreateWorker(ctx context.Context, req *ateapipb.CreateWorkerRequest) (*ateapipb.Worker, error) {
	if errs := validateCreateWorkerRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	name := req.GetWorker().GetMetadata().GetName()

	// status is output-only, so whatever the request carried there is replaced
	// rather than rejected.
	worker := proto.Clone(req.GetWorker()).(*ateapipb.Worker)
	worker.Status = &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE}

	created, err := s.impl.CreateWorker(ctx, worker)
	if errors.Is(err, store.ErrAlreadyExists) {
		return nil, status.Errorf(codes.AlreadyExists, "Worker %s already exists", name)
	}
	if err != nil {
		return nil, fmt.Errorf("while creating worker: %w", err)
	}
	return created, nil
}

func (s *ServiceImpl) CreateWorker(ctx context.Context, worker *ateapipb.Worker) (*ateapipb.Worker, error) {
	// TODO: implement this
	return s.store.CreateWorker(ctx, worker)
}

func validateCreateWorkerRequest(req *ateapipb.CreateWorkerRequest) field.ErrorList {
	var fldPath *field.Path

	worker, workerPath := req.GetWorker(), fldPath.Child("worker")
	if worker == nil {
		return field.ErrorList{field.Required(workerPath, "")}
	}
	return validateWorker(worker, workerPath)
}

// UpdateWorker replaces the stored Worker with the one the request carries.
// Only sandbox_class and labels are the caller's to change; a request that
// alters an immutable field — including by leaving it unset, which would clear
// it — is rejected. The store enforces that, since only it holds the stored
// worker to compare against.
func (s *RPCService) UpdateWorker(ctx context.Context, req *ateapipb.UpdateWorkerRequest) (*ateapipb.Worker, error) {
	if errs := validateUpdateWorkerRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	in := req.GetWorker()

	return s.mutateWorker(ctx, in.GetMetadata().GetName(), store.PreconditionFrom(in), func(toUpdate *ateapipb.Worker) error {
		// Status and metadata are server-owned fields.
		status, metadata := toUpdate.GetStatus(), toUpdate.GetMetadata()
		// Reset + merge from the input worker.
		proto.Reset(toUpdate)
		proto.Merge(toUpdate, in)
		// Restore status and metadata from the server.
		toUpdate.Status = status
		toUpdate.Metadata = metadata
		return nil
	})
}

func (s *ServiceImpl) UpdateWorker(ctx context.Context, name string, precondition store.Precondition, mutate func(toUpdate *ateapipb.Worker) error) (*ateapipb.Worker, error) {
	// TODO: implement this
	return s.store.UpdateWorker(ctx, name, precondition, mutate)
}

func validateUpdateWorkerRequest(req *ateapipb.UpdateWorkerRequest) field.ErrorList {
	var fldPath *field.Path

	worker, workerPath := req.GetWorker(), fldPath.Child("worker")
	if worker == nil {
		return field.ErrorList{field.Required(workerPath, "")}
	}

	// Only the metadata guards are checked here. The rest of the worker is
	// pinned to what create stored — validateWorker already passed on it, and
	// an update that changed any of it does not get written.
	return resources.ValidateGlobalUpdateMetadataRef(worker.GetMetadata(), workerPath.Child("metadata"))
}

func (s *RPCService) DeleteWorker(ctx context.Context, req *ateapipb.DeleteWorkerRequest) (*ateapipb.Worker, error) {
	if errs := validateDeleteWorkerRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	name := req.GetWorker().GetName()

	worker, err := s.impl.DeleteWorker(ctx, name, store.DeletePreconditions{
		UID:     req.GetOptions().GetUid(),
		Version: req.GetOptions().GetVersion(),
	})
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			return nil, status.Errorf(codes.NotFound, "Worker %s not found", name)
		case errors.Is(err, store.ErrUIDConflict):
			return nil, status.Errorf(codes.Aborted, "Worker %s does not have uid %s", name, req.GetOptions().GetUid())
		case errors.Is(err, store.ErrVersionConflict):
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while deleting worker: %w", err)
	}
	return worker, nil
}

func (s *ServiceImpl) DeleteWorker(ctx context.Context, name string, pre store.DeletePreconditions) (*ateapipb.Worker, error) {
	// TODO: implement this
	return s.store.DeleteWorker(ctx, name, pre)
}

func validateDeleteWorkerRequest(req *ateapipb.DeleteWorkerRequest) field.ErrorList {
	var fldPath *field.Path

	errs := resources.ValidateGlobalObjectRef(req.GetWorker(), fldPath.Child("worker"))

	// Delete carries its preconditions in options, and each is optional: a zero
	// value waives that guard. Absent options waive both, so nil needs no
	// special case.
	opts, optsPath := req.GetOptions(), fldPath.Child("options")
	if val, p := opts.GetUid(), optsPath.Child("uid"); val != "" {
		errs = append(errs, resources.ValidateUUID(val, p)...)
	}
	if val, p := opts.GetVersion(), optsPath.Child("version"); val < 0 {
		errs = append(errs, field.Invalid(p, val, "must not be negative"))
	}

	return errs
}

func (s *RPCService) DrainWorker(ctx context.Context, req *ateapipb.DrainWorkerRequest) (*ateapipb.Worker, error) {
	if errs := validateDrainWorkerRequest(req); len(errs) > 0 {
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

func validateDrainWorkerRequest(req *ateapipb.DrainWorkerRequest) field.ErrorList {
	var fldPath *field.Path
	return resources.ValidateGlobalObjectRef(req.GetWorker(), fldPath.Child("worker"))
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
	case errors.Is(err, store.ErrImmutableField):
		return nil, status.Errorf(codes.InvalidArgument, "while updating worker %s: %v", name, err)
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

// validateWorker checks that the caller-controlled fields of a Worker are
// well-formed. It is the create-time check: every field it covers is immutable
// afterwards, so no update path re-runs it.
func validateWorker(worker *ateapipb.Worker, fldPath *field.Path) field.ErrorList {
	var errs field.ErrorList

	// Worker is global-scoped: metadata.atespace must be empty, name required +
	// valid. uid and version are server-assigned, so a create ignores whatever
	// the request carried in them.
	metaPath := fldPath.Child("metadata")
	if val, p := worker.GetMetadata().GetAtespace(), metaPath.Child("atespace"); val != "" {
		errs = append(errs, field.Invalid(p, val, "must be empty for a global-scoped resource"))
	}
	if val, p := worker.GetMetadata().GetName(), metaPath.Child("name"); val == "" {
		errs = append(errs, field.Required(p, ""))
	} else {
		errs = append(errs, resources.ValidateResourceName(val, p)...)
	}

	if val, fldPath := worker.GetWorkerNamespace(), fldPath.Child("worker_namespace"); val == "" {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		for _, msg := range content.IsDNS1123Label(val) {
			errs = append(errs, field.Invalid(fldPath, val, msg))
		}
	}

	if val, fldPath := worker.GetWorkerPool(), fldPath.Child("worker_pool"); val == "" {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		for _, msg := range content.IsDNS1123Subdomain(val) {
			errs = append(errs, field.Invalid(fldPath, val, msg))
		}
	}

	if val, fldPath := worker.GetWorkerPod(), fldPath.Child("worker_pod"); val == "" {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		for _, msg := range content.IsDNS1123Subdomain(val) {
			errs = append(errs, field.Invalid(fldPath, val, msg))
		}
	}

	if val, fldPath := worker.GetIp(), fldPath.Child("ip"); val == "" {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, resources.ValidateIP(val, fldPath)...)
	}

	if val, fldPath := worker.GetWorkerPodUid(), fldPath.Child("worker_pod_uid"); val == "" {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, resources.ValidateUUID(val, fldPath)...)
	}

	if val, fldPath := worker.GetNodeName(), fldPath.Child("node_name"); val == "" {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		for _, msg := range content.IsDNS1123Subdomain(val) {
			errs = append(errs, field.Invalid(fldPath, val, msg))
		}
	}

	return errs
}

func (s *ServiceImpl) WatchWorkers(ctx context.Context) (*store.WorkerWatch, error) {
	// TODO: implement this
	return s.store.WatchWorkers(ctx)
}
