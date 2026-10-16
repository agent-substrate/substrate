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

package workerservice

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/apivalidation"
	"go.ate.dev/substrate/cmd/ateapi/internal/ateletauth"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
)

// AddWorkerAllocated grows a Worker's allocated resources by resource_delta.
// Under overcommit the worker calls it when an Actor's usage grows. As with
// RegisterWorker, the caller must be an atelet running on the Worker's node.
func (s *Server) AddWorkerAllocated(ctx context.Context, req *ateapi.AddWorkerAllocatedRequest) (*ateapi.AddWorkerAllocatedResponse, error) {
	caller, err := ateletauth.Authenticate(ctx, s.ateletSPIFFEID)
	if err != nil {
		return nil, err
	}
	if errs := apivalidation.ValidateAddWorkerAllocatedRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	if err := s.adjustWorkerAllocated(ctx, caller, req.GetWorker().GetName(), req.GetResourceDelta(), "allocation increase", s.store.AddWorkerAllocated); err != nil {
		return nil, err
	}
	return &ateapi.AddWorkerAllocatedResponse{}, nil
}

// SubtractWorkerAllocated shrinks a Worker's allocated resources by
// resource_delta, clamping each dimension at zero. Under overcommit the worker
// calls it to hand back what an Actor no longer uses.
func (s *Server) SubtractWorkerAllocated(ctx context.Context, req *ateapi.SubtractWorkerAllocatedRequest) (*ateapi.SubtractWorkerAllocatedResponse, error) {
	caller, err := ateletauth.Authenticate(ctx, s.ateletSPIFFEID)
	if err != nil {
		return nil, err
	}
	if errs := apivalidation.ValidateSubtractWorkerAllocatedRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	if err := s.adjustWorkerAllocated(ctx, caller, req.GetWorker().GetName(), req.GetResourceDelta(), "allocation decrease", s.store.SubtractWorkerAllocated); err != nil {
		return nil, err
	}
	return &ateapi.SubtractWorkerAllocatedResponse{}, nil
}

// adjustWorkerAllocated authorizes an allocation change on the Worker named
// name, then applies it with adjust.
func (s *Server) adjustWorkerAllocated(ctx context.Context, caller *ateletauth.Caller, name string, delta *ateapi.Resources, what string,
	adjust func(context.Context, string, *ateapi.Resources) (*ateapi.Worker, error)) error {
	// TODO(identity): This check should be handled by OpenFGA.
	if _, err := s.workerOnCallerNode(ctx, name, caller); err != nil {
		return err
	}

	updated, err := adjust(ctx, name, delta)
	switch {
	case err == nil:
	case errors.Is(err, store.ErrNotFound):
		return apierror.NotFound("Worker %s not found", name)
	default:
		return fmt.Errorf("while applying %s to worker %s: %w", what, name, err)
	}
	slog.DebugContext(ctx, "Worker allocation adjusted",
		slog.String("worker", name),
		slog.String("change", what),
		slog.String("delta", delta.String()),
		slog.String("now", updated.GetStatus().GetAllocated().String()))
	return nil
}
