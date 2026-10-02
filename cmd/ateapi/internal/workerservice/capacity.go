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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/apivalidation"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/ateletauth"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
)

// SetWorkerCapacity records a Worker's reported capacity. As with MintCert,
// the caller must be an atelet running on the Worker's node.
func (s *Server) SetWorkerCapacity(ctx context.Context, req *ateapipb.SetWorkerCapacityRequest) (*ateapipb.SetWorkerCapacityResponse, error) {
	// TODO(identity): This check should be handled by OpenFGA.
	caller, err := ateletauth.Authenticate(ctx, s.ateletSPIFFEID)
	if err != nil {
		return nil, err
	}
	if errs := apivalidation.ValidateSetWorkerCapacityRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	reported := req.GetCapacity()
	name := req.GetWorker().GetName()

	// Use authoritative state to authorize the write.
	worker, err := s.store.GetWorker(ctx, name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Worker %s not found", name)
		}
		return nil, fmt.Errorf("while fetching worker %s: %w", name, err)
	}
	if worker.GetNodeName() != caller.NodeName {
		// Do not disclose Workers on other nodes.
		slog.WarnContext(ctx, "Refusing a capacity report for a worker on another node",
			slog.String("worker", name),
			slog.String("worker_node", worker.GetNodeName()),
			slog.String("caller_node", caller.NodeName),
			slog.String("caller_pod", caller.PodName))
		return nil, apierror.NotFound("Worker %s not found", name)
	}

	if proto.Equal(worker.GetStatus().GetCapacity(), reported) {
		return &ateapipb.SetWorkerCapacityResponse{Worker: worker}, nil
	}

	updated, err := s.store.UpdateWorker(ctx, name, store.PreconditionFrom(worker), func(toUpdate *ateapipb.Worker) error {
		// Replaces rather than merges: a Worker reports everything it has, so a
		// dimension this report leaves out is one it no longer supplies.
		toUpdate.Status.Capacity = reported
		return nil
	})
	switch {
	case err == nil:
	case errors.Is(err, store.ErrNotFound):
		return nil, apierror.NotFound("Worker %s not found", name)
	case errors.Is(err, store.ErrUIDConflict), errors.Is(err, store.ErrVersionConflict):
		return nil, apierror.Aborted("concurrent update conflict, please retry")
	default:
		return nil, fmt.Errorf("while recording capacity for worker %s: %w", name, err)
	}
	slog.InfoContext(ctx, "Worker reported its capacity",
		slog.String("worker", name),
		slog.String("was", worker.GetStatus().GetCapacity().String()),
		slog.String("now", updated.GetStatus().GetCapacity().String()))
	return &ateapipb.SetWorkerCapacityResponse{Worker: updated}, nil
}
