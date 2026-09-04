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

// Package workerservice serves the RPCs a Worker uses to tell the control
// plane about itself.
package workerservice

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/ateletauth"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Server implements ateapipb.WorkerServiceServer.
type Server struct {
	ateapipb.UnimplementedWorkerServiceServer

	// store is where a Worker's reported capacity is recorded, and the
	// authoritative state the report is authorized against.
	store store.Interface
}

var _ ateapipb.WorkerServiceServer = (*Server)(nil)

func New(store store.Interface) *Server {
	return &Server{store: store}
}

// SetWorkerCapacity records a Worker's reported capacity. As with MintCert,
// the caller must be an atelet running on the Worker's node.
func (s *Server) SetWorkerCapacity(ctx context.Context, req *ateapipb.SetWorkerCapacityRequest) (*ateapipb.SetWorkerCapacityResponse, error) {
	caller, err := ateletauth.Authenticate(ctx)
	if err != nil {
		return nil, err
	}
	// Workers are global-scoped, so the reference carries no atespace.
	if errs := resources.ValidateGlobalObjectRef(req.GetWorker(), field.NewPath("worker")); len(errs) > 0 {
		return nil, status.Errorf(codes.InvalidArgument, "invalid worker: %v", errs.ToAggregate())
	}
	reported := req.GetCapacity()
	if reported == nil {
		return nil, status.Error(codes.InvalidArgument, "capacity is required")
	}
	if err := validateReportedCapacity(reported); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid capacity: %v", err)
	}
	name := req.GetWorker().GetName()

	// Use authoritative state to authorize the write.
	worker, err := s.store.GetWorker(ctx, name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Worker %s not found", name)
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
		return nil, status.Errorf(codes.NotFound, "Worker %s not found", name)
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
		return nil, status.Errorf(codes.NotFound, "Worker %s not found", name)
	case errors.Is(err, store.ErrUIDConflict), errors.Is(err, store.ErrVersionConflict):
		return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
	default:
		return nil, fmt.Errorf("while recording capacity for worker %s: %w", name, err)
	}
	slog.InfoContext(ctx, "Worker reported its capacity",
		slog.String("worker", name),
		slog.String("was", worker.GetStatus().GetCapacity().String()),
		slog.String("now", updated.GetStatus().GetCapacity().String()))
	return &ateapipb.SetWorkerCapacityResponse{Worker: updated}, nil
}

// validateReportedCapacity rejects a report that cannot mean anything. A report
// is written straight to the store, so it does not pass the declarative
// validation an UpdateWorker request would, and an unchecked ceiling persists.
// A negative one is the costly case: placement asks whether allocated is below
// capacity, which is false for every Actor, so the Worker silently never takes
// another one.
func validateReportedCapacity(reported *ateapipb.WorkerResources) error {
	if reported.GetActors() < 0 {
		return fmt.Errorf("actors is %d, must not be negative", reported.GetActors())
	}
	quantities, err := resources.ParseQuantities(reported.GetResources())
	if err != nil {
		return err
	}
	for _, name := range slices.Sorted(maps.Keys(quantities)) {
		if quantity := quantities[name]; quantity.Sign() < 0 {
			return fmt.Errorf("%s is %s, must not be negative", name, quantity.String())
		}
	}
	return nil
}
