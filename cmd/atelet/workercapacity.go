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

package main

import (
	"context"
	"log/slog"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// workerCapacityService forwards a worker's own account of what it can supply
// to the control plane. The worker is the only thing that knows: the control
// plane sees a Pod, not what the runtime will actually give an actor.
type workerCapacityService struct {
	ateletpb.UnimplementedWorkerCapacityServer

	workers ateapipb.WorkerServiceClient
}

// SetWorkerCapacity records what the calling worker says it has.
//
// It returns the control plane's error unwrapped so the caller retries: a
// worker reports once, so an accepted call is the only thing that puts
// capacity on the Worker, and a Worker record the syncer has not created yet
// is the ordinary reason for a first attempt to fail.
func (s *workerCapacityService) SetWorkerCapacity(ctx context.Context, req *ateletpb.SetWorkerCapacityRequest) (*ateletpb.SetWorkerCapacityResponse, error) {
	// Identity comes only from the mTLS certificate, never from the request:
	// a worker can report its own capacity and no one else's.
	workerIdentity, err := authenticatedWorkerIdentity(ctx)
	if err != nil {
		return nil, err
	}
	// Forwarded as reported: the worker speaks the vocabulary the control plane
	// records, so there is nothing to translate.
	if _, err := s.workers.SetWorkerCapacity(ctx, &ateapipb.SetWorkerCapacityRequest{
		// Workers are global-scoped and named by their pod UID.
		Worker:   &ateapipb.ObjectRef{Name: workerIdentity.PodUID},
		Capacity: req.GetCapacity(),
	}); err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "Recorded worker capacity",
		slog.String("pod_uid", workerIdentity.PodUID), slog.Any("capacity", req.GetCapacity()))
	return &ateletpb.SetWorkerCapacityResponse{}, nil
}
