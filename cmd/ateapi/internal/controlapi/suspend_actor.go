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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) SuspendActor(ctx context.Context, req *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error) {
	if err := validateSuspendActorRequest(req); err != nil {
		return nil, err
	}

	actor, err := s.actorWorkflow.SuspendActor(ctx, req.GetActorId())
	if err != nil {
		if errors.Is(err, store.ErrPersistenceRetry) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Actor %s not found", req.GetActorId())
		}
		return nil, err
	}

	return &ateapipb.SuspendActorResponse{Actor: actor}, nil
}

func validateSuspendActorRequest(req *ateapipb.SuspendActorRequest) error {
	if req.GetActorId() == "" {
		return status.Error(codes.InvalidArgument, "id is required")
	}
	return nil
}
