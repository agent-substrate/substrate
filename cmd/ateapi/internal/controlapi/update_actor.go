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
)

func (s *Service) UpdateActor(ctx context.Context, req *ateapipb.UpdateActorRequest) (*ateapipb.UpdateActorResponse, error) {
	if err := validateUpdateActorRequest(req); err != nil {
		return nil, err
	}

	actor, err := s.persistence.GetActor(ctx, req.GetActorId())
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Actor %s not found", req.GetActorId())
		}
		return nil, fmt.Errorf("while getting actor: %w", err)
	}
	actor.WorkerSelector = req.GetWorkerSelector()

	if err := s.persistence.UpdateActor(ctx, actor, actor.GetVersion()); err != nil {
		if errors.Is(err, store.ErrPersistenceRetry) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while updating actor: %w", err)
	}

	return &ateapipb.UpdateActorResponse{Actor: actor}, nil
}

func validateUpdateActorRequest(req *ateapipb.UpdateActorRequest) error {
	if req.GetActorId() == "" {
		return status.Error(codes.InvalidArgument, "actor_id is required")
	}
	if err := validateSelector(req.GetWorkerSelector()); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return nil
}
