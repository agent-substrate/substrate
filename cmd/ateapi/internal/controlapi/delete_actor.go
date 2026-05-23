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
)

func (s *Service) DeleteActor(ctx context.Context, req *ateapipb.DeleteActorRequest) (*ateapipb.DeleteActorResponse, error) {
	if err := validateDeleteActorRequest(req); err != nil {
		return nil, err
	}

	if err := s.persistence.DeleteActor(ctx, req.GetActorId()); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Actor %s not found", req.GetActorId())
		}
		if errors.Is(err, store.ErrFailedPrecondition) {
			actor, getErr := s.persistence.GetActor(ctx, req.GetActorId())
			if getErr == nil {
				return nil, status.Errorf(codes.FailedPrecondition, "Actor %s is not suspended (status: %v)", req.GetActorId(), actor.GetStatus())
			}
			return nil, status.Errorf(codes.FailedPrecondition, "Actor %s is not suspended", req.GetActorId())
		}
		if errors.Is(err, store.ErrPersistenceRetry) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while deleting actor from DB: %w", err)
	}

	return &ateapipb.DeleteActorResponse{}, nil
}

func validateDeleteActorRequest(req *ateapipb.DeleteActorRequest) error {
	if req.GetActorId() == "" {
		return status.Error(codes.InvalidArgument, "actor_id is required")
	}
	if err := resources.ValidateActorID(req.GetActorId()); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return nil
}
