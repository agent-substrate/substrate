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

func (s *Service) GetActor(ctx context.Context, req *ateapipb.GetActorRequest) (*ateapipb.GetActorResponse, error) {
	if err := validateGetActorRequest(req); err != nil {
		return nil, err
	}
	actor, err := s.persistence.GetActor(ctx, req.GetActorId())
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "Actor %s not found", req.GetActorId())
	} else if err != nil {
		return nil, fmt.Errorf("while getting actor from DB: %w", err)
	}
	return &ateapipb.GetActorResponse{
		Actor: actor,
	}, nil
}

func validateGetActorRequest(req *ateapipb.GetActorRequest) error {
	if req.GetActorId() == "" {
		return status.Error(codes.InvalidArgument, "id is required")
	}
	return nil
}
