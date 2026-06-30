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

func (s *Service) DeleteAtespace(ctx context.Context, req *ateapipb.DeleteAtespaceRequest) (*ateapipb.DeleteAtespaceResponse, error) {
	if err := validateDeleteAtespaceRequest(req); err != nil {
		return nil, err
	}

	if err := s.persistence.DeleteAtespace(ctx, req.GetName()); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Atespace %s not found", req.GetName())
		}
		if errors.Is(err, store.ErrFailedPrecondition) {
			return nil, status.Errorf(codes.FailedPrecondition, "Atespace %s is not empty", req.GetName())
		}
		return nil, fmt.Errorf("while deleting atespace from DB: %w", err)
	}

	return &ateapipb.DeleteAtespaceResponse{}, nil
}

func validateDeleteAtespaceRequest(req *ateapipb.DeleteAtespaceRequest) error {
	if req.GetName() == "" {
		return status.Error(codes.InvalidArgument, "name is required")
	}
	if err := resources.ValidateAtespace(req.GetName()); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return nil
}
