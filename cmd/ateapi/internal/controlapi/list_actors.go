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
	"fmt"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxPageSize = 1000

func (s *Service) ListActors(ctx context.Context, req *ateapipb.ListActorsRequest) (*ateapipb.ListActorsResponse, error) {
	if err := validateListActorsRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pageSize := req.GetPageSize()
	if pageSize == 0 {
		pageSize = maxPageSize
	}

	actors, nextToken, err := s.persistence.ListActors(ctx, req.GetAtespace(), pageSize, req.GetPageToken())
	if err != nil {
		return nil, fmt.Errorf("while listing actors in db: %w", err)
	}
	return &ateapipb.ListActorsResponse{
		Actors:        actors,
		NextPageToken: nextToken,
	}, nil
}

func validateListActorsRequest(req *ateapipb.ListActorsRequest) error {
	// An empty atespace is allowed here and means "all atespaces"(used by `kubectl ate get actors -A`).
	// A non-empty atespace is validated and scopes the listing to that atespace.
	if req.GetAtespace() != "" {
		if err := resources.ValidateAtespace(req.GetAtespace()); err != nil {
			return err
		}
	}
	pageSize := req.GetPageSize()
	if pageSize < 0 {
		return fmt.Errorf("page_size cannot be negative")
	}
	if pageSize > maxPageSize {
		return fmt.Errorf("page_size %d exceeds maximum page size %d", pageSize, maxPageSize)
	}
	return nil
}
