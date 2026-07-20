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
	"k8s.io/apimachinery/pkg/util/validation/field"
)

const maxPageSize = 1000

// effectivePageSize applies the server-chosen default for an unset page_size
// and silently coerces oversized values.
func effectivePageSize(requested int32) int32 {
	if requested == 0 || requested > maxPageSize {
		return maxPageSize
	}
	return requested
}

func (s *Service) ListActors(ctx context.Context, req *ateapipb.ListActorsRequest) (*ateapipb.ListActorsResponse, error) {
	if err := validateListActorsRequest(req); err != nil {
		return nil, err
	}

	actors, nextToken, err := s.persistence.ListActors(ctx, req.GetAtespace(), effectivePageSize(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, fmt.Errorf("while listing actors in db: %w", err)
	}
	return &ateapipb.ListActorsResponse{
		Actors:        actors,
		NextPageToken: nextToken,
	}, nil
}

func validateListActorsRequest(req *ateapipb.ListActorsRequest) error {
	var fldPath *field.Path
	var errs field.ErrorList

	// An empty atespace is allowed here and means "all atespaces" (used by
	// `kubectl ate get actors -A`). A non-empty atespace is validated and
	// scopes the listing to that atespace.
	if val, fldPath := req.Atespace, fldPath.Child("atespace"); val != "" {
		errs = append(errs, resources.ValidateResourceName(val, fldPath)...)
	}

	if val, fldPath := req.PageSize, fldPath.Child("page_size"); val < 0 {
		errs = append(errs, field.Invalid(fldPath, val, "must be greater than or equal to 0"))
	}

	if len(errs) > 0 {
		return status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
	}
	return nil
}
