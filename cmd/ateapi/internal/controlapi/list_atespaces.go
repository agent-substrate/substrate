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

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *Service) ListAtespaces(ctx context.Context, req *ateapipb.ListAtespacesRequest) (*ateapipb.ListAtespacesResponse, error) {
	if err := validateListAtespacesRequest(req); err != nil {
		return nil, err
	}

	atespaces, nextToken, err := s.persistence.ListAtespaces(ctx, effectivePageSize(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, fmt.Errorf("while listing atespaces in db: %w", err)
	}
	return &ateapipb.ListAtespacesResponse{
		Atespaces:     atespaces,
		NextPageToken: nextToken,
	}, nil
}

func validateListAtespacesRequest(req *ateapipb.ListAtespacesRequest) error {
	var fldPath *field.Path
	var errs field.ErrorList

	if val, fldPath := req.PageSize, fldPath.Child("page_size"); val < 0 {
		errs = append(errs, field.Invalid(fldPath, val, "must be greater than or equal to 0"))
	}

	if len(errs) > 0 {
		return status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
	}
	return nil
}
