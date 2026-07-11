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
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *Service) DeleteAtespace(ctx context.Context, req *ateapipb.DeleteAtespaceRequest) (*ateapipb.Atespace, error) {
	if err := validateDeleteAtespaceRequest(req); err != nil {
		return nil, err
	}

	name := req.GetAtespace().GetName()
	deleted, err := s.persistence.DeleteAtespace(ctx, name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Atespace %s not found", name)
		}
		if errors.Is(err, store.ErrFailedPrecondition) {
			return nil, status.Errorf(codes.FailedPrecondition, "Atespace %s is not empty", name)
		}
		return nil, fmt.Errorf("while deleting atespace from DB: %w", err)
	}

	return deleted, nil
}

func validateDeleteAtespaceRequest(req *ateapipb.DeleteAtespaceRequest) error {
	var fldPath *field.Path
	var errs field.ErrorList

	if val, fldPath := req.Atespace, fldPath.Child("atespace"); val == nil {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, resources.ValidateGlobalObjectRef(val, fldPath)...)
	}

	if len(errs) > 0 {
		return status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
	}
	return nil
}
