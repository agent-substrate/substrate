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

func (s *Service) CreateAtespace(ctx context.Context, req *ateapipb.CreateAtespaceRequest) (*ateapipb.Atespace, error) {
	if err := validateCreateAtespaceRequest(req); err != nil {
		return nil, err
	}

	name := req.GetAtespace().GetMetadata().GetName()
	atespace := &ateapipb.Atespace{
		Metadata: &ateapipb.ResourceMetadata{
			Name: name,
		},
	}
	stored, err := s.persistence.CreateAtespace(ctx, atespace)
	if err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "Atespace %s already exists", name)
		}
		return nil, fmt.Errorf("while recording atespace: %w", err)
	}

	return stored, nil
}

func validateCreateAtespaceRequest(req *ateapipb.CreateAtespaceRequest) error {
	var fldPath *field.Path
	var errs field.ErrorList

	atespace := req.GetAtespace()
	atespacePath := fldPath.Child("atespace")
	if atespace == nil {
		errs = append(errs, field.Required(atespacePath, ""))
		return status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
	}

	// Atespace is global-scoped: metadata.atespace must be empty, name required + valid.
	metaPath := atespacePath.Child("metadata")
	if val, p := atespace.GetMetadata().GetAtespace(), metaPath.Child("atespace"); val != "" {
		errs = append(errs, field.Invalid(p, val, "must be empty for a global-scoped resource"))
	}
	if val, p := atespace.GetMetadata().GetName(), metaPath.Child("name"); val == "" {
		errs = append(errs, field.Required(p, ""))
	} else {
		errs = append(errs, resources.ValidateResourceName(val, p)...)
	}

	if len(errs) > 0 {
		return status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
	}
	return nil
}
