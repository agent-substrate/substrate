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

func (s *RPCService) CreateAtespace(ctx context.Context, req *ateapipb.CreateAtespaceRequest) (*ateapipb.Atespace, error) {
	if errs := validateCreateAtespaceRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
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

func validateCreateAtespaceRequest(req *ateapipb.CreateAtespaceRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	atespace := req.GetAtespace()
	atespacePath := fldPath.Child("atespace")
	if atespace == nil {
		errs = append(errs, field.Required(atespacePath, ""))
		return errs
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

	return errs
}

func (s *RPCService) GetAtespace(ctx context.Context, req *ateapipb.GetAtespaceRequest) (*ateapipb.Atespace, error) {
	if errs := validateGetAtespaceRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	name := req.GetAtespace().GetName()
	atespace, err := s.persistence.GetAtespace(ctx, name)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "Atespace %s not found", name)
	} else if err != nil {
		return nil, fmt.Errorf("while getting atespace from DB: %w", err)
	}

	return atespace, nil
}

func validateGetAtespaceRequest(req *ateapipb.GetAtespaceRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	if val, fldPath := req.Atespace, fldPath.Child("atespace"); val == nil {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, resources.ValidateGlobalObjectRef(val, fldPath)...)
	}

	return errs
}

func (s *RPCService) ListAtespaces(ctx context.Context, req *ateapipb.ListAtespacesRequest) (*ateapipb.ListAtespacesResponse, error) {
	if errs := validateListAtespacesRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	page, err := s.persistence.ListAtespaces(ctx, store.ListOptions{PageSize: effectivePageSize(req.GetPageSize()), PageToken: req.GetPageToken()})
	if err != nil {
		return nil, mapListError(fmt.Errorf("while listing atespaces in db: %w", err))
	}
	return &ateapipb.ListAtespacesResponse{
		Atespaces:     page.Items,
		NextPageToken: page.NextPageToken,
	}, nil
}

func validateListAtespacesRequest(req *ateapipb.ListAtespacesRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	if val, fldPath := req.PageSize, fldPath.Child("page_size"); val < 0 {
		errs = append(errs, field.Invalid(fldPath, val, "must be greater than or equal to 0"))
	}

	return errs
}

func (s *RPCService) DeleteAtespace(ctx context.Context, req *ateapipb.DeleteAtespaceRequest) (*ateapipb.Atespace, error) {
	if errs := validateDeleteAtespaceRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	name := req.GetAtespace().GetName()
	lock, err := s.persistence.AcquireLock(ctx, "lock:atespace:"+name)
	if errors.Is(err, store.ErrLockConflict) {
		return nil, status.Error(codes.Aborted, "another operation is using this Atespace")
	}
	if err != nil {
		return nil, fmt.Errorf("while locking Atespace: %w", err)
	}
	defer lock.Close()
	deleted, err := s.persistence.DeleteAtespace(lock.Context(), name)
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

func validateDeleteAtespaceRequest(req *ateapipb.DeleteAtespaceRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	if val, fldPath := req.Atespace, fldPath.Child("atespace"); val == nil {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, resources.ValidateGlobalObjectRef(val, fldPath)...)
	}

	return errs
}
