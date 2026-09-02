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
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/validation"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *RPCService) CreateAtespace(ctx context.Context, req *ateapipb.CreateAtespaceRequest) (*ateapipb.Atespace, error) {
	inAtespace := req.GetAtespace()
	if inAtespace != nil { // otherwise validation will flag it
		scrubResourceMetadataForCreate(inAtespace.Metadata)
		// no status field, but if there were, we would scrub it here
	}

	// Validate the request, including the object within it.
	if errs := validation.ValidateCreateAtespaceRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	// Handle the creation, including validation of the final stored object.
	return s.impl.CreateAtespace(ctx, inAtespace)
}

func (s *ServiceImpl) CreateAtespace(ctx context.Context, inAtespace *ateapipb.Atespace) (*ateapipb.Atespace, error) {
	// no further processing or status, but if there were, we would do it here

	// Save the data in the storage layer.
	stored, err := s.store.CreateAtespace(ctx, inAtespace)
	if err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "Atespace %s already exists", inAtespace.Metadata.Name)
		}
		return nil, fmt.Errorf("while recording atespace: %w", err)
	}

	return stored, nil
}

func (s *RPCService) GetAtespace(ctx context.Context, req *ateapipb.GetAtespaceRequest) (*ateapipb.Atespace, error) {
	if errs := validation.ValidateGetAtespaceRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	return s.impl.GetAtespace(ctx, req.Atespace.Name)
}

func (s *ServiceImpl) GetAtespace(ctx context.Context, name string) (*ateapipb.Atespace, error) {
	atespace, err := s.store.GetAtespace(ctx, name)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "Atespace %s not found", name)
	} else if err != nil {
		return nil, fmt.Errorf("while getting atespace from DB: %w", err)
	}

	return atespace, nil
}

func (s *RPCService) ListAtespaces(ctx context.Context, req *ateapipb.ListAtespacesRequest) (*ateapipb.ListAtespacesResponse, error) {
	if errs := validation.ValidateListAtespacesRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	page, err := s.impl.ListAtespaces(ctx, store.ListOptions{PageSize: req.PageSize, PageToken: req.PageToken})
	if err != nil {
		return nil, err
	}
	return &ateapipb.ListAtespacesResponse{
		Atespaces:     page.Items,
		NextPageToken: page.NextPageToken,
	}, nil
}

func (s *ServiceImpl) ListAtespaces(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapipb.Atespace], error) {
	opts.PageSize = effectivePageSize(opts.PageSize)
	page, err := s.store.ListAtespaces(ctx, opts)
	if err != nil {
		return page, mapListError(fmt.Errorf("while listing atespaces in db: %w", err))
	}
	return page, nil
}

func (s *RPCService) DeleteAtespace(ctx context.Context, req *ateapipb.DeleteAtespaceRequest) (*ateapipb.Atespace, error) {
	if errs := validation.ValidateDeleteAtespaceRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	return s.impl.DeleteAtespace(ctx, req.Atespace.Name)
}

func (s *ServiceImpl) DeleteAtespace(ctx context.Context, name string) (*ateapipb.Atespace, error) {
	deleted, err := s.store.DeleteAtespace(ctx, name)
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
