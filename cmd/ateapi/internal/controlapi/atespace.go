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
	"github.com/agent-substrate/substrate/internal/authz"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *RPCService) CreateAtespace(ctx context.Context, req *ateapipb.CreateAtespaceRequest) (*ateapipb.Atespace, error) {
	// First scrub any fields that users are not allowed to set, then fill the
	// defaults so validation sees the final resource state.
	inAtespace := req.GetAtespace()
	if inAtespace != nil { // otherwise validation will flag it
		scrubResourceMetadataForCreate(inAtespace.Metadata)
		// no status field, but if there were, we would scrub it here
		defaultAtespace(inAtespace)
	}

	// Validate the request, including the object within it.
	if errs := validateCreateAtespaceRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	if err := s.authorize(ctx, authz.RelationCanCreateAtespace, authz.GlobalRootObject); err != nil {
		return nil, err
	}

	// Handle the creation, including validation of the final stored object.
	stored, err := s.impl.CreateAtespace(ctx, inAtespace)
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			// Ensure parent_global tuple exists without deleting any existing tuples on the live atespace.
			_ = s.ensureParentGlobal(ctx, inAtespace.GetMetadata().GetName())
		}
		return nil, err
	}

	// TODO: Consider a Postgres transactional outbox pattern to atomically coordinate
	// storage mutations and OpenFGA tuple writes across the two calls.
	if err := s.onCreateAtespace(ctx, stored.GetMetadata().GetName()); err != nil {
		// Roll back DB creation so no wedged/orphaned atespace remains without authorization tuples.
		_, _ = s.impl.DeleteAtespace(ctx, stored.GetMetadata().GetName())
		return nil, status.Errorf(codes.Internal, "failed to record authorization tuples for atespace %s: %v", stored.GetMetadata().GetName(), err)
	}

	return stored, nil
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

func validateCreateAtespaceRequest(ctx context.Context, req *ateapipb.CreateAtespaceRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_CreateAtespaceRequest(ctx, op, nil, req, nil)
}

func (s *RPCService) GetAtespace(ctx context.Context, req *ateapipb.GetAtespaceRequest) (*ateapipb.Atespace, error) {
	if errs := validateGetAtespaceRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	if err := s.authorize(ctx, authz.RelationCanGet, authz.AtespaceObject(req.Atespace.Name)); err != nil {
		return nil, err
	}

	res, err := s.impl.GetAtespace(ctx, req.Atespace.Name)
	if err != nil {
		return nil, err
	}
	_ = s.ensureParentGlobal(ctx, req.Atespace.Name)
	return res, nil
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

func validateGetAtespaceRequest(ctx context.Context, req *ateapipb.GetAtespaceRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_GetAtespaceRequest(ctx, op, nil, req, nil)
}

func (s *RPCService) ListAtespaces(ctx context.Context, req *ateapipb.ListAtespacesRequest) (*ateapipb.ListAtespacesResponse, error) {
	if errs := validateListAtespacesRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	var (
		all            = true
		allowedObjects map[string]bool
	)
	if s.authorizer != nil {
		var err error
		all, allowedObjects, err = s.authorizer.ListAccessibleAtespaces(ctx)
		if err != nil {
			return nil, err
		}
	}

	if all {
		page, err := s.impl.ListAtespaces(ctx, store.ListOptions{PageSize: req.PageSize, PageToken: req.PageToken})
		if err != nil {
			return nil, err
		}
		return &ateapipb.ListAtespacesResponse{
			Atespaces:     page.Items,
			NextPageToken: page.NextPageToken,
		}, nil
	}

	targetSize := int(effectivePageSize(req.PageSize))
	var filtered []*ateapipb.Atespace
	currToken := req.PageToken
	for {
		page, err := s.impl.ListAtespaces(ctx, store.ListOptions{PageSize: req.PageSize, PageToken: currToken})
		if err != nil {
			return nil, err
		}
		for _, item := range page.Items {
			if allowedObjects[authz.AtespaceObject(item.GetMetadata().GetName())] {
				filtered = append(filtered, item)
			}
		}
		currToken = page.NextPageToken
		if len(filtered) >= targetSize || currToken == "" {
			break
		}
	}
	return &ateapipb.ListAtespacesResponse{
		Atespaces:     filtered,
		NextPageToken: currToken,
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

func validateListAtespacesRequest(ctx context.Context, req *ateapipb.ListAtespacesRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_ListAtespacesRequest(ctx, op, nil, req, nil)
}

func (s *RPCService) DeleteAtespace(ctx context.Context, req *ateapipb.DeleteAtespaceRequest) (*ateapipb.Atespace, error) {
	if errs := validateDeleteAtespaceRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	if err := s.authorize(ctx, authz.RelationCanDelete, authz.AtespaceObject(req.Atespace.Name)); err != nil {
		return nil, err
	}

	deleted, err := s.impl.DeleteAtespace(ctx, req.Atespace.Name)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			if delErr := s.onDeleteAtespace(ctx, req.Atespace.Name); delErr != nil {
				return nil, status.Errorf(codes.Internal, "failed to delete authorization tuples for atespace %s: %v", req.Atespace.Name, delErr)
			}
		}
		return nil, err
	}

	// TODO: Consider a Postgres transactional outbox pattern to atomically coordinate
	// storage deletions and OpenFGA tuple cleanup across the two calls.
	if err := s.onDeleteAtespace(ctx, req.Atespace.Name); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete authorization tuples for atespace %s: %v", req.Atespace.Name, err)
	}

	return deleted, nil
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

func validateDeleteAtespaceRequest(ctx context.Context, req *ateapipb.DeleteAtespaceRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_DeleteAtespaceRequest(ctx, op, nil, req, nil)
}
