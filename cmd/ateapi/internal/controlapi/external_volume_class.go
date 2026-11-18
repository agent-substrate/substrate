// Copyright 2026 The Agent Substrate Authors
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

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/apivalidation"
	"go.ate.dev/substrate/cmd/ateapi/internal/defaults"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
)

func (s *RPCService) CreateExternalVolumeClass(ctx context.Context, req *ateapi.CreateExternalVolumeClassRequest) (*ateapi.ExternalVolumeClass, error) {
	class := req.GetExternalVolumeClass()
	if class != nil { // otherwise validation will flag it
		scrubResourceMetadataForCreate(class.Metadata)
		defaults.Apply(class)
	}
	if errs := apivalidation.ValidateCreateExternalVolumeClassRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	return s.impl.CreateExternalVolumeClass(ctx, class)
}

func (s *ServiceImpl) CreateExternalVolumeClass(ctx context.Context, class *ateapi.ExternalVolumeClass) (*ateapi.ExternalVolumeClass, error) {
	stored, err := s.store.CreateExternalVolumeClass(ctx, class)
	if errors.Is(err, store.ErrAlreadyExists) {
		return nil, apierror.AlreadyExists("ExternalVolumeClass %s already exists", class.GetMetadata().GetName())
	} else if err != nil {
		return nil, fmt.Errorf("while recording external volume class: %w", err)
	}
	return stored, nil
}

func (s *RPCService) GetExternalVolumeClass(ctx context.Context, req *ateapi.GetExternalVolumeClassRequest) (*ateapi.ExternalVolumeClass, error) {
	if errs := apivalidation.ValidateGetExternalVolumeClassRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	return s.impl.GetExternalVolumeClass(ctx, req.GetExternalVolumeClass().GetName())
}

func (s *ServiceImpl) GetExternalVolumeClass(ctx context.Context, name string) (*ateapi.ExternalVolumeClass, error) {
	class, err := s.store.GetExternalVolumeClass(ctx, name)
	if errors.Is(err, store.ErrNotFound) {
		return nil, apierror.NotFound("ExternalVolumeClass %s not found", name)
	} else if err != nil {
		return nil, fmt.Errorf("while getting external volume class from DB: %w", err)
	}
	return class, nil
}

func (s *RPCService) ListExternalVolumeClasses(ctx context.Context, req *ateapi.ListExternalVolumeClassesRequest) (*ateapi.ListExternalVolumeClassesResponse, error) {
	if errs := apivalidation.ValidateListExternalVolumeClassesRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	page, err := s.impl.ListExternalVolumeClasses(ctx, toStoreListOptions(req.GetOptions()))
	if err != nil {
		return nil, err
	}
	return &ateapi.ListExternalVolumeClassesResponse{
		ExternalVolumeClasses: page.Items,
		NextPageToken:         page.NextPageToken,
	}, nil
}

func (s *ServiceImpl) ListExternalVolumeClasses(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapi.ExternalVolumeClass], error) {
	page, err := s.store.ListExternalVolumeClasses(ctx, opts)
	if err != nil {
		return page, mapListError(fmt.Errorf("while listing external volume classes in db: %w", err))
	}
	return page, nil
}

func (s *RPCService) DeleteExternalVolumeClass(ctx context.Context, req *ateapi.DeleteExternalVolumeClassRequest) (*ateapi.ExternalVolumeClass, error) {
	if errs := apivalidation.ValidateDeleteExternalVolumeClassRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	return s.impl.DeleteExternalVolumeClass(ctx, req.GetExternalVolumeClass().GetName(), toDeletePreconditions(req.GetOptions()))
}

func (s *ServiceImpl) DeleteExternalVolumeClass(ctx context.Context, name string, precondition store.DeletePreconditions) (*ateapi.ExternalVolumeClass, error) {
	deleted, err := s.store.DeleteExternalVolumeClass(ctx, name, precondition)
	switch {
	case err == nil:
		return deleted, nil
	case errors.Is(err, store.ErrNotFound):
		return nil, apierror.NotFound("ExternalVolumeClass %s not found", name)
	case errors.Is(err, store.ErrUIDConflict):
		return nil, apierror.Aborted("ExternalVolumeClass %s does not have uid %s", name, precondition.UID)
	case errors.Is(err, store.ErrVersionConflict):
		return nil, apierror.Aborted("concurrent update conflict, please retry")
	default:
		return nil, fmt.Errorf("while deleting external volume class from DB: %w", err)
	}
}
