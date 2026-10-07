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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/admission"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/apivalidation"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/defaults"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func (s *RPCService) CreateAtespace(ctx context.Context, req *ateapipb.CreateAtespaceRequest) (*ateapipb.Atespace, error) {
	// First scrub any fields that users are not allowed to set, then fill the
	// defaults so validation sees the final resource state.
	inAtespace := req.GetAtespace()
	if inAtespace != nil { // otherwise validation will flag it
		scrubResourceMetadataForCreate(inAtespace.Metadata)
		// no status field, but if there were, we would scrub it here
		defaults.Apply(inAtespace)
	}

	// Validate the request, including the object within it.
	if errs := apivalidation.ValidateCreateAtespaceRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	stored, err := s.admission.CreateAtespace(ctx, inAtespace)
	if err != nil {
		// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, apierror.AlreadyExists("Atespace %s already exists", inAtespace.Metadata.Name)
		}
		if errors.Is(err, admission.ErrInvalid) {
			return nil, apierror.InvalidArgument("%w", err)
		}
		return nil, fmt.Errorf("while recording atespace: %w", err)
	}

	return stored, nil
}

func (s *RPCService) GetAtespace(ctx context.Context, req *ateapipb.GetAtespaceRequest) (*ateapipb.Atespace, error) {
	if errs := apivalidation.ValidateGetAtespaceRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	name := req.Atespace.Name
	atespace, err := s.admission.GetAtespace(ctx, name)
	// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
	if errors.Is(err, store.ErrNotFound) {
		return nil, apierror.NotFound("Atespace %s not found", name)
	} else if err != nil {
		return nil, fmt.Errorf("while getting atespace from DB: %w", err)
	}

	return atespace, nil
}

func (s *RPCService) ListAtespaces(ctx context.Context, req *ateapipb.ListAtespacesRequest) (*ateapipb.ListAtespacesResponse, error) {
	if errs := apivalidation.ValidateListAtespacesRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	page, err := s.admission.ListAtespaces(ctx, store.ListOptions{PageSize: effectivePageSize(req.PageSize), PageToken: req.PageToken})
	if err != nil {
		return nil, mapListError(fmt.Errorf("while listing atespaces in db: %w", err))
	}
	return &ateapipb.ListAtespacesResponse{
		Atespaces:     page.Items,
		NextPageToken: page.NextPageToken,
	}, nil
}

func (s *RPCService) DeleteAtespace(ctx context.Context, req *ateapipb.DeleteAtespaceRequest) (*ateapipb.Atespace, error) {
	if errs := apivalidation.ValidateDeleteAtespaceRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	name := req.Atespace.Name
	precondition := toDeletePreconditions(req.GetOptions())
	deleted, err := s.admission.DeleteAtespace(ctx, name, precondition)
	if err != nil {
		// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Atespace %s not found", name)
		}
		if errors.Is(err, store.ErrFailedPrecondition) {
			return nil, apierror.FailedPrecondition("Atespace %s is not empty", name)
		}
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, apierror.Aborted("Atespace %s does not have uid %s", name, precondition.UID)
		}
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while deleting atespace from DB: %w", err)
	}

	return deleted, nil
}
