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

func (s *RPCService) CreateGlobalAccessPolicy(ctx context.Context, req *ateapipb.CreateGlobalAccessPolicyRequest) (*ateapipb.AccessPolicy, error) {
	policy := req.GetAccessPolicy()
	if policy != nil {
		scrubResourceMetadataForCreate(policy.Metadata)
		defaults.Apply(policy)
	}
	if errs := apivalidation.ValidateCreateGlobalAccessPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	created, err := s.admission.CreateGlobalAccessPolicy(ctx, policy)
	return mapAccessPolicyWrite(created, err)
}

func (s *RPCService) GetGlobalAccessPolicy(ctx context.Context, req *ateapipb.GetGlobalAccessPolicyRequest) (*ateapipb.AccessPolicy, error) {
	if errs := apivalidation.ValidateGetGlobalAccessPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	policy, err := s.admission.GetGlobalAccessPolicy(ctx)
	if err != nil {
		// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Global AccessPolicy not found")
		}
		return nil, fmt.Errorf("while getting Global access policy: %w", err)
	}
	return policy, nil
}

func (s *RPCService) UpdateGlobalAccessPolicy(ctx context.Context, req *ateapipb.UpdateGlobalAccessPolicyRequest) (*ateapipb.AccessPolicy, error) {
	policy := req.GetAccessPolicy()
	if policy != nil {
		scrubResourceMetadataForUpdate(policy.Metadata)
	}
	if errs := apivalidation.ValidateUpdateGlobalAccessPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	updated, err := s.admission.UpdateGlobalAccessPolicy(ctx, policy)
	return mapAccessPolicyWrite(updated, err)
}

func (s *RPCService) CreateAtespaceAccessPolicy(ctx context.Context, req *ateapipb.CreateAtespaceAccessPolicyRequest) (*ateapipb.AccessPolicy, error) {
	policy := req.GetAccessPolicy()
	if policy != nil {
		scrubResourceMetadataForCreate(policy.Metadata)
		defaults.Apply(policy)
	}
	if errs := apivalidation.ValidateCreateAtespaceAccessPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	created, err := s.admission.CreateAtespaceAccessPolicy(ctx, req.GetAtespace().GetName(), policy)
	return mapAccessPolicyWrite(created, err)
}

func (s *RPCService) GetAtespaceAccessPolicy(ctx context.Context, req *ateapipb.GetAtespaceAccessPolicyRequest) (*ateapipb.AccessPolicy, error) {
	if errs := apivalidation.ValidateGetAtespaceAccessPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	name := req.GetAtespace().GetName()
	policy, err := s.admission.GetAtespaceAccessPolicy(ctx, name)
	if err != nil {
		// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("AccessPolicy for atespace %s not found", name)
		}
		return nil, fmt.Errorf("while getting Atespace access policy: %w", err)
	}
	return policy, nil
}

func (s *RPCService) UpdateAtespaceAccessPolicy(ctx context.Context, req *ateapipb.UpdateAtespaceAccessPolicyRequest) (*ateapipb.AccessPolicy, error) {
	policy := req.GetAccessPolicy()
	if policy != nil {
		scrubResourceMetadataForUpdate(policy.Metadata)
	}
	if errs := apivalidation.ValidateUpdateAtespaceAccessPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	updated, err := s.admission.UpdateAtespaceAccessPolicy(ctx, req.GetAtespace().GetName(), policy)
	return mapAccessPolicyWrite(updated, err)
}

func (s *RPCService) DeleteAtespaceAccessPolicy(ctx context.Context, req *ateapipb.DeleteAtespaceAccessPolicyRequest) (*ateapipb.AccessPolicy, error) {
	if errs := apivalidation.ValidateDeleteAtespaceAccessPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	deleted, err := s.admission.DeleteAtespaceAccessPolicy(ctx, req.GetAtespace().GetName(), toDeletePreconditions(req.GetOptions()))
	return mapAccessPolicyWrite(deleted, err)
}

func mapAccessPolicyWrite(policy *ateapipb.AccessPolicy, err error) (*ateapipb.AccessPolicy, error) {
	// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
	switch {
	case err == nil:
		return policy, nil
	case errors.Is(err, store.ErrNotFound):
		return nil, apierror.NotFound("AccessPolicy not found")
	case errors.Is(err, store.ErrAlreadyExists):
		return nil, apierror.AlreadyExists("AccessPolicy already exists")
	case errors.Is(err, store.ErrVersionConflict):
		return nil, apierror.Aborted("AccessPolicy version conflict")
	case errors.Is(err, store.ErrUIDConflict):
		return nil, apierror.Aborted("AccessPolicy UID conflict")
	case errors.Is(err, store.ErrPreconditionRequired):
		return nil, apierror.InvalidArgument("AccessPolicy UID and version are required")
	case errors.Is(err, store.ErrFailedPrecondition):
		return nil, apierror.FailedPrecondition("parent Atespace does not exist")
	case errors.Is(err, admission.ErrInvalid):
		return nil, apierror.InvalidArgument("%w", err)
	default:
		return nil, fmt.Errorf("while writing AccessPolicy: %w", err)
	}
}
