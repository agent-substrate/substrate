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

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/apivalidation"
	"go.ate.dev/substrate/cmd/ateapi/internal/defaults"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *RPCService) CreateGlobalAccessPolicy(ctx context.Context, req *ateapi.CreateGlobalAccessPolicyRequest) (*ateapi.AccessPolicy, error) {
	policy := req.GetAccessPolicy()
	if policy != nil {
		scrubResourceMetadataForCreate(policy.Metadata)
		defaults.Apply(policy)
	}
	if errs := apivalidation.ValidateCreateGlobalAccessPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	return s.impl.CreateGlobalAccessPolicy(ctx, policy)
}

func (s *ServiceImpl) CreateGlobalAccessPolicy(ctx context.Context, policy *ateapi.AccessPolicy) (*ateapi.AccessPolicy, error) {
	created, err := s.store.CreateGlobalAccessPolicy(ctx, policy)
	return mapAccessPolicyWrite(created, err)
}

func (s *RPCService) GetGlobalAccessPolicy(ctx context.Context, req *ateapi.GetGlobalAccessPolicyRequest) (*ateapi.AccessPolicy, error) {
	if errs := apivalidation.ValidateGetGlobalAccessPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	return s.impl.GetGlobalAccessPolicy(ctx)
}

func (s *ServiceImpl) GetGlobalAccessPolicy(ctx context.Context) (*ateapi.AccessPolicy, error) {
	policy, err := s.store.GetGlobalAccessPolicy(ctx)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Global AccessPolicy not found")
		}
		return nil, fmt.Errorf("while getting Global access policy: %w", err)
	}
	return policy, nil
}

func (s *RPCService) UpdateGlobalAccessPolicy(ctx context.Context, req *ateapi.UpdateGlobalAccessPolicyRequest) (*ateapi.AccessPolicy, error) {
	policy := req.GetAccessPolicy()
	if policy != nil {
		scrubResourceMetadataForUpdate(policy.Metadata)
	}
	if errs := apivalidation.ValidateUpdateGlobalAccessPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	return s.impl.UpdateGlobalAccessPolicy(ctx, store.PreconditionFrom(policy), replaceAccessPolicy(policy))
}

func (s *ServiceImpl) UpdateGlobalAccessPolicy(ctx context.Context, precondition store.Precondition, mutate func(*ateapi.AccessPolicy) error) (*ateapi.AccessPolicy, error) {
	updated, err := s.store.UpdateGlobalAccessPolicy(ctx, precondition, func(toUpdate *ateapi.AccessPolicy) error {
		oldVal := proto.Clone(toUpdate).(*ateapi.AccessPolicy)
		if err := mutate(toUpdate); err != nil {
			return err
		}
		errs := apivalidation.ValidateGlobalAccessPolicyUpdate(ctx, field.NewPath("access_policy"), toUpdate, oldVal)
		return resources.ToAPIError(errs)
	})
	return mapAccessPolicyWrite(updated, err)
}

func (s *RPCService) CreateAtespaceAccessPolicy(ctx context.Context, req *ateapi.CreateAtespaceAccessPolicyRequest) (*ateapi.AccessPolicy, error) {
	policy := req.GetAccessPolicy()
	if policy != nil {
		scrubResourceMetadataForCreate(policy.Metadata)
		defaults.Apply(policy)
	}
	if errs := apivalidation.ValidateCreateAtespaceAccessPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	return s.impl.CreateAtespaceAccessPolicy(ctx, req.GetAtespace().GetName(), policy)
}

func (s *ServiceImpl) CreateAtespaceAccessPolicy(ctx context.Context, name string, policy *ateapi.AccessPolicy) (*ateapi.AccessPolicy, error) {
	created, err := s.store.CreateAtespaceAccessPolicy(ctx, name, policy)
	return mapAccessPolicyWrite(created, err)
}

func (s *RPCService) GetAtespaceAccessPolicy(ctx context.Context, req *ateapi.GetAtespaceAccessPolicyRequest) (*ateapi.AccessPolicy, error) {
	if errs := apivalidation.ValidateGetAtespaceAccessPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	return s.impl.GetAtespaceAccessPolicy(ctx, req.GetAtespace().GetName())
}

func (s *ServiceImpl) GetAtespaceAccessPolicy(ctx context.Context, name string) (*ateapi.AccessPolicy, error) {
	policy, err := s.store.GetAtespaceAccessPolicy(ctx, name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("AccessPolicy for atespace %s not found", name)
		}
		return nil, fmt.Errorf("while getting Atespace access policy: %w", err)
	}
	return policy, nil
}

func (s *RPCService) UpdateAtespaceAccessPolicy(ctx context.Context, req *ateapi.UpdateAtespaceAccessPolicyRequest) (*ateapi.AccessPolicy, error) {
	policy := req.GetAccessPolicy()
	if policy != nil {
		scrubResourceMetadataForUpdate(policy.Metadata)
	}
	if errs := apivalidation.ValidateUpdateAtespaceAccessPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	return s.impl.UpdateAtespaceAccessPolicy(ctx, req.GetAtespace().GetName(), store.PreconditionFrom(policy), replaceAccessPolicy(policy))
}

func (s *ServiceImpl) UpdateAtespaceAccessPolicy(ctx context.Context, name string, precondition store.Precondition, mutate func(*ateapi.AccessPolicy) error) (*ateapi.AccessPolicy, error) {
	updated, err := s.store.UpdateAtespaceAccessPolicy(ctx, name, precondition, func(toUpdate *ateapi.AccessPolicy) error {
		oldVal := proto.Clone(toUpdate).(*ateapi.AccessPolicy)
		if err := mutate(toUpdate); err != nil {
			return err
		}
		errs := apivalidation.ValidateAtespaceAccessPolicyUpdate(ctx, field.NewPath("access_policy"), toUpdate, oldVal)
		return resources.ToAPIError(errs)
	})
	return mapAccessPolicyWrite(updated, err)
}

func (s *RPCService) DeleteAtespaceAccessPolicy(ctx context.Context, req *ateapi.DeleteAtespaceAccessPolicyRequest) (*ateapi.AccessPolicy, error) {
	if errs := apivalidation.ValidateDeleteAtespaceAccessPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	return s.impl.DeleteAtespaceAccessPolicy(ctx, req.GetAtespace().GetName(), toDeletePreconditions(req.GetOptions()))
}

func (s *ServiceImpl) DeleteAtespaceAccessPolicy(ctx context.Context, name string, precondition store.DeletePreconditions) (*ateapi.AccessPolicy, error) {
	deleted, err := s.store.DeleteAtespaceAccessPolicy(ctx, name, precondition)
	return mapAccessPolicyWrite(deleted, err)
}

func replaceAccessPolicy(policy *ateapi.AccessPolicy) func(*ateapi.AccessPolicy) error {
	return func(toUpdate *ateapi.AccessPolicy) error {
		metadata := toUpdate.GetMetadata()
		proto.Reset(toUpdate)
		proto.Merge(toUpdate, policy)
		toUpdate.Metadata = metadata
		defaults.Apply(toUpdate)
		return nil
	}
}

func mapAccessPolicyWrite(policy *ateapi.AccessPolicy, err error) (*ateapi.AccessPolicy, error) {
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
	default:
		return nil, fmt.Errorf("while writing AccessPolicy: %w", err)
	}
}
