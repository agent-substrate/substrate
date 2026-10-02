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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/apivalidation"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/defaults"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *RPCService) CreateGlobalAccessPolicy(ctx context.Context, req *ateapipb.CreateGlobalAccessPolicyRequest) (*ateapipb.AccessPolicy, error) {
	policy := req.GetAccessPolicy()
	if policy != nil {
		scrubResourceMetadataForCreate(policy.Metadata)
		defaults.Apply(policy)
	}
	if errs := apivalidation.ValidateCreateGlobalAccessPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToGRPCStatusError(errs)
	}
	return s.impl.CreateGlobalAccessPolicy(ctx, policy)
}

func (s *ServiceImpl) CreateGlobalAccessPolicy(ctx context.Context, policy *ateapipb.AccessPolicy) (*ateapipb.AccessPolicy, error) {
	created, err := s.store.CreateGlobalAccessPolicy(ctx, policy)
	return mapAccessPolicyWrite(created, err)
}

func (s *RPCService) GetGlobalAccessPolicy(ctx context.Context, req *ateapipb.GetGlobalAccessPolicyRequest) (*ateapipb.AccessPolicy, error) {
	if errs := apivalidation.ValidateGetGlobalAccessPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToGRPCStatusError(errs)
	}
	return s.impl.GetGlobalAccessPolicy(ctx)
}

func (s *ServiceImpl) GetGlobalAccessPolicy(ctx context.Context) (*ateapipb.AccessPolicy, error) {
	policy, err := s.store.GetGlobalAccessPolicy(ctx)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "Global AccessPolicy not found")
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
		return nil, resources.ToGRPCStatusError(errs)
	}
	return s.impl.UpdateGlobalAccessPolicy(ctx, store.PreconditionFrom(policy), replaceAccessPolicy(policy))
}

func (s *ServiceImpl) UpdateGlobalAccessPolicy(ctx context.Context, precondition store.Precondition, mutate func(*ateapipb.AccessPolicy) error) (*ateapipb.AccessPolicy, error) {
	updated, err := s.store.UpdateGlobalAccessPolicy(ctx, precondition, func(toUpdate *ateapipb.AccessPolicy) error {
		oldVal := proto.Clone(toUpdate).(*ateapipb.AccessPolicy)
		if err := mutate(toUpdate); err != nil {
			return err
		}
		if errs := apivalidation.ValidateGlobalAccessPolicyUpdate(ctx, field.NewPath("access_policy"), toUpdate, oldVal); len(errs) > 0 {
			return resources.ToGRPCStatusError(errs)
		}
		return nil
	})
	return mapAccessPolicyWrite(updated, err)
}

func (s *RPCService) CreateAtespaceAccessPolicy(ctx context.Context, req *ateapipb.CreateAtespaceAccessPolicyRequest) (*ateapipb.AccessPolicy, error) {
	policy := req.GetAccessPolicy()
	if policy != nil {
		scrubResourceMetadataForCreate(policy.Metadata)
		defaults.Apply(policy)
	}
	if errs := apivalidation.ValidateCreateAtespaceAccessPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToGRPCStatusError(errs)
	}
	return s.impl.CreateAtespaceAccessPolicy(ctx, req.GetAtespace().GetName(), policy)
}

func (s *ServiceImpl) CreateAtespaceAccessPolicy(ctx context.Context, name string, policy *ateapipb.AccessPolicy) (*ateapipb.AccessPolicy, error) {
	created, err := s.store.CreateAtespaceAccessPolicy(ctx, name, policy)
	return mapAccessPolicyWrite(created, err)
}

func (s *RPCService) GetAtespaceAccessPolicy(ctx context.Context, req *ateapipb.GetAtespaceAccessPolicyRequest) (*ateapipb.AccessPolicy, error) {
	if errs := apivalidation.ValidateGetAtespaceAccessPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToGRPCStatusError(errs)
	}
	return s.impl.GetAtespaceAccessPolicy(ctx, req.GetAtespace().GetName())
}

func (s *ServiceImpl) GetAtespaceAccessPolicy(ctx context.Context, name string) (*ateapipb.AccessPolicy, error) {
	policy, err := s.store.GetAtespaceAccessPolicy(ctx, name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "AccessPolicy for atespace %s not found", name)
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
		return nil, resources.ToGRPCStatusError(errs)
	}
	return s.impl.UpdateAtespaceAccessPolicy(ctx, req.GetAtespace().GetName(), store.PreconditionFrom(policy), replaceAccessPolicy(policy))
}

func (s *ServiceImpl) UpdateAtespaceAccessPolicy(ctx context.Context, name string, precondition store.Precondition, mutate func(*ateapipb.AccessPolicy) error) (*ateapipb.AccessPolicy, error) {
	updated, err := s.store.UpdateAtespaceAccessPolicy(ctx, name, precondition, func(toUpdate *ateapipb.AccessPolicy) error {
		oldVal := proto.Clone(toUpdate).(*ateapipb.AccessPolicy)
		if err := mutate(toUpdate); err != nil {
			return err
		}
		if errs := apivalidation.ValidateAtespaceAccessPolicyUpdate(ctx, field.NewPath("access_policy"), toUpdate, oldVal); len(errs) > 0 {
			return resources.ToGRPCStatusError(errs)
		}
		return nil
	})
	return mapAccessPolicyWrite(updated, err)
}

func (s *RPCService) DeleteAtespaceAccessPolicy(ctx context.Context, req *ateapipb.DeleteAtespaceAccessPolicyRequest) (*ateapipb.AccessPolicy, error) {
	if errs := apivalidation.ValidateDeleteAtespaceAccessPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToGRPCStatusError(errs)
	}
	return s.impl.DeleteAtespaceAccessPolicy(ctx, req.GetAtespace().GetName(), toDeletePreconditions(req.GetOptions()))
}

func (s *ServiceImpl) DeleteAtespaceAccessPolicy(ctx context.Context, name string, precondition store.DeletePreconditions) (*ateapipb.AccessPolicy, error) {
	deleted, err := s.store.DeleteAtespaceAccessPolicy(ctx, name, precondition)
	return mapAccessPolicyWrite(deleted, err)
}

func replaceAccessPolicy(policy *ateapipb.AccessPolicy) func(*ateapipb.AccessPolicy) error {
	return func(toUpdate *ateapipb.AccessPolicy) error {
		metadata := toUpdate.GetMetadata()
		proto.Reset(toUpdate)
		proto.Merge(toUpdate, policy)
		toUpdate.Metadata = metadata
		defaults.Apply(toUpdate)
		return nil
	}
}

func mapAccessPolicyWrite(policy *ateapipb.AccessPolicy, err error) (*ateapipb.AccessPolicy, error) {
	switch {
	case err == nil:
		return policy, nil
	case errors.Is(err, store.ErrNotFound):
		return nil, status.Error(codes.NotFound, "AccessPolicy not found")
	case errors.Is(err, store.ErrAlreadyExists):
		return nil, status.Error(codes.AlreadyExists, "AccessPolicy already exists")
	case errors.Is(err, store.ErrVersionConflict):
		return nil, status.Error(codes.Aborted, "AccessPolicy version conflict")
	case errors.Is(err, store.ErrUIDConflict):
		return nil, status.Error(codes.Aborted, "AccessPolicy UID conflict")
	case errors.Is(err, store.ErrPreconditionRequired):
		return nil, status.Error(codes.InvalidArgument, "AccessPolicy UID and version are required")
	case errors.Is(err, store.ErrFailedPrecondition):
		return nil, status.Error(codes.FailedPrecondition, "parent Atespace does not exist")
	default:
		return nil, toCanonicalStatus(fmt.Errorf("while writing AccessPolicy: %w", err))
	}
}

// toCanonicalStatus passes err through when it carries a canonical gRPC code
// (including a plain error, which gRPC reports as Unknown), and reports
// anything else as Internal. Backends such as OpenFGA return statuses with
// nonstandard codes (for example Code(2000)) that clients cannot interpret.
func toCanonicalStatus(err error) error {
	st, ok := status.FromError(err)
	if !ok || st.Code() <= codes.Unauthenticated {
		return err
	}
	return status.Error(codes.Internal, err.Error())
}
