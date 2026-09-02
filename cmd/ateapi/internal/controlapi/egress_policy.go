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
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *RPCService) CreateActorEgressPolicy(ctx context.Context, req *ateapipb.CreateActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	policy := req.GetEgressPolicy()
	if policy != nil {
		scrubResourceMetadataForCreate(policy.Metadata)
	}
	if errs := validation.ValidateCreateActorEgressPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	return s.impl.CreateEgressPolicy(ctx, actorRef, policy)
}

func (s *ServiceImpl) CreateEgressPolicy(ctx context.Context, actorRef resources.ActorRef, policy *ateapipb.EgressPolicy) (*ateapipb.EgressPolicy, error) {
	created, err := s.store.CreateEgressPolicy(ctx, actorRef, policy)
	return mapEgressPolicyWrite(created, err)
}

func (s *RPCService) GetActorEgressPolicy(ctx context.Context, req *ateapipb.GetActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	if errs := validation.ValidateGetActorEgressPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	return s.impl.GetEgressPolicy(ctx, resources.ActorRefFromObjectRef(req.GetActor()))
}

func (s *ServiceImpl) GetEgressPolicy(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.EgressPolicy, error) {
	policy, err := s.store.GetEgressPolicy(ctx, actorRef)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "EgressPolicy not found")
	}
	if err != nil {
		return nil, fmt.Errorf("while getting Actor egress policy: %w", err)
	}
	return policy, nil
}

func (s *RPCService) UpdateActorEgressPolicy(ctx context.Context, req *ateapipb.UpdateActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	policy := req.GetEgressPolicy()
	if policy != nil {
		scrubResourceMetadataForUpdate(policy.Metadata)
	}
	if errs := validation.ValidateUpdateActorEgressPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	return s.impl.UpdateEgressPolicy(ctx, actorRef, store.PreconditionFrom(policy), func(toUpdate *ateapipb.EgressPolicy) error {
		metadata := toUpdate.GetMetadata()
		proto.Reset(toUpdate)
		proto.Merge(toUpdate, policy)
		toUpdate.Metadata = metadata
		return nil
	})
}

func (s *ServiceImpl) UpdateEgressPolicy(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition, mutate func(*ateapipb.EgressPolicy) error) (*ateapipb.EgressPolicy, error) {
	updated, err := s.store.UpdateEgressPolicy(ctx, actorRef, precondition, func(toUpdate *ateapipb.EgressPolicy) error {
		oldVal := proto.Clone(toUpdate).(*ateapipb.EgressPolicy)
		if err := mutate(toUpdate); err != nil {
			return err
		}
		if errs := validation.ValidateEgressPolicyUpdate(ctx, field.NewPath("egress_policy"), toUpdate, oldVal); len(errs) > 0 {
			return toGRPCStatusError(errs)
		}
		// EgressPolicy has no status or other server-derived fields to verify.
		return nil
	})
	return mapEgressPolicyWrite(updated, err)
}

func (s *RPCService) DeleteActorEgressPolicy(ctx context.Context, req *ateapipb.DeleteActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	if errs := validation.ValidateDeleteActorEgressPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}

	return s.impl.DeleteEgressPolicy(ctx, resources.ActorRefFromObjectRef(req.GetActor()))
}

func (s *ServiceImpl) DeleteEgressPolicy(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.EgressPolicy, error) {
	deleted, err := s.store.DeleteEgressPolicy(ctx, actorRef)
	return mapEgressPolicyWrite(deleted, err)
}

func mapEgressPolicyWrite(policy *ateapipb.EgressPolicy, err error) (*ateapipb.EgressPolicy, error) {
	switch {
	case err == nil:
		return policy, nil
	case errors.Is(err, store.ErrNotFound):
		return nil, status.Error(codes.NotFound, "EgressPolicy not found")
	case errors.Is(err, store.ErrAlreadyExists):
		return nil, status.Error(codes.AlreadyExists, "EgressPolicy already exists")
	case errors.Is(err, store.ErrVersionConflict):
		return nil, status.Error(codes.Aborted, "EgressPolicy version conflict")
	case errors.Is(err, store.ErrUIDConflict):
		return nil, status.Error(codes.Aborted, "EgressPolicy UID conflict")
	case errors.Is(err, store.ErrPreconditionRequired):
		return nil, status.Error(codes.InvalidArgument, "EgressPolicy UID and version are required")
	case errors.Is(err, store.ErrFailedPrecondition):
		return nil, status.Error(codes.FailedPrecondition, "parent Actor does not exist")
	default:
		return nil, fmt.Errorf("while writing EgressPolicy: %w", err)
	}
}
