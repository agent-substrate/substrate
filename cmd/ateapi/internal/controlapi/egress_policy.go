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

func (s *RPCService) CreateActorEgressPolicy(ctx context.Context, req *ateapipb.CreateActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	// First scrub any fields that users are not allowed to set, then fill the
	// defaults so validation sees the final resource state.
	policy := req.GetEgressPolicy()
	if policy != nil {
		scrubResourceMetadataForCreate(policy.Metadata)
		defaults.Apply(policy)
	}
	if errs := apivalidation.ValidateCreateActorEgressPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	created, err := s.admission.CreateEgressPolicy(ctx, actorRef, policy)
	return mapEgressPolicyWrite(created, err)
}

func (s *RPCService) GetActorEgressPolicy(ctx context.Context, req *ateapipb.GetActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	if errs := apivalidation.ValidateGetActorEgressPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	policy, err := s.admission.GetEgressPolicy(ctx, actorRef)
	// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
	if errors.Is(err, store.ErrNotFound) {
		return nil, apierror.NotFound("EgressPolicy for actor %s not found", actorRef)
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
	if errs := apivalidation.ValidateUpdateActorEgressPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	updated, err := s.admission.UpdateEgressPolicy(ctx, actorRef, policy)
	return mapEgressPolicyWrite(updated, err)
}

func (s *RPCService) DeleteActorEgressPolicy(ctx context.Context, req *ateapipb.DeleteActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	if errs := apivalidation.ValidateDeleteActorEgressPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	deleted, err := s.admission.DeleteEgressPolicy(ctx, resources.ActorRefFromObjectRef(req.GetActor()), toDeletePreconditions(req.GetOptions()))
	return mapEgressPolicyWrite(deleted, err)
}

func mapEgressPolicyWrite(policy *ateapipb.EgressPolicy, err error) (*ateapipb.EgressPolicy, error) {
	// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
	switch {
	case err == nil:
		return policy, nil
	case errors.Is(err, store.ErrNotFound):
		return nil, apierror.NotFound("EgressPolicy not found")
	case errors.Is(err, store.ErrAlreadyExists):
		return nil, apierror.AlreadyExists("EgressPolicy already exists")
	case errors.Is(err, store.ErrVersionConflict):
		return nil, apierror.Aborted("EgressPolicy version conflict")
	case errors.Is(err, store.ErrUIDConflict):
		return nil, apierror.Aborted("EgressPolicy UID conflict")
	case errors.Is(err, store.ErrPreconditionRequired):
		return nil, apierror.InvalidArgument("EgressPolicy UID and version are required")
	case errors.Is(err, store.ErrFailedPrecondition):
		return nil, apierror.FailedPrecondition("parent Actor does not exist")
	case errors.Is(err, admission.ErrInvalid):
		return nil, apierror.InvalidArgument("%w", err)
	default:
		return nil, fmt.Errorf("while writing EgressPolicy: %w", err)
	}
}
