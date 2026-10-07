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

package admission

import (
	"context"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/apivalidation"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/defaults"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// CreateEgressPolicy validates and creates the EgressPolicy for actorRef.
//
// Returns ErrInvalid if policy fails declarative create validation,
// store.ErrAlreadyExists if the actor already has an egress policy, or
// store.ErrFailedPrecondition if the referenced actor does not exist.
func (a *Admission) CreateEgressPolicy(ctx context.Context, actorRef resources.ActorRef, policy *ateapipb.EgressPolicy) (*ateapipb.EgressPolicy, error) {
	outPolicy := proto.CloneOf(policy)
	defaults.Apply(outPolicy)
	if errs := apivalidation.ValidateEgressPolicyCreate(ctx, field.NewPath("egress_policy"), actorRef.ToObjectRef(), outPolicy); len(errs) > 0 {
		return nil, invalidf("%v", errs.ToAggregate())
	}
	return a.store.CreateEgressPolicy(ctx, actorRef, outPolicy)
}

// GetEgressPolicy retrieves the EgressPolicy for actorRef.
//
// Returns store.ErrNotFound if the egress policy does not exist.
func (a *Admission) GetEgressPolicy(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.EgressPolicy, error) {
	return a.store.GetEgressPolicy(ctx, actorRef)
}

// UpdateEgressPolicy replaces the EgressPolicy for actorRef, preserving
// server-owned metadata and enforcing declarative validation.
//
// Returns store.ErrPreconditionRequired if metadata uid or version is unset,
// store.ErrNotFound if the egress policy does not exist, store.ErrUIDConflict
// or store.ErrVersionConflict if the precondition does not match the stored
// policy, or ErrInvalid if the updated policy fails declarative validation.
func (a *Admission) UpdateEgressPolicy(ctx context.Context, actorRef resources.ActorRef, policy *ateapipb.EgressPolicy) (*ateapipb.EgressPolicy, error) {
	return a.store.UpdateEgressPolicy(ctx, actorRef, store.PreconditionFrom(policy), func(toUpdate *ateapipb.EgressPolicy) error {
		oldVal := proto.CloneOf(toUpdate)
		metadata := toUpdate.GetMetadata()
		proto.Reset(toUpdate)
		proto.Merge(toUpdate, policy)
		toUpdate.Metadata = metadata
		defaults.Apply(toUpdate)
		if errs := apivalidation.ValidateEgressPolicyUpdate(ctx, field.NewPath("egress_policy"), toUpdate, oldVal); len(errs) > 0 {
			return invalidf("%v", errs.ToAggregate())
		}
		return nil
	})
}

// DeleteEgressPolicy deletes the EgressPolicy for actorRef.
//
// Returns store.ErrNotFound if the egress policy does not exist, or
// store.ErrUIDConflict or store.ErrVersionConflict if precondition does not
// match the stored policy.
func (a *Admission) DeleteEgressPolicy(ctx context.Context, actorRef resources.ActorRef, precondition store.DeletePreconditions) (*ateapipb.EgressPolicy, error) {
	return a.store.DeleteEgressPolicy(ctx, actorRef, precondition)
}
