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
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// CreateGlobalAccessPolicy validates and creates the singleton global AccessPolicy.
//
// Returns ErrInvalid if policy fails declarative validation, or
// store.ErrAlreadyExists if the global access policy already exists.
func (a *Admission) CreateGlobalAccessPolicy(ctx context.Context, policy *ateapipb.AccessPolicy) (*ateapipb.AccessPolicy, error) {
	outPolicy := proto.CloneOf(policy)
	defaults.Apply(outPolicy)
	if errs := apivalidation.ValidateGlobalAccessPolicyCreate(ctx, field.NewPath("access_policy"), outPolicy); len(errs) > 0 {
		return nil, invalidf("%v", errs.ToAggregate())
	}
	return a.store.CreateGlobalAccessPolicy(ctx, outPolicy)
}

// GetGlobalAccessPolicy retrieves the singleton global AccessPolicy.
//
// Returns store.ErrNotFound if the global access policy does not exist.
func (a *Admission) GetGlobalAccessPolicy(ctx context.Context) (*ateapipb.AccessPolicy, error) {
	return a.store.GetGlobalAccessPolicy(ctx)
}

// UpdateGlobalAccessPolicy replaces the singleton global AccessPolicy,
// preserving server-owned metadata and enforcing declarative validation.
//
// Returns store.ErrPreconditionRequired if metadata uid or version is unset,
// store.ErrNotFound if the global access policy does not exist,
// store.ErrUIDConflict or store.ErrVersionConflict if the precondition does not
// match the stored policy, or ErrInvalid if the updated policy fails
// declarative validation.
func (a *Admission) UpdateGlobalAccessPolicy(ctx context.Context, policy *ateapipb.AccessPolicy) (*ateapipb.AccessPolicy, error) {
	return a.store.UpdateGlobalAccessPolicy(ctx, store.PreconditionFrom(policy), func(toUpdate *ateapipb.AccessPolicy) error {
		oldVal := proto.CloneOf(toUpdate)
		metadata := toUpdate.GetMetadata()
		proto.Reset(toUpdate)
		proto.Merge(toUpdate, policy)
		toUpdate.Metadata = metadata
		defaults.Apply(toUpdate)
		if errs := apivalidation.ValidateGlobalAccessPolicyUpdate(ctx, field.NewPath("access_policy"), toUpdate, oldVal); len(errs) > 0 {
			return invalidf("%v", errs.ToAggregate())
		}
		return nil
	})
}

// CreateAtespaceAccessPolicy validates and creates the AccessPolicy for the named Atespace.
//
// Returns ErrInvalid if policy fails declarative validation,
// store.ErrAlreadyExists if the atespace already has an access policy, or
// store.ErrFailedPrecondition if the atespace does not exist.
func (a *Admission) CreateAtespaceAccessPolicy(ctx context.Context, name string, policy *ateapipb.AccessPolicy) (*ateapipb.AccessPolicy, error) {
	outPolicy := proto.CloneOf(policy)
	defaults.Apply(outPolicy)
	if errs := apivalidation.ValidateAtespaceAccessPolicyCreate(ctx, field.NewPath("access_policy"), outPolicy); len(errs) > 0 {
		return nil, invalidf("%v", errs.ToAggregate())
	}
	return a.store.CreateAtespaceAccessPolicy(ctx, name, outPolicy)
}

// GetAtespaceAccessPolicy retrieves the AccessPolicy for the named Atespace.
//
// Returns store.ErrNotFound if the access policy does not exist.
func (a *Admission) GetAtespaceAccessPolicy(ctx context.Context, name string) (*ateapipb.AccessPolicy, error) {
	return a.store.GetAtespaceAccessPolicy(ctx, name)
}

// UpdateAtespaceAccessPolicy replaces the AccessPolicy for the named Atespace,
// preserving server-owned metadata and enforcing declarative validation.
//
// Returns store.ErrPreconditionRequired if metadata uid or version is unset,
// store.ErrNotFound if the access policy does not exist, store.ErrUIDConflict
// or store.ErrVersionConflict if the precondition does not match the stored
// policy, or ErrInvalid if the updated policy fails declarative validation.
func (a *Admission) UpdateAtespaceAccessPolicy(ctx context.Context, name string, policy *ateapipb.AccessPolicy) (*ateapipb.AccessPolicy, error) {
	return a.store.UpdateAtespaceAccessPolicy(ctx, name, store.PreconditionFrom(policy), func(toUpdate *ateapipb.AccessPolicy) error {
		oldVal := proto.CloneOf(toUpdate)
		metadata := toUpdate.GetMetadata()
		proto.Reset(toUpdate)
		proto.Merge(toUpdate, policy)
		toUpdate.Metadata = metadata
		defaults.Apply(toUpdate)
		if errs := apivalidation.ValidateAtespaceAccessPolicyUpdate(ctx, field.NewPath("access_policy"), toUpdate, oldVal); len(errs) > 0 {
			return invalidf("%v", errs.ToAggregate())
		}
		return nil
	})
}

// DeleteAtespaceAccessPolicy deletes the AccessPolicy for the named Atespace.
//
// Returns store.ErrNotFound if the access policy does not exist, or
// store.ErrUIDConflict or store.ErrVersionConflict if precondition does not
// match the stored policy.
func (a *Admission) DeleteAtespaceAccessPolicy(ctx context.Context, name string, precondition store.DeletePreconditions) (*ateapipb.AccessPolicy, error) {
	return a.store.DeleteAtespaceAccessPolicy(ctx, name, precondition)
}
