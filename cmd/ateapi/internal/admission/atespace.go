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

// CreateAtespace validates and creates a new Atespace.
//
// Returns ErrInvalid if inAtespace fails declarative create validation, or
// store.ErrAlreadyExists if an atespace with the same name already exists.
func (a *Admission) CreateAtespace(ctx context.Context, inAtespace *ateapipb.Atespace) (*ateapipb.Atespace, error) {
	outAtespace := proto.CloneOf(inAtespace)
	defaults.Apply(outAtespace)
	if errs := apivalidation.ValidateAtespaceCreate(ctx, field.NewPath("atespace"), outAtespace); len(errs) > 0 {
		return nil, invalidf("%v", errs.ToAggregate())
	}
	return a.store.CreateAtespace(ctx, outAtespace)
}

// GetAtespace retrieves an Atespace by name.
//
// Returns store.ErrNotFound if the atespace does not exist.
func (a *Admission) GetAtespace(ctx context.Context, name string) (*ateapipb.Atespace, error) {
	return a.store.GetAtespace(ctx, name)
}

// ListAtespaces lists all Atespaces.
//
// Returns store.ErrInvalidPageSize or store.ErrInvalidPageToken if pagination
// options are invalid.
func (a *Admission) ListAtespaces(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapipb.Atespace], error) {
	return a.store.ListAtespaces(ctx, opts)
}

// DeleteAtespace deletes an Atespace by name.
//
// Returns store.ErrNotFound if the atespace does not exist,
// store.ErrUIDConflict or store.ErrVersionConflict if precondition does not
// match the stored atespace, or store.ErrFailedPrecondition if the atespace is
// not empty.
func (a *Admission) DeleteAtespace(ctx context.Context, name string, precondition store.DeletePreconditions) (*ateapipb.Atespace, error) {
	return a.store.DeleteAtespace(ctx, name, precondition)
}
