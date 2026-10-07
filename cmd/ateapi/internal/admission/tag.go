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
	"fmt"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/apivalidation"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/defaults"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/validate"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// CreateTag validates the tag's specification, status, and storage location
// and creates the Tag record.
//
// Returns ErrInvalid if inTag fails declarative create validation,
// store.ErrAlreadyExists if the tag name is already taken in its atespace, or
// store.ErrFailedPrecondition if the tag's atespace does not exist.
func (a *Admission) CreateTag(ctx context.Context, inTag *ateapipb.Tag) (*ateapipb.Tag, error) {
	specTag := proto.CloneOf(inTag)
	specTag.Status = nil
	defaults.Apply(specTag)

	fldPath := field.NewPath("tag")
	if errs := apivalidation.ValidateTagCreate(ctx, fldPath, specTag); len(errs) > 0 {
		return nil, invalidf("%v", errs.ToAggregate())
	}

	outTag := proto.CloneOf(specTag)
	outTag.Status = proto.CloneOf(inTag.GetStatus())
	updateErrs := apivalidation.ValidateTagUpdate(ctx, fldPath, outTag, specTag)
	updateErrs = append(updateErrs, validate.RequiredPointer(ctx, operation.Operation{Type: operation.Update}, fldPath.Child("status"), outTag.GetStatus(), nil)...)
	if len(updateErrs) > 0 {
		return nil, fmt.Errorf("%v", updateErrs.ToAggregate())
	}
	if err := resources.ValidateSnapshotLocation(outTag.GetStatus().GetStorageLocation()); err != nil {
		return nil, fmt.Errorf("invalid storage location for tag %s: %w", resources.TagRefFromTag(outTag), err)
	}
	return a.store.CreateTag(ctx, outTag)
}

// GetTag retrieves a Tag by reference.
//
// Returns store.ErrNotFound if the tag does not exist.
func (a *Admission) GetTag(ctx context.Context, tagRef resources.TagRef) (*ateapipb.Tag, error) {
	return a.store.GetTag(ctx, tagRef)
}

// ListTags lists Tags in atespace (or across all atespaces if empty).
//
// Returns store.ErrInvalidPageSize or store.ErrInvalidPageToken if pagination
// options are invalid.
func (a *Admission) ListTags(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.Tag], error) {
	return a.store.ListTags(ctx, atespace, opts)
}

// UpdateTagSpec replaces the user-mutable specification fields of a Tag,
// preserving server-owned metadata and status, rejecting updates while the tag
// is still pending, and enforcing declarative validation.
//
// Returns store.ErrPreconditionRequired if metadata uid or version is unset,
// store.ErrNotFound if the tag does not exist, store.ErrUIDConflict or
// store.ErrVersionConflict if the precondition does not match the stored tag,
// ErrFailedPrecondition if the tag's snapshot copy has not finished yet, or
// ErrInvalid if the updated spec fails declarative validation (including
// immutable field mutations).
func (a *Admission) UpdateTagSpec(ctx context.Context, inTag *ateapipb.Tag) (*ateapipb.Tag, error) {
	tagRef := resources.TagRefFromTag(inTag)
	return a.store.UpdateTag(ctx, tagRef, store.PreconditionFrom(inTag), func(toUpdate *ateapipb.Tag) error {
		// A tag whose create never finished names a partial copy. Publishing it
		// — or changing its scope at all — would hand out content that is still
		// being written, or may never be.
		if toUpdate.GetStatus().GetSnapshot().GetSnapshotUri() == "" {
			return failedPreconditionf("Tag %s/%s is still being created", tagRef.Atespace, tagRef.Name)
		}
		oldVal := proto.CloneOf(toUpdate)

		// Metadata and status are server-owned fields.
		metadata, tagStatus := toUpdate.GetMetadata(), toUpdate.GetStatus()
		// Whole-object replace: clear first, so a field the client left unset is
		// cleared rather than kept from the stored tag. Merge cannot smuggle in
		// unknown fields because validation already rejected them, and a source
		// the client did not echo back is caught by the immutability check on
		// the merged tag: a tag never moves between snapshots, so it never
		// moves between sources either.
		proto.Reset(toUpdate)
		proto.Merge(toUpdate, inTag)
		toUpdate.Metadata, toUpdate.Status = metadata, tagStatus
		defaults.Apply(toUpdate)

		// Validate the merged tag against the one it replaces. This is where
		// the rules the request could not be checked against land: scope, and
		// the immutability of metadata and source_actor.
		if errs := apivalidation.ValidateTagUpdate(ctx, field.NewPath("tag"), toUpdate, oldVal); len(errs) > 0 {
			return invalidf("%v", errs.ToAggregate())
		}
		return nil
	})
}

// UpdateTagStatus updates the server-owned status of a Tag and validates the
// resulting resource before storing it.
//
// Returns store.ErrPreconditionRequired if precondition omits uid or version,
// store.ErrNotFound if the tag does not exist, store.ErrUIDConflict or
// store.ErrVersionConflict if precondition does not match the stored tag,
// store.ErrImmutableField if status.snapshot is modified after being set, or
// the error returned by mutate verbatim.
func (a *Admission) UpdateTagStatus(ctx context.Context, tagRef resources.TagRef, precondition store.Precondition, mutate func(*ateapipb.TagStatus) error) (*ateapipb.Tag, error) {
	return a.store.UpdateTag(ctx, tagRef, precondition, func(toUpdate *ateapipb.Tag) error {
		oldVal := proto.CloneOf(toUpdate)
		if err := mutate(toUpdate.Status); err != nil {
			return err
		}
		if errs := apivalidation.ValidateTagUpdate(ctx, field.NewPath("tag"), toUpdate, oldVal); len(errs) > 0 {
			return fmt.Errorf("%v", errs.ToAggregate())
		}
		return nil
	})
}

// DeleteTag deletes a Tag by reference.
//
// Returns store.ErrNotFound if the tag does not exist, or
// store.ErrUIDConflict or store.ErrVersionConflict if precondition does not
// match the stored tag.
func (a *Admission) DeleteTag(ctx context.Context, tagRef resources.TagRef, precondition store.DeletePreconditions) (*ateapipb.Tag, error) {
	return a.store.DeleteTag(ctx, tagRef, precondition)
}
