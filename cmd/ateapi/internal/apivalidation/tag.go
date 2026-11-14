// Copyright 2026 The Agent Substrate Authors
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

package apivalidation

import (
	"context"

	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/validate"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func ValidateCreateTagRequest(ctx context.Context, req *ateapi.CreateTagRequest) field.ErrorList {
	op := MakeCreateOp(ctx)
	return Validate_CreateTagRequest(ctx, op, nil, req, nil)
}

func ValidateGetTagRequest(ctx context.Context, req *ateapi.GetTagRequest) field.ErrorList {
	op := MakeCreateOp(ctx)
	return Validate_GetTagRequest(ctx, op, nil, req, nil)
}

func ValidateListTagsRequest(ctx context.Context, req *ateapi.ListTagsRequest) field.ErrorList {
	op := MakeCreateOp(ctx)
	return Validate_ListTagsRequest(ctx, op, nil, req, nil)
}

func ValidateUpdateTagRequest(ctx context.Context, req *ateapi.UpdateTagRequest) field.ErrorList {
	// We model this as a create rather than an update because updates assume
	// the existence of a "current" value, which we do not have yet.  This is
	// validating the request itself. The result will be validated later, after
	// we have a current value to compare against.
	op := MakeCreateOp(ctx)
	return Validate_UpdateTagRequest(ctx, op, nil, req, nil)
}

func ValidateDeleteTagRequest(ctx context.Context, req *ateapi.DeleteTagRequest) field.ErrorList {
	op := MakeCreateOp(ctx)
	return Validate_DeleteTagRequest(ctx, op, nil, req, nil)
}

func ValidateTagUpdate(ctx context.Context, fldPath *field.Path, newVal, oldVal *ateapi.Tag) field.ErrorList {
	op := MakeUpdateOp(ctx)
	return Validate_Tag(ctx, op, fldPath, newVal, oldVal)
}

func ValidateCustom_CreateTagRequest(_ context.Context, _ operation.Operation, p *field.Path, req, _ *ateapi.CreateTagRequest) field.ErrorList {
	tag := req.GetTag()
	sourceActorAtespace := tag.GetSourceActor().GetAtespace()
	tagAtespace := tag.GetMetadata().GetAtespace()
	if sourceActorAtespace == "" || tagAtespace == "" {
		return nil // regular DV will handle it
	}
	if tagAtespace != sourceActorAtespace {
		return field.ErrorList{
			field.Invalid(p.Child("tag", "metadata", "atespace"), tagAtespace, "must match source_actor.atespace"),
		}
	}
	return nil
}

// This exists only because nested subfield tags are not supported yet.
func ValidateCustom_UpdateTagRequest_Tag(ctx context.Context, op operation.Operation, fldPath *field.Path, tag, _ *ateapi.Tag) field.ErrorList {
	if tag == nil || tag.Metadata == nil {
		return nil // handled by DV
	}

	// Updates are validated in 2 steps: first the update request and then the
	// resource itself. DV for the request doesn't descend into the resource
	// metadata.  Once DV supports nested subfield tags, this can be changed to
	// something like:
	//   +k8s:subfield(metadata)=+k8s:subfield(atespace)=+k8s:required
	errs := Validate_ResourceMetadata(ctx, op, fldPath.Child("metadata"), tag.Metadata, nil)
	errs = append(errs, validate.RequiredValue(ctx, op, fldPath.Child("metadata", "atespace"), &tag.Metadata.Atespace, nil)...)
	return errs
}

// ValidateCustom_TagStatus_Snapshot allows an update to record status.snapshot
// once, and then only to move its durable_snapshot from PENDING to COMPLETED. A
// tag never moves between snapshots.
func ValidateCustom_TagStatus_Snapshot(_ context.Context, op operation.Operation, fldPath *field.Path, value, oldValue *ateapi.Snapshot) field.ErrorList {
	if op.Type != operation.Update || oldValue == nil || proto.Equal(value, oldValue) {
		return nil
	}
	if oldValue.GetDurableSnapshot().GetState() == ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING &&
		value.GetDurableSnapshot().GetState() == ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED {
		completed := proto.CloneOf(oldValue)
		completed.DurableSnapshot.State = ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED
		if proto.Equal(completed, value) {
			return nil
		}
	}
	return field.ErrorList{field.Forbidden(fldPath, "may only change durable_snapshot.state from PENDING to COMPLETED")}
}
