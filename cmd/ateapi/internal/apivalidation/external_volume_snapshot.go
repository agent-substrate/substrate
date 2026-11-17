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
	"go.ate.dev/substrate/internal/resources"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func ValidateGetExternalVolumeSnapshotRequest(ctx context.Context, req *ateapi.GetExternalVolumeSnapshotRequest) field.ErrorList {
	op := MakeCreateOp(ctx)
	return Validate_GetExternalVolumeSnapshotRequest(ctx, op, nil, req, nil)
}

func ValidateListExternalVolumeSnapshotsRequest(ctx context.Context, req *ateapi.ListExternalVolumeSnapshotsRequest) field.ErrorList {
	op := MakeCreateOp(ctx)
	return Validate_ListExternalVolumeSnapshotsRequest(ctx, op, nil, req, nil)
}

// ValidateExternalVolumeSnapshot checks a snapshot the server is about to
// create.
func ValidateExternalVolumeSnapshot(ctx context.Context, snap *ateapi.ExternalVolumeSnapshot) field.ErrorList {
	op := MakeCreateOp(ctx)
	return Validate_ExternalVolumeSnapshot(ctx, op, nil, snap, nil)
}

// ValidateExternalVolumeSnapshotUpdate checks newVal as an update of oldVal.
// Values that did not change are not re-validated.
func ValidateExternalVolumeSnapshotUpdate(ctx context.Context, newVal, oldVal *ateapi.ExternalVolumeSnapshot) field.ErrorList {
	op := MakeUpdateOp(ctx)
	return Validate_ExternalVolumeSnapshot(ctx, op, nil, newVal, oldVal)
}

// ValidateCustom_ExternalVolumeSnapshot requires the owning tag to be in the
// snapshot's atespace. Nested subfield tags are not supported yet.
func ValidateCustom_ExternalVolumeSnapshot(_ context.Context, op operation.Operation, fldPath *field.Path, snap, oldSnap *ateapi.ExternalVolumeSnapshot) field.ErrorList {
	atespace := snap.GetMetadata().GetAtespace()
	tagAtespace := snap.GetOrigin().GetTagCapture().GetTag().GetAtespace()
	if atespace == "" || tagAtespace == "" {
		return nil // handled by DV
	}
	if op.Type == operation.Update && atespace == oldSnap.GetMetadata().GetAtespace() &&
		tagAtespace == oldSnap.GetOrigin().GetTagCapture().GetTag().GetAtespace() {
		return nil
	}
	if tagAtespace != atespace {
		return field.ErrorList{field.Invalid(fldPath.Child("origin", "tag_capture", "tag", "atespace"), tagAtespace, "must match metadata.atespace")}
	}
	return nil
}

func ValidateCustom_ExternalVolumeSnapshot_DriverName(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateVolumeType(fldPath, *value)
}

func ValidateCustom_TagVolumeSnapshotOrigin_SourceVolumeId(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateStorageVolumeID(fldPath, *value)
}

func ValidateCustom_ExternalVolumeSnapshotStatus_SnapshotId(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateStorageVolumeID(fldPath, *value)
}
