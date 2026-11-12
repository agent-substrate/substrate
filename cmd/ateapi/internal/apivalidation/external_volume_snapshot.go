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
	"k8s.io/apimachinery/pkg/api/validate"
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
// store. No request carries one, so validation-gen generates no validator for
// it, and this applies the field tags in ateapi.proto by hand. It checks one
// object; the store checks which changes an update may make.
func ValidateExternalVolumeSnapshot(ctx context.Context, snap *ateapi.ExternalVolumeSnapshot) field.ErrorList {
	op := MakeCreateOp(ctx)
	var errs field.ErrorList

	md := snap.GetMetadata()
	mdPath := field.NewPath("metadata")
	if md == nil {
		return field.ErrorList{field.Required(mdPath, "")}
	}
	errs = append(errs, Validate_ResourceMetadata(ctx, op, mdPath, md, nil)...)
	errs = append(errs, validate.RequiredValue(ctx, op, mdPath.Child("atespace"), &md.Atespace, nil)...)

	switch vtPath, vt := field.NewPath("volume_type"), snap.GetVolumeType(); {
	case vt == "":
		errs = append(errs, field.Required(vtPath, ""))
	case len(vt) > 253:
		errs = append(errs, field.TooLong(vtPath, nil, 253))
	default:
		errs = append(errs, resources.ValidateVolumeType(vtPath, vt)...)
	}

	tagPath := field.NewPath("origin", "tag")
	if tag := snap.GetOrigin().GetTag(); tag == nil {
		errs = append(errs, field.Required(tagPath, "exactly one origin must be set"))
	} else {
		errs = append(errs, validateTagVolumeSnapshotOrigin(ctx, tagPath, tag, md.GetAtespace())...)
	}

	st := snap.GetStatus()
	stPath := field.NewPath("status")
	if len(st.GetStorageSnapshotId()) > 256 {
		errs = append(errs, field.TooLong(stPath.Child("storage_snapshot_id"), nil, 256))
	}
	errs = append(errs, resources.ValidateStorageVolumeID(stPath.Child("storage_snapshot_id"), st.GetStorageSnapshotId())...)
	if st.GetSizeBytes() < 0 {
		errs = append(errs, field.Invalid(stPath.Child("size_bytes"), st.GetSizeBytes(), "must be greater than or equal to 0"))
	}
	if len(st.GetErrorMessage()) > 4096 {
		errs = append(errs, field.TooLong(stPath.Child("error_message"), nil, 4096))
	}
	if st.GetErrorMessage() != "" && st.GetStorageSnapshotId() != "" {
		errs = append(errs, field.Invalid(stPath.Child("error_message"), nil, "must not be set together with storage_snapshot_id"))
	}
	if st.GetStorageSnapshotId() == "" {
		if st.GetReadyToUse() {
			errs = append(errs, field.Invalid(stPath.Child("ready_to_use"), true, "must not be set without storage_snapshot_id"))
		}
		if st.GetSizeBytes() > 0 {
			errs = append(errs, field.Invalid(stPath.Child("size_bytes"), st.GetSizeBytes(), "must not be set without storage_snapshot_id"))
		}
	}
	return errs
}

func validateTagVolumeSnapshotOrigin(ctx context.Context, fldPath *field.Path, origin *ateapi.TagVolumeSnapshotOrigin, atespace string) field.ErrorList {
	op := MakeCreateOp(ctx)
	var errs field.ErrorList
	tagPath := fldPath.Child("tag")
	if ref := origin.GetTag(); ref == nil {
		errs = append(errs, field.Required(tagPath, ""))
	} else {
		errs = append(errs, Validate_ObjectRef(ctx, op, tagPath, ref, nil)...)
		if ref.GetAtespace() != atespace {
			errs = append(errs, field.Invalid(tagPath.Child("atespace"), ref.GetAtespace(), "must match metadata.atespace"))
		}
	}
	if uid := origin.GetTagUid(); uid == "" {
		errs = append(errs, field.Required(fldPath.Child("tag_uid"), ""))
	} else {
		errs = append(errs, validate.UUID(ctx, op, fldPath.Child("tag_uid"), &uid, nil)...)
	}
	if name := origin.GetSourceVolumeName(); name == "" {
		errs = append(errs, field.Required(fldPath.Child("source_volume_name"), ""))
	} else {
		errs = append(errs, validate.ShortName(ctx, op, fldPath.Child("source_volume_name"), &name, nil)...)
	}
	switch idPath, id := fldPath.Child("source_volume_id"), origin.GetSourceVolumeId(); {
	case id == "":
		errs = append(errs, field.Required(idPath, ""))
	case len(id) > 256:
		errs = append(errs, field.TooLong(idPath, nil, 256))
	default:
		errs = append(errs, resources.ValidateStorageVolumeID(idPath, id)...)
	}
	return errs
}
