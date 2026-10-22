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

package apivalidation

import (
	"context"

	"go.ate.dev/substrate/api/ateapi/v1"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/validate"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func ValidateCreateExternalVolumeRequest(ctx context.Context, req *ateapi.CreateExternalVolumeRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_CreateExternalVolumeRequest(ctx, op, nil, req, nil)
}

func ValidateGetExternalVolumeRequest(ctx context.Context, req *ateapi.GetExternalVolumeRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_GetExternalVolumeRequest(ctx, op, nil, req, nil)
}

func ValidateListExternalVolumesRequest(ctx context.Context, req *ateapi.ListExternalVolumesRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_ListExternalVolumesRequest(ctx, op, nil, req, nil)
}

// The request is validated as a create; the result is validated as an update
// once the stored value is known, like UpdateTagRequest.
func ValidateUpdateExternalVolumeRequest(ctx context.Context, req *ateapi.UpdateExternalVolumeRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_UpdateExternalVolumeRequest(ctx, op, nil, req, nil)
}

func ValidateDeleteExternalVolumeRequest(ctx context.Context, req *ateapi.DeleteExternalVolumeRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_DeleteExternalVolumeRequest(ctx, op, nil, req, nil)
}

func ValidateExternalVolumeUpdate(ctx context.Context, fldPath *field.Path, newVal, oldVal *ateapi.ExternalVolume) field.ErrorList {
	op := operation.Operation{Type: operation.Update}
	return Validate_ExternalVolume(ctx, op, fldPath, newVal, oldVal)
}

// This exists only because nested subfield tags are not supported yet.
func ValidateCustom_UpdateExternalVolumeRequest_ExternalVolume(ctx context.Context, op operation.Operation, fldPath *field.Path, volume, _ *ateapi.ExternalVolume) field.ErrorList {
	if volume == nil || volume.Metadata == nil {
		return nil
	}
	errs := Validate_ResourceMetadata(ctx, op, fldPath.Child("metadata"), volume.Metadata, nil)
	errs = append(errs, validate.RequiredValue(ctx, op, fldPath.Child("metadata", "atespace"), &volume.Metadata.Atespace, nil)...)
	return errs
}

// An ExternalVolume names its storage the same way ExternalVolumeStatus does.
func ValidateCustom_ExternalVolume_VolumeType(ctx context.Context, op operation.Operation, fldPath *field.Path, value, oldValue *string) field.ErrorList {
	return ValidateCustom_ExternalVolumeStatus_VolumeType(ctx, op, fldPath, value, oldValue)
}

func ValidateCustom_ExternalVolume_VolumeId(ctx context.Context, op operation.Operation, fldPath *field.Path, value, oldValue *string) field.ErrorList {
	return ValidateCustom_ExternalVolumeStatus_StorageVolumeId(ctx, op, fldPath, value, oldValue)
}
