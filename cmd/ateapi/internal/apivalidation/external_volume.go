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
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func ValidateCreateExternalVolumeRequest(ctx context.Context, req *ateapi.CreateExternalVolumeRequest) field.ErrorList {
	op := MakeCreateOp(ctx)
	return Validate_CreateExternalVolumeRequest(ctx, op, nil, req, nil)
}

func ValidateGetExternalVolumeRequest(ctx context.Context, req *ateapi.GetExternalVolumeRequest) field.ErrorList {
	op := MakeCreateOp(ctx)
	return Validate_GetExternalVolumeRequest(ctx, op, nil, req, nil)
}

func ValidateListExternalVolumesRequest(ctx context.Context, req *ateapi.ListExternalVolumesRequest) field.ErrorList {
	op := MakeCreateOp(ctx)
	return Validate_ListExternalVolumesRequest(ctx, op, nil, req, nil)
}

func ValidateDeleteExternalVolumeRequest(ctx context.Context, req *ateapi.DeleteExternalVolumeRequest) field.ErrorList {
	op := MakeCreateOp(ctx)
	return Validate_DeleteExternalVolumeRequest(ctx, op, nil, req, nil)
}

// An ExternalVolume names its storage the same way ExternalVolumeStatus does.
func ValidateCustom_ExternalVolume_VolumeId(ctx context.Context, op operation.Operation, fldPath *field.Path, value, oldValue *string) field.ErrorList {
	return ValidateCustom_ExternalVolumeStatus_StorageVolumeId(ctx, op, fldPath, value, oldValue)
}

func ValidateCustom_ExternalVolume_VolumeType(ctx context.Context, op operation.Operation, fldPath *field.Path, value, oldValue *string) field.ErrorList {
	return ValidateCustom_ExternalVolumeStatus_VolumeType(ctx, op, fldPath, value, oldValue)
}

func ValidateCustom_ExternalVolume_VolumeContext(ctx context.Context, op operation.Operation, fldPath *field.Path, value, oldValue map[string]string) field.ErrorList {
	return ValidateCustom_ExternalVolumeStatus_VolumeContext(ctx, op, fldPath, value, oldValue)
}
