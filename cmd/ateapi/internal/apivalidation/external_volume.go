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
	"maps"
	"slices"
	"strings"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/resources"
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

// A caller types volume_id in, where a driver reports
// ExternalVolumeStatus.storage_volume_id, so whitespace a shell or editor
// adds is rejected too.
func ValidateCustom_ExternalVolume_VolumeId(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	if errs := resources.ValidateStorageVolumeID(fldPath, *value); len(errs) > 0 {
		return errs
	}
	if strings.ContainsAny(*value, "\t\n\r") {
		return field.ErrorList{field.Invalid(fldPath, *value, "must not contain tabs or line breaks")}
	}
	if strings.TrimSpace(*value) != *value {
		return field.ErrorList{field.Invalid(fldPath, *value, "must not start or end with whitespace")}
	}
	return nil
}

func ValidateCustom_ExternalVolume_DriverName(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateVolumeType(fldPath, *value)
}

// A caller types volume_context in, and node plugins pass its entries to
// mount(2) as C strings, so a NUL byte would truncate an option.
func ValidateCustom_ExternalVolume_VolumeContext(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ map[string]string) field.ErrorList {
	for _, k := range slices.Sorted(maps.Keys(value)) {
		if strings.ContainsRune(k, 0) {
			return field.ErrorList{field.Invalid(fldPath, k, "keys must not contain NUL bytes")}
		}
		if strings.ContainsRune(value[k], 0) {
			return field.ErrorList{field.Invalid(fldPath.Key(k), value[k], "must not contain NUL bytes")}
		}
	}
	return resources.ValidateCSIMap(fldPath, value)
}
