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
	"fmt"

	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/api/validate"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func ValidateCreateSharedVolumeRequest(ctx context.Context, req *ateapipb.CreateSharedVolumeRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_CreateSharedVolumeRequest(ctx, op, nil, req, nil)
}

func ValidateGetSharedVolumeRequest(ctx context.Context, req *ateapipb.GetSharedVolumeRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_GetSharedVolumeRequest(ctx, op, nil, req, nil)
}

func ValidateListSharedVolumesRequest(ctx context.Context, req *ateapipb.ListSharedVolumesRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_ListSharedVolumesRequest(ctx, op, nil, req, nil)
}

// The request is validated as a create; the result is validated as an update
// once the stored value is known, like UpdateTagRequest.
func ValidateUpdateSharedVolumeRequest(ctx context.Context, req *ateapipb.UpdateSharedVolumeRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_UpdateSharedVolumeRequest(ctx, op, nil, req, nil)
}

func ValidateDeleteSharedVolumeRequest(ctx context.Context, req *ateapipb.DeleteSharedVolumeRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_DeleteSharedVolumeRequest(ctx, op, nil, req, nil)
}

func ValidateSharedVolumeUpdate(ctx context.Context, fldPath *field.Path, newVal, oldVal *ateapipb.SharedVolume) field.ErrorList {
	op := operation.Operation{Type: operation.Update}
	return Validate_SharedVolume(ctx, op, fldPath, newVal, oldVal)
}

func ValidateCustom_CreateSharedVolumeRequest(_ context.Context, _ operation.Operation, p *field.Path, req, _ *ateapipb.CreateSharedVolumeRequest) field.ErrorList {
	volume := req.GetSharedVolume()
	if volume == nil {
		return nil
	}
	volumePath := p.Child("shared_volume")
	storageClassName, volumeID := volume.GetStorageClassName(), volume.GetVolumeId()

	var errs field.ErrorList
	// TODO: accept READ_WRITE_MANY once concurrent holders are supported.
	if mode := volume.GetAccessMode(); mode != ateapipb.AccessMode_ACCESS_MODE_UNSPECIFIED && mode != ateapipb.AccessMode_ACCESS_MODE_READ_WRITE_ONCE {
		errs = append(errs, field.NotSupported(volumePath.Child("access_mode"), mode.String(), []string{ateapipb.AccessMode_ACCESS_MODE_READ_WRITE_ONCE.String()}))
	}
	switch {
	case storageClassName == "" && volumeID == "":
		errs = append(errs, field.Required(volumePath.Child("storage_class_name"), "one of storage_class_name or volume_id is required"))
	case volumeID != "":
		if volume.GetCapacity() != "" {
			errs = append(errs, field.Forbidden(volumePath.Child("capacity"), "must not be set with volume_id"))
		}
		if volume.GetVolumeType() == "" && storageClassName == "" {
			errs = append(errs, field.Required(volumePath.Child("volume_type"), "required with volume_id unless storage_class_name is set"))
		}
	default:
		if volume.GetVolumeType() != "" {
			errs = append(errs, field.Forbidden(volumePath.Child("volume_type"), "must not be set with storage_class_name; it is taken from the class's provisioner"))
		}
		if len(volume.GetVolumeContext()) > 0 {
			errs = append(errs, field.Forbidden(volumePath.Child("volume_context"), "must not be set with storage_class_name; it is returned by the storage system"))
		}
		if volume.GetCapacity() == "" {
			errs = append(errs, field.Required(volumePath.Child("capacity"), "required with storage_class_name"))
		}
	}
	return errs
}

func ValidateCustom_SharedVolume_Capacity(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	if value == nil || *value == "" {
		return nil
	}
	if _, err := resource.ParseQuantity(*value); err != nil {
		return field.ErrorList{field.Invalid(fldPath, *value, fmt.Sprintf("must be a Kubernetes resource quantity: %v", err))}
	}
	return nil
}

// This exists only because nested subfield tags are not supported yet.
func ValidateCustom_UpdateSharedVolumeRequest_SharedVolume(ctx context.Context, op operation.Operation, fldPath *field.Path, volume, _ *ateapipb.SharedVolume) field.ErrorList {
	if volume == nil || volume.Metadata == nil {
		return nil
	}
	errs := Validate_ResourceMetadata(ctx, op, fldPath.Child("metadata"), volume.Metadata, nil)
	errs = append(errs, validate.RequiredValue(ctx, op, fldPath.Child("metadata", "atespace"), &volume.Metadata.Atespace, nil)...)
	return errs
}

// An SharedVolume names its storage the same way an ExternalVolume does.
func ValidateCustom_SharedVolume_VolumeType(ctx context.Context, op operation.Operation, fldPath *field.Path, value, oldValue *string) field.ErrorList {
	return ValidateCustom_ExternalVolume_VolumeType(ctx, op, fldPath, value, oldValue)
}

func ValidateCustom_SharedVolume_VolumeId(ctx context.Context, op operation.Operation, fldPath *field.Path, value, oldValue *string) field.ErrorList {
	return ValidateCustom_ExternalVolume_StorageVolumeId(ctx, op, fldPath, value, oldValue)
}
