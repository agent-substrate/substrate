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

// Custom validations for the WorkloadSpec tree. The field rules are shared
// with the control plane through internal/resources: values arriving here
// already passed them at template creation, so a failure is an internal
// inconsistency, not a user error.

package apivalidation

import (
	"context"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// ValidateCustom_Container_Name rejects the container name reserved for the
// sandbox-infra bundle, which shares the OCI bundle directory namespace and
// races its concurrent writer.
func ValidateCustom_Container_Name(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	if *value == "pause" {
		return field.ErrorList{field.Invalid(fldPath, *value, `"pause" is reserved for sandbox infrastructure`)}
	}
	return nil
}

func ValidateCustom_Container_Image(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidatePinnedImage(fldPath, *value)
}

func ValidateCustom_EnvEntry_Name(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateEnvVarName(fldPath, *value)
}

func ValidateCustom_VolumeMount_MountPath(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateMountPath(fldPath, *value)
}

// ValidateCustom_Container_VolumeMounts rejects nested mounts. Mount-path
// uniqueness is enforced by the list key.
func ValidateCustom_Container_VolumeMounts(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []*ateletpb.VolumeMount) field.ErrorList {
	paths := make([]string, len(value))
	for i, m := range value {
		paths[i] = m.GetMountPath()
	}
	return resources.ValidateNestedMountPaths(fldPath, paths)
}

func ValidateCustom_Capabilities_Add(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []string) field.ErrorList {
	return resources.ValidateCapabilities(fldPath, value, false)
}

func ValidateCustom_Capabilities_Drop(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []string) field.ErrorList {
	return resources.ValidateCapabilities(fldPath, value, true)
}

func ValidateCustom_HTTPGetAction_Path(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateHTTPGetPath(fldPath, *value)
}

func ValidateCustom_ExternalVolumeSource_StorageVolumeId(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateStorageVolumeID(fldPath, *value)
}

func ValidateCustom_ExternalVolumeSource_VolumeType(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateVolumeType(fldPath, *value)
}

func ValidateCustom_ImageVolumeSource_Reference(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidatePinnedImage(fldPath, *value)
}
