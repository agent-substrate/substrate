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
	"go.ate.dev/substrate/internal/resources"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func ValidateCreateActorTemplateRequest(ctx context.Context, req *ateapi.CreateActorTemplateRequest) field.ErrorList {
	// Call the generated validation.
	op := MakeCreateOp(ctx)
	return Validate_CreateActorTemplateRequest(ctx, op, nil, req, nil)
}

func ValidateGetActorTemplateRequest(ctx context.Context, req *ateapi.GetActorTemplateRequest) field.ErrorList {
	// Call the generated validation.
	op := MakeCreateOp(ctx)
	return Validate_GetActorTemplateRequest(ctx, op, nil, req, nil)
}

func ValidateListActorTemplatesRequest(ctx context.Context, req *ateapi.ListActorTemplatesRequest) field.ErrorList {
	// Call the generated validation.
	op := MakeCreateOp(ctx)
	return Validate_ListActorTemplatesRequest(ctx, op, nil, req, nil)
}

func ValidateDeleteActorTemplateRequest(ctx context.Context, req *ateapi.DeleteActorTemplateRequest) field.ErrorList {
	// Call the generated validation.
	op := MakeCreateOp(ctx)
	return Validate_DeleteActorTemplateRequest(ctx, op, nil, req, nil)
}

func ValidateActorTemplateUpdate(ctx context.Context, fldPath *field.Path, newVal, oldVal *ateapi.ActorTemplate) field.ErrorList {
	// Call the generated validation.
	op := MakeUpdateOp(ctx)
	return Validate_ActorTemplate(ctx, op, fldPath, newVal, oldVal)
}

// ValidateCustom_CreateActorTemplateRequest_ActorTemplate holds the rules
// that span more than one field of a template.
func ValidateCustom_CreateActorTemplateRequest_ActorTemplate(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *ateapi.ActorTemplate) field.ErrorList {
	var errs field.ErrorList
	if value.GetGoldenEgressPolicy() != nil && value.GetSnapshotConfig().GetGoldenSnapshotConfig().GetMode() == ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED {
		errs = append(errs, field.Forbidden(fldPath.Child("golden_egress_policy"), "requires golden snapshots to be enabled"))
	}
	declared := make(map[string]bool, len(value.GetVolumes()))
	for _, vol := range value.GetVolumes() {
		declared[vol.GetName()] = true
	}
	for i, ctr := range value.GetContainers() {
		for j, mount := range ctr.GetVolumeMounts() {
			name := mount.GetName()
			if name == "" {
				continue // required is enforced by tags
			}
			if !declared[name] {
				errs = append(errs, field.Invalid(
					fldPath.Child("containers").Index(i).Child("volume_mounts").Index(j).Child("name"),
					name, "must reference a volume declared in the template"))
			}
		}
	}
	return errs
}

// ValidateCustom_ActorTemplate_Resources requires both a cpu and a memory
// limit, which the scheduler books against worker capacity.
func ValidateCustom_ActorTemplate_Resources(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *ateapi.ResourceRequirements) field.ErrorList {
	var errs field.ErrorList
	if value.GetLimits().GetCpuMillicores() == 0 {
		errs = append(errs, field.Required(fldPath.Child("limits", "cpu_millicores"), ""))
	}
	if value.GetLimits().GetMemoryBytes() == 0 {
		errs = append(errs, field.Required(fldPath.Child("limits", "memory_bytes"), ""))
	}
	return errs
}

func ValidateCustom_HttpGetAction_Path(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateHTTPGetPath(fldPath, *value)
}

func ValidateCustom_VolumeMount_MountPath(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateMountPath(fldPath, *value)
}

// ValidateCustom_Container_VolumeMounts rejects mounts that nest under one
// another. Mount-path uniqueness is enforced by the list key.
func ValidateCustom_Container_VolumeMounts(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []*ateapi.VolumeMount) field.ErrorList {
	paths := make([]string, len(value))
	for i, m := range value {
		paths[i] = m.GetMountPath()
	}
	return resources.ValidateNestedMountPaths(fldPath, paths)
}

func ValidateCustom_ActorTemplate_ActorLabels(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ map[string]string) field.ErrorList {
	return validateLabels(fldPath, value)
}

func ValidateCustom_ActorMetadataItem_Path(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateProjectedPath(fldPath, *value)
}

func ValidateCustom_TrustBundleDataSource_Path(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateProjectedPath(fldPath, *value)
}

// ValidateCustom_SystemInfoVolumeSource_DataSources allows at most one
// actor_metadata entry and requires every projected file path to be unique
// across all data sources: atelet writes them in order into one tree, so a
// repeated path silently clobbers the earlier file.
func ValidateCustom_SystemInfoVolumeSource_DataSources(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []*ateapi.SystemInfoDataSource) field.ErrorList {
	var errs field.ErrorList
	seen := sets.New[string]()
	sawMetadata := false
	for i, ds := range value {
		switch {
		case ds == nil:
		case ds.TrustBundle != nil:
			if seen.Has(ds.TrustBundle.Path) {
				errs = append(errs, field.Duplicate(fldPath.Index(i).Child("trust_bundle", "path"), ds.TrustBundle.Path))
			}
			seen.Insert(ds.TrustBundle.Path)
		case ds.ActorMetadata != nil:
			if sawMetadata {
				errs = append(errs, field.Forbidden(fldPath.Index(i).Child("actor_metadata"), "at most one actor_metadata entry may appear"))
			}
			sawMetadata = true
			for j, item := range ds.ActorMetadata.Items {
				if item == nil {
					continue
				}
				if seen.Has(item.Path) {
					errs = append(errs, field.Duplicate(fldPath.Index(i).Child("actor_metadata", "items").Index(j).Child("path"), item.Path))
				}
				seen.Insert(item.Path)
			}
		}
	}
	return errs
}

func ValidateCustom_ImageVolumeSource_Reference(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidatePinnedImage(fldPath, *value)
}

func ValidateCustom_Container_Image(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidatePinnedImage(fldPath, *value)
}

// ValidateCustom_SnapshotConfig_StorageLocation ensures an
// ActorTemplate's snapshotConfig.storageLocation is a well-formed
// URI with a bucket, so a bad location fails fast.
func ValidateCustom_SnapshotConfig_StorageLocation(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	if err := resources.ValidateSnapshotLocation(*value); err != nil {
		return field.ErrorList{field.Invalid(fldPath, *value, err.Error())}
	}
	return nil
}

func ValidateCustom_EnvVar_Name(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateEnvVarName(fldPath, *value)
}

func ValidateCustom_Capabilities_Add(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []string) field.ErrorList {
	return resources.ValidateCapabilities(fldPath, value, false)
}

func ValidateCustom_Capabilities_Drop(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []string) field.ErrorList {
	return resources.ValidateCapabilities(fldPath, value, true)
}
