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

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// ValidateRequestActorSuspendRequest runs the generated validation for req.
func ValidateRequestActorSuspendRequest(ctx context.Context, req *ateletpb.RequestActorSuspendRequest) field.ErrorList {
	return Validate_RequestActorSuspendRequest(ctx, operation.Operation{Type: operation.Create}, nil, req, nil)
}

// ValidateMintActorCertificateRequest runs the generated validation for req.
func ValidateMintActorCertificateRequest(ctx context.Context, req *ateletpb.MintActorCertificateRequest) field.ErrorList {
	return Validate_MintActorCertificateRequest(ctx, operation.Operation{Type: operation.Create}, nil, req, nil)
}

// ValidateRunRequest runs the generated validation for req.
func ValidateRunRequest(ctx context.Context, req *ateletpb.RunRequest) field.ErrorList {
	return Validate_RunRequest(ctx, operation.Operation{Type: operation.Create}, nil, req, nil)
}

// ValidateTerminateRequest runs the generated validation for req.
func ValidateTerminateRequest(ctx context.Context, req *ateletpb.TerminateRequest) field.ErrorList {
	return Validate_TerminateRequest(ctx, operation.Operation{Type: operation.Create}, nil, req, nil)
}

// ValidateCheckpointRequest runs the generated validation for req.
func ValidateCheckpointRequest(ctx context.Context, req *ateletpb.CheckpointRequest) field.ErrorList {
	return Validate_CheckpointRequest(ctx, operation.Operation{Type: operation.Create}, nil, req, nil)
}

// ValidateRestoreRequest runs the generated validation for req.
func ValidateRestoreRequest(ctx context.Context, req *ateletpb.RestoreRequest) field.ErrorList {
	return Validate_RestoreRequest(ctx, operation.Operation{Type: operation.Create}, nil, req, nil)
}

// ValidateUploadPausedCheckpointRequest runs the generated validation for req.
func ValidateUploadPausedCheckpointRequest(ctx context.Context, req *ateletpb.UploadPausedCheckpointRequest) field.ErrorList {
	return Validate_UploadPausedCheckpointRequest(ctx, operation.Operation{Type: operation.Create}, nil, req, nil)
}

// ValidateRegisterWorkerRequest runs the generated validation for req.
func ValidateRegisterWorkerRequest(ctx context.Context, req *ateletpb.RegisterWorkerRequest) field.ErrorList {
	return Validate_RegisterWorkerRequest(ctx, operation.Operation{Type: operation.Create}, nil, req, nil)
}

// ValidateCustom_Limits validates one limit with resources.ValidateLimit, the
// rule the control plane applies to its own Limits. Presence and uniqueness
// of names are enforced by tags.
func ValidateCustom_Limits(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *ateletpb.Limits) field.ErrorList {
	return resources.ValidateLimit(fldPath, value.GetName(), value.GetQuantity())
}

func ValidateCustom_EgressGateway_Address(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateHostPort(fldPath, *value)
}

// ValidateCustom_SandboxAssets_SandboxClass allows the sandbox classes of the
// SandboxConfig CRD.
func ValidateCustom_SandboxAssets_SandboxClass(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	switch atev1alpha1.SandboxClass(*value) {
	case atev1alpha1.SandboxClassGvisor, atev1alpha1.SandboxClassMicroVM:
		return nil
	}
	return field.ErrorList{field.NotSupported(fldPath, *value, []string{
		string(atev1alpha1.SandboxClassGvisor), string(atev1alpha1.SandboxClassMicroVM),
	})}
}

func ValidateCustom_SandboxAssets_PauseImage(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidatePinnedImage(fldPath, *value)
}

func ValidateCustom_AssetFile_Sha256(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	if err := resources.ValidateRunscHash(*value); err != nil {
		return field.ErrorList{field.Invalid(fldPath, *value, err.Error())}
	}
	return nil
}

// ValidateCustom_UploadPausedCheckpointRequest_Atespace rejects the golden
// atespace: golden actors are never paused, so a paused checkpoint is never
// promoted to a golden snapshot.
func ValidateCustom_UploadPausedCheckpointRequest_Atespace(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	if *value == resources.GoldenActorAtespace {
		return field.ErrorList{field.Forbidden(fldPath, fmt.Sprintf("atespace %q holds golden actors, which are never paused", *value))}
	}
	return nil
}

// ValidateCustom_CheckpointRequest requires the config that matches type.
func ValidateCustom_CheckpointRequest(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *ateletpb.CheckpointRequest) field.ErrorList {
	return validateConfigMatchesType(fldPath, value.GetType(), value.GetLocalConfig() != nil, value.GetExternalConfig() != nil)
}

// ValidateCustom_RestoreRequest requires the config that matches type.
func ValidateCustom_RestoreRequest(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *ateletpb.RestoreRequest) field.ErrorList {
	return validateConfigMatchesType(fldPath, value.GetType(), value.GetLocalConfig() != nil, value.GetExternalConfig() != nil)
}

// validateConfigMatchesType requires the config that matches a checkpoint
// type. The union tags already require exactly one config.
func validateConfigMatchesType(fldPath *field.Path, typ ateletpb.CheckpointType, hasLocal, hasExternal bool) field.ErrorList {
	switch {
	case typ == ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL && !hasLocal:
		return field.ErrorList{field.Required(fldPath.Child("local_config"), "required when type is CHECKPOINT_TYPE_LOCAL")}
	case typ == ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL && !hasExternal:
		return field.ErrorList{field.Required(fldPath.Child("external_config"), "required when type is CHECKPOINT_TYPE_EXTERNAL")}
	}
	return nil
}

func ValidateCustom_ExternalCheckpointConfiguration_SnapshotUri(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return validateSnapshotURI(fldPath, *value)
}

func ValidateCustom_ExternalRestoreConfiguration_SnapshotUri(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return validateSnapshotURI(fldPath, *value)
}

func ValidateCustom_UploadPausedCheckpointRequest_DestinationSnapshotUri(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return validateSnapshotURI(fldPath, *value)
}

// validateSnapshotURI requires a snapshot URI that resources.ParseSnapshotURI
// accepts.
func validateSnapshotURI(fldPath *field.Path, uri string) field.ErrorList {
	if _, err := resources.ParseSnapshotURI(uri); err != nil {
		return field.ErrorList{field.Invalid(fldPath, uri, err.Error())}
	}
	return nil
}

// ateDeepEqual is the deep-equal function declarative validation's generated
// code calls by name; it delegates to resources.DeepEqual.
func ateDeepEqual[T any](a, b T) bool {
	return resources.DeepEqual(a, b)
}
