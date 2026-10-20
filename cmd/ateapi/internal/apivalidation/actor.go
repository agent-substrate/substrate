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
	"k8s.io/apimachinery/pkg/api/validate"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func ValidateCreateActorRequest(ctx context.Context, req *ateapi.CreateActorRequest) field.ErrorList {
	// Call the generated validation.
	op := MakeCreateOp(ctx)
	return Validate_CreateActorRequest(ctx, op, nil, req, nil)
}

func ValidateGetActorRequest(ctx context.Context, req *ateapi.GetActorRequest) field.ErrorList {
	// Call the generated validation.
	op := MakeCreateOp(ctx)
	return Validate_GetActorRequest(ctx, op, nil, req, nil)
}

func ValidateListActorsRequest(ctx context.Context, req *ateapi.ListActorsRequest) field.ErrorList {
	// Call the generated validation.
	op := MakeCreateOp(ctx)
	return Validate_ListActorsRequest(ctx, op, nil, req, nil)
}

func ValidateUpdateActorRequest(ctx context.Context, req *ateapi.UpdateActorRequest) field.ErrorList {
	// Call the generated validation.
	// We model this as a create rather than an update because updates assume
	// the existence of a "current" value, which we do not have yet.  This is
	// validating the request itself. The result will be validated later, after
	// we have a current value to compare against.
	op := MakeCreateOp(ctx)
	return Validate_UpdateActorRequest(ctx, op, nil, req, nil)
}

func ValidateDeleteActorRequest(ctx context.Context, req *ateapi.DeleteActorRequest) field.ErrorList {
	// Call the generated validation.
	op := MakeCreateOp(ctx)
	return Validate_DeleteActorRequest(ctx, op, nil, req, nil)
}

func ValidateResumeActorRequest(ctx context.Context, req *ateapi.ResumeActorRequest) field.ErrorList {
	// Call the generated validation.
	op := MakeCreateOp(ctx)
	return Validate_ResumeActorRequest(ctx, op, nil, req, nil)
}

func ValidateSuspendActorRequest(ctx context.Context, req *ateapi.SuspendActorRequest) field.ErrorList {
	// Call the generated validation.
	op := MakeCreateOp(ctx)
	return Validate_SuspendActorRequest(ctx, op, nil, req, nil)
}

func ValidateRevertActorRequest(ctx context.Context, req *ateapi.RevertActorRequest) field.ErrorList {
	// Call the generated validation.
	op := MakeCreateOp(ctx)
	return Validate_RevertActorRequest(ctx, op, nil, req, nil)
}

func ValidateMintActorJWTRequest(ctx context.Context, req *ateapi.MintActorJwtRequest) field.ErrorList {
	// Call the generated validation.
	op := MakeCreateOp(ctx)
	return Validate_MintActorJwtRequest(ctx, op, nil, req, nil)
}

func ValidateMintActorCertificateRequest(ctx context.Context, req *ateapi.MintActorCertificateRequest) field.ErrorList {
	// Call the generated validation.
	op := MakeCreateOp(ctx)
	return Validate_MintActorCertificateRequest(ctx, op, nil, req, nil)
}

func ValidateMintWorkerActorCertificateRequest(ctx context.Context, req *ateapi.MintWorkerActorCertificateRequest) field.ErrorList {
	op := MakeCreateOp(ctx)
	return Validate_MintWorkerActorCertificateRequest(ctx, op, nil, req, nil)
}

func ValidateMintWorkerActorJwtRequest(ctx context.Context, req *ateapi.MintWorkerActorJwtRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_MintWorkerActorJwtRequest(ctx, op, nil, req, nil)
}

func ValidateActorUpdate(ctx context.Context, fldPath *field.Path, newVal, oldVal *ateapi.Actor, requireStatus bool) field.ErrorList {
	// Call the generated validation.
	op := MakeUpdateOp(ctx)
	errs := Validate_Actor(ctx, op, fldPath, newVal, oldVal)
	if requireStatus {
		// Status is optional in the schema, but is actually required to be set
		// by the server.  If it was specified, it was already validated above,
		// but if it was not specified we need to flag that as an error.
		errs = append(errs, validate.RequiredPointer(ctx, op, fldPath.Child("status"), newVal.GetStatus(), nil)...)
	}
	return errs
}

// This exists only because nested subfield tags are not supported yet.
func ValidateCustom_UpdateActorRequest_Actor(ctx context.Context, op operation.Operation, fldPath *field.Path, actor, _ *ateapi.Actor) field.ErrorList {
	if actor == nil || actor.Metadata == nil {
		return nil // handled by DV
	}

	// Updates are validated in 2 steps: first the update request and then the
	// resource itself. DV for the request doesn't descend into the resource
	// metadata.  Once DV supports nested subfield tags, this can be changed to
	// something like:
	//   +k8s:subfield(metadata)=+k8s:subfield(atespace)=+k8s:required
	errs := Validate_ResourceMetadata(ctx, op, fldPath.Child("metadata"), actor.Metadata, nil)
	errs = append(errs, validate.RequiredValue(ctx, op, fldPath.Child("metadata", "atespace"), &actor.Metadata.Atespace, nil)...)
	return errs
}

// This is needed because DV doesn't have a standard format for IP addresses yet.
func ValidateCustom_WorkerAssignment_WorkerPodIps(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []string) field.ErrorList {
	return validateDualStackIPs(fldPath, value)
}

// This is needed because DV doesn't have a format for SPIFFE IDs.
func ValidateCustom_WorkerAssignment_WorkerSpiffeId(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return validateSPIFFEID(fldPath, *value)
}

func ValidateCustom_ExternalVolumeStatus_VolumeType(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateVolumeType(fldPath, *value)
}

func ValidateCustom_ExternalVolumeStatus_StorageVolumeId(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateStorageVolumeID(fldPath, *value)
}

func ValidateCustom_ExternalVolumeStatus_VolumeContext(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ map[string]string) field.ErrorList {
	return resources.ValidateCSIMap(fldPath, value)
}
