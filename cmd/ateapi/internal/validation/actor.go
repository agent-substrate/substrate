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

package validation

import (
	"context"
	"strings"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/validate"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func ValidateCreateActorRequest(ctx context.Context, req *ateapipb.CreateActorRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_CreateActorRequest(ctx, op, nil, req, nil)
}

func ValidateGetActorRequest(ctx context.Context, req *ateapipb.GetActorRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_GetActorRequest(ctx, op, nil, req, nil)
}

func ValidateListActorsRequest(ctx context.Context, req *ateapipb.ListActorsRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_ListActorsRequest(ctx, op, nil, req, nil)
}

func ValidateUpdateActorRequest(ctx context.Context, req *ateapipb.UpdateActorRequest) field.ErrorList {
	// We model this as a create rather than an update because updates assume
	// the existence of a "current" value, which we do not have yet.  This is
	// validating the request itself. The result will be validated later, after
	// we have a current value to compare against.
	op := operation.Operation{Type: operation.Create}
	return Validate_UpdateActorRequest(ctx, op, nil, req, nil)
}

func ValidateDeleteActorRequest(ctx context.Context, req *ateapipb.DeleteActorRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_DeleteActorRequest(ctx, op, nil, req, nil)
}

func ValidatePauseActorRequest(ctx context.Context, req *ateapipb.PauseActorRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_PauseActorRequest(ctx, op, nil, req, nil)
}

func ValidateResumeActorRequest(ctx context.Context, req *ateapipb.ResumeActorRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_ResumeActorRequest(ctx, op, nil, req, nil)
}

func ValidateSuspendActorRequest(ctx context.Context, req *ateapipb.SuspendActorRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_SuspendActorRequest(ctx, op, nil, req, nil)
}

func ValidateActorUpdate(ctx context.Context, fldPath *field.Path, newVal, oldVal *ateapipb.Actor, requireStatus bool) field.ErrorList {
	op := operation.Operation{Type: operation.Update}
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
func ValidateCustom_UpdateActorRequest_Actor(ctx context.Context, op operation.Operation, fldPath *field.Path, actor, _ *ateapipb.Actor) field.ErrorList {
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
func ValidateCustom_WorkerAssignment_WorkerPodIp(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return validation.IsValidIP(fldPath, *value)
}

// ValidateCustom_ExternalVolume_VolumeType checks that a volume type string is well-formed.
// It allows an optional "substrate.io/" prefix, followed by a valid DNS-1123 subdomain.
func ValidateCustom_ExternalVolume_VolumeType(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	if value == nil || *value == "" {
		return nil
	}
	var errs field.ErrorList
	valToValidate := strings.TrimPrefix(*value, "substrate.io/")
	for _, msg := range validation.IsDNS1123Subdomain(valToValidate) {
		errs = append(errs, field.Invalid(fldPath, *value, msg))
	}
	return errs
}

// ValidateCustom_ExternalVolume_StorageVolumeId checks that an external volume's storage ID does not
// contain control characters (U+0000-U+0008, U+000B, U+000C, U+000E-U+001F, U+007F-U+009F).
func ValidateCustom_ExternalVolume_StorageVolumeId(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	if value == nil || *value == "" {
		return nil
	}
	for _, r := range *value {
		if (r >= 0x0000 && r <= 0x0008) ||
			r == 0x000B ||
			r == 0x000C ||
			(r >= 0x000E && r <= 0x001F) ||
			(r >= 0x007F && r <= 0x009F) {
			return field.ErrorList{field.Invalid(fldPath, *value, "must not contain control characters (U+0000-U+0008, U+000B, U+000C, U+000E-U+001F, U+007F-U+009F)")}
		}
	}
	return nil
}
