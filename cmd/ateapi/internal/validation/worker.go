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

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/validate"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func ValidateListWorkersRequest(ctx context.Context, req *ateapipb.ListWorkersRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_ListWorkersRequest(ctx, op, nil, req, nil)
}

func ValidateGetWorkerRequest(ctx context.Context, req *ateapipb.GetWorkerRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_GetWorkerRequest(ctx, op, nil, req, nil)
}

func ValidateCreateWorkerRequest(ctx context.Context, req *ateapipb.CreateWorkerRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_CreateWorkerRequest(ctx, op, nil, req, nil)
}

func ValidateUpdateWorkerRequest(ctx context.Context, req *ateapipb.UpdateWorkerRequest) field.ErrorList {
	// We model this as a create rather than an update because updates assume
	// the existence of a "current" value, which we do not have yet.  This is
	// validating the request itself. The result will be validated later, after
	// we have a current value to compare against.
	op := operation.Operation{Type: operation.Create}
	return Validate_UpdateWorkerRequest(ctx, op, nil, req, nil)
}

func ValidateDeleteWorkerRequest(ctx context.Context, req *ateapipb.DeleteWorkerRequest) field.ErrorList {
	// The preconditions in options are each optional: a zero value waives that
	// guard, so only non-zero values are checked for shape.
	op := operation.Operation{Type: operation.Create}
	return Validate_DeleteWorkerRequest(ctx, op, nil, req, nil)
}

func ValidateDrainWorkerRequest(ctx context.Context, req *ateapipb.DrainWorkerRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_DrainWorkerRequest(ctx, op, nil, req, nil)
}

// ValidateWorkerUpdate validates a Worker against the previous stored value.
// It is what enforces the immutable fields, which need an old value to compare
// against.
func ValidateWorkerUpdate(ctx context.Context, fldPath *field.Path, newVal, oldVal *ateapipb.Worker, requireStatus bool) field.ErrorList {
	op := operation.Operation{Type: operation.Update}
	errs := Validate_Worker(ctx, op, fldPath, newVal, oldVal)
	if requireStatus {
		// Status is optional in the schema, but is actually required to be set
		// by the server.  If it was specified, it was already validated above,
		// but if it was not specified we need to flag that as an error.
		errs = append(errs, validate.RequiredPointer(ctx, op, fldPath.Child("status"), newVal.GetStatus(), nil)...)
	}
	return errs
}

// This is needed because DV doesn't have a standard format for IP addresses yet.
func ValidateCustom_Worker_Ip(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return validation.IsValidIP(fldPath, *value)
}

// This exists only because nested subfield tags are not supported yet.
func ValidateCustom_UpdateWorkerRequest_Worker(ctx context.Context, op operation.Operation, fldPath *field.Path, worker, _ *ateapipb.Worker) field.ErrorList {
	if worker == nil || worker.Metadata == nil {
		return nil // handled by DV
	}

	// Updates are validated in 2 steps: first the update request and then the
	// resource itself. DV for the request doesn't descend into the resource
	// metadata.  Once DV supports nested subfield tags, this can be changed to
	// something like:
	//   +k8s:subfield(metadata)=+k8s:subfield(atespace)=+k8s:forbidden
	// Workers are global-scoped, so metadata.atespace must be empty.
	errs := Validate_ResourceMetadata(ctx, op, fldPath.Child("metadata"), worker.Metadata, nil)
	errs = append(errs, validate.ForbiddenValue(ctx, op, fldPath.Child("metadata", "atespace"), &worker.Metadata.Atespace, nil)...)
	return errs
}
