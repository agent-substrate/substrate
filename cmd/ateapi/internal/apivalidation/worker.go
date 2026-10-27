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

func ValidateListWorkerActorAssignmentsRequest(ctx context.Context, req *ateapi.ListWorkerActorAssignmentsRequest) field.ErrorList {
	op := MakeCreateOp(ctx)
	return Validate_ListWorkerActorAssignmentsRequest(ctx, op, nil, req, nil)
}

func ValidateListWorkersRequest(ctx context.Context, req *ateapi.ListWorkersRequest) field.ErrorList {
	// Call the generated validation.
	op := MakeCreateOp(ctx)
	return Validate_ListWorkersRequest(ctx, op, nil, req, nil)
}

func ValidateGetWorkerRequest(ctx context.Context, req *ateapi.GetWorkerRequest) field.ErrorList {
	// Call the generated validation.
	op := MakeCreateOp(ctx)
	return Validate_GetWorkerRequest(ctx, op, nil, req, nil)
}

func ValidateCreateWorkerRequest(ctx context.Context, req *ateapi.CreateWorkerRequest) field.ErrorList {
	// Call the generated validation.
	op := MakeCreateOp(ctx)
	return Validate_CreateWorkerRequest(ctx, op, nil, req, nil)
}

func ValidateUpdateWorkerRequest(ctx context.Context, req *ateapi.UpdateWorkerRequest) field.ErrorList {
	// Call the generated validation.
	// We model this as a create rather than an update because updates assume
	// the existence of a "current" value, which we do not have yet.  This is
	// validating the request itself. The result will be validated later, after
	// we have a current value to compare against.
	op := MakeCreateOp(ctx)
	return Validate_UpdateWorkerRequest(ctx, op, nil, req, nil)
}

func ValidateDeleteWorkerRequest(ctx context.Context, req *ateapi.DeleteWorkerRequest) field.ErrorList {
	// Call the generated validation. The preconditions in options are each
	// optional: a zero value waives that guard, so only non-zero values are
	// checked for shape.
	op := MakeCreateOp(ctx)
	return Validate_DeleteWorkerRequest(ctx, op, nil, req, nil)
}

func ValidateDrainWorkerRequest(ctx context.Context, req *ateapi.DrainWorkerRequest) field.ErrorList {
	// Call the generated validation.
	op := MakeCreateOp(ctx)
	return Validate_DrainWorkerRequest(ctx, op, nil, req, nil)
}

func ValidateAcquireWorkerEpochRequest(ctx context.Context, req *ateapi.AcquireWorkerEpochRequest) field.ErrorList {
	op := MakeCreateOp(ctx)
	return Validate_AcquireWorkerEpochRequest(ctx, op, nil, req, nil)
}

func ValidateRegisterWorkerRequest(ctx context.Context, req *ateapi.RegisterWorkerRequest) field.ErrorList {
	op := MakeCreateOp(ctx)
	return Validate_RegisterWorkerRequest(ctx, op, nil, req, nil)
}

func ValidateSetWorkerSandboxRuntimesRequest(ctx context.Context, req *ateapi.SetWorkerSandboxRuntimesRequest) field.ErrorList {
	op := MakeCreateOp(ctx)
	return Validate_SetWorkerSandboxRuntimesRequest(ctx, op, nil, req, nil)
}

func ValidateRequestActorSuspendRequest(ctx context.Context, req *ateapi.RequestActorSuspendRequest) field.ErrorList {
	op := MakeCreateOp(ctx)
	return Validate_RequestActorSuspendRequest(ctx, op, nil, req, nil)
}

func ValidateReportActorCrashedRequest(ctx context.Context, req *ateapi.ReportActorCrashedRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_ReportActorCrashedRequest(ctx, op, nil, req, nil)
}

func ValidateMarkWorkerAvailableRequest(ctx context.Context, req *ateapi.MarkWorkerAvailableRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_MarkWorkerAvailableRequest(ctx, op, nil, req, nil)
}

func ValidateMarkWorkerUnavailableRequest(ctx context.Context, req *ateapi.MarkWorkerUnavailableRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return Validate_MarkWorkerUnavailableRequest(ctx, op, nil, req, nil)
}

func ValidateReportActorSnapshotStateRequest(ctx context.Context, req *ateapi.ReportActorSnapshotStateRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	errs := Validate_ReportActorSnapshotStateRequest(ctx, op, nil, req, nil)
	// A report with neither latest_local nor uploading is a detach, which
	// names the snapshot it made durable.
	if st := req.GetState(); st != nil && st.GetLatestLocal() == nil && st.GetUploading() == nil && st.GetLatestDurable() == nil {
		errs = append(errs, field.Required(field.NewPath("state", "latest_durable"), "a report with neither latest_local nor uploading must set latest_durable"))
	}
	return errs
}

// validateWorkerUpdate validates a Worker against the previous stored value.
// It is what enforces the immutable fields, which need an old value to compare
// against.
func ValidateWorkerUpdate(ctx context.Context, fldPath *field.Path, newVal, oldVal *ateapi.Worker, requireStatus bool) field.ErrorList {
	// Call the generated validation.
	op := MakeUpdateOp(ctx)
	errs := Validate_Worker(ctx, op, fldPath, newVal, oldVal)
	if requireStatus {
		// Status is optional in the schema, but is actually required to be set
		// by the server.  If it was specified, it was already validated above,
		// but if it was not specified we need to flag that as an error.
		errs = append(errs, validate.RequiredPointer(ctx, op, fldPath.Child("status"), newVal.GetStatus(), nil)...)
	}
	return errs
}

func ValidateCustom_ActorSandboxRuntime_SandboxClass(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateSandboxClass(fldPath, *value)
}

func ValidateCustom_WorkerSandboxClass_SandboxClass(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateSandboxClass(fldPath, *value)
}

// This is needed because DV doesn't have a standard format for IP addresses yet.
func ValidateCustom_WorkerStatus_Ips(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []string) field.ErrorList {
	return validateDualStackIPs(fldPath, value)
}

// This is needed because DV doesn't have a standard format for IP addresses yet.
func ValidateCustom_RegisterWorkerRequest_Ips(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []string) field.ErrorList {
	return validateDualStackIPs(fldPath, value)
}

// This is needed because DV doesn't have a format for SPIFFE IDs.
func ValidateCustom_Worker_WorkerSpiffeId(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return validateSPIFFEID(fldPath, *value)
}

// This exists only because nested subfield tags are not supported yet.
func ValidateCustom_UpdateWorkerRequest_Worker(ctx context.Context, op operation.Operation, fldPath *field.Path, worker, _ *ateapi.Worker) field.ErrorList {
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
