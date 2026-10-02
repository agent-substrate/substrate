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

package ateomvalidation

import (
	"context"

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Each wrapper validates its request at the RPC edge. A non-nil return is the
// InvalidArgument error the handler responds with, built by resources.ToAPIError. Requests are validated as
// creates: no Ateom RPC updates prior state, so there is nothing to ratchet
// against.

// ValidateRunWorkloadRequest validates req at the RPC edge.
func ValidateRunWorkloadRequest(ctx context.Context, req *ateompb.RunWorkloadRequest) error {
	if errs := Validate_RunWorkloadRequest(ctx, createOp(), nil, req, nil); len(errs) > 0 {
		return resources.ToAPIError(errs)
	}
	return nil
}

// ValidateCheckpointWorkloadRequest validates req at the RPC edge.
func ValidateCheckpointWorkloadRequest(ctx context.Context, req *ateompb.CheckpointWorkloadRequest) error {
	if errs := Validate_CheckpointWorkloadRequest(ctx, createOp(), nil, req, nil); len(errs) > 0 {
		return resources.ToAPIError(errs)
	}
	return nil
}

// ValidateRestoreWorkloadRequest validates req at the RPC edge.
func ValidateRestoreWorkloadRequest(ctx context.Context, req *ateompb.RestoreWorkloadRequest) error {
	if errs := Validate_RestoreWorkloadRequest(ctx, createOp(), nil, req, nil); len(errs) > 0 {
		return resources.ToAPIError(errs)
	}
	return nil
}

// ValidateTerminateWorkloadRequest validates req at the RPC edge.
func ValidateTerminateWorkloadRequest(ctx context.Context, req *ateompb.TerminateWorkloadRequest) error {
	if errs := Validate_TerminateWorkloadRequest(ctx, createOp(), nil, req, nil); len(errs) > 0 {
		return resources.ToAPIError(errs)
	}
	return nil
}

// ValidateGetWorkloadStatsRequest validates req at the RPC edge.
func ValidateGetWorkloadStatsRequest(ctx context.Context, req *ateompb.GetWorkloadStatsRequest) error {
	if errs := Validate_GetWorkloadStatsRequest(ctx, createOp(), nil, req, nil); len(errs) > 0 {
		return resources.ToAPIError(errs)
	}
	return nil
}

// MaxContainers is the bound on WorkloadSpec.containers, the template's own
// bound; the +k8s:maxItems tag on the field carries the same number.
const MaxContainers = 10

// Run and Restore need a container to start. The WorkloadSpec message itself
// allows none, for the fallback spec Terminate sends when an actor's template
// is gone, but a Run or Restore with none would boot the sandbox around
// nothing.

func ValidateCustom_RunWorkloadRequest_Spec(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *ateompb.WorkloadSpec) field.ErrorList {
	return requireContainers(fldPath, value)
}

func ValidateCustom_RestoreWorkloadRequest_Spec(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *ateompb.WorkloadSpec) field.ErrorList {
	return requireContainers(fldPath, value)
}

func requireContainers(fldPath *field.Path, spec *ateompb.WorkloadSpec) field.ErrorList {
	if len(spec.GetContainers()) == 0 {
		return field.ErrorList{field.Required(fldPath.Child("containers"), "a workload needs a container to start")}
	}
	return nil
}

// GetActiveWorkloadStatsRequest has no fields, so validation-gen emits
// nothing for it and there is no wrapper.

// ValidateCustom_RestoreWorkloadRequest_SnapshotUri holds snapshot_uri to
// the shape the storage layer appends object names to: a scheme, a bucket,
// and a path. The rule is the control plane's, shared through
// internal/resources.
func ValidateCustom_RestoreWorkloadRequest_SnapshotUri(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return validateSnapshotURI(fldPath, *value)
}

// ValidateCustom_CheckpointWorkloadRequest_SnapshotUri applies the
// snapshot_uri rule to the URI a checkpoint is written to.
func ValidateCustom_CheckpointWorkloadRequest_SnapshotUri(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return validateSnapshotURI(fldPath, *value)
}

// ValidateCustom_EgressGateway_Address holds the gateway address to the
// host:port shape the tunnel dials. The rule is atelet's, shared through
// internal/resources, so the two ends of the RPC agree on what is dialable.
func ValidateCustom_EgressGateway_Address(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateHostPort(fldPath, *value)
}

// ValidateCustom_ActorDirs holds every directory to an absolute, clean path.
// Presence and length are the tags' job. The rule is the one both ateom
// binaries applied by hand before, shared through internal/resources.
func ValidateCustom_ActorDirs(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *ateompb.ActorDirs) field.ErrorList {
	return resources.ValidateActorDirs(value, fldPath)
}

func validateSnapshotURI(fldPath *field.Path, uri string) field.ErrorList {
	if err := resources.ValidateSnapshotLocation(uri); err != nil {
		return field.ErrorList{field.Invalid(fldPath, uri, err.Error())}
	}
	return nil
}

func createOp() operation.Operation {
	return operation.Operation{Type: operation.Create}
}

// ateDeepEqual is the deep-equal function declarative validation's generated
// code calls by name; it delegates to resources.DeepEqual.
func ateDeepEqual[T any](a, b T) bool {
	return resources.DeepEqual(a, b)
}
