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

package ateworkervalidation

import (
	"context"

	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Each wrapper validates its request at the RPC edge. A non-nil return is the
// InvalidArgument error the handler responds with, built by
// resources.ToAPIError. Requests are validated as creates: no Worker RPC
// updates prior state, so there is nothing to ratchet against.

// ValidateRunWorkloadRequest validates req at the RPC edge.
func ValidateRunWorkloadRequest(ctx context.Context, req *ateworkerpb.RunWorkloadRequest) error {
	if errs := Validate_RunWorkloadRequest(ctx, createOp(), nil, req, nil); len(errs) > 0 {
		return resources.ToAPIError(errs)
	}
	return nil
}

// ValidateCheckpointWorkloadRequest validates req at the RPC edge.
func ValidateCheckpointWorkloadRequest(ctx context.Context, req *ateworkerpb.CheckpointWorkloadRequest) error {
	if errs := Validate_CheckpointWorkloadRequest(ctx, createOp(), nil, req, nil); len(errs) > 0 {
		return resources.ToAPIError(errs)
	}
	return nil
}

// ValidateRestoreWorkloadRequest validates req at the RPC edge.
func ValidateRestoreWorkloadRequest(ctx context.Context, req *ateworkerpb.RestoreWorkloadRequest) error {
	if errs := Validate_RestoreWorkloadRequest(ctx, createOp(), nil, req, nil); len(errs) > 0 {
		return resources.ToAPIError(errs)
	}
	return nil
}

// ValidateTerminateWorkloadRequest validates req at the RPC edge.
func ValidateTerminateWorkloadRequest(ctx context.Context, req *ateworkerpb.TerminateWorkloadRequest) error {
	if errs := Validate_TerminateWorkloadRequest(ctx, createOp(), nil, req, nil); len(errs) > 0 {
		return resources.ToAPIError(errs)
	}
	return nil
}

// ValidateGetWorkloadStatsRequest validates req at the RPC edge.
func ValidateGetWorkloadStatsRequest(ctx context.Context, req *ateworkerpb.GetWorkloadStatsRequest) error {
	if errs := Validate_GetWorkloadStatsRequest(ctx, createOp(), nil, req, nil); len(errs) > 0 {
		return resources.ToAPIError(errs)
	}
	return nil
}

// GetActiveWorkloadStatsRequest has no fields, so validation-gen emits
// nothing for it and there is no wrapper.

// ValidateCustom_EgressGateway_Address holds the gateway address to the
// host:port shape the tunnel dials. The rule is atelet's, shared through
// internal/resources, so the two ends of the RPC agree on what is dialable.
func ValidateCustom_EgressGateway_Address(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateHostPort(fldPath, *value)
}

// ValidateCustom_HttpGetAction_Path holds a wakeup probe's path to the RFC
// 3986 path shape atelet already enforces, shared through internal/resources.
func ValidateCustom_HttpGetAction_Path(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateHTTPGetPath(fldPath, *value)
}

// ValidateCustom_VolumeMount_MountPath holds a mount path to the clean
// absolute Unix path shape atelet already enforces, shared through
// internal/resources.
func ValidateCustom_VolumeMount_MountPath(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateMountPath(fldPath, *value)
}

// ValidateCustom_Container holds a container's mount paths unique and
// non-nesting across its four mount lists, as atelet does for its single
// list: volumes cannot mount onto other volumes. A repeat within one list is
// the list key's finding and is not reported again here.
func ValidateCustom_Container(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *ateworkerpb.Container) field.ErrorList {
	type mount struct {
		path, list string
		fld        *field.Path
	}
	var mounts []mount
	add := func(list string, i int, path string) {
		mounts = append(mounts, mount{path: path, list: list, fld: fldPath.Child(list).Index(i).Child("mount_path")})
	}
	for i, m := range value.GetDurableDirVolumeMounts() {
		add("durable_dir_volume_mounts", i, m.GetMountPath())
	}
	for i, m := range value.GetExternalVolumeMounts() {
		add("external_volume_mounts", i, m.GetMountPath())
	}
	for i, m := range value.GetSystemInfoVolumeMounts() {
		add("system_info_volume_mounts", i, m.GetMountPath())
	}
	for i, m := range value.GetImageVolumeMounts() {
		add("image_volume_mounts", i, m.GetMountPath())
	}

	var errs field.ErrorList
	firstList := make(map[string]string, len(mounts))
	paths := make([]string, len(mounts))
	for i, m := range mounts {
		paths[i] = m.path
		if m.path == "" {
			continue
		}
		if list, seen := firstList[m.path]; seen {
			if list != m.list {
				errs = append(errs, field.Duplicate(m.fld, m.path))
			}
			continue
		}
		firstList[m.path] = m.list
	}
	return append(errs, resources.ValidateNestedMountPathsAt(paths, func(i int) *field.Path { return mounts[i].fld })...)
}

// ValidateCustom_ActorDirs holds every directory to an absolute, clean path.
// Presence and length are the tags' job. The rule is the one both worker
// binaries applied by hand before, shared through internal/resources.
func ValidateCustom_ActorDirs(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *ateworkerpb.ActorDirs) field.ErrorList {
	return resources.ValidateActorDirs(value, fldPath)
}

func createOp() operation.Operation {
	return operation.Operation{Type: operation.Create}
}

// ateDeepEqual is the deep-equal function declarative validation's generated
// code calls by name; it delegates to resources.DeepEqual.
func ateDeepEqual[T any](a, b T) bool {
	return resources.DeepEqual(a, b)
}
