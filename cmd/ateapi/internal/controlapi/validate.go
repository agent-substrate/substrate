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

package controlapi

import (
	"context"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func toGRPCInternalError(errs field.ErrorList) error {
	return status.Error(codes.Internal, errs.ToAggregate().Error())
}

// scrubResourceMetadataForCreate removes fields that should not be set by the
// user when creating a resource.
func scrubResourceMetadataForCreate(in *ateapipb.ResourceMetadata) {
	if in == nil {
		return // validation will flag it
	}
	in.Uid = ""         // will be set later
	in.Version = 0      // will be set later
	in.CreateTime = nil // will be set later
	in.UpdateTime = nil // will be set later
}

// scrubResourceMetadataForUpdate removes fields that should not be set by the
// user when updating a resource.
func scrubResourceMetadataForUpdate(in *ateapipb.ResourceMetadata) {
	if in == nil {
		return // validation will flag it
	}
	// in.Uid and in.Version are preconditions, so we don't scrub them.
	in.CreateTime = nil // will be set later
	in.UpdateTime = nil // will be set later
}

// ateDeepEqual is the deep-equal function declarative validation's generated
// code calls by name; it delegates to resources.DeepEqual.
func ateDeepEqual[T any](a, b T) bool {
	return resources.DeepEqual(a, b)
}

// ValidateCustom_ResourceMetadata checks the server-stamped timestamps: each,
// when set, must be a valid google.protobuf.Timestamp, and update_time must
// not precede create_time. Both fields are scrubbed from input, so a
// violation here is a server stamping bug surfaced by the final-object
// validation pass, not a client error.
func ValidateCustom_ResourceMetadata(_ context.Context, _ operation.Operation, fldPath *field.Path, obj, _ *ateapipb.ResourceMetadata) field.ErrorList {
	var errs field.ErrorList
	createTimeValid := false
	if ct := obj.GetCreateTime(); ct != nil {
		if err := ct.CheckValid(); err != nil {
			errs = append(errs, field.Invalid(fldPath.Child("create_time"), ct.String(), err.Error()))
		} else {
			createTimeValid = true
		}
	}
	if ut := obj.GetUpdateTime(); ut != nil {
		if err := ut.CheckValid(); err != nil {
			errs = append(errs, field.Invalid(fldPath.Child("update_time"), ut.String(), err.Error()))
		} else if createTimeValid && ut.AsTime().Before(obj.GetCreateTime().AsTime()) {
			errs = append(errs, field.Invalid(fldPath.Child("update_time"), ut.String(), "must not precede create_time"))
		}
	}
	return errs
}

// This is needed because DV doesn't have a standard format for IP addresses yet.
func ValidateCustom_WorkerAssignment_WorkerPodIp(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return validation.IsValidIP(fldPath, *value)
}

func ValidateCustom_ExternalVolume_VolumeType(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateVolumeType(fldPath, *value)
}

func ValidateCustom_ExternalVolume_StorageVolumeId(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateStorageVolumeID(fldPath, *value)
}
