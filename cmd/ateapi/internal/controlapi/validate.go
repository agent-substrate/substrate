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
	"reflect"
	"strings"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func toGRPCStatusError(errs field.ErrorList) error {
	return status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
}

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

// ateDeepEqual compares two values of any type, using proto.Equal if both are
// proto messages, and reflect.DeepEqual otherwise.  This is called by
// declarative validation's generated code.
func ateDeepEqual[T any](a, b T) bool {
	asProto := func(x any) proto.Message {
		pm, ok := x.(proto.Message)
		if !ok {
			return nil
		}
		return pm
	}

	if pa, pb := asProto(a), asProto(b); pa != nil && pb != nil {
		return proto.Equal(pa, pb)
	}
	return reflect.DeepEqual(a, b)
}

// This is needed because DV doesn't have a standard format for IP addresses yet.
func ValidateCustom_WorkerAssignment_WorkerPodIp(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return validation.IsValidIP(fldPath, *value)
}

// maxCSRBytes bounds MintCertRequest's CSR. Real CSRs are a few KB; this is
// a guardrail, applied here because maxLength does not support bytes fields.
const maxCSRBytes = 16384

func ValidateCustom_MintCertRequest_CertificateSigningRequest(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []byte) field.ErrorList {
	if len(value) > maxCSRBytes {
		return field.ErrorList{field.TooLong(fldPath, nil, maxCSRBytes)}
	}
	return nil
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
