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
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

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

func toGRPCStatusError(errs field.ErrorList) error {
	return status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
}

func toGRPCInternalError(errs field.ErrorList) error {
	return status.Error(codes.Internal, errs.ToAggregate().Error())
}
