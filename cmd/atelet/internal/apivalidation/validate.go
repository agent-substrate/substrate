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

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// ValidateRequestActorSuspendRequest validates req at the RPC edge. A non-nil
// return is the InvalidArgument error the handler responds with.
func ValidateRequestActorSuspendRequest(ctx context.Context, req *ateletpb.RequestActorSuspendRequest) error {
	return toInvalidArgument(Validate_RequestActorSuspendRequest(ctx, operation.Operation{Type: operation.Create}, nil, req, nil))
}

// ValidateMintActorCertificateRequest validates req at the RPC edge. A
// non-nil return is the InvalidArgument error the handler responds with.
func ValidateMintActorCertificateRequest(ctx context.Context, req *ateletpb.MintActorCertificateRequest) error {
	return toInvalidArgument(Validate_MintActorCertificateRequest(ctx, operation.Operation{Type: operation.Create}, nil, req, nil))
}

// ValidateSetWorkerCapacityRequest validates req at the RPC edge. A non-nil
// return is the InvalidArgument error the handler responds with.
func ValidateSetWorkerCapacityRequest(ctx context.Context, req *ateletpb.SetWorkerCapacityRequest) error {
	return toInvalidArgument(Validate_SetWorkerCapacityRequest(ctx, operation.Operation{Type: operation.Create}, nil, req, nil))
}

func toInvalidArgument(errs field.ErrorList) error {
	if len(errs) == 0 {
		return nil
	}
	return status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
}

// ValidateCustom_Limits validates one limit with resources.ValidateLimit, the
// rule the control plane applies to its own Limits. Presence and uniqueness
// of names are enforced by tags.
func ValidateCustom_Limits(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *ateletpb.Limits) field.ErrorList {
	return resources.ValidateLimit(fldPath, value.GetName(), value.GetQuantity())
}

// ateDeepEqual is the deep-equal function declarative validation's generated
// code calls by name; it delegates to resources.DeepEqual.
func ateDeepEqual[T any](a, b T) bool {
	return resources.DeepEqual(a, b)
}
