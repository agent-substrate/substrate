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

	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// ValidateRequestActorSuspendRequest runs the generated validation for req.
func ValidateRequestActorSuspendRequest(ctx context.Context, req *ateletpb.RequestActorSuspendRequest) field.ErrorList {
	return Validate_RequestActorSuspendRequest(ctx, operation.Operation{Type: operation.Create}, nil, req, nil)
}

// ValidateMintActorCertificateRequest runs the generated validation for req.
func ValidateMintActorCertificateRequest(ctx context.Context, req *ateletpb.MintActorCertificateRequest) field.ErrorList {
	return Validate_MintActorCertificateRequest(ctx, operation.Operation{Type: operation.Create}, nil, req, nil)
}

// ValidateSetWorkerCapacityRequest runs the generated validation for req.
func ValidateSetWorkerCapacityRequest(ctx context.Context, req *ateletpb.SetWorkerCapacityRequest) field.ErrorList {
	return Validate_SetWorkerCapacityRequest(ctx, operation.Operation{Type: operation.Create}, nil, req, nil)
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
