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
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func ValidateCreateAtespaceRequest(ctx context.Context, req *ateapi.CreateAtespaceRequest) field.ErrorList {
	// Call the generated validation.
	op := MakeCreateOp(ctx)
	return Validate_CreateAtespaceRequest(ctx, op, nil, req, nil)
}

func ValidateGetAtespaceRequest(ctx context.Context, req *ateapi.GetAtespaceRequest) field.ErrorList {
	// Call the generated validation.
	op := MakeCreateOp(ctx)
	return Validate_GetAtespaceRequest(ctx, op, nil, req, nil)
}

func ValidateListAtespacesRequest(ctx context.Context, req *ateapi.ListAtespacesRequest) field.ErrorList {
	// Call the generated validation.
	op := MakeCreateOp(ctx)
	return Validate_ListAtespacesRequest(ctx, op, nil, req, nil)
}

func ValidateDeleteAtespaceRequest(ctx context.Context, req *ateapi.DeleteAtespaceRequest) field.ErrorList {
	// Call the generated validation.
	op := MakeCreateOp(ctx)
	return Validate_DeleteAtespaceRequest(ctx, op, nil, req, nil)
}
