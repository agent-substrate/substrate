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
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func ValidateCreateAtespaceRequest(ctx context.Context, req *ateapipb.CreateAtespaceRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_CreateAtespaceRequest(ctx, op, nil, req, nil)
}

func ValidateGetAtespaceRequest(ctx context.Context, req *ateapipb.GetAtespaceRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_GetAtespaceRequest(ctx, op, nil, req, nil)
}

func ValidateListAtespacesRequest(ctx context.Context, req *ateapipb.ListAtespacesRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_ListAtespacesRequest(ctx, op, nil, req, nil)
}

func ValidateDeleteAtespaceRequest(ctx context.Context, req *ateapipb.DeleteAtespaceRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_DeleteAtespaceRequest(ctx, op, nil, req, nil)
}
