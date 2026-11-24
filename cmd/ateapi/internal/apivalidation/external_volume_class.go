// Copyright 2026 The Agent Substrate Authors
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

func ValidateCreateExternalVolumeClassRequest(ctx context.Context, req *ateapi.CreateExternalVolumeClassRequest) field.ErrorList {
	return Validate_CreateExternalVolumeClassRequest(ctx, MakeCreateOp(ctx), nil, req, nil)
}

func ValidateGetExternalVolumeClassRequest(ctx context.Context, req *ateapi.GetExternalVolumeClassRequest) field.ErrorList {
	return Validate_GetExternalVolumeClassRequest(ctx, MakeCreateOp(ctx), nil, req, nil)
}

func ValidateListExternalVolumeClassesRequest(ctx context.Context, req *ateapi.ListExternalVolumeClassesRequest) field.ErrorList {
	return Validate_ListExternalVolumeClassesRequest(ctx, MakeCreateOp(ctx), nil, req, nil)
}

func ValidateDeleteExternalVolumeClassRequest(ctx context.Context, req *ateapi.DeleteExternalVolumeClassRequest) field.ErrorList {
	return Validate_DeleteExternalVolumeClassRequest(ctx, MakeCreateOp(ctx), nil, req, nil)
}
