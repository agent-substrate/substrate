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

package ateapipb

import (
	"context"

	operation "k8s.io/apimachinery/pkg/api/operation"
	field "k8s.io/apimachinery/pkg/util/validation/field"
)

func (x *CreateActorRequest) Validate(ctx context.Context) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return Validate_CreateActorRequest(ctx, op, nil, x, nil)
}
