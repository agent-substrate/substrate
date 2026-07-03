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

	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	operation "k8s.io/apimachinery/pkg/api/operation"
)

func (x *CreateActorRequest) Validate(ctx context.Context) error {
	op := operation.Operation{Type: operation.Create}
	errs := Validate_CreateActorRequest(ctx, op, nil, x, nil)

	if len(errs) > 0 {
		result := status.New(codes.InvalidArgument, errs.ToAggregate().Error())
		for _, e := range errs {
			detail := ValidationError{
				Type:   string(e.Type),
				Field:  e.Field,
				Detail: e.Detail,
				Origin: e.Origin,
			}
			newResult, err := result.WithDetails(&detail)
			if err != nil {
				return status.Errorf(codes.Internal, "Unexpected error attaching details to error: %v", err)
			}
			result = newResult
		}
		return result.Err()
	}
	return nil
}
