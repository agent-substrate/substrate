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
	"errors"
	"fmt"
	"testing"

	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"google.golang.org/grpc/codes"
)

func TestMapStoreError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{
			name: "nil",
			err:  nil,
			want: codes.OK,
		},
		{
			name: "wrapped not found",
			err:  fmt.Errorf("while getting actor: %w", store.ErrNotFound),
			want: codes.NotFound,
		},
		{
			name: "uid conflict",
			err:  store.ErrUIDConflict,
			want: codes.Aborted,
		},
		{
			name: "precondition required",
			err:  fmt.Errorf("%w: uid", store.ErrPreconditionRequired),
			want: codes.InvalidArgument,
		},
		{
			name: "apierror wrapping a sentinel keeps its code",
			err:  apierror.FailedPrecondition("tag gone: %w", store.ErrNotFound),
			want: codes.FailedPrecondition,
		},
		{
			name: "context error",
			err:  fmt.Errorf("while getting actor: %w", context.Canceled),
			want: codes.Canceled,
		},
		{
			name: "plain error",
			err:  errors.New("boom"),
			want: codes.Internal,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := apierror.Code(MapStoreError(tt.err, "Actor %s", "team-a/id1")); got != tt.want {
				t.Errorf("MapStoreError(%v) code = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
