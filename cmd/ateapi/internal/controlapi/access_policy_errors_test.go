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

package controlapi

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMapAccessPolicyWrite_Codes(t *testing.T) {
	// OpenFGA reports tuple validation failures with nonstandard codes such as 2000.
	openFGAErr := status.Error(codes.Code(2000), "invalid tuple")

	tests := []struct {
		name string
		err  error
		msg  string
		want codes.Code
	}{
		{name: "nonstandard backend code", err: openFGAErr, msg: "custom precondition message", want: codes.Internal},
		{name: "wrapped nonstandard backend code", err: fmt.Errorf("reconciling: %w", openFGAErr), msg: "custom precondition message", want: codes.Internal},
		{name: "apierror passes through", err: apierror.InvalidArgument("bad"), msg: "custom precondition message", want: codes.InvalidArgument},
		{name: "plain error", err: errors.New("boom"), msg: "custom precondition message", want: codes.Internal},
		{name: "not found", err: store.ErrNotFound, msg: "custom precondition message", want: codes.NotFound},
		{name: "failed precondition", err: store.ErrFailedPrecondition, msg: "custom precondition message", want: codes.FailedPrecondition},
		{name: "unexpected failed precondition falls through", err: store.ErrFailedPrecondition, msg: "", want: codes.Internal},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := mapAccessPolicyWrite(nil, tc.err, tc.msg)
			if got := apierror.Code(err); got != tc.want {
				t.Errorf("mapAccessPolicyWrite(%v) code = %v, want %v", tc.err, got, tc.want)
			}
			if tc.want == codes.FailedPrecondition && !strings.Contains(err.Error(), tc.msg) {
				t.Errorf("mapAccessPolicyWrite(ErrFailedPrecondition) = %q, want %q", err.Error(), tc.msg)
			}
		})
	}
}
