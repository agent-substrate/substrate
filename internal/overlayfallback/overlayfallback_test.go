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

package overlayfallback

import (
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsMountFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "plain error", err: errors.New("boom"), want: false},
		{name: "mount failure", err: MountFailure(errors.New("no such device")), want: true},
		{name: "mount failure wrapping wrapped err", err: MountFailure(fmt.Errorf("while mounting overlay rootfs: %w", errors.New("EPERM"))), want: true},
		{
			name: "same code without marker",
			err:  status.Error(codes.FailedPrecondition, "some other precondition"),
			want: false,
		},
		{
			name: "marker under wrong code",
			err:  status.Error(codes.Internal, marker),
			want: false,
		},
		{name: "unrelated internal error", err: status.Error(codes.Internal, "internal server error"), want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsMountFailure(tc.err); got != tc.want {
				t.Errorf("IsMountFailure(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestIsMountFailure_SurvivesInterceptorRebuild verifies the signal survives the
// ateom server interceptor, which reconstructs any status error as
// status.Error(code, message) — dropping details but keeping code and message.
// The marker lives in the message precisely so it survives this rebuild.
func TestIsMountFailure_SurvivesInterceptorRebuild(t *testing.T) {
	orig := MountFailure(errors.New("mount(2): operation not permitted"))

	st, ok := status.FromError(orig)
	if !ok {
		t.Fatalf("MountFailure did not produce a status error")
	}
	rebuilt := status.Error(st.Code(), st.Message())

	if !IsMountFailure(rebuilt) {
		t.Errorf("IsMountFailure(rebuilt) = false, want true; rebuilt=%v", rebuilt)
	}
}
