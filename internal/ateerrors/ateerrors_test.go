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

package ateerrors

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	epb "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestErrorsIs verifies sentinel matching works directly and through a
// %w-wrapped chain (how the storage layer surfaces them), and does not cross
// reasons.
func TestErrorsIs(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "sentinel only", err: ErrAteletSnapshotNotFound, want: true},
		{name: "wrapped", err: fmt.Errorf("fetching: %w", ErrAteletSnapshotNotFound), want: true},
		{name: "different reason", err: ErrAteletSnapshotCorrupt, want: false},
		{name: "plain error", err: errors.New("boom"), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := errors.Is(tt.err, ErrAteletSnapshotNotFound); got != tt.want {
				t.Errorf("errors.Is(%v, ErrAteletSnapshotNotFound) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// TestNewGRPCError verifies the message comes from err while the Reason comes
// from the sentinel, and that the reason round-trips through the gRPC status as
// an ErrorInfo detail.
func TestNewGRPCError(t *testing.T) {
	cause := fmt.Errorf("fetching manifest: %w", ErrAteletSnapshotNotFound)
	err := NewGRPCError(codes.NotFound, ErrReasonCrashActor, cause)

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("status.FromError(%v) = _, false; want a status error", err)
	}
	if got, want := st.Code(), codes.NotFound; got != want {
		t.Errorf("status code = %v, want %v", got, want)
	}
	if got, want := st.Message(), cause.Error(); got != want {
		t.Errorf("status message = %q, want %q", got, want)
	}

	// The reason must be extractable so the ateapi control plane can classify the
	// failure as terminal (CRASHED).
	if got := ErrorReasonsFromStatus(err); !slices.Contains(got, ErrReasonCrashActor.Error()) {
		t.Errorf("ErrorReasonsFromStatus() = %q, want it to contain %q", got, ErrReasonCrashActor.Error())
	}

	var info *epb.ErrorInfo
	for _, d := range st.Details() {
		if v, ok := d.(*epb.ErrorInfo); ok {
			info = v
		}
	}
	if info == nil {
		t.Fatal("status is missing the ErrorInfo detail")
	}
	if got, want := info.GetReason(), ErrReasonCrashActor.Error(); got != want {
		t.Errorf("ErrorInfo.Reason = %q, want %q", got, want)
	}
	if got, want := info.GetDomain(), errorDomain; got != want {
		t.Errorf("ErrorInfo.Domain = %q, want %q", got, want)
	}
}

func TestErrorReasonsFromStatus(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want []string
	}{
		{name: "nil error", err: nil, want: nil},
		{name: "plain error without status", err: errors.New("boom"), want: nil},
		{name: "status without error info", err: status.Error(codes.Unavailable, "transient"), want: nil},
		{
			name: "grpc error from sentinel",
			err:  NewGRPCError(codes.NotFound, ErrReasonCrashActor, ErrAteletSnapshotNotFound),
			want: []string{ErrReasonCrashActor.Error()},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// slices.Equal treats nil and empty as equal, which is the intent here:
			// "no reasons" may surface as either.
			if got := ErrorReasonsFromStatus(tt.err); !slices.Equal(got, tt.want) {
				t.Errorf("ErrorReasonsFromStatus() = %q, want %q", got, tt.want)
			}
		})
	}
}
