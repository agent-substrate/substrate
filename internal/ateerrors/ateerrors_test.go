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

// errorReasonsFromStatus extracts the ErrorInfo reasons carried by a gRPC
// status error, mirroring how the ateapi control plane classifies failures.
// It returns nil when err is not a status error or carries no ErrorInfo.
func errorReasonsFromStatus(err error) []string {
	st, ok := status.FromError(err)
	if !ok {
		return nil
	}
	var reasons []string
	for _, d := range st.Details() {
		if info, ok := d.(*epb.ErrorInfo); ok {
			reasons = append(reasons, info.GetReason())
		}
	}
	return reasons
}

// statusWithReason builds a status error carrying the reason as an AIP-193
// ErrorInfo detail, standing in for a peer that stamps the detail on the wire.
func statusWithReason(t *testing.T, reason string) error {
	t.Helper()
	st, err := status.New(codes.DataLoss, "boom").WithDetails(&epb.ErrorInfo{
		Domain: "substrate.dev",
		Reason: reason,
	})
	if err != nil {
		t.Fatalf("WithDetails: %v", err)
	}
	return st.Err()
}

// TestReasonTagging verifies a Reason is itself an error: the layer that knows
// the domain meaning of a failure wraps it with %w, and callers recover it with
// errors.Is (a specific Reason) or errors.As (any Reason).
func TestReasonTagging(t *testing.T) {
	err := fmt.Errorf("%w: while reading record: %w", ReasonFailedGetExternalObject, errors.New("eof"))
	if !errors.Is(err, ReasonFailedGetExternalObject) {
		t.Errorf("errors.Is(%v, ReasonFailedGetExternalObject) = false, want true", err)
	}
	if errors.Is(err, ReasonInvalidSandboxAsset) {
		t.Errorf("errors.Is(%v, ReasonInvalidSandboxAsset) = true, want false", err)
	}
	var r Reason
	if !errors.As(err, &r) {
		t.Fatalf("errors.As(%v, *Reason) = false, want true", err)
	}
	if r != ReasonFailedGetExternalObject {
		t.Errorf("errors.As recovered Reason %q, want %q", r, ReasonFailedGetExternalObject)
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
			name: "grpc error carries reason",
			err:  statusWithReason(t, string(ReasonFaileSaveSnapshot)),
			want: []string{string(ReasonFaileSaveSnapshot)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// slices.Equal treats nil and empty as equal, which is the intent here:
			// "no reasons" may surface as either.
			if got := errorReasonsFromStatus(tt.err); !slices.Equal(got, tt.want) {
				t.Errorf("errorReasonsFromStatus() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExtractReason_EnforcesAllowedEnumValuesOnly(t *testing.T) {
	t.Run("wrapped tag returned", func(t *testing.T) {
		err := fmt.Errorf("%w: boom", ReasonFaileSaveSnapshot)
		if got := ExtractReason(err); got != "FAILED_SAVE_SNAPSHOT" {
			t.Errorf("ExtractReason(%v) = %q, want %q", err, got, "FAILED_SAVE_SNAPSHOT")
		}
	})

	t.Run("valid enum reason in ErrorInfo returned", func(t *testing.T) {
		err := statusWithReason(t, string(ReasonFaileSaveSnapshot))
		if got := ExtractReason(err); got != "FAILED_SAVE_SNAPSHOT" {
			t.Errorf("ExtractReason(%v) = %q, want %q", err, got, "FAILED_SAVE_SNAPSHOT")
		}
	})

	t.Run("unlisted dynamic reason rejected to prevent metric high cardinality", func(t *testing.T) {
		err := statusWithReason(t, "UNLISTED_DYNAMIC_ERROR_STRING")
		if got := ExtractReason(err); got != "" {
			t.Errorf("ExtractReason(%v) = %q, want %q (empty string)", err, got, "")
		}
	})
}

func TestAllReasonsRegistered(t *testing.T) {
	if len(AllReasons) == 0 {
		t.Fatal("AllReasons slice is empty")
	}

	for _, r := range AllReasons {
		if !IsValidReason(string(r)) {
			t.Errorf("IsValidReason(%q) = false, want true", r)
		}
		err := fmt.Errorf("%w: boom", r)
		if got := ExtractReason(err); got != string(r) {
			t.Errorf("ExtractReason for %q = %q, want %q", r, got, r)
		}
	}
}
