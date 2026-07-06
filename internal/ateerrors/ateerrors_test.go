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
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
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

// TestNewGRPCError verifies the message comes from err, the Reason and metadata
// come from the arguments, the Domain is the package constant, and that they
// round-trip through the gRPC status as an ErrorInfo detail.
func TestNewGRPCError(t *testing.T) {
	tests := []struct {
		name         string
		reason       Reason
		metadata     map[string]string
		wantReason   string
		wantMetadata map[string]string
	}{
		{
			name:         "actor crashed metadata",
			reason:       ReasonFaileSaveSnapshot,
			metadata:     ActorCrashedMetadata(),
			wantReason:   string(ReasonFaileSaveSnapshot),
			wantMetadata: map[string]string{MetadataKeyActorCrashed: "true"},
		},
		{
			name:         "no metadata",
			reason:       ReasonInvalidCheckpointResult,
			metadata:     nil,
			wantReason:   string(ReasonInvalidCheckpointResult),
			wantMetadata: nil,
		},
		{
			name:         "empty reason defaults to UNSET",
			reason:       "",
			metadata:     nil,
			wantReason:   "UNSET",
			wantMetadata: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cause := errors.New("fetching manifest: snapshot missing")
			err := NewGRPCError(context.Background(), codes.NotFound, tt.reason, tt.metadata, cause)

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

			// The reason must be extractable so the ateapi control plane can classify
			// the failure.
			if got := errorReasonsFromStatus(err); !slices.Contains(got, tt.wantReason) {
				t.Errorf("errorReasonsFromStatus() = %q, want it to contain %q", got, tt.wantReason)
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
			if got := info.GetReason(); got != tt.wantReason {
				t.Errorf("ErrorInfo.Reason = %q, want %q", got, tt.wantReason)
			}
			if diff := cmp.Diff(tt.wantMetadata, info.GetMetadata(), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("ErrorInfo.Metadata mismatch (-want +got):\n%s", diff)
			}
			// NewGRPCError stamps the package Domain into the ErrorInfo.
			if got, want := info.GetDomain(), errorDomain; got != want {
				t.Errorf("ErrorInfo.Domain = %q, want %q", got, want)
			}
		})
	}
}

// TestNewGRPCErrorInvalidInput verifies that a nil err or an OK code yields a
// plain validation error (not a gRPC status, so it carries no Reason or crash
// directive that the control plane could misclassify).
func TestNewGRPCErrorInvalidInput(t *testing.T) {
	tests := []struct {
		name     string
		grpcCode codes.Code
		err      error
	}{
		{name: "nil err", grpcCode: codes.NotFound, err: nil},
		{name: "OK code with valid err", grpcCode: codes.OK, err: errors.New("boom")},
		{name: "OK code with nil err", grpcCode: codes.OK, err: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewGRPCError(context.Background(), tt.grpcCode, ReasonInvalidCheckpointResult, nil, tt.err)
			if err == nil {
				t.Fatalf("NewGRPCError(%v, nil, %v) = nil, want a validation error", tt.grpcCode, tt.err)
			}
			// The validation error is a plain error, not a gRPC status, and it must
			// not carry a classifiable Reason or crash directive.
			if _, ok := status.FromError(err); ok {
				t.Errorf("NewGRPCError(...) = %v; want a plain error, not a gRPC status", err)
			}
			if got := errorReasonsFromStatus(err); len(got) != 0 {
				t.Errorf("errorReasonsFromStatus() = %q, want no reasons", got)
			}
			if ActorCrashRequested(err) {
				t.Errorf("ActorCrashRequested(%v) = true, want false", err)
			}
		})
	}
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

// TestCrashIfReason verifies the boundary rule: an error whose chain carries a
// Reason the call site explicitly claims escalates to a DataLoss gRPC status
// with that Reason and the actor-crash directive; anything else — untagged, or
// tagged with an unclaimed Reason — passes through unchanged.
func TestCrashIfReason(t *testing.T) {
	t.Run("claimed reason escalates to DataLoss and crash", func(t *testing.T) {
		tagged := fmt.Errorf("%w: while parsing manifest: %w", ReasonInvalidSandboxAsset, errors.New("bad json"))
		err := CrashIfReason(context.Background(), tagged, ReasonFailedGetExternalObject, ReasonInvalidSandboxAsset)

		st, ok := status.FromError(err)
		if !ok {
			t.Fatalf("CrashIfReason(tagged, claimed) = %v, want a gRPC status error", err)
		}
		if got, want := st.Code(), codes.DataLoss; got != want {
			t.Errorf("status code = %v, want %v", got, want)
		}
		if got := errorReasonsFromStatus(err); !slices.Contains(got, string(ReasonInvalidSandboxAsset)) {
			t.Errorf("errorReasonsFromStatus() = %q, want it to contain %q", got, ReasonInvalidSandboxAsset)
		}
		if !ActorCrashRequested(err) {
			t.Errorf("ActorCrashRequested(%v) = false, want true", err)
		}
	})

	t.Run("unclaimed reason passes through unchanged", func(t *testing.T) {
		// The chain is tagged terminal, but this boundary does not claim that
		// Reason, so the actor must not crash.
		tagged := fmt.Errorf("%w: sha256 mismatch: %w", ReasonInvalidObjectURL, errors.New("boom"))
		got := CrashIfReason(context.Background(), tagged, ReasonInvalidSandboxAsset)
		if got != tagged {
			t.Errorf("CrashIfReason(tagged, unclaimed) = %v, want the same error back", got)
		}
		if ActorCrashRequested(got) {
			t.Errorf("ActorCrashRequested(%v) = true, want false", got)
		}
	})

	t.Run("untagged error passes through unchanged", func(t *testing.T) {
		plain := errors.New("transient network failure")
		if got := CrashIfReason(context.Background(), plain, ReasonInvalidSandboxAsset); got != plain {
			t.Errorf("CrashIfReason(plain) = %v, want the same error back", got)
		}
	})

	t.Run("nil error returns nil", func(t *testing.T) {
		if got := CrashIfReason(context.Background(), nil, ReasonInvalidSandboxAsset); got != nil {
			t.Errorf("CrashIfReason(nil) = %v, want nil", got)
		}
	})
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
			err:  NewGRPCError(context.Background(), codes.NotFound, ReasonFaileSaveSnapshot, nil, errors.New("boom")),
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

func TestActorCrashRequested(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil error", err: nil, want: false},
		{name: "plain error without status", err: errors.New("boom"), want: false},
		{name: "status without error info", err: status.Error(codes.Unavailable, "transient"), want: false},
		{
			name: "actor crashed metadata",
			err:  NewGRPCError(context.Background(), codes.DataLoss, ReasonInvalidCheckpointResult, ActorCrashedMetadata(), errors.New("boom")),
			want: true,
		},
		{
			name: "no metadata",
			err:  NewGRPCError(context.Background(), codes.DataLoss, ReasonInvalidCheckpointResult, nil, errors.New("boom")),
			want: false,
		},
		{
			name: "metadata without crash key",
			err:  NewGRPCError(context.Background(), codes.DataLoss, ReasonInvalidCheckpointResult, map[string]string{"other": "x"}, errors.New("boom")),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ActorCrashRequested(tt.err); got != tt.want {
				t.Errorf("ActorCrashRequested(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
