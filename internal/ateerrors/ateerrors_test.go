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
	"strings"
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

// TestMarkRetriable verifies the marker is a plain in-process tag: it keeps
// the cause in the unwrap chain, carries no gRPC status of its own (the
// crash boundary encodes it onto the wire), and never invents an error from
// nil.
func TestMarkRetriable(t *testing.T) {
	if got := MarkRetriable(nil); got != nil {
		t.Errorf("MarkRetriable(nil) = %v, want nil", got)
	}
	cause := errors.New("connection reset by peer")
	marked := MarkRetriable(cause)
	if !IsRetriableError(marked) {
		t.Errorf("IsRetriableError(MarkRetriable(err)) = false, want true")
	}
	if got, want := marked.Error(), cause.Error(); got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
	if !errors.Is(marked, cause) {
		t.Errorf("errors.Is(MarkRetriable(err), err) = false, want true")
	}
	if !IsRetriableError(fmt.Errorf("while fetching manifest: %w", marked)) {
		t.Errorf("IsRetriableError(wrapped) = false, want true")
	}
	if IsRetriableError(cause) {
		t.Errorf("IsRetriableError(unclassified) = true, want false")
	}
	if IsRetriableError(status.Error(codes.Unavailable, "raw transport failure")) {
		t.Errorf("IsRetriableError(raw Unavailable status) = true, want false: only the explicit marker counts")
	}
	// The marker deliberately carries no status of its own: an error that
	// skips the boundary surfaces as Unknown, never as a silent retriable.
	if _, ok := status.FromError(marked); ok {
		t.Errorf("bare marker is a status error; it must stay a plain in-process tag the boundary encodes")
	}
}

// TestCrashUnlessRetriable verifies the crash boundary rule: every failure
// escalates to a status carrying the actor-crash directive and the Reason
// from the chain (ReasonUnknown when untagged) — a fresh DataLoss status for
// plain failures, the original status kept (code, message, ErrorInfo) for
// status-bearing ones. Failures marked retriable (MarkRetriable) encode as
// an Unavailable status carrying the tagged Reason. Only context
// cancellation/deadline — plain or as a gRPC status — and request-level
// rejection codes pass through unchanged. Raw transient gRPC codes (a
// transport failure nobody classified) crash.
func TestCrashUnlessRetriable(t *testing.T) {
	passThrough := []struct {
		name string
		err  error
	}{
		{name: "context canceled", err: fmt.Errorf("while copying: %w", context.Canceled)},
		{name: "context deadline", err: fmt.Errorf("while copying: %w", context.DeadlineExceeded)},
		{name: "grpc canceled", err: status.Error(codes.Canceled, "caller went away")},
		{name: "grpc deadline exceeded", err: status.Error(codes.DeadlineExceeded, "request deadline hit")},
		{name: "invalid argument rejection", err: status.Error(codes.InvalidArgument, "bad spec")},
		{name: "failed precondition rejection", err: status.Error(codes.FailedPrecondition, "scope mismatch")},
	}
	for _, tt := range passThrough {
		t.Run(tt.name+" passes through unchanged", func(t *testing.T) {
			got := CrashUnlessRetriable(context.Background(), tt.err)
			if got != tt.err {
				t.Errorf("CrashUnlessRetriable(%v) = %v, want the same error back", tt.err, got)
			}
			if ActorCrashRequested(got) {
				t.Errorf("ActorCrashRequested(%v) = true, want false", got)
			}
		})
	}

	crash := []struct {
		name       string
		err        error
		wantReason Reason
		wantCode   codes.Code
	}{
		{
			name:       "tagged reason escalates with that reason",
			err:        fmt.Errorf("%w: while parsing manifest: %w", ReasonInvalidSandboxAsset, errors.New("bad json")),
			wantReason: ReasonInvalidSandboxAsset,
			wantCode:   codes.DataLoss,
		},
		{
			name:       "untagged error escalates as UNKNOWN",
			err:        errors.New("some failure nobody classified"),
			wantReason: ReasonUnknown,
			wantCode:   codes.DataLoss,
		},
		{
			name:       "grpc internal escalates as UNKNOWN keeping its code",
			err:        status.Error(codes.Internal, "ateom blew up"),
			wantReason: ReasonUnknown,
			wantCode:   codes.Internal,
		},
		{
			name:       "grpc not found escalates as UNKNOWN keeping its code",
			err:        status.Error(codes.NotFound, "workload not on ateom"),
			wantReason: ReasonUnknown,
			wantCode:   codes.NotFound,
		},
		{
			name:       "raw grpc unavailable escalates as UNKNOWN keeping its code",
			err:        status.Error(codes.Unavailable, "ateom unreachable"),
			wantReason: ReasonUnknown,
			wantCode:   codes.Unavailable,
		},
		{
			name:       "wrapped raw grpc unavailable escalates as UNKNOWN keeping its code",
			err:        fmt.Errorf("while calling ateom.CheckpointWorkload: %w", status.Error(codes.Unavailable, "unreachable")),
			wantReason: ReasonUnknown,
			wantCode:   codes.Unavailable,
		},
		{
			name:       "raw grpc aborted escalates as UNKNOWN keeping its code",
			err:        status.Error(codes.Aborted, "checkpoint raced"),
			wantReason: ReasonUnknown,
			wantCode:   codes.Aborted,
		},
		{
			name:       "grpc unimplemented escalates as UNKNOWN keeping its code",
			err:        status.Error(codes.Unimplemented, "gVisor split checkpoint"),
			wantReason: ReasonUnknown,
			wantCode:   codes.Unimplemented,
		},
		{
			name:       "status with a downstream ErrorInfo keeps its reason and code",
			err:        NewGRPCError(context.Background(), codes.Internal, ReasonFailedGetExternalObject, nil, errors.New("blob fetch failed")),
			wantReason: ReasonFailedGetExternalObject,
			wantCode:   codes.Internal,
		},
	}
	for _, tt := range crash {
		t.Run(tt.name, func(t *testing.T) {
			err := CrashUnlessRetriable(context.Background(), tt.err)
			st, ok := status.FromError(err)
			if !ok {
				t.Fatalf("CrashUnlessRetriable(%v) = %v, want a gRPC status error", tt.err, err)
			}
			if got, want := st.Code(), tt.wantCode; got != want {
				t.Errorf("status code = %v, want %v", got, want)
			}
			if got := errorReasonsFromStatus(err); !slices.Contains(got, string(tt.wantReason)) {
				t.Errorf("errorReasonsFromStatus() = %q, want it to contain %q", got, tt.wantReason)
			}
			if !ActorCrashRequested(err) {
				t.Errorf("ActorCrashRequested(%v) = false, want true", err)
			}
		})
	}

	t.Run("marked retriable encodes as Unavailable with the tagged reason", func(t *testing.T) {
		err := fmt.Errorf("while downloading gs://bucket/assets/runsc: %w",
			MarkRetriable(fmt.Errorf("%w: connection reset by peer", ReasonTransientObjectStorage)))
		got := CrashUnlessRetriable(context.Background(), err)
		st, ok := status.FromError(got)
		if !ok {
			t.Fatalf("CrashUnlessRetriable(%v) = %v, want a gRPC status error", err, got)
		}
		if code := st.Code(); code != codes.Unavailable {
			t.Errorf("status code = %v, want Unavailable", code)
		}
		if ActorCrashRequested(got) {
			t.Errorf("ActorCrashRequested(%v) = true, want false", got)
		}
		// The boundary encodes from the full wrapped error, so context above
		// the marker survives onto the wire.
		if want := "while downloading gs://bucket/assets/runsc"; !strings.Contains(st.Message(), want) {
			t.Errorf("status message %q lost the outer wrap %q", st.Message(), want)
		}
		if reasons := errorReasonsFromStatus(got); !slices.Contains(reasons, string(ReasonTransientObjectStorage)) {
			t.Errorf("errorReasonsFromStatus() = %q, want it to contain %q", reasons, ReasonTransientObjectStorage)
		}
	})

	t.Run("marked retriable without a fact tag encodes as UNSET", func(t *testing.T) {
		got := CrashUnlessRetriable(context.Background(), MarkRetriable(errors.New("dial tcp: connection refused")))
		if code := status.Code(got); code != codes.Unavailable {
			t.Errorf("status code = %v, want Unavailable", code)
		}
		if reasons := errorReasonsFromStatus(got); !slices.Contains(reasons, "UNSET") {
			t.Errorf("errorReasonsFromStatus() = %q, want it to contain UNSET", reasons)
		}
	})

	t.Run("call-site crash directive beats the context pass-through", func(t *testing.T) {
		// A point-of-no-return call site builds the crash directive directly
		// with NewGRPCError: it must survive even when the cause chain matches
		// context.DeadlineExceeded (an http.Client timeout does, with the
		// RPC's own context still alive).
		err := NewGRPCError(context.Background(), codes.DataLoss, ReasonFaileSaveSnapshot,
			ActorCrashedMetadata(), fmt.Errorf("while uploading external snapshot: %w", context.DeadlineExceeded))
		got := CrashUnlessRetriable(context.Background(), err)
		if got != err {
			t.Errorf("CrashUnlessRetriable(%v) = %v, want the crash error back unchanged", err, got)
		}
		if !ActorCrashRequested(got) {
			t.Errorf("ActorCrashRequested(%v) = false, want true", got)
		}
		if code := status.Code(got); code != codes.DataLoss {
			t.Errorf("status code = %v, want DataLoss", code)
		}
		if reasons := errorReasonsFromStatus(got); !slices.Contains(reasons, string(ReasonFaileSaveSnapshot)) {
			t.Errorf("errorReasonsFromStatus() = %q, want it to contain %q", reasons, ReasonFaileSaveSnapshot)
		}
	})

	t.Run("downstream classification wins over the appended directive", func(t *testing.T) {
		orig := NewGRPCError(context.Background(), codes.Internal, ReasonFailedGetExternalObject, nil, errors.New("blob fetch failed"))
		got := CrashUnlessRetriable(context.Background(), orig)
		st, ok := status.FromError(got)
		if !ok {
			t.Fatal("CrashUnlessRetriable(status error) is not a gRPC status error")
		}
		if got, want := st.Message(), "blob fetch failed"; got != want {
			t.Errorf("status message = %q, want %q", got, want)
		}
		// The downstream ErrorInfo comes first in the details, so its reason
		// wins over the ReasonUnknown carried by the appended crash directive.
		if gotReason, want := ExtractReason(got), string(ReasonFailedGetExternalObject); gotReason != want {
			t.Errorf("ExtractReason() = %q, want %q", gotReason, want)
		}
	})

	t.Run("already crash-marked error passes through unchanged", func(t *testing.T) {
		marked := NewGRPCError(context.Background(), codes.DataLoss, ReasonFaileSaveSnapshot, ActorCrashedMetadata(), errors.New("upload failed"))
		if got := CrashUnlessRetriable(context.Background(), marked); got != marked {
			t.Errorf("CrashUnlessRetriable(crash-marked) = %v, want the same error back", got)
		}
	})

	t.Run("nil error returns nil", func(t *testing.T) {
		if got := CrashUnlessRetriable(context.Background(), nil); got != nil {
			t.Errorf("CrashUnlessRetriable(nil) = %v, want nil", got)
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

func TestExtractReason_EnforcesAllowedEnumValuesOnly(t *testing.T) {
	t.Run("valid enum reason returned", func(t *testing.T) {
		err := NewGRPCError(context.Background(), codes.DataLoss, ReasonFaileSaveSnapshot, nil, errors.New("boom"))
		if got := ExtractReason(err); got != "FAILED_SAVE_SNAPSHOT" {
			t.Errorf("ExtractReason(%v) = %q, want %q", err, got, "FAILED_SAVE_SNAPSHOT")
		}
	})

	t.Run("unlisted dynamic reason rejected to prevent metric high cardinality", func(t *testing.T) {
		err := NewGRPCError(context.Background(), codes.DataLoss, Reason("UNLISTED_DYNAMIC_ERROR_STRING"), nil, errors.New("boom"))
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
		err := NewGRPCError(context.Background(), codes.DataLoss, r, nil, errors.New("boom"))
		if got := ExtractReason(err); got != string(r) {
			t.Errorf("ExtractReason for %q = %q, want %q", r, got, r)
		}
	}
}
