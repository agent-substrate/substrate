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

package apierror

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestConstructors(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
	}{
		{name: "InvalidArgument", err: InvalidArgument("tier %d", 1), wantCode: codes.InvalidArgument},
		{name: "NotFound", err: NotFound("tier %d", 1), wantCode: codes.NotFound},
		{name: "AlreadyExists", err: AlreadyExists("tier %d", 1), wantCode: codes.AlreadyExists},
		{name: "FailedPrecondition", err: FailedPrecondition("tier %d", 1), wantCode: codes.FailedPrecondition},
		{name: "Aborted", err: Aborted("tier %d", 1), wantCode: codes.Aborted},
		{name: "PermissionDenied", err: PermissionDenied("tier %d", 1), wantCode: codes.PermissionDenied},
		{name: "Unauthenticated", err: Unauthenticated("tier %d", 1), wantCode: codes.Unauthenticated},
		{name: "ResourceExhausted", err: ResourceExhausted("tier %d", 1), wantCode: codes.ResourceExhausted},
		{name: "Unimplemented", err: Unimplemented("tier %d", 1), wantCode: codes.Unimplemented},
		{name: "Unavailable", err: Unavailable("tier %d", 1), wantCode: codes.Unavailable},
		{name: "DataLoss", err: DataLoss("tier %d", 1), wantCode: codes.DataLoss},
		{name: "Internal", err: Internal("tier %d", 1), wantCode: codes.Internal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Code(tt.err); got != tt.wantCode {
				t.Errorf("Code() = %v, want %v", got, tt.wantCode)
			}
			if got := tt.err.Error(); got != "tier 1" {
				t.Errorf("Error() = %q, want %q", got, "tier 1")
			}
		})
	}
}

func TestCode(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{
			name: "nil is OK",
			err:  nil,
			want: codes.OK,
		},
		{
			name: "apierror",
			err:  NotFound("tier 1 not found"),
			want: codes.NotFound,
		},
		{
			name: "wrapped apierror keeps its code",
			err:  fmt.Errorf("while reading tier 1: %w", NotFound("tier 1 not found")),
			want: codes.NotFound,
		},
		{
			name: "outermost apierror wins",
			err:  FailedPrecondition("tier 1 is draining: %w", NotFound("tier 2 not found")),
			want: codes.FailedPrecondition,
		},
		{
			name: "apierror wrapping an upstream status keeps its own code",
			err:  Internal("while calling upstream: %w", status.Error(codes.Unavailable, "backend down")),
			want: codes.Internal,
		},
		{
			name: "upstream status is internal",
			err:  status.Error(codes.Unavailable, "backend down"),
			want: codes.Internal,
		},
		{
			name: "wrapped upstream status is internal",
			err:  fmt.Errorf("while calling upstream: %w", status.Error(codes.Unavailable, "backend down")),
			want: codes.Internal,
		},
		{
			name: "plain error is internal",
			err:  errors.New("bad json"),
			want: codes.Internal,
		},
		{
			name: "context canceled",
			err:  context.Canceled,
			want: codes.Canceled,
		},
		{
			name: "wrapped context deadline",
			err:  fmt.Errorf("gave up waiting for the lock: %w", context.DeadlineExceeded),
			want: codes.DeadlineExceeded,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Code(tt.err); got != tt.want {
				t.Errorf("Code() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFromError(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		wantCode    codes.Code
		wantMessage string
		wantOK      bool
	}{
		{
			name:        "apierror",
			err:         NotFound("tier 1 not found"),
			wantCode:    codes.NotFound,
			wantMessage: "tier 1 not found",
			wantOK:      true,
		},
		{
			name:        "wrapped apierror carries only its own message",
			err:         fmt.Errorf("while reading tier 1: %w", NotFound("tier 1 not found")),
			wantCode:    codes.NotFound,
			wantMessage: "tier 1 not found",
			wantOK:      true,
		},
		{
			name:        "apierror message includes its cause",
			err:         Internal("while calling upstream: %w", errors.New("backend down")),
			wantCode:    codes.Internal,
			wantMessage: "while calling upstream: backend down",
			wantOK:      true,
		},
		{
			name:        "wrapped upstream status becomes internal",
			err:         fmt.Errorf("while calling upstream: %w", status.Error(codes.Unavailable, "backend down")),
			wantCode:    codes.Internal,
			wantMessage: "while calling upstream: rpc error: code = Unavailable desc = backend down",
			wantOK:      false,
		},
		{
			name:        "plain error becomes internal",
			err:         errors.New("bad json"),
			wantCode:    codes.Internal,
			wantMessage: "bad json",
			wantOK:      false,
		},
		{
			name:        "wrapped context error",
			err:         fmt.Errorf("gave up waiting for the lock: %w", context.Canceled),
			wantCode:    codes.Canceled,
			wantMessage: "gave up waiting for the lock: context canceled",
			wantOK:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, ok := FromError(tt.err)
			if ok != tt.wantOK {
				t.Errorf("FromError() ok = %v, want %v", ok, tt.wantOK)
			}
			if s.Code() != tt.wantCode {
				t.Errorf("FromError() code = %v, want %v", s.Code(), tt.wantCode)
			}
			if s.Message() != tt.wantMessage {
				t.Errorf("FromError() message = %q, want %q", s.Message(), tt.wantMessage)
			}
		})
	}
}

func TestFromErrorNil(t *testing.T) {
	s, ok := FromError(nil)
	if s != nil || !ok {
		t.Errorf("FromError(nil) = %v, %v, want nil, true", s, ok)
	}
}

func TestCauseIsReachable(t *testing.T) {
	cause := errors.New("backend down")
	err := Internal("while calling upstream: %w", cause)
	if !errors.Is(err, cause) {
		t.Errorf("errors.Is(%v, cause) = false, want true", err)
	}
}

func TestNotAGRPCStatus(t *testing.T) {
	err := fmt.Errorf("while reading tier 1: %w", NotFound("tier 1 not found"))
	if s, ok := status.FromError(err); ok || s.Code() != codes.Unknown {
		t.Errorf("status.FromError() = %v, %v, want Unknown, false", s.Code(), ok)
	}
}

func TestWithCause(t *testing.T) {
	sentinel := errors.New("version mismatch in postgres")
	fmtCause := errors.New("bad input")

	tests := []struct {
		name           string
		err            error
		wantCode       codes.Code
		wantErr        string
		wantStatus     string
		wantTargets    []error
		notWantTargets []error
	}{
		{
			name:           "cause included in Error and omitted from FromError and Unwrap",
			err:            Aborted("concurrent update conflict, please retry").WithCause(sentinel),
			wantCode:       codes.Aborted,
			wantErr:        "concurrent update conflict, please retry: version mismatch in postgres",
			wantStatus:     "concurrent update conflict, please retry",
			notWantTargets: []error{sentinel},
		},
		{
			name:       "nil cause leaves Error unchanged",
			err:        NotFound("actor not found").WithCause(nil),
			wantCode:   codes.NotFound,
			wantErr:    "actor not found",
			wantStatus: "actor not found",
		},
		{
			name:           "formatted %w stays reachable while cause does not leak to errors.Is",
			err:            InvalidArgument("bad request: %w", fmtCause).WithCause(sentinel),
			wantCode:       codes.InvalidArgument,
			wantErr:        "bad request: bad input: version mismatch in postgres",
			wantStatus:     "bad request: bad input",
			wantTargets:    []error{fmtCause},
			notWantTargets: []error{sentinel},
		},
		{
			name:       "outermost apierror code and message win over apierror cause",
			err:        FailedPrecondition("cannot resume").WithCause(NotFound("worker missing")),
			wantCode:   codes.FailedPrecondition,
			wantErr:    "cannot resume: worker missing",
			wantStatus: "cannot resume",
		},
		{
			name:           "wrapped apierror with cause keeps only own message in FromError",
			err:            fmt.Errorf("step Load: %w", Aborted("conflict").WithCause(sentinel)),
			wantCode:       codes.Aborted,
			wantErr:        "step Load: conflict: version mismatch in postgres",
			wantStatus:     "conflict",
			notWantTargets: []error{sentinel},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Code(tt.err); got != tt.wantCode {
				t.Errorf("Code() = %v, want %v", got, tt.wantCode)
			}
			if got := tt.err.Error(); got != tt.wantErr {
				t.Errorf("Error() = %q, want %q", got, tt.wantErr)
			}
			st, ok := FromError(tt.err)
			if !ok {
				t.Fatalf("FromError() ok = false, want true")
			}
			if st.Code() != tt.wantCode {
				t.Errorf("FromError().Code() = %v, want %v", st.Code(), tt.wantCode)
			}
			if st.Message() != tt.wantStatus {
				t.Errorf("FromError().Message() = %q, want %q", st.Message(), tt.wantStatus)
			}
			for _, target := range tt.wantTargets {
				if !errors.Is(tt.err, target) {
					t.Errorf("errors.Is(%v, %v) = false, want true", tt.err, target)
				}
			}
			for _, target := range tt.notWantTargets {
				if errors.Is(tt.err, target) {
					t.Errorf("errors.Is(%v, %v) = true, want false", tt.err, target)
				}
			}
		})
	}
}

func TestWithCauseDoesNotMutateReceiver(t *testing.T) {
	base := NotFound("actor not found")
	withCause := base.WithCause(errors.New("db row missing"))

	if got := base.Error(); got != "actor not found" {
		t.Errorf("base.Error() = %q, want %q", got, "actor not found")
	}
	if got := withCause.Error(); got != "actor not found: db row missing" {
		t.Errorf("withCause.Error() = %q, want %q", got, "actor not found: db row missing")
	}
}

func TestTypedNilError(t *testing.T) {
	var typedNil *Error
	var err error = typedNil

	st, ok := FromError(err)
	if ok || st.Code() != codes.Internal {
		t.Errorf("FromError((*Error)(nil)) = (%v, %v), want (Internal, false)", st.Code(), ok)
	}
	if got := Code(err); got != codes.Internal {
		t.Errorf("Code((*Error)(nil)) = %v, want Internal", got)
	}
	if got := typedNil.WithCause(errors.New("ignored")); got != nil {
		t.Errorf("(*Error)(nil).WithCause() = %v, want nil", got)
	}
}
