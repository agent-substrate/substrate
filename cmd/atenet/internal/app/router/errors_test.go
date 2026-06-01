//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package router

import (
	"errors"
	"testing"

	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewReqError(t *testing.T) {
	t.Parallel()

	err := newReqError(envoy_type.StatusCode_BadRequest, "actor %q is %s", "abc", "bad")
	if err == nil {
		t.Fatal("newReqError returned nil")
	}
	var reqErr *reqError
	if !errors.As(err, &reqErr) {
		t.Fatalf("errors.As(*reqError) = false, want true; err type = %T", err)
	}
	if reqErr.statusCode != int(envoy_type.StatusCode_BadRequest) {
		t.Errorf("statusCode = %d, want %d", reqErr.statusCode, envoy_type.StatusCode_BadRequest)
	}
	if got, want := err.Error(), `actor "abc" is bad`; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestActorNotFoundErr(t *testing.T) {
	t.Parallel()

	err := actorNotFoundErr("ctr6")
	var reqErr *reqError
	if !errors.As(err, &reqErr) {
		t.Fatalf("errors.As(*reqError) = false, want true; err type = %T", err)
	}
	if reqErr.statusCode != int(envoy_type.StatusCode_NotFound) {
		t.Errorf("statusCode = %d, want %d", reqErr.statusCode, envoy_type.StatusCode_NotFound)
	}
	if got, want := err.Error(), `actor "ctr6" not found`; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestInvalidHostErr(t *testing.T) {
	t.Parallel()

	cause := errors.New("missing suffix")
	err := invalidHostErr("foo.example.com", cause)

	var reqErr *reqError
	if !errors.As(err, &reqErr) {
		t.Fatalf("errors.As(*reqError) = false, want true; err type = %T", err)
	}
	if reqErr.statusCode != int(envoy_type.StatusCode_NotFound) {
		t.Errorf("statusCode = %d, want %d", reqErr.statusCode, envoy_type.StatusCode_NotFound)
	}
	if got, want := err.Error(), `invalid host "foo.example.com": missing suffix`; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
	if !errors.Is(err, cause) {
		t.Errorf("errors.Is(err, cause) = false, want true (cause should be wrapped for logging)")
	}
}

func TestMapResumeError(t *testing.T) {
	t.Parallel()

	const actorID = "ctr6"

	tests := []struct {
		name     string
		err      error
		wantCode envoy_type.StatusCode
		wantBody string
	}{
		{
			name:     "NotFound maps to 404",
			err:      status.Error(codes.NotFound, "actor not found"),
			wantCode: envoy_type.StatusCode_NotFound,
			wantBody: `actor "ctr6" not found`,
		},
		{
			name:     "FailedPrecondition maps to 503 and preserves desc",
			err:      status.Error(codes.FailedPrecondition, "no free workers available"),
			wantCode: envoy_type.StatusCode_ServiceUnavailable,
			wantBody: `actor "ctr6" unavailable: no free workers available`,
		},
		{
			name:     "Unavailable maps to 503",
			err:      status.Error(codes.Unavailable, "control-plane down"),
			wantCode: envoy_type.StatusCode_ServiceUnavailable,
			wantBody: `actor "ctr6" unavailable`,
		},
		{
			name:     "DeadlineExceeded maps to 504",
			err:      status.Error(codes.DeadlineExceeded, "context deadline exceeded"),
			wantCode: envoy_type.StatusCode_GatewayTimeout,
			wantBody: `actor "ctr6" request timed out`,
		},
		{
			name:     "PermissionDenied maps to 403",
			err:      status.Error(codes.PermissionDenied, "denied"),
			wantCode: envoy_type.StatusCode_Forbidden,
			wantBody: `actor "ctr6" access denied`,
		},
		{
			name:     "Unauthenticated maps to 401",
			err:      status.Error(codes.Unauthenticated, "no creds"),
			wantCode: envoy_type.StatusCode_Unauthorized,
			wantBody: `actor "ctr6" authentication required`,
		},
		{
			name:     "ResourceExhausted maps to 429",
			err:      status.Error(codes.ResourceExhausted, "quota"),
			wantCode: envoy_type.StatusCode_TooManyRequests,
			wantBody: `actor "ctr6" rate limited`,
		},
		{
			name:     "unknown gRPC code maps to 500 without leaking desc",
			err:      status.Error(codes.Internal, "stack trace: foo bar"),
			wantCode: envoy_type.StatusCode_InternalServerError,
			wantBody: `error resuming actor "ctr6"`,
		},
		{
			name:     "non-gRPC error maps to 500 without leaking message",
			err:      errors.New("raw error with secret"),
			wantCode: envoy_type.StatusCode_InternalServerError,
			wantBody: `error resuming actor "ctr6"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := mapResumeError(actorID, tc.err)
			if got == nil {
				t.Fatal("mapResumeError returned nil")
			}
			var reqErr *reqError
			if !errors.As(got, &reqErr) {
				t.Fatalf("errors.As(*reqError) = false, want true; err type = %T", got)
			}
			if reqErr.statusCode != int(tc.wantCode) {
				t.Errorf("statusCode = %d, want %d", reqErr.statusCode, tc.wantCode)
			}
			if got.Error() != tc.wantBody {
				t.Errorf("Error() = %q, want %q", got.Error(), tc.wantBody)
			}
			if !errors.Is(got, tc.err) {
				t.Errorf("errors.Is(result, original) = false, want true (original must be preserved for logs)")
			}
		})
	}
}

func TestMapResumeError_NilError(t *testing.T) {
	t.Parallel()

	// Guard against accidental nil-error calls. Returning nil keeps the
	// happy path explicit at callsites instead of constructing a bogus 500.
	if got := mapResumeError("ctr6", nil); got != nil {
		t.Errorf("mapResumeError(_, nil) = %v, want nil", got)
	}
}

// Ensures mapResumeError result satisfies the reqError contract so the
// existing handleRequestHeaders branch (errors.As(err, &reqErr)) keeps working.
func TestMapResumeError_IsReqError(t *testing.T) {
	t.Parallel()

	err := mapResumeError("x", status.Error(codes.NotFound, "x"))
	var reqErr *reqError
	if !errors.As(err, &reqErr) {
		t.Fatalf("errors.As(*reqError) = false, want true; err type = %T", err)
	}
}
