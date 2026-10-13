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

// Package apierror holds the errors a substrate server deliberately returns to
// its caller.
//
// An apierror is not a gRPC status: it has no GRPCStatus method, so a status
// received from an upstream service never decides the code a server returns,
// however it is wrapped. To propagate an upstream code, check it at the call
// site and construct the apierror that says what it means to this server's
// caller. Anything that is neither an apierror nor a context error reaches the
// caller as codes.Internal.
//
// There is one constructor per code a server chooses. OK and Unknown have none,
// and neither do Canceled and DeadlineExceeded: return or wrap the context's
// error instead, and Code and FromError map it.
package apierror

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type apiError struct {
	code codes.Code
	err  error
}

func (e *apiError) Error() string { return e.err.Error() }

func (e *apiError) Unwrap() error { return e.err }

func newf(code codes.Code, format string, args ...any) error {
	return &apiError{code: code, err: fmt.Errorf(format, args...)}
}

// InvalidArgument reports a request that is malformed regardless of the
// system's state.
func InvalidArgument(format string, args ...any) error {
	return newf(codes.InvalidArgument, format, args...)
}

// NotFound reports that a resource the request names does not exist.
func NotFound(format string, args ...any) error {
	return newf(codes.NotFound, format, args...)
}

// AlreadyExists reports that a resource the request creates already exists.
func AlreadyExists(format string, args ...any) error {
	return newf(codes.AlreadyExists, format, args...)
}

// FailedPrecondition reports that the system is not in the state the request
// requires. Retrying is pointless until that state changes.
func FailedPrecondition(format string, args ...any) error {
	return newf(codes.FailedPrecondition, format, args...)
}

// Aborted reports a request that lost a concurrency conflict, such as a
// version mismatch. Retrying from a fresh read can succeed.
func Aborted(format string, args ...any) error {
	return newf(codes.Aborted, format, args...)
}

// PermissionDenied reports an authenticated caller that may not perform the
// request.
func PermissionDenied(format string, args ...any) error {
	return newf(codes.PermissionDenied, format, args...)
}

// Unauthenticated reports a caller whose identity could not be established.
func Unauthenticated(format string, args ...any) error {
	return newf(codes.Unauthenticated, format, args...)
}

// ResourceExhausted reports that a quota or capacity limit refused the
// request.
func ResourceExhausted(format string, args ...any) error {
	return newf(codes.ResourceExhausted, format, args...)
}

// Unimplemented reports a request this server does not support.
func Unimplemented(format string, args ...any) error {
	return newf(codes.Unimplemented, format, args...)
}

// Unavailable reports that the request was not acted on, so retrying it, here
// or elsewhere, is safe. Never use it for a failure partway through.
func Unavailable(format string, args ...any) error {
	return newf(codes.Unavailable, format, args...)
}

// DataLoss reports unrecoverable loss or corruption of data.
func DataLoss(format string, args ...any) error {
	return newf(codes.DataLoss, format, args...)
}

// Internal reports a server-side failure with a message chosen for the
// caller. A plain error also reaches the caller as codes.Internal, but with
// whatever text it carries.
func Internal(format string, args ...any) error {
	return newf(codes.Internal, format, args...)
}

// Code returns the code err reaches the caller with, the same as
// FromError(err).Code(): codes.OK for nil, and codes.Internal for an error that
// is neither an apierror nor a context error. It never reads a gRPC status.
func Code(err error) codes.Code {
	s, _ := FromError(err)
	return s.Code()
}

// FromError converts err to the status a server returns for it. The message
// is the apierror's own, without the context added by errors wrapping it. ok
// is false when err is neither an apierror nor a context error; the status is
// then codes.Internal with err's text, which the caller may reword.
func FromError(err error) (s *status.Status, ok bool) {
	if err == nil {
		return nil, true
	}
	var apiErr *apiError
	if errors.As(err, &apiErr) {
		return status.New(apiErr.code, apiErr.Error()), true
	}
	if s := status.FromContextError(err); s.Code() != codes.Unknown {
		return s, true
	}
	return status.New(codes.Internal, err.Error()), false
}
