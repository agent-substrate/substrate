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
	"fmt"

	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// newReqError builds a reqError whose body is the formatted message and no
// wrapped cause. Set the cause field directly when one is available.
func newReqError(code envoy_type.StatusCode, format string, args ...any) error {
	return &reqError{
		msg:        fmt.Sprintf(format, args...),
		statusCode: int(code),
	}
}

// actorNotFoundErr returns a 404 reqError identifying the missing actor.
func actorNotFoundErr(actorID string) error {
	return newReqError(envoy_type.StatusCode_NotFound, "actor %q not found", actorID)
}

// invalidHostErr returns a 404 reqError explaining why the request host was
// rejected. The cause is preserved for log inspection via Unwrap.
func invalidHostErr(host string, cause error) error {
	return &reqError{
		msg:        fmt.Sprintf("invalid host %q: %v", host, cause),
		cause:      cause,
		statusCode: int(envoy_type.StatusCode_NotFound),
	}
}

// parkingFullErr returns a 503 reqError signaling that the router's parking lot
// is at capacity, so the request was shed without waiting. Clients should retry.
func parkingFullErr(actorID string) error {
	return newReqError(envoy_type.StatusCode_ServiceUnavailable,
		"actor %q unavailable: router at capacity", actorID)
}

// mapResumeError translates an ActorResumer error into a client-facing
// reqError. It maps gRPC status codes to appropriate HTTP status codes and
// short, human-readable bodies. The original error is preserved via Unwrap
// so callers can still inspect it via errors.Is / errors.As when logging.
//
// Unrecognised errors collapse to 500 with a generic body to avoid leaking
// server-side detail (stack traces, internal IDs) to clients.
func mapResumeError(actorID string, err error) error {
	if err == nil {
		return nil
	}

	re := &reqError{cause: err}
	switch status.Code(err) {
	case codes.NotFound:
		re.statusCode = int(envoy_type.StatusCode_NotFound)
		re.msg = fmt.Sprintf("actor %q not found", actorID)
	case codes.FailedPrecondition:
		// Preserve the gRPC description for FailedPrecondition only: it carries
		// actionable client-facing context (e.g. "no free workers available")
		// and is not security-sensitive.
		re.statusCode = int(envoy_type.StatusCode_ServiceUnavailable)
		re.msg = fmt.Sprintf("actor %q unavailable: %s", actorID, status.Convert(err).Message())
	case codes.Unavailable:
		re.statusCode = int(envoy_type.StatusCode_ServiceUnavailable)
		re.msg = fmt.Sprintf("actor %q unavailable", actorID)
	case codes.DeadlineExceeded:
		re.statusCode = int(envoy_type.StatusCode_GatewayTimeout)
		re.msg = fmt.Sprintf("actor %q request timed out", actorID)
	case codes.PermissionDenied:
		re.statusCode = int(envoy_type.StatusCode_Forbidden)
		re.msg = fmt.Sprintf("actor %q access denied", actorID)
	case codes.Unauthenticated:
		re.statusCode = int(envoy_type.StatusCode_Unauthorized)
		re.msg = fmt.Sprintf("actor %q authentication required", actorID)
	case codes.ResourceExhausted:
		re.statusCode = int(envoy_type.StatusCode_TooManyRequests)
		re.msg = fmt.Sprintf("actor %q rate limited", actorID)
	default:
		re.statusCode = int(envoy_type.StatusCode_InternalServerError)
		re.msg = fmt.Sprintf("error resuming actor %q", actorID)
	}
	return re
}
