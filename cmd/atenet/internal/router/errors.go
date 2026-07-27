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

	"github.com/agent-substrate/substrate/internal/resources"
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
func actorNotFoundErr(actorRef resources.ActorRef) error {
	return newReqError(envoy_type.StatusCode_NotFound, "actor %s not found", actorRef)
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

// mapResumeError translates an ActorResumer error into a client-facing
// reqError. It maps gRPC status codes to appropriate HTTP status codes and
// short, human-readable bodies. The original error is preserved via Unwrap
// so callers can still inspect it via errors.Is / errors.As when logging.
//
// Unrecognized errors collapse to 500 with a generic body to avoid leaking
// server-side detail (stack traces, internal IDs) to clients.
func mapResumeError(actorRef resources.ActorRef, err error) error {
	if err == nil {
		return nil
	}

	re := &reqError{cause: err}
	switch status.Code(err) {
	case codes.NotFound:
		re.statusCode = int(envoy_type.StatusCode_NotFound)
		re.msg = fmt.Sprintf("actor %s not found", actorRef)
	case codes.FailedPrecondition:
		// Preserve the gRPC description for FailedPrecondition only: it carries
		// actionable client-facing context (e.g. "no free workers available")
		// and is not security-sensitive.
		re.statusCode = int(envoy_type.StatusCode_ServiceUnavailable)
		re.msg = fmt.Sprintf("actor %s unavailable: %s", actorRef, status.Convert(err).Message())
	case codes.Unavailable:
		re.statusCode = int(envoy_type.StatusCode_ServiceUnavailable)
		re.msg = fmt.Sprintf("actor %s unavailable", actorRef)
	case codes.DeadlineExceeded:
		re.statusCode = int(envoy_type.StatusCode_GatewayTimeout)
		re.msg = fmt.Sprintf("actor %s request timed out", actorRef)
	case codes.PermissionDenied:
		re.statusCode = int(envoy_type.StatusCode_Forbidden)
		re.msg = fmt.Sprintf("actor %s access denied", actorRef)
	case codes.Unauthenticated:
		re.statusCode = int(envoy_type.StatusCode_Unauthorized)
		re.msg = fmt.Sprintf("actor %s authentication required", actorRef)
	case codes.ResourceExhausted:
		re.statusCode = int(envoy_type.StatusCode_TooManyRequests)
		re.msg = fmt.Sprintf("actor %s rate limited", actorRef)
	default:
		re.statusCode = int(envoy_type.StatusCode_InternalServerError)
		re.msg = fmt.Sprintf("error resuming actor %s", actorRef)
	}
	return re
}
