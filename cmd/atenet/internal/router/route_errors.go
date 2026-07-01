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

package router

import (
	"fmt"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// invalidHostDenial returns a 404 RouteDenial for a host that cannot be parsed
// as an actor address. The parse error is preserved for log inspection.
func invalidHostDenial(host string, cause error) *RouteDenial {
	return &RouteDenial{
		HTTPStatus:  http.StatusNotFound,
		Message:     fmt.Sprintf("invalid host %q: %v", host, cause),
		Cause:       cause,
		OutcomeCode: "not_found",
	}
}

// mapResumeDenial translates an ActorResumer error into a gateway-neutral
// RouteDenial. gRPC status codes are mapped to HTTP equivalents; the original
// error is preserved via Cause so callers can inspect the full chain.
//
// Unrecognised codes collapse to 500 to avoid leaking server-side detail.
func mapResumeDenial(actorID string, err error) *RouteDenial {
	if err == nil {
		return nil
	}
	d := &RouteDenial{Cause: err}
	switch status.Code(err) {
	case codes.NotFound:
		d.HTTPStatus = http.StatusNotFound
		d.Message = fmt.Sprintf("actor %q not found", actorID)
		d.OutcomeCode = "not_found"
	case codes.FailedPrecondition:
		// Preserve the gRPC description for FailedPrecondition only: it carries
		// actionable client-facing context (e.g. "no free workers available")
		// and is not security-sensitive.
		d.HTTPStatus = http.StatusServiceUnavailable
		d.Message = fmt.Sprintf("actor %q unavailable: %s", actorID, status.Convert(err).Message())
		d.OutcomeCode = "error"
	case codes.Unavailable:
		d.HTTPStatus = http.StatusServiceUnavailable
		d.Message = fmt.Sprintf("actor %q unavailable", actorID)
		d.OutcomeCode = "error"
	case codes.DeadlineExceeded:
		d.HTTPStatus = http.StatusGatewayTimeout
		d.Message = fmt.Sprintf("actor %q request timed out", actorID)
		d.OutcomeCode = "cancelled"
	case codes.PermissionDenied:
		d.HTTPStatus = http.StatusForbidden
		d.Message = fmt.Sprintf("actor %q access denied", actorID)
		d.OutcomeCode = "error"
	case codes.Unauthenticated:
		d.HTTPStatus = http.StatusUnauthorized
		d.Message = fmt.Sprintf("actor %q authentication required", actorID)
		d.OutcomeCode = "error"
	case codes.ResourceExhausted:
		d.HTTPStatus = http.StatusTooManyRequests
		d.Message = fmt.Sprintf("actor %q rate limited", actorID)
		d.OutcomeCode = "error"
	default:
		d.HTTPStatus = http.StatusInternalServerError
		d.Message = fmt.Sprintf("error resuming actor %q", actorID)
		d.OutcomeCode = "error"
	}
	return d
}
