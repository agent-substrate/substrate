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

package ingress

import (
	"context"
	"errors"
	"fmt"

	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
	"github.com/agent-substrate/substrate/internal/resources"
)

// actorNotFoundErr returns a 404 denial identifying the missing actor.
func actorNotFoundErr(actorRef resources.ActorRef) error {
	return extproc.NewReqError(envoy_type.StatusCode_NotFound, "actor %s not found", actorRef)
}

// invalidHostErr returns a 404 denial explaining why the request host was
// rejected. The cause is preserved for log inspection via Unwrap.
func invalidHostErr(host string, cause error) error {
	return extproc.WrapReqError(envoy_type.StatusCode_NotFound, cause, "invalid host %q: %v", host, cause)
}

// statusDescription returns the gRPC status description of err, unwrapping
// any wrapper (e.g. budgetExhaustedError) first. status.Convert on a wrapping
// error replaces the description with the wrapper's full "rpc error: ..."
// string; going through the unwrapped status keeps client-facing bodies clean.
func statusDescription(err error) string {
	type grpcStatus interface{ GRPCStatus() *status.Status }
	var gs grpcStatus
	if errors.As(err, &gs) {
		return gs.GRPCStatus().Message()
	}
	return status.Convert(err).Message()
}

// parkingFullErr returns a 503 denial signaling that the router's parking lot
// is at capacity, so the request was shed without waiting. Clients should retry.
func parkingFullErr(actorID string) error {
	return extproc.NewReqError(envoy_type.StatusCode_ServiceUnavailable,
		"actor %q unavailable: router at capacity", actorID)
}

// mapResumeError translates an ActorResumer error into a client-facing
// denial. It maps gRPC status codes to appropriate HTTP status codes and
// short, human-readable bodies. The original error is preserved via Unwrap
// so callers can still inspect it via errors.Is / errors.As when logging.
//
// Unrecognized errors collapse to 500 with a generic body to avoid leaking
// server-side detail (stack traces, internal IDs) to clients.
func mapResumeError(actorRef resources.ActorRef, err error) error {
	if err == nil {
		return nil
	}

	re := &extproc.ReqError{Cause: err}

	// Bare context sentinels reach here when the request's own context ends
	// (client disconnect or stream deadline) — status.Code would classify them
	// Unknown and fall through to 500. Map them explicitly so logs and the
	// route metrics agree with the parking outcome. In both cases the stream is
	// already dead, so the code is observability-only; Envoy's StatusCode enum
	// has no 499 ("client closed request"), so 408 is the nearest defined code.
	if errors.Is(err, context.Canceled) {
		re.StatusCode = int(envoy_type.StatusCode_RequestTimeout)
		re.Msg = fmt.Sprintf("request for actor %s canceled by client", actorRef)
		return re
	}
	if errors.Is(err, context.DeadlineExceeded) {
		re.StatusCode = int(envoy_type.StatusCode_GatewayTimeout)
		re.Msg = fmt.Sprintf("actor %s request timed out", actorRef)
		return re
	}

	switch status.Code(err) {
	case codes.NotFound:
		re.StatusCode = int(envoy_type.StatusCode_NotFound)
		re.Msg = fmt.Sprintf("actor %s not found", actorRef)
	case codes.FailedPrecondition:
		// Preserve the gRPC description for FailedPrecondition and Aborted:
		// they carry actionable client-facing context (e.g. "no free workers
		// available", "another operation is in progress for this actor") and
		// are not security-sensitive.
		re.StatusCode = int(envoy_type.StatusCode_ServiceUnavailable)
		re.Msg = fmt.Sprintf("actor %s unavailable: %s", actorRef, statusDescription(err))
	case codes.Aborted:
		// A concurrency conflict that outlived its retries (e.g. a park budget
		// spent entirely on Aborted). Retryable by the client, hence 503.
		re.StatusCode = int(envoy_type.StatusCode_ServiceUnavailable)
		re.Msg = fmt.Sprintf("actor %s unavailable: %s", actorRef, statusDescription(err))
	case codes.Unavailable:
		re.StatusCode = int(envoy_type.StatusCode_ServiceUnavailable)
		re.Msg = fmt.Sprintf("actor %s unavailable", actorRef)
	case codes.DeadlineExceeded:
		re.StatusCode = int(envoy_type.StatusCode_GatewayTimeout)
		re.Msg = fmt.Sprintf("actor %s request timed out", actorRef)
	case codes.PermissionDenied:
		re.StatusCode = int(envoy_type.StatusCode_Forbidden)
		re.Msg = fmt.Sprintf("actor %s access denied", actorRef)
	case codes.Unauthenticated:
		re.StatusCode = int(envoy_type.StatusCode_Unauthorized)
		re.Msg = fmt.Sprintf("actor %s authentication required", actorRef)
	case codes.ResourceExhausted:
		re.StatusCode = int(envoy_type.StatusCode_TooManyRequests)
		re.Msg = fmt.Sprintf("actor %s rate limited", actorRef)
	default:
		re.StatusCode = int(envoy_type.StatusCode_InternalServerError)
		re.Msg = fmt.Sprintf("error resuming actor %s", actorRef)
	}
	return re
}
