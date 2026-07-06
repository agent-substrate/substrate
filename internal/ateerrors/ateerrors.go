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
	"log/slog"
	"slices"

	epb "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// errorDomain is the AIP-193 ErrorInfo.errorDomain (https://google.aip.dev/193) stamped
// into every error built by NewGRPCError, identifying Agent Substrate as the
// source service.
const errorDomain = "substrate.dev"

// Reason is the AIP-193 ErrorInfo.Reason: a bounded, UPPER_SNAKE_CASE enum of
// failure causes the control plane can classify on. A Reason is also an error:
// source layers tag failures with fmt.Errorf("%w: ...", ReasonX, err), and
// each RPC boundary claims the Reasons it treats as terminal (CrashIfReason);
// untagged errors stay retriable.
type Reason string

// Error makes a Reason wrappable with %w and matchable with errors.Is/As.
func (r Reason) Error() string { return string(r) }

const (
	ReasonTerminalFileSystemError Reason = "TERMINAL_FILE_SYSTEM_ERROR"
	ReasonInvalidSandboxAsset     Reason = "INVALID_SANDBOX_ASSET"
	ReasonInvalidCheckpointResult Reason = "INVALID_CHECKPOINT_RESULT"
	ReasonFaileSaveSnapshot       Reason = "FAILED_SAVE_SNAPSHOT"
	ReasonInvalidObjectURL        Reason = "INVALID_OBJECT_URL"
	ReasonFailedGetExternalObject Reason = "FAILED_GET_EXTERNAL_OBJECT"
)

// MetadataKeyActorCrashed marks (in ErrorInfo.Metadata) a failure that requires
// the control plane to crash the actor.
const MetadataKeyActorCrashed = "actorCrashed"

// ActorCrashedMetadata returns the AIP-193 metadata marking a failure as
// requiring the actor to be crashed. The control plane reads it via
// ActorCrashRequested.
func ActorCrashedMetadata() map[string]string {
	return map[string]string{MetadataKeyActorCrashed: "true"}
}

// NewGRPCError builds an internal gRPC status error per AIP-193
// (https://google.aip.dev/193#status-message), with a google.rpc.ErrorInfo detail
// carrying the given Reason ("UNSET" when empty).
// metadata carries additional structured directives such as ActorCrashedMetadata(),
// which the control plane reads via ActorCrashRequested to decide whether to crash
// the actor.
func NewGRPCError(ctx context.Context, grpcCode codes.Code, reason Reason, metadata map[string]string, err error) error {
	// Validate the input parameters.
	if err == nil || grpcCode == codes.OK {
		return fmt.Errorf("cannot use NewGRPCError with OK error code or a nil err grpcCode=%v, err=%w. Return nil instead", grpcCode, err)
	}
	if reason == "" {
		reason = "UNSET"
	}
	st, derr := status.New(grpcCode, err.Error()).WithDetails(
		&epb.ErrorInfo{
			Domain:   errorDomain,
			Reason:   string(reason),
			Metadata: metadata,
		},
	)
	if derr != nil {
		// WithDetails on *epb.ErrorInfo should never fail; but if it ever does, the
		// reason and metadata are lost and the control plane will misclassify the
		// failure (e.g. a real crash read as a transient error). Log loudly for
		// debugging purpose.
		slog.ErrorContext(ctx, "ateerrors: failed to attach ErrorInfo to gRPC status; adding Reason/metadata to the error message instead",
			"err", derr, "reason", reason, "metadata", metadata, "code", grpcCode)
		return status.Error(grpcCode, fmt.Errorf("reason:%s metadata:%v, error %w", reason, metadata, err).Error())
	}
	return st.Err()
}

// CrashIfReason sets the err to a DataLoss gRPC status with the actor-crash
// directive iff its chain carries one of the given Reasons; any other error is
// returned unchanged. Claiming is per call site: the same tagged failure may
// crash the actor in one RPC and stay retriable in another.
func CrashIfReason(ctx context.Context, err error, reasons ...Reason) error {
	r, ok := errors.AsType[Reason](err)
	if !ok || !slices.Contains(reasons, r) {
		return err
	}
	return NewGRPCError(ctx, codes.DataLoss, r, ActorCrashedMetadata(), err)
}

// ActorCrashRequested reports whether any ErrorInfo carried by err has the
// actorCrashed=true directive, i.e. the failure requires the control plane to
// crash the actor.
func ActorCrashRequested(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	for _, d := range st.Details() {
		if info, ok := d.(*epb.ErrorInfo); ok {
			if info.GetMetadata()[MetadataKeyActorCrashed] == "true" {
				return true
			}
		}
	}
	return false
}
