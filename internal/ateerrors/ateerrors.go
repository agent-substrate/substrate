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
// source layers tag failures with fmt.Errorf("%w: ...", ReasonX, err). RPC
// boundaries crash the actor on every failure not explicitly marked retriable
// (CrashUnlessRetriable).
type Reason string

// Error makes a Reason wrappable with %w and matchable with errors.Is/As.
func (r Reason) Error() string { return string(r) }

// NOTE: When adding a Reason constant below, also add it to AllReasons.
const (
	ReasonTerminalFileSystemError Reason = "TERMINAL_FILE_SYSTEM_ERROR"
	ReasonInvalidSandboxAsset     Reason = "INVALID_SANDBOX_ASSET"
	ReasonInvalidCheckpointResult Reason = "INVALID_CHECKPOINT_RESULT"
	ReasonFaileSaveSnapshot       Reason = "FAILED_SAVE_SNAPSHOT"
	ReasonInvalidObjectURL        Reason = "INVALID_OBJECT_URL"
	ReasonFailedGetExternalObject Reason = "FAILED_GET_EXTERNAL_OBJECT"
	// ReasonInvalidContainerConfig marks a container whose configuration cannot
	// produce a runnable process (e.g. the resolved argv is empty because the
	// image defines no ENTRYPOINT/CMD and the ActorTemplate sets no command/args).
	ReasonInvalidContainerConfig Reason = "INVALID_CONTAINER_CONFIG"

	// ReasonLocalSnapshotGone marks a paused actor whose local snapshot is
	// missing from the node it was recorded on and absent from object storage:
	// its state is unrecoverable.
	ReasonLocalSnapshotGone Reason = "LOCAL_SNAPSHOT_GONE"

	// Transient failures: the backend failed in a way its client library
	// considers retryable (5xx, throttling, connection trouble).
	ReasonTransientObjectStorage Reason = "TRANSIENT_OBJECT_STORAGE"
	ReasonTransientImageRegistry Reason = "TRANSIENT_IMAGE_REGISTRY"

	// Control-plane failure reasons for ate.actor.crashes metric.
	ReasonCorruptedAssignment Reason = "CORRUPTED_ASSIGNMENT"
	ReasonWorkerReassigned    Reason = "WORKER_REASSIGNED"
	ReasonWorkerPodGone       Reason = "WORKER_POD_GONE"
	ReasonUnknown             Reason = "UNKNOWN"
)

// AllReasons contains all valid Reason constants for validation. Keep in sync with const block above.
var AllReasons = []Reason{
	ReasonTerminalFileSystemError,
	ReasonInvalidSandboxAsset,
	ReasonInvalidCheckpointResult,
	ReasonFaileSaveSnapshot,
	ReasonInvalidObjectURL,
	ReasonFailedGetExternalObject,
	ReasonInvalidContainerConfig,
	ReasonLocalSnapshotGone,
	ReasonTransientObjectStorage,
	ReasonTransientImageRegistry,
	ReasonCorruptedAssignment,
	ReasonWorkerReassigned,
	ReasonWorkerPodGone,
	ReasonUnknown,
}

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

// retriableError marks a failure a handler call site decided to keep
// retriable.
type retriableError struct {
	cause error
}

func (e *retriableError) Error() string { return e.cause.Error() }

func (e *retriableError) Unwrap() error { return e.cause }

func MarkRetriable(err error) error {
	if err == nil {
		return nil
	}
	return &retriableError{cause: err}
}

// IsRetriableError reports whether err was explicitly marked retriable with
// MarkRetriable.
func IsRetriableError(err error) bool {
	_, ok := errors.AsType[*retriableError](err)
	return ok
}

// CrashUnlessRetriable will append ActorCrashedMetadata unless the
// error was marked as retriable.
func CrashUnlessRetriable(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ActorCrashRequested(err) {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if IsRetriableError(err) {
		r, ok := errors.AsType[Reason](err)
		if !ok {
			r = "UNSET"
		}
		return NewGRPCError(ctx, codes.Unavailable, r, nil, err)
	}
	r, ok := errors.AsType[Reason](err)
	if !ok {
		r = ReasonUnknown
	}
	var grpcErr interface{ GRPCStatus() *status.Status }
	if errors.As(err, &grpcErr) {
		st := grpcErr.GRPCStatus()
		switch st.Code() {
		case codes.Canceled, codes.DeadlineExceeded,
			codes.InvalidArgument, codes.FailedPrecondition:
			return err
		}
		if crashed, derr := st.WithDetails(&epb.ErrorInfo{
			Domain:   errorDomain,
			Reason:   string(r),
			Metadata: ActorCrashedMetadata(),
		}); derr == nil {
			return crashed.Err()
		}
		// WithDetails on an ErrorInfo should never fail; fall through so the
		// crash directive is never lost.
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

// IsValidReason reports whether a string matches a known ateerrors.Reason enum.
func IsValidReason(s string) bool {
	return slices.Contains(AllReasons, Reason(s))
}

// ExtractReason returns the validated enum reason string from an error's AIP-193 ErrorInfo detail
// or wrapped ateerrors.Reason, or empty string if unclassified.
func ExtractReason(err error) string {
	if err == nil {
		return ""
	}
	var r Reason
	if errors.As(err, &r) && IsValidReason(string(r)) {
		return string(r)
	}
	st, ok := status.FromError(err)
	if ok {
		for _, d := range st.Details() {
			if info, ok := d.(*epb.ErrorInfo); ok {
				if rStr := info.GetReason(); rStr != "" && IsValidReason(rStr) {
					return rStr
				}
			}
		}
	}
	return ""
}
