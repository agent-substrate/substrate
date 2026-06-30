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
	"errors"
	"fmt"
	"log/slog"

	epb "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// errorDomain is the ErrorInfo.Domain(https://google.aip.dev/193)
// for all internal substrate errors.
const errorDomain = "substrate.dev"

var (
	ErrReasonCrashActor = errors.New("CRASH_ACTOR")
)

// NewGRPCError builds an internal gRPC status error per AIP-193
// (https://google.aip.dev/193#status-message), with google.rpc.ErrorInfo details
// The control plane reads the ErrorInfo.Reason via ErrorReasonsFromStatus
// to classify the failure.
func NewGRPCError(grpcCode codes.Code, sentinel, err error) error {
	// Validate the input parameters.
	if sentinel == nil || err == nil || grpcCode == codes.OK {
		return fmt.Errorf("cannot use NewGRPCError with OK error code, or a nil err or sentinel. sentinel=%w, err=%w, grpcCode=%v. Return nil instead", sentinel, err, grpcCode)
	}
	// Construct the grpc Error with details.
	st, derr := status.New(grpcCode, err.Error()).WithDetails(
		&epb.ErrorInfo{
			Domain: errorDomain,
			Reason: sentinel.Error(),
		},
	)
	if derr != nil {
		// WithDetails on *epb.ErrorInfo should never fail; but if it ever does, the
		// reason is lost and the control plane will misclassify the failure
		// (e.g. a real crash read as a transient error). Log loudly for debugging purpose.
		slog.Error("ateerrors: failed to attach ErrorInfo to gRPC status; adding `Reason` to the error message instead",
			"err", derr, "reason", sentinel.Error(), "code", grpcCode)
		return status.Error(grpcCode, fmt.Errorf("Reason:%w, Error %w", sentinel, err).Error())
	}
	return st.Err()
}

// ErrorReasonsFromStatus returns the google.rpc.ErrorInfo.Reason carried by err,
// or "" if none.
func ErrorReasonsFromStatus(err error) []string {
	st, ok := status.FromError(err)
	if !ok {
		return nil
	}
	reasons := []string{}
	// Returns the reason of the first ErrorInfo found in st.Details.
	for _, d := range st.Details() {
		if info, ok := d.(*epb.ErrorInfo); ok {
			reasons = append(reasons, info.GetReason())
		}
	}
	return reasons
}

var (
	// Atelet internal errors.
	ErrAteletSnapshotNotFound = errors.New("snapshot not found")
	ErrAteletSnapshotCorrupt  = errors.New("snapshot corrupt")
)
