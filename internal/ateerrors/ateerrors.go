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
	"slices"

	epb "google.golang.org/genproto/googleapis/rpc/errdetails"

	"google.golang.org/grpc/status"
)

// Reason is a bounded, UPPER_SNAKE_CASE enum of failure causes. A Reason is
// also an error: source layers tag failures with fmt.Errorf("%w: ...",
// ReasonX, err), and consumers read the tag back with ExtractReason to label
// crash telemetry.
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

	// ReasonWorkloadNotReady marks a container that started but never passed its
	// wakeup probe before the probe's deadline. First reason in the workload
	// fault domain (ateattr.FailureDomain); the operation it failed under is
	// ate.actor.operation.name, not part of this value.
	//
	// Confounded by a slow node: a cold image or a throttled CPU also runs the
	// probe out of time. ate.actor.restore.duration.* on the same record
	// separates the two.
	ReasonWorkloadNotReady Reason = "WORKLOAD_NOT_READY"

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
	ReasonWorkloadNotReady,
	ReasonCorruptedAssignment,
	ReasonWorkerReassigned,
	ReasonWorkerPodGone,
	ReasonUnknown,
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
