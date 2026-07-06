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

// Package overlayfallback defines the cross-process contract that lets atelet
// recognize an overlay rootfs mount failure reported by ateom and recover from
// it by re-preparing the OCI bundle with a plain untar (the pre-overlay path)
// and retrying the RPC.
//
// The overlay mount is performed by the privileged ateom worker, after atelet
// has already returned from bundle preparation, so atelet cannot fall back
// in-band: it only learns of the failure through the RPC error. To fall back
// safely atelet must distinguish "the overlay could not be mounted" (recover by
// untar) from every other RunWorkload/RestoreWorkload failure (do not retry).
//
// The signal travels as a gRPC status. ateom's server interceptor rebuilds
// returned status errors as status.Error(code, message), preserving only the
// code and message and discarding any structured details — so the marker must
// live in the message, guarded by a specific code to make a false positive
// from an unrelated FailedPrecondition error effectively impossible.
package overlayfallback

import (
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// marker is embedded in the status message of an overlay-mount-failure error.
// It survives the ateom interceptor's status.Error(code, message) rebuild,
// unlike status details.
const marker = "ate-overlay-mount-failed"

// MountFailure wraps err as the gRPC status atelet recognizes as an overlay
// rootfs mount failure eligible for untar fallback. err's text is preserved for
// diagnosis; the code and marker are what atelet matches on.
func MountFailure(err error) error {
	return status.Errorf(codes.FailedPrecondition, "%s: %v", marker, err)
}

// IsMountFailure reports whether err, as received by the atelet gRPC client,
// was produced by MountFailure. It matches on both the FailedPrecondition code
// and the marker so an unrelated FailedPrecondition from either RPC is not
// mistaken for a recoverable overlay failure.
func IsMountFailure(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	return st.Code() == codes.FailedPrecondition && strings.Contains(st.Message(), marker)
}
