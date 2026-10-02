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

package boomerutil

import (
	"context"
	"math/rand/v2"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FailureAction is what a user class does about a failed lifecycle RPC: is
// this actor wedged, or is the cluster busy? Only the first is worth a
// delete + create.
type FailureAction int

const (
	// RetryLater: a replacement would hit the same error, so keep the actor.
	RetryLater FailureAction = iota
	// ReplaceNow: this actor can never make progress again.
	ReplaceNow
	// ReplaceIfPersistent: counts toward the caller's consecutive-failure
	// threshold.
	ReplaceIfPersistent
)

const (
	// ConcurrentUpdateMsg is the message ateapi returns with codes.Aborted
	// when two callers race an actor update (see
	// cmd/ateapi/internal/controlapi/workflow_resume.go). It's transient —
	// the loser retries and one of them wins.
	ConcurrentUpdateMsg = "concurrent update conflict, please retry"

	// ConflictRetryAttempts bounds RetryOnConflict. The first retry is
	// immediate (conflicts often clear the instant the racing writer
	// commits); later gaps are conflictRetryBackoff plus uniform jitter in
	// [0, conflictRetryJitter) so the losers of one race do not retry in
	// lockstep and lose the next one the same way.
	ConflictRetryAttempts = 5
	// ConflictRetryBackoff is the gap before the second and later retries.
	ConflictRetryBackoff = 50 * time.Millisecond
	conflictRetryJitter  = 5 * time.Millisecond
)

// ClassifyLifecycleFailure maps an error from ResumeActor / SuspendActor /
// PauseActor onto what to do about it. Unrecognized codes are recoverable
// until proven otherwise: a wrapped atelet error arrives as Unknown.
func ClassifyLifecycleFailure(err error) FailureAction {
	s, ok := status.FromError(err)
	if !ok {
		return ReplaceIfPersistent
	}
	switch s.Code() {
	case codes.NotFound, codes.DataLoss:
		// The actor, or the snapshot it would resume from, is gone.
		return ReplaceNow
	case codes.FailedPrecondition:
		// A state this operation has no edge out of — the left-in-SUSPENDING
		// case, or a CRASHED actor. Nothing a client can call moves it on.
		return ReplaceNow
	case codes.Aborted:
		if IsCrashed(err) {
			return ReplaceNow
		}
		// Concurrent update conflict. The caller already spent its retry
		// budget, but losing every race in one burst is still a race.
		return ReplaceIfPersistent
	case codes.ResourceExhausted, codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		// Cluster-wide and load-dependent ("no worker has room for the actor",
		// a restarting ate-api-server): every VU sees these at once, and the
		// replacement needs the capacity the original was denied.
		return RetryLater
	case codes.InvalidArgument, codes.PermissionDenied, codes.Unauthenticated, codes.Unimplemented:
		// A misconfigured run. The replacement is created the same way and
		// fails the same way.
		return RetryLater
	default:
		return ReplaceIfPersistent
	}
}

// IsCrashed reports whether err is ateapi's ResumeActor verdict on an actor
// in ACTOR_STATE_CRASHED: codes.Aborted with "crashed" in the message.
// ateapi never rehabilitates one.
func IsCrashed(err error) bool {
	s, ok := status.FromError(err)
	return ok && s.Code() == codes.Aborted && strings.Contains(s.Message(), "crashed")
}

// IsConcurrentUpdateConflict identifies the transient racy-update error
// ateapi's workflow_*.go returns as codes.Aborted with the retry-me message.
// Kept distinct from IsCrashed because the two look the same at the code
// level and mean opposite things.
func IsConcurrentUpdateConflict(err error) bool {
	s, ok := status.FromError(err)
	return ok && s.Code() == codes.Aborted && strings.Contains(s.Message(), ConcurrentUpdateMsg)
}

// RetryOnConflict runs call up to ConflictRetryAttempts times while it keeps
// returning a concurrent-update conflict, and returns any other error
// unchanged. A canceled ctx ends the loop between attempts.
func RetryOnConflict(ctx context.Context, call func() error) error {
	var backoff time.Duration // 0 → first retry runs immediately
	var lastErr error
	for range ConflictRetryAttempts {
		lastErr = call()
		if lastErr == nil || !IsConcurrentUpdateConflict(lastErr) {
			return lastErr
		}
		if backoff > 0 {
			jitter := time.Duration(rand.Float64() * float64(conflictRetryJitter))
			select {
			case <-time.After(backoff + jitter):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		backoff = ConflictRetryBackoff
	}
	return lastErr
}
