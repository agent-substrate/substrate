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
	"context"
	"errors"
	"math"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/wait"
)

// legacyResumeBudget is the total time the resumer spends retrying a resume when
// request parking is disabled. It preserves the historical fail-fast-on-capacity
// behavior (only concurrent-update conflicts are retried).
const legacyResumeBudget = 15 * time.Second

// ActorResumer coordinates safe, deduplicated resumption of actors.
type ActorResumer struct {
	apiClient ateapipb.ControlClient
	flight    singleflight.Group

	// parkEnabled makes transient worker-pool saturation (FailedPrecondition)
	// retryable, so a request is parked and retried until budget rather than
	// failing immediately.
	parkEnabled bool
	// budget bounds the total time a single resume operation retries before the
	// underlying error is returned.
	budget time.Duration
}

// resumerOption configures an ActorResumer.
type resumerOption func(*ActorResumer)

// withParking configures parking behavior. When enabled, FailedPrecondition
// ("no free workers available") becomes retryable and the resume is retried for
// up to maxWait; a non-positive maxWait keeps the default budget. When disabled,
// the resumer preserves its legacy fail-fast-on-capacity behavior.
func withParking(enabled bool, maxWait time.Duration) resumerOption {
	return func(r *ActorResumer) {
		r.parkEnabled = enabled
		if maxWait > 0 {
			r.budget = maxWait
		}
	}
}

func NewActorResumer(apiClient ateapipb.ControlClient, opts ...resumerOption) *ActorResumer {
	r := &ActorResumer{
		apiClient: apiClient,
		budget:    legacyResumeBudget,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// retryable reports whether err warrants another resume attempt while the
// request remains parked. A concurrent-resume conflict (Aborted) is always
// retried. Transient pool saturation (FailedPrecondition, "no free workers
// available") is retried only when parking is enabled, turning a momentary
// shortage into a bounded wait instead of an immediate failure. All other codes
// (NotFound, Unavailable, DeadlineExceeded, ...) are returned to the caller so
// the HTTP boundary can map them with full fidelity.
func (r *ActorResumer) retryable(err error) bool {
	switch status.Code(err) {
	case codes.Aborted:
		return true
	case codes.FailedPrecondition:
		return r.parkEnabled
	default:
		return false
	}
}

// ResumeActor ensures the requested actor is running. It deduplicates concurrent
// requests within the process and, when parking is enabled, holds the request
// while retrying transient failures until the budget elapses.
func (r *ActorResumer) ResumeActor(ctx context.Context, actorID string) (*ateapipb.Actor, error) {
	ch := r.flight.DoChan(actorID, func() (interface{}, error) {
		// We detach the context from the first caller using a fixed background budget.
		// This guarantees that if Caller 1 disconnects or times out, the underlying
		// resume operation continues running for Caller 2 and Caller 3 without failing.
		bgCtx, bgCancel := context.WithTimeout(context.Background(), r.budget)
		defer bgCancel()

		// Retry with capped exponential backoff until the budget (bgCtx) elapses;
		// Steps is set high so the budget, not the step count, bounds the wait.
		backoff := wait.Backoff{
			Steps:    math.MaxInt32,
			Duration: 200 * time.Millisecond,
			Factor:   1.5,
			Jitter:   0.2,
			Cap:      2 * time.Second,
		}

		var resumeResp *ateapipb.ResumeActorResponse
		var lastRetryErr error

		err := wait.ExponentialBackoffWithContext(bgCtx, backoff, func(ctx context.Context) (bool, error) {
			var err error
			resumeResp, err = r.apiClient.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
				ActorId: actorID,
			})
			if err == nil {
				return true, nil
			}

			if r.retryable(err) {
				lastRetryErr = err // remember it in case the budget elapses
				return false, nil  // park: retry until the budget elapses
			}
			return false, err
		})

		if err != nil {
			// If the budget elapsed (DeadlineExceeded) or steps ran out
			// (wait.Interrupted) while we were still retrying a transient error,
			// surface that underlying error rather than the generic wait error so
			// the HTTP boundary maps it faithfully (e.g. 503 "no free workers
			// available") instead of a misleading timeout.
			if lastRetryErr != nil && (errors.Is(err, context.DeadlineExceeded) || wait.Interrupted(err)) {
				return nil, lastRetryErr
			}
			return nil, err
		}

		return resumeResp.GetActor(), nil
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*ateapipb.Actor), nil
	}
}
