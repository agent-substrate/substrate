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
	"math"
	"time"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/wait"
)

// failFastResumeBudget is the total time the resumer spends retrying a resume
// when request parking is disabled. In that mode only concurrent-update
// conflicts are retried; capacity errors fail immediately.
const failFastResumeBudget = 15 * time.Second

// resumeBackoff builds the backoff between resume attempts while a request is
// parked, from the configured retry parameters.
//
// It intentionally sets NO Cap. wait.Backoff's delay() zeroes Steps the moment
// the delay reaches Cap, which would end retries long before the parking budget
// (a Cap of 2s stops the loop in ~7 steps regardless of the budget). A gentle
// Factor keeps the gap small on its own — from 100ms at the default 1.1 the gap
// only grows to ~0.5s over a 5s budget — while Steps is set high so the budget
// context passed to ExponentialBackoffWithContext, not the step count, bounds
// the wait.
func resumeBackoff(interval time.Duration, factor, jitter float64) wait.Backoff {
	return wait.Backoff{
		Steps:    math.MaxInt32,
		Duration: interval,
		Factor:   factor,
		Jitter:   jitter,
	}
}

// budgetExhaustedError marks a resume that was still blocked on a retryable
// condition (e.g. "no free workers available") when the parking budget elapsed.
// It wraps the last retryable error, so the HTTP boundary still maps the
// underlying gRPC status faithfully (503 with the capacity message), while the
// parking metrics can report budget exhaustion as its own outcome.
type budgetExhaustedError struct{ lastErr error }

func (e *budgetExhaustedError) Error() string { return e.lastErr.Error() }
func (e *budgetExhaustedError) Unwrap() error { return e.lastErr }

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
	// backoff paces the retries within the budget.
	backoff wait.Backoff
}

// resumerOption configures an ActorResumer.
type resumerOption func(*ActorResumer)

// withParking configures parking behavior from cfg. When parking is enabled,
// FailedPrecondition ("no free workers available") becomes retryable and the
// resume is retried, at cfg's retry cadence, for up to cfg's budget. When
// disabled, the resumer applies fail-fast-on-capacity behavior.
func withParking(cfg parkingConfig) resumerOption {
	cfg = cfg.normalized()
	return func(r *ActorResumer) {
		r.parkEnabled = cfg.enabled()
		if r.parkEnabled {
			r.budget = cfg.budget
		}
		r.backoff = resumeBackoff(cfg.retryInterval, cfg.retryFactor, cfg.retryJitter)
	}
}

func NewActorResumer(apiClient ateapipb.ControlClient, opts ...resumerOption) *ActorResumer {
	r := &ActorResumer{
		apiClient: apiClient,
		budget:    failFastResumeBudget,
		backoff: resumeBackoff(defaultParkedRequestRetryInterval,
			defaultParkedRequestRetryFactor, defaultParkedRequestRetryJitter),
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
func (r *ActorResumer) ResumeActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error) {
	ctx, span := otel.Tracer(routerServiceName).Start(ctx, "ResumeActor",
		trace.WithAttributes(ateattr.ActorRefAttributes(actorRef)...))
	defer span.End()

	ch := r.flight.DoChan(actorRef.String(), func() (interface{}, error) {
		// We detach the context from the first caller using a fixed background budget.
		// This guarantees that if Caller 1 disconnects or times out, the underlying
		// resume operation continues running for Caller 2 and Caller 3 without failing.
		bgCtx, bgCancel := context.WithTimeout(context.Background(), r.budget)
		defer bgCancel()

		backoff := r.backoff

		var resumeResp *ateapipb.ResumeActorResponse
		var lastRetryErr error

		err := wait.ExponentialBackoffWithContext(bgCtx, backoff, func(ctx context.Context) (bool, error) {
			var err error
			resumeResp, err = r.apiClient.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
				Actor: actorRef.ToObjectRef(),
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
			// If the budget elapsed while we were still retrying a transient error,
			// surface that underlying error rather than the generic wait/deadline
			// error so the HTTP boundary maps it faithfully (e.g. 503 "no free
			// workers available") instead of a misleading timeout. The wrapper marks
			// the exhaustion explicitly for the parking wait-duration metric.
			//
			// Gate on bgCtx itself, not on errors.Is(err, context.DeadlineExceeded):
			// when the deadline lands during an in-flight ResumeActor RPC, gRPC
			// surfaces a *status* error with code DeadlineExceeded that does not
			// match the context sentinel, which would misreport budget exhaustion
			// as a 504. bgCtx is this loop's only deadline source, so checking it
			// covers both landing spots (mid-RPC and between retries).
			if lastRetryErr != nil && (bgCtx.Err() != nil || wait.Interrupted(err)) {
				return nil, &budgetExhaustedError{lastErr: lastRetryErr}
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
