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

package ingress

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
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

// ResumeOutcome indicates the singleflight execution state of an actor resumption request.
type ResumeOutcome string

const (
	ResumeOutcomeNone      ResumeOutcome = ateattr.RouterResumeNone
	ResumeOutcomeTriggered ResumeOutcome = ateattr.RouterResumeTriggered
	ResumeOutcomeJoined    ResumeOutcome = ateattr.RouterResumeJoined
)

type resumeCallResult struct {
	actor *ateapipb.Actor
	// resumed is true if ResumeActor call executed a cold activation
	// false if the actor was already running
	resumed bool
	// leaderID is the unique request ID (reqID) of the leader that initiated
	// the singleflight execution. It helps disambiguates the leader caller
	// (ResumeOutcomeTriggered) from joiner callers (ResumeOutcomeJoined).
	leaderID uint64
	err      error
}

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
	// nextID is a counter assigned to each incoming ResumeActor call.
	// Used as a unique ID to identify requests (reqID) and disambiguate the
	// leader vs joiners for singleflight outcome classification.
	nextID uint64
}

// resumerOption configures an ActorResumer.
type resumerOption func(*ActorResumer)

// withParking configures parking behavior from cfg. When parking is enabled,
// ResourceExhausted ("no free workers available") becomes retryable and the
// resume is retried, at cfg's retry cadence, for up to cfg's budget. When
// disabled, the resumer applies fail-fast-on-capacity behavior.
func withParking(cfg ParkedRequestConfig) resumerOption {
	cfg = cfg.Normalized()
	return func(r *ActorResumer) {
		r.parkEnabled = cfg.Enabled()
		if r.parkEnabled {
			r.budget = cfg.Budget
		}
		r.backoff = resumeBackoff(cfg.RetryInterval, cfg.RetryFactor, cfg.RetryJitter)
	}
}

func NewActorResumer(apiClient ateapipb.ControlClient, opts ...resumerOption) *ActorResumer {
	r := &ActorResumer{
		apiClient: apiClient,
		budget:    failFastResumeBudget,
		backoff: resumeBackoff(DefaultParkedRequestRetryInterval,
			DefaultParkedRequestRetryFactor, DefaultParkedRequestRetryJitter),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// retryable reports whether err warrants another resume attempt while the
// request remains parked. A concurrent-resume conflict (Aborted) is always
// retried. Transient pool saturation (ResourceExhausted, "no free workers
// available") and transient control-plane unavailability (Unavailable, e.g. an
// ateapi rolling restart) are retried only when parking is enabled, turning a
// momentary condition into a bounded wait instead of an immediate failure — a
// parked request should ride out a blip, not fail on it with budget remaining.
// FailedPrecondition stays retryable too: it no longer carries saturation, but
// it does cover states a concurrent operation can move the actor out of.
// All other codes (NotFound, DeadlineExceeded, PermissionDenied, ...) are
// returned to the caller so the HTTP boundary can map them with full fidelity.
func (r *ActorResumer) retryable(err error) bool {
	switch status.Code(err) {
	case codes.Aborted:
		return true
	case codes.ResourceExhausted, codes.FailedPrecondition, codes.Unavailable:
		return r.parkEnabled
	default:
		return false
	}
}

// ResumeActor ensures the requested actor is running. It deduplicates concurrent
// requests within the process and, when parking is enabled, holds the request
// while retrying transient failures until the budget elapses.
func (r *ActorResumer) ResumeActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, ResumeOutcome, error) {
	ctx, span := otel.Tracer(extproc.ServiceName).Start(ctx, "ResumeActor",
		trace.WithAttributes(ateattr.ActorRefAttributes(actorRef)...))
	defer span.End()

	reqID := atomic.AddUint64(&r.nextID, 1)

	// The flight below detaches from the caller's lifecycle but must keep its
	// trace identity: detaching with a bare background context makes the
	// ateapi call start a fresh root trace, so the server-side resume spans
	// (step.*, atelet, snapshot fetch) fragment away from the request that
	// triggered them and sample independently of it. Joiners share the
	// leader's flight, so the flight carries the leader's span context.
	callerSpanCtx := trace.SpanContextFromContext(ctx)

	ch := r.flight.DoChan(actorRef.String(), func() (interface{}, error) {
		// We detach the context from the first caller using a fixed background budget.
		// This guarantees that if Caller 1 disconnects or times out, the underlying
		// resume operation continues running for Caller 2 and Caller 3 without failing.
		// Only cancellation is detached; the trace context above is kept.
		//
		// The budget is therefore per-FLIGHT, not per-caller: its clock starts with
		// the first caller, and later callers de-duplicated onto this flight share
		// its remaining budget and outcome. A late joiner can see budget_exhausted
		// after waiting far less than a full budget itself — the accepted cost of
		// one control-plane RPC per hot actor (see docs/request-parking.md).
		bgCtx, bgCancel := context.WithTimeout(trace.ContextWithSpanContext(context.Background(), callerSpanCtx), r.budget)
		defer bgCancel()
		// The budget bounds the RETRY LOOP only — it never cancels an
		// in-flight ResumeActor. ateapi durably claims the worker and marks
		// the actor RESUMING before the expensive snapshot restore begins,
		// rolls back neither on cancellation, and nothing reclaims a RESUMING
		// actor whose worker pod is alive — a budget cancel therefore throws
		// the restore away and strands the worker (#675). An attempt still
		// running when the budget elapses is waited for and its real result
		// classified below; ateapi's own server-side RPC deadline bounds it.
		attemptCtx := context.WithoutCancel(bgCtx)

		backoff := r.backoff

		var resumeResp *ateapipb.ResumeActorResponse
		var lastRetryErr error

		err := wait.ExponentialBackoffWithContext(bgCtx, backoff, func(context.Context) (bool, error) {
			var err error
			resumeResp, err = r.apiClient.ResumeActor(attemptCtx, &ateapipb.ResumeActorRequest{
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
			// If the budget elapsed while we were still blocked on a retryable
			// condition, surface that underlying error rather than the generic
			// wait/deadline error so the HTTP boundary maps it faithfully
			// (e.g. 503 "no free workers available") instead of a misleading
			// timeout. The wrapper marks the exhaustion explicitly for the
			// parking wait-duration metric.
			//
			// wait.Interrupted covers the budget landing between retries; the
			// bgCtx check covers an attempt that came back with a retryable
			// error only after the budget had already elapsed (the loop then
			// exits with the context error). The RPC itself is never canceled,
			// so the loop cannot end before its first attempt has completed —
			// lastRetryErr is always the attempt's real answer here, and a
			// definitive error (NotFound, ...) still passes through untouched.
			if lastRetryErr != nil && (bgCtx.Err() != nil || wait.Interrupted(err)) {
				return &resumeCallResult{leaderID: reqID, err: &budgetExhaustedError{lastErr: lastRetryErr}}, nil
			}
			return &resumeCallResult{leaderID: reqID, err: err}, nil
		}

		return &resumeCallResult{
			actor:    resumeResp.GetActor(),
			resumed:  resumeResp.GetResumed(),
			leaderID: reqID,
		}, nil
	})

	select {
	case <-ctx.Done():
		// The caller's request context was canceled before the singleflight resume completed.
		// Return early with ResumeOutcomeNone ("none")
		return nil, ResumeOutcomeNone, ctx.Err()
	case res := <-ch:
		callRes, _ := res.Val.(*resumeCallResult)
		if callRes == nil {
			if res.Err != nil {
				return nil, ResumeOutcomeNone, res.Err
			}
			return nil, ResumeOutcomeNone, status.Error(codes.Internal, "resume call returned nil result")
		}

		// On error, return ResumeOutcomeNone ("none") so the failure is tagged
		// under the 'outcome' label rather than misreported as an activation.
		if callRes.err != nil {
			return nil, ResumeOutcomeNone, callRes.err
		}

		// Disambiguate singleflight resume outcome:
		// - ResumeOutcomeNone ("none"): resumed == false, actor was already active/running.
		// - ResumeOutcomeTriggered ("triggered"): Cold activation leader (resumed == true, caller's reqID == leaderID).
		// - ResumeOutcomeJoined ("joined"): Cold activation joiner (resumed == true, caller's reqID != leaderID).
		outcome := ResumeOutcomeNone
		if callRes.resumed {
			if callRes.leaderID == reqID {
				outcome = ResumeOutcomeTriggered
			} else {
				outcome = ResumeOutcomeJoined
			}
		}

		return callRes.actor, outcome, nil
	}
}
