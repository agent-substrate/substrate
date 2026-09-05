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
	"sync"
	"sync/atomic"
	"time"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
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

	// mu guards flights, the per-actor registry of in-flight resumes.
	mu sync.Mutex
	// flights deduplicates concurrent resumes per actor, singleflight-style:
	// the first caller creates the flight and later callers attach to it. An
	// entry is removed the moment its flight completes.
	flights map[string]*resumeActorFlight

	// lot bounds parked callers; a slot is taken at the park transition,
	// never on the fast path (#1081). A nil lot admits everyone.
	lot *parkingLot

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
	// leader vs joiners for flight outcome classification.
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

// withParkingLot bounds concurrent parked callers with lot. A caller acquires
// a slot only at its flight's park transition; when the lot is full at that
// moment the caller is shed with a 503 instead of waiting.
func withParkingLot(lot *parkingLot) resumerOption {
	return func(r *ActorResumer) { r.lot = lot }
}

func NewActorResumer(apiClient ateapipb.ControlClient, opts ...resumerOption) *ActorResumer {
	r := &ActorResumer{
		apiClient: apiClient,
		flights:   make(map[string]*resumeActorFlight),
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

// ResumeActor ensures the actor is running.
// This method will block until the actor is resumed or error.
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

	key := actorRef.String()
	r.mu.Lock()
	f, ok := r.flights[key]
	if !ok {
		f = newResumeActorFlight()
		r.flights[key] = f
		go r.runFlight(f, key, actorRef, reqID, callerSpanCtx)
	}
	r.mu.Unlock()

	return r.awaitFlight(ctx, f, actorRef, reqID)
}

// runFlight runs one shared resume for actorRef and publishes its outcome to
// every attached caller. reqID identifies the caller that created the flight.
func (r *ActorResumer) runFlight(f *resumeActorFlight, key string, actorRef resources.ActorRef, reqID uint64, callerSpanCtx trace.SpanContext) {
	// The budget is per flight, not per caller: it starts with the first caller
	// and later joiners share what remains, so no caller's disconnect can abort
	// the shared resume. Only cancellation is detached; the trace context is kept.
	bgCtx, bgCancel := context.WithTimeout(trace.ContextWithSpanContext(context.Background(), callerSpanCtx), r.budget)
	defer bgCancel()
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
			f.signalRetrying()
			lastRetryErr = err // remember it in case the budget elapses
			return false, nil  // park: retry until the budget elapses
		}
		return false, err
	})

	r.publish(f, key, flightResult(bgCtx, resumeResp, err, lastRetryErr, reqID))
}

// flightResult classifies the retry loop's terminal state into the shared
// result every caller attached to the flight receives. bgCtx is the flight's
// budget context, consulted only for whether the budget expired.
func flightResult(bgCtx context.Context, resumeResp *ateapipb.ResumeActorResponse, err, lastRetryErr error, reqID uint64) *resumeCallResult {
	result := &resumeCallResult{leaderID: reqID}
	switch {
	case err == nil:
		result.actor = resumeResp.GetActor()
		result.resumed = resumeResp.GetResumed()
	// Budget elapsed while still retryable (between retries, or a late retryable
	// answer): surface the underlying error, marked as exhaustion for the metric,
	// so the client sees the capacity 503 rather than a timeout.
	case lastRetryErr != nil && (bgCtx.Err() != nil || wait.Interrupted(err)):
		result.err = &budgetExhaustedError{lastErr: lastRetryErr}
	default:
		result.err = err
	}
	return result
}

// awaitFlight waits for f's outcome on behalf of one caller. The wait is
// two-phase: while the flight is resolving the caller holds nothing; once the
// flight is retrying (or already was), the caller must hold a parking-lot slot
// to keep waiting and is shed with a 503 when the lot is full.
func (r *ActorResumer) awaitFlight(ctx context.Context, f *resumeActorFlight, actorRef resources.ActorRef, reqID uint64) (*ateapipb.Actor, ResumeOutcome, error) {
	select {
	case <-ctx.Done():
		// The caller's request context was canceled before the shared resume
		// completed. Return early with ResumeOutcomeNone ("none").
		return nil, ResumeOutcomeNone, ctx.Err()
	case <-f.done:
		// Fast path: the flight finished without ever retrying, or before this
		// caller saw retrying. The lot is never touched.
		return f.callerResult(reqID)
	case <-f.retrying:
		// A closed channel is always ready, so a caller attaching after the
		// flight began retrying lands here immediately.
	}

	// select picks randomly among ready cases, so retrying may have won even
	// though done was ready too: serve a published result without charging the lot.
	select {
	case <-f.done:
		return f.callerResult(reqID)
	default:
	}

	release, ok := r.enterLot(ctx)
	if !ok {
		return nil, ResumeOutcomeNone, parkingFullErr(actorRef.String())
	}
	var finalErr error
	defer func() { release(parkOutcomeFor(finalErr)) }()

	select {
	case <-ctx.Done():
		finalErr = ctx.Err()
		return nil, ResumeOutcomeNone, finalErr
	case <-f.done:
		actor, outcome, err := f.callerResult(reqID)
		finalErr = err
		return actor, outcome, err
	}
}

// enterLot admits the caller to the parking lot, treating a nil lot as
// unbounded (no admission control).
func (r *ActorResumer) enterLot(ctx context.Context) (func(parkOutcome), bool) {
	if r.lot == nil {
		return func(parkOutcome) {}, true
	}
	return r.lot.enter(ctx)
}
