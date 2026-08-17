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
// only grows to ~0.5s over a 5s budget — while Steps is set high so flight and
// caller deadlines, not the step count, bound retries.
func resumeBackoff(interval time.Duration, factor, jitter float64) wait.Backoff {
	return wait.Backoff{
		Steps:    math.MaxInt32,
		Duration: interval,
		Factor:   factor,
		Jitter:   jitter,
	}
}

// budgetExhaustedError marks a caller whose parking budget elapsed. It wraps
// the last retryable error when one is available, so the HTTP boundary can
// preserve that status (for example, a capacity 503). If the first RPC consumes
// the whole budget, it wraps context.DeadlineExceeded instead and maps to 504.
type budgetExhaustedError struct{ lastErr error }

func (e *budgetExhaustedError) Error() string { return e.lastErr.Error() }
func (e *budgetExhaustedError) Unwrap() error { return e.lastErr }

// ResumeOutcome indicates the shared-flight execution state of an actor resumption request.
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
	// the shared execution. It helps disambiguate the leader caller
	// (ResumeOutcomeTriggered) from joiner callers (ResumeOutcomeJoined).
	leaderID uint64
	err      error
}

// resumeFlight owns one control-plane retry loop shared by requests for an
// Actor. Its deadline tracks the latest joining caller, while each caller has
// an independent timer and can stop waiting earlier.
type resumeFlight struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	joined chan struct{}

	mu           sync.Mutex
	deadline     time.Time
	timer        *time.Timer
	expired      bool
	finished     bool
	lastRetryErr error
	result       *resumeCallResult
}

func newResumeFlight(deadline time.Time) *resumeFlight {
	ctx, cancel := context.WithCancel(context.Background())
	f := &resumeFlight{
		ctx:      ctx,
		cancel:   cancel,
		done:     make(chan struct{}),
		joined:   make(chan struct{}, 1),
		deadline: deadline,
	}
	f.timer = time.AfterFunc(time.Until(deadline), f.expire)
	return f
}

// join extends the shared loop through the caller's deadline and notifies the
// retry scheduler that fresh demand arrived. The notification is coalesced:
// ten simultaneous joiners need one backoff adjustment, not ten RPCs.
func (f *resumeFlight) join(deadline time.Time) bool {
	f.mu.Lock()
	if f.finished || f.expired {
		f.mu.Unlock()
		return false
	}
	if !time.Now().Before(f.deadline) {
		f.expired = true
		f.mu.Unlock()
		f.cancel()
		return false
	}
	if deadline.After(f.deadline) {
		f.deadline = deadline
		f.timer.Reset(time.Until(deadline))
	}
	f.mu.Unlock()

	select {
	case f.joined <- struct{}{}:
	default:
	}
	return true
}

func (f *resumeFlight) expire() {
	f.mu.Lock()
	if f.finished || f.expired {
		f.mu.Unlock()
		return
	}
	if remaining := time.Until(f.deadline); remaining > 0 {
		// A previous timer callback can race with join extending the deadline.
		// Re-check the guarded value instead of canceling the extended flight.
		f.timer.Reset(remaining)
		f.mu.Unlock()
		return
	}
	f.expired = true
	f.mu.Unlock()
	f.cancel()
}

func (f *resumeFlight) setLastRetryErr(err error) {
	f.mu.Lock()
	f.lastRetryErr = err
	f.mu.Unlock()
}

func (f *resumeFlight) budgetError() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lastRetryErr == nil {
		// The caller's park budget still expired even though an in-flight first RPC
		// occupied the whole window before producing a retryable error.
		return &budgetExhaustedError{lastErr: context.DeadlineExceeded}
	}
	return &budgetExhaustedError{lastErr: f.lastRetryErr}
}

// finish publishes the retry loop's one terminal result. Deadline expiry only
// cancels ctx; the loop must observe that cancellation and return before done
// is closed, after which joiners can safely read result.
func (f *resumeFlight) finish(result *resumeCallResult) {
	f.mu.Lock()
	f.finished = true
	f.result = result
	f.timer.Stop()
	close(f.done)
	f.mu.Unlock()
	f.cancel()
}

// ActorResumer coordinates safe, deduplicated resumption of actors.
type ActorResumer struct {
	apiClient ateapipb.ControlClient

	mu      sync.Mutex
	flights map[string]*resumeFlight

	// parkEnabled makes transient worker-pool saturation (FailedPrecondition)
	// retryable, so a request is parked and retried until budget rather than
	// failing immediately.
	parkEnabled bool
	// budget is each caller's maximum parking time. The shared flight tracks the
	// latest caller deadline, but callers stop waiting on their own timers.
	budget time.Duration
	// backoff paces the shared retry loop.
	backoff wait.Backoff
	// nextID is a counter assigned to each incoming ResumeActor call.
	// Used as a unique ID to identify requests (reqID) and disambiguate the
	// leader vs joiners for shared-flight outcome classification.
	nextID uint64
}

// resumerOption configures an ActorResumer.
type resumerOption func(*ActorResumer)

// withParking configures parking behavior from cfg. When parking is enabled,
// FailedPrecondition ("no free workers available") becomes retryable and the
// shared resume is retried at cfg's cadence, while each caller waits for at
// most cfg's budget. When disabled, the resumer applies fail-fast-on-capacity
// behavior.
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
		flights:   make(map[string]*resumeFlight),
		budget:    failFastResumeBudget,
		backoff: resumeBackoff(DefaultParkedRequestRetryInterval,
			DefaultParkedRequestRetryFactor, DefaultParkedRequestRetryJitter),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func (r *ActorResumer) joinOrStartFlight(actorRef resources.ActorRef, reqID uint64, deadline time.Time) *resumeFlight {
	key := actorRef.String()
	r.mu.Lock()
	if flight := r.flights[key]; flight != nil && flight.join(deadline) {
		r.mu.Unlock()
		return flight
	}

	flight := newResumeFlight(deadline)
	r.flights[key] = flight
	r.mu.Unlock()

	go func() {
		flight.finish(r.runResumeFlight(flight, actorRef, reqID))

		r.mu.Lock()
		if r.flights[key] == flight {
			delete(r.flights, key)
		}
		r.mu.Unlock()
	}()
	return flight
}

func (r *ActorResumer) runResumeFlight(flight *resumeFlight, actorRef resources.ActorRef, leaderID uint64) *resumeCallResult {
	backoff := r.backoff

retry:
	for {
		resumeResp, err := r.apiClient.ResumeActor(flight.ctx, &ateapipb.ResumeActorRequest{
			Actor: actorRef.ToObjectRef(),
		})
		if err == nil {
			return &resumeCallResult{
				actor:    resumeResp.GetActor(),
				resumed:  resumeResp.GetResumed(),
				leaderID: leaderID,
			}
		}

		if flight.ctx.Err() != nil {
			return &resumeCallResult{leaderID: leaderID, err: flight.budgetError()}
		}
		if !r.retryable(err) {
			return &resumeCallResult{leaderID: leaderID, err: err}
		}
		flight.setLastRetryErr(err)

		delay := backoff.Step()
		retryAt := time.Now().Add(delay)
		timer := time.NewTimer(delay)
		for {
			select {
			case <-flight.ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return &resumeCallResult{leaderID: leaderID, err: flight.budgetError()}
			case <-flight.joined:
				// If accumulated backoff puts the next attempt more than one initial
				// interval away, bring it forward and restart backoff growth. Leave an
				// already-sooner attempt and its future growth unchanged. The channel's
				// single slot coalesces simultaneous joins.
				freshBackoff := r.backoff
				freshDelay := freshBackoff.Step()
				freshRetryAt := time.Now().Add(freshDelay)
				if freshRetryAt.Before(retryAt) {
					backoff = freshBackoff
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timer.Reset(freshDelay)
					retryAt = freshRetryAt
				}
			case <-timer.C:
				continue retry
			}
		}
	}
}

// retryable reports whether err warrants another resume attempt while the
// request remains parked. A concurrent-resume conflict (Aborted) is always
// retried. Transient pool saturation (FailedPrecondition, "no free workers
// available") and transient control-plane unavailability (Unavailable, e.g. an
// ateapi rolling restart) are retried only when parking is enabled, turning a
// momentary condition into a bounded wait instead of an immediate failure — a
// parked request should ride out a blip, not fail on it with budget remaining.
// All other codes (NotFound, DeadlineExceeded, PermissionDenied, ...) are
// returned to the caller so the HTTP boundary can map them with full fidelity.
func (r *ActorResumer) retryable(err error) bool {
	switch status.Code(err) {
	case codes.Aborted:
		return true
	case codes.FailedPrecondition, codes.Unavailable:
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
	callerDeadline := time.Now().Add(r.budget)
	budgetTimer := time.NewTimer(time.Until(callerDeadline))
	defer budgetTimer.Stop()
	flight := r.joinOrStartFlight(actorRef, reqID, callerDeadline)

	select {
	case <-ctx.Done():
		// The caller's request context was canceled before the shared resume completed.
		// Return early with ResumeOutcomeNone ("none"). The detached flight keeps
		// running so another caller can still share it.
		return nil, ResumeOutcomeNone, ctx.Err()
	case <-budgetTimer.C:
		return nil, ResumeOutcomeNone, flight.budgetError()
	case <-flight.done:
		callRes := flight.result

		// On error, return ResumeOutcomeNone ("none") so the failure is tagged
		// under the 'outcome' label rather than misreported as an activation.
		if callRes.err != nil {
			return nil, ResumeOutcomeNone, callRes.err
		}

		// Disambiguate shared resume outcome:
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
