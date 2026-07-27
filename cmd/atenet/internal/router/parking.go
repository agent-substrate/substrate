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
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Default request-parking parameters. See parkingConfig for the meaning of each
// field; these are also the flag defaults wired up in NewRouterCmd.
const (
	defaultParkedRequestBudget = 5 * time.Second

	// defaultParkedRequestMax is sized to Envoy's default per-cluster circuit
	// breaker (max_requests = 1024): each parked request holds one ext_proc
	// stream, i.e. one active request against the ext_proc cluster, and that
	// cluster (buildCluster in xds.go) sets no explicit circuit_breakers. A lot
	// larger than the circuit breaker is unreachable — Envoy rejects the
	// overflow itself with 503s that never reach the lot (and so never count in
	// parking.rejected). Raise --parked-request-max beyond 1024 only together
	// with an explicit circuit_breakers.max_requests on the ext_proc cluster.
	defaultParkedRequestMax = 1024

	// Retry cadence between resume attempts while a request is parked: a gentle
	// exponential backoff.
	defaultParkedRequestRetryInterval = 100 * time.Millisecond
	defaultParkedRequestRetryFactor   = 1.1
	defaultParkedRequestRetryJitter   = 0.1
)

// parkOutcome is the terminal disposition of a parked request. It is recorded
// as the `outcome` label on the parking.wait.duration histogram.
type parkOutcome string

// Park-wait outcomes, recorded on the parking.wait.duration histogram.
const (
	parkOutcomeServed          parkOutcome = "served"           // resume succeeded and the request was routed
	parkOutcomeBudgetExhausted parkOutcome = "budget_exhausted" // the park budget elapsed while still blocked on a retryable condition
	parkOutcomeTimeout         parkOutcome = "timeout"          // the request's deadline elapsed while parked
	parkOutcomeCanceled        parkOutcome = "canceled"         // the client disconnected while parked
	parkOutcomeError           parkOutcome = "error"            // resume failed
)

// parkingConfig controls how the router parks resume-gated requests.
//
// When a request targets a suspended actor, the router resumes it via the
// control plane before routing. If the worker pool is momentarily saturated the
// control plane returns FailedPrecondition ("no free workers available"). With
// parking enabled the router holds ("parks") such a request and keeps retrying
// the resume until the actor becomes routable or budget elapses, instead of
// failing the request immediately. maxParked bounds how many requests may be
// parked at once so the router sheds load rather than queueing without bound;
// a non-positive maxParked disables parking entirely.
//
// retryInterval/retryFactor/retryJitter shape the backoff between resume
// attempts while a request is parked. The backoff deliberately has no cap and
// no step limit: the budget alone bounds the wait.
type parkingConfig struct {
	budget    time.Duration
	maxParked int

	retryInterval time.Duration
	retryFactor   float64
	retryJitter   float64
}

// enabled reports whether request parking is active. Parking has no separate
// on/off switch: setting maxParked to 0 disables it, applying a fail-fast
// behavior (no admission cap, no retry on pool saturation).
func (c parkingConfig) enabled() bool { return c.maxParked > 0 }

// normalized returns the config with non-positive budget and retry parameters
// replaced by their defaults, so every consumer (the resumer's retry loop and
// the Envoy ext_proc timeout) sees the same effective values.
func (c parkingConfig) normalized() parkingConfig {
	if c.budget <= 0 {
		c.budget = defaultParkedRequestBudget
	}
	if c.retryInterval <= 0 {
		c.retryInterval = defaultParkedRequestRetryInterval
	}
	if c.retryFactor == 0 {
		c.retryFactor = defaultParkedRequestRetryFactor
	}
	return c
}

// validate rejects retry parameters that would make parking misbehave rather
// than merely differ: a factor below 1 shrinks delays toward zero and turns
// the parked retry loop into a hot loop against the control plane.
func (c parkingConfig) validate() error {
	if c.retryFactor != 0 && c.retryFactor < 1.0 {
		return fmt.Errorf("parked-request retry factor must be >= 1.0, got %v", c.retryFactor)
	}
	if c.retryJitter < 0 || c.retryJitter >= 1 {
		return fmt.Errorf("parked-request retry jitter must be in [0, 1), got %v", c.retryJitter)
	}
	return nil
}

// defaultParkingConfig returns the built-in parking configuration (matching the
// NewRouterCmd flag defaults).
func defaultParkingConfig() parkingConfig {
	return parkingConfig{
		budget:        defaultParkedRequestBudget,
		maxParked:     defaultParkedRequestMax,
		retryInterval: defaultParkedRequestRetryInterval,
		retryFactor:   defaultParkedRequestRetryFactor,
		retryJitter:   defaultParkedRequestRetryJitter,
	}
}

// parkingLot is a bounded, non-blocking admission gate for resume-gated
// requests. Each admitted request holds a slot for the duration of its resume
// attempt; when the lot is full further requests are shed immediately so the
// router applies backpressure instead of accumulating waiters without bound.
//
// With parking disabled (maxParked <= 0) enter always admits and performs no
// accounting, applying the router's fail-fast behavior.
type parkingLot struct {
	cfg     parkingConfig
	metrics *parkingMetrics

	mu     sync.Mutex
	active int // current number of occupied slots; guarded by mu
}

func newParkingLot(cfg parkingConfig, m *parkingMetrics) *parkingLot {
	return &parkingLot{cfg: cfg, metrics: m}
}

// enter attempts to reserve a parking slot. On success it returns a release
// func and ok=true; the caller MUST invoke release exactly once (passing the
// request outcome, e.g. parkOutcomeServed) when the resume attempt completes.
// ok=false means the lot is full and the request should be shed without
// waiting. When parking is disabled every request is admitted and no slot
// accounting or metrics are recorded.
func (l *parkingLot) enter(ctx context.Context) (release func(outcome parkOutcome), ok bool) {
	if !l.cfg.enabled() {
		return func(parkOutcome) {}, true
	}

	l.mu.Lock()
	if l.active >= l.cfg.maxParked {
		l.mu.Unlock()
		l.metrics.recordRejected(ctx)
		return nil, false
	}
	l.active++
	l.mu.Unlock()

	start := time.Now()
	l.metrics.addActive(ctx, 1)

	var once sync.Once
	return func(outcome parkOutcome) {
		once.Do(func() {
			l.mu.Lock()
			// The counter cannot go negative today (a release only exists after a
			// successful enter, and it is Once-guarded), so a violation means an
			// accounting bug elsewhere: clamp, but say so loudly.
			if l.active > 0 {
				l.active--
			} else {
				slog.Error("parking lot slot released more times than acquired")
			}
			l.mu.Unlock()
			l.metrics.addActive(ctx, -1)
			l.metrics.recordWait(ctx, time.Since(start), outcome)
		})
	}, true
}

// activeCount returns the number of requests currently parked.
func (l *parkingLot) activeCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.active
}

// status returns a snapshot of the lot for the /statusz page.
func (l *parkingLot) status() ParkingStatus {
	return ParkingStatus{
		Enabled:   l.cfg.enabled(),
		Active:    l.activeCount(),
		MaxParked: l.cfg.maxParked,
		MaxWait:   l.cfg.budget.String(),
	}
}

// parkOutcomeFor classifies a completed resume attempt for the wait-duration
// metric.
func parkOutcomeFor(err error) parkOutcome {
	var budget *budgetExhaustedError
	switch {
	case err == nil:
		return parkOutcomeServed
	case errors.As(err, &budget):
		return parkOutcomeBudgetExhausted
	case errors.Is(err, context.Canceled):
		return parkOutcomeCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return parkOutcomeTimeout
	default:
		return parkOutcomeError
	}
}
