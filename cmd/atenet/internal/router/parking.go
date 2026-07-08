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
	"sync"
	"time"
)

// Default request-parking parameters. See parkingConfig for the meaning of each
// field; these are also the flag defaults wired up in NewCmd.
const (
	defaultParkingMaxWait   = 30 * time.Second
	defaultParkingMaxParked = 2048
)

// parkOutcome is the terminal disposition of a parked request. It is recorded
// as the `outcome` label on the parking.wait.duration histogram.
type parkOutcome string

// Park-wait outcomes, recorded on the parking.wait.duration histogram.
const (
	parkOutcomeServed   parkOutcome = "served"   // resume succeeded and the request was routed
	parkOutcomeTimeout  parkOutcome = "timeout"  // the request's deadline elapsed while parked
	parkOutcomeCanceled parkOutcome = "canceled" // the client disconnected while parked
	parkOutcomeError    parkOutcome = "error"    // resume failed (including park-budget exhaustion)
)

// parkingConfig controls how the router parks resume-gated requests.
//
// When a request targets a suspended actor, the router resumes it via the
// control plane before routing. If the worker pool is momentarily saturated the
// control plane returns FailedPrecondition ("no free workers available"). With
// parking enabled the router holds ("parks") such a request and keeps retrying
// the resume until the actor becomes routable or maxWait elapses, instead of
// failing the request immediately. maxParked bounds how many requests may be
// parked at once so the router sheds load rather than queueing without bound;
// a non-positive maxParked disables parking entirely.
type parkingConfig struct {
	maxWait   time.Duration
	maxParked int
}

// enabled reports whether request parking is active. Parking has no separate
// on/off switch: setting maxParked to 0 disables it, preserving the legacy
// fail-fast behavior (no admission cap, no retry on pool saturation).
func (c parkingConfig) enabled() bool { return c.maxParked > 0 }

// defaultParkingConfig returns the built-in parking configuration (matching the
// NewCmd flag defaults).
func defaultParkingConfig() parkingConfig {
	return parkingConfig{
		maxWait:   defaultParkingMaxWait,
		maxParked: defaultParkingMaxParked,
	}
}

// parkingLot is a bounded, non-blocking admission gate for resume-gated
// requests. Each admitted request holds a slot for the duration of its resume
// attempt; when the lot is full further requests are shed immediately so the
// router applies backpressure instead of accumulating waiters without bound.
//
// With parking disabled (maxParked <= 0) enter always admits and performs no
// accounting, preserving the router's legacy behavior.
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
			l.active--
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
		MaxWait:   l.cfg.maxWait.String(),
	}
}

// parkOutcomeFor classifies a completed resume attempt for the wait-duration
// metric. A budget-exhausted park surfaces the underlying capacity error and is
// reported as parkOutcomeError.
func parkOutcomeFor(err error) parkOutcome {
	switch {
	case err == nil:
		return parkOutcomeServed
	case errors.Is(err, context.Canceled):
		return parkOutcomeCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return parkOutcomeTimeout
	default:
		return parkOutcomeError
	}
}
