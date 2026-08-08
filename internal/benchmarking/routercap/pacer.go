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

// The open-loop pacer and the rung schedule it walks. Open-loop: it fires on a fixed
// tick whether or not earlier requests have come back.

package routercap

import (
	"context"
	"sync"
	"time"
)

// Rung is one step of the ladder: a constant offered rate held for a fixed
// duration.
type Rung struct {
	Index   int           `json:"index"`
	RateQPS float64       `json:"rate_qps"`
	Hold    time.Duration `json:"hold"`
	// Warmup is the leading part of the rung excluded from the summary; the
	// samples are still written.
	Warmup time.Duration `json:"warmup"`
	// StartAt is set when the rung actually begins, since it depends on how
	// long the preceding rung's teardown took.
	StartAt time.Time `json:"start_at"`
}

// End is the instant after which the pacer schedules nothing more for r.
func (r Rung) End() time.Time { return r.StartAt.Add(r.Hold) }

// Schedule is the record of what the pacer was asked to produce. Offered load
// is read from here rather than counted, so a struggling generator cannot
// quietly redefine the x-axis.
type Schedule struct {
	mu    sync.RWMutex
	rungs []Rung
}

// Begin appends r as started, and returns it with StartAt filled in.
func (s *Schedule) Begin(r Rung, at time.Time) Rung {
	r.StartAt = at
	s.mu.Lock()
	s.rungs = append(s.rungs, r)
	s.mu.Unlock()
	return r
}

// Rungs returns a copy of the rungs begun so far.
func (s *Schedule) Rungs() []Rung {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]Rung(nil), s.rungs...)
}

// OfferedIn is how many requests the schedule placed in [t0, t1). Intervals
// that straddle a rung boundary get each rung's share, so a window is never
// attributed a rate the pacer was not actually running.
func (s *Schedule) OfferedIn(t0, t1 time.Time) float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var total float64
	for _, r := range s.rungs {
		lo, hi := r.StartAt, r.End()
		if t0.After(lo) {
			lo = t0
		}
		if t1.Before(hi) {
			hi = t1
		}
		if d := hi.Sub(lo); d > 0 {
			total += r.RateQPS * d.Seconds()
		}
	}
	return total
}

// RungAt returns the rung covering t, and whether t falls inside that rung's
// warmup prefix. ok is false for instants between rungs.
func (s *Schedule) RungAt(t time.Time) (r Rung, warmup, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, cand := range s.rungs {
		if t.Before(cand.StartAt) || !t.Before(cand.End()) {
			continue
		}
		return cand, t.Before(cand.StartAt.Add(cand.Warmup)), true
	}
	return Rung{}, false, false
}

// SendFunc performs one request and classifies it. It must not retry: a retry
// hidden inside the send would show up as one slow request instead of two, and
// the retry's own load would be invisible to the offered rate.
type SendFunc func(ctx context.Context) (Outcome, int)

// Pacer drives an open loop: it emits requests on a schedule fixed in advance
// and lets concurrency grow as the system slows. Latency is measured from each
// request's scheduled send time, which rules out coordinated omission.
type Pacer struct {
	Collector *Collector
	// MaxInFlight bounds the generator's own concurrency; reaching it is a rig
	// failure, not a result. Requests beyond it are recorded as shed and the
	// guards trip.
	MaxInFlight int64
	// TickCap bounds how long the dispatch loop sleeps, and so bounds the
	// dispatch lag the loop itself can introduce. At rates where the
	// inter-arrival gap is below the OS timer granularity the loop wakes on
	// this interval and emits the whole batch that has come due.
	TickCap time.Duration
}

// RunRung emits r's requests on schedule and returns once the last one has
// been dispatched. It does not wait for in-flight requests to complete —
// draining at a rung boundary would idle the system between rungs and make the
// next rung's first seconds measure a cold pool rather than a running one.
func (p *Pacer) RunRung(ctx context.Context, r Rung, send SendFunc) error {
	if r.RateQPS <= 0 || r.Hold <= 0 {
		return nil
	}
	tickCap := p.TickCap
	if tickCap <= 0 {
		tickCap = time.Millisecond
	}

	total := int(r.RateQPS * r.Hold.Seconds())
	gap := float64(time.Second) / r.RateQPS
	at := func(i int) time.Time {
		// Offsets are computed from the rung start rather than accumulated, so
		// a late wake-up delays one request instead of every request after it.
		return r.StartAt.Add(time.Duration(float64(i) * gap))
	}

	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	for i := 0; i < total; {
		now := time.Now()
		for i < total && !at(i).After(now) {
			p.fire(ctx, at(i), send)
			i++
		}
		if i >= total {
			break
		}
		wait := time.Until(at(i))
		if wait > tickCap {
			wait = tickCap
		}
		if wait <= 0 {
			continue
		}
		timer.Reset(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ctx.Err()
}

// fire launches one request. The goroutine-per-request shape is what makes the
// loop open: the dispatcher never blocks on a response, so a slow system grows
// the in-flight count instead of throttling the offered rate.
func (p *Pacer) fire(ctx context.Context, scheduled time.Time, send SendFunc) {
	if p.MaxInFlight > 0 && p.Collector.InFlight() >= p.MaxInFlight {
		p.Collector.RecordShed(scheduled, time.Now())
		return
	}
	go func() {
		p.Collector.RecordDispatch(scheduled, time.Now())
		outcome, status := send(ctx)
		p.Collector.RecordCompletion(scheduled, time.Now(), outcome, status)
	}()
}

// Drain blocks until nothing is in flight or timeout elapses, reporting
// whether it emptied. Used between arms, where the next arm restarts the
// router pod and any request still outstanding would be attributed to it.
func (p *Pacer) Drain(ctx context.Context, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if p.Collector.InFlight() == 0 {
			return true
		}
		select {
		case <-ctx.Done():
			return p.Collector.InFlight() == 0
		case <-time.After(20 * time.Millisecond):
		}
	}
	return p.Collector.InFlight() == 0
}
