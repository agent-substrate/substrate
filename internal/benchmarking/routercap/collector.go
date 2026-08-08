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

// The in-memory tally of every request the generator scheduled — dispatch lag,
// latency, outcome and peak concurrency — sliceable by an arbitrary time window.

package routercap

import (
	"sync"
	"sync/atomic"
	"time"
)

// Outcome classifies how one request ended. Failures the generator produced
// (shed) mean the rig ran out, not the router, and must never be read as the
// router refusing load.
type Outcome string

const (
	// OutcomeOK is a 2xx whose body echoed back what was sent.
	OutcomeOK Outcome = "ok"
	// OutcomeHTTPError is a response with a >=400 status, including the 503s
	// Envoy generates when a circuit breaker trips.
	OutcomeHTTPError Outcome = "httperror"
	// OutcomeTransportError is a connection that never produced a response:
	// refused, reset, timed out, or EOF mid-body.
	OutcomeTransportError Outcome = "transport"
	// OutcomeBadBody is a 2xx whose payload did not round-trip: something
	// answered, but not the actor we addressed.
	OutcomeBadBody Outcome = "badbody"
	// OutcomeShed is a request the generator declined to send because its own
	// in-flight cap was reached. Never a statement about the router.
	OutcomeShed Outcome = "shed"
)

// completion is one finished request. Latency is measured from Scheduled, not
// Dispatched, to avoid coordinated omission.
type completion struct {
	Scheduled time.Time
	Completed time.Time
	Outcome   Outcome
	Status    int
}

// dispatch records that a request reached the wire. Emitted separately from
// its completion so a window's dispatch-lag statistics include requests still
// in flight when the window closes.
type dispatch struct {
	Scheduled  time.Time
	Dispatched time.Time
}

// peakSlot names one consumer of the in-flight high-water mark. Reading a slot
// resets it, so each consumer needs its own.
type peakSlot int

const (
	// peakAligned is the ~10s series in samples.jsonl.
	peakAligned peakSlot = iota
	// peakFine is the 1s generator-only series in fine.jsonl.
	peakFine
	numPeakSlots
)

// Collector accumulates raw per-request events and answers interval queries
// over them. Raw events rather than pre-aggregated buckets, because window
// boundaries come from cAdvisor's clock (see window.go) and are not known in
// advance.
type Collector struct {
	mu          sync.Mutex
	completions []completion
	dispatches  []dispatch

	inFlight atomic.Int64
	// maxInFlight holds one high-water slot per consumer. A single slot shared
	// by the ~10s and 1s series would let whichever read last hand the other a
	// maximum covering only the sliver since that read.
	maxInFlight [numPeakSlots]atomic.Int64

	// schedule answers "how many requests were due in this interval": the
	// pacer's schedule is deterministic, so offered load is arithmetic, not
	// measurement.
	schedule *Schedule
}

// NewCollector returns a Collector reporting offered load from sched.
func NewCollector(sched *Schedule) *Collector {
	return &Collector{schedule: sched}
}

// RecordDispatch notes that a request scheduled for sched reached the wire at
// at. It also raises the in-flight count, which RecordCompletion lowers.
func (c *Collector) RecordDispatch(sched, at time.Time) {
	n := c.inFlight.Add(1)
	for i := range c.maxInFlight {
		for {
			hi := c.maxInFlight[i].Load()
			if n <= hi || c.maxInFlight[i].CompareAndSwap(hi, n) {
				break
			}
		}
	}
	c.mu.Lock()
	c.dispatches = append(c.dispatches, dispatch{Scheduled: sched, Dispatched: at})
	c.mu.Unlock()
}

// RecordCompletion notes a finished request. Callers must pair it with exactly
// one RecordDispatch, except for OutcomeShed which never reached the wire and
// so is recorded with RecordShed instead.
func (c *Collector) RecordCompletion(sched, at time.Time, outcome Outcome, status int) {
	c.inFlight.Add(-1)
	c.mu.Lock()
	c.completions = append(c.completions, completion{
		Scheduled: sched, Completed: at, Outcome: outcome, Status: status,
	})
	c.mu.Unlock()
}

// RecordShed notes a request the generator refused to send. It is counted but
// excluded from the latency distribution.
func (c *Collector) RecordShed(sched, at time.Time) {
	c.mu.Lock()
	c.completions = append(c.completions, completion{
		Scheduled: sched, Completed: at, Outcome: OutcomeShed,
	})
	c.mu.Unlock()
}

// InFlight is the number of requests currently sent but unanswered.
func (c *Collector) InFlight() int64 { return c.inFlight.Load() }

// GenStats is everything the generator knows about one interval. Every field
// is scoped to the same [t0, t1) as the resource samples it will be written
// alongside.
type GenStats struct {
	// OfferedQPS is the rate the pacer was scheduled to produce, independent of
	// what happened.
	OfferedQPS float64 `json:"offered_qps"`
	// DispatchedQPS is the rate that actually reached the wire. Below offered
	// means the generator itself could not keep up.
	DispatchedQPS float64 `json:"dispatched_qps"`
	// AchievedQPS is the rate of completed requests, successful or not. Below
	// offered means the system did not keep up.
	AchievedQPS float64 `json:"achieved_qps"`
	// SuccessQPS counts only OutcomeOK. The gap to AchievedQPS is the error
	// rate expressed in the same units as the rest of the chart.
	SuccessQPS float64 `json:"success_qps"`

	// InFlightEnd is the concurrency at t1, and InFlightMax the high-water
	// mark during the window; both are sampled, not integrated. On an HTTP/1.1
	// upstream each in-flight request holds one connection and one ephemeral
	// source port.
	InFlightEnd int64 `json:"in_flight_end"`
	InFlightMax int64 `json:"in_flight_max"`

	// Latency is measured from scheduled send time and includes failures, so
	// a timeout raises the tail instead of vanishing from it.
	Latency LatencyStats `json:"latency"`
	// DispatchLag is scheduled-to-wire delay: the generator measuring itself.
	// Large means the generator fell behind and that part of the curve
	// describes the rig.
	DispatchLag LatencyStats `json:"dispatch_lag"`

	Outcomes map[Outcome]int `json:"outcomes"`
	Statuses map[int]int     `json:"statuses,omitempty"`
}

// Stats summarizes the interval [t0, t1). Completions are attributed by
// completion instant and dispatches by dispatch instant, so a slow request
// contributes its lag to one window and its latency to a later one.
func (c *Collector) Stats(t0, t1 time.Time) GenStats {
	return c.stats(t0, t1, peakAligned)
}

// FineStats is Stats for the 1s generator-only series. It differs only in
// which in-flight high-water slot it reads and resets.
func (c *Collector) FineStats(t0, t1 time.Time) GenStats {
	return c.stats(t0, t1, peakFine)
}

func (c *Collector) stats(t0, t1 time.Time, slot peakSlot) GenStats {
	secs := t1.Sub(t0).Seconds()
	if secs <= 0 {
		return GenStats{Outcomes: map[Outcome]int{}}
	}

	c.mu.Lock()
	var lats []time.Duration
	outcomes := map[Outcome]int{}
	statuses := map[int]int{}
	var completed, success int
	for _, cp := range c.completions {
		if cp.Completed.Before(t0) || !cp.Completed.Before(t1) {
			continue
		}
		completed++
		outcomes[cp.Outcome]++
		if cp.Status != 0 {
			statuses[cp.Status]++
		}
		if cp.Outcome == OutcomeOK {
			success++
		}
		if cp.Outcome != OutcomeShed {
			lats = append(lats, cp.Completed.Sub(cp.Scheduled))
		}
	}
	var lags []time.Duration
	var dispatched int
	for _, d := range c.dispatches {
		if d.Dispatched.Before(t0) || !d.Dispatched.Before(t1) {
			continue
		}
		dispatched++
		lags = append(lags, d.Dispatched.Sub(d.Scheduled))
	}
	c.mu.Unlock()

	return GenStats{
		OfferedQPS:    c.schedule.OfferedIn(t0, t1) / secs,
		DispatchedQPS: float64(dispatched) / secs,
		AchievedQPS:   float64(completed) / secs,
		SuccessQPS:    float64(success) / secs,
		InFlightEnd:   c.inFlight.Load(),
		InFlightMax:   c.maxInFlight[slot].Swap(c.inFlight.Load()),
		Latency:       summarize(lats),
		DispatchLag:   summarize(lags),
		Outcomes:      outcomes,
		Statuses:      statuses,
	}
}

// Prune drops events that completed or dispatched before cutoff. Callers must
// keep cutoff at or below the earliest interval they still intend to query —
// with two consumers, the older of their two frontiers.
func (c *Collector) Prune(cutoff time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	keptC := c.completions[:0]
	for _, cp := range c.completions {
		if !cp.Completed.Before(cutoff) {
			keptC = append(keptC, cp)
		}
	}
	c.completions = keptC
	keptD := c.dispatches[:0]
	for _, d := range c.dispatches {
		if !d.Dispatched.Before(cutoff) {
			keptD = append(keptD, d)
		}
	}
	c.dispatches = keptD
}
