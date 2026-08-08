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

// Tests for the pacer, the rung schedule, and the collector's windowed statistics.

package routercap

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSummarize(t *testing.T) {
	t.Run("NearestRankPicksAnObservedValue", func(t *testing.T) {
		// 1..100ms. Nearest rank puts p50 at the 50th value and p95 at the
		// 95th, both of which are latencies that actually occurred.
		var ds []time.Duration
		for i := 1; i <= 100; i++ {
			ds = append(ds, time.Duration(i)*time.Millisecond)
		}
		got := summarize(ds)
		for _, tc := range []struct {
			name string
			got  float64
			want float64
		}{
			{"count", float64(got.Count), 100},
			{"p50", got.P50Ms, 50},
			{"p95", got.P95Ms, 95},
			{"mean", got.MeanMs, 50.5},
		} {
			if math.Abs(tc.got-tc.want) > 1e-9 {
				t.Errorf("%s = %v, want %v", tc.name, tc.got, tc.want)
			}
		}
	})

	t.Run("UnsortedInputIsSorted", func(t *testing.T) {
		// Nearest rank puts p95 of three samples at the largest. Unsorted, the
		// same index would land on the 5ms sample instead.
		ds := []time.Duration{9 * time.Millisecond, time.Millisecond, 5 * time.Millisecond}
		if got := summarize(ds).P95Ms; got != 9 {
			t.Errorf("p95 = %v, want 9", got)
		}
	})

	t.Run("EmptyIsZeroNotPanic", func(t *testing.T) {
		if got := summarize(nil); got.Count != 0 || got.P95Ms != 0 {
			t.Errorf("summarize(nil) = %+v, want zero", got)
		}
	})

	t.Run("SingleSampleIsEveryPercentile", func(t *testing.T) {
		got := summarize([]time.Duration{7 * time.Millisecond})
		if got.P50Ms != 7 || got.P95Ms != 7 || got.MeanMs != 7 {
			t.Errorf("single-sample percentiles = %+v, want all 7ms", got)
		}
	})
}

func TestScheduleOfferedIn(t *testing.T) {
	base := time.Unix(1_800_000_000, 0)
	s := &Schedule{}
	s.Begin(Rung{Index: 0, RateQPS: 1000, Hold: 10 * time.Second}, base)
	s.Begin(Rung{Index: 1, RateQPS: 2000, Hold: 10 * time.Second}, base.Add(10*time.Second))

	tests := []struct {
		name   string
		t0, t1 time.Time
		want   float64
	}{
		{"WhollyInsideFirstRung", base, base.Add(5 * time.Second), 5000},
		{"WhollyInsideSecondRung", base.Add(12 * time.Second), base.Add(15 * time.Second), 6000},
		// The case that matters: a window driven by cAdvisor's clock does not
		// respect rung boundaries, so it must be credited each rung's share.
		{"StraddlesTheBoundary", base.Add(8 * time.Second), base.Add(13 * time.Second), 2000 + 6000},
		{"BeforeAnyRung", base.Add(-5 * time.Second), base, 0},
		{"AfterEveryRung", base.Add(20 * time.Second), base.Add(25 * time.Second), 0},
		{"SpansEverything", base.Add(-1 * time.Second), base.Add(21 * time.Second), 30000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := s.OfferedIn(tc.t0, tc.t1); math.Abs(got-tc.want) > 1e-6 {
				t.Errorf("OfferedIn = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestScheduleRungAt(t *testing.T) {
	base := time.Unix(1_800_000_000, 0)
	s := &Schedule{}
	s.Begin(Rung{Index: 3, RateQPS: 500, Hold: 45 * time.Second, Warmup: 10 * time.Second}, base)

	for _, tc := range []struct {
		name       string
		at         time.Time
		wantOK     bool
		wantWarmup bool
	}{
		{"AtStartIsWarmup", base, true, true},
		{"JustBeforeWarmupEnds", base.Add(9 * time.Second), true, true},
		{"AtWarmupEndIsMeasured", base.Add(10 * time.Second), true, false},
		{"LateInRungIsMeasured", base.Add(44 * time.Second), true, false},
		{"AtRungEndIsOutside", base.Add(45 * time.Second), false, false},
		{"BeforeRungIsOutside", base.Add(-time.Second), false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, warmup, ok := s.RungAt(tc.at)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if ok && r.Index != 3 {
				t.Errorf("rung index = %d, want 3", r.Index)
			}
			if warmup != tc.wantWarmup {
				t.Errorf("warmup = %v, want %v", warmup, tc.wantWarmup)
			}
		})
	}
}

func TestCollectorStats(t *testing.T) {
	base := time.Unix(1_800_000_000, 0)
	sched := &Schedule{}
	sched.Begin(Rung{RateQPS: 100, Hold: 10 * time.Second}, base)
	c := NewCollector(sched)

	// Three requests completing inside [base+1s, base+2s), each scheduled 1s
	// before it completed, plus one that completes after the window.
	for i, off := range []time.Duration{1100, 1200, 1300} {
		at := base.Add(off * time.Millisecond)
		c.RecordDispatch(at.Add(-time.Second), at.Add(-time.Second))
		c.RecordCompletion(at.Add(-time.Second), at, OutcomeOK, 200)
		_ = i
	}
	late := base.Add(5 * time.Second)
	c.RecordDispatch(late, late)
	c.RecordCompletion(late, late.Add(time.Second), OutcomeHTTPError, 503)

	got := c.Stats(base.Add(time.Second), base.Add(2*time.Second))

	if got.OfferedQPS != 100 {
		t.Errorf("offered = %v, want 100 (from the schedule, not from what completed)", got.OfferedQPS)
	}
	if got.AchievedQPS != 3 {
		t.Errorf("achieved = %v, want 3", got.AchievedQPS)
	}
	if got.SuccessQPS != 3 {
		t.Errorf("success = %v, want 3", got.SuccessQPS)
	}
	// Every one of the three waited exactly a second from its scheduled time.
	if got.Latency.Count != 3 || math.Abs(got.Latency.P50Ms-1000) > 1 {
		t.Errorf("latency = %+v, want 3 samples at ~1000ms", got.Latency)
	}
	if got.Outcomes[OutcomeHTTPError] != 0 {
		t.Errorf("the 503 completed outside the window but was counted: %+v", got.Outcomes)
	}
}

func TestCollectorLatencyIsFromScheduledTime(t *testing.T) {
	// A request dispatched 400ms late and answered 100ms later took 500ms from
	// the caller's view. Reporting 100ms would be coordinated omission.
	base := time.Unix(1_800_000_000, 0)
	sched := &Schedule{}
	sched.Begin(Rung{RateQPS: 1, Hold: time.Minute}, base)
	c := NewCollector(sched)

	scheduled := base
	dispatched := base.Add(400 * time.Millisecond)
	completed := dispatched.Add(100 * time.Millisecond)
	c.RecordDispatch(scheduled, dispatched)
	c.RecordCompletion(scheduled, completed, OutcomeOK, 200)

	got := c.Stats(base, base.Add(time.Second))
	if math.Abs(got.Latency.P50Ms-500) > 1 {
		t.Errorf("latency p50 = %vms, want 500ms measured from the scheduled send time", got.Latency.P50Ms)
	}
	if math.Abs(got.DispatchLag.P50Ms-400) > 1 {
		t.Errorf("dispatch lag p50 = %vms, want 400ms", got.DispatchLag.P50Ms)
	}
}

func TestCollectorDispatchLagIncludesUnfinishedRequests(t *testing.T) {
	// A request dispatched inside the window but still in flight when it
	// closes must still contribute its lag. Waiting for completions would drop
	// exactly the requests the system is struggling with.
	base := time.Unix(1_800_000_000, 0)
	sched := &Schedule{}
	sched.Begin(Rung{RateQPS: 1, Hold: time.Minute}, base)
	c := NewCollector(sched)

	c.RecordDispatch(base, base.Add(250*time.Millisecond))

	got := c.Stats(base, base.Add(time.Second))
	if got.DispatchLag.Count != 1 {
		t.Fatalf("dispatch lag count = %d, want 1 for an in-flight request", got.DispatchLag.Count)
	}
	if got.AchievedQPS != 0 {
		t.Errorf("achieved = %v, want 0 — nothing completed yet", got.AchievedQPS)
	}
	if got.InFlightEnd != 1 {
		t.Errorf("in-flight = %d, want 1", got.InFlightEnd)
	}
}

func TestCollectorShedIsExcludedFromLatency(t *testing.T) {
	// A shed request never reached the server, so it has no server latency to
	// contribute. Counting a zero would drag the reported percentiles down at
	// precisely the moment the rig was failing.
	base := time.Unix(1_800_000_000, 0)
	sched := &Schedule{}
	sched.Begin(Rung{RateQPS: 10, Hold: time.Minute}, base)
	c := NewCollector(sched)

	c.RecordDispatch(base, base)
	c.RecordCompletion(base, base.Add(900*time.Millisecond), OutcomeOK, 200)
	for i := 0; i < 5; i++ {
		c.RecordShed(base, base.Add(100*time.Millisecond))
	}

	got := c.Stats(base, base.Add(time.Second))
	if got.Latency.Count != 1 {
		t.Errorf("latency count = %d, want 1 (shed requests excluded)", got.Latency.Count)
	}
	if math.Abs(got.Latency.P50Ms-900) > 1 {
		t.Errorf("latency p50 = %v, want 900ms undiluted by shed zeroes", got.Latency.P50Ms)
	}
	if got.Outcomes[OutcomeShed] != 5 {
		t.Errorf("shed count = %d, want 5 — shed must still be visible", got.Outcomes[OutcomeShed])
	}
	if got.AchievedQPS != 6 {
		t.Errorf("achieved = %v, want 6: shed requests are accounted for, just not timed", got.AchievedQPS)
	}
}

func TestCollectorPrune(t *testing.T) {
	base := time.Unix(1_800_000_000, 0)
	sched := &Schedule{}
	sched.Begin(Rung{RateQPS: 1, Hold: time.Minute}, base)
	c := NewCollector(sched)

	for i := 0; i < 10; i++ {
		at := base.Add(time.Duration(i) * time.Second)
		c.RecordDispatch(at, at)
		c.RecordCompletion(at, at, OutcomeOK, 200)
	}
	c.Prune(base.Add(5 * time.Second))

	if got := c.Stats(base, base.Add(5*time.Second)); got.AchievedQPS != 0 {
		t.Errorf("pruned interval still reports %v achieved", got.AchievedQPS)
	}
	if got := c.Stats(base.Add(5*time.Second), base.Add(10*time.Second)); got.AchievedQPS != 1 {
		t.Errorf("retained interval reports %v achieved, want 1/s", got.AchievedQPS)
	}
}

func TestPacerHoldsItsSchedule(t *testing.T) {
	// The pacer must emit the requested count at the requested spacing, and
	// must do it without letting a slow server slow the schedule down.
	sched := &Schedule{}
	c := NewCollector(sched)
	p := &Pacer{Collector: c, MaxInFlight: 10_000, TickCap: time.Millisecond}

	const rate = 500.0
	const hold = 400 * time.Millisecond
	rung := sched.Begin(Rung{RateQPS: rate, Hold: hold}, time.Now())

	var sent atomic.Int64
	start := time.Now()
	if err := p.RunRung(context.Background(), rung, func(ctx context.Context) (Outcome, int) {
		sent.Add(1)
		// Every response takes far longer than the inter-arrival gap. A closed
		// loop would collapse to ~1 request per 50ms here.
		time.Sleep(50 * time.Millisecond)
		return OutcomeOK, 200
	}); err != nil {
		t.Fatalf("RunRung: %v", err)
	}
	elapsed := time.Since(start)

	// The dispatch loop should finish when the schedule does, not when the
	// responses do.
	if elapsed > hold+150*time.Millisecond {
		t.Errorf("RunRung took %v for a %v rung: the loop is waiting on responses", elapsed, hold)
	}
	if !p.Drain(context.Background(), 5*time.Second) {
		t.Fatalf("in-flight did not drain: %d left", c.InFlight())
	}

	want := int64(rate * hold.Seconds())
	if got := sent.Load(); got != want {
		t.Errorf("dispatched %d requests, want %d", got, want)
	}

	stats := c.Stats(rung.StartAt, rung.End())
	if math.Abs(stats.OfferedQPS-rate) > 1 {
		t.Errorf("offered = %v, want %v", stats.OfferedQPS, rate)
	}
	// Real timers, so allow slack; the point is that lag is small and measured,
	// not that it is zero.
	if stats.DispatchLag.P95Ms > 25 {
		t.Errorf("dispatch lag p95 = %vms; the pacer is not keeping its own schedule", stats.DispatchLag.P95Ms)
	}
}

func TestPacerShedsRatherThanBlockingAtTheInFlightCap(t *testing.T) {
	// Blocking would turn the open loop closed at the worst possible moment.
	// Shedding keeps the schedule and records that the generator, not the
	// system, dropped the request.
	sched := &Schedule{}
	c := NewCollector(sched)
	p := &Pacer{Collector: c, MaxInFlight: 5, TickCap: time.Millisecond}

	release := make(chan struct{})
	var wg sync.WaitGroup
	rung := sched.Begin(Rung{RateQPS: 500, Hold: 200 * time.Millisecond}, time.Now())

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = p.RunRung(context.Background(), rung, func(ctx context.Context) (Outcome, int) {
			<-release
			return OutcomeOK, 200
		})
	}()
	wg.Wait()
	close(release)

	if !p.Drain(context.Background(), 5*time.Second) {
		t.Fatalf("did not drain: %d in flight", c.InFlight())
	}
	stats := c.Stats(rung.StartAt, rung.End().Add(time.Second))
	if stats.Outcomes[OutcomeShed] == 0 {
		t.Fatalf("nothing was shed despite a cap of 5 and 100 requests: %+v", stats.Outcomes)
	}
	if got := c.InFlight(); got != 0 {
		t.Errorf("in-flight = %d after drain, want 0 — shed requests must not leak the counter", got)
	}
}

func TestPacerStopsOnContextCancel(t *testing.T) {
	sched := &Schedule{}
	c := NewCollector(sched)
	p := &Pacer{Collector: c, MaxInFlight: 1000, TickCap: time.Millisecond}
	rung := sched.Begin(Rung{RateQPS: 100, Hold: time.Minute}, time.Now())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := p.RunRung(ctx, rung, func(ctx context.Context) (Outcome, int) { return OutcomeOK, 200 })
	if err == nil {
		t.Error("RunRung returned nil on a cancelled context, want the context error")
	}
	if time.Since(start) > 2*time.Second {
		t.Errorf("RunRung ran %v past cancellation", time.Since(start))
	}
}
