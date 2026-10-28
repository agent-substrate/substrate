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

package load

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.ate.dev/substrate/benchmarking/atunnel/internal/report"
)

// pacer issues units of work at a fixed rate whatever each one takes: open
// loop. Each unit carries the instant it was due, and latency is measured from
// then, so a generator that falls behind shows it as latency and lag instead
// of quietly offering less load.
type pacer struct {
	rate     float64
	duration time.Duration
	warmup   time.Duration
	workers  int
	// maxPending bounds units queued for a free worker; beyond it they are
	// skipped and counted.
	maxPending int

	scheduled atomic.Uint64
	skipped   atomic.Uint64
	lag       samples
}

// unit is one scheduled piece of work.
type unit struct {
	due time.Time
	// measured is false during warmup.
	measured bool
}

// run calls work for every unit until the duration is over or ctx ends, with
// at most workers in flight, and waits for them to finish.
func (p *pacer) run(ctx context.Context, work func(context.Context, unit)) {
	queue := make(chan unit, p.maxPending)
	var wg sync.WaitGroup
	for range p.workers {
		wg.Go(func() {
			for u := range queue {
				if u.measured {
					p.lag.record(time.Since(u.due))
				}
				work(ctx, u)
			}
		})
	}
	defer wg.Wait()
	defer close(queue)

	start := time.Now()
	interval := time.Duration(float64(time.Second) / p.rate)
	timer := time.NewTimer(0)
	defer timer.Stop()
	for i := 0; ; i++ {
		due := start.Add(time.Duration(i) * interval)
		if due.Sub(start) >= p.duration {
			return
		}
		if wait := time.Until(due); wait > 0 {
			timer.Reset(wait)
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
		} else if ctx.Err() != nil {
			return
		}
		p.scheduled.Add(1)
		select {
		case queue <- unit{due: due, measured: due.Sub(start) >= p.warmup}:
		default:
			p.skipped.Add(1)
		}
	}
}

// samples collects durations. Runs are short enough to keep every sample, so
// percentiles are exact.
type samples struct {
	mu sync.Mutex
	d  []time.Duration
}

func (s *samples) record(d time.Duration) {
	s.mu.Lock()
	s.d = append(s.d, d)
	s.mu.Unlock()
}

func (s *samples) percentiles() report.Percentiles {
	s.mu.Lock()
	d := s.d
	s.d = nil
	s.mu.Unlock()
	if len(d) == 0 {
		return report.Percentiles{}
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	var sum time.Duration
	for _, v := range d {
		sum += v
	}
	at := func(q float64) float64 {
		return ms(d[min(len(d)-1, int(q*float64(len(d))))])
	}
	return report.Percentiles{
		P50:  at(0.50),
		P90:  at(0.90),
		P99:  at(0.99),
		Max:  ms(d[len(d)-1]),
		Mean: ms(sum / time.Duration(len(d))),
	}
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
