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

// The runner: primes the actors, walks the ladder, and emits one aligned sample per
// cAdvisor window until the schedule ends or a fatal guard stops it.

package routercap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Client is what the runner needs from the load generator: a way to issue one
// request, and a way for the generator to report on itself. *Sender is the
// implementation; the interface exists so the orchestration can be tested
// without a socket.
type Client interface {
	Send(ctx context.Context) (Outcome, int)
	Stats() ClientStats
}

// Runner executes one arm: one Envoy CPU size, one ladder, one pair of output
// streams. Arms are separate processes, so this binary needs no write access
// to anything in the cluster.
type Runner struct {
	// Arm is the Envoy container's CPU limit in cores, and Pass distinguishes
	// repeats of the same arm. Both are stamped on every record so four arms'
	// files concatenate into one dataset without losing which is which.
	Arm  int
	Pass int

	Rungs  []Rung
	Client Client
	Sink   Sink

	Windows *WindowDriver
	// Envoy, Contention and Router are optional. A nil one leaves its section off every
	// record rather than filling it with zeros.
	Envoy      *EnvoyClient
	Contention *ContentionClient
	Router     *RouterClient

	Targets []Target
	Guards  GuardConfig

	PortRange           PortRange
	CircuitBreakerLimit int

	// MaxInFlight bounds the generator's own concurrency; reaching it is a rig
	// failure recorded as shed requests, not a result.
	MaxInFlight int64
	// TickCap bounds the pacer's sleep and so bounds the dispatch lag the
	// dispatch loop itself can introduce.
	TickCap time.Duration
	// FineInterval is the cadence of the generator-only series. Zero means 1s.
	FineInterval time.Duration
	// DrainTimeout bounds the wait for in-flight requests at the end of the
	// ladder. Whether it emptied is recorded, because an arm that ended with
	// requests outstanding hands them to whatever runs next.
	DrainTimeout time.Duration

	Log *slog.Logger

	sched     *Schedule
	collector *Collector

	// Envoy and router scrapes are taken and differenced entirely inside the
	// sampler goroutine, so they need no lock.
	prevEnvoy      EnvoyStats
	prevRouter     RouterStats
	prevContention ContentionStats
	haveEnvoy      bool
	haveRouter     bool
	haveContention bool

	mu             sync.Mutex
	fineFrontier   time.Time
	windowFrontier time.Time
}

// RunResult is what one arm produced, for the run header.
type RunResult struct {
	Arm  int `json:"arm_cores"`
	Pass int `json:"pass"`

	Rungs       []Rung `json:"rungs"`
	Windows     int    `json:"windows"`
	FineSamples int    `json:"fine_samples"`

	// EnvoyConcurrency is the worker-thread count Envoy reported; it must
	// equal Arm. Left unset, Envoy sizes it from the node's core count, and
	// the arm measures CFS throttling instead of the proxy.
	EnvoyConcurrency float64 `json:"envoy_concurrency"`
	// ClockSkewMs is the residual error in the alignment claim; see
	// WindowDriver.Skew.
	ClockSkewMs float64 `json:"clock_skew_ms"`

	// Drained says whether every request had completed when the arm ended.
	Drained bool `json:"drained"`
	// Interrupted marks an arm cut short by its context rather than by the
	// ladder finishing.
	Interrupted bool        `json:"interrupted,omitempty"`
	FatalTrips  []GuardTrip `json:"fatal_trips,omitempty"`
}

func (r *Runner) log() *slog.Logger {
	if r.Log != nil {
		return r.Log
	}
	return slog.Default()
}

func (r *Runner) validate() error {
	switch {
	case r.Client == nil:
		return fmt.Errorf("runner needs a client")
	case r.Sink == nil:
		return fmt.Errorf("runner needs a sink")
	case r.Windows == nil:
		return fmt.Errorf("runner needs a window driver: the whole series is aligned off its clock")
	case len(r.Rungs) == 0:
		return fmt.Errorf("runner needs at least one rung")
	}
	return nil
}

// Run executes the arm and returns once the ladder has finished, the guards
// have stopped it, or ctx is cancelled. Three concurrent parts — the pacer,
// the cAdvisor-clocked sampler, and the 1s generator-only series — share one
// collector of raw request events.
func (r *Runner) Run(ctx context.Context) (RunResult, error) {
	res := RunResult{Arm: r.Arm, Pass: r.Pass}
	if err := r.validate(); err != nil {
		return res, err
	}

	r.sched = &Schedule{}
	r.collector = NewCollector(r.sched)

	if err := r.prime(ctx); err != nil {
		return res, err
	}
	if skew, ok := r.Windows.Skew(); ok {
		res.ClockSkewMs = float64(skew) / float64(time.Millisecond)
	}

	// loadCtx is cancelled by a fatal guard; the sampler keeps its own ctx so
	// it can still write the record that explains why the load stopped.
	loadCtx, stopLoad := context.WithCancel(ctx)
	defer stopLoad()
	fineCtx, stopFine := context.WithCancel(ctx)
	defer stopFine()

	finishing := make(chan struct{})
	// Two wait groups, not one: the fine loop stops only after the sampler has
	// taken its last window, so waiting on both together would deadlock.
	var (
		samplerWG, fineWG sync.WaitGroup
		sampleErr         error
	)

	samplerWG.Add(1)
	go func() {
		defer samplerWG.Done()
		sampleErr = r.sampleLoop(ctx, finishing, stopLoad, &res)
	}()
	fineWG.Add(1)
	go func() {
		defer fineWG.Done()
		r.fineLoop(fineCtx, &res)
	}()

	pacer := &Pacer{Collector: r.collector, MaxInFlight: r.MaxInFlight, TickCap: r.TickCap}
	ladderErr := r.runLadder(loadCtx, pacer)
	res.Rungs = r.sched.Rungs()

	drainTimeout := r.DrainTimeout
	if drainTimeout <= 0 {
		drainTimeout = 30 * time.Second
	}
	res.Drained = pacer.Drain(ctx, drainTimeout)
	if !res.Drained {
		r.log().Warn("arm ended with requests still in flight",
			"arm", r.Arm, "pass", r.Pass, "in_flight", r.collector.InFlight())
	}

	// One more window after the load stops, so the ladder's final rung is
	// covered by an aligned record rather than truncated mid-interval. The fine
	// series keeps running through that wait and stops only afterwards.
	close(finishing)
	samplerWG.Wait()
	stopFine()
	fineWG.Wait()

	switch {
	case sampleErr != nil:
		return res, sampleErr
	case ladderErr != nil && !errors.Is(ladderErr, context.Canceled):
		return res, ladderErr
	case ctx.Err() != nil:
		res.Interrupted = true
		return res, ctx.Err()
	}
	return res, nil
}

// prime establishes the first boundary for every differenced source, so the
// first emitted window is a real interval rather than a delta against zero.
func (r *Runner) prime(ctx context.Context) error {
	if err := r.Windows.Prime(ctx); err != nil {
		return fmt.Errorf("prime cadvisor window: %w", err)
	}
	if r.Envoy != nil {
		s, err := r.Envoy.Scrape(ctx)
		if err != nil {
			// Fatal here, unlike mid-run: an admin endpoint unreachable before
			// any load has been offered is a broken rig.
			return fmt.Errorf("prime envoy stats: %w", err)
		}
		r.prevEnvoy, r.haveEnvoy = s, true
	}
	if r.Router != nil {
		s, err := r.Router.Scrape(ctx)
		if err != nil {
			return fmt.Errorf("prime router stats: %w", err)
		}
		r.prevRouter, r.haveRouter = s, true
	}
	return nil
}

// runLadder walks the rungs back to back. Rungs are not drained between steps:
// idling the system at a boundary would make the next rung's first seconds
// measure a cold connection pool rather than a running one.
func (r *Runner) runLadder(ctx context.Context, p *Pacer) error {
	for _, rung := range r.Rungs {
		if err := ctx.Err(); err != nil {
			return err
		}
		started := r.sched.Begin(rung, time.Now())
		r.log().Info("rung start",
			"arm", r.Arm, "pass", r.Pass, "rung", started.Index,
			"offered_qps", started.RateQPS, "hold", started.Hold)
		if err := p.RunRung(ctx, started, r.Client.Send); err != nil {
			return err
		}
	}
	return nil
}

// sampleLoop emits the aligned series. It ticks off cAdvisor's housekeeping
// clock rather than a local timer, which is the whole reason a vertical line
// through the four panels describes one moment.
func (r *Runner) sampleLoop(ctx context.Context, finishing <-chan struct{}, stopLoad func(), res *RunResult) error {
	for {
		w, err := r.Windows.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}

		s := r.buildRecord(ctx, w)
		res.Windows++
		if s.Envoy != nil && s.Envoy.Concurrency > 0 {
			res.EnvoyConcurrency = s.Envoy.Concurrency
		}
		if err := r.Sink.Sample(s); err != nil {
			return fmt.Errorf("write sample: %w", err)
		}
		r.noteFrontier(false, w.T1)
		r.prune()

		if fatal := FatalTrips(s.Guards); len(fatal) > 0 {
			// Stop the load first, then report. The record that names the trip
			// is already written, so the run directory explains itself.
			res.FatalTrips = fatal
			stopLoad()
			for _, t := range fatal {
				r.log().Error("rig guard tripped", "arm", r.Arm, "guard", t.Guard, "detail", t.Detail)
			}
			return &RigLimitedError{Trips: fatal}
		}

		select {
		case <-finishing:
			return nil
		default:
		}
	}
}

// buildRecord assembles one aligned sample. Every source is asked for the same
// [T0, T1) the window defines, and anything that could not be read lands in
// Missing or Errors rather than being silently zero.
func (r *Runner) buildRecord(ctx context.Context, w Window) Sample {
	containers, groups, spread, missing, errs := buildSample(w, r.Targets)

	s := Sample{
		Arm:               r.Arm,
		Pass:              r.Pass,
		Rung:              -1,
		T:                 w.Mid(),
		T0:                w.T0,
		T1:                w.T1,
		WindowSeconds:     w.Duration().Seconds(),
		WindowPolls:       w.Polls,
		AlignmentSpreadMs: float64(spread) / float64(time.Millisecond),
		Load:              r.collector.Stats(w.T0, w.T1),
		Client:            r.Client.Stats(),
		Containers:        containers,
		Groups:            groups,
		Missing:           missing,
		Errors:            errs,
	}
	// Rung -1 is a window outside any rung: before the first starts or after
	// the last ends. Kept, because an idle window immediately after saturation
	// is one of the more informative records in the run.
	if rung, warm, ok := r.sched.RungAt(s.T); ok {
		s.Rung, s.RungQPS, s.Warmup = rung.Index, rung.RateQPS, warm
	}

	if r.Envoy != nil {
		cur, err := r.Envoy.Scrape(ctx)
		switch {
		case err != nil:
			s.Errors = append(s.Errors, err.Error())
		case !r.haveEnvoy:
			r.prevEnvoy, r.haveEnvoy = cur, true
		default:
			// Rated over the two scrapes' own interval, not the window's: the
			// scrapes bracket the window, and using the window's length would
			// bias the per-second connection rate the worker guard reads.
			secs := cur.At.Sub(r.prevEnvoy.At).Seconds()
			d, derr := envoyDelta(r.prevEnvoy, cur, secs)
			if derr != nil {
				s.Errors = append(s.Errors, derr.Error())
			} else {
				s.Envoy = &d
			}
			r.prevEnvoy = cur
		}
	}

	// Attached to the Envoy section only when this window has one, so a failed
	// fetch reads as absent rather than as zero contention.
	if r.Contention != nil && s.Envoy != nil {
		cur, err := r.Contention.Scrape(ctx)
		switch {
		case err != nil:
			s.Errors = append(s.Errors, err.Error())
		case !r.haveContention:
			r.prevContention, r.haveContention = cur, true
		default:
			d := contentionDelta(r.prevContention, cur)
			s.Envoy.Contention = &d
			r.prevContention = cur
		}
	}

	if r.Router != nil {
		cur, err := r.Router.Scrape(ctx)
		switch {
		case err != nil:
			s.Errors = append(s.Errors, err.Error())
		case !r.haveRouter:
			r.prevRouter, r.haveRouter = cur, true
		default:
			d := routerDelta(r.prevRouter, cur)
			s.Router = &d
			r.prevRouter = cur
		}
	}

	var cxActive, newCxPerSec float64
	if s.Envoy != nil {
		if actor, ok := s.Envoy.Clusters[ActorClusterName]; ok {
			cxActive, newCxPerSec = actor.CxActive, actor.NewConnectionsPerSec
		}
	}
	s.Ports = portBudget(r.PortRange.Low, r.PortRange.High, r.CircuitBreakerLimit, cxActive, newCxPerSec)

	// After both scrapes: the breakdown needs Envoy's totals and the sidecar's
	// route duration together, and is nil if either is absent.
	s.Spans = latencySpans(s.Load, s.Envoy, s.Router)

	// Last, so the guards see the Envoy section they depend on.
	s.Guards = r.Guards.Check(&s)
	return s
}

// fineLoop emits the generator-only series: the cliff's shape is faster than
// the kubelet's housekeeping, so the aligned series cannot resolve it. It
// carries no resource fields so it cannot be mistaken for an aligned series.
func (r *Runner) fineLoop(ctx context.Context, res *RunResult) {
	interval := r.FineInterval
	if interval <= 0 {
		interval = time.Second
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()

	last := time.Now()
	r.noteFrontier(true, last)
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			fs := FineSample{
				Arm:  r.Arm,
				Pass: r.Pass,
				Rung: -1,
				T:    last.Add(now.Sub(last) / 2),
				T0:   last,
				T1:   now,
				Load: r.collector.FineStats(last, now),
			}
			if rung, warm, ok := r.sched.RungAt(fs.T); ok {
				fs.Rung, fs.RungQPS, fs.Warmup = rung.Index, rung.RateQPS, warm
			}
			if err := r.Sink.Fine(fs); err != nil {
				// Non-fatal: the fine series is supplementary, and losing it
				// must not cost the aligned series the run.
				r.log().Warn("write fine sample", "error", err)
			}
			res.FineSamples++
			last = now
			r.noteFrontier(true, last)
		}
	}
}

func (r *Runner) noteFrontier(fine bool, t time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if fine {
		r.fineFrontier = t
		return
	}
	r.windowFrontier = t
}

// prune drops raw request events both consumers have already summarized. The
// cutoff is the *older* of the two frontiers: the fine series runs ~10x ahead
// of the aligned one, and pruning to it would delete the events the aligned
// window is about to read.
func (r *Runner) prune() {
	r.mu.Lock()
	fine, window := r.fineFrontier, r.windowFrontier
	r.mu.Unlock()
	if fine.IsZero() || window.IsZero() {
		return
	}
	cutoff := window
	if fine.Before(cutoff) {
		cutoff = fine
	}
	r.collector.Prune(cutoff)
}
