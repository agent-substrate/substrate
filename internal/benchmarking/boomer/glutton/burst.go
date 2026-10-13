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

package glutton

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/boomerutil"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	bmetrics "github.com/agent-substrate/substrate/internal/benchmarking/boomer/metrics"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"github.com/myzhan/boomer"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	burstUserClass = "BurstUser"

	// Gap between readiness pings once ResumeActor has returned.
	burstReadyPingGap = 100 * time.Millisecond
	// Budget for one round of DeleteActor calls.
	burstDeleteTimeout = 60 * time.Second
	// Cap on concurrent DeleteActor calls.
	burstDeleteConcurrency = 32
)

func init() {
	userclass.Add(userclass.Entry{
		Name:       "burst",
		LocustFile: "burst.py",
		UserClass:  burstUserClass,
		Init:       initBurst,
	})
}

// initBurst returns BurstUser's task and shutdown hooks. Like spawn, each
// worker process runs one burst, driven by the first VU. The phase barriers
// live in this process, so a burst run uses one user on one worker (-u 1).
func initBurst(cfg *userclass.Config) (taskFn func(), shutdown func(context.Context)) {
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer("substrate-boomer/glutton-burst")
	}
	rt := newBurstRuntime(cfg)
	// A stop from the web UI or the master ends the run, which then deletes
	// its actors. boomer holds its event bus lock while the handler runs, so
	// the handler only cancels.
	if err := boomer.Events.Subscribe(boomer.EVENT_STOP, rt.cancelRun); err != nil {
		slog.Error("burst: subscribe to boomer:stop failed", slog.String("err", err.Error()))
	}
	return rt.iterate, rt.shutdown
}

// burstRuntime runs one burst per worker process, so a restart needs a new
// worker.
type burstRuntime struct {
	cfg       *userclass.Config
	runOnce   sync.Once
	runCtx    context.Context
	cancelRun context.CancelFunc
	// done is closed once the run has deleted its actors.
	done chan struct{}
}

func newBurstRuntime(cfg *userclass.Config) *burstRuntime {
	runCtx, cancelRun := context.WithCancel(context.Background())
	return &burstRuntime{cfg: cfg, runCtx: runCtx, cancelRun: cancelRun, done: make(chan struct{})}
}

func (r *burstRuntime) iterate() {
	r.runOnce.Do(func() {
		defer close(r.done)
		r.coordinate()
	})
	// Never returns, as in spawn: boomer calls Fn in a loop, and returning
	// would spin on the spent runOnce.
	select {}
}

// coordinate runs the burst until a stop or until no actor is left, then
// deletes every actor it created.
func (r *burstRuntime) coordinate() {
	bmetrics.UpdateUsers(burstUserClass, 1)
	defer bmetrics.UpdateUsers(burstUserClass, -1)
	b := newBurstRun(r.runCtx, r.cfg)
	defer b.teardown()
	b.run()
}

// shutdown ends the run and waits until its actors are deleted, or until
// ctx is done.
func (r *burstRuntime) shutdown(ctx context.Context) {
	r.cancelRun()
	// If no run started, this takes runOnce so that none can, and closes
	// done. If one did, it waits for that run in the background and does
	// nothing.
	go r.runOnce.Do(func() { close(r.done) })
	select {
	case <-r.done:
	case <-ctx.Done():
		slog.Warn("burst: shutdown deadline passed before the actors were deleted; some may leak")
	}
}

// burstRun is the run's cohort and the settings frozen at its start.
type burstRun struct {
	ctx context.Context
	// live is the operator's current config, read again before each batch
	// for the wait and live windows and the pings per wake.
	live     *dynconfig.Holder
	total    int
	deadline time.Duration
	actors   []*burstActor
}

// burstActor is one cohort member. Its gluttonActor reads the settings
// frozen at the run's start.
type burstActor struct {
	*gluttonActor
	// created: CreateActor was sent, so the actor may exist and teardown
	// must delete it.
	created bool
	// deleteDone: DeleteActor succeeded or found the actor already gone
	// (NotFound), so later delete rounds skip it.
	deleteDone bool
	// retired: the actor failed a phase, so it is deleted and left out of
	// every later phase.
	retired bool
}

// newBurstRun freezes the run's settings. Values set in the web UI form
// (dynconfig) override the flags. A form change made during the run has no
// effect, except on the wait and live windows and the pings per wake.
func newBurstRun(ctx context.Context, cfg *userclass.Config) *burstRun {
	dyn := cfg.Dyn.Load()
	b := &burstRun{ctx: ctx, live: cfg.Dyn, total: cfg.TotalActors, deadline: cfg.ActorDeadline}
	if dyn.TotalActors > 0 {
		b.total = dyn.TotalActors
	}
	if dyn.ActorDeadline > 0 {
		b.deadline = dyn.ActorDeadline
	}
	frozen := *cfg
	frozen.Dyn = dynconfig.NewHolder(dyn)
	runID := uuid.NewString()[:8]
	b.actors = make([]*burstActor, max(b.total, 0))
	for i := range b.actors {
		b.actors[i] = &burstActor{gluttonActor: &gluttonActor{
			cfg:         &frozen,
			actorName:   fmt.Sprintf("burst-%s-%d", runID, i+1),
			firstResume: true,
		}}
	}
	return b
}

// burstPhase names one burst's stats rows.
type burstPhase struct {
	actorMetric  string // per actor: its own operation's duration
	decilePrefix string // <prefix>_<k>pct: burst start until k% of the cohort is done
	allMetric    string // burst start until the last success
}

var (
	firstResumePhase  = burstPhase{"ActorTimeToFirstResume", "TimeToFirstResume", "TimeToAllFirstResumed"}
	resumePhase       = burstPhase{"ActorTimeToResume", "TimeToResume", "TimeToAllResumed"}
	firstSuspendPhase = burstPhase{"ActorTimeToFirstSuspend", "TimeToFirstSuspend", "TimeToAllFirstSuspended"}
	suspendPhase      = burstPhase{"ActorTimeToSuspend", "TimeToSuspend", "TimeToAllSuspended"}
)

// run creates the cohort, then repeats resume burst, churn, suspend burst
// until a stop or until no actor is left. Batch 1 resumes every actor from
// the template's golden snapshot and later batches from each actor's own
// snapshot, so batch 1 books its own rows.
func (b *burstRun) run() {
	if b.total < 1 || b.deadline <= 0 {
		slog.Error("burst: run not started: needs --total-actors of at least 1 and --actor-deadline above 0",
			slog.Int("total_actors", b.total), slog.Duration("actor_deadline", b.deadline))
		return
	}
	frozen := b.actors[0].cfg.Dyn.Load()
	slog.Info("burst: run starting",
		slog.Int("total_actors", b.total),
		slog.Duration("actor_deadline", b.deadline),
		slog.String("lifecycle_mode", frozen.LifecycleMode),
		slog.String("mem_target", frozen.MemTarget),
		slog.Int("cpu_cores", frozen.CPUCores))
	ctx, cancel := context.WithTimeout(b.ctx, b.deadline)
	err := retryWithBackoff(ctx, func(int) error { return b.actors[0].ensureAtespace(ctx) })
	cancel()
	if err != nil {
		slog.Error("burst: run not started: CreateAtespace failed", slog.String("err", err.Error()))
		return
	}
	b.setup()

	for batch := 1; b.ctx.Err() == nil; batch++ {
		active := b.active()
		if len(active) == 0 {
			slog.Warn("burst: every actor is retired; the run is over")
			return
		}
		if batch > 1 {
			cfg := b.live.Load()
			if !sleepCtx(b.ctx, uniformWait(cfg.MinWait, cfg.MaxWait)) {
				return
			}
		}
		first := batch == 1
		resumePh, suspendPh := resumePhase, suspendPhase
		if first {
			resumePh, suspendPh = firstResumePhase, firstSuspendPhase
		}

		resumed, resumeTimes := b.burst(resumePh, active, b.resumeOne)
		live := b.churn(resumed, first)
		if b.ctx.Err() != nil {
			return
		}
		suspended, suspendTimes := b.burst(suspendPh, live, b.suspendOne)
		if b.ctx.Err() != nil {
			return
		}
		slog.Info("burst: batch done",
			slog.Int("batch", batch),
			slog.Int("cohort", len(active)),
			slog.Int("resumed", len(resumed)),
			slog.Int("after_churn", len(live)),
			slog.Int("suspended", len(suspended)),
			slog.Int("retired_total", len(b.actors)-len(b.active())),
			slog.Duration("resume_p50", percentile(resumeTimes, 50)),
			slog.Duration("resume_p90", percentile(resumeTimes, 90)),
			slog.Duration("resume_max", percentile(resumeTimes, 100)),
			slog.Duration("suspend_p50", percentile(suspendTimes, 50)),
			slog.Duration("suspend_p90", percentile(suspendTimes, 90)),
			slog.Duration("suspend_max", percentile(suspendTimes, 100)))
	}
}

// setup creates the cohort; every actor starts SUSPENDED on the template's
// golden snapshot. An actor that cannot be created is retired, and its
// CreateActor rows show why.
func (b *burstRun) setup() {
	b.forEach(b.actors, b.deadline, func(ctx context.Context, _ int, a *burstActor) {
		a.created = true
		err := retryWithBackoff(ctx, func(attempt int) error {
			err := a.create(ctx)
			if createLanded(attempt, err) {
				return nil
			}
			return err
		})
		a.retired = err != nil
	})
	b.deleteRetired()
	if b.ctx.Err() == nil {
		slog.Info("burst: setup done", slog.Int("created", len(b.active())), slog.Int("wanted", b.total))
	}
}

// resumeOne resumes a and waits until it answers through the router.
func (b *burstRun) resumeOne(ctx context.Context, a *burstActor) error {
	if err := retryWithBackoff(ctx, func(int) error { return a.resume(ctx) }); err != nil {
		return err
	}
	for {
		err := b.readyPing(ctx, a)
		if err == nil {
			return nil
		}
		if !sleepCtx(ctx, burstReadyPingGap) {
			return fmt.Errorf("not ready before the deadline: %w", err)
		}
	}
}

// readyPing sends one ping through the router and books it on burst's
// GluttonReadyPing row. It is gluttonActor.ping with the error returned, so
// resumeOne can tell when the actor answers.
func (b *burstRun) readyPing(ctx context.Context, a *burstActor) error {
	ctx, span := a.cfg.Tracer.Start(ctx, "GluttonReadyPing")
	defer span.End()

	message := uuid.NewString()
	pong := &gluttonpb.PingResponse{}
	start := time.Now()
	err := a.postProto(ctx, pingPath, &gluttonpb.PingRequest{Message: message}, pong)
	latency := time.Since(start)
	if err == nil && pong.GetMessage() != message {
		err = fmt.Errorf("ping echo mismatch: sent=%q recv=%q", message, pong.GetMessage())
	}
	boomerutil.LogSampledTrace(span, "GluttonReadyPing", latency, boomerutil.SourceClient, err)
	if err != nil {
		b.recordFailure("http", "GluttonReadyPing", latency, err.Error())
		return err
	}
	b.recordSuccess("http", "GluttonReadyPing", latency)
	return nil
}

// suspendOne suspends or pauses a, per the frozen lifecycle mode.
func (b *burstRun) suspendOne(ctx context.Context, a *burstActor) error {
	return retryWithBackoff(ctx, func(int) error { return a.hibernate(ctx) })
}

// churn runs the workloads on the resumed actors and keeps them running for
// one live window drawn for the whole batch. It returns the actors still in
// the cohort.
func (b *burstRun) churn(actors []*burstActor, first bool) []*burstActor {
	if len(actors) == 0 {
		return nil
	}
	cfg := b.live.Load()
	window := uniformWait(cfg.MinLive, cfg.MaxLive)
	end := time.Now().Add(window)
	maxPings := max(cfg.MaxPingsPerWake, 1)
	b.forEach(actors, b.deadline+window, func(ctx context.Context, _ int, a *burstActor) {
		if b.work(ctx, a, first) {
			a.pingWindow(ctx, end, maxPings)
		}
	})
	sleepCtx(b.ctx, time.Until(end))
	b.deleteRetired()
	return activeOf(actors)
}

// work runs a's workloads under the actor deadline and reports whether a
// stays in the cohort. A step whose flag is unset does nothing.
//
// Batch 1 fills RAM, then starts the CPU load, so the first suspend
// snapshots the actor at its full working set. It skips the churn, as the
// fill just wrote fresh random bytes. Either step failing retires the
// actor.
//
// Later batches read the RAM the snapshot kept, then churn part of it. A
// failed step books its own row and the actor stays; only a step still
// running at the deadline retires it. glutton's CPU goroutines survive
// suspend and resume, so the CPU load is never restarted.
func (b *burstRun) work(ctx context.Context, a *burstActor, first bool) bool {
	ctx, cancel := context.WithTimeout(ctx, b.deadline)
	defer cancel()
	if first {
		a.ensureRAMFilled(ctx)
		if a.ramFilled {
			a.ensureCPULoad(ctx)
		}
		switch {
		case !a.ramFilled:
			b.retire(a, "RetiredSetup", "GluttonFillRAM failed")
		case !a.cpuLoaded:
			b.retire(a, "RetiredSetup", "GluttonUseCPU failed")
		}
		return !a.retired
	}
	a.readRAM(ctx)
	if ctx.Err() == nil {
		a.churnRAM(ctx)
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		b.retire(a, "RetiredChurn", "a workload step was still running at the actor deadline")
		return false
	}
	return true
}

// pingWindow pings right away, then up to maxPings-1 more times, each after
// a random gap in [minPingGap, maxPingGap), while the window lasts. A
// failed ping is a measurement, not a reason to retire the actor.
func (a *burstActor) pingWindow(ctx context.Context, end time.Time, maxPings int) {
	if ctx.Err() != nil {
		return
	}
	a.ping(ctx)
	for sent := 1; sent < maxPings; sent++ {
		gap := minPingGap + time.Duration(rand.Float64()*float64(maxPingGap-minPingGap))
		if time.Now().Add(gap).After(end) || !sleepCtx(ctx, gap) {
			return
		}
		a.ping(ctx)
	}
}

// burst runs op on every actor at once behind one barrier, retires the
// actors it failed on, and books the phase's rows. It returns the actors op
// succeeded on and their sorted times from the burst's start.
func (b *burstRun) burst(p burstPhase, actors []*burstActor, op func(context.Context, *burstActor) error) ([]*burstActor, []time.Duration) {
	if len(actors) == 0 {
		return nil, nil
	}
	start := time.Now()
	ok := make([]bool, len(actors))
	doneAt := make([]time.Duration, len(actors))
	b.forEach(actors, b.deadline, func(ctx context.Context, i int, a *burstActor) {
		opStart := time.Now()
		err := op(ctx, a)
		elapsed := time.Since(opStart)
		if err != nil {
			a.retired = true
			b.recordFailure("summary", p.actorMetric, elapsed, err.Error())
			return
		}
		ok[i], doneAt[i] = true, time.Since(start)
		b.recordSuccess("summary", p.actorMetric, elapsed)
	})
	b.deleteRetired()

	var succeeded []*burstActor
	var times []time.Duration
	for i, a := range actors {
		if ok[i] {
			succeeded = append(succeeded, a)
			times = append(times, doneAt[i])
		}
	}
	slices.Sort(times)
	b.bookPhase(p, len(actors), times)
	return succeeded, times
}

// bookPhase books the k% marks and the last success of one burst over a
// cohort of n actors; times holds the successes' sorted durations. A burst
// a stop cut short books nothing, as the record helpers drop every row
// after a stop.
func (b *burstRun) bookPhase(p burstPhase, n int, times []time.Duration) {
	if len(times) == 0 {
		b.recordFailure("summary", p.allMetric, 0, "no actor succeeded")
		return
	}
	for i, d := range times {
		for _, k := range firedDeciles(i+1, n) {
			b.recordSuccess("summary", fmt.Sprintf("%s_%dpct", p.decilePrefix, k), d)
		}
	}
	b.recordSuccess("summary", p.allMetric, times[len(times)-1])
}

// forEach calls fn on every actor at once, each under its own timeout, and
// returns once all calls have finished.
func (b *burstRun) forEach(actors []*burstActor, timeout time.Duration, fn func(ctx context.Context, i int, a *burstActor)) {
	runEach(b.ctx, len(actors), len(actors), func(ctx context.Context, i int) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		fn(ctx, i, actors[i])
	})
}

// runEach calls fn for every index in [0, n) with at most conc calls in
// flight, starts no new call once ctx is done, and returns once all started
// calls have finished.
func runEach(ctx context.Context, n, conc int, fn func(ctx context.Context, i int)) {
	var next atomic.Int64
	var wg sync.WaitGroup
	for range min(conc, n) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= n || ctx.Err() != nil {
					return
				}
				fn(ctx, i)
			}
		}()
	}
	wg.Wait()
}

// retire takes a out of the cohort and books why.
func (b *burstRun) retire(a *burstActor, reason, msg string) {
	a.retired = true
	b.recordFailure("actor", reason, 0, msg)
}

// deleteRetired deletes the actors retired since the last call, so a stuck
// RUNNING or SUSPENDING actor does not keep holding a worker. It runs on
// the run's context: a stop ends the round, and the teardown deletes
// whatever is left.
func (b *burstRun) deleteRetired() {
	if b.ctx.Err() != nil {
		return
	}
	var retired []*burstActor
	for _, a := range b.actors {
		if a.retired {
			retired = append(retired, a)
		}
	}
	ctx, cancel := context.WithTimeout(b.ctx, burstDeleteTimeout)
	defer cancel()
	deleteActors(ctx, retired)
}

// teardown deletes every actor not yet deleted, in one round on its own
// context, so a stop does not cut it short.
func (b *burstRun) teardown() {
	ctx, cancel := context.WithTimeout(context.Background(), burstDeleteTimeout)
	defer cancel()
	if left := deleteActors(ctx, b.actors); left > 0 {
		slog.Warn("burst: some actors were not deleted and may leak", slog.Int("not_deleted", left))
	}
}

// deleteActors deletes the listed actors that may exist and are not yet
// deleted, at most burstDeleteConcurrency at a time, and returns how many
// are still not deleted. A failed delete is retried with backoff until it
// lands, the error is terminal, or ctx is done: under load, a stuck actor
// left behind keeps holding its worker. NotFound means the actor is
// already gone. Any actor still not deleted waits for the next round.
func deleteActors(ctx context.Context, actors []*burstActor) int {
	var todo []*burstActor
	for _, a := range actors {
		if a.created && !a.deleteDone {
			todo = append(todo, a)
		}
	}
	runEach(ctx, len(todo), burstDeleteConcurrency, func(ctx context.Context, i int) {
		a := todo[i]
		err := retryWithBackoff(ctx, func(int) error { return a.deleteActor(ctx) })
		a.deleteDone = err == nil || status.Code(err) == codes.NotFound
	})
	left := 0
	for _, a := range todo {
		if !a.deleteDone {
			left++
		}
	}
	return left
}

// deleteActor deletes a in any state. Unlike gluttonActor.delete it returns
// the error, so a failed delete can be retried.
func (a *burstActor) deleteActor(ctx context.Context) error {
	return a.tracedCall(ctx, "DeleteActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := a.cfg.APIStub.DeleteActor(callCtx, &ateapipb.DeleteActorRequest{
			Actor:    a.ref(),
			AnyState: true,
		}, grpc.Trailer(tr))
		return err
	})
}

func (b *burstRun) active() []*burstActor { return activeOf(b.actors) }

func activeOf(actors []*burstActor) []*burstActor {
	var out []*burstActor
	for _, a := range actors {
		if !a.retired {
			out = append(out, a)
		}
	}
	return out
}

// recordSuccess and recordFailure book one of burst's own rows unless the
// run has stopped, as spawn does: a stop cuts the burst short, so its
// numbers are partial, and the requests it abandons are not failures of the
// system under test.
func (b *burstRun) recordSuccess(category, name string, latency time.Duration) {
	if b.ctx.Err() != nil {
		return
	}
	bmetrics.RecordSuccess(category, name, burstUserClass, latency, 0)
}

func (b *burstRun) recordFailure(category, name string, latency time.Duration, errMsg string) {
	if b.ctx.Err() != nil {
		return
	}
	bmetrics.RecordFailure(category, name, burstUserClass, latency, errMsg)
}

// sleepCtx sleeps for d or until ctx is done, and reports whether the whole
// sleep elapsed.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// percentile returns the p-th percentile (nearest rank) of sorted, or 0
// when it is empty.
func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := max((p*len(sorted)+99)/100, 1)
	return sorted[rank-1]
}
