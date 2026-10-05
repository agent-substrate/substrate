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

// Package resumecold drives ate-api-server's cold resume path: every
// ResumeActor targets a paused actor, so ateapi takes the actor lease, binds
// a worker, and waits on an atelet restore. Each resume is followed by a
// pause that makes the actor cold again. Run on the fake data plane
// (benchmarking/workloads/deploy.sh --fake-data-plane), it measures what a
// cold resume costs ateapi and Postgres without the router or the restore in
// the way. The pauses run at the resume rate, so a capacity it finds is one
// of resume and pause cycles, not of resumes alone. It sends the actors
// themselves no requests, since none exist there.
package resumecold

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/boomerutil"
	bmetrics "github.com/agent-substrate/substrate/internal/benchmarking/boomer/metrics"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const userClass = "ResumeColdUser"

func init() {
	userclass.Add(userclass.Entry{
		Name:       "resumecold",
		LocustFile: "resumecold.py",
		UserClass:  userClass,
		Init:       initResumeCold,
	})
}

// initResumeCold creates the fleet and makes every actor cold before
// returning, so the worker connects to the locust master only once the
// warm-up is over and it stays out of the measured steps. With no actor
// cold, every sample would be FleetExhausted, so it exits instead.
func initResumeCold(cfg *userclass.Config) (taskFn func(), shutdown func(context.Context)) {
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer("substrate-boomer/resumecold")
	}
	rt := newRuntime(cfg)
	if err := rt.startFleet(context.Background()); err != nil {
		slog.Error("fatal: resumecold: no fleet to run", slog.String("err", err.Error()))
		os.Exit(1)
	}
	return rt.iterate, rt.shutdown
}

type runtime struct {
	cfg *userclass.Config
	// ready holds the cold actors no user is cycling. Its capacity is the
	// fleet size, so returning an actor never blocks.
	ready chan string
	// fleetMu guards fleet, every actor that joined, for shutdown.
	fleetMu sync.Mutex
	fleet   []string
	// limiter paces the resumes of every user. Its burst of 1 spaces them
	// evenly: Wait hands each caller the next slot rather than releasing a
	// second's budget at once.
	limiter *rate.Limiter
	// retryBackoff spaces the retries of a delete.
	retryBackoff time.Duration
	// exhaustedWait is how long a user waits after finding no cold actor, so
	// an unpaced run does not spin on the failure counter.
	exhaustedWait time.Duration
	// cycleMu guards closing and every cycles.Add, so no cycle starts once
	// shutdown has begun waiting for the running ones.
	cycleMu sync.Mutex
	closing bool
	cycles  sync.WaitGroup
	// cycleCtx carries every cycle's RPCs; shutdown cancels it when the
	// running cycles outlast their share of its budget.
	cycleCtx     context.Context
	cancelCycles context.CancelFunc
}

func newRuntime(cfg *userclass.Config) *runtime {
	// The fleet size is --total-actors, which a total_actors in dynconfig
	// overrides, as for the spawn batch.
	n := cfg.TotalActors
	if v := cfg.Dyn.Load().TotalActors; v > 0 {
		n = v
	}
	n = max(n, 1)
	cycleCtx, cancelCycles := context.WithCancel(context.Background())
	return &runtime{
		cfg:           cfg,
		ready:         make(chan string, n),
		fleet:         make([]string, 0, n),
		limiter:       rate.NewLimiter(rate.Inf, 1),
		retryBackoff:  time.Second,
		exhaustedWait: 10 * time.Millisecond,
		cycleCtx:      cycleCtx,
		cancelCycles:  cancelCycles,
	}
}

// startFleet creates the fleet's actors and makes each cold: create, a first
// resume, then a pause. An actor that fails a step is deleted and left out;
// the run goes on with the rest, and the log says how many there are. It
// fails if the atespace cannot be ensured or no actor ends up cold.
func (r *runtime) startFleet(ctx context.Context) error {
	n := cap(r.ready)
	if err := r.ensureAtespace(ctx); err != nil {
		return fmt.Errorf("while ensuring atespace %s: %w", r.cfg.Atespace, err)
	}

	// Bounds the RPCs in flight while warming: each first resume is a cold
	// start on a worker, and a few hundred at once would queue in ateapi.
	const warmParallelism = 16
	sem := make(chan struct{}, warmParallelism)
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			r.addActor(ctx)
		}()
	}
	wg.Wait()
	if len(r.ready) == 0 {
		return fmt.Errorf("none of %d actors became cold", n)
	}
	slog.Info("resumecold: fleet ready",
		slog.Int("wanted", n), slog.Int("cold", len(r.ready)))
	return nil
}

// addActor creates one actor, makes it cold, and puts it in the ready queue.
func (r *runtime) addActor(ctx context.Context) {
	name := "rc-" + uuid.NewString()
	if err := r.warm(ctx, name); err != nil {
		slog.Warn("resumecold: actor did not warm; leaving it out",
			slog.String("actor", name), slog.String("err", err.Error()))
		_ = r.delete(ctx, name)
		return
	}
	r.fleetMu.Lock()
	r.fleet = append(r.fleet, name)
	r.fleetMu.Unlock()
	r.ready <- name
}

// The template every fleet actor is created from.
const (
	templateName     = "glutton"
	templateAtespace = "benchmark-workloads"
)

// warm creates one actor, resumes it, and pauses it.
func (r *runtime) warm(ctx context.Context, name string) error {
	if err := r.call(ctx, "CreateActor", func(ctx context.Context) error {
		_, err := r.cfg.APIStub.CreateActor(ctx, &ateapipb.CreateActorRequest{
			Actor: &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: r.cfg.Atespace, Name: name},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: templateAtespace, Name: templateName},
			},
		})
		return err
	}); err != nil {
		return err
	}
	if err := r.resume(ctx, "ResumeActorWarmup", name); err != nil {
		return err
	}
	return r.pause(ctx, "Warmup", name)
}

// iterate is the boomer task: one paced cold resume of a ready actor, then
// the pause that makes it cold again. Only the resume is paced, so the
// target rate is cold resumes per second. Finding the queue empty is
// reported as a FleetExhausted failure: the fleet, not the server, set the
// rate, and the step is not valid.
func (r *runtime) iterate() {
	ctx := r.cycleCtx
	r.pace(ctx)
	if !r.startCycle() {
		return
	}
	defer r.cycles.Done()
	var name string
	select {
	case name = <-r.ready:
	default:
		bmetrics.RecordFailure("grpc", "FleetExhausted", userClass, 0, "no cold actor ready")
		time.Sleep(r.exhaustedWait)
		return
	}
	err := r.resume(ctx, "ResumeActorCold", name)
	if err == nil {
		err = r.pause(ctx, "", name)
	}
	if err != nil {
		// The actor may have been left running. Pausing it again makes it
		// cold for the next cycle. This is housekeeping, not a sample, so it
		// goes around call(): on an actor already paused it fails, and that
		// failure must not count against the server.
		if _, err := r.cfg.APIStub.PauseActor(ctx, &ateapipb.PauseActorRequest{Actor: r.ref(name)}); err != nil {
			slog.Debug("resumecold: recovery pause failed",
				slog.String("actor", name), slog.String("err", err.Error()))
		}
	}
	r.ready <- name
}

// startCycle registers a cycle with shutdown, or reports false once shutdown
// has begun.
func (r *runtime) startCycle() bool {
	r.cycleMu.Lock()
	defer r.cycleMu.Unlock()
	if r.closing {
		return false
	}
	r.cycles.Add(1)
	return true
}

// pace blocks until the limiter grants the next resume slot, applying the
// current TargetRPS first. Zero means unpaced.
func (r *runtime) pace(ctx context.Context) {
	want := rate.Inf
	if target := r.cfg.Dyn.Load().TargetRPS; target > 0 {
		want = rate.Limit(target)
	}
	if r.limiter.Limit() != want {
		r.limiter.SetLimit(want)
	}
	_ = r.limiter.Wait(ctx)
}

// errAlreadyRunning marks a ResumeActor that found its actor running, so no
// cold resume happened and the sample must not count as one.
var errAlreadyRunning = errors.New("actor was already running: not a cold resume")

// resume calls ResumeActor. A response that reports no resume happened fails
// the sample with errAlreadyRunning.
func (r *runtime) resume(ctx context.Context, metricName, name string) error {
	return r.call(ctx, metricName, func(ctx context.Context) error {
		resp, err := r.cfg.APIStub.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
			Actor: r.ref(name),
		})
		if err == nil && !resp.GetResumed() {
			return errAlreadyRunning
		}
		return err
	})
}

// pause calls PauseActor. suffix keeps warm-up calls apart from the measured
// ones.
func (r *runtime) pause(ctx context.Context, suffix, name string) error {
	return r.call(ctx, "PauseActor"+suffix, func(ctx context.Context) error {
		_, err := r.cfg.APIStub.PauseActor(ctx, &ateapipb.PauseActorRequest{
			Actor: r.ref(name),
		})
		return err
	})
}

// shutdown waits for the running cycles, then deletes the fleet. Boomer
// stops scheduling tasks before shutdown but does not wait for the ones in
// flight, and a delete that races one of them loses the actor lease and
// fails with Aborted. The cycles get half of ctx's remaining time; any still
// running then are canceled, so a hung RPC cannot spend the time the deletes
// need. DeleteActor with AnyState removes an actor in any state, so a
// canceled cycle still leaves nothing behind.
func (r *runtime) shutdown(ctx context.Context) {
	r.cycleMu.Lock()
	r.closing = true
	r.cycleMu.Unlock()
	drained := make(chan struct{})
	go func() {
		r.cycles.Wait()
		close(drained)
	}()
	drainCtx := ctx
	if deadline, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		drainCtx, cancel = context.WithTimeout(ctx, time.Until(deadline)/2)
		defer cancel()
	}
	select {
	case <-drained:
	case <-drainCtx.Done():
		r.cancelCycles()
		select {
		case <-drained:
		case <-ctx.Done():
		}
	}
	defer r.cancelCycles()

	r.fleetMu.Lock()
	names := r.fleet
	r.fleet = nil
	r.fleetMu.Unlock()

	const parallelism = 16
	sem := make(chan struct{}, parallelism)
	var wg sync.WaitGroup
	var leftMu sync.Mutex
	left := 0
	for _, name := range names {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if r.delete(ctx, name) != nil {
				leftMu.Lock()
				left++
				leftMu.Unlock()
			}
		}()
	}
	wg.Wait()
	if left > 0 {
		slog.Warn("resumecold: actors left behind after shutdown",
			slog.Int("count", left), slog.String("atespace", r.cfg.Atespace))
	}
}

// delete deletes an actor, retrying while another operation holds its lease.
func (r *runtime) delete(ctx context.Context, name string) error {
	const attempts = 5
	var err error
	for attempt := range attempts {
		if attempt > 0 {
			select {
			case <-time.After(r.retryBackoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		err = r.call(ctx, "DeleteActor", func(ctx context.Context) error {
			_, err := r.cfg.APIStub.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
				Actor:    r.ref(name),
				AnyState: true,
			})
			return err
		})
		if c := status.Code(err); c != codes.Aborted && c != codes.Unavailable {
			return err
		}
	}
	return err
}

func (r *runtime) ref(name string) *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Atespace: r.cfg.Atespace, Name: name}
}

func (r *runtime) ensureAtespace(ctx context.Context) error {
	return r.call(ctx, "CreateAtespace", func(ctx context.Context) error {
		_, err := r.cfg.APIStub.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
			Atespace: &ateapipb.Atespace{
				Metadata: &ateapipb.ResourceMetadata{Name: r.cfg.Atespace},
			},
		})
		if status.Code(err) == codes.AlreadyExists {
			return nil
		}
		return err
	})
}

// call times one unary RPC and reports its latency to locust and Prometheus.
func (r *runtime) call(ctx context.Context, name string, do func(context.Context) error) error {
	ctx, span := r.cfg.Tracer.Start(ctx, name)
	defer span.End()

	start := time.Now()
	err := do(ctx)
	latency := time.Since(start)

	boomerutil.LogSampledTrace(span, name, latency, boomerutil.SourceClient, err)
	if err != nil {
		bmetrics.RecordFailure("grpc", name, userClass, latency, err.Error())
		return err
	}
	bmetrics.RecordSuccess("grpc", name, userClass, latency, 0)
	return nil
}
