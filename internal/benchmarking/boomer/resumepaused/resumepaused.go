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

// Package resumepaused drives ate-api-server's cold resume path: every
// ResumeActor targets a paused actor, so ateapi takes the actor lease, binds
// a worker, and waits on an atelet restore. Each resume is followed by a
// pause that makes the actor cold again. Run on the fake data plane
// (benchmarking/workloads/deploy.sh --fake-data-plane), it measures what a
// cold resume costs ateapi and Postgres without the router or the restore in
// the way. The pauses run at the resume rate, so a capacity it finds is one
// of resume and pause cycles, not of resumes alone. It sends the actors
// themselves no requests, since none exist there.
package resumepaused

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.ate.dev/substrate/internal/benchmarking/boomer/boomerutil"
	"go.ate.dev/substrate/internal/benchmarking/boomer/dynconfig"
	bmetrics "go.ate.dev/substrate/internal/benchmarking/boomer/metrics"
	"go.ate.dev/substrate/internal/benchmarking/boomer/userclass"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const userClass = "ResumePausedUser"

func init() {
	userclass.Add(userclass.Entry{
		Name:       "resumepaused",
		LocustFile: "resumepaused.py",
		UserClass:  userClass,
		Config:     knobsCodec,
		Init:       initResumePaused,
	})
}

// knobs is the ResumePausedUser slice of the runtime config. The master
// populates target_rps from the --target-rps locust flag
// (common/rate_config.py) or the ladder step's @rps suffix.
type knobs struct {
	// TargetRPS is the cold resumes per second this worker paces to; zero
	// means unpaced.
	TargetRPS float64 `json:"target_rps"`
}

var knobsCodec = dynconfig.Typed[knobs]{
	Validate: func(k knobs) error {
		if k.TargetRPS < 0 {
			return fmt.Errorf("target_rps cannot be negative: %v", k.TargetRPS)
		}
		return nil
	},
}

// initResumePaused ensures the atespace and returns the task. Each user
// creates and warms its own actor on its first iteration.
func initResumePaused(cfg *userclass.Config) (taskFn func(), shutdown func(context.Context)) {
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer("substrate-boomer/resumepaused")
	}
	rt := newRuntime(cfg)
	if err := rt.start(context.Background()); err != nil {
		slog.Error("fatal: resumepaused", slog.String("err", err.Error()))
		os.Exit(1)
	}
	return rt.iterate, rt.shutdown
}

type runtime struct {
	cfg *userclass.Config
	// users maps a boomer goroutine to the actor it cycles. Boomer gives a
	// task no user identity, so the goroutine stands in for the user. Go
	// never reuses a goroutine ID, so the entry of a user boomer stopped is
	// never looked up again; its actor stays paused until shutdown.
	users sync.Map // goroutineID -> actor name
	// actorsMu guards actors, every actor that may exist, for shutdown.
	actorsMu sync.Mutex
	actors   map[string]struct{}
	// limiter paces the resumes of every user. Its burst of 1 spaces them
	// evenly: Wait hands each caller the next slot rather than releasing a
	// second's budget at once.
	limiter *rate.Limiter
	// retryBackoff spaces the retries of a delete.
	retryBackoff time.Duration
	// setupRetryWait is how long a user waits after its actor failed to
	// warm, so a failing setup does not loop on boomer's zero-delay
	// re-entry.
	setupRetryWait time.Duration
	// cycleMu guards closing and every cycles.Add, so no cycle or setup
	// starts once shutdown has begun waiting for the running ones.
	cycleMu sync.Mutex
	closing bool
	cycles  sync.WaitGroup
	// cycleCtx carries every cycle's and setup's RPCs; shutdown cancels it
	// when the running ones outlast their share of its budget.
	cycleCtx     context.Context
	cancelCycles context.CancelFunc
	// stopped is set when shutdown begins, after boomer.Run has returned.
	// Boomer may have stopped draining its stats channel by then, so a
	// sample sent to it could block for good once the buffer fills: from
	// then on, samples go to Prometheus only.
	stopped atomic.Bool
	// recordSuccess and recordFailure send a sample to both locust and
	// Prometheus while boomer runs.
	recordSuccess func(method, name, userClass string, latency time.Duration, responseBytes int64)
	recordFailure func(method, name, userClass string, latency time.Duration, errMsg string)
}

func newRuntime(cfg *userclass.Config) *runtime {
	cycleCtx, cancelCycles := context.WithCancel(context.Background())
	return &runtime{
		cfg:            cfg,
		actors:         map[string]struct{}{},
		limiter:        rate.NewLimiter(rate.Inf, 1),
		retryBackoff:   time.Second,
		setupRetryWait: time.Second,
		cycleCtx:       cycleCtx,
		cancelCycles:   cancelCycles,
		recordSuccess:  bmetrics.RecordSuccess,
		recordFailure:  bmetrics.RecordFailure,
	}
}

// start ensures the atespace every actor is created in.
func (r *runtime) start(ctx context.Context) error {
	if err := r.ensureAtespace(ctx); err != nil {
		return fmt.Errorf("while ensuring atespace %s: %w", r.cfg.Atespace, err)
	}
	return nil
}

// iterate is the boomer task. A user's first iteration creates its actor and
// makes it cold; every later one runs a measured cycle on that actor.
func (r *runtime) iterate() {
	gid := boomerutil.GoroutineID()
	if v, ok := r.users.Load(gid); ok {
		r.cycle(v.(string))
		return
	}
	name, ok := r.setUp()
	if !ok {
		time.Sleep(r.setupRetryWait)
		return
	}
	r.users.Store(gid, name)
}

// setUp creates one actor and makes it cold. It is not paced, so its calls
// land in the step that added the user, recorded under their own names. An
// actor that fails to warm is deleted, and the user tries again on its next
// iteration.
func (r *runtime) setUp() (string, bool) {
	if !r.startCycle() {
		return "", false
	}
	defer r.cycles.Done()
	ctx := r.cycleCtx
	name := "rp-" + uuid.NewString()
	if err := r.create(ctx, name); err != nil {
		return "", false
	}
	r.track(name)
	if err := r.warm(ctx, name); err != nil {
		slog.Warn("resumepaused: actor did not warm; the user will retry",
			slog.String("actor", name), slog.String("err", err.Error()))
		if r.delete(ctx, name) == nil {
			r.untrack(name)
		}
		return "", false
	}
	return name, true
}

func (r *runtime) track(name string) {
	r.actorsMu.Lock()
	defer r.actorsMu.Unlock()
	r.actors[name] = struct{}{}
}

func (r *runtime) untrack(name string) {
	r.actorsMu.Lock()
	defer r.actorsMu.Unlock()
	delete(r.actors, name)
}

// The template every actor is created from.
const (
	templateName     = "glutton"
	templateAtespace = "benchmark-workloads"
)

// create creates one actor from the template.
func (r *runtime) create(ctx context.Context, name string) error {
	return r.call(ctx, "CreateActor", func(ctx context.Context) error {
		_, err := r.cfg.APIStub.CreateActor(ctx, &ateapipb.CreateActorRequest{
			Actor: &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: r.cfg.Atespace, Name: name},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: templateAtespace, Name: templateName},
			},
		})
		return err
	})
}

// warm makes a new actor cold: a first resume, then a pause.
func (r *runtime) warm(ctx context.Context, name string) error {
	if err := r.resume(ctx, "ResumeActorWarmup", name); err != nil {
		return err
	}
	return r.pause(ctx, "Warmup", name)
}

// cycle is one paced cold resume of the user's actor, then the pause that
// makes it cold again. Only the resume is paced, so the target rate is cold
// resumes per second.
func (r *runtime) cycle(name string) {
	ctx := r.cycleCtx
	r.pace(ctx)
	if !r.startCycle() {
		return
	}
	defer r.cycles.Done()
	err := r.resume(ctx, "ResumeActorCold", name)
	if err == nil {
		err = r.pause(ctx, "", name)
	}
	if err != nil {
		// The actor may have been left running. Pausing it again makes it
		// cold for the next cycle.
		r.recover(ctx, name)
	}
}

// recover suspends an actor asynchronously after a failed cycle, so it is cold
// for the next one. It is load on the server like any other call, so it is
// recorded, as SuspendActorAsyncRecovery to keep it apart from the measured
// suspends. An actor already suspended fails it with FailedPrecondition; the
// actor is cold, which is all recovery wants, so that counts as a success.
func (r *runtime) recover(ctx context.Context, name string) {
	_ = r.call(ctx, "SuspendActorAsyncRecovery", func(ctx context.Context) error {
		_, err := r.cfg.APIStub.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
			Actor:   r.ref(name),
			Options: &ateapipb.SuspendOptions{Mode: ateapipb.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS},
		})
		if status.Code(err) == codes.FailedPrecondition {
			return nil
		}
		return err
	})
}

// startCycle registers a cycle or setup with shutdown, or reports false once
// shutdown has begun.
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
	if target := dynconfig.Get[knobs](r.cfg.Dyn).TargetRPS; target > 0 {
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

// pause suspends an actor asynchronously, keeping its snapshot on the node.
// suffix keeps warm-up calls apart from the measured ones.
func (r *runtime) pause(ctx context.Context, suffix, name string) error {
	return r.call(ctx, "SuspendActorAsync"+suffix, func(ctx context.Context) error {
		_, err := r.cfg.APIStub.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
			Actor:   r.ref(name),
			Options: &ateapipb.SuspendOptions{Mode: ateapipb.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS},
		})
		return err
	})
}

// shutdown waits for the running cycles and setups, then deletes every
// actor. Boomer stops scheduling tasks before shutdown but does not wait for
// the ones in flight, and a delete that races one of them loses the actor
// lease and fails with Aborted. The running ones get half of ctx's remaining
// time; any still running then are canceled, so a hung RPC cannot spend the
// time the deletes need. DeleteActor with AnyState removes an actor in any
// state, so a canceled cycle or setup still leaves nothing behind.
func (r *runtime) shutdown(ctx context.Context) {
	r.stopped.Store(true)
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

	r.actorsMu.Lock()
	names := slices.Collect(maps.Keys(r.actors))
	r.actors = map[string]struct{}{}
	r.actorsMu.Unlock()

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
		slog.Warn("resumepaused: actors left behind after shutdown",
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
	r.record(name, latency, err)
	return err
}

// record reports one sample: to locust and Prometheus while boomer runs, to
// Prometheus only once shutdown has begun.
func (r *runtime) record(name string, latency time.Duration, err error) {
	stopped := r.stopped.Load()
	switch {
	case err != nil && stopped:
		bmetrics.RecordPromFailure("grpc", name, userClass, latency)
	case err != nil:
		r.recordFailure("grpc", name, userClass, latency, err.Error())
	case stopped:
		bmetrics.RecordPromSuccess("grpc", name, userClass, latency)
	default:
		r.recordSuccess("grpc", name, userClass, latency, 0)
	}
}
