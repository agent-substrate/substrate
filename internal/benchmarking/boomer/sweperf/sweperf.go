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

// Package sweperf implements the boomer-Go implementation of the
// SweperfUser locust test. Each user creates an actor from a SWE-bench workload
// template and drives it through a trajectory of steps that are partitioned into
// cycles. Once the trajectory is done the actor is deleted and a fresh one is
// created to run it again. This is workload-agnostic and can be used to benchmark any SWE-bench
// workload by providing the appropriate template and dynamic configuration.

package sweperf

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.ate.dev/substrate/internal/ateinterceptors"
	"go.ate.dev/substrate/internal/atenet"
	"go.ate.dev/substrate/internal/benchmarking/boomer/boomerutil"
	"go.ate.dev/substrate/internal/benchmarking/boomer/dynconfig"
	bmetrics "go.ate.dev/substrate/internal/benchmarking/boomer/metrics"
	"go.ate.dev/substrate/internal/benchmarking/boomer/userclass"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// templateNS is the namespace holding the actor templates this workload
// instantiates from.
const (
	templateNS = "benchmark-workloads"
)

// Defaults for a sweperf session, used when neither the dynamic config nor the
// environment supplies a value.
const (
	sweperfUserClass         = "SweperfUser"
	defaultSweperfTemplate   = "sweperf-astropy-7336"
	defaultSweperfTotalSteps = 21
	defaultSweperfNumCycles  = 4
	// defaultSweperfPollInterval trades CycleCEL-to-ack accuracy against
	// /status load on the router; sympy cycles take roughly 1.5-15s.
	defaultSweperfPollInterval = 100 * time.Millisecond
)

// Derived rows: each spans more than one call, hence the actor method.
const (
	resumeToFirstExecMetric = "ResumeToFirstExec"
	cycleCELMetric          = "CycleCEL"
	taskCELMetric           = "TaskCEL"
	taskWallClockMetric     = "TaskWallClock"
	derivedMetricMethod     = "actor"
)

// init registers the sweperf user class so the boomer worker can select it by
// name and the runner can match it to the equivalent locust file.
func init() {
	userclass.Add(userclass.Entry{
		Name:       "sweperf",
		LocustFile: "sweperf.py",
		UserClass:  sweperfUserClass,
		Config:     knobsCodec,
		Init:       initSweperf,
	})
}

// knobs is the SweperfUser slice of the runtime config. Each zero falls
// back to the default above; the master populates the keys from the
// --sweperf-* locust flags (common/sweperf_config.py).
type knobs struct {
	dynconfig.WaitTime
	Template       string `json:"sweperf_template"`
	TotalSteps     int    `json:"sweperf_total_steps"`
	NumCycles      int    `json:"sweperf_num_cycles"`
	PollIntervalMs int    `json:"sweperf_poll_interval_ms"`
}

var knobsCodec = dynconfig.Typed[knobs]{
	Validate: func(k knobs) error {
		if err := k.WaitTime.Validate(); err != nil {
			return err
		}
		if k.TotalSteps < 0 {
			return fmt.Errorf("sweperf_total_steps cannot be negative: %d", k.TotalSteps)
		}
		if k.NumCycles < 0 {
			return fmt.Errorf("sweperf_num_cycles cannot be negative: %d", k.NumCycles)
		}
		if k.PollIntervalMs < 0 {
			return fmt.Errorf("sweperf_poll_interval_ms cannot be negative: %d", k.PollIntervalMs)
		}
		return nil
	},
}

// chunk partitions totalSteps instructions into numCycles slices.
type chunk struct {
	start int
	end   int
}

// generateDynamicChunks splits totalSteps into numCycles contiguous chunks.
// The remainder is spread over the earliest cycles, so chunk lengths differ by
// at most one.
func generateDynamicChunks(totalSteps int, numCycles int) []chunk {
	if numCycles <= 1 {
		return []chunk{{0, totalSteps}}
	}
	if totalSteps < 1 {
		totalSteps = 1
	}
	if numCycles > totalSteps {
		numCycles = totalSteps
	}
	baseChunk := totalSteps / numCycles
	remainder := totalSteps % numCycles

	chunks := make([]chunk, 0, numCycles)
	currentStart := 0
	for i := 0; i < numCycles; i++ {
		extra := 0
		if i < remainder {
			extra = 1
		}
		currentEnd := currentStart + baseChunk + extra
		chunks = append(chunks, chunk{currentStart, currentEnd})
		currentStart = currentEnd
	}
	return chunks
}

// initSweperf creates a runtime tied to cfg and returns a boomer-compatible
// task function plus a shutdown hook the caller should run before exit (it
// deletes every actor this worker created).
func initSweperf(cfg *userclass.Config) (taskFn func(), shutdown func(context.Context)) {
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer("substrate-boomer/sweperf")
	}

	rt := newSweperfRuntime(cfg)
	return rt.iterate, rt.shutdown
}

// newSweperfRuntime returns a runtime whose run context lives until shutdown.
func newSweperfRuntime(cfg *userclass.Config) *sweperfRuntime {
	ctx, cancel := context.WithCancel(context.Background())
	return &sweperfRuntime{cfg: cfg, runCtx: ctx, cancelRun: cancel}
}

// sweperfRuntime is the per-worker state shared by every boomer goroutine.
// Each goroutine keeps its own session in users, keyed by goroutine ID,
// because boomer offers no per-VU context.
type sweperfRuntime struct {
	cfg   *userclass.Config
	users sync.Map // goroutineID -> *sweperfUser
	// runCtx carries every startup and cycle; shutdown cancels it so a slow
	// liveness poll or cycle does not hold up teardown.
	runCtx    context.Context
	cancelRun context.CancelFunc
	// mu orders enter against shutdown, so no inflight.Add can race the
	// inflight.Wait in shutdown.
	mu sync.Mutex
	// stopping is set once by shutdown, under mu, and never cleared.
	stopping bool
	// inflight counts iterate calls in progress, each of which may create or
	// delete an actor. shutdown waits for them before sweeping users.
	inflight sync.WaitGroup
	// atespaceReady is set once CreateAtespace succeeds, so later actors skip
	// it. Cleared when CreateActor reports the atespace missing, so the next
	// start recreates it.
	atespaceReady atomic.Bool
}

// enter registers an iterate call with inflight, or reports false once
// shutdown has begun. A true result must be paired with inflight.Done.
func (r *sweperfRuntime) enter() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopping {
		return false
	}
	r.inflight.Add(1)
	return true
}

// teardownTimeout bounds deleting an actor once the run context may already
// be canceled. A var so tests can shorten it.
var teardownTimeout = 30 * time.Second

// teardownCtx returns a context for deleting an actor that outlives the run
// context, so cancellation by shutdown does not strand the actor.
func (r *sweperfRuntime) teardownCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(r.runCtx), teardownTimeout)
}

// canceled reports whether ctx was canceled, which only happens when shutdown
// cuts a call short, so the call says nothing about the system under test.
// It checks the context, not the error: gRPC returns a codes.Canceled status
// that errors.Is does not match. Timeouts are DeadlineExceeded, so teardown
// calls that run out of time are still recorded.
func canceled(ctx context.Context) bool {
	return errors.Is(ctx.Err(), context.Canceled)
}

// resolveConfig returns the template, total step count and cycle count for a
// new session. Values come from dynconfig, which the master populates from
// the --sweperf-* locust flags (common/sweperf_config.py); an unset field
// falls back to the built-in default below.
func (r *sweperfRuntime) resolveConfig() (string, int, int) {
	dyn := dynconfig.Get[knobs](r.cfg.Dyn)

	template := dyn.Template
	if template == "" {
		template = defaultSweperfTemplate
	}

	totalSteps := dyn.TotalSteps
	if totalSteps <= 0 {
		totalSteps = defaultSweperfTotalSteps
	}

	numCycles := dyn.NumCycles
	if numCycles <= 0 {
		numCycles = defaultSweperfNumCycles
	}

	return template, totalSteps, numCycles
}

// pollInterval is the /status poll interval from dynconfig
// (--sweperf-poll-interval-ms), or the default when unset. Read per job, so a
// mid-run change applies to the next cycle.
func pollInterval(cfg *userclass.Config) time.Duration {
	if ms := dynconfig.Get[knobs](cfg.Dyn).PollIntervalMs; ms > 0 {
		return time.Duration(ms) * time.Millisecond
	}
	return defaultSweperfPollInterval
}

// dynamicWait is the think time between cycles: a uniform draw from
// [MinWait, MaxWait), or MinWait when the range is empty.
func (r *sweperfRuntime) dynamicWait() time.Duration {
	return dynconfig.Get[knobs](r.cfg.Dyn).WaitTime.Draw()
}

// iterate is the boomer task function, one call per goroutine per iteration.
// It binds a session to the calling goroutine on first use and runs one cycle
// per call thereafter. When the trace is exhausted the session's actor is
// deleted and the goroutine is unbound, so the next call starts a fresh actor
// that runs the trajectory again. A session that fails to start is retried on
// the next iteration. Once shutdown has begun no new actor is created.
func (r *sweperfRuntime) iterate() {
	if !r.enter() {
		return
	}
	gid := boomerutil.GoroutineID()
	cycleBefore := 0
	if val, ok := r.users.Load(gid); ok {
		cycleBefore = val.(*sweperfUser).cycleIndex
	}
	r.runCycle()

	wait := r.dynamicWait()
	// Think time counts toward a task only between its cycles: this call
	// completed a cycle and the session is still bound, so it was not the last.
	if val, ok := r.users.Load(gid); ok {
		if u := val.(*sweperfUser); u.cycleIndex > cycleBefore {
			u.loopThink += wait
		}
	}
	time.Sleep(wait)
}

// runCycle is the part of iterate that shutdown waits for: it may start,
// cycle or delete an actor, but excludes the think time.
func (r *sweperfRuntime) runCycle() {
	defer r.inflight.Done()
	gid := boomerutil.GoroutineID()
	val, loaded := r.users.Load(gid)
	user, _ := val.(*sweperfUser)
	if !loaded {
		if user = r.bindUser(gid); user == nil {
			return
		}
	}
	if r.runCtx.Err() != nil {
		return // shutdown began; its sweep tears the actor down
	}

	user.step(r.runCtx)

	if user.isDone() {
		if r.runCtx.Err() == nil { // a task finished as shutdown began records nothing
			user.recordTaskMetrics()
		}
		r.retireUser(gid, user)
	}
}

// bindUser starts a session and publishes it for goroutine gid. It returns
// nil if the session failed to start. shutdown waits for the calling
// runCycle, so it always sees a published session.
func (r *sweperfRuntime) bindUser(gid int64) *sweperfUser {
	u, err := r.startUser(r.runCtx)
	if err != nil {
		return nil // startUser logged it
	}
	r.users.Store(gid, u)
	return u
}

// startUser creates an actor and blocks until its sandbox serves, so the
// caller gets a session that is ready to take cycles. Resolves the config per
// session, which lets a value changed mid-run apply to later sessions. On
// failure it tears the actor down and logs the error with the user, actor and
// template, so a failed start can be tied to its task.
func (r *sweperfRuntime) startUser(ctx context.Context) (_ *sweperfUser, err error) {
	tmpl, totalSteps, numCycles := r.resolveConfig()
	chunks := generateDynamicChunks(totalSteps, numCycles)

	u := &sweperfUser{
		cfg:          r.cfg,
		actorName:    "sb-" + uuid.NewString(),
		templateName: tmpl,
		userClass:    sweperfUserClass,
		chunks:       chunks,
		cycleIndex:   0,
		// bindUser and every cycle of this session run on this goroutine.
		userID:    boomerutil.GoroutineID(),
		taskStart: time.Now(),
	}

	slog.Info("Creating new sweperf user session",
		slog.String("actor", u.actorName),
		slog.String("template", u.templateName),
	)
	defer func() {
		if err != nil {
			slog.Warn("sweperf on_start failed; goroutine will retry next iter",
				slog.Int64("user", u.userID),
				slog.String("actor", u.actorName),
				slog.String("template", u.templateName),
				slog.String("err", err.Error()),
			)
		}
	}()

	bmetrics.UpdateUsers(u.userClass, 1)

	if !r.atespaceReady.Load() {
		if err := u.ensureAtespace(ctx); err != nil {
			bmetrics.UpdateUsers(u.userClass, -1)
			return nil, fmt.Errorf("ensureAtespace: %w", err)
		}
		r.atespaceReady.Store(true)
	}
	// CreateActor runs detached from ctx: canceled mid-call, the actor might
	// exist without this session knowing to delete it.
	createCtx, cancelCreate := r.teardownCtx()
	startupStart := time.Now()
	err = u.create(createCtx)
	cancelCreate()
	if err != nil {
		// ate-api answers FailedPrecondition when the atespace is missing.
		if status.Code(err) == codes.FailedPrecondition {
			r.atespaceReady.Store(false)
		}
		bmetrics.UpdateUsers(u.userClass, -1)
		return nil, fmt.Errorf("createActor: %w", err)
	}

	// poll status for liveness
	if err := u.pollLiveness(ctx); err != nil {
		// ctx may be canceled by shutdown; tear down regardless.
		tctx, cancel := r.teardownCtx()
		defer cancel()
		// teardown also decrements the user gauge.
		u.teardown(tctx)
		return nil, fmt.Errorf("pollLiveness: %w", err)
	}
	u.startupDur = time.Since(startupStart)

	// pollLiveness left the actor running, so cycle 1's resume is a no-op.
	u.awake = true

	slog.Info("Sweperf user session is ready", slog.String("actor", u.actorName))
	return u, nil
}

// retireUser deletes the actor of a session that finished its trajectory and
// unbinds it from goroutine gid, so the next iterate starts a fresh actor. The
// final cycle leaves the actor running, and DeleteActor with AnyState removes
// it from any state, so no suspend is sent first. The delete runs detached
// from the run context so shutdown cannot cut it short. A failed delete is
// retried once, with a fresh timeout; if that also fails the actor is left
// behind and logged.
func (r *sweperfRuntime) retireUser(gid int64, u *sweperfUser) {
	r.users.CompareAndDelete(gid, u)
	if !u.cleanedUp.CompareAndSwap(false, true) {
		return // shutdown already owns this actor's teardown
	}
	bmetrics.UpdateUsers(u.userClass, -1)
	deleteActor := func() error {
		ctx, cancel := r.teardownCtx()
		defer cancel()
		return u.delete(ctx)
	}
	if err := deleteActor(); err != nil {
		if err := deleteActor(); err != nil {
			slog.Error("DeleteActor failed twice for finished actor; it is leaked",
				slog.String("actor", u.actorName),
				slog.String("err", err.Error()),
			)
			return
		}
	}
	slog.Info("Actor finished all cycles and was deleted; a new actor will run the trajectory next",
		slog.String("actor", u.actorName),
		slog.Int("cycles", len(u.chunks)),
	)
}

// inflightWaitTimeout caps how long shutdown waits for in-flight iterations.
// They are canceled and normally end at once; the cap leaves the rest of the
// shutdown budget for the sweep. A var so tests can shorten it.
var inflightWaitTimeout = 10 * time.Second

// shutdownConcurrency bounds how many actors shutdown deletes at once.
// In-cluster the pod's 30s termination grace period, not boomer-worker's
// 60s shutdown context, is the real budget, and a serial sweep over thousands
// of sessions would overrun it.
const shutdownConcurrency = 64

// shutdown stops new iterations, cancels in-flight startups and cycles, waits
// up to inflightWaitTimeout for them to finish, then deletes every actor still
// bound to a goroutine, shutdownConcurrency at a time, until done or ctx
// expires. Boomer has no per-VU stop hook, so a mid-run decrease in user
// count leaks actors until this runs — acceptable for a run that ramps up,
// holds, then tears down.
func (r *sweperfRuntime) shutdown(ctx context.Context) {
	slog.Info("sweperf shutdown started")
	r.mu.Lock()
	r.stopping = true
	r.mu.Unlock()
	r.cancelRun()

	waitCtx, cancelWait := context.WithTimeout(ctx, inflightWaitTimeout)
	defer cancelWait()
	done := make(chan struct{})
	go func() {
		r.inflight.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-waitCtx.Done():
		slog.Warn("shutdown timed out waiting for in-flight iterations; their actors may leak")
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, shutdownConcurrency)
	r.users.Range(func(_, val any) bool {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return false
		}
		wg.Add(1)
		go func(u *sweperfUser) {
			defer wg.Done()
			defer func() { <-sem }()
			u.teardown(ctx)
		}(val.(*sweperfUser))
		return true
	})
	wg.Wait()
}

// sweperfUser is one benchmark session: a single actor, the cycle plan it
// runs once, and its progress through that plan. Owned by one boomer goroutine,
// so its fields need no locking, except cleanedUp, which shutdown also claims.
type sweperfUser struct {
	cfg          *userclass.Config
	actorName    string
	templateName string
	userClass    string
	chunks       []chunk
	cycleIndex   int
	// cleanedUp is claimed by whichever of the owning goroutine or shutdown
	// tears the actor down first, so the actor is deleted once.
	cleanedUp atomic.Bool
	// awake is set while the actor is still running from pollLiveness; the
	// next resume is then a no-op and its success samples are not recorded.
	awake bool
	// Container compute and per-cycle wall clock so far, and whether any cycle failed.
	loopCEL    time.Duration
	loopWall   time.Duration
	loopFailed bool
	// Facts for the task record (logTaskRecord).
	userID      int64         // boomer goroutine ID, unique per boomer process
	taskStart   time.Time     // when startUser began
	startupDur  time.Duration // CreateActor until the sandbox answered /status
	loopThink   time.Duration // think time between this task's cycles
	loopRetries int           // failed resumes, each of which retries its cycle
	resumeMax   time.Duration // slowest non-no-op ResumeActor, as recorded on its row
	ttfeMax     time.Duration // slowest resume-to-ack; zero when no cycle after the no-op resume got an async ack
	failStep    string        // first failed step: resume, execute or suspend
	failCycle   int           // cycle of failStep
}

// ref is the control-plane reference to this user's actor.
func (u *sweperfUser) ref() *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Atespace: u.cfg.Atespace, Name: u.actorName}
}

// setActorRouting names the actor the atenet router should route this request
// to. The router selects the actor from this header alone, so the request's
// own Host stays the router's; a request without it is refused with a 404.
func (u *sweperfUser) setActorRouting(req *http.Request) {
	req.Header.Set(atenet.TargetActorHeader, u.cfg.Atespace+"/"+u.actorName)
}

// ensureAtespace creates the configured atespace, treating AlreadyExists as
// success so concurrent users racing the first creation all proceed.
func (u *sweperfUser) ensureAtespace(ctx context.Context) error {
	return u.tracedCall(ctx, "CreateAtespace", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.CreateAtespace(callCtx, &ateapipb.CreateAtespaceRequest{
			Atespace: &ateapipb.Atespace{
				Metadata: &ateapipb.ResourceMetadata{
					Name: u.cfg.Atespace,
				},
			},
		}, grpc.Trailer(tr))
		if err == nil {
			return nil
		}
		if s, ok := status.FromError(err); ok && s.Code() == codes.AlreadyExists {
			return nil
		}
		return err
	})
}

// create registers the actor against its ActorTemplate. The actor is not yet
// running when this returns: the first request through the router starts it
// (see pollLiveness).
func (u *sweperfUser) create(ctx context.Context) error {
	return u.tracedCall(ctx, "CreateActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.CreateActor(callCtx, &ateapipb.CreateActorRequest{
			Actor: &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: u.cfg.Atespace, Name: u.actorName},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: templateNS, Name: u.templateName},
			},
		}, grpc.Trailer(tr))
		return err
	})
}

// resume wakes the actor for the next cycle and reports the latency recorded
// on its ResumeActor row and whether it worked. A failure is the caller's cue
// to abandon the cycle rather than talk to a sandbox that is not running.
func (u *sweperfUser) resume(ctx context.Context) (time.Duration, bool) {
	latency, err := u.tracedCallRecord(ctx, "ResumeActor", !u.awake, func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.ResumeActor(callCtx, &ateapipb.ResumeActorRequest{
			Actor: u.ref(),
		}, grpc.Trailer(tr))
		return err
	})
	if err != nil {
		slog.Error("ResumeActor failed", slog.String("actor", u.actorName), slog.String("err", err.Error()))
		return latency, false
	}
	return latency, true
}

// suspend snapshots the actor and puts it to sleep, ending a cycle. It reports
// whether the suspend succeeded, like resume.
func (u *sweperfUser) suspend(ctx context.Context) bool {
	err := u.tracedCall(ctx, "SuspendActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.SuspendActor(callCtx, &ateapipb.SuspendActorRequest{
			Actor:   u.ref(),
			Options: &ateapipb.SuspendOptions{Mode: ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
		}, grpc.Trailer(tr))
		return err
	})
	if err != nil {
		slog.Error("SuspendActor failed", slog.String("actor", u.actorName), slog.String("err", err.Error()))
		return false
	}
	return true
}

// delete removes the actor. Errors are recorded as a failed DeleteActor row
// and returned so the caller can retry.
func (u *sweperfUser) delete(ctx context.Context) error {
	return u.tracedCall(ctx, "DeleteActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.DeleteActor(callCtx, &ateapipb.DeleteActorRequest{
			Actor: u.ref(),
			// Teardown discards the actor, so skip the SUSPENDED precondition.
			AnyState: true,
		}, grpc.Trailer(tr))
		return err
	})
}

// teardown releases the actor and the worker it holds, and decrements the
// user gauge. Like retireUser, it deletes with AnyState and sends no suspend
// first. Safe to call more than once and from several goroutines: only the
// first call does anything.
func (u *sweperfUser) teardown(ctx context.Context) {
	if !u.cleanedUp.CompareAndSwap(false, true) {
		return
	}
	_ = u.delete(ctx)
	bmetrics.UpdateUsers(u.userClass, -1)
}

// tracedCall runs one control-plane RPC under a span named name and records
// it as a locust stats row of the same name. It prefers the server-measured
// elapsed time from the response trailer over the client-observed latency, so
// the figure excludes the client's own queueing. The client figure goes out as
// a <name>_rtt row when the two differ.
func (u *sweperfUser) tracedCall(ctx context.Context, name string, do func(context.Context, *metadata.MD) error) error {
	_, err := u.tracedCallRecord(ctx, name, true, do)
	return err
}

// tracedCallRecord is tracedCall with optional success rows; failures are
// always recorded. It also returns the latency it records.
func (u *sweperfUser) tracedCallRecord(ctx context.Context, name string, recordSuccess bool, do func(context.Context, *metadata.MD) error) (time.Duration, error) {
	ctx, span := u.cfg.Tracer.Start(ctx, name)
	defer span.End()

	start := time.Now()
	var tr metadata.MD
	err := do(ctx, &tr)
	clientLatency := time.Since(start)
	if err != nil && canceled(ctx) {
		return clientLatency, err
	}

	latency, source := boomerutil.ElapsedFromMD(tr, ateinterceptors.ServerElapsedTrailer, clientLatency)
	if source == boomerutil.SourceServer {
		span.SetAttributes(attribute.Float64("server.elapsed_ms", boomerutil.MsFloat(latency)))
		if err != nil || recordSuccess {
			u.recordClientRTT(name, clientLatency, err)
		}
	}
	boomerutil.LogSampledTrace(span, name, latency, source, err)
	if err != nil {
		bmetrics.RecordFailure("grpc", name, u.userClass, latency, err.Error())
		return latency, err
	}
	if recordSuccess {
		bmetrics.RecordSuccess("grpc", name, u.userClass, latency, 0)
	}
	return latency, nil
}

// recordClientRTT publishes the client wall clock for an RPC whose own row carries server time.
func (u *sweperfUser) recordClientRTT(name string, clientLatency time.Duration, err error) {
	if err != nil {
		bmetrics.RecordFailure("grpc", name+"_rtt", u.userClass, clientLatency, err.Error())
		return
	}
	bmetrics.RecordSuccess("grpc", name+"_rtt", u.userClass, clientLatency, 0)
}

// statusResponse is the /status liveness reply; Status is "up" once the
// in-sandbox server can serve.
type statusResponse struct {
	Status string `json:"status"`
}

// livenessTimeout bounds how long pollLiveness waits for a new actor.
const livenessTimeout = time.Minute

// pollLiveness waits for the actor's in-sandbox server to answer /status with
// "up", for up to livenessTimeout. The first request through the router is
// also what wakes a newly created actor, so this doubles as the implicit
// resume. It polls at the job-completion interval, since every task starts
// a fresh actor.
func (u *sweperfUser) pollLiveness(ctx context.Context) error {
	statusURL := u.cfg.RouterURL + "/status"
	retryInterval := pollInterval(u.cfg)
	deadline := time.Now().Add(livenessTimeout)

	for attempt := 0; time.Now().Before(deadline); attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, statusURL, nil)
		if err != nil {
			return err
		}
		u.setActorRouting(req)

		resp, err := u.cfg.HTTPClient.Do(req)
		if err == nil {
			body, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr == nil && resp.StatusCode == http.StatusOK {
				var statusResp statusResponse
				if err := json.Unmarshal(body, &statusResp); err == nil {
					if strings.EqualFold(statusResp.Status, "up") {
						slog.Info("Server is up and ready",
							slog.String("actor", u.actorName),
							slog.Int("attempt", attempt),
						)
						return nil
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryInterval):
		}
	}
	return fmt.Errorf("failed to verify server liveness for actor %s", u.actorName)
}

// isDone reports whether every cycle of the trace has run.
func (u *sweperfUser) isDone() bool {
	return u.cycleIndex >= len(u.chunks)
}

// totalSteps is the number of trace steps the session's cycles cover. The
// chunks are contiguous from step 0, so it is the last chunk's end.
func (u *sweperfUser) totalSteps() int {
	if len(u.chunks) == 0 {
		return 0
	}
	return u.chunks[len(u.chunks)-1].end
}

// step runs one cycle: resume the actor, replay that cycle's slice of the
// trace, then suspend it again. The final cycle skips the suspend, since the
// actor is deleted straight after.
func (u *sweperfUser) step(ctx context.Context) {
	if u.cycleIndex >= len(u.chunks) {
		return
	}

	cycleNum := u.cycleIndex + 1
	chunk := u.chunks[u.cycleIndex]

	slog.Info("Starting sweperf cycle",
		slog.String("actor", u.actorName),
		slog.Int("cycle", cycleNum),
		slog.Int("start_step", chunk.start+1),
		slog.Int("end_step", chunk.end),
	)

	// 1. Resume actor
	noopResume := u.awake
	resumeStart := time.Now()
	// Summed per cycle so the between-cycle think time stays out of the total.
	defer func() { u.loopWall += time.Since(resumeStart) }()
	// A cycle cut short by shutdown returns without recording or advancing.
	resumeLatency, resumed := u.resume(ctx)
	if !resumed {
		if canceled(ctx) {
			return
		}
		u.loopFailed = true
		u.loopRetries++
		u.noteFailure("resume", cycleNum)
		bmetrics.RecordFailure(derivedMetricMethod, resumeToFirstExecMetric, u.userClass,
			time.Since(resumeStart), "ResumeActor failed")
		slog.Error("ResumeActor failed in step",
			slog.String("actor", u.actorName),
			slog.Int("cycle", cycleNum),
			slog.Float64("wall_ms", boomerutil.MsFloat(time.Since(resumeStart))),
		)
		return
	}
	u.awake = false
	if !noopResume {
		u.resumeMax = max(u.resumeMax, resumeLatency)
	}

	// 2. Execute step range chunk inside sandbox
	ackAt, execDur, err := u.execute(ctx, cycleNum, chunk.start, chunk.end)
	if err != nil && canceled(ctx) {
		return
	}
	cycleCEL := execDur
	if err != nil {
		u.loopFailed = true
		u.noteFailure("execute", cycleNum)
		cycleCEL = 0
		slog.Error("execute steps failed",
			slog.String("actor", u.actorName),
			slog.Int("cycle", cycleNum),
			slog.String("err", err.Error()),
		)
	} else {
		u.loopCEL += execDur
	}
	var ttfe time.Duration
	if !noopResume && !ackAt.IsZero() {
		ttfe = ackAt.Sub(resumeStart)
		u.ttfeMax = max(u.ttfeMax, ttfe)
	}
	// A no-op resume would record a near-zero sample; failures still count.
	if !noopResume || ackAt.IsZero() {
		u.recordResumeToFirstExec(resumeStart, ackAt, err)
	}
	u.recordCycleCEL(execDur, err)

	// 3. Suspend actor, unless this was the last cycle
	suspended := true
	if cycleNum < len(u.chunks) {
		if suspended = u.suspend(ctx); !suspended {
			if canceled(ctx) {
				return
			}
			u.loopFailed = true
			u.noteFailure("suspend", cycleNum)
		}
	}

	attrs := []any{
		slog.String("actor", u.actorName),
		slog.Int("cycle", cycleNum),
		slog.Float64("cel_ms", boomerutil.MsFloat(cycleCEL)),
		slog.Float64("wall_ms", boomerutil.MsFloat(time.Since(resumeStart))),
		slog.Bool("ok", err == nil && suspended),
	}
	// A no-op resume and a reply without an async ack have nothing to report.
	if !noopResume {
		attrs = append(attrs, slog.Float64("resume_ms", boomerutil.MsFloat(resumeLatency)))
	}
	if ttfe > 0 {
		attrs = append(attrs, slog.Float64("ttfe_ms", boomerutil.MsFloat(ttfe)))
	}
	slog.Info("Completed sweperf cycle", attrs...)

	u.cycleIndex++
}

// errSyncExit marks a synchronous /execute reply whose command exited non-zero:
// the resume and HTTP routing succeeded, so it is not a ResumeToFirstExec failure.
var errSyncExit = errors.New("synchronous exec failed")

// recordResumeToFirstExec times the resume through to the sandbox accepting the
// chunk. A set ackAt is a success even when the chunk fails later, because the
// resume itself worked; a synchronous reply leaves ackAt zero and is skipped.
func (u *sweperfUser) recordResumeToFirstExec(resumeStart, ackAt time.Time, err error) {
	switch {
	case !ackAt.IsZero():
		bmetrics.RecordSuccess(derivedMetricMethod, resumeToFirstExecMetric, u.userClass,
			ackAt.Sub(resumeStart), 0)
	case errors.Is(err, errSyncExit):
		return
	case err != nil:
		bmetrics.RecordFailure(derivedMetricMethod, resumeToFirstExecMetric, u.userClass,
			time.Since(resumeStart), err.Error())
	}
}

// recordCycleCEL reports one chunk's in-container command time. A synchronous
// reply carries no duration, so there is nothing to record.
func (u *sweperfUser) recordCycleCEL(execDur time.Duration, err error) {
	switch {
	case err != nil:
		bmetrics.RecordFailure(derivedMetricMethod, cycleCELMetric, u.userClass, execDur, err.Error())
	case execDur > 0:
		bmetrics.RecordSuccess(derivedMetricMethod, cycleCELMetric, u.userClass, execDur, 0)
	}
}

// recordTaskMetrics reports the finished trajectory's container compute time
// and its wall clock, logs its task record, then resets the loop counters.
func (u *sweperfUser) recordTaskMetrics() {
	u.logTaskRecord()
	if u.loopFailed {
		bmetrics.RecordFailure(derivedMetricMethod, taskCELMetric, u.userClass,
			u.loopCEL, "cycle failure in trajectory")
		bmetrics.RecordFailure(derivedMetricMethod, taskWallClockMetric, u.userClass,
			u.loopWall, "cycle failure in trajectory")
	} else {
		bmetrics.RecordSuccess(derivedMetricMethod, taskCELMetric, u.userClass, u.loopCEL, 0)
		bmetrics.RecordSuccess(derivedMetricMethod, taskWallClockMetric, u.userClass, u.loopWall, 0)
	}
	u.loopCEL = 0
	u.loopWall = 0
	u.loopFailed = false
}

// logTaskRecord logs one line of facts about the finished task, so a separate
// script can decide whether it was good. Nothing here judges the task. The
// cycle and step counts are the ones startUser resolved for this task, so
// records from before and after a mid-run config change stay distinguishable.
func (u *sweperfUser) logTaskRecord() {
	attrs := []any{
		slog.Int("v", 1),
		slog.Int64("user", u.userID),
		slog.String("actor", u.actorName),
		slog.String("template", u.templateName),
		slog.Int("cycles", len(u.chunks)),
		slog.Int("steps", u.totalSteps()),
		slog.Time("task_start", u.taskStart),
		slog.Time("end", time.Now()),
		slog.Float64("startup_ms", boomerutil.MsFloat(u.startupDur)),
		slog.Float64("cel_ms", boomerutil.MsFloat(u.loopCEL)),
		slog.Float64("wall_ms", boomerutil.MsFloat(u.loopWall)),
		slog.Float64("think_ms", boomerutil.MsFloat(u.loopThink)),
		slog.Float64("resume_max_ms", boomerutil.MsFloat(u.resumeMax)),
		slog.Int("retries", u.loopRetries),
		slog.Bool("failed", u.loopFailed),
		slog.String("fail_step", u.failStep),
		slog.Int("fail_cycle", u.failCycle),
	}
	if u.ttfeMax > 0 {
		attrs = append(attrs, slog.Float64("ttfe_max_ms", boomerutil.MsFloat(u.ttfeMax)))
	}
	slog.Info("sweperf task record", attrs...)
}

// noteFailure keeps the task's first failed step and its cycle.
func (u *sweperfUser) noteFailure(step string, cycle int) {
	if u.failStep == "" {
		u.failStep, u.failCycle = step, cycle
	}
}

// executeRequest is the /execute body: the 1-based, inclusive range of trace
// steps to replay.
type executeRequest struct {
	StartStep int `json:"start_step"`
	EndStep   int `json:"end_step"`
}

// executeResponse is the /execute reply. A non-empty JobID means the server
// accepted the work and runs it asynchronously, leaving ExitCode unset until
// the job is polled; otherwise the run is already over and ExitCode, Stderr
// and Error describe how it went.
type executeResponse struct {
	JobID    string `json:"job_id"`
	Status   string `json:"status"`
	ExitCode int    `json:"exit_code"`
	Stderr   string `json:"stderr"`
	Error    string `json:"error"`
}

// jobStatusResponse is the /status?job_id= reply. ExitCode is a pointer
// because a job that has not finished reports none, which is distinct from an
// exit code of 0.
type jobStatusResponse struct {
	JobID    string `json:"job_id"`
	Status   string `json:"status"`
	ExitCode *int   `json:"exit_code"`
	// ExecutionDurationMs is this chunk's in-container command time, not the trajectory's.
	ExecutionDurationMs float64 `json:"execution_duration_ms"`
	CompletedStep       int     `json:"completed_step"`
	Error               string  `json:"error"`
}

// pollJobCompletion polls /status every pollInterval, starting one interval after spawn, until the job ends,
// for at most 2 minutes including in-flight requests; errors on job failure, non-zero exit or timeout.
func (u *sweperfUser) pollJobCompletion(ctx context.Context, jobID string, cycleNum int) (time.Duration, error) {
	retryInterval := pollInterval(u.cfg)
	pollCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	for {
		select {
		case <-pollCtx.Done():
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			return 0, fmt.Errorf("timed out waiting for job %s to complete in cycle %d", jobID, cycleNum)
		case <-time.After(retryInterval):
		}

		url := fmt.Sprintf("%s/status?job_id=%s", u.cfg.RouterURL, jobID)
		req, err := http.NewRequestWithContext(pollCtx, http.MethodGet, url, nil)
		if err != nil {
			return 0, err
		}
		u.setActorRouting(req)

		resp, err := u.cfg.HTTPClient.Do(req)
		if err == nil {
			body, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr == nil && resp.StatusCode == http.StatusOK {
				var jobResp jobStatusResponse
				if err := json.Unmarshal(body, &jobResp); err == nil {
					if strings.EqualFold(jobResp.Status, "COMPLETED") {
						if jobResp.ExitCode != nil && *jobResp.ExitCode != 0 {
							return 0, fmt.Errorf("cycle %d job %s failed with exit code %d", cycleNum, jobID, *jobResp.ExitCode)
						}
						return time.Duration(jobResp.ExecutionDurationMs * float64(time.Millisecond)), nil
					}
					if strings.EqualFold(jobResp.Status, "FAILED") {
						exitCode := -1
						if jobResp.ExitCode != nil {
							exitCode = *jobResp.ExitCode
						}
						return 0, fmt.Errorf("cycle %d job %s failed with exit code %d: %s", cycleNum, jobID, exitCode, jobResp.Error)
					}
				}
			}
		}
	}
}

// execute runs the trace steps of one cycle inside the actor's sandbox and
// times them as Workload_Cycle_<cycleNum>. The server may answer either way:
// a job ID means the run is asynchronous and pollJobCompletion waits it out,
// while an exit code alone means it already finished. The returned time is the
// instant an asynchronous job was accepted, or zero when none was, and the
// duration is the chunk's in-container command time, or zero when unreported.
func (u *sweperfUser) execute(ctx context.Context, cycleNum int, startIdx int, endIdx int) (time.Time, time.Duration, error) {
	metricName := fmt.Sprintf("Workload_Cycle_%d", cycleNum)

	reqPayload := executeRequest{
		StartStep: startIdx + 1,
		EndStep:   endIdx,
	}

	body, err := json.Marshal(reqPayload)
	if err != nil {
		bmetrics.RecordFailure("http", metricName, u.userClass, 0, err.Error())
		return time.Time{}, 0, err
	}

	var ackAt time.Time
	var execDur time.Duration
	_, err = u.httpJSONCall(ctx, metricName, "/execute", body, func(respBytes []byte) error {
		var resp executeResponse
		if err := json.Unmarshal(respBytes, &resp); err != nil {
			return fmt.Errorf("unmarshal executeResponse: %w", err)
		}
		if resp.JobID != "" {
			// A synchronous reply only lands after the chunk ran, so it is not an ack.
			ackAt = time.Now()
			var pollErr error
			execDur, pollErr = u.pollJobCompletion(ctx, resp.JobID, cycleNum)
			return pollErr
		}
		if resp.ExitCode != 0 {
			return fmt.Errorf("%w: cycle %d failed: exit code %d, stderr: %s", errSyncExit, cycleNum, resp.ExitCode, resp.Stderr)
		}
		return nil
	})
	return ackAt, execDur, err
}

// httpJSONCall posts body to route on the actor's in-sandbox server and
// records the result under metricName. validate inspects the response body and
// decides the outcome. A call canceled by shutdown records no failure.
func (u *sweperfUser) httpJSONCall(ctx context.Context, metricName, route string, body []byte, validate func([]byte) error) ([]byte, error) {
	ctx, span := u.cfg.Tracer.Start(ctx, metricName)
	defer span.End()
	recordFailure := func(latency time.Duration, msg string) {
		if !canceled(ctx) {
			bmetrics.RecordFailure("http", metricName, u.userClass, latency, msg)
		}
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.cfg.RouterURL+route, bytes.NewReader(body))
	if err != nil {
		recordFailure(0, err.Error())
		return nil, err
	}
	u.setActorRouting(httpReq)
	httpReq.Header.Set("Content-Type", "application/json")
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(httpReq.Header))

	start := time.Now()
	resp, err := u.cfg.HTTPClient.Do(httpReq)
	clientLatency := time.Since(start)
	if err != nil {
		recordFailure(clientLatency, err.Error())
		return nil, err
	}
	defer resp.Body.Close()

	respBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		recordFailure(clientLatency, readErr.Error())
		return nil, readErr
	}

	serverLatency, source := boomerutil.ElapsedFromHeader(resp.Header, ateinterceptors.ServerElapsedTrailer, clientLatency)
	if source == boomerutil.SourceServer {
		span.SetAttributes(attribute.Float64("server.elapsed_ms", boomerutil.MsFloat(serverLatency)))
	}

	if resp.StatusCode >= 400 {
		httpErr := fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
		boomerutil.LogSampledTrace(span, metricName, clientLatency, boomerutil.SourceClient, httpErr)
		recordFailure(clientLatency, httpErr.Error())
		return nil, httpErr
	}

	if validate != nil {
		if err := validate(respBody); err != nil {
			elapsed := time.Since(start)
			boomerutil.LogSampledTrace(span, metricName, elapsed, boomerutil.SourceClient, err)
			recordFailure(elapsed, err.Error())
			return nil, err
		}
	}

	totalLatency := time.Since(start)
	boomerutil.LogSampledTrace(span, metricName, totalLatency, boomerutil.SourceClient, nil)
	bmetrics.RecordSuccess("http", metricName, u.userClass, totalLatency, int64(len(respBody)))
	return respBody, nil
}
