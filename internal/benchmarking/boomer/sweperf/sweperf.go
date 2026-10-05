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
// SweperfUser locust test. Each user creates an actor from a SWE-perf workload
// template and drives it through a trajectory of steps that are partitioned into
// cycles. This is workload-agnostic and can be used to benchmark any SWE-perf
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
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/atenet"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/boomerutil"
	bmetrics "github.com/agent-substrate/substrate/internal/benchmarking/boomer/metrics"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
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

// totalStepsEnv is the container env var through which an ActorTemplate
// declares its trajectory's step count. benchmarking/workloads/deploy.sh sets
// it from the task's images.json entry; the replay server ignores it. A
// template without it cannot be driven, since nothing else knows how long its
// trace is.
const totalStepsEnv = "SWEPERF_TOTAL_STEPS"

// getTemplateTimeout bounds the GetActorTemplate call that reads a template's
// step count.
const getTemplateTimeout = 30 * time.Second

// Defaults for a sweperf session, used when neither the dynamic config nor the
// environment supplies a value.
const (
	sweperfUserClass        = "SweperfUser"
	defaultSweperfTemplate  = "sweperf-astropy-7336"
	defaultSweperfNumCycles = 4
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
		Init:       initSweperf,
	})
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
// suspend+deletes every actor this worker created).
func initSweperf(cfg *userclass.Config) (taskFn func(), shutdown func(context.Context)) {
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer("substrate-boomer/sweperf")
	}

	rt := &sweperfRuntime{
		cfg: cfg,
	}
	return rt.iterate, rt.shutdown
}

// sweperfRuntime is the per-worker state shared by every boomer goroutine.
// Each goroutine keeps its own session in users, keyed by goroutine ID,
// because boomer offers no per-VU context.
type sweperfRuntime struct {
	cfg   *userclass.Config
	users sync.Map // goroutineID -> *sweperfUser

	// stepsMu guards stepsCache, the step count each template declares
	// through totalStepsEnv. Templates are immutable, so a lookup is cached
	// for the life of the worker.
	stepsMu    sync.Mutex
	stepsCache map[string]int
}

// resolveConfig returns the template and cycle count for a new session.
// Values come from dynconfig, which the master populates from the --sweperf-*
// locust flags (common/sweperf_config.py); an unset field falls back to the
// built-in default below. The step count comes from the template itself (see
// templateSteps).
func (r *sweperfRuntime) resolveConfig() (string, int) {
	dyn := r.cfg.Dyn.Load()

	template := dyn.SweperfTemplate
	if template == "" {
		template = defaultSweperfTemplate
	}

	numCycles := dyn.SweperfNumCycles
	if numCycles <= 0 {
		numCycles = defaultSweperfNumCycles
	}

	return template, numCycles
}

// templateSteps returns the step count the named ActorTemplate declares
// through its totalStepsEnv container env var. The template is read with
// GetActorTemplate once per worker and cached; a failed read, or a template
// that declares no valid step count, is returned uncached as an error, so
// the next session retries.
func (r *sweperfRuntime) templateSteps(ctx context.Context, template string) (int, error) {
	r.stepsMu.Lock()
	defer r.stepsMu.Unlock()
	if n, ok := r.stepsCache[template]; ok {
		return n, nil
	}

	callCtx, cancel := context.WithTimeout(ctx, getTemplateTimeout)
	defer cancel()
	tmpl, err := r.cfg.APIStub.GetActorTemplate(callCtx, &ateapipb.GetActorTemplateRequest{
		ActorTemplate: &ateapipb.ObjectRef{Atespace: templateNS, Name: template},
	})
	if err != nil {
		return 0, fmt.Errorf("GetActorTemplate %s/%s: %w", templateNS, template, err)
	}
	n, err := stepsFromTemplate(tmpl)
	if err != nil {
		return 0, fmt.Errorf("template %s/%s: %w", templateNS, template, err)
	}
	if r.stepsCache == nil {
		r.stepsCache = make(map[string]int)
	}
	r.stepsCache[template] = n
	return n, nil
}

// stepsFromTemplate reads totalStepsEnv from the template's containers. It
// errors when no container sets it or the value is not a positive integer.
func stepsFromTemplate(tmpl *ateapipb.ActorTemplate) (int, error) {
	for _, c := range tmpl.GetContainers() {
		for _, e := range c.GetEnv() {
			if e.GetName() != totalStepsEnv {
				continue
			}
			n, err := strconv.Atoi(e.GetValue())
			if err != nil || n <= 0 {
				return 0, fmt.Errorf("container %s: %s=%q is not a positive integer", c.GetName(), totalStepsEnv, e.GetValue())
			}
			return n, nil
		}
	}
	return 0, fmt.Errorf("no container sets %s; deploy SWE-perf templates with benchmarking/workloads/deploy.sh", totalStepsEnv)
}

// pollInterval is the /status poll interval from dynconfig
// (--sweperf-poll-interval-ms), or the default when unset. Read per job, so a
// mid-run change applies to the next cycle.
func pollInterval(cfg *userclass.Config) time.Duration {
	if ms := cfg.Dyn.Load().SweperfPollIntervalMs; ms > 0 {
		return time.Duration(ms) * time.Millisecond
	}
	return defaultSweperfPollInterval
}

// dynamicWait is the think time between cycles: a uniform draw from
// [MinWait, MaxWait), or MinWait when the range is empty.
func (r *sweperfRuntime) dynamicWait() time.Duration {
	cfg := r.cfg.Dyn.Load()
	if cfg.MaxWait <= cfg.MinWait {
		return cfg.MinWait
	}
	jitter := cfg.MaxWait - cfg.MinWait
	return cfg.MinWait + time.Duration(rand.Float64()*float64(jitter))
}

// iterate is the boomer task function, one call per goroutine per iteration.
// It binds a session to the calling goroutine on first use and runs one cycle
// per call thereafter, looping back to the first cycle when the trace is
// exhausted so the actor keeps serving load. A session that fails to start is
// retried on the next iteration.
func (r *sweperfRuntime) iterate() {
	gid := boomerutil.GoroutineID()
	val, loaded := r.users.Load(gid)
	if !loaded {
		u, err := r.startUser(context.Background())
		if err != nil {
			slog.Warn("sweperf on_start failed; goroutine will retry next iter",
				slog.String("err", err.Error()))
			time.Sleep(r.dynamicWait())
			return
		}
		val, _ = r.users.LoadOrStore(gid, u)
	}
	user := val.(*sweperfUser)

	ctx := context.Background()
	user.step(ctx)

	if user.isDone() {
		user.recordTaskMetrics()
		user.resetCycles()
		slog.Info("Actor finished all cycles and reset session back to cycle 1",
			slog.String("actor", user.actorName),
			slog.Int("cycles", len(user.chunks)),
		)
	}

	time.Sleep(r.dynamicWait())
}

// startUser creates an actor and blocks until its sandbox serves, so the
// caller gets a session that is ready to take cycles. Resolves the config per
// session, which lets a value changed mid-run apply to later sessions. The
// step count is the one the template declares. On failure it tears the actor
// down.
func (r *sweperfRuntime) startUser(ctx context.Context) (*sweperfUser, error) {
	tmpl, numCycles := r.resolveConfig()
	totalSteps, err := r.templateSteps(ctx, tmpl)
	if err != nil {
		return nil, fmt.Errorf("templateSteps: %w", err)
	}
	chunks := generateDynamicChunks(totalSteps, numCycles)

	u := &sweperfUser{
		cfg:          r.cfg,
		actorName:    "sb-" + uuid.NewString(),
		templateName: tmpl,
		userClass:    sweperfUserClass,
		chunks:       chunks,
		cycleIndex:   0,
	}

	slog.Info("Creating new sweperf user session",
		slog.String("actor", u.actorName),
		slog.String("template", u.templateName),
		slog.Int("total_steps", totalSteps),
	)

	bmetrics.UpdateUsers(u.userClass, 1)

	if err := u.ensureAtespace(ctx); err != nil {
		bmetrics.UpdateUsers(u.userClass, -1)
		return nil, fmt.Errorf("ensureAtespace: %w", err)
	}
	if err := u.create(ctx); err != nil {
		bmetrics.UpdateUsers(u.userClass, -1)
		return nil, fmt.Errorf("createActor: %w", err)
	}

	// poll status for liveness
	if err := u.pollLiveness(ctx); err != nil {
		u.suspendAndDelete(ctx)
		bmetrics.UpdateUsers(u.userClass, -1)
		return nil, fmt.Errorf("pollLiveness: %w", err)
	}

	// pollLiveness left the actor running, so cycle 1's resume is a no-op.
	u.awake = true

	slog.Info("Sweperf user session is ready", slog.String("actor", u.actorName))
	return u, nil
}

// shutdown suspends and deletes every actor this worker created. Boomer has
// no per-VU stop hook, so a mid-run decrease in user count leaks actors until
// this runs — acceptable for a run that ramps up, holds, then tears down.
func (r *sweperfRuntime) shutdown(ctx context.Context) {
	r.users.Range(func(_, val any) bool {
		u := val.(*sweperfUser)
		if !u.cleanedUp {
			u.suspendAndDelete(ctx)
			u.cleanedUp = true
		}
		return true
	})
}

// sweperfUser is one benchmark session: a single actor, the cycle plan it
// replays, and its progress through that plan. Owned by one boomer goroutine,
// so its fields need no locking.
type sweperfUser struct {
	cfg          *userclass.Config
	actorName    string
	templateName string
	userClass    string
	chunks       []chunk
	cycleIndex   int
	cleanedUp    bool
	// awake is set while the actor is still running from pollLiveness; the
	// next resume is then a no-op and its success samples are not recorded.
	awake bool
	// Container compute and per-cycle wall clock so far, and whether any cycle failed.
	loopCEL    time.Duration
	loopWall   time.Duration
	loopFailed bool
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

// resume wakes the actor for the next cycle and reports whether it worked. A
// failure is the caller's cue to abandon the cycle rather than talk to a
// sandbox that is not running.
func (u *sweperfUser) resume(ctx context.Context) bool {
	err := u.tracedCallRecord(ctx, "ResumeActor", !u.awake, func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.ResumeActor(callCtx, &ateapipb.ResumeActorRequest{
			Actor: u.ref(),
		}, grpc.Trailer(tr))
		return err
	})
	if err != nil {
		slog.Error("ResumeActor failed", slog.String("actor", u.actorName), slog.String("err", err.Error()))
		return false
	}
	return true
}

// suspend snapshots the actor and puts it to sleep, ending a cycle. It reports
// whether the suspend succeeded, like resume.
func (u *sweperfUser) suspend(ctx context.Context) bool {
	err := u.tracedCall(ctx, "SuspendActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.SuspendActor(callCtx, &ateapipb.SuspendActorRequest{
			Actor: u.ref(),
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
// and otherwise ignored, since the only caller is already tearing down.
func (u *sweperfUser) delete(ctx context.Context) {
	_ = u.tracedCall(ctx, "DeleteActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.DeleteActor(callCtx, &ateapipb.DeleteActorRequest{
			Actor: u.ref(),
			// Teardown discards the actor, so skip the SUSPENDED precondition.
			AnyState: true,
		}, grpc.Trailer(tr))
		return err
	})
}

// suspendAndDelete releases the actor and the worker it holds, and decrements
// the user gauge. It suspends first because a worker only frees an actor it
// can account for, and an actor still awake at delete risks being left
// CRASHED. Safe to call more than once: it is a no-op after the first.
func (u *sweperfUser) suspendAndDelete(ctx context.Context) {
	if u.cleanedUp {
		return
	}
	_, _ = u.cfg.APIStub.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: u.ref(),
	})
	u.delete(ctx)
	bmetrics.UpdateUsers(u.userClass, -1)
	u.cleanedUp = true
}

// tracedCall runs one control-plane RPC under a span named name and records
// it as a locust stats row of the same name. It prefers the server-measured
// elapsed time from the response trailer over the client-observed latency, so
// the figure excludes the client's own queueing. The client figure goes out as
// a <name>_rtt row when the two differ.
func (u *sweperfUser) tracedCall(ctx context.Context, name string, do func(context.Context, *metadata.MD) error) error {
	return u.tracedCallRecord(ctx, name, true, do)
}

// tracedCallRecord is tracedCall with optional success rows; failures are
// always recorded.
func (u *sweperfUser) tracedCallRecord(ctx context.Context, name string, recordSuccess bool, do func(context.Context, *metadata.MD) error) error {
	ctx, span := u.cfg.Tracer.Start(ctx, name)
	defer span.End()

	start := time.Now()
	var tr metadata.MD
	err := do(ctx, &tr)
	clientLatency := time.Since(start)

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
		return err
	}
	if recordSuccess {
		bmetrics.RecordSuccess("grpc", name, u.userClass, latency, 0)
	}
	return nil
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

// pollLiveness waits for the actor's in-sandbox server to answer /status with
// "up", for up to a minute. The first request through the router is also what
// wakes a newly created actor, so this doubles as the implicit resume.
func (u *sweperfUser) pollLiveness(ctx context.Context) error {
	statusURL := u.cfg.RouterURL + "/status"
	maxRetries := 30
	retryInterval := 2 * time.Second

	for attempt := 0; attempt < maxRetries; attempt++ {
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

// resetCycles rewinds to the first cycle so the same actor replays the trace
// again, which is how one session keeps producing load for the whole run.
func (u *sweperfUser) resetCycles() {
	u.cycleIndex = 0
}

// step runs one cycle: resume the actor, replay that cycle's slice of the
// trace, then suspend it again.
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
	if !u.resume(ctx) {
		u.loopFailed = true
		bmetrics.RecordFailure(derivedMetricMethod, resumeToFirstExecMetric, u.userClass,
			time.Since(resumeStart), "ResumeActor failed")
		slog.Error("ResumeActor failed in step", slog.String("actor", u.actorName), slog.Int("cycle", cycleNum))
		return
	}
	u.awake = false

	// 2. Execute step range chunk inside sandbox
	ackAt, execDur, err := u.execute(ctx, cycleNum, chunk.start, chunk.end)
	if err != nil {
		u.loopFailed = true
		slog.Error("execute steps failed",
			slog.String("actor", u.actorName),
			slog.Int("cycle", cycleNum),
			slog.String("err", err.Error()),
		)
	} else {
		u.loopCEL += execDur
	}
	// A no-op resume would record a near-zero sample; failures still count.
	if !noopResume || ackAt.IsZero() {
		u.recordResumeToFirstExec(resumeStart, ackAt, err)
	}
	u.recordCycleCEL(execDur, err)

	// 3. Suspend actor
	if !u.suspend(ctx) {
		u.loopFailed = true
	}

	slog.Info("Completed sweperf cycle",
		slog.String("actor", u.actorName),
		slog.Int("cycle", cycleNum),
	)

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
// and its wall clock, then rearms the counters for the next pass.
func (u *sweperfUser) recordTaskMetrics() {
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
// decides the outcome.
func (u *sweperfUser) httpJSONCall(ctx context.Context, metricName, route string, body []byte, validate func([]byte) error) ([]byte, error) {
	ctx, span := u.cfg.Tracer.Start(ctx, metricName)
	defer span.End()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.cfg.RouterURL+route, bytes.NewReader(body))
	if err != nil {
		bmetrics.RecordFailure("http", metricName, u.userClass, 0, err.Error())
		return nil, err
	}
	u.setActorRouting(httpReq)
	httpReq.Header.Set("Content-Type", "application/json")
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(httpReq.Header))

	start := time.Now()
	resp, err := u.cfg.HTTPClient.Do(httpReq)
	clientLatency := time.Since(start)
	if err != nil {
		bmetrics.RecordFailure("http", metricName, u.userClass, clientLatency, err.Error())
		return nil, err
	}
	defer resp.Body.Close()

	respBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		bmetrics.RecordFailure("http", metricName, u.userClass, clientLatency, readErr.Error())
		return nil, readErr
	}

	serverLatency, source := boomerutil.ElapsedFromHeader(resp.Header, ateinterceptors.ServerElapsedTrailer, clientLatency)
	if source == boomerutil.SourceServer {
		span.SetAttributes(attribute.Float64("server.elapsed_ms", boomerutil.MsFloat(serverLatency)))
	}

	if resp.StatusCode >= 400 {
		httpErr := fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
		boomerutil.LogSampledTrace(span, metricName, clientLatency, boomerutil.SourceClient, httpErr)
		bmetrics.RecordFailure("http", metricName, u.userClass, clientLatency, httpErr.Error())
		return nil, httpErr
	}

	if validate != nil {
		if err := validate(respBody); err != nil {
			elapsed := time.Since(start)
			boomerutil.LogSampledTrace(span, metricName, elapsed, boomerutil.SourceClient, err)
			bmetrics.RecordFailure("http", metricName, u.userClass, elapsed, err.Error())
			return nil, err
		}
	}

	totalLatency := time.Since(start)
	boomerutil.LogSampledTrace(span, metricName, totalLatency, boomerutil.SourceClient, nil)
	bmetrics.RecordSuccess("http", metricName, u.userClass, totalLatency, int64(len(respBody)))
	return respBody, nil
}
