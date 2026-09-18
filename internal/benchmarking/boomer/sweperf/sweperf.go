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
// cycles. This is workload-agnostic and can be used to benchmark any SWE-bench
// workload by providing the appropriate template and dynamic configuration.

package sweperf

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
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

// Defaults for a sweperf session, used when neither the dynamic config nor the
// environment supplies a value.
const (
	sweperfUserClass         = "SweperfUser"
	defaultSweperfTemplate   = "swebench-astropy-7336"
	defaultSweperfTotalSteps = 21
	defaultSweperfNumCycles  = 4
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
}

// resolveConfig returns the template, total step count and cycle count for a
// new session. Values come from dynconfig, which the master populates from
// the --sweperf-* locust flags (common/sweperf_config.py); an unset field
// falls back to the built-in default below.
func (r *sweperfRuntime) resolveConfig() (string, int, int) {
	dyn := r.cfg.Dyn.Load()

	template := dyn.SweperfTemplate
	if template == "" {
		template = defaultSweperfTemplate
	}

	totalSteps := dyn.SweperfTotalSteps
	if totalSteps <= 0 {
		totalSteps = defaultSweperfTotalSteps
	}

	numCycles := dyn.SweperfNumCycles
	if numCycles <= 0 {
		numCycles = defaultSweperfNumCycles
	}

	return template, totalSteps, numCycles
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
// session, which lets a value changed mid-run apply to later sessions. On
// failure it tears the actor down.
func (r *sweperfRuntime) startUser(ctx context.Context) (*sweperfUser, error) {
	tmpl, totalSteps, numCycles := r.resolveConfig()
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
	err := u.tracedCall(ctx, "ResumeActor", func(callCtx context.Context, tr *metadata.MD) error {
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

// suspend snapshots the actor and puts it to sleep, ending a cycle. A failure
// is logged and recorded but not returned: the run continues, and the next
// resume reports the resulting state.
func (u *sweperfUser) suspend(ctx context.Context) {
	err := u.tracedCall(ctx, "SuspendActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.SuspendActor(callCtx, &ateapipb.SuspendActorRequest{
			Actor: u.ref(),
		}, grpc.Trailer(tr))
		return err
	})
	if err != nil {
		slog.Error("SuspendActor failed", slog.String("actor", u.actorName), slog.String("err", err.Error()))
	}
}

// delete removes the actor. Errors are recorded as a failed DeleteActor row
// and otherwise ignored, since the only caller is already tearing down.
func (u *sweperfUser) delete(ctx context.Context) {
	_ = u.tracedCall(ctx, "DeleteActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.DeleteActor(callCtx, &ateapipb.DeleteActorRequest{
			Actor: u.ref(),
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
// the figure excludes the client's own queueing.
func (u *sweperfUser) tracedCall(ctx context.Context, name string, do func(context.Context, *metadata.MD) error) error {
	ctx, span := u.cfg.Tracer.Start(ctx, name)
	defer span.End()

	start := time.Now()
	var tr metadata.MD
	err := do(ctx, &tr)
	clientLatency := time.Since(start)

	latency, source := boomerutil.ElapsedFromMD(tr, ateinterceptors.ServerElapsedTrailer, clientLatency)
	if source == boomerutil.SourceServer {
		span.SetAttributes(attribute.Float64("server.elapsed_ms", boomerutil.MsFloat(latency)))
	}
	boomerutil.LogSampledTrace(span, name, latency, source, err)
	if err != nil {
		bmetrics.RecordFailure("grpc", name, u.userClass, latency, err.Error())
		return err
	}
	bmetrics.RecordSuccess("grpc", name, u.userClass, latency, 0)
	return nil
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
	if !u.resume(ctx) {
		slog.Error("ResumeActor failed in step", slog.String("actor", u.actorName), slog.Int("cycle", cycleNum))
		return
	}

	// 2. Execute step range chunk inside sandbox
	if err := u.execute(ctx, cycleNum, chunk.start, chunk.end); err != nil {
		slog.Error("execute steps failed",
			slog.String("actor", u.actorName),
			slog.Int("cycle", cycleNum),
			slog.String("err", err.Error()),
		)
	}

	// 3. Suspend actor
	u.suspend(ctx)

	slog.Info("Completed sweperf cycle",
		slog.String("actor", u.actorName),
		slog.Int("cycle", cycleNum),
	)

	u.cycleIndex++
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
	JobID         string `json:"job_id"`
	Status        string `json:"status"`
	ExitCode      *int   `json:"exit_code"`
	CompletedStep int    `json:"completed_step"`
	Error         string `json:"error"`
}

// pollJobCompletion waits for an asynchronous /execute job to reach a terminal
// state, for up to two minutes. It returns an error when the job fails, when
// it completes with a non-zero exit code, or when that budget runs out.
func (u *sweperfUser) pollJobCompletion(ctx context.Context, jobID string, cycleNum int) error {
	const maxRetries = 600
	const retryInterval = 200 * time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		url := fmt.Sprintf("%s/status?job_id=%s", u.cfg.RouterURL, jobID)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
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
							return fmt.Errorf("cycle %d job %s failed with exit code %d", cycleNum, jobID, *jobResp.ExitCode)
						}
						return nil
					}
					if strings.EqualFold(jobResp.Status, "FAILED") {
						exitCode := -1
						if jobResp.ExitCode != nil {
							exitCode = *jobResp.ExitCode
						}
						return fmt.Errorf("cycle %d job %s failed with exit code %d: %s", cycleNum, jobID, exitCode, jobResp.Error)
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
	return fmt.Errorf("timed out waiting for job %s to complete in cycle %d", jobID, cycleNum)
}

// execute runs the trace steps of one cycle inside the actor's sandbox and
// times them as Workload_Cycle_<cycleNum>. The server may answer either way:
// a job ID means the run is asynchronous and pollJobCompletion waits it out,
// while an exit code alone means it already finished.
func (u *sweperfUser) execute(ctx context.Context, cycleNum int, startIdx int, endIdx int) error {
	metricName := fmt.Sprintf("Workload_Cycle_%d", cycleNum)

	reqPayload := executeRequest{
		StartStep: startIdx + 1,
		EndStep:   endIdx,
	}

	body, err := json.Marshal(reqPayload)
	if err != nil {
		bmetrics.RecordFailure("http", metricName, u.userClass, 0, err.Error())
		return err
	}

	_, err = u.httpJSONCall(ctx, metricName, "/execute", body, func(respBytes []byte) error {
		var resp executeResponse
		if err := json.Unmarshal(respBytes, &resp); err != nil {
			return fmt.Errorf("unmarshal executeResponse: %w", err)
		}
		if resp.JobID != "" {
			return u.pollJobCompletion(ctx, resp.JobID, cycleNum)
		}
		if resp.ExitCode != 0 {
			return fmt.Errorf("cycle %d failed: exit code %d, stderr: %s", cycleNum, resp.ExitCode, resp.Stderr)
		}
		return nil
	})
	return err
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
