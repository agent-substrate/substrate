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

// Package agentsession implements the AgentSessionUser locust test: each VU
// is one coding-agent session replaying the scripted steps in script.go
// against a glutton actor. After every step the actor is suspended for the
// step's "LLM thinking" gap; by default the next step's first request wakes
// it through the atenet router (request parking), so the benchmark measures
// the user-visible wake latency Substrate's oversubscription story rests on.
//
// Per-step timings land in locust stats as Step_<name>. The first request
// after each suspend is additionally recorded as WakeFirstTouch — in
// implicit resume mode that row IS the parking wake latency distribution.
package agentsession

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	mathrand "math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/atenet"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/boomerutil"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	bmetrics "github.com/agent-substrate/substrate/internal/benchmarking/boomer/metrics"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
	"github.com/agent-substrate/substrate/internal/benchmarking/glutton"
	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	agentSessionUserClass = "AgentSessionUser"
	// templateNS is the atespace holding the benchmark ActorTemplates.
	templateNS = "benchmark-workloads"
	// templateName is the ActorTemplate instantiated per session: the stock
	// glutton actor. The script needs it deployed with --actor-memory 1Gi
	// (see benchmarking/README.md).
	templateName = "glutton"

	// controlRPCTimeout bounds every control-plane RPC. The router HTTP
	// client already times out at 30s; without a deadline here a
	// server-side hang in SuspendActor or DeleteActor would park the VU
	// goroutine for the rest of the run.
	controlRPCTimeout = 60 * time.Second

	// maxConsecutiveStepFailures is how many ReplaceIfPersistent failures in
	// a row a session tolerates before its actor is deleted and recreated.
	maxConsecutiveStepFailures = 3
)

// burnRate is the per-goroutine sha256 iteration rate BurnCPU reports.
// BurnCPU runs for a fixed wall-clock duration, so Step_* latency stays flat
// when the sandbox is CPU-starved; this rate is what drops.
var burnRate = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "agentsession_burn_cpu_iterations_per_second",
	Help:    "BurnCPU sha256 iterations per second per goroutine; drops under CPU contention while Step_* latency stays flat.",
	Buckets: prometheus.ExponentialBuckets(256, 2, 16),
})

func init() {
	prometheus.MustRegister(burnRate)
	userclass.Add(userclass.Entry{
		Name:       "agentsession",
		LocustFile: "agentsession.py",
		UserClass:  agentSessionUserClass,
		Init:       initAgentSession,
	})
}

func initAgentSession(cfg *userclass.Config) (taskFn func(), shutdown func(context.Context)) {
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer("substrate-boomer/agentsession")
	}
	rt := &runtime{cfg: cfg, steps: Session()}
	rt.ingestBuf = makeIngestBuf(rt.steps)
	return rt.iterate, rt.shutdown
}

// makeIngestBuf pre-generates one random payload sized to the script's
// largest ingest. Generating bytes per op would count client-side
// crypto/rand time inside the step timings and allocate tens of MiB per VU
// per op; glutton only writes the payload, so every session can safely
// slice the same buffer.
func makeIngestBuf(steps []Step) []byte {
	var maxBytes int64
	for _, s := range steps {
		for _, o := range s.Ops {
			if o.kind == opIngest && o.bytes > maxBytes {
				maxBytes = o.bytes
			}
		}
	}
	buf := make([]byte, maxBytes)
	if _, err := rand.Read(buf); err != nil {
		// Payload content is irrelevant to the benchmark; zeros still move
		// the same bytes over the wire.
		slog.Warn("failed to randomize ingest payload; using zeros", slog.String("err", err.Error()))
	}
	return buf
}

// runtime is the per-worker state shared by every boomer goroutine. Each
// goroutine keeps its own session in users, keyed by goroutine ID, because
// boomer offers no per-VU context.
type runtime struct {
	cfg       *userclass.Config
	steps     []Step
	ingestBuf []byte   // shared read-only ingest payload, sized to the largest ingest op
	users     sync.Map // goroutineID -> *sessionUser
}

// think returns the suspended "LLM thinking" gap before step s, which is the
// script's think time scaled by --agentsession-think-scale (0 reads as 1.0),
// with ±20% jitter so a fleet of sessions doesn't move in lockstep.
func (r *runtime) think(s Step) time.Duration {
	scale := r.cfg.Dyn.Load().AgentSessionThinkScale
	if scale <= 0 {
		scale = 1.0
	}
	jitter := 0.8 + 0.4*mathrand.Float64()
	return time.Duration(float64(s.Think) * scale * jitter)
}

// iterate is the boomer task function: one scripted step per call. The VU's
// session is created lazily on first use and wraps back to step 1 after the
// script completes, so one actor keeps producing suspend/resume cycles for
// the whole run.
func (r *runtime) iterate() {
	gid := boomerutil.GoroutineID()
	val, loaded := r.users.Load(gid)
	if !loaded {
		u, err := r.startUser(context.Background())
		if err != nil {
			slog.Warn("agentsession start failed; goroutine will retry next iter",
				slog.String("err", err.Error()))
			time.Sleep(2 * time.Second)
			return
		}
		val, _ = r.users.LoadOrStore(gid, u)
	}
	u := val.(*sessionUser)

	step := r.steps[u.stepIndex]

	// The LLM is "thinking": the actor stays suspended for the gap.
	time.Sleep(r.think(step))

	if u.runStep(context.Background(), step) {
		u.stepIndex = (u.stepIndex + 1) % len(r.steps)
		if u.stepIndex == 0 {
			slog.Info("agent session completed the full script; starting over",
				slog.String("actor", u.actorName))
		}
	}

	if u.broken {
		slog.Warn("agent session wedged; deleting its actor and starting a fresh session",
			slog.String("actor", u.actorName),
			slog.Int("consecutive_failures", u.consecutiveFailures))
		u.suspendAndDelete(context.Background())
		r.users.Delete(gid)
	}
}

// startUser creates the session's actor, waits for its sandbox to serve,
// then suspends it so the first scripted step begins — like every later
// step — with a wake from suspension.
func (r *runtime) startUser(ctx context.Context) (*sessionUser, error) {
	u := &sessionUser{
		cfg:       r.cfg,
		actorName: "agent-" + uuid.NewString(),
		ingestBuf: r.ingestBuf,
	}
	slog.Info("Creating agent session", slog.String("actor", u.actorName))
	bmetrics.UpdateUsers(agentSessionUserClass, 1)

	if err := u.ensureAtespace(ctx); err != nil {
		bmetrics.UpdateUsers(agentSessionUserClass, -1)
		return nil, fmt.Errorf("ensureAtespace: %w", err)
	}
	if err := u.create(ctx); err != nil {
		bmetrics.UpdateUsers(agentSessionUserClass, -1)
		return nil, fmt.Errorf("createActor: %w", err)
	}
	if err := u.waitServing(ctx); err != nil {
		// suspendAndDelete decrements the user gauge; no extra decrement here.
		u.suspendAndDelete(ctx)
		return nil, fmt.Errorf("waitServing: %w", err)
	}
	// Park the fresh actor; step 01 wakes it like any other step. A failed
	// park is re-driven at the top of the first step.
	u.hibernate(ctx)
	return u, nil
}

func (r *runtime) shutdown(ctx context.Context) {
	r.users.Range(func(_, val any) bool {
		val.(*sessionUser).suspendAndDelete(ctx)
		return true
	})
}

// sessionUser is one coding-agent session: a single actor plus its progress
// through the script. Owned by one boomer goroutine; no locking needed.
type sessionUser struct {
	cfg       *userclass.Config
	actorName string
	stepIndex int
	cleanedUp bool
	// ingestBuf is the runtime's shared read-only ingest payload.
	ingestBuf []byte
	// hibernatePending is set by a failed Pause/Suspend: the actor is
	// stranded RUNNING or SUSPENDING. Waking it from there would misreport
	// WakeFirstTouch, so runStep finishes the hibernate first.
	hibernatePending bool
	// consecutiveFailures counts ReplaceIfPersistent failures since the
	// last successful step; see noteFailure.
	consecutiveFailures int
	// broken is set once the actor should be replaced: a CRASHED or
	// otherwise stuck actor never recovers on its own, and without
	// replacement it would wedge its VU for the rest of the run.
	broken bool
}

// noteFailure classifies a failed step or lifecycle call and marks the actor
// broken when a replacement is warranted. Cluster-wide errors (no capacity,
// ate-api-server restarting) are not the actor's fault and do not count.
func (u *sessionUser) noteFailure(err error) {
	switch boomerutil.ClassifyLifecycleFailure(err) {
	case boomerutil.ReplaceNow:
		u.broken = true
	case boomerutil.RetryLater:
	default:
		u.consecutiveFailures++
		if u.consecutiveFailures >= maxConsecutiveStepFailures {
			u.broken = true
		}
	}
}

// noteSuccess clears the failure count: the actor just completed a whole
// step, so the earlier failures were transient after all.
func (u *sessionUser) noteSuccess() {
	u.consecutiveFailures = 0
}

func (u *sessionUser) ref() *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Atespace: u.cfg.Atespace, Name: u.actorName}
}

// runStep wakes the actor, executes the step's ops in order, and suspends
// it again, reporting whether everything succeeded. The wake is measured by
// a dedicated ping sent ahead of the step's ops and recorded as
// WakeFirstTouch; in implicit resume mode that ping is also what triggers
// the parked wake, so the row measures the wake alone rather than the wake
// plus whatever heavy op happens to lead the step.
func (u *sessionUser) runStep(ctx context.Context, step Step) bool {
	// A failed hibernate left the actor awake. In implicit mode the wake
	// ping would return in a few ms and be booked as a WakeFirstTouch
	// success; in explicit mode ResumeActor would fail FailedPrecondition.
	// SuspendActor and PauseActor are re-entrant, so finish the hibernate
	// and pick the step up next iteration.
	if u.hibernatePending {
		u.hibernate(ctx)
		return false
	}

	if u.cfg.Dyn.Load().ResumeMode == dynconfig.ResumeModeExplicit {
		if err := u.resume(ctx); err != nil {
			u.noteFailure(err)
			return false
		}
	}

	wakeStart := time.Now()
	if err := u.execOp(ctx, ping()); err != nil {
		u.noteFailure(err)
		bmetrics.RecordFailure("http", "WakeFirstTouch", agentSessionUserClass, time.Since(wakeStart), err.Error())
		slog.Warn("agent session wake failed",
			slog.String("actor", u.actorName),
			slog.String("step", step.Name),
			slog.String("err", err.Error()))
		u.hibernate(ctx)
		return false
	}
	bmetrics.RecordSuccess("http", "WakeFirstTouch", agentSessionUserClass, time.Since(wakeStart), 0)

	metricName := "Step_" + step.Name
	stepStart := time.Now()
	for i, o := range step.Ops {
		if err := u.execOp(ctx, o); err != nil {
			u.noteFailure(err)
			bmetrics.RecordFailure("http", metricName, agentSessionUserClass, time.Since(stepStart), err.Error())
			slog.Warn("agent session step failed",
				slog.String("actor", u.actorName),
				slog.String("step", step.Name),
				slog.Int("op", i),
				slog.String("err", err.Error()))
			u.hibernate(ctx)
			return false
		}
	}
	u.noteSuccess()
	bmetrics.RecordSuccess("http", metricName, agentSessionUserClass, time.Since(stepStart), 0)

	u.hibernate(ctx)
	return true
}

// execOp performs one scripted op as a glutton RPC through the router.
func (u *sessionUser) execOp(ctx context.Context, o op) error {
	switch o.kind {
	case opIngest:
		payload := u.ingestBuf
		if int64(len(payload)) < o.bytes {
			// Fallback for callers (tests) that built the user by hand.
			payload = make([]byte, o.bytes)
		}
		return u.postProto(ctx, glutton.IngestRoute,
			&gluttonpb.IngestRequest{Key: o.key, Payload: payload[:o.bytes]},
			&gluttonpb.IngestResponse{})
	case opBurnCPU:
		resp := &gluttonpb.BurnCPUResponse{}
		if err := u.postProto(ctx, glutton.BurnCPURoute,
			&gluttonpb.BurnCPURequest{DurationMs: o.millis, Parallelism: o.parallel}, resp); err != nil {
			return err
		}
		observeBurnRate(o, resp.GetIterations())
		return nil
	case opWriteDisk:
		return u.postProto(ctx, glutton.WriteDiskRoute,
			&gluttonpb.WriteDiskRequest{Key: o.key, Size: int32(o.bytes), WriteMode: gluttonpb.WriteMode_WRITE_MODE_TRUNCATE},
			&gluttonpb.WriteDiskResponse{})
	case opReadDiskDigest:
		return u.postProto(ctx, glutton.ReadDiskRoute,
			&gluttonpb.ReadDiskRequest{Key: o.key, ReadMode: gluttonpb.ReadMode_READ_MODE_DIGEST_ONLY},
			&gluttonpb.ReadDiskResponse{})
	case opReadDiskData:
		return u.postProto(ctx, glutton.ReadDiskRoute,
			&gluttonpb.ReadDiskRequest{Key: o.key, ReadMode: gluttonpb.ReadMode_READ_MODE_DATA},
			&gluttonpb.ReadDiskResponse{})
	case opFillRAM:
		// OVERWRITE grows the array on first touch and re-randomizes it in
		// place on later laps; TRUNCATE would reallocate and transiently
		// double the guest heap, which OOMs a tightly-sized sandbox.
		return u.postProto(ctx, glutton.WriteRAMRoute,
			&gluttonpb.WriteRAMRequest{Key: o.key, Size: fmt.Sprintf("%d", o.bytes), WriteMode: gluttonpb.WriteMode_WRITE_MODE_OVERWRITE},
			&gluttonpb.WriteRAMResponse{})
	case opChurnRAM:
		return u.postProto(ctx, glutton.WriteRAMRoute,
			&gluttonpb.WriteRAMRequest{Key: o.key, Size: fmt.Sprintf("%d", o.bytes), WriteMode: gluttonpb.WriteMode_WRITE_MODE_OVERWRITE_ROTATE},
			&gluttonpb.WriteRAMResponse{})
	case opWalkRAM:
		return u.postProto(ctx, glutton.ReadRAMRoute,
			&gluttonpb.ReadRAMRequest{Key: o.key},
			&gluttonpb.ReadRAMResponse{})
	case opPing:
		return u.postProto(ctx, glutton.PingRoute,
			&gluttonpb.PingRequest{Message: "think"},
			&gluttonpb.PingResponse{})
	default:
		return fmt.Errorf("unknown op kind %d", o.kind)
	}
}

// observeBurnRate records a burn's iterations per goroutine-second.
func observeBurnRate(o op, iterations int64) {
	if rate, ok := burnRatePerGoroutine(o, iterations); ok {
		burnRate.Observe(rate)
	}
}

// burnRatePerGoroutine normalizes a BurnCPU result by the burn's goroutine
// count and wall-clock seconds. A zero-duration burn has no rate.
func burnRatePerGoroutine(o op, iterations int64) (float64, bool) {
	if o.millis <= 0 {
		return 0, false
	}
	goroutines := max(int64(o.parallel), 1)
	return float64(iterations) / float64(goroutines) / (float64(o.millis) / 1000), true
}

// postProto POSTs req to the actor's route through the router and
// unmarshals the reply into resp. HTTP status >= 400 is an error carrying
// the response body.
func (u *sessionUser) postProto(ctx context.Context, route string, req, resp proto.Message) error {
	body, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.cfg.RouterURL+route, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set(atenet.TargetActorHeader, u.cfg.Atespace+"/"+u.actorName)
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(httpReq.Header))

	httpResp, err := u.cfg.HTTPClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()
	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return err
	}
	if httpResp.StatusCode >= 400 {
		return fmt.Errorf("%s: HTTP %d: %s", route, httpResp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return proto.Unmarshal(respBody, resp)
}

// ensureAtespace creates the configured atespace, treating AlreadyExists as
// success so concurrent sessions racing the first creation all proceed.
func (u *sessionUser) ensureAtespace(ctx context.Context) error {
	return u.tracedCall(ctx, "CreateAtespace", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.CreateAtespace(callCtx, &ateapipb.CreateAtespaceRequest{
			Atespace: &ateapipb.Atespace{
				Metadata: &ateapipb.ResourceMetadata{Name: u.cfg.Atespace},
			},
		}, grpc.Trailer(tr))
		if s, ok := status.FromError(err); ok && s.Code() == codes.AlreadyExists {
			return nil
		}
		return err
	})
}

func (u *sessionUser) create(ctx context.Context) error {
	return u.tracedCall(ctx, "CreateActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.CreateActor(callCtx, &ateapipb.CreateActorRequest{
			Actor: &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: u.cfg.Atespace, Name: u.actorName},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName},
			},
		}, grpc.Trailer(tr))
		return err
	})
}

// waitServing pings the actor through the router until glutton answers. The
// first request is also what wakes a newly created actor, so this doubles
// as the initial activation.
func (u *sessionUser) waitServing(ctx context.Context) error {
	const maxRetries = 30
	const retryInterval = 2 * time.Second
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		lastErr = u.postProto(ctx, glutton.PingRoute,
			&gluttonpb.PingRequest{Message: "hello"}, &gluttonpb.PingResponse{})
		if lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryInterval):
		}
	}
	return fmt.Errorf("actor %s never served: %w", u.actorName, lastErr)
}

// resume issues ResumeActor, retrying concurrent-update conflicts inside
// the traced call so the reported latency spans every attempt.
func (u *sessionUser) resume(ctx context.Context) error {
	err := u.tracedCall(ctx, "ResumeActor", func(callCtx context.Context, tr *metadata.MD) error {
		return boomerutil.RetryOnConflict(callCtx, func() error {
			_, err := u.cfg.APIStub.ResumeActor(callCtx, &ateapipb.ResumeActorRequest{Actor: u.ref()}, grpc.Trailer(tr))
			return err
		})
	})
	if err != nil {
		if boomerutil.IsCrashed(err) {
			bmetrics.RecordFailure("actor", "CrashCount", agentSessionUserClass, 0, "actor entered ACTOR_STATE_CRASHED")
		}
		slog.Error("ResumeActor failed", slog.String("actor", u.actorName), slog.String("err", err.Error()))
	}
	return err
}

// hibernate suspends (or pauses, per LifecycleMode) the actor at the end of
// a step. A failure leaves hibernatePending set so the next step re-drives
// it instead of waking a stranded actor.
func (u *sessionUser) hibernate(ctx context.Context) {
	var err error
	if u.cfg.Dyn.Load().LifecycleMode == dynconfig.LifecycleModePause {
		err = u.tracedCall(ctx, "PauseActor", func(callCtx context.Context, tr *metadata.MD) error {
			_, err := u.cfg.APIStub.PauseActor(callCtx, &ateapipb.PauseActorRequest{Actor: u.ref()}, grpc.Trailer(tr))
			return err
		})
	} else {
		err = u.tracedCall(ctx, "SuspendActor", func(callCtx context.Context, tr *metadata.MD) error {
			_, err := u.cfg.APIStub.SuspendActor(callCtx, &ateapipb.SuspendActorRequest{Actor: u.ref()}, grpc.Trailer(tr))
			return err
		})
	}
	u.hibernatePending = err != nil
	if err != nil {
		u.noteFailure(err)
	}
}

// suspendAndDelete releases the actor and its worker. Suspend first: a
// worker only frees an actor it can account for, and an actor still awake
// at delete risks being left CRASHED. Safe to call more than once.
func (u *sessionUser) suspendAndDelete(ctx context.Context) {
	if u.cleanedUp {
		return
	}
	suspendCtx, cancel := context.WithTimeout(ctx, controlRPCTimeout)
	_, _ = u.cfg.APIStub.SuspendActor(suspendCtx, &ateapipb.SuspendActorRequest{Actor: u.ref()})
	cancel()
	_ = u.tracedCall(ctx, "DeleteActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.DeleteActor(callCtx, &ateapipb.DeleteActorRequest{Actor: u.ref(), AnyState: true}, grpc.Trailer(tr))
		return err
	})
	bmetrics.UpdateUsers(agentSessionUserClass, -1)
	u.cleanedUp = true
}

// tracedCall runs one control-plane RPC under a span and a deadline, and
// records it as a locust stats row of the same name, preferring the
// server-measured elapsed time from the response trailer.
func (u *sessionUser) tracedCall(ctx context.Context, name string, do func(context.Context, *metadata.MD) error) error {
	ctx, cancel := context.WithTimeout(ctx, controlRPCTimeout)
	defer cancel()
	ctx, span := u.cfg.Tracer.Start(ctx, name)
	defer span.End()

	start := time.Now()
	var tr metadata.MD
	err := do(ctx, &tr)
	latency := time.Since(start)

	serverLatency, source := boomerutil.ElapsedFromMD(tr, ateinterceptors.ServerElapsedTrailer, latency)
	boomerutil.LogSampledTrace(span, name, serverLatency, source, err)
	if err != nil {
		bmetrics.RecordFailure("grpc", name, agentSessionUserClass, latency, err.Error())
		return err
	}
	bmetrics.RecordSuccess("grpc", name, agentSessionUserClass, latency, 0)
	return nil
}
