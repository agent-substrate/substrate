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

// Package glutton implements the boomer-Go re-implementation of the
// GluttonUser locust test (see the legacy Python in
// benchmarking/locust/tests/glutton.py for the reference behavior).
package glutton

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	bmetrics "github.com/agent-substrate/substrate/internal/benchmarking/boomer/metrics"
	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	userClass    = "GluttonUser"
	templateName = "glutton"
	templateNS   = "benchmark-workloads"
	actorDomain  = "actors.resources.substrate.ate.dev"
	pingPath     = "/ping"

	sourceClient = "client"
	sourceServer = "server"
)

// Config holds the dependencies a glutton task needs. Built once at startup
// and passed to Register.
type Config struct {
	// APIStub is the shared gRPC client to ateapi (one connection, all goroutines).
	APIStub ateapipb.ControlClient
	// HTTPClient is the shared HTTP client for atenet pings.
	HTTPClient *http.Client
	// RouterURL is the base URL of the atenet router (no trailing slash).
	RouterURL string
	// Atespace every actor this worker creates lives in. Required; caller
	// is responsible for having ensured it exists (see EnsureAtespace).
	Atespace string
	// Dyn is the runtime-mutable config (wait-time bounds, trace
	// probability). Required — every per-iteration read goes through it,
	// so tests can mutate it without touching glutton internals.
	Dyn *dynconfig.Holder
	// Tracer anchors sampled spans; falls back to the otel global if nil.
	Tracer trace.Tracer
}

// Register creates a runtime tied to cfg and returns a boomer-compatible task
// function plus a Shutdown hook the caller should run before exit (it
// suspend+deletes every actor this worker created).
func Register(cfg *Config) (taskFn func(), shutdown func(context.Context)) {
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer("substrate-boomer/glutton")
	}
	rt := &taskRuntime{cfg: cfg}
	return rt.iterate, rt.shutdown
}

type taskRuntime struct {
	cfg   *Config
	users sync.Map // goroutineID → *gluttonUser
}

// iterate is the task function boomer calls in a loop on each VU goroutine.
// On first call from a given goroutine we lazily create the user's actor
// (the analog of locust's per-user on_start); subsequent calls run a
// resume/ping/suspend cycle.
func (r *taskRuntime) iterate() {
	gid := goroutineID()
	val, loaded := r.users.Load(gid)
	if !loaded {
		u, err := r.startUser(context.Background())
		if err != nil {
			slog.Warn("glutton on_start failed; goroutine will retry next iter",
				slog.String("err", err.Error()))
			return
		}
		val, _ = r.users.LoadOrStore(gid, u)
	}
	user := val.(*gluttonUser)

	ctx := context.Background()
	if !user.resume(ctx) {
		return
	}
	user.ping(ctx)
	user.suspend(ctx)

	time.Sleep(r.dynamicWait())
}

func (r *taskRuntime) startUser(ctx context.Context) (*gluttonUser, error) {
	u := &gluttonUser{
		cfg:         r.cfg,
		actorID:     "sb-" + uuid.NewString(),
		firstResume: true,
	}
	u.hostHeader = u.actorID + "." + u.cfg.Atespace + "." + actorDomain
	bmetrics.UpdateUsers(userClass, 1)
	if err := u.ensureAtespace(ctx); err != nil {
		bmetrics.UpdateUsers(userClass, -1)
		return nil, err
	}
	if err := u.create(ctx); err != nil {
		bmetrics.UpdateUsers(userClass, -1)
		return nil, err
	}
	return u, nil
}

// shutdown suspends (if still running) and deletes every actor this worker
// created. Boomer has no per-VU stop hook, so a mid-run user-count decrease
// leaks actors until shutdown — acceptable for benchmark runs that ramp up,
// hold, then tear down cleanly.
func (r *taskRuntime) shutdown(ctx context.Context) {
	r.users.Range(func(_, val any) bool {
		u := val.(*gluttonUser)
		if u.actorRunning {
			u.suspend(ctx)
		}
		u.delete(ctx)
		bmetrics.UpdateUsers(userClass, -1)
		return true
	})
}

func (r *taskRuntime) dynamicWait() time.Duration {
	cfg := r.cfg.Dyn.Load()
	if cfg.MaxWait <= cfg.MinWait {
		return cfg.MinWait
	}
	jitter := cfg.MaxWait - cfg.MinWait
	return cfg.MinWait + time.Duration(rand.Float64()*float64(jitter))
}

type gluttonUser struct {
	cfg          *Config
	actorID      string
	hostHeader   string
	firstResume  bool
	actorRunning bool
}

func (u *gluttonUser) ref() *ateapipb.ActorRef {
	return &ateapipb.ActorRef{Atespace: u.cfg.Atespace, Name: u.actorID}
}

// ensureAtespace creates the configured atespace, swallowing AlreadyExists
// so concurrent VUs racing the first creation all see it as a success. The
// call goes through tracedCall so it shows up in stats/spans like every
// other API call.
func (u *gluttonUser) ensureAtespace(ctx context.Context) error {
	return u.tracedCall(ctx, "CreateAtespace", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.CreateAtespace(callCtx, &ateapipb.CreateAtespaceRequest{
			Name: u.cfg.Atespace,
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

func (u *gluttonUser) create(ctx context.Context) error {
	return u.tracedCall(ctx, "CreateActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.CreateActor(callCtx, &ateapipb.CreateActorRequest{
			ActorRef:               u.ref(),
			ActorTemplateNamespace: templateNS,
			ActorTemplateName:      templateName,
		}, grpc.Trailer(tr))
		return err
	})
}

func (u *gluttonUser) resume(ctx context.Context) bool {
	metricName := "ResumeActor"
	if u.firstResume {
		metricName = "ResumeActorColdStart"
	}
	err := u.tracedCall(ctx, metricName, func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.ResumeActor(callCtx, &ateapipb.ResumeActorRequest{
			ActorRef: u.ref(),
			Boot:     u.firstResume,
		}, grpc.Trailer(tr))
		return err
	})
	if err != nil {
		return false
	}
	u.firstResume = false
	u.actorRunning = true
	return true
}

func (u *gluttonUser) suspend(ctx context.Context) {
	_ = u.tracedCall(ctx, "SuspendActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.SuspendActor(callCtx, &ateapipb.SuspendActorRequest{
			ActorRef: u.ref(),
		}, grpc.Trailer(tr))
		return err
	})
	u.actorRunning = false
}

func (u *gluttonUser) delete(ctx context.Context) {
	_ = u.tracedCall(ctx, "DeleteActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.DeleteActor(callCtx, &ateapipb.DeleteActorRequest{
			ActorRef: u.ref(),
		}, grpc.Trailer(tr))
		return err
	})
}

// tracedCall wraps a unary gRPC call with a span and Prometheus/locust
// reporting. The reported latency comes from the server-side trailer
// emitted by ateinterceptors.ServerUnaryInterceptor when present, falling
// back to client-measured wall clock otherwise.
func (u *gluttonUser) tracedCall(ctx context.Context, name string, do func(context.Context, *metadata.MD) error) error {
	ctx, span := u.cfg.Tracer.Start(ctx, name)
	defer span.End()

	start := time.Now()
	var tr metadata.MD
	err := do(ctx, &tr)
	clientLatency := time.Since(start)

	latency, source := elapsedFromMD(tr, ateinterceptors.ServerElapsedTrailer, clientLatency)
	if source == sourceServer {
		span.SetAttributes(attribute.Float64("server.elapsed_ms", msFloat(latency)))
	}
	logSampledTrace(span, name, latency, source, err)
	if err != nil {
		bmetrics.RecordFailure("grpc", name, userClass, latency, err.Error())
		return err
	}
	bmetrics.RecordSuccess("grpc", name, userClass, latency, 0)
	return nil
}

func (u *gluttonUser) ping(ctx context.Context) {
	ctx, span := u.cfg.Tracer.Start(ctx, "GluttonPing")
	defer span.End()

	message := uuid.NewString()
	body, err := proto.Marshal(&gluttonpb.PingRequest{Message: message})
	if err != nil {
		bmetrics.RecordFailure("http", "GluttonPing", userClass, 0, err.Error())
		return
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.cfg.RouterURL+pingPath, bytes.NewReader(body))
	if err != nil {
		bmetrics.RecordFailure("http", "GluttonPing", userClass, 0, err.Error())
		return
	}
	httpReq.Host = u.hostHeader
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(httpReq.Header))

	start := time.Now()
	resp, err := u.cfg.HTTPClient.Do(httpReq)
	clientLatency := time.Since(start)
	if err != nil {
		bmetrics.RecordFailure("http", "GluttonPing", userClass, clientLatency, err.Error())
		return
	}
	defer resp.Body.Close()

	respBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		bmetrics.RecordFailure("http", "GluttonPing", userClass, clientLatency, readErr.Error())
		return
	}

	if resp.StatusCode >= 400 {
		httpErr := fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
		logSampledTrace(span, "GluttonPing", clientLatency, sourceClient, httpErr)
		bmetrics.RecordFailure("http", "GluttonPing", userClass, clientLatency, httpErr.Error())
		return
	}

	pong := &gluttonpb.PingResponse{}
	if err := proto.Unmarshal(respBody, pong); err != nil {
		logSampledTrace(span, "GluttonPing", clientLatency, sourceClient, err)
		bmetrics.RecordFailure("http", "GluttonPing", userClass, clientLatency, err.Error())
		return
	}
	if pong.Message != message {
		mismatch := fmt.Errorf("ping echo mismatch: sent=%q recv=%q", message, pong.Message)
		logSampledTrace(span, "GluttonPing", clientLatency, sourceClient, mismatch)
		bmetrics.RecordFailure("http", "GluttonPing", userClass, clientLatency, mismatch.Error())
		return
	}
	logSampledTrace(span, "GluttonPing", clientLatency, sourceClient, nil)
	bmetrics.RecordSuccess("http", "GluttonPing", userClass, clientLatency, int64(len(respBody)))
}

// logSampledTrace emits a single structured line per sampled span. Operators
// (and runner.py) grep stdout for `trace_id=` to find the trace IDs to look
// up in the OTLP backend. Matches the format of the Python locust workers'
// equivalent log line so a single regex covers both sources.
func logSampledTrace(span trace.Span, name string, latency time.Duration, source string, err error) {
	sc := span.SpanContext()
	if !sc.IsSampled() {
		return
	}
	attrs := []any{
		slog.String("name", name),
		slog.String("trace_id", sc.TraceID().String()),
		slog.Float64("duration_ms", msFloat(latency)),
		slog.String("source", source),
	}
	if err != nil {
		attrs = append(attrs, slog.String("err", err.Error()))
		slog.Info("traced span (failed)", attrs...)
		return
	}
	slog.Info("traced span", attrs...)
}

func elapsedFromMD(tr metadata.MD, key string, fallback time.Duration) (time.Duration, string) {
	vals := tr.Get(key)
	if len(vals) == 0 {
		return fallback, sourceClient
	}
	us, err := strconv.ParseInt(vals[0], 10, 64)
	if err != nil {
		return fallback, sourceClient
	}
	return time.Duration(us) * time.Microsecond, sourceServer
}

func msFloat(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1e6 }

// goroutineID extracts the runtime's per-goroutine ID via the standard
// runtime.Stack trick. Used to key per-VU state because boomer's Task model
// has no built-in per-VU hook — see the runtime.shutdown comment for the
// limitation this implies on user-count rescale.
func goroutineID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	line := string(buf[:n])
	const prefix = "goroutine "
	if !strings.HasPrefix(line, prefix) {
		return 0
	}
	end := strings.IndexByte(line[len(prefix):], ' ')
	if end < 0 {
		return 0
	}
	id, _ := strconv.ParseInt(line[len(prefix):len(prefix)+end], 10, 64)
	return id
}
