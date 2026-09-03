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
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/boomerutil"
	bmetrics "github.com/agent-substrate/substrate/internal/benchmarking/boomer/metrics"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	userClass        = "GluttonUser"
	templateName     = "glutton"
	templateAtespace = "benchmark-workloads"
	actorDomain      = "actors.resources.substrate.ate.dev"
	pingPath         = "/ping"
	writeRAMPath     = "/writeram"
	readRAMPath      = "/readram"
	memLoadKey       = "memload"
	memReadAll       = "all"
)

func init() {
	userclass.Add(userclass.Entry{
		Name:       "glutton",
		LocustFile: "glutton.py",
		UserClass:  userClass,
		Init:       initPing,
	})
}

// initPing creates a runtime tied to cfg and returns a boomer-compatible task
// function plus a Shutdown hook the caller should run before exit (it
// suspend+deletes every actor this worker created).
func initPing(cfg *userclass.Config) (taskFn func(), shutdown func(context.Context)) {
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer("substrate-boomer/glutton")
	}
	rt := &taskRuntime{cfg: cfg}
	return rt.iterate, rt.shutdown
}

type taskRuntime struct {
	cfg   *userclass.Config
	users sync.Map // goroutineID → *gluttonUser
}

// iterate is the task function boomer calls in a loop on each VU goroutine.
// On first call from a given goroutine we lazily create the user's actor
// (the analog of locust's per-user on_start); subsequent calls run a
// resume/ping/suspend cycle.
func (r *taskRuntime) iterate() {
	gid := boomerutil.GoroutineID()
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
	// Fill before the first suspend so every snapshot from cycle one on
	// carries the full working set; glutton keeps the allocations across
	// suspend/resume, so this runs once per actor (retried if it fails).
	user.ensureRAMFilled(ctx)
	// Walk the working set right after resume, before churn dirties it:
	// under a demand-paged restore every touched page must be paged back
	// in before the walk returns, so its latency measures the true cost
	// of reaching the previous snapshot's memory.
	user.readRAM(ctx)
	// Re-dirty part of the working set each cycle so repeated suspends
	// snapshot an actor whose memory is changing, like a live application's.
	// Rotate mode advances through the array cycle over cycle, so the dirty
	// window moves instead of re-dirtying the same prefix.
	user.churnRAM(ctx)
	user.ping(ctx)
	user.suspend(ctx)

	time.Sleep(r.dynamicWait())
}

func (r *taskRuntime) startUser(ctx context.Context) (*gluttonUser, error) {
	u := &gluttonUser{
		cfg:         r.cfg,
		actorName:   "sb-" + uuid.NewString(),
		firstResume: true,
	}
	u.hostHeader = u.actorName + "." + u.cfg.Atespace + "." + actorDomain
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
	cfg          *userclass.Config
	actorName    string
	hostHeader   string
	firstResume  bool
	actorRunning bool
	ramFilled    bool
}

func (u *gluttonUser) ref() *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Atespace: u.cfg.Atespace, Name: u.actorName}
}

// ensureAtespace creates the configured atespace, swallowing AlreadyExists
// so concurrent VUs racing the first creation all see it as a success. The
// call goes through tracedCall so it shows up in stats/spans like every
// other API call.
func (u *gluttonUser) ensureAtespace(ctx context.Context) error {
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

func (u *gluttonUser) create(ctx context.Context) error {
	return u.tracedCall(ctx, "CreateActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.CreateActor(callCtx, &ateapipb.CreateActorRequest{
			Actor: &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: u.cfg.Atespace, Name: u.actorName},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: templateAtespace, Name: templateName},
			},
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
			Actor: u.ref(),
			Boot:  u.firstResume,
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
			Actor: u.ref(),
		}, grpc.Trailer(tr))
		return err
	})
	u.actorRunning = false
}

func (u *gluttonUser) delete(ctx context.Context) {
	_ = u.tracedCall(ctx, "DeleteActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.DeleteActor(callCtx, &ateapipb.DeleteActorRequest{
			Actor: u.ref(),
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

	latency, source := boomerutil.ElapsedFromMD(tr, ateinterceptors.ServerElapsedTrailer, clientLatency)
	if source == boomerutil.SourceServer {
		span.SetAttributes(attribute.Float64("server.elapsed_ms", boomerutil.MsFloat(latency)))
	}
	boomerutil.LogSampledTrace(span, name, latency, source, err)
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
		boomerutil.LogSampledTrace(span, "GluttonPing", clientLatency, boomerutil.SourceClient, httpErr)
		bmetrics.RecordFailure("http", "GluttonPing", userClass, clientLatency, httpErr.Error())
		return
	}

	pong := &gluttonpb.PingResponse{}
	if err := proto.Unmarshal(respBody, pong); err != nil {
		boomerutil.LogSampledTrace(span, "GluttonPing", clientLatency, boomerutil.SourceClient, err)
		bmetrics.RecordFailure("http", "GluttonPing", userClass, clientLatency, err.Error())
		return
	}
	if pong.Message != message {
		mismatch := fmt.Errorf("ping echo mismatch: sent=%q recv=%q", message, pong.Message)
		boomerutil.LogSampledTrace(span, "GluttonPing", clientLatency, boomerutil.SourceClient, mismatch)
		bmetrics.RecordFailure("http", "GluttonPing", userClass, clientLatency, mismatch.Error())
		return
	}
	boomerutil.LogSampledTrace(span, "GluttonPing", clientLatency, boomerutil.SourceClient, nil)
	bmetrics.RecordSuccess("http", "GluttonPing", userClass, clientLatency, int64(len(respBody)))
}

// ensureRAMFilled grows the actor's resident working set to the configured
// mem_target through the glutton WriteRAM API. Runs once per actor:
// glutton holds the allocation for its lifetime, so it persists across
// suspend/resume and every snapshot from the first suspend onward is at
// size. A failure leaves ramFilled unset so the next iteration retries.
// The fill reports as its own GluttonFillRAM stats row so it never
// pollutes ping or resume numbers.
func (u *gluttonUser) ensureRAMFilled(ctx context.Context) {
	if u.ramFilled {
		return
	}
	target := u.cfg.Dyn.Load().MemTarget
	if target == "" {
		u.ramFilled = true
		return
	}

	ctx, span := u.cfg.Tracer.Start(ctx, "GluttonFillRAM")
	defer span.End()
	start := time.Now()

	err := u.writeRAM(ctx, memLoadKey, target, gluttonpb.WriteMode_WRITE_MODE_TRUNCATE)
	clientLatency := time.Since(start)
	boomerutil.LogSampledTrace(span, "GluttonFillRAM", clientLatency, boomerutil.SourceClient, err)
	if err != nil {
		bmetrics.RecordFailure("http", "GluttonFillRAM", userClass, clientLatency, err.Error())
		return
	}
	u.ramFilled = true
	bmetrics.RecordSuccess("http", "GluttonFillRAM", userClass, clientLatency, 0)
}

// churnRAM re-randomizes mem_churn bytes of the working set in place
// (WriteRAM rotate on the fill's key), so pages arrive dirty at every
// suspend instead of only the first: a fill-once set is static, and any
// future incremental snapshotting would make cycles two onward
// unrepresentative of a live application. Rotate mode advances glutton's
// per-key cursor past each write, wrapping at the end, so consecutive
// cycles dirty a moving window rather than the same prefix. Runs once per
// iteration, only after the fill has succeeded, and reports as its own
// GluttonChurnRAM stats row.
func (u *gluttonUser) churnRAM(ctx context.Context) {
	churn := u.cfg.Dyn.Load().MemChurn
	if churn == "" || !u.ramFilled {
		return
	}

	ctx, span := u.cfg.Tracer.Start(ctx, "GluttonChurnRAM")
	defer span.End()
	start := time.Now()

	err := u.writeRAM(ctx, memLoadKey, churn, gluttonpb.WriteMode_WRITE_MODE_OVERWRITE_ROTATE)
	clientLatency := time.Since(start)
	boomerutil.LogSampledTrace(span, "GluttonChurnRAM", clientLatency, boomerutil.SourceClient, err)
	if err != nil {
		bmetrics.RecordFailure("http", "GluttonChurnRAM", userClass, clientLatency, err.Error())
		return
	}
	bmetrics.RecordSuccess("http", "GluttonChurnRAM", userClass, clientLatency, 0)
}

// readRAM walks mem_read bytes of the working set (memReadAll walks all of
// it) through the glutton ReadRAM API, one byte per page, and reports the
// walk as its own GluttonReadRAM stats row. Placed right after resume, the
// row's latency is the demand-paging cost of the previous snapshot's
// memory; on an eagerly-restored actor it degenerates to a fast in-memory
// scan, so the two restore modes are directly comparable.
func (u *gluttonUser) readRAM(ctx context.Context) {
	read := u.cfg.Dyn.Load().MemRead
	if read == "" || !u.ramFilled {
		return
	}
	size := read
	if read == memReadAll {
		size = "" // ReadRAM walks the whole array on empty size
	}

	ctx, span := u.cfg.Tracer.Start(ctx, "GluttonReadRAM")
	defer span.End()
	start := time.Now()

	resp := &gluttonpb.ReadRAMResponse{}
	err := u.postProto(ctx, readRAMPath, &gluttonpb.ReadRAMRequest{Key: memLoadKey, Size: size}, resp)
	clientLatency := time.Since(start)
	boomerutil.LogSampledTrace(span, "GluttonReadRAM", clientLatency, boomerutil.SourceClient, err)
	if err != nil {
		bmetrics.RecordFailure("http", "GluttonReadRAM", userClass, clientLatency, err.Error())
		return
	}
	bmetrics.RecordSuccess("http", "GluttonReadRAM", userClass, clientLatency, resp.GetSize())
}

// writeRAM POSTs one WriteRAM request to the actor through the router,
// mirroring ping's wire format (protobuf over HTTP). size is a suffixed
// string (e.g. "2Gi") passed through verbatim; glutton parses it.
func (u *gluttonUser) writeRAM(ctx context.Context, key, size string, mode gluttonpb.WriteMode) error {
	err := u.postProto(ctx, writeRAMPath, &gluttonpb.WriteRAMRequest{
		Key:       key,
		Size:      size,
		WriteMode: mode,
	}, &gluttonpb.WriteRAMResponse{})
	if err != nil {
		return fmt.Errorf("WriteRAM %s (%s): %w", key, size, err)
	}
	return nil
}

// postProto POSTs one protobuf request to the actor through the router and
// unmarshals the protobuf response into resp.
func (u *gluttonUser) postProto(ctx context.Context, path string, req, resp proto.Message) error {
	body, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.cfg.RouterURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Host = u.hostHeader
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
		return fmt.Errorf("%s: HTTP %d: %s", path, httpResp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return proto.Unmarshal(respBody, resp)
}
