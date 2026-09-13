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
	"github.com/agent-substrate/substrate/internal/atenet"
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
	pingPath         = "/ping"
	writeRAMPath     = "/writeram"
	readRAMPath      = "/readram"
	memLoadKey       = "memload"
	memReadAll       = "all"

	// ateapi returns Aborted with this message when two callers race an
	// actor update (see cmd/ateapi/internal/controlapi/workflow_resume.go).
	// It's transient — the loser retries and one of them wins.
	concurrentUpdateMsg = "concurrent update conflict, please retry"
	// Retry budget for ResumeActor concurrent-update conflicts. First retry
	// is immediate (no initial backoff — conflicts often clear the instant
	// the racing writer commits); subsequent gaps are pinned at 50ms.
	resumeMaxAttempts = 5
	resumeMaxBackoff  = 50 * time.Millisecond

	// Per-wake ping loop: pings after the first are spaced by a random gap
	// in [minPingGap, maxPingGap). The loop stops early once the live
	// window (liveWait) elapses so we still suspend on schedule.
	minPingGap = 200 * time.Millisecond
	maxPingGap = 1 * time.Second
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
// On first call from a given goroutine we lazily create the user's actors
// (the analog of locust's per-user on_start); subsequent calls advance the
// VU's round-robin cursor by one and run a resume/ping/suspend cycle on
// that actor.
func (r *taskRuntime) iterate() {
	// Every return path sleeps the wait window (--min/--max-wait-time). On
	// the happy path that is the gap between suspend and the VU's next
	// resume, as in the legacy Python test; on the error paths it keeps a
	// failing startUser / resume / crashed actor from looping on boomer's
	// zero-delay re-entry and hammering ate-api-server.
	defer func() {
		time.Sleep(r.dynamicWait())
	}()

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

	actor := user.nextActor()
	if actor == nil {
		return
	}

	// A crashed actor stays crashed for the rest of the run: further
	// Resume calls will keep returning Aborted and would just churn API
	// traffic. The rotation still advances, so the other actors in the VU
	// keep making progress.
	if actor.crashed {
		return
	}

	ctx := context.Background()
	if !actor.resume(ctx) {
		return
	}
	// Fill before the first suspend so every snapshot from cycle one on
	// carries the full working set; glutton keeps the allocations across
	// suspend/resume, so this runs once per actor (retried if it fails).
	actor.ensureRAMFilled(ctx)
	// Walk the working set right after resume, before churn dirties it:
	// under a demand-paged restore every touched page must be paged back
	// in before the walk returns, so its latency measures the true cost
	// of reaching the previous snapshot's memory.
	actor.readRAM(ctx)
	// Re-dirty part of the working set each cycle so repeated suspends
	// snapshot an actor whose memory is changing, like a live application's.
	// Rotate mode advances through the array cycle over cycle, so the dirty
	// window moves instead of re-dirtying the same prefix.
	actor.churnRAM(ctx)
	// Live window (--min/--max-live-time): ping, then hold the actor
	// running until the window closes. The first ping runs immediately,
	// then up to maxPings-1 more, each preceded by a random gap in
	// [minPingGap, maxPingGap). The loop stops when either the ping cap or
	// the deadline is reached; any leftover time is slept below so the
	// actor stays live for the full window. With the default zero window
	// the actor is suspended right after the first ping.
	deadline := time.Now().Add(r.liveWait())
	maxPings := max(r.cfg.Dyn.Load().MaxPingsPerWake, 1)
	actor.ping(ctx)
	for sent := 1; sent < maxPings; sent++ {
		gap := minPingGap + time.Duration(rand.Float64()*float64(maxPingGap-minPingGap))
		if time.Now().Add(gap).After(deadline) {
			break
		}
		time.Sleep(gap)
		actor.ping(ctx)
	}
	if remaining := time.Until(deadline); remaining > 0 {
		time.Sleep(remaining)
	}
	actor.suspend(ctx)
}

func (r *taskRuntime) startUser(ctx context.Context) (*gluttonUser, error) {
	n := r.cfg.ActorsPerUser
	if n < 1 {
		n = 1
	}
	u := &gluttonUser{actors: make([]*gluttonActor, 0, n)}
	bmetrics.UpdateUsers(userClass, 1)
	var lastCreateErr error
	for i := 0; i < n; i++ {
		a := &gluttonActor{
			cfg:         r.cfg,
			actorName:   "sb-" + uuid.NewString(),
			firstResume: true,
		}
		// Ensuring the atespace is idempotent (swallows AlreadyExists), so
		// doing it once per VU is enough — subsequent actors would just make
		// the same round-trip return AlreadyExists.
		if i == 0 {
			if err := a.ensureAtespace(ctx); err != nil {
				bmetrics.UpdateUsers(userClass, -1)
				return nil, err
			}
		}
		if err := a.create(ctx); err != nil {
			lastCreateErr = err
			slog.Warn("glutton create failed partway; using actors created so far",
				slog.String("atespace", r.cfg.Atespace),
				slog.Int("wanted", n),
				slog.Int("got", len(u.actors)),
				slog.String("err", err.Error()))
			break
		}
		u.actors = append(u.actors, a)
	}
	if len(u.actors) == 0 {
		bmetrics.UpdateUsers(userClass, -1)
		return nil, fmt.Errorf("no actors created: %w", lastCreateErr)
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
		for _, a := range u.actors {
			if a.actorRunning {
				a.suspend(ctx)
			}
			a.delete(ctx)
		}
		bmetrics.UpdateUsers(userClass, -1)
		return true
	})
}

// dynamicWait is the gap between suspending one actor and resuming the
// VU's next one, drawn uniformly from [MinWait, MaxWait].
func (r *taskRuntime) dynamicWait() time.Duration {
	cfg := r.cfg.Dyn.Load()
	return uniformWait(cfg.MinWait, cfg.MaxWait)
}

// liveWait is how long an actor stays resumed between its first ping and
// its suspend, drawn uniformly from [MinLive, MaxLive]. The default zero
// window suspends right after the ping.
func (r *taskRuntime) liveWait() time.Duration {
	cfg := r.cfg.Dyn.Load()
	return uniformWait(cfg.MinLive, cfg.MaxLive)
}

// uniformWait draws from [lo, hi]; an inverted or empty range yields lo.
func uniformWait(lo, hi time.Duration) time.Duration {
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(rand.Float64()*float64(hi-lo))
}

// gluttonUser is one VU (boomer goroutine). It owns --actors-per-user actors
// and hands them out round-robin, one per iterate() call.
type gluttonUser struct {
	actors  []*gluttonActor
	nextIdx int
}

// nextActor returns the current actor and advances the round-robin cursor.
// Returns nil only if the VU started with zero actors (startUser guarantees
// at least one on success).
func (u *gluttonUser) nextActor() *gluttonActor {
	if len(u.actors) == 0 {
		return nil
	}
	a := u.actors[u.nextIdx]
	u.nextIdx = (u.nextIdx + 1) % len(u.actors)
	return a
}

// gluttonActor is one actor's lifetime state within a VU. Every per-iteration
// call in iterate() targets exactly one of these.
type gluttonActor struct {
	cfg          *userclass.Config
	actorName    string
	firstResume  bool
	actorRunning bool
	ramFilled    bool
	// crashed is set the first time ResumeActor reports the actor as
	// ACTOR_STATE_CRASHED (codes.Aborted with "crashed" in the message).
	// Once set, iterate() skips this actor forever — ateapi never
	// rehabilitates a crashed actor, so retrying would just fail forever.
	// The VU's other actors are unaffected.
	crashed bool
}

func (u *gluttonActor) ref() *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Atespace: u.cfg.Atespace, Name: u.actorName}
}

// ensureAtespace creates the configured atespace, swallowing AlreadyExists
// so concurrent VUs racing the first creation all see it as a success. The
// call goes through tracedCall so it shows up in stats/spans like every
// other API call.
func (u *gluttonActor) ensureAtespace(ctx context.Context) error {
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

func (u *gluttonActor) create(ctx context.Context) error {
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

func (u *gluttonActor) resume(ctx context.Context) bool {
	metricName := "ResumeActor"
	if u.firstResume {
		metricName = "ResumeActorFirstResume"
	}
	err := u.tracedCall(ctx, metricName, func(callCtx context.Context, tr *metadata.MD) error {
		// Retry Aborted "concurrent update conflict" transparently — it's a
		// race, not a real failure, and the caller is expected to retry per
		// the ateapi contract. Kept inside the tracedCall closure so the
		// reported latency spans every attempt and the span carries the
		// last attempt's server trailer, same as any other single-shot RPC.
		var backoff time.Duration // 0 → first retry runs immediately
		var lastErr error
		for range resumeMaxAttempts {
			_, lastErr = u.cfg.APIStub.ResumeActor(callCtx, &ateapipb.ResumeActorRequest{
				Actor: u.ref(),
			}, grpc.Trailer(tr))
			if lastErr == nil {
				return nil
			}
			if !isConcurrentUpdateConflict(lastErr) {
				return lastErr
			}
			if backoff > 0 {
				select {
				case <-time.After(backoff):
				case <-callCtx.Done():
					return callCtx.Err()
				}
			}
			backoff = resumeMaxBackoff
		}
		return lastErr
	})
	if err != nil {
		// ateapi reports a crashed actor as codes.Aborted with "crashed" in
		// the message (see workflow_resume.go). Mark the user so iterate()
		// stops touching it, and surface a CrashCount tick so operators can
		// see the crash total in the locust stats table.
		if s, ok := status.FromError(err); ok && s.Code() == codes.Aborted && strings.Contains(s.Message(), "crashed") {
			u.crashed = true
			bmetrics.RecordFailure("actor", "CrashCount", userClass, 0, "actor entered ACTOR_STATE_CRASHED")
			slog.Warn("glutton actor crashed; will stop sending requests",
				slog.String("actor", u.actorName),
				slog.String("err", err.Error()))
		}
		return false
	}
	u.firstResume = false
	u.actorRunning = true
	return true
}

// isConcurrentUpdateConflict identifies the transient racy-update error
// ateapi's workflow_*.go returns as codes.Aborted with the retry-me message.
// Kept distinct from the "crashed" Aborted check in resume() because the two
// look the same at the code level and mean opposite things.
func isConcurrentUpdateConflict(err error) bool {
	s, ok := status.FromError(err)
	return ok && s.Code() == codes.Aborted && strings.Contains(s.Message(), concurrentUpdateMsg)
}

func (u *gluttonActor) suspend(ctx context.Context) {
	_ = u.tracedCall(ctx, "SuspendActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.SuspendActor(callCtx, &ateapipb.SuspendActorRequest{
			Actor: u.ref(),
		}, grpc.Trailer(tr))
		return err
	})
	u.actorRunning = false
}

func (u *gluttonActor) delete(ctx context.Context) {
	_ = u.tracedCall(ctx, "DeleteActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.DeleteActor(callCtx, &ateapipb.DeleteActorRequest{
			Actor: u.ref(),
		}, grpc.Trailer(tr))
		return err
	})
}

// tracedCall wraps a unary gRPC call with a span and Prometheus/locust
// reporting. The reported latency is client wall clock, so it covers
// retries inside do, queueing, and the network. The server-side elapsed
// time from ateinterceptors.ServerUnaryInterceptor's trailer, when present,
// goes on the span only: it measures the last attempt alone.
func (u *gluttonActor) tracedCall(ctx context.Context, name string, do func(context.Context, *metadata.MD) error) error {
	ctx, span := u.cfg.Tracer.Start(ctx, name)
	defer span.End()

	start := time.Now()
	var tr metadata.MD
	err := do(ctx, &tr)
	latency := time.Since(start)

	if serverLatency, source := boomerutil.ElapsedFromMD(tr, ateinterceptors.ServerElapsedTrailer, 0); source == boomerutil.SourceServer {
		span.SetAttributes(attribute.Float64("server.elapsed_ms", boomerutil.MsFloat(serverLatency)))
	}
	boomerutil.LogSampledTrace(span, name, latency, boomerutil.SourceClient, err)
	if err != nil {
		bmetrics.RecordFailure("grpc", name, userClass, latency, err.Error())
		return err
	}
	bmetrics.RecordSuccess("grpc", name, userClass, latency, 0)
	return nil
}

func (u *gluttonActor) ping(ctx context.Context) {
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
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set(atenet.TargetActorHeader, u.cfg.Atespace+"/"+u.actorName)
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
func (u *gluttonActor) ensureRAMFilled(ctx context.Context) {
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
func (u *gluttonActor) churnRAM(ctx context.Context) {
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
func (u *gluttonActor) readRAM(ctx context.Context) {
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
func (u *gluttonActor) writeRAM(ctx context.Context, key, size string, mode gluttonpb.WriteMode) error {
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
func (u *gluttonActor) postProto(ctx context.Context, path string, req, resp proto.Message) error {
	body, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.cfg.RouterURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set(atenet.TargetActorHeader, u.cfg.Atespace+"/"+u.actorName)
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
