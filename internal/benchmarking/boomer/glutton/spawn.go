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
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.ate.dev/substrate/internal/ateinterceptors"
	"go.ate.dev/substrate/internal/atenet"
	"go.ate.dev/substrate/internal/benchmarking/boomer/boomerutil"
	bmetrics "go.ate.dev/substrate/internal/benchmarking/boomer/metrics"
	"go.ate.dev/substrate/internal/benchmarking/boomer/userclass"
	gluttonpb "go.ate.dev/substrate/internal/proto/glutton"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
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
	spawnUserClass = "SpawnUser"

	spawnInitialRetryBackoff = 50 * time.Millisecond
	spawnMaxRetryBackoff     = 1 * time.Second
	spawnRetryBackoffJitter  = 5 * time.Millisecond
)

var deciles = [...]int{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}

// firedDeciles returns the deciles (10..100) whose readiness threshold equals rank for a batch of totalActors actors.
func firedDeciles(rank, totalActors int) []int {
	if totalActors <= 0 || rank <= 0 {
		return nil
	}
	var fired []int
	for _, k := range deciles {
		threshold := (k*totalActors + 99) / 100
		if threshold == rank {
			fired = append(fired, k)
		}
	}
	return fired
}

func init() {
	userclass.Add(userclass.Entry{
		Name:       "spawn",
		LocustFile: "spawn.py",
		UserClass:  spawnUserClass,
		Init:       initSpawn,
	})
}

func initSpawn(cfg *userclass.Config) (taskFn func(), shutdown func(context.Context)) {
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer("substrate-boomer/glutton-spawn")
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	rt := &spawnRuntime{
		cfg:       cfg,
		runCtx:    runCtx,
		cancelRun: cancelRun,
	}
	return rt.iterate, rt.shutdown
}

// spawnRuntime creates one batch of actors per process and records how long
// each takes to answer its first ping, and how long the whole batch takes.
type spawnRuntime struct {
	cfg       *userclass.Config
	runOnce   sync.Once
	runCtx    context.Context
	cancelRun context.CancelFunc
	wg        sync.WaitGroup

	readyCount       atomic.Int64
	lastReadyElapsed atomic.Int64 // nanoseconds since batch start

	actorMu    sync.Mutex
	actorNames []string
}

func (r *spawnRuntime) recordActorName(name string) {
	r.actorMu.Lock()
	defer r.actorMu.Unlock()
	r.actorNames = append(r.actorNames, name)
}

func (r *spawnRuntime) getActorNames() []string {
	r.actorMu.Lock()
	defer r.actorMu.Unlock()
	names := make([]string, len(r.actorNames))
	copy(names, r.actorNames)
	return names
}

func (r *spawnRuntime) recordSuccess(category, name string, latency time.Duration, respBytes int64) {
	if r.runCtx != nil && r.runCtx.Err() != nil {
		return
	}
	bmetrics.RecordSuccess(category, name, spawnUserClass, latency, respBytes)
}

func (r *spawnRuntime) recordFailure(category, name string, latency time.Duration, errMsg string) {
	if r.runCtx != nil && r.runCtx.Err() != nil {
		return
	}
	bmetrics.RecordFailure(category, name, spawnUserClass, latency, errMsg)
}

func (r *spawnRuntime) iterate() {
	r.runOnce.Do(func() {
		r.runBatch(r.runCtx)
	})
	// Never returns: boomer calls Fn in a loop, and returning would spin on
	// the spent runOnce. boomer's stats ticker keeps the runtime deadlock
	// detector quiet while every user goroutine blocks here.
	select {}
}

func (r *spawnRuntime) runBatch(ctx context.Context) {
	bmetrics.UpdateUsers(spawnUserClass, 1)

	if err := r.ensureAtespace(ctx); err != nil {
		slog.Error("failed to ensure atespace", slog.String("err", err.Error()))
		r.recordFailure("summary", "TimeToAllReady", 0, err.Error())
		return
	}

	batchStart := time.Now()
	totalActors := r.cfg.TotalActors
	spawnConcurrency := r.cfg.SpawnConcurrency
	deadline := r.cfg.ActorDeadline
	// Values set in the web UI form (dynconfig) override the flags.
	if r.cfg.Dyn != nil {
		dyn := r.cfg.Dyn.Load()
		if dyn.TotalActors > 0 {
			totalActors = dyn.TotalActors
		}
		if dyn.SpawnConcurrency > 0 {
			spawnConcurrency = dyn.SpawnConcurrency
		}
		if dyn.ActorDeadline > 0 {
			deadline = dyn.ActorDeadline
		}
	}
	spawnConcurrency = min(spawnConcurrency, totalActors)
	if spawnConcurrency < 1 {
		spawnConcurrency = 1
	}

	runID := uuid.NewString()[:8]
	var nextIdx atomic.Int64

	r.wg.Add(spawnConcurrency)
	for i := 0; i < spawnConcurrency; i++ {
		go func() {
			defer r.wg.Done()
			for {
				idx := int(nextIdx.Add(1))
				if idx > totalActors || ctx.Err() != nil {
					return
				}
				actorName := fmt.Sprintf("spawn-%s-%d", runID, idx)
				r.runOneActor(ctx, batchStart, actorName, totalActors, deadline)
			}
		}()
	}

	r.wg.Wait()

	if r.runCtx != nil && r.runCtx.Err() != nil {
		return
	}

	if r.readyCount.Load() > 0 {
		r.recordSuccess("summary", "TimeToAllReady", time.Duration(r.lastReadyElapsed.Load()), 0)
	} else {
		r.recordFailure("summary", "TimeToAllReady", time.Since(batchStart), "no actors became ready")
	}
}

func (r *spawnRuntime) runOneActor(ctx context.Context, batchStart time.Time, actorName string, totalActors int, deadline time.Duration) {
	r.recordActorName(actorName)

	actorCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	actorStart := time.Now()

	if err := r.createActorWithRetry(actorCtx, actorName); err != nil {
		r.recordFailure("summary", "ActorTimeToReady", time.Since(actorStart), err.Error())
		return
	}

	if err := r.resumeActorWithRetry(actorCtx, actorName); err != nil {
		r.recordFailure("summary", "ActorTimeToReady", time.Since(actorStart), err.Error())
		return
	}

	if err := r.pingUntilReady(actorCtx, actorName); err != nil {
		r.recordFailure("summary", "ActorTimeToReady", time.Since(actorStart), err.Error())
		return
	}

	readyElapsed := time.Since(batchStart)
	actorElapsed := time.Since(actorStart)

	// Max, not last write: goroutines finish out of order.
	for {
		cur := r.lastReadyElapsed.Load()
		if int64(readyElapsed) <= cur {
			break
		}
		if r.lastReadyElapsed.CompareAndSwap(cur, int64(readyElapsed)) {
			break
		}
	}

	rank := int(r.readyCount.Add(1))
	for _, k := range firedDeciles(rank, totalActors) {
		r.recordSuccess("summary", fmt.Sprintf("TimeToReady_%dpct", k), readyElapsed, 0)
	}

	r.recordSuccess("summary", "ActorTimeToReady", actorElapsed, 0)
}

func (r *spawnRuntime) createActorWithRetry(ctx context.Context, actorName string) error {
	delay := spawnInitialRetryBackoff
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := r.tracedCall(ctx, "CreateActor", func(callCtx context.Context, tr *metadata.MD) error {
			_, callErr := r.cfg.APIStub.CreateActor(callCtx, &ateapipb.CreateActorRequest{
				Actor: &ateapipb.Actor{
					Metadata:      &ateapipb.ResourceMetadata{Atespace: r.cfg.Atespace, Name: actorName},
					ActorTemplate: &ateapipb.ObjectRef{Atespace: templateAtespace, Name: templateName},
				},
			}, grpc.Trailer(tr))
			return callErr
		})

		if err == nil {
			return nil
		}

		// A create retry can receive AlreadyExists if an earlier attempt landed on the server
		// before failing with e.g. Unavailable on the network. Since names carry a random
		// per-run prefix, AlreadyExists on a retry means our own attempt succeeded.
		if attempt > 0 {
			if s, ok := status.FromError(err); ok && s.Code() == codes.AlreadyExists {
				return nil
			}
		}

		if isSpawnTerminalError(err) {
			return err
		}

		jitter := time.Duration(rand.Float64() * float64(spawnRetryBackoffJitter))
		select {
		case <-time.After(delay + jitter):
		case <-ctx.Done():
			return ctx.Err()
		}
		delay = min(delay*2, spawnMaxRetryBackoff)
	}
}

func (r *spawnRuntime) resumeActorWithRetry(ctx context.Context, actorName string) error {
	actorRef := &ateapipb.ObjectRef{Atespace: r.cfg.Atespace, Name: actorName}
	delay := spawnInitialRetryBackoff

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := r.tracedCall(ctx, "ResumeActor", func(callCtx context.Context, tr *metadata.MD) error {
			return boomerutil.RetryOnConflict(callCtx, func() error {
				_, err := r.cfg.APIStub.ResumeActor(callCtx, &ateapipb.ResumeActorRequest{
					Actor: actorRef,
				}, grpc.Trailer(tr))
				return err
			})
		})

		if err == nil {
			return nil
		}

		if isSpawnCrashed(err) {
			r.recordFailure("actor", "CrashCount", 0, "actor entered ACTOR_STATE_CRASHED")
			return err
		}

		if isSpawnTerminalError(err) {
			return err
		}

		jitter := time.Duration(rand.Float64() * float64(spawnRetryBackoffJitter))
		select {
		case <-time.After(delay + jitter):
		case <-ctx.Done():
			return ctx.Err()
		}
		delay = min(delay*2, spawnMaxRetryBackoff)
	}
}

func (r *spawnRuntime) pingUntilReady(ctx context.Context, actorName string) error {
	message := uuid.NewString()
	body, err := proto.Marshal(&gluttonpb.PingRequest{Message: message})
	if err != nil {
		r.recordFailure("http", "GluttonPing", 0, err.Error())
		return err
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := r.doOnePing(ctx, actorName, body)
		if err == nil {
			return nil
		}

		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (r *spawnRuntime) doOnePing(ctx context.Context, actorName string, body []byte) error {
	ctx, span := r.cfg.Tracer.Start(ctx, "GluttonPing")
	defer span.End()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, r.cfg.RouterURL+pingPath, bytes.NewReader(body))
	if err != nil {
		r.recordFailure("http", "GluttonPing", 0, err.Error())
		return err
	}
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set(atenet.TargetActorHeader, r.cfg.Atespace+"/"+actorName)
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(httpReq.Header))

	start := time.Now()
	resp, err := r.cfg.HTTPClient.Do(httpReq)
	latency := time.Since(start)
	if err != nil {
		boomerutil.LogSampledTrace(span, "GluttonPing", latency, boomerutil.SourceClient, err)
		r.recordFailure("http", "GluttonPing", latency, err.Error())
		return err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		boomerutil.LogSampledTrace(span, "GluttonPing", latency, boomerutil.SourceClient, err)
		r.recordFailure("http", "GluttonPing", latency, err.Error())
		return err
	}
	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
		boomerutil.LogSampledTrace(span, "GluttonPing", latency, boomerutil.SourceClient, err)
		r.recordFailure("http", "GluttonPing", latency, err.Error())
		return err
	}
	var pingResp gluttonpb.PingResponse
	if err := proto.Unmarshal(respBody, &pingResp); err != nil {
		boomerutil.LogSampledTrace(span, "GluttonPing", latency, boomerutil.SourceClient, err)
		r.recordFailure("http", "GluttonPing", latency, err.Error())
		return err
	}
	boomerutil.LogSampledTrace(span, "GluttonPing", latency, boomerutil.SourceClient, nil)
	r.recordSuccess("http", "GluttonPing", latency, int64(len(respBody)))
	return nil
}

func (r *spawnRuntime) ensureAtespace(ctx context.Context) error {
	return r.tracedCall(ctx, "CreateAtespace", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := r.cfg.APIStub.CreateAtespace(callCtx, &ateapipb.CreateAtespaceRequest{
			Atespace: &ateapipb.Atespace{
				Metadata: &ateapipb.ResourceMetadata{
					Name: r.cfg.Atespace,
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

func (r *spawnRuntime) shutdown(shutdownCtx context.Context) {
	r.cancelRun()
	waitCtx, cancel := context.WithTimeout(shutdownCtx, 2*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-waitCtx.Done():
	}
	r.deleteAll(shutdownCtx)
	bmetrics.UpdateUsers(spawnUserClass, -1)
}

func (r *spawnRuntime) deleteAll(ctx context.Context) {
	names := r.getActorNames()
	if len(names) == 0 {
		return
	}
	const maxConcurrency = 32
	concurrency := min(maxConcurrency, len(names))
	ch := make(chan string, len(names))
	for _, name := range names {
		ch <- name
	}
	close(ch)

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for name := range ch {
				if ctx.Err() != nil {
					return
				}
				_ = r.deleteActor(ctx, name)
			}
		}()
	}
	wg.Wait()
}

func (r *spawnRuntime) deleteActor(ctx context.Context, name string) error {
	return r.tracedCall(ctx, "DeleteActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := r.cfg.APIStub.DeleteActor(callCtx, &ateapipb.DeleteActorRequest{
			Actor: &ateapipb.ObjectRef{
				Atespace: r.cfg.Atespace,
				Name:     name,
			},
			AnyState: true,
		}, grpc.Trailer(tr))
		return err
	})
}

func (r *spawnRuntime) tracedCall(ctx context.Context, name string, do func(context.Context, *metadata.MD) error) error {
	ctx, span := r.cfg.Tracer.Start(ctx, name)
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
		r.recordFailure("grpc", name, latency, err.Error())
		return err
	}
	r.recordSuccess("grpc", name, latency, 0)
	return nil
}

// isSpawnTerminalError reports whether retrying err cannot help. Unlisted
// codes, ResourceExhausted included, are retried until the actor deadline;
// Aborted is terminal only for a crashed actor.
func isSpawnTerminalError(err error) bool {
	if err == nil {
		return false
	}
	s, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch s.Code() {
	case codes.NotFound,
		codes.DataLoss,
		codes.AlreadyExists,
		codes.FailedPrecondition,
		codes.InvalidArgument,
		codes.PermissionDenied,
		codes.Unauthenticated,
		codes.Unimplemented:
		return true
	case codes.Aborted:
		return strings.Contains(strings.ToLower(s.Message()), "crashed")
	default:
		return false
	}
}

func isSpawnCrashed(err error) bool {
	s, ok := status.FromError(err)
	return ok && s.Code() == codes.Aborted && strings.Contains(strings.ToLower(s.Message()), "crashed")
}
