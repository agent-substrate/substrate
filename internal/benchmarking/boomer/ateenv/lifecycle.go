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

// Package ateenv implements the boomer-Go user class for the ate-env benchmark
// workload (see benchmarking/locust/tests/ate_env.py for the locust harness stub).
package ateenv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/ateinterceptors"
	"go.ate.dev/substrate/internal/atenet"
	"go.ate.dev/substrate/internal/benchmarking/boomer/boomerutil"
	"go.ate.dev/substrate/internal/benchmarking/boomer/dynconfig"
	bmetrics "go.ate.dev/substrate/internal/benchmarking/boomer/metrics"
	"go.ate.dev/substrate/internal/benchmarking/boomer/userclass"
	ateenvv1alpha "go.ate.dev/substrate/internal/proto/ateenv/v1alpha"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	userClass        = "AteEnvUser"
	templateName     = "ate-env"
	templateAtespace = "benchmark-workloads"
	expectedStdout   = "hello world\n"
	execTimeout      = 30 * time.Second

	maxConsecutiveFailures = 3
	minPingGap             = 200 * time.Millisecond
	maxPingGap             = 1 * time.Second
)

type ateEnvKnobs struct {
	dynconfig.WaitTime
	dynconfig.Lifecycle
	MinLive         dynconfig.Seconds `json:"min_live_time"`
	MaxLive         dynconfig.Seconds `json:"max_live_time"`
	MaxPingsPerWake int               `json:"max_pings_per_wake"`
}

var ateEnvCodec = dynconfig.Typed[ateEnvKnobs]{
	Defaults: ateEnvKnobs{
		WaitTime:        dynconfig.WaitTime{MaxWait: dynconfig.Seconds(500 * time.Millisecond)},
		MaxPingsPerWake: 1,
	},
	Validate: func(k ateEnvKnobs) error {
		if err := k.WaitTime.Validate(); err != nil {
			return err
		}
		if err := k.Lifecycle.Validate(); err != nil {
			return err
		}
		if k.MinLive < 0 {
			return fmt.Errorf("min_live_time cannot be negative: %v", k.MinLive.Duration())
		}
		if k.MaxLive < 0 {
			return fmt.Errorf("max_live_time cannot be negative: %v", k.MaxLive.Duration())
		}
		if k.MaxLive < k.MinLive {
			return fmt.Errorf("max_live_time (%v) cannot be less than min_live_time (%v)", k.MaxLive.Duration(), k.MinLive.Duration())
		}
		return nil
	},
}

func init() {
	userclass.Add(userclass.Entry{
		Name:       "ate_env",
		LocustFile: "ate_env.py",
		UserClass:  userClass,
		Config:     ateEnvCodec,
		Init:       initAteEnv,
	})
}

// routerGRPCTarget converts an HTTP/HTTPS router URL into a dns:/// gRPC dial target.
func routerGRPCTarget(rawURL string) (string, error) {
	if rawURL == "" {
		rawURL = "http://atenet-router.ate-system.svc.cluster.local"
	}
	if !strings.Contains(rawURL, "://") {
		rawURL = "http://" + rawURL
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	if strings.EqualFold(u.Scheme, "https") {
		return "", fmt.Errorf("router URL %q uses https, which is not supported by the plaintext gRPC dialer", rawURL)
	}
	host := u.Hostname()
	if host == "" {
		return "", fmt.Errorf("router URL %q has empty host", rawURL)
	}
	port := u.Port()
	if port == "" {
		port = "80"
	}
	return "dns:///" + net.JoinHostPort(host, port), nil
}

// initAteEnv creates a runtime tied to cfg and returns a boomer-compatible task
// function plus a Shutdown hook the caller should run before exit (it
// suspend+deletes every actor this worker created and closes router connections).
func initAteEnv(cfg *userclass.Config) (taskFn func(), shutdown func(context.Context)) {
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer("substrate-boomer/ateenv")
	}
	rt := &taskRuntime{
		cfg:    cfg,
		stopCh: make(chan struct{}),
	}
	return rt.iterate, rt.shutdown
}

type taskRuntime struct {
	cfg               *userclass.Config
	users             sync.Map // goroutineID → *ateEnvUser
	dialProcessClient func() (ateenvv1alpha.ProcessServiceClient, io.Closer, error)
	sleepFn           func(time.Duration) bool
	stopMu            sync.RWMutex
	stopCh            chan struct{}
	stopping          bool
	iterWG            sync.WaitGroup
}

func (r *taskRuntime) stopChan() <-chan struct{} {
	r.stopMu.RLock()
	ch := r.stopCh
	r.stopMu.RUnlock()
	if ch != nil {
		return ch
	}
	r.stopMu.Lock()
	defer r.stopMu.Unlock()
	if r.stopCh == nil {
		r.stopCh = make(chan struct{})
		if r.stopping {
			close(r.stopCh)
		}
	}
	return r.stopCh
}

func (r *taskRuntime) isStopping() bool {
	r.stopMu.RLock()
	defer r.stopMu.RUnlock()
	return r.stopping
}

func (r *taskRuntime) tryStartIter() bool {
	r.stopMu.RLock()
	defer r.stopMu.RUnlock()
	if r.stopping {
		return false
	}
	r.iterWG.Add(1)
	return true
}

func (r *taskRuntime) markStopping() {
	r.stopMu.Lock()
	if !r.stopping {
		r.stopping = true
		if r.stopCh != nil {
			close(r.stopCh)
		}
	}
	r.stopMu.Unlock()
}

func (r *taskRuntime) sleep(d time.Duration) bool {
	if r.sleepFn != nil {
		return r.sleepFn(d)
	}
	if d <= 0 {
		return !r.isStopping()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-r.stopChan():
		return false
	}
}

// iterate is the task function boomer calls in a loop on each VU goroutine.
func (r *taskRuntime) iterate() {
	if !r.tryStartIter() {
		return
	}
	defer func() {
		r.iterWG.Done()
		r.sleep(r.dynamicWait())
	}()

	gid := boomerutil.GoroutineID()
	val, loaded := r.users.Load(gid)
	if !loaded {
		u, err := r.startUser(context.Background())
		if err != nil {
			slog.Warn("ate_env on_start failed; goroutine will retry next iter",
				slog.String("err", err.Error()))
			return
		}
		val, _ = r.users.LoadOrStore(gid, u)
	}
	user := val.(*ateEnvUser)

	actor := user.nextActor()
	if actor == nil {
		return
	}

	ctx := context.Background()

	// resume()'s failure path replaces a crashed actor on the spot, so
	// reaching here means the replacement's create failed last time. Retry
	// it, or the VU runs a slot short for the rest of the run.
	if actor.isCrashed() {
		user.replaceActor(ctx, actor)
		return
	}

	// Going straight back to resume on a stranded actor would turn every
	// retryable hibernate failure into a FailedPrecondition and cost the
	// actor anyway. SuspendActor is re-entrant in either mode, so finish
	// the hibernate and pick the cycle up next iteration.
	if actor.isHibernatePending() {
		if err := actor.hibernate(ctx); err != nil {
			if actor.noteFailure(err) {
				user.replaceActor(ctx, actor)
			}
		} else {
			actor.noteSuccess()
		}
		return
	}

	if !actor.isRunning() || dynconfig.Get[ateEnvKnobs](r.cfg.Dyn).LifecycleMode != dynconfig.LifecycleModeNone {
		if err := actor.resume(ctx); err != nil {
			if actor.noteFailure(err) {
				user.replaceActor(ctx, actor)
			}
			return
		}
	}

	// Live window (--min/--max-live-time): exec HelloWorld, then hold the actor
	// running until the window closes. At most one exec failure per wake is
	// counted toward replacement (after hibernate completes), so a multi-ping
	// wake cannot trigger replacement from a single transient blip.
	deadline := time.Now().Add(r.liveWait())
	maxPings := max(dynconfig.Get[ateEnvKnobs](r.cfg.Dyn).MaxPingsPerWake, 1)
	var execErr error
	for sent := 0; sent < maxPings; sent++ {
		if sent > 0 {
			gap := minPingGap + time.Duration(rand.Float64()*float64(maxPingGap-minPingGap))
			if time.Now().Add(gap).After(deadline) {
				break
			}
			if !r.sleep(gap) {
				break
			}
		}
		if err := actor.execHelloWorld(ctx); err != nil {
			execErr = err
		} else {
			execErr = nil
			actor.noteSuccess()
		}
	}
	if remaining := time.Until(deadline); remaining > 0 {
		r.sleep(remaining)
	}
	if err := actor.hibernate(ctx); err != nil {
		if actor.noteFailure(err) {
			user.replaceActor(ctx, actor)
		}
		return
	}
	if execErr != nil {
		if actor.noteExecFailure(execErr) {
			user.replaceActor(ctx, actor)
		}
		return
	}
	actor.noteSuccess()
}

func (r *taskRuntime) startUser(ctx context.Context) (*ateEnvUser, error) {
	n := r.cfg.ActorsPerUser
	if n < 1 {
		n = 1
	}

	dialFn := r.dialProcessClient
	if dialFn == nil {
		target, err := routerGRPCTarget(r.cfg.RouterURL)
		if err != nil {
			return nil, err
		}
		dialFn = func() (ateenvv1alpha.ProcessServiceClient, io.Closer, error) {
			conn, err := grpc.NewClient(
				target,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithDisableServiceConfig(),
			)
			if err != nil {
				return nil, nil, err
			}
			return ateenvv1alpha.NewProcessServiceClient(conn), conn, nil
		}
	}

	client, closer, err := dialFn()
	if err != nil {
		return nil, fmt.Errorf("dial router gRPC: %w", err)
	}

	u := &ateEnvUser{
		routerConn: closer,
		actors:     make([]*ateEnvActor, 0, n),
	}
	bmetrics.UpdateUsers(userClass, 1)

	var lastCreateErr error
	for i := 0; i < n; i++ {
		a := &ateEnvActor{
			cfg:           r.cfg,
			actorName:     "sb-" + uuid.NewString(),
			firstResume:   true,
			processClient: client,
		}
		if i == 0 {
			if err := a.ensureAtespace(ctx); err != nil {
				if closer != nil {
					_ = closer.Close()
				}
				bmetrics.UpdateUsers(userClass, -1)
				return nil, err
			}
		}
		if err := a.create(ctx); err != nil {
			lastCreateErr = err
			slog.Warn("ate_env create failed partway; using actors created so far",
				slog.String("atespace", r.cfg.Atespace),
				slog.Int("wanted", n),
				slog.Int("got", len(u.actors)),
				slog.String("err", err.Error()))
			break
		}
		u.actors = append(u.actors, a)
	}
	if len(u.actors) == 0 {
		if closer != nil {
			_ = closer.Close()
		}
		bmetrics.UpdateUsers(userClass, -1)
		return nil, fmt.Errorf("no actors created: %w", lastCreateErr)
	}
	return u, nil
}

// shutdown stops new iterations, waits for any in-flight iterate() calls to
// finish (capped at half of ctx's remaining deadline so cleanup still has time
// to run), hibernates (if still running) and deletes every actor this worker
// created, then closes each VU's gRPC channel.
func (r *taskRuntime) shutdown(ctx context.Context) {
	r.markStopping()
	waitCtx := ctx
	if deadline, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, max(time.Until(deadline)/2, 0))
		defer cancel()
	}
	done := make(chan struct{})
	go func() {
		r.iterWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-waitCtx.Done():
	}
	r.users.Range(func(_, val any) bool {
		u := val.(*ateEnvUser)
		for _, a := range u.snapshotActors() {
			if a.isRunning() {
				if dynconfig.Get[ateEnvKnobs](r.cfg.Dyn).LifecycleMode == dynconfig.LifecycleModeSync {
					_ = a.suspend(ctx)
				} else {
					_ = a.pause(ctx)
				}
			}
			a.delete(ctx)
		}
		if u.routerConn != nil {
			_ = u.routerConn.Close()
		}
		bmetrics.UpdateUsers(userClass, -1)
		return true
	})
}

func (r *taskRuntime) dynamicWait() time.Duration {
	return dynconfig.Get[ateEnvKnobs](r.cfg.Dyn).WaitTime.Draw()
}

func (r *taskRuntime) liveWait() time.Duration {
	knobs := dynconfig.Get[ateEnvKnobs](r.cfg.Dyn)
	return dynconfig.Uniform(knobs.MinLive.Duration(), knobs.MaxLive.Duration())
}

// ateEnvUser represents one virtual user (goroutine).
type ateEnvUser struct {
	mu         sync.Mutex
	routerConn io.Closer
	actors     []*ateEnvActor
	nextIdx    int
}

func (u *ateEnvUser) nextActor() *ateEnvActor {
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.actors) == 0 {
		return nil
	}
	a := u.actors[u.nextIdx]
	u.nextIdx = (u.nextIdx + 1) % len(u.actors)
	return a
}

func (u *ateEnvUser) snapshotActors() []*ateEnvActor {
	u.mu.Lock()
	defer u.mu.Unlock()
	return slices.Clone(u.actors)
}

func (u *ateEnvUser) replaceActor(ctx context.Context, broken *ateEnvActor) {
	if broken.markDeleted() {
		broken.delete(ctx)
	}
	replacement := &ateEnvActor{
		cfg:           broken.cfg,
		actorName:     "sb-" + uuid.NewString(),
		firstResume:   true,
		processClient: broken.processClient,
	}
	if err := replacement.create(ctx); err != nil {
		slog.Warn("ate_env actor replacement create failed; will retry on next failure",
			slog.String("old_actor", broken.actorName),
			slog.String("err", err.Error()))
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	for i, a := range u.actors {
		if a == broken {
			u.actors[i] = replacement
			return
		}
	}
}

// ateEnvActor tracks the state of a single actor within a VU.
type ateEnvActor struct {
	mu                  sync.Mutex
	cfg                 *userclass.Config
	actorName           string
	firstResume         bool
	actorRunning        bool
	crashed             bool
	hibernatePending    bool
	deleted             bool
	consecutiveFailures int
	processClient       ateenvv1alpha.ProcessServiceClient
	execTimeout         time.Duration
}

func (u *ateEnvActor) isRunning() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.actorRunning
}

func (u *ateEnvActor) isHibernatePending() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.hibernatePending
}

func (u *ateEnvActor) isCrashed() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.crashed
}

func (u *ateEnvActor) markDeleted() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.deleted {
		return false
	}
	u.deleted = true
	return true
}

func (u *ateEnvActor) noteFailure(err error) bool {
	switch boomerutil.ClassifyLifecycleFailure(err) {
	case boomerutil.ReplaceNow:
		return true
	case boomerutil.RetryLater:
		return false
	}
	return u.countFailure(err)
}

func (u *ateEnvActor) noteExecFailure(err error) bool {
	if s, ok := status.FromError(err); ok {
		switch s.Code() {
		case codes.Canceled, codes.ResourceExhausted, codes.InvalidArgument,
			codes.PermissionDenied, codes.Unauthenticated, codes.Unimplemented:
			return false
		}
	}
	return u.countFailure(err)
}

func (u *ateEnvActor) countFailure(err error) bool {
	u.mu.Lock()
	u.consecutiveFailures++
	n := u.consecutiveFailures
	u.mu.Unlock()
	if n < maxConsecutiveFailures {
		return false
	}
	slog.Warn("ate_env actor failed repeatedly; replacing it",
		slog.String("actor", u.actorName),
		slog.Int("consecutive_failures", n),
		slog.String("err", err.Error()))
	return true
}

func (u *ateEnvActor) noteSuccess() {
	u.mu.Lock()
	u.consecutiveFailures = 0
	u.mu.Unlock()
}

func (u *ateEnvActor) ref() *ateapi.ObjectRef {
	return &ateapi.ObjectRef{Atespace: u.cfg.Atespace, Name: u.actorName}
}

func (u *ateEnvActor) ensureAtespace(ctx context.Context) error {
	return u.tracedCall(ctx, "CreateAtespace", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.CreateAtespace(callCtx, &ateapi.CreateAtespaceRequest{
			Atespace: &ateapi.Atespace{
				Metadata: &ateapi.ResourceMetadata{
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

func (u *ateEnvActor) create(ctx context.Context) error {
	return u.tracedCall(ctx, "CreateActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.CreateActor(callCtx, &ateapi.CreateActorRequest{
			Actor: &ateapi.Actor{
				Metadata:      &ateapi.ResourceMetadata{Atespace: u.cfg.Atespace, Name: u.actorName},
				ActorTemplate: &ateapi.ObjectRef{Atespace: templateAtespace, Name: templateName},
			},
		}, grpc.Trailer(tr))
		return err
	})
}

func (u *ateEnvActor) resume(ctx context.Context) error {
	u.mu.Lock()
	first := u.firstResume
	u.mu.Unlock()
	metricName := "ResumeActor"
	if first {
		metricName = "ResumeActorFirstResume"
	}
	err := u.tracedCall(ctx, metricName, func(callCtx context.Context, tr *metadata.MD) error {
		return boomerutil.RetryOnConflict(callCtx, func() error {
			_, err := u.cfg.APIStub.ResumeActor(callCtx, &ateapi.ResumeActorRequest{
				Actor: u.ref(),
			}, grpc.Trailer(tr))
			return err
		})
	})
	if err != nil {
		if boomerutil.IsCrashed(err) {
			u.mu.Lock()
			u.crashed = true
			u.mu.Unlock()
			bmetrics.RecordFailure("actor", "CrashCount", userClass, 0, "actor entered ACTOR_STATE_CRASHED")
			slog.Warn("ate_env actor crashed; will stop sending requests",
				slog.String("actor", u.actorName),
				slog.String("err", err.Error()))
		}
		return err
	}
	u.mu.Lock()
	u.firstResume = false
	u.actorRunning = true
	u.mu.Unlock()
	return nil
}

func (u *ateEnvActor) hibernate(ctx context.Context) error {
	switch dynconfig.Get[ateEnvKnobs](u.cfg.Dyn).LifecycleMode {
	case dynconfig.LifecycleModeNone:
		return nil
	case dynconfig.LifecycleModeSync:
		return u.suspend(ctx)
	default:
		return u.pause(ctx)
	}
}

func (u *ateEnvActor) pause(ctx context.Context) error {
	err := u.tracedCall(ctx, "SuspendActorAsync", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.SuspendActor(callCtx, &ateapi.SuspendActorRequest{
			Actor:   u.ref(),
			Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS},
		}, grpc.Trailer(tr))
		return err
	})
	u.mu.Lock()
	u.actorRunning = false
	u.hibernatePending = err != nil
	u.mu.Unlock()
	return err
}

func (u *ateEnvActor) suspend(ctx context.Context) error {
	err := u.tracedCall(ctx, "SuspendActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.SuspendActor(callCtx, &ateapi.SuspendActorRequest{
			Actor:   u.ref(),
			Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
		}, grpc.Trailer(tr))
		return err
	})
	u.mu.Lock()
	u.actorRunning = false
	u.hibernatePending = err != nil
	u.mu.Unlock()
	return err
}

func (u *ateEnvActor) delete(ctx context.Context) {
	_ = u.tracedCall(ctx, "DeleteActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.DeleteActor(callCtx, &ateapi.DeleteActorRequest{
			Actor: u.ref(),
		}, grpc.Trailer(tr))
		return err
	})
}

func (u *ateEnvActor) tracedCall(ctx context.Context, name string, do func(context.Context, *metadata.MD) error) error {
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

// metadataCarrier adapts metadata.MD to OpenTelemetry's TextMapCarrier.
type metadataCarrier metadata.MD

func (m metadataCarrier) Get(key string) string {
	vals := metadata.MD(m).Get(key)
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

func (m metadataCarrier) Set(key string, value string) {
	metadata.MD(m).Set(key, value)
}

func (m metadataCarrier) Keys() []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// execHelloWorld executes "echo hello world" inside the actor container via
// StartProcess and drains StreamProcessOutput until completion.
func (u *ateEnvActor) execHelloWorld(ctx context.Context) error {
	timeout := u.execTimeout
	if timeout <= 0 {
		timeout = execTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ctx, span := u.cfg.Tracer.Start(ctx, "AteEnvExec")
	defer span.End()

	start := time.Now()

	md := metadata.MD{}
	otel.GetTextMapPropagator().Inject(ctx, metadataCarrier(md))
	md.Set(atenet.TargetActorHeader, u.cfg.Atespace+"/"+u.actorName)
	callCtx := metadata.NewOutgoingContext(ctx, md)

	fail := func(err error) error {
		clientLatency := time.Since(start)
		boomerutil.LogSampledTrace(span, "AteEnvExec", clientLatency, boomerutil.SourceClient, err)
		bmetrics.RecordFailure("grpc", "AteEnvExec", userClass, clientLatency, err.Error())
		return err
	}

	startResp, err := u.processClient.StartProcess(callCtx, &ateenvv1alpha.StartProcessRequest{
		Command: []string{"echo", "hello world"},
	})
	if err != nil {
		return fail(fmt.Errorf("StartProcess: %w", err))
	}
	pid := startResp.GetProcessId()
	if pid == "" {
		return fail(errors.New("StartProcess returned empty process_id"))
	}

	stream, err := u.processClient.StreamProcessOutput(callCtx, &ateenvv1alpha.StreamProcessOutputRequest{
		ProcessId: pid,
		Follow:    true,
	})
	if err != nil {
		return fail(fmt.Errorf("StreamProcessOutput: %w", err))
	}

	var (
		stdoutBuf bytes.Buffer
		exitProc  *ateenvv1alpha.Process
	)
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fail(fmt.Errorf("StreamProcessOutput.Recv: %w", err))
		}
		if chunk := msg.GetStdout(); len(chunk) > 0 {
			stdoutBuf.Write(chunk)
		}
		if exit := msg.GetExit(); exit != nil {
			exitProc = exit
		}
	}

	if exitProc == nil {
		return fail(errors.New("stream closed without exit event"))
	}
	if state := exitProc.GetState(); state != ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED {
		return fail(fmt.Errorf("unexpected process state: %v, want PROCESS_STATE_EXITED", state))
	}
	if code := exitProc.GetExitCode(); code != 0 {
		return fail(fmt.Errorf("unexpected exit code: %d, want 0", code))
	}
	if got := stdoutBuf.String(); got != expectedStdout {
		return fail(fmt.Errorf("stdout mismatch: got %q, want %q", got, expectedStdout))
	}

	clientLatency := time.Since(start)
	boomerutil.LogSampledTrace(span, "AteEnvExec", clientLatency, boomerutil.SourceClient, nil)
	bmetrics.RecordSuccess("grpc", "AteEnvExec", userClass, clientLatency, int64(stdoutBuf.Len()))
	return nil
}
