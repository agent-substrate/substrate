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

// Package storage implements the boomer-Go storage workload runner (GluttonStorageUser).
package storage

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
	bmetrics "github.com/agent-substrate/substrate/internal/benchmarking/boomer/metrics"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
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
	userClass     = "GluttonStorageUser"
	templateName  = "glutton-storage"
	templateNS    = "benchmark-workloads"
	actorDomain   = "actors.resources.substrate.ate.dev"
	writeDiskPath = "/writedisk"
	pingPath      = "/ping"

	defaultWriteSize int32 = 1048576 // 1 MiB

	sourceClient = "client"
	sourceServer = "server"
)

func init() {
	userclass.Add(userclass.Entry{
		Name:       "glutton_storage",
		LocustFile: "glutton_storage.py",
		UserClass:  userClass,
		Init:       InitStorage,
	})
}

// InitStorage creates a runtime tied to cfg and returns a boomer-compatible task
// function plus a Shutdown hook the caller should run before exit.
func InitStorage(cfg *userclass.Config) (taskFn func(), shutdown func(context.Context)) {
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer("substrate-boomer/storage")
	}
	rt := &taskRuntime{cfg: cfg}
	return rt.iterate, rt.shutdown
}

type taskRuntime struct {
	cfg   *userclass.Config
	users sync.Map // goroutineID → *gluttonStorageUser
}

// iterate is the task function boomer calls in a loop on each VU goroutine.
func (r *taskRuntime) iterate() {
	gid := goroutineID()
	val, loaded := r.users.Load(gid)
	if !loaded {
		u, err := r.startUser(context.Background())
		if err != nil {
			slog.Warn("glutton storage on_start failed; goroutine will retry next iter",
				slog.String("err", err.Error()))
			return
		}
		val, _ = r.users.LoadOrStore(gid, u)
	}
	user := val.(*gluttonStorageUser)

	ctx := context.Background()
	if !user.resume(ctx) {
		return
	}
	user.writeDisk(ctx)
	user.suspend(ctx)

	time.Sleep(r.dynamicWait())
}

func (r *taskRuntime) startUser(ctx context.Context) (*gluttonStorageUser, error) {
	u := &gluttonStorageUser{
		cfg:         r.cfg,
		actorName:   "sb-st-" + uuid.NewString(),
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

func (r *taskRuntime) shutdown(ctx context.Context) {
	r.users.Range(func(_, val any) bool {
		u := val.(*gluttonStorageUser)
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

type gluttonStorageUser struct {
	cfg          *userclass.Config
	actorName    string
	hostHeader   string
	firstResume  bool
	actorRunning bool
	epoch        int64
}

func (u *gluttonStorageUser) ref() *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Atespace: u.cfg.Atespace, Name: u.actorName}
}

func (u *gluttonStorageUser) ensureAtespace(ctx context.Context) error {
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

func (u *gluttonStorageUser) create(ctx context.Context) error {
	return u.tracedCall(ctx, "CreateActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.CreateActor(callCtx, &ateapipb.CreateActorRequest{
			Actor: &ateapipb.Actor{
				Metadata:               &ateapipb.ResourceMetadata{Atespace: u.cfg.Atespace, Name: u.actorName},
				ActorTemplateNamespace: templateNS,
				ActorTemplateName:      templateName,
			},
		}, grpc.Trailer(tr))
		return err
	})
}

func (u *gluttonStorageUser) resume(ctx context.Context) bool {
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

func (u *gluttonStorageUser) suspend(ctx context.Context) {
	_ = u.tracedCall(ctx, "SuspendActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.SuspendActor(callCtx, &ateapipb.SuspendActorRequest{
			Actor: u.ref(),
		}, grpc.Trailer(tr))
		return err
	})
	u.actorRunning = false
}

func (u *gluttonStorageUser) delete(ctx context.Context) {
	_ = u.tracedCall(ctx, "DeleteActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.DeleteActor(callCtx, &ateapipb.DeleteActorRequest{
			Actor: u.ref(),
		}, grpc.Trailer(tr))
		return err
	})
}

func (u *gluttonStorageUser) tracedCall(ctx context.Context, name string, do func(context.Context, *metadata.MD) error) error {
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

func (u *gluttonStorageUser) writeDisk(ctx context.Context) {
	ctx, span := u.cfg.Tracer.Start(ctx, "GluttonWriteDisk")
	defer span.End()

	key := fmt.Sprintf("bench_%d", u.epoch)
	u.epoch++

	body, err := proto.Marshal(&gluttonpb.WriteDiskRequest{
		Key:       key,
		Size:      defaultWriteSize,
		WriteMode: gluttonpb.WriteMode_WRITE_MODE_TRUNCATE,
	})
	if err != nil {
		bmetrics.RecordFailure("http", "GluttonWriteDisk", userClass, 0, err.Error())
		return
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.cfg.RouterURL+writeDiskPath, bytes.NewReader(body))
	if err != nil {
		bmetrics.RecordFailure("http", "GluttonWriteDisk", userClass, 0, err.Error())
		return
	}
	httpReq.Host = u.hostHeader
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(httpReq.Header))

	start := time.Now()
	resp, err := u.cfg.HTTPClient.Do(httpReq)
	clientLatency := time.Since(start)
	if err != nil {
		bmetrics.RecordFailure("http", "GluttonWriteDisk", userClass, clientLatency, err.Error())
		return
	}
	defer resp.Body.Close()

	respBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		bmetrics.RecordFailure("http", "GluttonWriteDisk", userClass, clientLatency, readErr.Error())
		return
	}

	if resp.StatusCode >= 400 {
		httpErr := fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
		logSampledTrace(span, "GluttonWriteDisk", clientLatency, sourceClient, httpErr)
		bmetrics.RecordFailure("http", "GluttonWriteDisk", userClass, clientLatency, httpErr.Error())
		return
	}

	writeResp := &gluttonpb.WriteDiskResponse{}
	if err := proto.Unmarshal(respBody, writeResp); err != nil {
		logSampledTrace(span, "GluttonWriteDisk", clientLatency, sourceClient, err)
		bmetrics.RecordFailure("http", "GluttonWriteDisk", userClass, clientLatency, err.Error())
		return
	}

	logSampledTrace(span, "GluttonWriteDisk", clientLatency, sourceClient, nil)
	bmetrics.RecordSuccess("http", "GluttonWriteDisk", userClass, clientLatency, int64(defaultWriteSize))
}

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
