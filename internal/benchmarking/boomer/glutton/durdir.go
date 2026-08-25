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
	"crypto/sha256"
	"encoding/hex"
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
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
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
	// Locust class name from tests/durdir.py; must match boomer.Task.Name.
	durDirUserClass    = "DurdirUser"
	defaultDurTemplate = "glutton-durdir-data"

	writeDiskRoute = "/writedisk"
	readDiskRoute  = "/readdisk"

	durDirTestFile = "bench-data"

	defaultFileSize int64 = 8388608 // 8 MiB
)

func init() {
	userclass.Add(userclass.Entry{
		Name:       "durdir",
		LocustFile: "durdir.py",
		UserClass:  durDirUserClass,
		Init:       initDurDir,
	})
}

// initDurDir creates a runtime tied to cfg and returns a boomer-compatible task
// function plus a Shutdown hook the caller should run before exit.
func initDurDir(cfg *userclass.Config) (taskFn func(), shutdown func(context.Context)) {
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer("substrate-boomer/glutton-durdir")
	}
	rt := &durDirRuntime{cfg: cfg}
	return rt.iterate, rt.shutdown
}

type durDirRuntime struct {
	cfg   *userclass.Config
	users sync.Map // goroutineID -> *durDirUser
}

func (r *durDirRuntime) dynamicWait() time.Duration {
	cfg := r.cfg.Dyn.Load()
	if cfg.MaxWait <= cfg.MinWait {
		return cfg.MinWait
	}
	jitter := cfg.MaxWait - cfg.MinWait
	return cfg.MinWait + time.Duration(rand.Float64()*float64(jitter))
}

func (r *durDirRuntime) iterate() {
	gid := goroutineID()
	val, loaded := r.users.Load(gid)
	if !loaded {
		dynCfg := r.cfg.Dyn.Load()
		u, err := r.startUser(context.Background(), dynCfg)
		if err != nil {
			slog.Warn("durdir on_start failed; goroutine will retry next iter",
				slog.String("err", err.Error()))
			time.Sleep(r.dynamicWait())
			return
		}
		val, _ = r.users.LoadOrStore(gid, u)
	}
	user := val.(*durDirUser)

	dynCfg := r.cfg.Dyn.Load()
	ctx := context.Background()
	user.step(ctx, dynCfg)

	time.Sleep(r.dynamicWait())
}

func (r *durDirRuntime) startUser(ctx context.Context, dynCfg dynconfig.Config) (*durDirUser, error) {
	tmpl := dynCfg.DurDirTemplate
	if tmpl == "" {
		tmpl = defaultDurTemplate
	}

	u := &durDirUser{
		cfg:          r.cfg,
		actorName:    "sb-" + uuid.NewString(),
		templateName: tmpl,
		userClass:    durDirUserClass,
	}
	u.hostHeader = u.actorName + "." + u.cfg.Atespace + "." + actorDomain
	bmetrics.UpdateUsers(durDirUserClass, 1)
	if err := u.ensureAtespace(ctx); err != nil {
		bmetrics.UpdateUsers(durDirUserClass, -1)
		return nil, err
	}
	if err := u.create(ctx); err != nil {
		bmetrics.UpdateUsers(durDirUserClass, -1)
		return nil, err
	}
	if err := u.bootstrap(ctx, dynCfg); err != nil {
		u.suspendAndDelete(ctx)
		bmetrics.UpdateUsers(durDirUserClass, -1)
		return nil, err
	}
	return u, nil
}

func (r *durDirRuntime) shutdown(ctx context.Context) {
	r.users.Range(func(_, val any) bool {
		u := val.(*durDirUser)
		u.suspendAndDelete(ctx)
		bmetrics.UpdateUsers(durDirUserClass, -1)
		return true
	})
}

type durDirUser struct {
	cfg            *userclass.Config
	actorName      string
	hostHeader     string
	templateName   string
	userClass      string
	expectedDigest string
	expectedSize   int64
}

func (u *durDirUser) ref() *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Atespace: u.cfg.Atespace, Name: u.actorName}
}

func (u *durDirUser) ensureAtespace(ctx context.Context) error {
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

func (u *durDirUser) create(ctx context.Context) error {
	return u.tracedCall(ctx, "CreateActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.CreateActor(callCtx, &ateapipb.CreateActorRequest{
			Actor: &ateapipb.Actor{
				Metadata:               &ateapipb.ResourceMetadata{Atespace: u.cfg.Atespace, Name: u.actorName},
				ActorTemplateNamespace: templateNS,
				ActorTemplateName:      u.templateName,
			},
		}, grpc.Trailer(tr))
		return err
	})
}

func (u *durDirUser) resume(ctx context.Context, mode string) bool {
	// In implicit mode, the actor stays suspended until router traffic wakes it.
	if mode == dynconfig.ResumeModeImplicit {
		return true
	}
	err := u.tracedCall(ctx, "ResumeActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.ResumeActor(callCtx, &ateapipb.ResumeActorRequest{
			Actor: u.ref(),
		}, grpc.Trailer(tr))
		return err
	})
	return err == nil
}

func (u *durDirUser) suspend(ctx context.Context) {
	_ = u.tracedCall(ctx, "SuspendActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.SuspendActor(callCtx, &ateapipb.SuspendActorRequest{
			Actor: u.ref(),
		}, grpc.Trailer(tr))
		return err
	})
}

// suspendAndDelete suspends the actor before deleting it. DeleteActor requires
// SUSPENDED or CRASHED; deleting a running actor leaks it. The suspend is
// unmetered (teardown precondition, not benchmark latency), while the delete
// is metered so true leaks still surface in failures.csv.
func (u *durDirUser) suspendAndDelete(ctx context.Context) {
	_, _ = u.cfg.APIStub.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: u.ref(),
	})
	u.delete(ctx)
}

func (u *durDirUser) delete(ctx context.Context) {
	_ = u.tracedCall(ctx, "DeleteActor", func(callCtx context.Context, tr *metadata.MD) error {
		_, err := u.cfg.APIStub.DeleteActor(callCtx, &ateapipb.DeleteActorRequest{
			Actor: u.ref(),
		}, grpc.Trailer(tr))
		return err
	})
}

func (u *durDirUser) tracedCall(ctx context.Context, name string, do func(context.Context, *metadata.MD) error) error {
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
		bmetrics.RecordFailure("grpc", name, u.userClass, latency, err.Error())
		return err
	}
	bmetrics.RecordSuccess("grpc", name, u.userClass, latency, 0)
	return nil
}

func (u *durDirUser) params(dynCfg dynconfig.Config) (int64, gluttonpb.ReadMode) {
	fileSize := dynCfg.DurDirFileSize
	if fileSize <= 0 {
		fileSize = defaultFileSize
	}
	readMode := gluttonpb.ReadMode_READ_MODE_DATA
	if dynCfg.DurDirReadMode == dynconfig.ReadModeDigest {
		readMode = gluttonpb.ReadMode_READ_MODE_DIGEST_ONLY
	}
	return fileSize, readMode
}

func (u *durDirUser) step(ctx context.Context, dynCfg dynconfig.Config) {
	fileSize, readMode := u.params(dynCfg)

	// 1. Suspend actor
	u.suspend(ctx)

	// 2. Resume — a no-op in implicit mode, where router traffic wakes the actor.
	if !u.resume(ctx, dynCfg.ResumeMode) {
		return
	}

	// 3. Serve after resume (durability assertion: verify restored bytes)
	if err := u.readDisk(ctx, "DurDirServeAfterResume", readMode); err != nil {
		return
	}

	// 4. Serve warm (immediate second read: measures page cache warming delta)
	if err := u.readDisk(ctx, "DurDirServeWarm", readMode); err != nil {
		return
	}

	// 5. Overwrite file with fresh random bytes
	if err := u.writeDisk(ctx, "DurDirOverwrite", fileSize, gluttonpb.WriteMode_WRITE_MODE_TRUNCATE); err != nil {
		return
	}
}

func (u *durDirUser) bootstrap(ctx context.Context, dynCfg dynconfig.Config) error {
	fileSize, readMode := u.params(dynCfg)

	if !u.resume(ctx, dynCfg.ResumeMode) {
		return fmt.Errorf("initial resume failed")
	}

	// Initial write to create DurDir file
	if err := u.writeDisk(ctx, "DurDirWrite", fileSize, gluttonpb.WriteMode_WRITE_MODE_TRUNCATE); err != nil {
		return fmt.Errorf("initial WriteDisk failed: %w", err)
	}

	// Initial read to verify file
	if err := u.readDisk(ctx, "DurDirServeInitial", readMode); err != nil {
		return fmt.Errorf("initial ReadDisk failed: %w", err)
	}

	return nil
}

func (u *durDirUser) writeDisk(ctx context.Context, metricName string, size int64, mode gluttonpb.WriteMode) error {
	req := &gluttonpb.WriteDiskRequest{
		Key:       durDirTestFile,
		Size:      int32(size),
		WriteMode: mode,
	}
	body, err := proto.Marshal(req)
	if err != nil {
		bmetrics.RecordFailure("http", metricName, u.userClass, 0, err.Error())
		return err
	}

	var newDigest string
	var newSize int64
	_, err = u.httpProtoCall(ctx, metricName, writeDiskRoute, body, func(respBytes []byte) error {
		var resp gluttonpb.WriteDiskResponse
		if err := proto.Unmarshal(respBytes, &resp); err != nil {
			return fmt.Errorf("unmarshal WriteDiskResponse: %w", err)
		}
		if resp.GetSize() != size {
			return fmt.Errorf("WriteDisk size mismatch: got %d, want %d", resp.GetSize(), size)
		}
		if len(resp.GetSha256()) == 0 {
			return fmt.Errorf("WriteDisk sha256 is empty")
		}
		newDigest = hex.EncodeToString(resp.GetSha256())
		newSize = resp.GetSize()
		return nil
	})

	if err != nil {
		return err
	}
	u.expectedDigest = newDigest
	u.expectedSize = newSize
	return nil
}

func (u *durDirUser) readDisk(ctx context.Context, metricName string, readMode gluttonpb.ReadMode) error {
	req := &gluttonpb.ReadDiskRequest{
		Key:      durDirTestFile,
		ReadMode: readMode,
	}
	body, err := proto.Marshal(req)
	if err != nil {
		bmetrics.RecordFailure("http", metricName, u.userClass, 0, err.Error())
		return err
	}

	expectedDigest := u.expectedDigest
	expectedSize := u.expectedSize

	_, err = u.httpProtoCall(ctx, metricName, readDiskRoute, body, func(respBytes []byte) error {
		var resp gluttonpb.ReadDiskResponse
		if err := proto.Unmarshal(respBytes, &resp); err != nil {
			return fmt.Errorf("unmarshal ReadDiskResponse: %w", err)
		}
		if resp.GetSize() != expectedSize {
			return fmt.Errorf("ReadDisk size mismatch: got %d, want %d", resp.GetSize(), expectedSize)
		}
		respDigest := hex.EncodeToString(resp.GetSha256())
		if respDigest != expectedDigest {
			return fmt.Errorf("ReadDisk response sha256 mismatch: got %q, want %q", respDigest, expectedDigest)
		}
		if readMode == gluttonpb.ReadMode_READ_MODE_DIGEST_ONLY {
			return nil
		}
		h := sha256.Sum256(resp.GetData())
		computedDigest := hex.EncodeToString(h[:])
		if computedDigest != expectedDigest {
			return fmt.Errorf("ReadDisk payload sha256 mismatch: computed %q, want %q", computedDigest, expectedDigest)
		}
		return nil
	})

	return err
}

// httpProtoCall issues a POST request to route with body and records metrics and traces.
// Metrics record client-perceived latency because the measurement target is Substrate,
// not glutton.
func (u *durDirUser) httpProtoCall(ctx context.Context, metricName, route string, body []byte, validate func([]byte) error) ([]byte, error) {
	ctx, span := u.cfg.Tracer.Start(ctx, metricName)
	defer span.End()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.cfg.RouterURL+route, bytes.NewReader(body))
	if err != nil {
		bmetrics.RecordFailure("http", metricName, u.userClass, 0, err.Error())
		return nil, err
	}
	httpReq.Host = u.hostHeader
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
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

	serverLatency, source := elapsedFromHeader(resp.Header, ateinterceptors.ServerElapsedTrailer, clientLatency)
	if source == sourceServer {
		span.SetAttributes(attribute.Float64("server.elapsed_ms", msFloat(serverLatency)))
	}

	if resp.StatusCode >= 400 {
		httpErr := fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
		logSampledTrace(span, metricName, clientLatency, sourceClient, httpErr)
		bmetrics.RecordFailure("http", metricName, u.userClass, clientLatency, httpErr.Error())
		return nil, httpErr
	}

	if validate != nil {
		if err := validate(respBody); err != nil {
			logSampledTrace(span, metricName, clientLatency, sourceClient, err)
			bmetrics.RecordFailure("http", metricName, u.userClass, clientLatency, err.Error())
			return nil, err
		}
	}

	logSampledTrace(span, metricName, clientLatency, sourceClient, nil)
	bmetrics.RecordSuccess("http", metricName, u.userClass, clientLatency, int64(len(respBody)))
	return respBody, nil
}

func elapsedFromHeader(h http.Header, key string, fallback time.Duration) (time.Duration, string) {
	val := h.Get(key)
	if val == "" {
		return fallback, sourceClient
	}
	us, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return fallback, sourceClient
	}
	return time.Duration(us) * time.Microsecond, sourceServer
}
