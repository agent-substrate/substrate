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

package ateenv

import (
	"context"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/atenet"
	"go.ate.dev/substrate/internal/benchmarking/boomer/boomerutil"
	"go.ate.dev/substrate/internal/benchmarking/boomer/dynconfig"
	"go.ate.dev/substrate/internal/benchmarking/boomer/userclass"
	ateenvv1alpha "go.ate.dev/substrate/internal/proto/ateenv/v1alpha"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// fakeControlClient implements ateapi.ControlClient for testing.
type fakeControlClient struct {
	ateapi.ControlClient
	mu          sync.Mutex
	calls       []string
	suspendReqs []*ateapi.SuspendActorRequest
	resumeErrs  []error
	suspendErrs []error
}

func nextErr(errs *[]error) error {
	if len(*errs) == 0 {
		return nil
	}
	err := (*errs)[0]
	*errs = (*errs)[1:]
	return err
}

func (f *fakeControlClient) CreateAtespace(ctx context.Context, in *ateapi.CreateAtespaceRequest, opts ...grpc.CallOption) (*ateapi.Atespace, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "CreateAtespace")
	return &ateapi.Atespace{}, nil
}

func (f *fakeControlClient) CreateActor(ctx context.Context, in *ateapi.CreateActorRequest, opts ...grpc.CallOption) (*ateapi.Actor, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "CreateActor")
	return &ateapi.Actor{}, nil
}

func (f *fakeControlClient) ResumeActor(ctx context.Context, in *ateapi.ResumeActorRequest, opts ...grpc.CallOption) (*ateapi.ResumeActorResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "ResumeActor")
	if err := nextErr(&f.resumeErrs); err != nil {
		return nil, err
	}
	return &ateapi.ResumeActorResponse{}, nil
}

func (f *fakeControlClient) SuspendActor(ctx context.Context, in *ateapi.SuspendActorRequest, opts ...grpc.CallOption) (*ateapi.SuspendActorResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, suspendCallName(in))
	f.suspendReqs = append(f.suspendReqs, in)
	if err := nextErr(&f.suspendErrs); err != nil {
		return nil, err
	}
	return &ateapi.SuspendActorResponse{}, nil
}

// suspendCallName is the name a SuspendActor call is recorded under: an
// async suspend is told apart from a sync one.
func suspendCallName(in *ateapi.SuspendActorRequest) string {
	if in.GetOptions().GetMode() == ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS {
		return "SuspendActor(async)"
	}
	return "SuspendActor"
}

func (f *fakeControlClient) DeleteActor(ctx context.Context, in *ateapi.DeleteActorRequest, opts ...grpc.CallOption) (*ateapi.Actor, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "DeleteActor")
	return &ateapi.Actor{}, nil
}

func (f *fakeControlClient) recordedCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func (f *fakeControlClient) recordedSuspendRequests() []*ateapi.SuspendActorRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*ateapi.SuspendActorRequest(nil), f.suspendReqs...)
}

// fakeProcessServer implements ateenvv1alpha.ProcessServiceServer for testing.
type fakeProcessServer struct {
	ateenvv1alpha.UnimplementedProcessServiceServer
	mu              sync.Mutex
	startReqs       []*ateenvv1alpha.StartProcessRequest
	streamReqs      []*ateenvv1alpha.StreamProcessOutputRequest
	incomingMD      []metadata.MD
	startErr        error
	streamErr       error
	emptyPID        bool
	omitExit        bool
	blockStream     bool
	customStdout    *string
	stdoutChunks    []string
	customExitState *ateenvv1alpha.ProcessState
	customExitCode  *int32
}

func (s *fakeProcessServer) StartProcess(ctx context.Context, in *ateenvv1alpha.StartProcessRequest) (*ateenvv1alpha.Process, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.incomingMD = append(s.incomingMD, md.Copy())
	}
	s.startReqs = append(s.startReqs, in)
	if s.startErr != nil {
		return nil, s.startErr
	}
	pid := "proc-1"
	if s.emptyPID {
		pid = ""
	}
	return &ateenvv1alpha.Process{
		ProcessId: pid,
		Command:   in.GetCommand(),
		State:     ateenvv1alpha.ProcessState_PROCESS_STATE_RUNNING,
	}, nil
}

func (s *fakeProcessServer) StreamProcessOutput(in *ateenvv1alpha.StreamProcessOutputRequest, stream grpc.ServerStreamingServer[ateenvv1alpha.ProcessOutput]) error {
	s.mu.Lock()
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		s.incomingMD = append(s.incomingMD, md.Copy())
	}
	s.streamReqs = append(s.streamReqs, in)
	streamErr := s.streamErr
	omitExit := s.omitExit
	blockStream := s.blockStream
	stdout := expectedStdout
	if s.customStdout != nil {
		stdout = *s.customStdout
	}
	chunks := append([]string(nil), s.stdoutChunks...)
	exitState := ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED
	if s.customExitState != nil {
		exitState = *s.customExitState
	}
	exitCode := int32(0)
	if s.customExitCode != nil {
		exitCode = *s.customExitCode
	}
	s.mu.Unlock()

	if blockStream {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	if streamErr != nil {
		return streamErr
	}

	if len(chunks) > 0 {
		for _, chunk := range chunks {
			if err := stream.Send(&ateenvv1alpha.ProcessOutput{
				Output: &ateenvv1alpha.ProcessOutput_Stdout{Stdout: []byte(chunk)},
			}); err != nil {
				return err
			}
		}
	} else if len(stdout) > 0 {
		if err := stream.Send(&ateenvv1alpha.ProcessOutput{
			Output: &ateenvv1alpha.ProcessOutput_Stdout{Stdout: []byte(stdout)},
		}); err != nil {
			return err
		}
	}

	if !omitExit {
		if err := stream.Send(&ateenvv1alpha.ProcessOutput{
			Output: &ateenvv1alpha.ProcessOutput_Exit{
				Exit: &ateenvv1alpha.Process{
					ProcessId: in.GetProcessId(),
					State:     exitState,
					ExitCode:  exitCode,
				},
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

type closerTracker struct {
	io.Closer
	closed bool
	mu     sync.Mutex
}

func (c *closerTracker) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	if c.Closer != nil {
		return c.Closer.Close()
	}
	return nil
}

func (c *closerTracker) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// testFixture sets up an in-memory gRPC server with fakeProcessServer.
type testFixture struct {
	srv           *fakeProcessServer
	grpcServer    *grpc.Server
	listener      *bufconn.Listener
	client        ateenvv1alpha.ProcessServiceClient
	closerTracker *closerTracker
}

func newTestFixture(t *testing.T, srv *fakeProcessServer) *testFixture {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	grpcSrv := grpc.NewServer()
	ateenvv1alpha.RegisterProcessServiceServer(grpcSrv, srv)
	go func() {
		_ = grpcSrv.Serve(lis)
	}()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufnet: %v", err)
	}

	ct := &closerTracker{Closer: conn}
	t.Cleanup(func() {
		_ = conn.Close()
		grpcSrv.Stop()
		_ = lis.Close()
	})

	return &testFixture{
		srv:           srv,
		grpcServer:    grpcSrv,
		listener:      lis,
		client:        ateenvv1alpha.NewProcessServiceClient(conn),
		closerTracker: ct,
	}
}

func newTestRuntime(t *testing.T, ctrl *fakeControlClient, procSrv *fakeProcessServer, mode string) (*taskRuntime, *testFixture) {
	t.Helper()
	fix := newTestFixture(t, procSrv)
	cfg := &userclass.Config{
		APIStub:       ctrl,
		Atespace:      "bench-test",
		ActorsPerUser: 1,
		Tracer:        otel.Tracer("test"),
		Dyn: dynconfig.Static(ateEnvKnobs{
			Lifecycle: dynconfig.Lifecycle{LifecycleMode: mode},
		}),
	}
	rt := &taskRuntime{
		cfg: cfg,
		dialProcessClient: func() (ateenvv1alpha.ProcessServiceClient, io.Closer, error) {
			return fix.client, fix.closerTracker, nil
		},
	}
	return rt, fix
}

func TestRouterGRPCTarget(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{
			input: "",
			want:  "dns:///atenet-router.ate-system.svc:443",
		},
		{
			input: "https://atenet-router.ate-system.svc",
			want:  "dns:///atenet-router.ate-system.svc:443",
		},
		{
			input: "https://atenet-router.ate-system.svc.cluster.local",
			want:  "dns:///atenet-router.ate-system.svc.cluster.local:443",
		},
		{
			input: "https://atenet-router.ate-system.svc.cluster.local:8443",
			want:  "dns:///atenet-router.ate-system.svc.cluster.local:8443",
		},
		{
			input: "atenet-router.ate-system.svc",
			want:  "dns:///atenet-router.ate-system.svc:443",
		},
		{
			input: "router.example.internal:9000",
			want:  "dns:///router.example.internal:9000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := routerGRPCTarget(tt.input)
			if err != nil {
				t.Fatalf("routerGRPCTarget(%q) unexpected err: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("routerGRPCTarget(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}

	if _, err := routerGRPCTarget("https:///missing-host"); err == nil {
		t.Errorf("routerGRPCTarget(empty host) succeeded, want error")
	}
	if _, err := routerGRPCTarget("http://atenet-router.ate-system.svc.cluster.local"); err == nil {
		t.Errorf("routerGRPCTarget(http) succeeded, want error")
	}
}

func TestAteEnvIterate_SuspendMode(t *testing.T) {
	ctrl := &fakeControlClient{}
	srv := &fakeProcessServer{}
	rt, fix := newTestRuntime(t, ctrl, srv, dynconfig.LifecycleModeSync)

	rt.iterate()

	calls := ctrl.recordedCalls()
	if !slices.Contains(calls, "CreateAtespace") {
		t.Errorf("expected CreateAtespace in calls, got: %v", calls)
	}
	if !slices.Contains(calls, "CreateActor") {
		t.Errorf("expected CreateActor in calls, got: %v", calls)
	}
	if !slices.Contains(calls, "ResumeActor") {
		t.Errorf("expected ResumeActor in calls, got: %v", calls)
	}
	if !slices.Contains(calls, "SuspendActor") {
		t.Errorf("expected SuspendActor in calls, got: %v", calls)
	}
	if slices.Contains(calls, "SuspendActor(async)") {
		t.Errorf("did not expect SuspendActor(async) in suspend mode calls, got: %v", calls)
	}
	for _, req := range ctrl.recordedSuspendRequests() {
		if got := req.GetOptions().GetMode(); got != ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS {
			t.Errorf("suspend mode = %v, want %v", got, ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS)
		}
	}

	fix.srv.mu.Lock()
	defer fix.srv.mu.Unlock()
	if len(fix.srv.startReqs) != 1 {
		t.Fatalf("expected 1 StartProcess request, got %d", len(fix.srv.startReqs))
	}
	wantCmd := []string{"echo", "hello world"}
	if !slices.Equal(fix.srv.startReqs[0].GetCommand(), wantCmd) {
		t.Errorf("command = %v, want %v", fix.srv.startReqs[0].GetCommand(), wantCmd)
	}
	if len(fix.srv.streamReqs) != 1 {
		t.Fatalf("expected 1 StreamProcessOutput request, got %d", len(fix.srv.streamReqs))
	}
	if fix.srv.streamReqs[0].GetProcessId() != "proc-1" {
		t.Errorf("stream process_id = %q, want 'proc-1'", fix.srv.streamReqs[0].GetProcessId())
	}
	if !fix.srv.streamReqs[0].GetFollow() {
		t.Errorf("stream follow = false, want true")
	}
}

func TestAteEnvIterate_PauseMode(t *testing.T) {
	ctrl := &fakeControlClient{}
	srv := &fakeProcessServer{}
	rt, _ := newTestRuntime(t, ctrl, srv, dynconfig.LifecycleModeAsync)

	rt.iterate()

	calls := ctrl.recordedCalls()
	if !slices.Contains(calls, "SuspendActor(async)") {
		t.Errorf("expected SuspendActor(async) in calls, got: %v", calls)
	}
	if slices.Contains(calls, "SuspendActor") {
		t.Errorf("did not expect SuspendActor in pause mode calls, got: %v", calls)
	}
}

func TestAteEnvIterate_NoneMode(t *testing.T) {
	ctrl := &fakeControlClient{}
	srv := &fakeProcessServer{}
	rt, _ := newTestRuntime(t, ctrl, srv, dynconfig.LifecycleModeNone)

	rt.iterate()
	rt.iterate()

	calls := ctrl.recordedCalls()
	if slices.Contains(calls, "SuspendActor") || slices.Contains(calls, "SuspendActor(async)") {
		t.Errorf("did not expect a sync or async SuspendActor during iterate in none mode, got: %v", calls)
	}
	if got := countCalls(calls, "ResumeActor"); got != 1 {
		t.Errorf("ResumeActor count across 2 iterations = %d, want 1 (first activation only); calls = %v", got, calls)
	}

	rt.shutdown(context.Background())
	afterShutdown := ctrl.recordedCalls()
	if len(afterShutdown) < 2 || afterShutdown[len(afterShutdown)-2] != "SuspendActor(async)" || afterShutdown[len(afterShutdown)-1] != "DeleteActor" {
		t.Errorf("shutdown in none mode must end with [SuspendActor(async), DeleteActor], got %v", afterShutdown)
	}
}

func TestAteEnvExec_MetadataPropagation(t *testing.T) {
	prevProp := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTextMapPropagator(prevProp)
	})
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
	})

	ctrl := &fakeControlClient{}
	srv := &fakeProcessServer{
		stdoutChunks: []string{"hello ", "world\n"},
	}
	rt, fix := newTestRuntime(t, ctrl, srv, dynconfig.LifecycleModeSync)
	rt.cfg.Tracer = tp.Tracer("test-sampled")

	rt.iterate()

	fix.srv.mu.Lock()
	defer fix.srv.mu.Unlock()
	if len(fix.srv.incomingMD) < 2 {
		t.Fatalf("expected at least 2 incoming metadata captures, got %d", len(fix.srv.incomingMD))
	}

	for i, md := range fix.srv.incomingMD {
		vals := md.Get(atenet.TargetActorHeader)
		if len(vals) != 1 {
			t.Errorf("call %d: expected exactly 1 %q header, got %v", i, atenet.TargetActorHeader, vals)
		} else if !strings.HasPrefix(vals[0], "bench-test/sb-") {
			t.Errorf("call %d: invalid target actor header: %v", i, vals)
		}
		if tpVals := md.Get("traceparent"); len(tpVals) != 1 || tpVals[0] == "" {
			t.Errorf("call %d: expected sampled traceparent header in metadata, got %v", i, tpVals)
		}
	}
}

func TestAteEnvExec_ValidationFailures(t *testing.T) {
	tests := []struct {
		name     string
		setupSrv func(*fakeProcessServer)
		wantErr  string
	}{
		{
			name: "start process error",
			setupSrv: func(s *fakeProcessServer) {
				s.startErr = status.Error(codes.Internal, "failed to fork")
			},
			wantErr: "StartProcess:",
		},
		{
			name: "empty process_id",
			setupSrv: func(s *fakeProcessServer) {
				s.emptyPID = true
			},
			wantErr: "StartProcess returned empty process_id",
		},
		{
			name: "stream process output error",
			setupSrv: func(s *fakeProcessServer) {
				s.streamErr = status.Error(codes.Unavailable, "guest down")
			},
			wantErr: "StreamProcessOutput.Recv:",
		},
		{
			name: "stream closed without exit event",
			setupSrv: func(s *fakeProcessServer) {
				s.omitExit = true
			},
			wantErr: "stream closed without exit event",
		},
		{
			name: "non-zero exit code",
			setupSrv: func(s *fakeProcessServer) {
				code := int32(1)
				s.customExitCode = &code
			},
			wantErr: "unexpected exit code: 1, want 0",
		},
		{
			name: "unexpected process state",
			setupSrv: func(s *fakeProcessServer) {
				state := ateenvv1alpha.ProcessState_PROCESS_STATE_RUNNING
				s.customExitState = &state
			},
			wantErr: "unexpected process state: PROCESS_STATE_RUNNING, want PROCESS_STATE_EXITED",
		},
		{
			name: "stdout mismatch",
			setupSrv: func(s *fakeProcessServer) {
				mismatch := "hello mars\n"
				s.customStdout = &mismatch
			},
			wantErr: "stdout mismatch:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := &fakeControlClient{}
			srv := &fakeProcessServer{}
			tt.setupSrv(srv)
			rt, fix := newTestRuntime(t, ctrl, srv, dynconfig.LifecycleModeSync)

			actor := &ateEnvActor{
				cfg:           rt.cfg,
				actorName:     "sb-test",
				processClient: fix.client,
			}
			err := actor.execHelloWorld(context.Background())
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("execHelloWorld() err = %v, want substring %q", err, tt.wantErr)
			}

			// iterate should execute and not panic or crash the runtime
			rt.iterate()

			calls := ctrl.recordedCalls()
			// Control-plane lifecycle should still execute normally
			if !slices.Contains(calls, "ResumeActor") || !slices.Contains(calls, "SuspendActor") {
				t.Errorf("expected ResumeActor and SuspendActor in calls, got: %v", calls)
			}
			// A single exec failure is below maxConsecutiveFailures, so actor is not replaced yet.
			if slices.Contains(calls, "DeleteActor") {
				t.Errorf("did not expect DeleteActor on single exec failure, got: %v", calls)
			}
		})
	}
}

func TestAteEnvExec_Timeout(t *testing.T) {
	ctrl := &fakeControlClient{}
	srv := &fakeProcessServer{blockStream: true}
	rt, fix := newTestRuntime(t, ctrl, srv, dynconfig.LifecycleModeSync)

	actor := &ateEnvActor{
		cfg:           rt.cfg,
		actorName:     "sb-timeout",
		processClient: fix.client,
		execTimeout:   20 * time.Millisecond,
	}
	err := actor.execHelloWorld(context.Background())
	if err == nil || status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("execHelloWorld() on hung stream err = %v, want DeadlineExceeded", err)
	}
}

func TestAteEnvIterate_RetriesStrandedHibernate(t *testing.T) {
	ctrl := &fakeControlClient{
		suspendErrs: []error{status.Error(codes.Unavailable, "ate-api-server restarting")},
	}
	srv := &fakeProcessServer{}
	rt, _ := newTestRuntime(t, ctrl, srv, dynconfig.LifecycleModeSync)

	rt.iterate() // resume, exec, suspend -> suspend fails
	before := ctrl.recordedCalls()
	rt.iterate() // must re-drive suspend, not resume

	after := ctrl.recordedCalls()
	if got := after[len(after)-1]; got != "SuspendActor" {
		t.Errorf("last call = %q, want SuspendActor; calls = %v", got, after)
	}
	resumeBefore := countCalls(before, "ResumeActor")
	resumeAfter := countCalls(after, "ResumeActor")
	if resumeBefore != resumeAfter {
		t.Errorf("ResumeActor count increased from %d to %d (stranded actor should not resume)", resumeBefore, resumeAfter)
	}
}

func TestAteEnvIterate_ReplacesActorOnTerminalResumeFailure(t *testing.T) {
	ctrl := &fakeControlClient{
		resumeErrs: []error{status.Error(codes.FailedPrecondition, "AssignWorker prerequisite not met")},
	}
	srv := &fakeProcessServer{}
	rt, _ := newTestRuntime(t, ctrl, srv, dynconfig.LifecycleModeSync)

	rt.iterate()

	calls := ctrl.recordedCalls()
	if got := countCalls(calls, "CreateActor"); got != 2 {
		t.Errorf("CreateActor count = %d, want 2 (initial + replacement); calls = %v", got, calls)
	}
	if got := countCalls(calls, "DeleteActor"); got != 1 {
		t.Errorf("DeleteActor count = %d, want 1; calls = %v", got, calls)
	}
}

func TestAteEnvIterate_ReplacesActorOnRepeatedExecFailures(t *testing.T) {
	ctrl := &fakeControlClient{}
	srv := &fakeProcessServer{
		startErr: status.Error(codes.Unavailable, "guest wedged"),
	}
	rt, _ := newTestRuntime(t, ctrl, srv, dynconfig.LifecycleModeSync)

	for i := 0; i < maxConsecutiveFailures; i++ {
		rt.iterate()
	}

	calls := ctrl.recordedCalls()
	if got := countCalls(calls, "CreateActor"); got != 2 {
		t.Errorf("CreateActor count = %d, want 2 (initial + replacement); calls = %v", got, calls)
	}
	if got := countCalls(calls, "SuspendActor"); got != maxConsecutiveFailures {
		t.Errorf("SuspendActor count = %d, want %d (hibernated before replacement); calls = %v", got, maxConsecutiveFailures, calls)
	}
	if got := countCalls(calls, "DeleteActor"); got != 1 {
		t.Errorf("DeleteActor count = %d, want 1; calls = %v", got, calls)
	}
}

func TestAteEnvIterate_CountsAtMostOneExecFailurePerWake(t *testing.T) {
	ctrl := &fakeControlClient{}
	srv := &fakeProcessServer{
		startErr: status.Error(codes.Unavailable, "transient router blip"),
	}
	rt, _ := newTestRuntime(t, ctrl, srv, dynconfig.LifecycleModeSync)
	rt.cfg.Dyn = dynconfig.Static(ateEnvKnobs{
		Lifecycle:       dynconfig.Lifecycle{LifecycleMode: dynconfig.LifecycleModeSync},
		MinLive:         dynconfig.Seconds(5 * time.Second),
		MaxLive:         dynconfig.Seconds(5 * time.Second),
		MaxPingsPerWake: maxConsecutiveFailures,
	})
	rt.sleepFn = func(time.Duration) bool { return true }

	// A single wake with MaxPingsPerWake == maxConsecutiveFailures failed pings
	// must only count as 1 failed wake, not replace the actor immediately.
	rt.iterate()

	calls := ctrl.recordedCalls()
	if got := countCalls(calls, "CreateActor"); got != 1 {
		t.Errorf("CreateActor count after 1 wake = %d, want 1; calls = %v", got, calls)
	}
	if got := countCalls(calls, "DeleteActor"); got != 0 {
		t.Errorf("DeleteActor count after 1 wake = %d, want 0; calls = %v", got, calls)
	}
}

func TestAteEnvIterate_DoesNotReplaceOnResourceExhaustedExecFailure(t *testing.T) {
	ctrl := &fakeControlClient{}
	srv := &fakeProcessServer{
		startErr: status.Error(codes.ResourceExhausted, "rate limit exceeded"),
	}
	rt, _ := newTestRuntime(t, ctrl, srv, dynconfig.LifecycleModeSync)

	for i := 0; i < maxConsecutiveFailures+1; i++ {
		rt.iterate()
	}

	calls := ctrl.recordedCalls()
	if got := countCalls(calls, "CreateActor"); got != 1 {
		t.Errorf("CreateActor count = %d, want 1 (ResourceExhausted should not replace actor); calls = %v", got, calls)
	}
	if got := countCalls(calls, "DeleteActor"); got != 0 {
		t.Errorf("DeleteActor count = %d, want 0; calls = %v", got, calls)
	}
}

func TestAteEnvShutdown_PauseModeRunningActor(t *testing.T) {
	ctrl := &fakeControlClient{}
	srv := &fakeProcessServer{}
	rt, fix := newTestRuntime(t, ctrl, srv, dynconfig.LifecycleModeAsync)

	u := &ateEnvUser{
		routerConn: fix.closerTracker,
		actors: []*ateEnvActor{{
			cfg:           rt.cfg,
			actorName:     "running-actor",
			actorRunning:  true,
			processClient: fix.client,
		}},
	}
	rt.users.Store(boomerutil.GoroutineID(), u)

	rt.shutdown(context.Background())

	calls := ctrl.recordedCalls()
	if len(calls) != 2 || calls[0] != "SuspendActor(async)" || calls[1] != "DeleteActor" {
		t.Errorf("recordedCalls = %v, want [SuspendActor(async) DeleteActor]", calls)
	}
	if !fix.closerTracker.isClosed() {
		t.Errorf("router gRPC connection was not closed on shutdown")
	}
}

func TestAteEnvShutdown_StoppedActor(t *testing.T) {
	ctrl := &fakeControlClient{}
	srv := &fakeProcessServer{}
	rt, fix := newTestRuntime(t, ctrl, srv, dynconfig.LifecycleModeSync)

	u := &ateEnvUser{
		routerConn: fix.closerTracker,
		actors: []*ateEnvActor{{
			cfg:           rt.cfg,
			actorName:     "stopped-actor",
			actorRunning:  false,
			processClient: fix.client,
		}},
	}
	rt.users.Store(boomerutil.GoroutineID(), u)

	rt.shutdown(context.Background())

	calls := ctrl.recordedCalls()
	if len(calls) != 1 || calls[0] != "DeleteActor" {
		t.Errorf("recordedCalls = %v, want [DeleteActor]", calls)
	}
	if !fix.closerTracker.isClosed() {
		t.Errorf("router gRPC connection was not closed on shutdown")
	}
}

func TestAteEnvShutdown_WaitsForInFlightIterate(t *testing.T) {
	ctrl := &fakeControlClient{}
	srv := &fakeProcessServer{}
	rt, fix := newTestRuntime(t, ctrl, srv, dynconfig.LifecycleModeSync)
	rt.cfg.Dyn = dynconfig.Static(ateEnvKnobs{
		Lifecycle:       dynconfig.Lifecycle{LifecycleMode: dynconfig.LifecycleModeSync},
		WaitTime:        dynconfig.WaitTime{MinWait: dynconfig.Seconds(5 * time.Second), MaxWait: dynconfig.Seconds(5 * time.Second)},
		MinLive:         dynconfig.Seconds(5 * time.Second),
		MaxLive:         dynconfig.Seconds(5 * time.Second),
		MaxPingsPerWake: 1,
	})

	iterDone := make(chan struct{})
	go func() {
		defer close(iterDone)
		rt.iterate()
	}()

	// Wait until StartProcess has been invoked inside iterate().
	deadline := time.Now().Add(2 * time.Second)
	for {
		fix.srv.mu.Lock()
		n := len(fix.srv.startReqs)
		fix.srv.mu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for iterate() to reach StartProcess")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// shutdown should interrupt the 5s liveWait/dynamicWait sleep, wait for
	// iterate()'s SuspendActor to finish, and then DeleteActor once without
	// triggering any replacement CreateActor.
	start := time.Now()
	rt.shutdown(context.Background())
	<-iterDone
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("shutdown took %v, expected live/wait sleeps to be interrupted", elapsed)
	}

	calls := ctrl.recordedCalls()
	if got := countCalls(calls, "CreateActor"); got != 1 {
		t.Errorf("CreateActor count = %d, want 1; calls = %v", got, calls)
	}
	if got := countCalls(calls, "SuspendActor"); got != 1 {
		t.Errorf("SuspendActor count = %d, want 1; calls = %v", got, calls)
	}
	if got := countCalls(calls, "DeleteActor"); got != 1 {
		t.Errorf("DeleteActor count = %d, want 1; calls = %v", got, calls)
	}
}

func TestAteEnvShutdown_RespectsContextCancellation(t *testing.T) {
	ctrl := &fakeControlClient{}
	srv := &fakeProcessServer{blockStream: true}
	rt, fix := newTestRuntime(t, ctrl, srv, dynconfig.LifecycleModeSync)

	iterDone := make(chan struct{})
	go func() {
		defer close(iterDone)
		rt.iterate()
	}()

	// Wait until StreamProcessOutput is blocked inside iterate().
	deadline := time.Now().Add(2 * time.Second)
	for {
		fix.srv.mu.Lock()
		n := len(fix.srv.streamReqs)
		fix.srv.mu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for iterate() to block in StreamProcessOutput")
		}
		time.Sleep(5 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	rt.shutdown(ctx)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("shutdown ignored ctx cancellation and took %v", elapsed)
	}
	// Closing routerConn during shutdown unblocks the stuck stream so iterate()
	// also exits cleanly.
	<-iterDone

	calls := ctrl.recordedCalls()
	if !slices.Contains(calls, "SuspendActor") {
		t.Errorf("expected SuspendActor to succeed before ctx expired, got: %v", calls)
	}
	if !slices.Contains(calls, "DeleteActor") {
		t.Errorf("expected DeleteActor to succeed before ctx expired, got: %v", calls)
	}
}

func countCalls(calls []string, name string) int {
	n := 0
	for _, c := range calls {
		if c == name {
			n++
		}
	}
	return n
}
