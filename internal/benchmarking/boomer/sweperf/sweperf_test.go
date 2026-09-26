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

package sweperf

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/atenet"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/boomerutil"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// statKey identifies one locust_request_duration_milliseconds series.
type statKey struct {
	name   string
	status string
}

// statSample is what that series has observed.
type statSample struct {
	count uint64
	sumMs float64
}

// readStats snapshots every stats row bmetrics has recorded. The registry is
// global to the test binary, so a test reads it twice and compares.
func readStats(t *testing.T) map[statKey]statSample {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	out := map[statKey]statSample{}
	for _, mf := range families {
		if mf.GetName() != "locust_request_duration_milliseconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			var key statKey
			for _, lp := range m.GetLabel() {
				switch lp.GetName() {
				case "name":
					key.name = lp.GetValue()
				case "status":
					key.status = lp.GetValue()
				}
			}
			h := m.GetHistogram()
			out[key] = statSample{count: h.GetSampleCount(), sumMs: h.GetSampleSum()}
		}
	}
	return out
}

// recordedSince returns what one row observed after the given snapshot.
func recordedSince(t *testing.T, before map[statKey]statSample, name, status string) statSample {
	t.Helper()
	key := statKey{name: name, status: status}
	now := readStats(t)[key]
	was := before[key]
	return statSample{count: now.count - was.count, sumMs: now.sumMs - was.sumMs}
}

type fakeControlClient struct {
	ateapipb.ControlClient
	mu             sync.Mutex
	calls          []string
	createSpaceErr error
	createActorErr error
	resumeErr      error
	suspendErr     error
	deleteErr      error
	// Make ResumeActor take time and report a smaller server-side elapsed.
	resumeDelay     time.Duration
	resumeElapsedUs string
	// Report SUSPENDING for the first suspendedAfter GetActor calls.
	suspendedAfter int
	getActorCalls  int
	// AnyState carried by the most recent DeleteActor request.
	deleteAnyState bool
}

func (f *fakeControlClient) CreateAtespace(ctx context.Context, in *ateapipb.CreateAtespaceRequest, opts ...grpc.CallOption) (*ateapipb.Atespace, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "CreateAtespace")
	if f.createSpaceErr != nil {
		return nil, f.createSpaceErr
	}
	return &ateapipb.Atespace{}, nil
}

func (f *fakeControlClient) CreateActor(ctx context.Context, in *ateapipb.CreateActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "CreateActor")
	if f.createActorErr != nil {
		return nil, f.createActorErr
	}
	return &ateapipb.Actor{}, nil
}

func (f *fakeControlClient) ResumeActor(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "ResumeActor")
	time.Sleep(f.resumeDelay)
	if f.resumeElapsedUs != "" {
		setTrailer(opts, metadata.Pairs(ateinterceptors.ServerElapsedTrailer, f.resumeElapsedUs))
	}
	if f.resumeErr != nil {
		return nil, f.resumeErr
	}
	return &ateapipb.ResumeActorResponse{}, nil
}

// setTrailer fills the metadata that grpc.Trailer asked the call to populate.
func setTrailer(opts []grpc.CallOption, md metadata.MD) {
	for _, o := range opts {
		if to, ok := o.(grpc.TrailerCallOption); ok {
			*to.TrailerAddr = md
		}
	}
}

func (f *fakeControlClient) SuspendActor(ctx context.Context, in *ateapipb.SuspendActorRequest, opts ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "SuspendActor")
	if f.suspendErr != nil {
		return nil, f.suspendErr
	}
	return &ateapipb.SuspendActorResponse{}, nil
}

func (f *fakeControlClient) GetActor(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "GetActor")
	state := ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	if f.getActorCalls < f.suspendedAfter {
		state = ateapipb.ActorState_ACTOR_STATE_SUSPENDING
	}
	f.getActorCalls++
	return &ateapipb.Actor{Status: &ateapipb.ActorStatus{State: state}}, nil
}

func (f *fakeControlClient) DeleteActor(ctx context.Context, in *ateapipb.DeleteActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "DeleteActor")
	f.deleteAnyState = in.GetAnyState()
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	return &ateapipb.Actor{}, nil
}

func (f *fakeControlClient) recordedCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func newTestConfig(t *testing.T, handler http.Handler) (*userclass.Config, *httptest.Server, *fakeControlClient) {
	t.Helper()
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	fakeCtrl := &fakeControlClient{}
	cfg := &userclass.Config{
		APIStub:    fakeCtrl,
		HTTPClient: ts.Client(),
		RouterURL:  ts.URL,
		Atespace:   "benchmark-test",
		Dyn:        dynconfig.NewHolder(dynconfig.Config{}),
		Tracer:     otel.Tracer("test-sweperf"),
	}
	return cfg, ts, fakeCtrl
}

func TestActorRoutingHeader(t *testing.T) {
	var mu sync.Mutex
	var gotHeaders []string

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotHeaders = append(gotHeaders, r.Header.Get(atenet.TargetActorHeader))
		mu.Unlock()

		if r.URL.Path == "/status" {
			json.NewEncoder(w).Encode(statusResponse{Status: "up"})
			return
		}
		exitCode := 0
		json.NewEncoder(w).Encode(executeResponse{Status: "COMPLETED", ExitCode: exitCode})
	})

	cfg, _, _ := newTestConfig(t, handler)
	u := &sweperfUser{cfg: cfg, actorName: "test-actor", userClass: sweperfUserClass}

	if err := u.pollLiveness(context.Background()); err != nil {
		t.Fatalf("pollLiveness: %v", err)
	}
	if _, _, err := u.execute(context.Background(), 1, 0, 5); err != nil {
		t.Fatalf("execute: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(gotHeaders) == 0 {
		t.Fatal("no requests reached the router")
	}
	want := "benchmark-test/test-actor"
	for i, got := range gotHeaders {
		if got != want {
			t.Errorf("request %d: %s = %q, want %q", i, atenet.TargetActorHeader, got, want)
		}
	}
}

func TestGenerateDynamicChunks(t *testing.T) {
	tests := []struct {
		name       string
		totalSteps int
		numCycles  int
		want       []chunk
	}{
		{
			name:       "default 21 steps into 4 cycles",
			totalSteps: 21,
			numCycles:  4,
			want:       []chunk{{0, 6}, {6, 11}, {11, 16}, {16, 21}},
		},
		{
			name:       "single cycle",
			totalSteps: 10,
			numCycles:  1,
			want:       []chunk{{0, 10}},
		},
		{
			name:       "numCycles greater than totalSteps",
			totalSteps: 2,
			numCycles:  5,
			want:       []chunk{{0, 1}, {1, 2}},
		},
		{
			name:       "zero steps",
			totalSteps: 0,
			numCycles:  1,
			want:       []chunk{{0, 0}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := generateDynamicChunks(tt.totalSteps, tt.numCycles)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("generateDynamicChunks(%d, %d) = %v, want %v", tt.totalSteps, tt.numCycles, got, tt.want)
			}
		})
	}
}

func TestResolveConfig(t *testing.T) {
	t.Run("default fallback values", func(t *testing.T) {
		rt := &sweperfRuntime{
			cfg: &userclass.Config{
				Dyn: dynconfig.NewHolder(dynconfig.Config{}),
			},
		}
		tmpl, steps, cycles := rt.resolveConfig()
		if tmpl != defaultSweperfTemplate {
			t.Errorf("template = %q, want %q", tmpl, defaultSweperfTemplate)
		}
		if steps != defaultSweperfTotalSteps {
			t.Errorf("totalSteps = %d, want %d", steps, defaultSweperfTotalSteps)
		}
		if cycles != defaultSweperfNumCycles {
			t.Errorf("numCycles = %d, want %d", cycles, defaultSweperfNumCycles)
		}
	})

	t.Run("dynamic config overrides", func(t *testing.T) {
		rt := &sweperfRuntime{
			cfg: &userclass.Config{
				Dyn: dynconfig.NewHolder(dynconfig.Config{
					SweperfTemplate:   "custom-template",
					SweperfTotalSteps: 50,
					SweperfNumCycles:  5,
				}),
			},
		}
		tmpl, steps, cycles := rt.resolveConfig()
		if tmpl != "custom-template" {
			t.Errorf("template = %q, want %q", tmpl, "custom-template")
		}
		if steps != 50 {
			t.Errorf("totalSteps = %d, want 50", steps)
		}
		if cycles != 5 {
			t.Errorf("numCycles = %d, want 5", cycles)
		}
	})
}

func TestSweperfUserCycleSequence(t *testing.T) {
	var httpCalls []string
	var mu sync.Mutex

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		httpCalls = append(httpCalls, r.URL.Path+"?"+r.URL.RawQuery)
		mu.Unlock()

		switch r.URL.Path {
		case "/status":
			jobID := r.URL.Query().Get("job_id")
			if jobID == "" {
				// Liveness probe
				json.NewEncoder(w).Encode(statusResponse{Status: "up"})
			} else {
				// Job completion probe
				exitCode := 0
				json.NewEncoder(w).Encode(jobStatusResponse{
					JobID:    jobID,
					Status:   "COMPLETED",
					ExitCode: &exitCode,
				})
			}
		case "/execute":
			var req executeRequest
			json.NewDecoder(r.Body).Decode(&req)
			json.NewEncoder(w).Encode(executeResponse{
				JobID:  fmt.Sprintf("job-%d-%d", req.StartStep, req.EndStep),
				Status: "RUNNING",
			})
		default:
			http.NotFound(w, r)
		}
	})

	cfg, _, fakeCtrl := newTestConfig(t, handler)
	u := &sweperfUser{
		cfg:          cfg,
		actorName:    "test-actor",
		templateName: defaultSweperfTemplate,
		userClass:    sweperfUserClass,
		chunks:       []chunk{{0, 5}, {5, 10}},
		cycleIndex:   0,
	}

	// Run step 1
	u.step(context.Background())

	if u.cycleIndex != 1 {
		t.Errorf("cycleIndex = %d, want 1", u.cycleIndex)
	}

	grpcCalls := fakeCtrl.recordedCalls()
	wantGRPCCalls := []string{"ResumeActor", "SuspendActor"}
	if !reflect.DeepEqual(grpcCalls, wantGRPCCalls) {
		t.Errorf("gRPC calls: got %v, want %v", grpcCalls, wantGRPCCalls)
	}

	mu.Lock()
	defer mu.Unlock()
	wantHTTPCalls := []string{"/execute?", "/status?job_id=job-1-5"}
	if !reflect.DeepEqual(httpCalls, wantHTTPCalls) {
		t.Errorf("HTTP calls: got %v, want %v", httpCalls, wantHTTPCalls)
	}
}

func TestEnsureAtespaceHandling(t *testing.T) {
	t.Run("treats AlreadyExists error as success", func(t *testing.T) {
		cfg, _, fakeCtrl := newTestConfig(t, http.HandlerFunc(nil))
		fakeCtrl.createSpaceErr = status.Error(codes.AlreadyExists, "atespace already exists")
		u := &sweperfUser{cfg: cfg, actorName: "act"}

		if err := u.ensureAtespace(context.Background()); err != nil {
			t.Errorf("ensureAtespace failed on AlreadyExists error: %v", err)
		}
	})

	t.Run("returns unexpected gRPC error", func(t *testing.T) {
		cfg, _, fakeCtrl := newTestConfig(t, http.HandlerFunc(nil))
		fakeCtrl.createSpaceErr = status.Error(codes.Internal, "internal database error")
		u := &sweperfUser{cfg: cfg, actorName: "act"}

		if err := u.ensureAtespace(context.Background()); err == nil {
			t.Errorf("ensureAtespace expected error, got nil")
		}
	})
}

func TestControlClientErrors(t *testing.T) {
	t.Run("ResumeActor error returns false", func(t *testing.T) {
		cfg, _, fakeCtrl := newTestConfig(t, http.HandlerFunc(nil))
		fakeCtrl.resumeErr = errors.New("resume RPC failed")
		u := &sweperfUser{cfg: cfg, actorName: "act"}

		if ok := u.resume(context.Background()); ok {
			t.Errorf("resume = true on RPC failure, want false")
		}
	})

	t.Run("SuspendActor and DeleteActor errors handled gracefully", func(t *testing.T) {
		cfg, _, fakeCtrl := newTestConfig(t, http.HandlerFunc(nil))
		fakeCtrl.suspendErr = errors.New("suspend failed")
		fakeCtrl.deleteErr = errors.New("delete failed")
		u := &sweperfUser{cfg: cfg, actorName: "act"}

		// Should not panic or crash
		u.suspend(context.Background())
		u.delete(context.Background())
	})
}

func TestExecuteSyncExitCodeFailure(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(executeResponse{
			JobID:    "",
			Status:   "FAILED",
			ExitCode: 2,
			Stderr:   "command not found",
		})
	})
	cfg, _, _ := newTestConfig(t, handler)
	u := &sweperfUser{cfg: cfg, actorName: "act"}

	_, _, err := u.execute(context.Background(), 1, 0, 5)
	if err == nil {
		t.Fatalf("execute expected error on non-zero exit code, got nil")
	}
	wantSubstring := "exit code 2, stderr: command not found"
	if !strings.Contains(err.Error(), wantSubstring) {
		t.Errorf("execute error = %q, want substring %q", err.Error(), wantSubstring)
	}
}

func TestExecuteHTTPStatusError(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	})
	cfg, _, _ := newTestConfig(t, handler)
	u := &sweperfUser{cfg: cfg, actorName: "act"}

	_, _, err := u.execute(context.Background(), 1, 0, 5)
	if err == nil {
		t.Fatalf("execute expected error on HTTP 500, got nil")
	}
}

func TestSweperfPollLiveness(t *testing.T) {
	t.Run("liveness succeeds when server is up", func(t *testing.T) {
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(statusResponse{Status: "up"})
		})
		cfg, _, _ := newTestConfig(t, handler)
		u := &sweperfUser{cfg: cfg, actorName: "act"}

		if err := u.pollLiveness(context.Background()); err != nil {
			t.Errorf("pollLiveness failed unexpectedly: %v", err)
		}
	})

	t.Run("liveness fails when context is canceled", func(t *testing.T) {
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		})
		cfg, _, _ := newTestConfig(t, handler)
		u := &sweperfUser{cfg: cfg, actorName: "act"}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		if err := u.pollLiveness(ctx); err == nil {
			t.Errorf("pollLiveness expected error on timeout, got nil")
		}
	})
}

func TestSweperfPollJobCompletion(t *testing.T) {
	t.Run("job completes with exit code 0", func(t *testing.T) {
		// Literal payload from replay.py, so a wrong json tag fails here.
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, `{"job_id":"job-1","status":"COMPLETED","exit_code":0,"completed_step":5,"execution_duration_ms":1234.5}`)
		})
		cfg, _, _ := newTestConfig(t, handler)
		u := &sweperfUser{cfg: cfg, actorName: "act"}

		got, err := u.pollJobCompletion(context.Background(), "job-1", 1)
		if err != nil {
			t.Errorf("pollJobCompletion failed unexpectedly: %v", err)
		}
		if want := 1234500 * time.Microsecond; got != want {
			t.Errorf("pollJobCompletion returned %v, want the reported %v", got, want)
		}
	})

	t.Run("job fails with non-zero exit code", func(t *testing.T) {
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			exitCode := 1
			json.NewEncoder(w).Encode(jobStatusResponse{
				JobID:    "job-1",
				Status:   "FAILED",
				ExitCode: &exitCode,
				Error:    "step execution failed",
			})
		})
		cfg, _, _ := newTestConfig(t, handler)
		u := &sweperfUser{cfg: cfg, actorName: "act"}

		if _, err := u.pollJobCompletion(context.Background(), "job-1", 1); err == nil {
			t.Errorf("pollJobCompletion expected error on failed job, got nil")
		}
	})
}

func TestSweperfUserLifecycleAndReset(t *testing.T) {
	u := &sweperfUser{
		chunks:     []chunk{{0, 5}, {5, 10}},
		cycleIndex: 0,
	}

	if u.isDone() {
		t.Errorf("isDone = true, want false")
	}

	u.cycleIndex = 2
	if !u.isDone() {
		t.Errorf("isDone = false, want true")
	}

	u.resetCycles()
	if u.cycleIndex != 0 || u.isDone() {
		t.Errorf("resetCycles failed: cycleIndex = %d, isDone = %v", u.cycleIndex, u.isDone())
	}
}

func TestDynamicWait(t *testing.T) {
	t.Run("returns MinWait when MaxWait <= MinWait", func(t *testing.T) {
		rt := &sweperfRuntime{
			cfg: &userclass.Config{
				Dyn: dynconfig.NewHolder(dynconfig.Config{
					MinWait: 100 * time.Millisecond,
					MaxWait: 50 * time.Millisecond,
				}),
			},
		}
		if got := rt.dynamicWait(); got != 100*time.Millisecond {
			t.Errorf("dynamicWait = %v, want 100ms", got)
		}
	})

	t.Run("returns value in range [MinWait, MaxWait]", func(t *testing.T) {
		minW := 10 * time.Millisecond
		maxW := 50 * time.Millisecond
		rt := &sweperfRuntime{
			cfg: &userclass.Config{
				Dyn: dynconfig.NewHolder(dynconfig.Config{
					MinWait: minW,
					MaxWait: maxW,
				}),
			},
		}
		got := rt.dynamicWait()
		if got < minW || got > maxW {
			t.Errorf("dynamicWait = %v out of range [%v, %v]", got, minW, maxW)
		}
	})
}

func TestInitSweperfAndTaskFn(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status":
			json.NewEncoder(w).Encode(statusResponse{Status: "up"})
		case "/execute":
			json.NewEncoder(w).Encode(executeResponse{JobID: "", Status: "COMPLETED", ExitCode: 0})
		}
	})
	cfg, _, _ := newTestConfig(t, handler)
	cfg.Dyn = dynconfig.NewHolder(dynconfig.Config{
		MinWait: 1 * time.Millisecond,
		MaxWait: 2 * time.Millisecond,
	})

	taskFn, shutdownFn := initSweperf(cfg)
	if taskFn == nil || shutdownFn == nil {
		t.Fatalf("initSweperf returned nil functions")
	}

	// Run taskFn once
	taskFn()

	// Shutdown
	shutdownFn(context.Background())
}

func TestStartUserWaitsForBootstrapSuspend(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(statusResponse{Status: "up"})
	})
	cfg, _, fakeCtrl := newTestConfig(t, handler)
	fakeCtrl.suspendedAfter = 2

	rt := &sweperfRuntime{cfg: cfg}
	if _, err := rt.startUser(context.Background()); err != nil {
		t.Fatalf("startUser: %v", err)
	}

	gets := 0
	for _, c := range fakeCtrl.recordedCalls() {
		if c == "GetActor" {
			gets++
		}
	}
	if gets < 3 {
		t.Errorf("GetActor called %d times, want at least 3; startUser returned before the actor reached SUSPENDED", gets)
	}
}

func TestSweperfBootstrapFailureSuspendsBeforeDelete(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server not ready", http.StatusInternalServerError)
	})
	cfg, _, fakeCtrl := newTestConfig(t, handler)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	rt := &sweperfRuntime{cfg: cfg}
	_, err := rt.startUser(ctx)
	if err == nil {
		t.Fatalf("startUser expected error on liveness failure, got nil")
	}

	calls := fakeCtrl.recordedCalls()
	if len(calls) < 2 || calls[len(calls)-2] != "SuspendActor" || calls[len(calls)-1] != "DeleteActor" {
		t.Errorf("recordedCalls must end with [SuspendActor, DeleteActor], got %v", calls)
	}
}

func TestSweperfShutdownSuspendsBeforeDelete(t *testing.T) {
	cfg, _, fakeCtrl := newTestConfig(t, http.HandlerFunc(nil))

	u := &sweperfUser{
		cfg:       cfg,
		actorName: "swe-actor",
		userClass: sweperfUserClass,
	}

	rt := &sweperfRuntime{cfg: cfg}
	rt.users.Store("goroutine-1", u)
	rt.shutdown(context.Background())

	calls := fakeCtrl.recordedCalls()
	if len(calls) < 2 || calls[len(calls)-2] != "SuspendActor" || calls[len(calls)-1] != "DeleteActor" {
		t.Errorf("recordedCalls must end with [SuspendActor, DeleteActor], got %v", calls)
	}
	// Without AnyState the server refuses to delete an actor caught mid-cycle.
	if !fakeCtrl.deleteAnyState {
		t.Error("DeleteActor sent AnyState=false; an actor not yet SUSPENDED would leak its worker")
	}
}

func TestTracedCallRecordsBothLatencies(t *testing.T) {
	cfg, _, fakeCtrl := newTestConfig(t, http.HandlerFunc(nil))
	// 30ms on the wire, 5ms reported by the server.
	fakeCtrl.resumeDelay = 30 * time.Millisecond
	fakeCtrl.resumeElapsedUs = "5000"
	u := &sweperfUser{cfg: cfg, actorName: "act", userClass: sweperfUserClass}

	before := readStats(t)
	if ok := u.resume(context.Background()); !ok {
		t.Fatalf("resume failed")
	}

	server := recordedSince(t, before, "ResumeActor", "success")
	client := recordedSince(t, before, "ResumeActor_rtt", "success")

	if server.count != 1 || client.count != 1 {
		t.Fatalf("want one sample each, got ResumeActor=%d ResumeActor_rtt=%d", server.count, client.count)
	}
	if server.sumMs != 5 {
		t.Errorf("ResumeActor = %vms, want the 5ms from the trailer", server.sumMs)
	}
	if client.sumMs < 30 {
		t.Errorf("ResumeActor_rtt = %vms, want at least the 30ms spent on the wire", client.sumMs)
	}
}

func TestTracedCallSkipsRTTRowWithoutTrailer(t *testing.T) {
	cfg, _, _ := newTestConfig(t, http.HandlerFunc(nil))
	u := &sweperfUser{cfg: cfg, actorName: "act", userClass: sweperfUserClass}

	before := readStats(t)
	if ok := u.resume(context.Background()); !ok {
		t.Fatalf("resume failed")
	}

	// Without a trailer both figures are the client's, so the second row
	// would only duplicate the first.
	if got := recordedSince(t, before, "ResumeActor_rtt", "success"); got.count != 0 {
		t.Errorf("ResumeActor_rtt recorded %d samples, want none without a trailer", got.count)
	}
}

func TestResumeToFirstExecStopsBeforeJobPolling(t *testing.T) {
	const pollDelay = 60 * time.Millisecond
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/execute":
			// Accepted at once; the work happens in the background.
			json.NewEncoder(w).Encode(executeResponse{JobID: "job-1", Status: "ACCEPTED"})
		case r.URL.Path == "/status" && r.URL.Query().Get("job_id") != "":
			time.Sleep(pollDelay)
			exitCode := 0
			json.NewEncoder(w).Encode(jobStatusResponse{
				JobID: "job-1", Status: "COMPLETED", ExitCode: &exitCode,
			})
		default:
			http.NotFound(w, r)
		}
	})
	cfg, _, _ := newTestConfig(t, handler)
	u := &sweperfUser{
		cfg:       cfg,
		actorName: "act",
		userClass: sweperfUserClass,
		chunks:    []chunk{{0, 5}},
	}

	before := readStats(t)
	u.step(context.Background())

	ttfe := recordedSince(t, before, "ResumeToFirstExec", "success")
	cycle := recordedSince(t, before, "Workload_Cycle_1", "success")

	if ttfe.count != 1 {
		t.Fatalf("ResumeToFirstExec recorded %d samples, want 1", ttfe.count)
	}
	if cycle.sumMs < float64(pollDelay.Milliseconds()) {
		t.Fatalf("Workload_Cycle_1 = %vms, want at least the %v spent polling", cycle.sumMs, pollDelay)
	}
	if ttfe.sumMs >= cycle.sumMs {
		t.Errorf("ResumeToFirstExec = %vms, want well under Workload_Cycle_1 = %vms; it is timing the workload",
			ttfe.sumMs, cycle.sumMs)
	}
}

func TestTaskCELOnlyOnFullTrajectory(t *testing.T) {
	// Chunk 1 reports 1200ms of container time, chunk 2 reports 1800ms.
	durations := map[string]string{"job-1": "1200", "job-2": "1800"}
	cycle := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/execute":
			cycle++
			fmt.Fprintf(w, `{"job_id":"job-%d","status":"ACCEPTED"}`, cycle)
		case "/status":
			jobID := r.URL.Query().Get("job_id")
			fmt.Fprintf(w, `{"job_id":%q,"status":"COMPLETED","exit_code":0,"execution_duration_ms":%s}`,
				jobID, durations[jobID])
		default:
			http.NotFound(w, r)
		}
	})
	cfg, _, _ := newTestConfig(t, handler)
	u := &sweperfUser{
		cfg:       cfg,
		actorName: "act",
		userClass: sweperfUserClass,
		chunks:    []chunk{{0, 5}, {5, 10}},
	}

	before := readStats(t)

	u.step(context.Background())
	if u.isDone() {
		t.Fatalf("isDone after 1 of 2 cycles")
	}
	if want := 1200 * time.Millisecond; u.loopCEL != want {
		t.Fatalf("loopCEL = %v after one cycle, want the reported %v", u.loopCEL, want)
	}
	if got := recordedSince(t, before, "TaskCEL", "success"); got.count != 0 {
		t.Fatalf("TaskCEL recorded %d samples mid-trajectory, want none", got.count)
	}

	u.step(context.Background())
	if !u.isDone() {
		t.Fatalf("isDone false after both cycles")
	}
	if want := 3000 * time.Millisecond; u.loopCEL != want {
		t.Fatalf("loopCEL = %v after two cycles, want the summed %v", u.loopCEL, want)
	}

	if got := recordedSince(t, before, "CycleCEL", "success"); got.count != 2 || got.sumMs != 3000 {
		t.Errorf("CycleCEL recorded %d samples totalling %vms, want 2 totalling 3000", got.count, got.sumMs)
	}

	wall := u.loopWall
	u.recordTaskMetrics()
	got := recordedSince(t, before, "TaskCEL", "success")
	if got.count != 1 {
		t.Fatalf("TaskCEL recorded %d samples, want 1", got.count)
	}
	if got.sumMs != 3000 {
		t.Errorf("TaskCEL = %vms, want the summed 3000ms", got.sumMs)
	}
	if gotWall := recordedSince(t, before, "TaskWallClock", "success"); gotWall.count != 1 ||
		gotWall.sumMs != float64(wall.Milliseconds()) {
		t.Errorf("TaskWallClock recorded %d samples totalling %vms, want 1 totalling %vms",
			gotWall.count, gotWall.sumMs, wall.Milliseconds())
	}
	if u.loopCEL != 0 || u.loopWall != 0 || u.loopFailed {
		t.Errorf("recordTaskMetrics left loopCEL=%v loopWall=%v loopFailed=%v, want them rearmed",
			u.loopCEL, u.loopWall, u.loopFailed)
	}
}

func TestCycleCELUsesContainerDurationNotWallClock(t *testing.T) {
	// Two RUNNING polls at 200ms each, so wall clock dwarfs the reported 50ms.
	polls := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/execute":
			fmt.Fprint(w, `{"job_id":"job-1","status":"ACCEPTED"}`)
		case "/status":
			polls++
			if polls <= 2 {
				fmt.Fprint(w, `{"job_id":"job-1","status":"RUNNING"}`)
				return
			}
			fmt.Fprint(w, `{"job_id":"job-1","status":"COMPLETED","exit_code":0,"execution_duration_ms":50}`)
		default:
			http.NotFound(w, r)
		}
	})
	cfg, _, _ := newTestConfig(t, handler)
	u := &sweperfUser{
		cfg:       cfg,
		actorName: "act",
		userClass: sweperfUserClass,
		chunks:    []chunk{{0, 5}},
	}

	before := readStats(t)
	u.step(context.Background())

	if want := 50 * time.Millisecond; u.loopCEL != want {
		t.Errorf("loopCEL = %v, want the reported %v; it is timing the client", u.loopCEL, want)
	}
	if got := recordedSince(t, before, "CycleCEL", "success"); got.count != 1 || got.sumMs != 50 {
		t.Errorf("CycleCEL recorded %d samples totalling %vms, want 1 totalling 50", got.count, got.sumMs)
	}
	if u.loopWall < 400*time.Millisecond {
		t.Errorf("loopWall = %v, want at least the 400ms spent polling", u.loopWall)
	}
}

func TestIterateEmitsTaskMetricsOnlyOnLastCycle(t *testing.T) {
	cycle := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/execute":
			cycle++
			fmt.Fprintf(w, `{"job_id":"job-%d","status":"ACCEPTED"}`, cycle)
		case "/status":
			fmt.Fprint(w, `{"job_id":"job-1","status":"COMPLETED","exit_code":0,"execution_duration_ms":100}`)
		default:
			http.NotFound(w, r)
		}
	})
	cfg, _, _ := newTestConfig(t, handler)
	cfg.Dyn = dynconfig.NewHolder(dynconfig.Config{})
	r := &sweperfRuntime{cfg: cfg}
	// Preloaded so iterate() skips startUser and runs the cycles directly.
	r.users.Store(boomerutil.GoroutineID(), &sweperfUser{
		cfg:       cfg,
		actorName: "act",
		userClass: sweperfUserClass,
		chunks:    []chunk{{0, 5}, {5, 10}},
	})

	before := readStats(t)

	r.iterate()
	if got := recordedSince(t, before, "TaskCEL", "success"); got.count != 0 {
		t.Fatalf("TaskCEL recorded %d samples after cycle 1 of 2, want none", got.count)
	}

	r.iterate()
	if got := recordedSince(t, before, "TaskCEL", "success"); got.count != 1 {
		t.Errorf("TaskCEL recorded %d samples after the full trajectory, want 1", got.count)
	}
	if got := recordedSince(t, before, "TaskWallClock", "success"); got.count != 1 {
		t.Errorf("TaskWallClock recorded %d samples after the full trajectory, want 1", got.count)
	}
}
