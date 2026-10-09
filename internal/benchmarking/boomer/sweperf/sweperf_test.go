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
			prev := out[key]
			out[key] = statSample{count: prev.count + h.GetSampleCount(), sumMs: prev.sumMs + h.GetSampleSum()}
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

func TestPollInterval(t *testing.T) {
	for _, tt := range []struct {
		ms   int
		want time.Duration
	}{
		{0, defaultSweperfPollInterval},
		{250, 250 * time.Millisecond},
	} {
		cfg := &userclass.Config{Dyn: dynconfig.NewHolder(dynconfig.Config{SweperfPollIntervalMs: tt.ms})}
		if got := pollInterval(cfg); got != tt.want {
			t.Errorf("pollInterval(%d) = %v, want %v", tt.ms, got, tt.want)
		}
	}
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

	t.Run("SuspendActor error returns false", func(t *testing.T) {
		cfg, _, fakeCtrl := newTestConfig(t, http.HandlerFunc(nil))
		fakeCtrl.suspendErr = errors.New("suspend failed")
		u := &sweperfUser{cfg: cfg, actorName: "act"}

		if ok := u.suspend(context.Background()); ok {
			t.Errorf("suspend = true on RPC failure, want false")
		}
	})

	t.Run("DeleteActor error handled gracefully", func(t *testing.T) {
		cfg, _, fakeCtrl := newTestConfig(t, http.HandlerFunc(nil))
		fakeCtrl.deleteErr = errors.New("delete failed")
		u := &sweperfUser{cfg: cfg, actorName: "act"}

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

func TestSweperfUserIsDone(t *testing.T) {
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

// asyncJobHandler answers liveness, accepts every /execute and reports the job
// completed.
func asyncJobHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/execute":
			json.NewEncoder(w).Encode(executeResponse{JobID: "job-1", Status: "ACCEPTED"})
		case r.URL.Path == "/status" && r.URL.Query().Get("job_id") != "":
			exitCode := 0
			json.NewEncoder(w).Encode(jobStatusResponse{
				JobID: "job-1", Status: "COMPLETED", ExitCode: &exitCode, ExecutionDurationMs: 1200,
			})
		case r.URL.Path == "/status":
			json.NewEncoder(w).Encode(statusResponse{Status: "up"})
		default:
			http.NotFound(w, r)
		}
	}
}

func TestNoopFirstResumeNotRecorded(t *testing.T) {
	cfg, _, fakeCtrl := newTestConfig(t, asyncJobHandler())
	fakeCtrl.resumeElapsedUs = "5000"
	rt := &sweperfRuntime{cfg: cfg}
	u, err := rt.startUser(context.Background())
	if err != nil {
		t.Fatalf("startUser: %v", err)
	}
	// No extra lifecycle ops at setup: the benchmark flow must match the CUJ.
	if got, want := fakeCtrl.recordedCalls(), []string{"CreateAtespace", "CreateActor"}; !reflect.DeepEqual(got, want) {
		t.Errorf("recordedCalls after startUser = %v, want %v", got, want)
	}
	u.chunks = []chunk{{0, 5}, {5, 10}}
	resumeRows := []string{"ResumeActor", "ResumeActor_rtt", "ResumeToFirstExec"}

	before := readStats(t)
	u.step(context.Background())
	for _, name := range resumeRows {
		if got := recordedSince(t, before, name, "success"); got.count != 0 {
			t.Errorf("cycle 1: %s recorded %d samples, want 0 for the no-op resume", name, got.count)
		}
	}
	if got := recordedSince(t, before, "CycleCEL", "success"); got.count != 1 {
		t.Errorf("cycle 1: CycleCEL recorded %d samples, want 1", got.count)
	}

	before = readStats(t)
	u.step(context.Background())
	for _, name := range resumeRows {
		if got := recordedSince(t, before, name, "success"); got.count != 1 {
			t.Errorf("cycle 2: %s recorded %d samples, want 1", name, got.count)
		}
	}
}

func TestNoopFirstResumeFailureStillRecorded(t *testing.T) {
	cfg, _, fakeCtrl := newTestConfig(t, asyncJobHandler())
	fakeCtrl.resumeErr = errors.New("resume failed")
	u := &sweperfUser{
		cfg:       cfg,
		actorName: "act",
		userClass: sweperfUserClass,
		chunks:    []chunk{{0, 5}},
		awake:     true,
	}

	before := readStats(t)
	u.step(context.Background())
	for _, name := range []string{"ResumeActor", "ResumeToFirstExec"} {
		if got := recordedSince(t, before, name, "failure"); got.count != 1 {
			t.Errorf("%s recorded %d failures, want 1", name, got.count)
		}
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
	r := &sweperfRuntime{cfg: cfg}
	u := &sweperfUser{
		cfg:       cfg,
		actorName: "act",
		userClass: sweperfUserClass,
		chunks:    []chunk{{0, 5}, {5, 10}},
	}
	// Preloaded so iterate() skips startUser and runs the cycles directly.
	r.users.Store(boomerutil.GoroutineID(), u)

	before := readStats(t)

	r.iterate()
	if want := 1200 * time.Millisecond; u.loopCEL != want {
		t.Fatalf("loopCEL = %v after one cycle, want the reported %v", u.loopCEL, want)
	}
	if got := recordedSince(t, before, "TaskCEL", "success"); got.count != 0 {
		t.Fatalf("TaskCEL recorded %d samples mid-trajectory, want none", got.count)
	}

	r.iterate()
	if got := recordedSince(t, before, "CycleCEL", "success"); got.count != 2 || got.sumMs != 3000 {
		t.Errorf("CycleCEL recorded %d samples totalling %vms, want 2 totalling 3000", got.count, got.sumMs)
	}
	got := recordedSince(t, before, "TaskCEL", "success")
	if got.count != 1 {
		t.Fatalf("TaskCEL recorded %d samples, want 1", got.count)
	}
	if got.sumMs != 3000 {
		t.Errorf("TaskCEL = %vms, want the summed 3000ms", got.sumMs)
	}
	if gotWall := recordedSince(t, before, "TaskWallClock", "success"); gotWall.count != 1 {
		t.Errorf("TaskWallClock recorded %d samples, want 1", gotWall.count)
	}
	if u.loopCEL != 0 || u.loopWall != 0 || u.loopFailed {
		t.Errorf("after the task loopCEL=%v loopWall=%v loopFailed=%v, want all rearmed",
			u.loopCEL, u.loopWall, u.loopFailed)
	}
}

func TestFinishedTrajectoryDeletesActorAndStartsFresh(t *testing.T) {
	cfg, _, fakeCtrl := newTestConfig(t, asyncJobHandler())
	r := &sweperfRuntime{cfg: cfg}
	gid := boomerutil.GoroutineID()
	u := &sweperfUser{
		cfg:       cfg,
		actorName: "act-1",
		userClass: sweperfUserClass,
		chunks:    []chunk{{0, 5}},
	}
	r.users.Store(gid, u)

	// The single cycle finishes the trajectory, so the actor must go.
	r.iterate()
	if !u.cleanedUp.Load() {
		t.Errorf("cleanedUp = false after the trajectory finished; the actor was not deleted")
	}
	// The cycle's own suspend is followed straight by the delete; no second suspend.
	if got, want := fakeCtrl.recordedCalls(), []string{"ResumeActor", "SuspendActor", "DeleteActor"}; !reflect.DeepEqual(got, want) {
		t.Errorf("recordedCalls = %v, want %v", got, want)
	}
	if _, ok := r.users.Load(gid); ok {
		t.Fatalf("finished session still bound to its goroutine; the next iterate would reuse it")
	}

	// The next iterate creates a brand-new actor for the next trajectory. The
	// worker already made the atespace, so only CreateActor is sent.
	r.atespaceReady.Store(true)
	before := len(fakeCtrl.recordedCalls())
	beforeStats := readStats(t)
	r.iterate()
	val, ok := r.users.Load(gid)
	if !ok {
		t.Fatalf("no session bound after the next iterate; want a fresh actor")
	}
	next := val.(*sweperfUser)
	if next == u || next.actorName == u.actorName {
		t.Errorf("next session reuses actor %q, want a new one", u.actorName)
	}
	newCalls := fakeCtrl.recordedCalls()[before:]
	if len(newCalls) == 0 || newCalls[0] != "CreateActor" {
		t.Errorf("next iterate calls = %v, want to start with CreateActor and no CreateAtespace", newCalls)
	}
	if got := recordedSince(t, beforeStats, "ActorStartup", "success"); got.count != 1 {
		t.Errorf("ActorStartup recorded %d samples for the new actor, want 1", got.count)
	}
	r.shutdown(context.Background())
}

func TestAtespaceCreatedOncePerWorker(t *testing.T) {
	cfg, _, fakeCtrl := newTestConfig(t, asyncJobHandler())
	r := &sweperfRuntime{cfg: cfg}

	for i := 0; i < 3; i++ {
		u, err := r.startUser(context.Background())
		if err != nil {
			t.Fatalf("startUser %d: %v", i, err)
		}
		u.suspendAndDelete(context.Background())
	}

	n := 0
	for _, c := range fakeCtrl.recordedCalls() {
		if c == "CreateAtespace" {
			n++
		}
	}
	if n != 1 {
		t.Errorf("CreateAtespace sent %d times across 3 sessions, want 1", n)
	}
}

func TestAtespaceRetriedAfterFailure(t *testing.T) {
	cfg, _, fakeCtrl := newTestConfig(t, asyncJobHandler())
	fakeCtrl.createSpaceErr = errors.New("unavailable")
	r := &sweperfRuntime{cfg: cfg}

	if _, err := r.startUser(context.Background()); err == nil {
		t.Fatalf("startUser succeeded despite CreateAtespace failing")
	}
	fakeCtrl.mu.Lock()
	fakeCtrl.createSpaceErr = nil
	fakeCtrl.mu.Unlock()

	u, err := r.startUser(context.Background())
	if err != nil {
		t.Fatalf("startUser after recovery: %v", err)
	}
	defer u.suspendAndDelete(context.Background())

	n := 0
	for _, c := range fakeCtrl.recordedCalls() {
		if c == "CreateAtespace" {
			n++
		}
	}
	if n != 2 {
		t.Errorf("CreateAtespace sent %d times, want 2 (failed attempt, then retry)", n)
	}
}

func TestActorStartupFailureRecorded(t *testing.T) {
	t.Run("liveness failure is recorded", func(t *testing.T) {
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		})
		cfg, _, _ := newTestConfig(t, handler)
		r := &sweperfRuntime{cfg: cfg}
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		before := readStats(t)
		if _, err := r.startUser(ctx); err == nil {
			t.Fatalf("startUser succeeded despite the sandbox never coming up")
		}
		if got := recordedSince(t, before, "ActorStartup", "failure"); got.count != 1 {
			t.Errorf("ActorStartup recorded %d failures, want 1", got.count)
		}
		if got := recordedSince(t, before, "ActorStartup", "success"); got.count != 0 {
			t.Errorf("ActorStartup recorded %d successes on a failed start, want 0", got.count)
		}
	})

	t.Run("CreateActor failure is recorded only on its own row", func(t *testing.T) {
		cfg, _, fakeCtrl := newTestConfig(t, asyncJobHandler())
		fakeCtrl.createActorErr = errors.New("quota exceeded")
		r := &sweperfRuntime{cfg: cfg}

		before := readStats(t)
		if _, err := r.startUser(context.Background()); err == nil {
			t.Fatalf("startUser succeeded despite CreateActor failing")
		}
		if got := recordedSince(t, before, "CreateActor", "failure"); got.count != 1 {
			t.Errorf("CreateActor recorded %d failures, want 1", got.count)
		}
		if got := recordedSince(t, before, "ActorStartup", "failure"); got.count != 0 {
			t.Errorf("ActorStartup recorded %d failures for a failed CreateActor, want 0", got.count)
		}
	})
}

func TestRetireDeleteFailureKeptForShutdown(t *testing.T) {
	cfg, _, fakeCtrl := newTestConfig(t, asyncJobHandler())
	fakeCtrl.deleteErr = errors.New("unavailable")
	r := &sweperfRuntime{cfg: cfg}
	u := &sweperfUser{cfg: cfg, actorName: "act-1", userClass: sweperfUserClass, chunks: []chunk{{0, 5}}}
	r.users.Store(boomerutil.GoroutineID(), u)

	r.iterate()
	if _, ok := r.orphans.Load("act-1"); !ok {
		t.Fatalf("actor whose delete failed was dropped; it would leak past shutdown")
	}

	fakeCtrl.mu.Lock()
	fakeCtrl.deleteErr = nil
	fakeCtrl.mu.Unlock()
	before := len(fakeCtrl.recordedCalls())
	r.shutdown(context.Background())

	if got, want := fakeCtrl.recordedCalls()[before:], []string{"DeleteActor"}; !reflect.DeepEqual(got, want) {
		t.Errorf("shutdown calls = %v, want %v", got, want)
	}
	if _, ok := r.orphans.Load("act-1"); ok {
		t.Errorf("orphan still tracked after a successful delete at shutdown")
	}
}

func TestRetireResuspendsAfterFailedSuspend(t *testing.T) {
	cfg, _, fakeCtrl := newTestConfig(t, asyncJobHandler())
	fakeCtrl.suspendErr = errors.New("suspend failed")
	r := &sweperfRuntime{cfg: cfg}
	u := &sweperfUser{cfg: cfg, actorName: "act-1", userClass: sweperfUserClass, chunks: []chunk{{0, 5}}}
	r.users.Store(boomerutil.GoroutineID(), u)

	r.iterate()

	// The cycle's suspend failed, so the actor may be awake: suspend again before deleting.
	want := []string{"ResumeActor", "SuspendActor", "SuspendActor", "DeleteActor"}
	if got := fakeCtrl.recordedCalls(); !reflect.DeepEqual(got, want) {
		t.Errorf("recordedCalls = %v, want %v", got, want)
	}
}

func TestShutdownAndRetireDeleteOnce(t *testing.T) {
	cfg, _, fakeCtrl := newTestConfig(t, asyncJobHandler())
	r := &sweperfRuntime{cfg: cfg}
	gid := boomerutil.GoroutineID()
	u := &sweperfUser{cfg: cfg, actorName: "act-1", userClass: sweperfUserClass, chunks: []chunk{{0, 5}}}
	r.users.Store(gid, u)

	// Shutdown claims the actor first; a late retire must not delete it again.
	r.shutdown(context.Background())
	r.retireUser(context.Background(), gid, u)

	n := 0
	for _, c := range fakeCtrl.recordedCalls() {
		if c == "DeleteActor" {
			n++
		}
	}
	if n != 1 {
		t.Errorf("DeleteActor sent %d times, want 1", n)
	}
}

func TestCreateActorFailedPreconditionResetsAtespace(t *testing.T) {
	cfg, _, fakeCtrl := newTestConfig(t, asyncJobHandler())
	fakeCtrl.createActorErr = status.Error(codes.FailedPrecondition, "atespace not found")
	r := &sweperfRuntime{cfg: cfg}
	r.atespaceReady.Store(true)

	if _, err := r.startUser(context.Background()); err == nil {
		t.Fatalf("startUser succeeded despite CreateActor failing")
	}
	if r.atespaceReady.Load() {
		t.Errorf("atespaceReady still set after CreateActor returned FailedPrecondition; the atespace would never be re-created")
	}
}

func TestCycleCELUsesContainerDurationNotWallClock(t *testing.T) {
	// Three polls, so wall clock dwarfs the reported 10ms.
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
			fmt.Fprint(w, `{"job_id":"job-1","status":"COMPLETED","exit_code":0,"execution_duration_ms":10}`)
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

	if want := 10 * time.Millisecond; u.loopCEL != want {
		t.Errorf("loopCEL = %v, want the reported %v; it is timing the client", u.loopCEL, want)
	}
	if got := recordedSince(t, before, "CycleCEL", "success"); got.count != 1 || got.sumMs != 10 {
		t.Errorf("CycleCEL recorded %d samples totalling %vms, want 1 totalling 10", got.count, got.sumMs)
	}
	if u.loopWall < 75*time.Millisecond {
		t.Errorf("loopWall = %v, want at least the 75ms spent polling", u.loopWall)
	}
}

func TestStepSuspendFailureFailsTask(t *testing.T) {
	cfg, _, fakeCtrl := newTestConfig(t, asyncJobHandler())
	fakeCtrl.suspendErr = errors.New("suspend failed")
	u := &sweperfUser{
		cfg:       cfg,
		actorName: "act",
		userClass: sweperfUserClass,
		chunks:    []chunk{{0, 5}},
	}

	u.step(context.Background())

	if !u.loopFailed {
		t.Errorf("loopFailed = false after SuspendActor failed; the task would be recorded as a success")
	}
}

func TestStepSyncExitSkipsResumeToFirstExec(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(executeResponse{Status: "FAILED", ExitCode: 2, Stderr: "boom"})
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

	// The resume worked; only the command failed, so there is no row to record.
	for _, result := range []string{"success", "failure"} {
		if got := recordedSince(t, before, "ResumeToFirstExec", result); got.count != 0 {
			t.Errorf("ResumeToFirstExec %s recorded %d samples on a synchronous reply, want none", result, got.count)
		}
	}
	if !u.loopFailed {
		t.Errorf("loopFailed = false after the command exited non-zero")
	}
}

// usersGauge reads the locust_users gauge for the sweperf user class. The
// registry is global to the test binary, so tests using it must not run in
// parallel.
func usersGauge(t *testing.T) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "locust_users" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "user_class" && lp.GetValue() == sweperfUserClass {
					return m.GetGauge().GetValue()
				}
			}
		}
	}
	return 0
}

func TestPollLivenessFailureDecrementsUsersOnce(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server not ready", http.StatusInternalServerError)
	})
	cfg, _, _ := newTestConfig(t, handler)
	r := &sweperfRuntime{cfg: cfg}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	before := usersGauge(t)
	if _, err := r.startUser(ctx); err == nil {
		t.Fatalf("startUser expected error on liveness failure, got nil")
	}
	if got := usersGauge(t); got != before {
		t.Errorf("locust_users = %v after a failed start, want %v unchanged", got, before)
	}
}

func TestNoNewActorAfterShutdown(t *testing.T) {
	cfg, _, fakeCtrl := newTestConfig(t, asyncJobHandler())
	r := &sweperfRuntime{cfg: cfg}

	r.shutdown(context.Background())
	r.iterate()

	if calls := fakeCtrl.recordedCalls(); len(calls) != 0 {
		t.Errorf("iterate after shutdown sent %v, want no calls", calls)
	}
	if _, ok := r.users.Load(boomerutil.GoroutineID()); ok {
		t.Errorf("iterate after shutdown bound a session")
	}
}

func TestStopDuringStartupTearsActorDown(t *testing.T) {
	var r *sweperfRuntime
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Shutdown begins while the actor is still starting.
		r.stopping.Store(true)
		json.NewEncoder(w).Encode(statusResponse{Status: "up"})
	})
	cfg, _, fakeCtrl := newTestConfig(t, handler)
	r = &sweperfRuntime{cfg: cfg}

	before := usersGauge(t)
	r.iterate()

	want := []string{"CreateAtespace", "CreateActor", "SuspendActor", "DeleteActor"}
	if got := fakeCtrl.recordedCalls(); !reflect.DeepEqual(got, want) {
		t.Errorf("recordedCalls = %v, want %v", got, want)
	}
	if got := usersGauge(t); got != before {
		t.Errorf("locust_users = %v after self-teardown, want %v unchanged", got, before)
	}
}

func TestShutdownWaitsForStartingActor(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		once.Do(func() { close(entered) })
		<-release
		json.NewEncoder(w).Encode(statusResponse{Status: "up"})
	})
	cfg, _, fakeCtrl := newTestConfig(t, handler)
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock) // runs before the server closes, should the test fail early
	r := &sweperfRuntime{cfg: cfg}

	go r.iterate()
	<-entered

	done := make(chan struct{})
	go func() {
		r.shutdown(context.Background())
		close(done)
	}()
	select {
	case <-done:
		t.Fatalf("shutdown returned while an actor was still starting; the process would exit and leak it")
	case <-time.After(50 * time.Millisecond):
	}

	unblock()
	<-done
	calls := fakeCtrl.recordedCalls()
	if n := len(calls); n < 2 || calls[n-2] != "SuspendActor" || calls[n-1] != "DeleteActor" {
		t.Errorf("recordedCalls = %v, want the starting actor suspended and deleted before shutdown returns", calls)
	}
}

func TestSuspendAndDeleteClaimsTeardownOnce(t *testing.T) {
	cfg, _, fakeCtrl := newTestConfig(t, http.HandlerFunc(nil))
	u := &sweperfUser{cfg: cfg, actorName: "act", userClass: sweperfUserClass}

	// Shutdown and a goroutine that published its session late both try to
	// tear the actor down; only one may.
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u.suspendAndDelete(context.Background())
		}()
	}
	wg.Wait()

	n := 0
	for _, c := range fakeCtrl.recordedCalls() {
		if c == "DeleteActor" {
			n++
		}
	}
	if n != 1 {
		t.Errorf("DeleteActor sent %d times, want 1", n)
	}
}
