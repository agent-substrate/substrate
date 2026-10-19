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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.ate.dev/substrate/internal/ateinterceptors"
	"go.ate.dev/substrate/internal/atenet"
	"go.ate.dev/substrate/internal/benchmarking/boomer/boomerutil"
	"go.ate.dev/substrate/internal/benchmarking/boomer/dynconfig"
	"go.ate.dev/substrate/internal/benchmarking/boomer/userclass"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
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
	suspendReqs    []*ateapipb.SuspendActorRequest
	// Make ResumeActor take time and report a smaller server-side elapsed.
	resumeDelay     time.Duration
	resumeElapsedUs string
	// AnyState carried by the most recent DeleteActor request.
	deleteAnyState bool
	// beforeDelete, if set, runs at the start of each DeleteActor, outside mu.
	beforeDelete func()
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
	f.suspendReqs = append(f.suspendReqs, in)
	if f.suspendErr != nil {
		return nil, f.suspendErr
	}
	return &ateapipb.SuspendActorResponse{}, nil
}

func (f *fakeControlClient) DeleteActor(ctx context.Context, in *ateapipb.DeleteActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	if f.beforeDelete != nil {
		f.beforeDelete()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "DeleteActor")
	f.deleteAnyState = in.GetAnyState()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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
		Dyn:        dynconfig.Static(knobs{}),
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
				Dyn: dynconfig.Static(knobs{}),
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
				Dyn: dynconfig.Static(knobs{
					Template:   "custom-template",
					TotalSteps: 50,
					NumCycles:  5,
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
		cfg := &userclass.Config{Dyn: dynconfig.Static(knobs{PollIntervalMs: tt.ms})}
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
	fakeCtrl.mu.Lock()
	suspendReqs := fakeCtrl.suspendReqs
	fakeCtrl.mu.Unlock()
	for _, req := range suspendReqs {
		if got := req.GetOptions().GetMode(); got != ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS {
			t.Errorf("suspend mode = %v, want %v", got, ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS)
		}
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

func TestAtespaceCreatedOncePerWorker(t *testing.T) {
	cfg, _, fakeCtrl := newTestConfig(t, asyncJobHandler())
	r := newSweperfRuntime(cfg)
	countCreateAtespace := func() int {
		n := 0
		for _, c := range fakeCtrl.recordedCalls() {
			if c == "CreateAtespace" {
				n++
			}
		}
		return n
	}

	for range 2 {
		if _, err := r.startUser(context.Background()); err != nil {
			t.Fatalf("startUser: %v", err)
		}
	}
	if n := countCreateAtespace(); n != 1 {
		t.Errorf("CreateAtespace sent %d times for two actors, want 1", n)
	}

	// A missing atespace comes back from CreateActor as FailedPrecondition;
	// the next start recreates it.
	fakeCtrl.createActorErr = status.Error(codes.FailedPrecondition, "Atespace benchmark-test not found")
	if _, err := r.startUser(context.Background()); err == nil {
		t.Fatalf("startUser succeeded despite CreateActor failing")
	}
	fakeCtrl.createActorErr = nil
	if _, err := r.startUser(context.Background()); err != nil {
		t.Fatalf("startUser: %v", err)
	}
	if n := countCreateAtespace(); n != 2 {
		t.Errorf("CreateAtespace sent %d times, want 2 after the atespace went missing", n)
	}
}

func TestControlClientErrors(t *testing.T) {
	t.Run("ResumeActor error returns false", func(t *testing.T) {
		cfg, _, fakeCtrl := newTestConfig(t, http.HandlerFunc(nil))
		fakeCtrl.resumeErr = errors.New("resume RPC failed")
		u := &sweperfUser{cfg: cfg, actorName: "act"}

		if _, ok := u.resume(context.Background()); ok {
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

	t.Run("retries at the sweperf poll interval", func(t *testing.T) {
		var mu sync.Mutex
		calls := 0
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			calls++
			n := calls
			mu.Unlock()
			if n < 3 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			json.NewEncoder(w).Encode(statusResponse{Status: "up"})
		})
		cfg, _, _ := newTestConfig(t, handler)
		cfg.Dyn = dynconfig.Static(knobs{PollIntervalMs: 10})
		u := &sweperfUser{cfg: cfg, actorName: "act"}

		start := time.Now()
		if err := u.pollLiveness(context.Background()); err != nil {
			t.Fatalf("pollLiveness: %v", err)
		}
		// Two retries at a fixed 2s interval would take 4s.
		if d := time.Since(start); d > time.Second {
			t.Errorf("pollLiveness took %v for two retries at a 10ms interval", d)
		}
	})
}

func TestStartupFailureLogsUserActorAndTemplate(t *testing.T) {
	logs := captureLogs(t)
	cfg, _, fakeCtrl := newTestConfig(t, asyncJobHandler())
	fakeCtrl.createActorErr = errors.New("quota exceeded")
	r := newSweperfRuntime(cfg)

	if _, err := r.startUser(context.Background()); err == nil {
		t.Fatalf("startUser succeeded despite CreateActor failing")
	}

	failure := onlyLine(t, logs(), startupFailedMsg)
	if actor, _ := failure["actor"].(string); !strings.HasPrefix(actor, "sb-") {
		t.Errorf("failure log actor = %q, want the new actor's name", actor)
	}
	if got := failure["template"]; got != defaultSweperfTemplate {
		t.Errorf("failure log template = %v, want %q", got, defaultSweperfTemplate)
	}
	if got := failure["user"]; got != float64(boomerutil.GoroutineID()) {
		t.Errorf("failure log user = %v, want this goroutine's ID", got)
	}
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
				Dyn: dynconfig.Static(knobs{
					WaitTime: dynconfig.WaitTime{MinWait: dynconfig.Seconds(100 * time.Millisecond), MaxWait: dynconfig.Seconds(50 * time.Millisecond)},
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
				Dyn: dynconfig.Static(knobs{
					WaitTime: dynconfig.WaitTime{MinWait: dynconfig.Seconds(minW), MaxWait: dynconfig.Seconds(maxW)},
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
	cfg.Dyn = dynconfig.Static(knobs{
		WaitTime: dynconfig.WaitTime{MinWait: dynconfig.Seconds(time.Millisecond), MaxWait: dynconfig.Seconds(2 * time.Millisecond)},
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
	rt := newSweperfRuntime(cfg)
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

func TestStartupFailureDeletesWithoutSuspend(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server not ready", http.StatusInternalServerError)
	})
	cfg, _, fakeCtrl := newTestConfig(t, handler)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	rt := newSweperfRuntime(cfg)
	_, err := rt.startUser(ctx)
	if err == nil {
		t.Fatalf("startUser expected error on liveness failure, got nil")
	}

	if got, want := fakeCtrl.recordedCalls(), []string{"CreateAtespace", "CreateActor", "DeleteActor"}; !reflect.DeepEqual(got, want) {
		t.Errorf("recordedCalls = %v, want %v", got, want)
	}
}

func TestShutdownDeletesWithoutSuspend(t *testing.T) {
	cfg, _, fakeCtrl := newTestConfig(t, http.HandlerFunc(nil))

	u := &sweperfUser{
		cfg:       cfg,
		actorName: "swe-actor",
		userClass: sweperfUserClass,
	}

	rt := newSweperfRuntime(cfg)
	rt.users.Store("goroutine-1", u)
	rt.shutdown(context.Background())

	if got, want := fakeCtrl.recordedCalls(), []string{"DeleteActor"}; !reflect.DeepEqual(got, want) {
		t.Errorf("recordedCalls = %v, want %v", got, want)
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
	if _, ok := u.resume(context.Background()); !ok {
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
	if _, ok := u.resume(context.Background()); !ok {
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
	r := newSweperfRuntime(cfg)
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
	r := newSweperfRuntime(cfg)
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
	// The final cycle is not suspended; the actor is deleted straight away.
	if got, want := fakeCtrl.recordedCalls(), []string{"ResumeActor", "DeleteActor"}; !reflect.DeepEqual(got, want) {
		t.Errorf("recordedCalls = %v, want %v", got, want)
	}
	if _, ok := r.users.Load(gid); ok {
		t.Fatalf("finished session still bound to its goroutine; the next iterate would reuse it")
	}

	// The next iterate creates a brand-new actor for the next trajectory.
	before := len(fakeCtrl.recordedCalls())
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
	if len(newCalls) < 2 || newCalls[0] != "CreateAtespace" || newCalls[1] != "CreateActor" {
		t.Errorf("next iterate calls = %v, want to start with CreateAtespace, CreateActor", newCalls)
	}
	r.shutdown(context.Background())
}

func TestRetireRetriesFailedDeleteOnce(t *testing.T) {
	cfg, _, fakeCtrl := newTestConfig(t, asyncJobHandler())
	fakeCtrl.deleteErr = errors.New("unavailable")
	r := newSweperfRuntime(cfg)
	u := &sweperfUser{cfg: cfg, actorName: "act-1", userClass: sweperfUserClass, chunks: []chunk{{0, 5}}}
	r.users.Store(boomerutil.GoroutineID(), u)

	r.iterate()

	// One retry, then the actor is given up on rather than retried forever.
	want := []string{"ResumeActor", "DeleteActor", "DeleteActor"}
	if got := fakeCtrl.recordedCalls(); !reflect.DeepEqual(got, want) {
		t.Errorf("recordedCalls = %v, want %v", got, want)
	}
}

func TestRetireRetryGetsFreshTeardownCtx(t *testing.T) {
	prev := teardownTimeout
	teardownTimeout = 50 * time.Millisecond
	t.Cleanup(func() { teardownTimeout = prev })

	cfg, _, fakeCtrl := newTestConfig(t, asyncJobHandler())
	// The first DeleteActor outlasts its teardown context; the retry is quick.
	var once sync.Once
	fakeCtrl.beforeDelete = func() { once.Do(func() { time.Sleep(100 * time.Millisecond) }) }
	r := newSweperfRuntime(cfg)
	u := &sweperfUser{cfg: cfg, actorName: "act-1", userClass: sweperfUserClass, chunks: []chunk{{0, 5}}}
	r.users.Store(boomerutil.GoroutineID(), u)

	before := readStats(t)
	r.iterate()

	if got := recordedSince(t, before, "DeleteActor", "failure"); got.count != 1 {
		t.Errorf("DeleteActor recorded %d failures, want 1 for the timed-out first attempt", got.count)
	}
	if got := recordedSince(t, before, "DeleteActor", "success"); got.count != 1 {
		t.Errorf("DeleteActor recorded %d successes, want the retry to succeed on a fresh context", got.count)
	}
}

func TestOnlyFinalCycleSkipsSuspend(t *testing.T) {
	cfg, _, fakeCtrl := newTestConfig(t, asyncJobHandler())
	r := newSweperfRuntime(cfg)
	u := &sweperfUser{cfg: cfg, actorName: "act-1", userClass: sweperfUserClass, chunks: []chunk{{0, 2}, {2, 5}}}
	r.users.Store(boomerutil.GoroutineID(), u)

	r.iterate()
	r.iterate()

	want := []string{"ResumeActor", "SuspendActor", "ResumeActor", "DeleteActor"}
	if got := fakeCtrl.recordedCalls(); !reflect.DeepEqual(got, want) {
		t.Errorf("recordedCalls = %v, want %v", got, want)
	}
}

func TestShutdownAndRetireDeleteOnce(t *testing.T) {
	cfg, _, fakeCtrl := newTestConfig(t, asyncJobHandler())
	r := newSweperfRuntime(cfg)
	gid := boomerutil.GoroutineID()
	u := &sweperfUser{cfg: cfg, actorName: "act-1", userClass: sweperfUserClass, chunks: []chunk{{0, 5}}}
	r.users.Store(gid, u)

	// Shutdown claims the actor first; a late retire must not delete it again.
	r.shutdown(context.Background())
	r.retireUser(gid, u)

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
		// Two cycles: the final one is never suspended.
		chunks: []chunk{{0, 2}, {2, 5}},
	}

	u.step(context.Background())

	if !u.loopFailed {
		t.Errorf("loopFailed = false after SuspendActor failed; the task would be recorded as a success")
	}
	if u.failStep != "suspend" || u.failCycle != 1 {
		t.Errorf("first failure = %q in cycle %d, want suspend in cycle 1", u.failStep, u.failCycle)
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
	r := newSweperfRuntime(cfg)

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
	r := newSweperfRuntime(cfg)

	r.shutdown(context.Background())
	r.iterate()

	if calls := fakeCtrl.recordedCalls(); len(calls) != 0 {
		t.Errorf("iterate after shutdown sent %v, want no calls", calls)
	}
	if _, ok := r.users.Load(boomerutil.GoroutineID()); ok {
		t.Errorf("iterate after shutdown bound a session")
	}
}

func TestShutdownCancelsSlowStartup(t *testing.T) {
	entered := make(chan struct{})
	var once sync.Once
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		once.Do(func() { close(entered) })
		<-req.Context().Done() // the sandbox never comes up
	})
	cfg, _, fakeCtrl := newTestConfig(t, handler)
	r := newSweperfRuntime(cfg)

	before := usersGauge(t)
	go r.iterate()
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	r.shutdown(ctx)
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("shutdown took %v; the liveness poll was not canceled", d)
	}

	if got, want := fakeCtrl.recordedCalls(), []string{"CreateAtespace", "CreateActor", "DeleteActor"}; !reflect.DeepEqual(got, want) {
		t.Errorf("recordedCalls = %v, want %v: the starting actor deleted despite cancellation", got, want)
	}
	if got := usersGauge(t); got != before {
		t.Errorf("locust_users = %v after shutdown, want %v unchanged", got, before)
	}
}

func TestTracedCallSkipsFailureWhenCanceled(t *testing.T) {
	cfg, _, fakeCtrl := newTestConfig(t, http.HandlerFunc(nil))
	// gRPC reports cancellation as a status, which errors.Is does not match.
	fakeCtrl.resumeErr = status.Error(codes.Canceled, "context canceled")
	fakeCtrl.resumeElapsedUs = "5000"
	u := &sweperfUser{cfg: cfg, actorName: "act", userClass: sweperfUserClass}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	before := readStats(t)
	if _, ok := u.resume(ctx); ok {
		t.Fatalf("resume = true on RPC failure, want false")
	}
	for _, name := range []string{"ResumeActor", "ResumeActor_rtt"} {
		if got := recordedSince(t, before, name, "failure"); got.count != 0 {
			t.Errorf("%s recorded %d failures for a canceled call, want 0", name, got.count)
		}
	}
}

func TestShutdownMidCycleRecordsNoFailures(t *testing.T) {
	entered := make(chan struct{})
	var once sync.Once
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/execute":
			json.NewEncoder(w).Encode(executeResponse{JobID: "job-1", Status: "ACCEPTED"})
		case r.URL.Path == "/status" && r.URL.Query().Get("job_id") != "":
			once.Do(func() { close(entered) })
			<-r.Context().Done() // the job never finishes
		default:
			http.NotFound(w, r)
		}
	})
	cfg, _, fakeCtrl := newTestConfig(t, handler)
	cfg.Dyn = dynconfig.Static(knobs{PollIntervalMs: 10})
	r := newSweperfRuntime(cfg)
	// The last cycle, so an interrupted task would also emit task rows.
	u := &sweperfUser{cfg: cfg, actorName: "act-1", userClass: sweperfUserClass, chunks: []chunk{{0, 5}}}

	before := readStats(t)
	go func() {
		r.users.Store(boomerutil.GoroutineID(), u)
		r.iterate()
	}()
	<-entered // the cycle is polling its job

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r.shutdown(ctx)

	for _, name := range []string{
		"ResumeActor", "ResumeActor_rtt", "ResumeToFirstExec", "Workload_Cycle_1",
		"CycleCEL", "TaskCEL", "TaskWallClock", "SuspendActor", "DeleteActor",
	} {
		if got := recordedSince(t, before, name, "failure"); got.count != 0 {
			t.Errorf("%s recorded %d failures after shutdown canceled the cycle, want 0", name, got.count)
		}
	}
	if got, want := fakeCtrl.recordedCalls(), []string{"ResumeActor", "DeleteActor"}; !reflect.DeepEqual(got, want) {
		t.Errorf("recordedCalls = %v, want %v: the interrupted actor deleted by the sweep", got, want)
	}
}

func TestShutdownWaitsForRetire(t *testing.T) {
	cfg, _, fakeCtrl := newTestConfig(t, asyncJobHandler())
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	fakeCtrl.beforeDelete = func() {
		once.Do(func() { close(entered) })
		<-release
	}
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	r := newSweperfRuntime(cfg)
	u := &sweperfUser{cfg: cfg, actorName: "act-1", userClass: sweperfUserClass, chunks: []chunk{{0, 5}}}

	// Sessions are keyed by goroutine, so bind u on the goroutine that iterates.
	go func() {
		r.users.Store(boomerutil.GoroutineID(), u)
		r.iterate()
	}()
	<-entered // retireUser is inside DeleteActor

	done := make(chan struct{})
	go func() {
		r.shutdown(context.Background())
		close(done)
	}()
	select {
	case <-done:
		t.Fatalf("shutdown returned mid-DeleteActor; the process would exit and leak the actor")
	case <-time.After(50 * time.Millisecond):
	}

	unblock()
	<-done
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

func TestTeardownClaimsOnce(t *testing.T) {
	cfg, _, fakeCtrl := newTestConfig(t, http.HandlerFunc(nil))
	u := &sweperfUser{cfg: cfg, actorName: "act", userClass: sweperfUserClass}

	// Shutdown's sweep and a goroutine it stopped waiting for may both try to
	// tear the actor down; only one may.
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u.teardown(context.Background())
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

func TestShutdownSweepsConcurrently(t *testing.T) {
	cfg, _, fakeCtrl := newTestConfig(t, http.HandlerFunc(nil))
	fakeCtrl.beforeDelete = func() { time.Sleep(50 * time.Millisecond) }
	r := newSweperfRuntime(cfg)
	const n = 20
	for i := range n {
		r.users.Store(int64(i), &sweperfUser{cfg: cfg, actorName: fmt.Sprintf("act-%d", i), userClass: sweperfUserClass})
	}

	start := time.Now()
	r.shutdown(context.Background())

	// A serial sweep would take n * 50ms = 1s.
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Errorf("shutdown took %v for %d actors; the sweep is not concurrent", d, n)
	}
	deletes := 0
	for _, c := range fakeCtrl.recordedCalls() {
		if c == "DeleteActor" {
			deletes++
		}
	}
	if deletes != n {
		t.Errorf("DeleteActor sent %d times, want %d", deletes, n)
	}
}

func TestShutdownSweepsAfterInflightWaitCap(t *testing.T) {
	prev := inflightWaitTimeout
	inflightWaitTimeout = 50 * time.Millisecond
	t.Cleanup(func() { inflightWaitTimeout = prev })

	cfg, _, fakeCtrl := newTestConfig(t, http.HandlerFunc(nil))
	r := newSweperfRuntime(cfg)
	r.users.Store(int64(1), &sweperfUser{cfg: cfg, actorName: "act-1", userClass: sweperfUserClass})
	// An iteration that never finishes.
	r.inflight.Add(1)
	t.Cleanup(r.inflight.Done)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	r.shutdown(ctx)

	if d := time.Since(start); d > time.Second {
		t.Errorf("shutdown took %v; the in-flight wait used the whole budget", d)
	}
	if got, want := fakeCtrl.recordedCalls(), []string{"DeleteActor"}; !reflect.DeepEqual(got, want) {
		t.Errorf("recordedCalls = %v, want the bound actor swept despite the stuck iteration", got)
	}
}

const (
	taskRecordMsg     = "sweperf task record"
	completedCycleMsg = "Completed sweperf cycle"
	shutdownMsg       = "sweperf shutdown started"
	startupFailedMsg  = "sweperf on_start failed; goroutine will retry next iter"
)

// syncWriter serializes writes from goroutines logging while a test reads.
type syncWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

// captureLogs routes slog to JSON in memory for the rest of the test and
// returns a reader of every line logged so far, in order.
func captureLogs(t *testing.T) func() []map[string]any {
	t.Helper()
	w := &syncWriter{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(w, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return func() []map[string]any {
		w.mu.Lock()
		defer w.mu.Unlock()
		var out []map[string]any
		for _, line := range strings.Split(w.buf.String(), "\n") {
			var rec map[string]any
			if json.Unmarshal([]byte(line), &rec) == nil {
				out = append(out, rec)
			}
		}
		return out
	}
}

// linesWith returns the lines whose msg is msg.
func linesWith(lines []map[string]any, msg string) []map[string]any {
	var out []map[string]any
	for _, l := range lines {
		if l["msg"] == msg {
			out = append(out, l)
		}
	}
	return out
}

// onlyLine returns the single line whose msg is msg.
func onlyLine(t *testing.T, lines []map[string]any, msg string) map[string]any {
	t.Helper()
	got := linesWith(lines, msg)
	if len(got) != 1 {
		t.Fatalf("%d %q lines, want 1: %v", len(got), msg, got)
	}
	return got[0]
}

func TestTaskRecordForCleanTask(t *testing.T) {
	logs := captureLogs(t)
	cfg, _, fakeCtrl := newTestConfig(t, asyncJobHandler())
	fakeCtrl.resumeElapsedUs = "5000"
	cfg.Dyn = dynconfig.Static(knobs{
		WaitTime:   dynconfig.WaitTime{MinWait: dynconfig.Seconds(3 * time.Millisecond), MaxWait: dynconfig.Seconds(3 * time.Millisecond)},
		TotalSteps: 10, NumCycles: 2,
	})
	r := newSweperfRuntime(cfg)
	t.Cleanup(func() { r.shutdown(context.Background()) })

	r.iterate() // starts the actor and runs cycle 1 (no-op resume)
	r.iterate() // cycle 2, the last, ends the task
	lines := logs()

	cycles := linesWith(lines, completedCycleMsg)
	if len(cycles) != 2 {
		t.Fatalf("%d completed cycles, want 2", len(cycles))
	}
	first, last := cycles[0], cycles[1]
	if first["resume_ms"] != nil || first["ttfe_ms"] != nil {
		t.Errorf("cycle 1 resume_ms = %v, ttfe_ms = %v; want neither for a no-op resume",
			first["resume_ms"], first["ttfe_ms"])
	}
	if first["cel_ms"] != 1200.0 || first["ok"] != true {
		t.Errorf("cycle 1 cel_ms = %v, ok = %v; want 1200, true", first["cel_ms"], first["ok"])
	}
	// The last cycle is not suspended, and is still ok.
	if last["ok"] != true || last["resume_ms"] != 5.0 || last["ttfe_ms"] == nil {
		t.Errorf("cycle 2 ok = %v, resume_ms = %v, ttfe_ms = %v; want true, the server's 5ms, set",
			last["ok"], last["resume_ms"], last["ttfe_ms"])
	}

	rec := onlyLine(t, lines, taskRecordMsg)
	// One think time between the two cycles; none after the last.
	want := map[string]any{"v": 1.0, "user": float64(boomerutil.GoroutineID()), "template": defaultSweperfTemplate,
		"cycles": 2.0, "steps": 10.0,
		"cel_ms": 2400.0, "think_ms": 3.0, "resume_max_ms": 5.0, "retries": 0.0,
		"failed": false, "fail_step": "", "fail_cycle": 0.0}
	for k, v := range want {
		if rec[k] != v {
			t.Errorf("record %s = %v, want %v", k, rec[k], v)
		}
	}
	if actor, _ := rec["actor"].(string); !strings.HasPrefix(actor, "sb-") {
		t.Errorf("record actor = %q, want the session's sb- actor", actor)
	}
	if rec["startup_ms"].(float64) <= 0 || rec["ttfe_max_ms"] == nil {
		t.Errorf("record startup_ms = %v, ttfe_max_ms = %v; want > 0 and set", rec["startup_ms"], rec["ttfe_max_ms"])
	}
	if sum := first["wall_ms"].(float64) + last["wall_ms"].(float64); rec["wall_ms"].(float64) < sum {
		t.Errorf("record wall_ms = %v, want at least the cycles' %v", rec["wall_ms"], sum)
	}
	start, _ := time.Parse(time.RFC3339Nano, rec["task_start"].(string))
	end, _ := time.Parse(time.RFC3339Nano, rec["end"].(string))
	if start.IsZero() || !end.After(start) {
		t.Errorf("record task_start = %v, end = %v; want start set and before end", rec["task_start"], rec["end"])
	}
}

func TestTaskRecordResumeFailureAndRetry(t *testing.T) {
	logs := captureLogs(t)
	cfg, _, fakeCtrl := newTestConfig(t, asyncJobHandler())
	cfg.Dyn = dynconfig.Static(knobs{
		WaitTime: dynconfig.WaitTime{MinWait: dynconfig.Seconds(4 * time.Millisecond), MaxWait: dynconfig.Seconds(4 * time.Millisecond)},
	})
	r := newSweperfRuntime(cfg)
	u := &sweperfUser{cfg: cfg, actorName: "act", userClass: sweperfUserClass, chunks: []chunk{{0, 5}, {5, 10}}}
	r.users.Store(boomerutil.GoroutineID(), u)

	fakeCtrl.resumeErr = errors.New("resume failed")
	r.iterate() // cycle 1 fails at resume
	fakeCtrl.mu.Lock()
	fakeCtrl.resumeErr = nil
	fakeCtrl.mu.Unlock()
	r.iterate() // cycle 1 retried
	r.iterate() // cycle 2 ends the task
	lines := logs()

	rec := onlyLine(t, lines, taskRecordMsg)
	// The wait after the failed resume is a retry, not think time.
	want := map[string]any{"retries": 1.0, "failed": true, "fail_step": "resume", "fail_cycle": 1.0, "think_ms": 4.0,
		"cycles": 2.0, "steps": 10.0}
	for k, v := range want {
		if rec[k] != v {
			t.Errorf("record %s = %v, want %v", k, rec[k], v)
		}
	}
	// The failed attempt has no cycle line, but its wall time is part of the task's.
	sum := onlyLine(t, lines, "ResumeActor failed in step")["wall_ms"].(float64)
	for _, c := range linesWith(lines, completedCycleMsg) {
		sum += c["wall_ms"].(float64)
	}
	if rec["wall_ms"].(float64) < sum {
		t.Errorf("record wall_ms = %v, want at least %v including the failed resume", rec["wall_ms"], sum)
	}
}

func TestTaskRecordCountsFollowConfigChange(t *testing.T) {
	logs := captureLogs(t)
	cfg, _, _ := newTestConfig(t, asyncJobHandler())
	cfg.Dyn = dynconfig.Static(knobs{TotalSteps: 4, NumCycles: 2})
	r := newSweperfRuntime(cfg)
	t.Cleanup(func() { r.shutdown(context.Background()) })

	r.iterate() // task 1, cycle 1
	// A change mid-task applies from the next task on.
	if _, _, err := cfg.Dyn.Apply([]byte(`{"sweperf_total_steps": 3, "sweperf_num_cycles": 5}`)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	r.iterate() // task 1, cycle 2, the last
	for range 3 {
		r.iterate() // task 2: five cycles over three steps run as three
	}

	recs := linesWith(logs(), taskRecordMsg)
	if len(recs) != 2 {
		t.Fatalf("%d task records, want 2", len(recs))
	}
	for i, want := range []struct{ cycles, steps float64 }{{2, 4}, {3, 3}} {
		if recs[i]["cycles"] != want.cycles || recs[i]["steps"] != want.steps {
			t.Errorf("record %d cycles = %v, steps = %v; want %v, %v",
				i+1, recs[i]["cycles"], recs[i]["steps"], want.cycles, want.steps)
		}
	}
}

func TestTaskRecordExecuteFailureOnLastCycle(t *testing.T) {
	logs := captureLogs(t)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	cfg, _, _ := newTestConfig(t, handler)
	u := &sweperfUser{cfg: cfg, actorName: "act", userClass: sweperfUserClass, chunks: []chunk{{0, 5}}}

	u.step(context.Background())
	u.recordTaskMetrics()
	lines := logs()

	if cyc := onlyLine(t, lines, completedCycleMsg); cyc["ok"] != false || cyc["cel_ms"] != 0.0 || cyc["ttfe_ms"] != nil {
		t.Errorf("cycle ok = %v, cel_ms = %v, ttfe_ms = %v; want false, 0, unset", cyc["ok"], cyc["cel_ms"], cyc["ttfe_ms"])
	}
	rec := onlyLine(t, lines, taskRecordMsg)
	if rec["failed"] != true || rec["fail_step"] != "execute" || rec["fail_cycle"] != 1.0 || rec["ttfe_max_ms"] != nil {
		t.Errorf("record failed = %v, fail_step = %v, fail_cycle = %v, ttfe_max_ms = %v; want true, execute, 1, unset",
			rec["failed"], rec["fail_step"], rec["fail_cycle"], rec["ttfe_max_ms"])
	}
}

func TestShutdownCutsOffInFlightCycle(t *testing.T) {
	for _, tc := range []struct {
		name   string
		chunks []chunk
	}{
		{name: "last cycle", chunks: []chunk{{0, 5}}},
		{name: "earlier cycle", chunks: []chunk{{0, 5}, {5, 10}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureLogs(t)
			entered := make(chan struct{})
			var once sync.Once
			handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				// Read the body so the server notices when the client cancels.
				json.NewDecoder(req.Body).Decode(&executeRequest{})
				once.Do(func() { close(entered) })
				<-req.Context().Done() // the chunk is still running when shutdown begins
			})
			cfg, _, _ := newTestConfig(t, handler)
			r := newSweperfRuntime(cfg)
			u := &sweperfUser{cfg: cfg, actorName: "act", userClass: sweperfUserClass, chunks: tc.chunks}

			// Sessions are keyed by goroutine, so bind u on the goroutine that iterates.
			go func() {
				r.users.Store(boomerutil.GoroutineID(), u)
				r.iterate()
			}()
			<-entered
			r.shutdown(context.Background())
			lines := logs()

			onlyLine(t, lines, shutdownMsg)
			if cycs := linesWith(lines, completedCycleMsg); len(cycs) != 0 {
				t.Errorf("completed cycle logged for a canceled cycle: %v", cycs)
			}
			if recs := linesWith(lines, taskRecordMsg); len(recs) != 0 {
				t.Errorf("task record logged for a canceled task: %v", recs)
			}
		})
	}
}
