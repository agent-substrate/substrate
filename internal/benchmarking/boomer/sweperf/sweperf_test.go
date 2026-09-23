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

	"github.com/agent-substrate/substrate/internal/atenet"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeControlClient struct {
	ateapipb.ControlClient
	mu             sync.Mutex
	calls          []string
	createSpaceErr error
	createActorErr error
	resumeErr      error
	suspendErr     error
	deleteErr      error
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
	if f.resumeErr != nil {
		return nil, f.resumeErr
	}
	return &ateapipb.ResumeActorResponse{}, nil
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
	if err := u.execute(context.Background(), 1, 0, 5); err != nil {
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

	err := u.execute(context.Background(), 1, 0, 5)
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

	err := u.execute(context.Background(), 1, 0, 5)
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
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			exitCode := 0
			json.NewEncoder(w).Encode(jobStatusResponse{
				JobID:    "job-1",
				Status:   "COMPLETED",
				ExitCode: &exitCode,
			})
		})
		cfg, _, _ := newTestConfig(t, handler)
		u := &sweperfUser{cfg: cfg, actorName: "act"}

		if err := u.pollJobCompletion(context.Background(), "job-1", 1); err != nil {
			t.Errorf("pollJobCompletion failed unexpectedly: %v", err)
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

		if err := u.pollJobCompletion(context.Background(), "job-1", 1); err == nil {
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
}
