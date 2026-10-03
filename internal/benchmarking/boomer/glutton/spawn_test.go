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
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/boomerutil"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func newTestHTTPServer(t *testing.T, pingHandler http.HandlerFunc) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	if pingHandler == nil {
		pingHandler = func(w http.ResponseWriter, r *http.Request) {
			resp, _ := proto.Marshal(&gluttonpb.PingResponse{Message: "pong"})
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(resp)
		}
	}
	mux.HandleFunc("/ping", pingHandler)
	return httptest.NewServer(mux)
}

func getPromCounter(name, status string) float64 {
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return 0
	}
	for _, mf := range mfs {
		if mf.GetName() == "locust_requests_total" {
			for _, m := range mf.GetMetric() {
				var hasName, hasStatus bool
				for _, label := range m.GetLabel() {
					if label.GetName() == "name" && label.GetValue() == name {
						hasName = true
					}
					if label.GetName() == "status" && label.GetValue() == status {
						hasStatus = true
					}
				}
				if hasName && hasStatus {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

func TestSpawnDecileThresholds(t *testing.T) {
	tests := []struct {
		totalActors int
		wantFires   map[int][]int // rank -> fired deciles
	}{
		{
			totalActors: 1,
			wantFires: map[int][]int{
				1: {10, 20, 30, 40, 50, 60, 70, 80, 90, 100},
			},
		},
		{
			totalActors: 5,
			wantFires: map[int][]int{
				1: {10, 20},
				2: {30, 40},
				3: {50, 60},
				4: {70, 80},
				5: {90, 100},
			},
		},
		{
			totalActors: 10,
			wantFires: map[int][]int{
				1:  {10},
				2:  {20},
				3:  {30},
				4:  {40},
				5:  {50},
				6:  {60},
				7:  {70},
				8:  {80},
				9:  {90},
				10: {100},
			},
		},
		{
			totalActors: 100,
			wantFires: map[int][]int{
				10:  {10},
				50:  {50},
				100: {100},
			},
		},
	}

	for _, tt := range tests {
		for rank, wantDeciles := range tt.wantFires {
			fired := firedDeciles(rank, tt.totalActors)
			if len(fired) != len(wantDeciles) {
				t.Fatalf("totalActors=%d, rank=%d: got fired deciles %v, want %v", tt.totalActors, rank, fired, wantDeciles)
			}
			for i := range fired {
				if fired[i] != wantDeciles[i] {
					t.Errorf("totalActors=%d, rank=%d: got fired[%d]=%d, want %d", tt.totalActors, rank, i, fired[i], wantDeciles[i])
				}
			}
		}
	}
}

func TestSpawnIsTerminalError(t *testing.T) {
	terminalCodes := []codes.Code{
		codes.NotFound,
		codes.DataLoss,
		codes.AlreadyExists,
		codes.FailedPrecondition,
		codes.InvalidArgument,
		codes.PermissionDenied,
		codes.Unauthenticated,
		codes.Unimplemented,
	}

	for _, code := range terminalCodes {
		err := status.Error(code, "test terminal")
		if !isSpawnTerminalError(err) {
			t.Errorf("expected code %v to be terminal", code)
		}
	}

	crashedErr := status.Error(codes.Aborted, "actor crashed while restoring")
	if !isSpawnTerminalError(crashedErr) {
		t.Errorf("expected Aborted with 'crashed' to be terminal")
	}

	crashedUpperErr := status.Error(codes.Aborted, "actor entered ACTOR_STATE_CRASHED")
	if !isSpawnTerminalError(crashedUpperErr) {
		t.Errorf("expected Aborted with uppercase 'CRASHED' to be terminal")
	}

	nonTerminalCodes := []codes.Code{
		codes.Unavailable,
		codes.ResourceExhausted,
		codes.Unknown,
		codes.Internal,
		codes.DeadlineExceeded,
	}

	for _, code := range nonTerminalCodes {
		err := status.Error(code, "test retryable")
		if isSpawnTerminalError(err) {
			t.Errorf("expected code %v to NOT be terminal", code)
		}
	}

	conflictErr := status.Error(codes.Aborted, boomerutil.ConcurrentUpdateMsg)
	if isSpawnTerminalError(conflictErr) {
		t.Errorf("expected Aborted concurrent update conflict to NOT be terminal")
	}

	if isSpawnTerminalError(nil) {
		t.Errorf("expected nil error to NOT be terminal")
	}

	if isSpawnTerminalError(errors.New("plain non-status error")) {
		t.Errorf("expected non-grpc error to NOT be terminal")
	}
}

func TestSpawnRunBatch_Success(t *testing.T) {
	srv := newTestHTTPServer(t, nil)
	defer srv.Close()

	fakeAPI := &fakeControlClient{}
	cfg := &userclass.Config{
		APIStub:          fakeAPI,
		HTTPClient:       srv.Client(),
		RouterURL:        srv.URL,
		Atespace:         "test-space",
		Tracer:           otel.Tracer("test"),
		TotalActors:      10,
		SpawnConcurrency: 3,
		ActorDeadline:    5 * time.Second,
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rt := &spawnRuntime{
		cfg:       cfg,
		runCtx:    runCtx,
		cancelRun: cancel,
	}

	beforeSuccess := getPromCounter("TimeToAllReady", "success")

	rt.runBatch(runCtx)

	if got := rt.readyCount.Load(); got != 10 {
		t.Fatalf("expected readyCount 10, got %d", got)
	}

	if rt.lastReadyElapsed.Load() <= 0 {
		t.Fatalf("expected positive lastReadyElapsed, got %d", rt.lastReadyElapsed.Load())
	}

	afterSuccess := getPromCounter("TimeToAllReady", "success")
	if afterSuccess-beforeSuccess != 1 {
		t.Fatalf("expected TimeToAllReady success counter to increase by 1, got before=%v, after=%v", beforeSuccess, afterSuccess)
	}

	names := rt.getActorNames()
	if len(names) != 10 {
		t.Fatalf("expected 10 recorded actor names, got %d", len(names))
	}
}

func TestSpawnRunBatch_PartialFailure(t *testing.T) {
	srv := newTestHTTPServer(t, nil)
	defer srv.Close()

	var failCount atomic.Int64
	fakeAPI := &fakeControlClient{
		createActorFn: func(name string) error {
			// Fail 2 out of 5 actors terminally
			if strings.HasSuffix(name, "-1") || strings.HasSuffix(name, "-2") {
				failCount.Add(1)
				return status.Error(codes.InvalidArgument, "invalid actor spec")
			}
			return nil
		},
	}

	cfg := &userclass.Config{
		APIStub:          fakeAPI,
		HTTPClient:       srv.Client(),
		RouterURL:        srv.URL,
		Atespace:         "test-space",
		Tracer:           otel.Tracer("test"),
		TotalActors:      5,
		SpawnConcurrency: 2,
		ActorDeadline:    5 * time.Second,
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rt := &spawnRuntime{
		cfg:       cfg,
		runCtx:    runCtx,
		cancelRun: cancel,
	}

	beforeSuccess := getPromCounter("TimeToAllReady", "success")
	beforeActorSuccess := getPromCounter("ActorTimeToReady", "success")
	beforeActorFailure := getPromCounter("ActorTimeToReady", "failure")

	rt.runBatch(runCtx)

	if got := rt.readyCount.Load(); got != 3 {
		t.Fatalf("expected readyCount 3, got %d", got)
	}

	if failCount.Load() != 2 {
		t.Fatalf("expected 2 failed create calls, got %d", failCount.Load())
	}

	afterSuccess := getPromCounter("TimeToAllReady", "success")
	if afterSuccess-beforeSuccess != 1 {
		t.Fatalf("expected TimeToAllReady success counter to increase by 1, got before=%v, after=%v", beforeSuccess, afterSuccess)
	}

	afterActorSuccess := getPromCounter("ActorTimeToReady", "success")
	afterActorFailure := getPromCounter("ActorTimeToReady", "failure")
	if afterActorSuccess-beforeActorSuccess != 3 {
		t.Fatalf("expected ActorTimeToReady success increase by 3, got %v", afterActorSuccess-beforeActorSuccess)
	}
	if afterActorFailure-beforeActorFailure != 2 {
		t.Fatalf("expected ActorTimeToReady failure increase by 2, got %v", afterActorFailure-beforeActorFailure)
	}
}

func TestSpawnRunBatch_AllFailed(t *testing.T) {
	srv := newTestHTTPServer(t, nil)
	defer srv.Close()

	fakeAPI := &fakeControlClient{
		createActorFn: func(name string) error {
			return status.Error(codes.InvalidArgument, "invalid actor spec")
		},
	}

	cfg := &userclass.Config{
		APIStub:          fakeAPI,
		HTTPClient:       srv.Client(),
		RouterURL:        srv.URL,
		Atespace:         "test-space",
		Tracer:           otel.Tracer("test"),
		TotalActors:      4,
		SpawnConcurrency: 2,
		ActorDeadline:    5 * time.Second,
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rt := &spawnRuntime{
		cfg:       cfg,
		runCtx:    runCtx,
		cancelRun: cancel,
	}

	beforeFailure := getPromCounter("TimeToAllReady", "failure")

	rt.runBatch(runCtx)

	if got := rt.readyCount.Load(); got != 0 {
		t.Fatalf("expected readyCount 0 on all failed, got %d", got)
	}

	afterFailure := getPromCounter("TimeToAllReady", "failure")
	if afterFailure-beforeFailure != 1 {
		t.Fatalf("expected TimeToAllReady failure counter to increase by 1, got before=%v, after=%v", beforeFailure, afterFailure)
	}
}

func TestSpawnIterate_StopOnceBlocking(t *testing.T) {
	srv := newTestHTTPServer(t, nil)
	defer srv.Close()

	var batchRuns atomic.Int64
	fakeAPI := &fakeControlClient{
		createActorFn: func(name string) error {
			batchRuns.Add(1)
			return nil
		},
	}

	cfg := &userclass.Config{
		APIStub:          fakeAPI,
		HTTPClient:       srv.Client(),
		RouterURL:        srv.URL,
		Atespace:         "test-space",
		Tracer:           otel.Tracer("test"),
		TotalActors:      1,
		SpawnConcurrency: 1,
		ActorDeadline:    5 * time.Second,
	}

	taskFn, shutdownFn := initSpawn(cfg)

	done1 := make(chan struct{})
	done2 := make(chan struct{})

	go func() {
		taskFn() // iterate() runs batch once, then blocks in select {}
		close(done1)
	}()
	go func() {
		taskFn() // second caller blocked by runOnce.Do, then blocks in select {}
		close(done2)
	}()

	// Poll until batchRuns reaches 1 or timeout.
	deadline := time.Now().Add(2 * time.Second)
	for batchRuns.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if got := batchRuns.Load(); got != 1 {
		t.Fatalf("expected batch to run exactly 1 time across multiple goroutines, ran %d times", got)
	}

	// Verify both goroutines remain blocked in select {} and have not exited.
	select {
	case <-done1:
		t.Fatal("expected goroutine 1 to block forever in select {}, but it returned")
	case <-done2:
		t.Fatal("expected goroutine 2 to block forever in select {}, but it returned")
	case <-time.After(50 * time.Millisecond):
		// Expected: both blocked
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	shutdownFn(shutdownCtx)
}

func TestSpawnCreate_AlreadyExistsOnRetrySucceeds(t *testing.T) {
	srv := newTestHTTPServer(t, nil)
	defer srv.Close()

	var attempts atomic.Int64
	fakeAPI := &fakeControlClient{
		createActorFn: func(name string) error {
			// First attempt fails with Unavailable (transient network blip after server committed)
			if attempts.Add(1) == 1 {
				return status.Error(codes.Unavailable, "network timeout")
			}
			// Retry receives AlreadyExists from server
			return status.Error(codes.AlreadyExists, "actor already exists")
		},
	}

	cfg := &userclass.Config{
		APIStub:          fakeAPI,
		HTTPClient:       srv.Client(),
		RouterURL:        srv.URL,
		Atespace:         "test-space",
		Tracer:           otel.Tracer("test"),
		TotalActors:      1,
		SpawnConcurrency: 1,
		ActorDeadline:    5 * time.Second,
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rt := &spawnRuntime{
		cfg:       cfg,
		runCtx:    runCtx,
		cancelRun: cancel,
	}

	rt.runBatch(runCtx)

	if got := rt.readyCount.Load(); got != 1 {
		t.Fatalf("expected readyCount 1 after retry with AlreadyExists, got %d", got)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("expected 2 create attempts, got %d", got)
	}
}

func TestSpawnResume_ConcurrentUpdateRetry(t *testing.T) {
	srv := newTestHTTPServer(t, nil)
	defer srv.Close()

	fakeAPI := &fakeControlClient{
		resumeErrs: []error{
			status.Error(codes.Aborted, boomerutil.ConcurrentUpdateMsg),
			status.Error(codes.Aborted, boomerutil.ConcurrentUpdateMsg),
		},
	}

	cfg := &userclass.Config{
		APIStub:          fakeAPI,
		HTTPClient:       srv.Client(),
		RouterURL:        srv.URL,
		Atespace:         "test-space",
		Tracer:           otel.Tracer("test"),
		TotalActors:      1,
		SpawnConcurrency: 1,
		ActorDeadline:    5 * time.Second,
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rt := &spawnRuntime{
		cfg:       cfg,
		runCtx:    runCtx,
		cancelRun: cancel,
	}

	rt.runBatch(runCtx)

	if got := rt.readyCount.Load(); got != 1 {
		t.Fatalf("expected readyCount 1 after retry, got %d", got)
	}
	if got := resumeCalls(fakeAPI); got != 3 {
		t.Fatalf("expected 3 resume attempts (2 retries + 1 success), got %d", got)
	}
}

func TestSpawnResume_Crashed(t *testing.T) {
	srv := newTestHTTPServer(t, nil)
	defer srv.Close()

	fakeAPI := &fakeControlClient{
		resumeErrs: []error{
			status.Error(codes.Aborted, "actor crashed while restoring"),
		},
	}

	cfg := &userclass.Config{
		APIStub:          fakeAPI,
		HTTPClient:       srv.Client(),
		RouterURL:        srv.URL,
		Atespace:         "test-space",
		Tracer:           otel.Tracer("test"),
		TotalActors:      1,
		SpawnConcurrency: 1,
		ActorDeadline:    5 * time.Second,
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rt := &spawnRuntime{
		cfg:       cfg,
		runCtx:    runCtx,
		cancelRun: cancel,
	}

	rt.runBatch(runCtx)

	if got := rt.readyCount.Load(); got != 0 {
		t.Fatalf("expected readyCount 0 for crashed actor, got %d", got)
	}

	if got := resumeCalls(fakeAPI); got != 1 {
		t.Fatalf("expected exactly 1 ResumeActor call, got %d", got)
	}
}

func TestSpawnShutdown_DeleteAll(t *testing.T) {
	srv := newTestHTTPServer(t, nil)
	defer srv.Close()

	fakeAPI := &fakeControlClient{}
	cfg := &userclass.Config{
		APIStub:          fakeAPI,
		HTTPClient:       srv.Client(),
		RouterURL:        srv.URL,
		Atespace:         "test-space",
		Tracer:           otel.Tracer("test"),
		TotalActors:      4,
		SpawnConcurrency: 2,
		ActorDeadline:    5 * time.Second,
	}

	names := []string{"spawn-a-1", "spawn-a-2", "spawn-a-3", "spawn-a-4"}
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rt := &spawnRuntime{
		cfg:        cfg,
		runCtx:     runCtx,
		cancelRun:  cancel,
		actorNames: names,
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	rt.shutdown(shutdownCtx)

	if got := len(fakeAPI.recordedDeleteRequests()); got != 4 {
		t.Fatalf("expected 4 deleted actors, got %d", got)
	}
}

func TestSpawnShutdown_MidBatchBoundedWait(t *testing.T) {
	srv := newTestHTTPServer(t, nil)
	defer srv.Close()

	// A fake CreateActor that ignores ctx and blocks indefinitely.
	createStarted := make(chan struct{})
	var started atomic.Int32
	fakeAPI := &fakeControlClient{
		createActorFn: func(name string) error {
			if started.Add(1) == 2 {
				close(createStarted)
			}
			select {}
		},
	}

	cfg := &userclass.Config{
		APIStub:          fakeAPI,
		HTTPClient:       srv.Client(),
		RouterURL:        srv.URL,
		Atespace:         "test-space",
		Tracer:           otel.Tracer("test"),
		TotalActors:      2,
		SpawnConcurrency: 2,
		ActorDeadline:    10 * time.Second,
	}

	taskFn, shutdownFn := initSpawn(cfg)

	go taskFn()

	// Wait until both goroutines have entered CreateActor and recorded their actor names.
	<-createStarted

	// Call shutdown. The goroutines in runBatch are blocked in CreateActor ignoring ctx,
	// so r.wg.Wait() will not finish. shutdown must unblock after its 2s sub-timeout
	// and delete every recorded actor name.
	start := time.Now()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	shutdownFn(shutdownCtx)
	elapsed := time.Since(start)

	// Bounded wait should elapse ~2s (allow 1.8s - 4.5s range for scheduling).
	if elapsed < 1800*time.Millisecond || elapsed > 4500*time.Millisecond {
		t.Fatalf("expected shutdown to bound wait around 2s, took %v", elapsed)
	}

	deletedCount := len(fakeAPI.recordedDeleteRequests())
	var createdCount int
	for _, call := range fakeAPI.recordedCalls() {
		if call == "CreateActor" {
			createdCount++
		}
	}

	if deletedCount == 0 || deletedCount != createdCount {
		t.Fatalf("expected DeleteActor for all %d started actors, got %d deletes", createdCount, deletedCount)
	}
}

func TestSpawnRunBatch_DynConfigOverride(t *testing.T) {
	srv := newTestHTTPServer(t, nil)
	defer srv.Close()

	t.Run("overrides when set", func(t *testing.T) {
		fakeAPI := &fakeControlClient{}
		dyn := dynconfig.Static(spawnKnobs{
			TotalActors:      3,
			SpawnConcurrency: 2,
			ActorDeadline:    dynconfig.Seconds(5 * time.Second),
		})
		cfg := &userclass.Config{
			APIStub:          fakeAPI,
			HTTPClient:       srv.Client(),
			RouterURL:        srv.URL,
			Atespace:         "test-space",
			Tracer:           otel.Tracer("test"),
			Dyn:              dyn,
			TotalActors:      100, // flag values; Dyn overrides them
			SpawnConcurrency: 1,
			ActorDeadline:    10 * time.Second,
		}

		runCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		rt := &spawnRuntime{
			cfg:       cfg,
			runCtx:    runCtx,
			cancelRun: cancel,
		}

		rt.runBatch(runCtx)

		if got := rt.readyCount.Load(); got != 3 {
			t.Fatalf("expected readyCount 3 from dynconfig override, got %d", got)
		}
		if got := len(rt.getActorNames()); got != 3 {
			t.Fatalf("expected 3 recorded actor names, got %d", got)
		}
	})

	t.Run("falls back to cfg when dynconfig is zero", func(t *testing.T) {
		fakeAPI := &fakeControlClient{}
		dyn := dynconfig.Static(spawnKnobs{
			TotalActors:      0, // 0 = unset
			SpawnConcurrency: 0,
			ActorDeadline:    0,
		})
		cfg := &userclass.Config{
			APIStub:          fakeAPI,
			HTTPClient:       srv.Client(),
			RouterURL:        srv.URL,
			Atespace:         "test-space",
			Tracer:           otel.Tracer("test"),
			Dyn:              dyn,
			TotalActors:      4,
			SpawnConcurrency: 2,
			ActorDeadline:    5 * time.Second,
		}

		runCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		rt := &spawnRuntime{
			cfg:       cfg,
			runCtx:    runCtx,
			cancelRun: cancel,
		}

		rt.runBatch(runCtx)

		if got := rt.readyCount.Load(); got != 4 {
			t.Fatalf("expected readyCount 4 from cfg fallback, got %d", got)
		}
		if got := len(rt.getActorNames()); got != 4 {
			t.Fatalf("expected 4 recorded actor names, got %d", got)
		}
	})
}
