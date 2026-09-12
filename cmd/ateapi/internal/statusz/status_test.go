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

package statusz

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestSnapshotGroupsAndBoundsKnownWorkers(t *testing.T) {
	workers := []*ateapipb.Worker{
		worker("zeta", "pool-b"),
		worker("alpha", "pool-a"),
		worker("alpha", "pool-a"),
	}
	for i := range 100 {
		workers = append(workers, worker("middle", "pool-"+threeDigits(i)))
	}
	original := slices.Clone(workers)

	handler := NewHandler(testConfig(), func() ([]*ateapipb.Worker, error) {
		return workers, nil
	}, func() bool { return true }, nil, nil, func() time.Time {
		return testConfig().StartedAt.Add(90 * time.Second)
	})
	snapshot := requestJSON(t, handler, "/statusz?format=json", "")

	if !snapshot.Workers.Available {
		t.Fatal("workers available = false, want true")
	}
	if intValue(snapshot.Workers.TotalWorkers) != 103 || intValue(snapshot.Workers.TotalGroups) != 102 {
		t.Fatalf("worker totals = %v workers in %v groups, want 103 workers in 102 groups", snapshot.Workers.TotalWorkers, snapshot.Workers.TotalGroups)
	}
	if len(snapshot.Workers.Groups) != 100 || !snapshot.Workers.Truncated {
		t.Fatalf("displayed groups = %d, truncated = %t; want 100, true", len(snapshot.Workers.Groups), snapshot.Workers.Truncated)
	}
	if got := snapshot.Workers.Groups[0]; got.Namespace != "alpha" || got.Pool != "pool-a" || got.Count != 2 {
		t.Fatalf("first worker group = %#v, want alpha/pool-a count 2", got)
	}
	if !slices.Equal(workers, original) {
		t.Fatal("snapshot assembly reordered the worker-cache slice")
	}
}

func TestCollectWorkersDetachesTruncatedGroupStorage(t *testing.T) {
	workers := make([]*ateapipb.Worker, 0, maxWorkerGroups+10)
	for i := range maxWorkerGroups + 10 {
		workers = append(workers, worker("tenant", "pool-"+threeDigits(i)))
	}
	snapshot := collectWorkers(func() ([]*ateapipb.Worker, error) {
		return workers, nil
	})
	if len(snapshot.Groups) != maxWorkerGroups || cap(snapshot.Groups) != maxWorkerGroups {
		t.Fatalf("retained worker groups have len=%d cap=%d, want %d/%d", len(snapshot.Groups), cap(snapshot.Groups), maxWorkerGroups, maxWorkerGroups)
	}
}

func TestSnapshotDistinguishesEmptyAndUnavailableWorkerCache(t *testing.T) {
	tests := []struct {
		name      string
		workers   WorkerList
		available bool
	}{
		{
			name: "ready and empty",
			workers: func() ([]*ateapipb.Worker, error) {
				return []*ateapipb.Worker{}, nil
			},
			available: true,
		},
		{
			name: "cache unavailable",
			workers: func() ([]*ateapipb.Worker, error) {
				return nil, errors.New("database password must never be rendered")
			},
			available: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := NewHandler(testConfig(), tt.workers, func() bool { return true }, nil, nil, func() time.Time {
				return testConfig().StartedAt
			})
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/statusz?format=json", nil))
			if strings.Contains(recorder.Body.String(), "database password") {
				t.Fatal("worker-cache error leaked into response")
			}
			var snapshot Snapshot
			if err := json.Unmarshal(recorder.Body.Bytes(), &snapshot); err != nil {
				t.Fatalf("decode JSON: %v", err)
			}
			if snapshot.Workers.Available != tt.available {
				t.Fatalf("workers available = %t, want %t", snapshot.Workers.Available, tt.available)
			}
			if tt.available {
				if snapshot.Workers.TotalWorkers == nil || *snapshot.Workers.TotalWorkers != 0 || snapshot.Workers.TotalGroups == nil || *snapshot.Workers.TotalGroups != 0 {
					t.Fatalf("ready empty workers = %#v, want explicit zero counts", snapshot.Workers)
				}
			} else if snapshot.Workers.TotalWorkers != nil || snapshot.Workers.TotalGroups != nil {
				t.Fatalf("unavailable workers = %#v, want omitted counts", snapshot.Workers)
			}
			if len(snapshot.Workers.Groups) != 0 {
				t.Fatalf("unpopulated workers = %#v, want no groups", snapshot.Workers)
			}
		})
	}
}

func TestHandlerFormatsAndEscapesOneSnapshot(t *testing.T) {
	config := testConfig()
	config.Flags = []Flag{{Name: "safe", Value: `<script>alert("x")</script>`, Source: "command line"}}
	handler := NewHandler(config, func() ([]*ateapipb.Worker, error) {
		return []*ateapipb.Worker{worker("tenant-a", "general")}, nil
	}, func() bool { return false }, nil, nil, func() time.Time {
		return config.StartedAt.Add(2*time.Hour + 3*time.Minute + 4*time.Second)
	})

	for _, tc := range []struct {
		name   string
		target string
		accept string
	}{
		{name: "query", target: "/statusz?format=json"},
		{name: "accept", target: "/statusz", accept: "text/html, application/json;q=0.9"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := requestJSON(t, handler, tc.target, tc.accept)
			if snapshot.Build.Version != "v1.2.3" || snapshot.Build.Revision != "abc123" {
				t.Fatalf("build = %#v, want v1.2.3/abc123", snapshot.Build)
			}
			if snapshot.Process.StartedAt != "2026-09-11T01:02:03Z" || snapshot.Process.Uptime != "2h3m4s" {
				t.Fatalf("process = %#v, want fixed start and uptime", snapshot.Process)
			}
			if snapshot.Readiness.Ready || snapshot.Readiness.State != "Not ready" {
				t.Fatalf("readiness = %#v, want Not ready", snapshot.Readiness)
			}
		})
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/statusz", nil))
	if got := recorder.Header().Get("Content-Type"); got != "text/html; charset=utf-8" {
		t.Fatalf("Content-Type = %q, want HTML", got)
	}
	if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	body := recorder.Body.String()
	for _, want := range []string{"ateapi Status", "v1.2.3", "abc123", "tenant-a", "general", "Not ready", "2h3m4s", "Configuration", "Known workers"} {
		if !strings.Contains(body, want) {
			t.Errorf("HTML missing %q", want)
		}
	}
	if strings.Contains(body, "<script>alert") || !strings.Contains(body, "&lt;script&gt;") {
		t.Fatal("script-shaped flag value was not HTML-escaped")
	}
	for _, deferred := range []string{"PostgreSQL diagnostics", "Recent RPC", "Golden activity", "Outbox"} {
		if strings.Contains(body, deferred) {
			t.Errorf("HTML contains deferred section %q", deferred)
		}
	}
}

func TestHandlerSetsNoStoreOnJSON(t *testing.T) {
	handler := NewHandler(testConfig(), func() ([]*ateapipb.Worker, error) { return nil, nil }, func() bool { return true }, nil, nil, time.Now)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/statusz?format=json", nil))
	if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
}

func TestDiagnosticCacheReusesFreshSampleConcurrently(t *testing.T) {
	clock := newFakeClock(testConfig().StartedAt.Add(time.Minute))
	var workerCalls atomic.Int32
	var poolCalls atomic.Int32
	var failureCalls atomic.Int32
	handler := NewHandler(testConfig(), func() ([]*ateapipb.Worker, error) {
		workerCalls.Add(1)
		return []*ateapipb.Worker{worker("tenant", "pool")}, nil
	}, func() bool { return true }, func() (PoolCounts, PoolCounts) {
		poolCalls.Add(1)
		return PoolCounts{AcquiredConns: 1}, PoolCounts{IdleConns: 1}
	}, func() []RPCFailure {
		failureCalls.Add(1)
		return []RPCFailure{{Method: "/ateapi.Control/GetActor"}}
	}, clock.Now)

	first := requestJSON(t, handler, "/statusz?format=json", "")
	if first.DiagnosticSample.RefreshInProgress || first.DiagnosticSample.Age != "0s" {
		t.Fatalf("first diagnostic sample = %#v, want completed current sample", first.DiagnosticSample)
	}

	const requests = 32
	start := make(chan struct{})
	responses := make(chan jsonHandlerResponse, requests)
	var wg sync.WaitGroup
	for range requests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			responses <- invokeJSON(handler)
		}()
	}
	close(start)
	wg.Wait()
	close(responses)
	for response := range responses {
		if response.err != nil || response.code != http.StatusOK {
			t.Fatalf("fresh cached response = code %d, error %v", response.code, response.err)
		}
		if response.snapshot.DiagnosticSample != first.DiagnosticSample {
			t.Fatalf("fresh diagnostic sample = %#v, want %#v", response.snapshot.DiagnosticSample, first.DiagnosticSample)
		}
	}
	if got := []int32{workerCalls.Load(), poolCalls.Load(), failureCalls.Load()}; got[0] != 1 || got[1] != 1 || got[2] != 1 {
		t.Fatalf("reader calls = %v, want [1 1 1]", got)
	}
}

func TestDiagnosticCacheFirstConcurrentRequestFailsFast(t *testing.T) {
	clock := newFakeClock(testConfig().StartedAt.Add(time.Minute))
	workerEntered := make(chan struct{})
	releaseWorker := make(chan struct{})
	var workerCalls atomic.Int32
	var poolCalls atomic.Int32
	var failureCalls atomic.Int32
	handler := NewHandler(testConfig(), func() ([]*ateapipb.Worker, error) {
		workerCalls.Add(1)
		close(workerEntered)
		<-releaseWorker
		return nil, nil
	}, func() bool { return true }, func() (PoolCounts, PoolCounts) {
		poolCalls.Add(1)
		return PoolCounts{}, PoolCounts{}
	}, func() []RPCFailure {
		failureCalls.Add(1)
		return nil
	}, clock.Now)

	firstDone := make(chan jsonHandlerResponse, 1)
	go func() { firstDone <- invokeJSON(handler) }()
	<-workerEntered
	secondDone := make(chan jsonHandlerResponse, 1)
	go func() { secondDone <- invokeJSON(handler) }()
	second := awaitJSONResponse(t, secondDone)
	if second.code != http.StatusServiceUnavailable || second.retryAfter != "1" {
		t.Fatalf("concurrent first-fill response = code %d Retry-After %q, want 503 and 1", second.code, second.retryAfter)
	}
	close(releaseWorker)
	first := awaitJSONResponse(t, firstDone)
	if first.err != nil || first.code != http.StatusOK {
		t.Fatalf("first collector response = code %d, error %v", first.code, first.err)
	}
	if got := []int32{workerCalls.Load(), poolCalls.Load(), failureCalls.Load()}; got[0] != 1 || got[1] != 1 || got[2] != 1 {
		t.Fatalf("reader calls = %v, want [1 1 1]", got)
	}
}

func TestDiagnosticCacheServesStaleSampleDuringRefresh(t *testing.T) {
	clock := newFakeClock(testConfig().StartedAt.Add(time.Minute))
	refreshEntered := make(chan struct{})
	releaseRefresh := make(chan struct{})
	var version atomic.Int32
	var workerCalls atomic.Int32
	var poolCalls atomic.Int32
	var failureCalls atomic.Int32
	handler := NewHandler(testConfig(), func() ([]*ateapipb.Worker, error) {
		call := workerCalls.Add(1)
		if call == 2 {
			close(refreshEntered)
			<-releaseRefresh
		}
		return []*ateapipb.Worker{worker("tenant", "pool-"+threeDigits(int(version.Load())))}, nil
	}, func() bool { return true }, func() (PoolCounts, PoolCounts) {
		poolCalls.Add(1)
		return PoolCounts{AcquiredConns: version.Load()}, PoolCounts{}
	}, func() []RPCFailure {
		failureCalls.Add(1)
		return []RPCFailure{{Method: "/ateapi.Control/Call" + threeDigits(int(version.Load()))}}
	}, clock.Now)

	initial := requestJSON(t, handler, "/statusz?format=json", "")
	version.Store(1)
	clock.Advance(5 * time.Second)
	refreshDone := make(chan jsonHandlerResponse, 1)
	go func() { refreshDone <- invokeJSON(handler) }()
	<-refreshEntered

	staleDone := make(chan jsonHandlerResponse, 1)
	go func() { staleDone <- invokeJSON(handler) }()
	stale := awaitJSONResponse(t, staleDone)
	if stale.err != nil || stale.code != http.StatusOK {
		t.Fatalf("stale response = code %d, error %v", stale.code, stale.err)
	}
	if !stale.snapshot.DiagnosticSample.RefreshInProgress || stale.snapshot.DiagnosticSample.Age != "5s" || stale.snapshot.DiagnosticSample.SampledAt != initial.DiagnosticSample.SampledAt {
		t.Fatalf("stale diagnostic sample = %#v, want age 5s with refresh in progress", stale.snapshot.DiagnosticSample)
	}
	if got := stale.snapshot.Workers.Groups[0].Pool; got != "pool-000" {
		t.Fatalf("stale worker pool = %q, want pool-000", got)
	}
	if got := stale.snapshot.PostgreSQLPools.Pools[0].AcquiredConns; got != 0 {
		t.Fatalf("stale acquired connections = %d, want 0", got)
	}
	if got := stale.snapshot.RecentControlFailures.Failures[0].Method; got != "/ateapi.Control/Call000" {
		t.Fatalf("stale failure method = %q, want Call000", got)
	}

	close(releaseRefresh)
	refreshed := awaitJSONResponse(t, refreshDone)
	if refreshed.err != nil || refreshed.code != http.StatusOK {
		t.Fatalf("refresh response = code %d, error %v", refreshed.code, refreshed.err)
	}
	if refreshed.snapshot.DiagnosticSample.RefreshInProgress || refreshed.snapshot.DiagnosticSample.Age != "0s" {
		t.Fatalf("refreshed diagnostic sample = %#v, want completed current sample", refreshed.snapshot.DiagnosticSample)
	}
	if got := refreshed.snapshot.Workers.Groups[0].Pool; got != "pool-001" {
		t.Fatalf("refreshed worker pool = %q, want pool-001", got)
	}
	if got := []int32{workerCalls.Load(), poolCalls.Load(), failureCalls.Load()}; got[0] != 2 || got[1] != 2 || got[2] != 2 {
		t.Fatalf("reader calls = %v, want [2 2 2]", got)
	}
}

func TestDiagnosticCacheExpiryStartsAtRefreshCompletion(t *testing.T) {
	clock := newFakeClock(testConfig().StartedAt.Add(time.Minute))
	refreshEntered := make(chan struct{})
	releaseRefresh := make(chan struct{})
	var workerCalls atomic.Int32
	handler := NewHandler(testConfig(), func() ([]*ateapipb.Worker, error) {
		if workerCalls.Add(1) == 2 {
			close(refreshEntered)
			<-releaseRefresh
		}
		return nil, nil
	}, func() bool { return true }, nil, nil, clock.Now)

	_ = requestJSON(t, handler, "/statusz?format=json", "")
	clock.Advance(5 * time.Second)
	refreshDone := make(chan jsonHandlerResponse, 1)
	go func() { refreshDone <- invokeJSON(handler) }()
	<-refreshEntered
	clock.Advance(4 * time.Second)
	close(releaseRefresh)
	refreshed := awaitJSONResponse(t, refreshDone)
	if refreshed.err != nil || refreshed.code != http.StatusOK {
		t.Fatalf("refresh response = code %d, error %v", refreshed.code, refreshed.err)
	}

	clock.Advance(4 * time.Second)
	fresh := requestJSON(t, handler, "/statusz?format=json", "")
	if fresh.DiagnosticSample.Age != "4s" || workerCalls.Load() != 2 {
		t.Fatalf("four seconds after completion: sample=%#v reader calls=%d, want fresh cache and 2 calls", fresh.DiagnosticSample, workerCalls.Load())
	}
	clock.Advance(time.Second)
	expired := requestJSON(t, handler, "/statusz?format=json", "")
	if expired.DiagnosticSample.Age != "0s" || workerCalls.Load() != 3 {
		t.Fatalf("five seconds after completion: sample=%#v reader calls=%d, want refreshed cache and 3 calls", expired.DiagnosticSample, workerCalls.Load())
	}
}

func TestDiagnosticCacheKeepsReadinessAndUptimeLive(t *testing.T) {
	config := testConfig()
	clock := newFakeClock(config.StartedAt.Add(10 * time.Second))
	var ready atomic.Bool
	var workerCalls atomic.Int32
	handler := NewHandler(config, func() ([]*ateapipb.Worker, error) {
		workerCalls.Add(1)
		return nil, nil
	}, ready.Load, nil, nil, clock.Now)

	initial := requestJSON(t, handler, "/statusz?format=json", "")
	ready.Store(true)
	clock.Advance(time.Second)
	updated := requestJSON(t, handler, "/statusz?format=json", "")
	if initial.Readiness.Ready || !updated.Readiness.Ready {
		t.Fatalf("readiness initial=%#v updated=%#v, want false then true", initial.Readiness, updated.Readiness)
	}
	if initial.Process.Uptime != "10s" || updated.Process.Uptime != "11s" {
		t.Fatalf("uptime initial=%q updated=%q, want 10s then 11s", initial.Process.Uptime, updated.Process.Uptime)
	}
	if updated.DiagnosticSample.Age != "1s" || workerCalls.Load() != 1 {
		t.Fatalf("updated sample=%#v reader calls=%d, want one-second-old cached aggregate and 1 call", updated.DiagnosticSample, workerCalls.Load())
	}
}

func TestSnapshotDistinguishesUnavailableAndZeroPoolUsage(t *testing.T) {
	tests := []struct {
		name      string
		pools     PoolReader
		available bool
	}{
		{name: "unavailable"},
		{
			name: "available with zero occupancy",
			pools: func() (PoolCounts, PoolCounts) {
				return PoolCounts{}, PoolCounts{}
			},
			available: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := NewHandler(testConfig(), func() ([]*ateapipb.Worker, error) { return nil, nil }, func() bool { return true }, tt.pools, nil, time.Now)
			snapshot := requestJSON(t, handler, "/statusz?format=json", "")
			if snapshot.PostgreSQLPools.Available != tt.available {
				t.Fatalf("pool availability = %t, want %t", snapshot.PostgreSQLPools.Available, tt.available)
			}
			if !tt.available {
				if len(snapshot.PostgreSQLPools.Pools) != 0 {
					t.Fatalf("unavailable pool rows = %#v, want none", snapshot.PostgreSQLPools.Pools)
				}
				return
			}
			if len(snapshot.PostgreSQLPools.Pools) != 2 {
				t.Fatalf("pool rows = %#v, want operational and watch", snapshot.PostgreSQLPools.Pools)
			}
			if got := snapshot.PostgreSQLPools.Pools[0]; got.Role != "Operational" || got.AcquiredConns != 0 || got.IdleConns != 0 || got.MaxConns != 0 {
				t.Fatalf("operational pool = %#v, want explicit zero values", got)
			}
			if got := snapshot.PostgreSQLPools.Pools[1]; got.Role != "Watch" || got.AcquiredConns != 0 || got.IdleConns != 0 || got.MaxConns != 0 {
				t.Fatalf("watch pool = %#v, want explicit zero values", got)
			}
		})
	}
}

func TestDiagnosticsHTMLJSONParityAndSafeFields(t *testing.T) {
	completed := "2026-09-12T04:05:06.025Z"
	sampledAt := time.Date(2026, 9, 12, 4, 6, 0, 0, time.UTC)
	failures := []RPCFailure{{
		CompletedAt:   completed,
		Method:        "/ateapi.Control/<script>bad()</script>",
		PrincipalKind: "jwt",
		PrincipalID:   "<img src=x onerror=\"bad()\">",
		Code:          "Internal",
		Elapsed:       "25ms",
	}}
	handler := NewHandler(testConfig(), func() ([]*ateapipb.Worker, error) { return nil, nil }, func() bool { return true }, func() (PoolCounts, PoolCounts) {
		return PoolCounts{AcquiredConns: 2, IdleConns: 3, MaxConns: 10}, PoolCounts{AcquiredConns: 1, IdleConns: 2, MaxConns: 3}
	}, func() []RPCFailure { return failures }, func() time.Time { return sampledAt })

	snapshot := requestJSON(t, handler, "/statusz?format=json", "")
	if snapshot.DiagnosticSample.SampledAt != "2026-09-12T04:06:00Z" || snapshot.DiagnosticSample.Age != "0s" || snapshot.DiagnosticSample.RefreshInProgress {
		t.Fatalf("JSON diagnostic sample = %#v, want completed current sample", snapshot.DiagnosticSample)
	}
	if !snapshot.PostgreSQLPools.Available || len(snapshot.PostgreSQLPools.Pools) != 2 {
		t.Fatalf("JSON pool snapshot = %#v", snapshot.PostgreSQLPools)
	}
	if !strings.Contains(snapshot.PostgreSQLPools.Coverage, "replica-local") || !strings.Contains(snapshot.PostgreSQLPools.Coverage, "not a connectivity health check") {
		t.Fatalf("JSON pool coverage = %q", snapshot.PostgreSQLPools.Coverage)
	}
	if got := snapshot.PostgreSQLPools.Pools[0]; got.Role != "Operational" || got.AcquiredConns != 2 || got.IdleConns != 3 || got.MaxConns != 10 {
		t.Fatalf("JSON operational pool = %#v", got)
	}
	if len(snapshot.RecentControlFailures.Failures) != 1 || snapshot.RecentControlFailures.Empty {
		t.Fatalf("JSON failure history = %#v", snapshot.RecentControlFailures)
	}
	if !strings.Contains(snapshot.RecentControlFailures.Coverage, "authenticated unary Control") || !strings.Contains(snapshot.RecentControlFailures.Retention, "resets on process restart") || !strings.Contains(snapshot.RecentControlFailures.Retention, "not an error rate") {
		t.Fatalf("JSON failure semantics = coverage %q, retention %q", snapshot.RecentControlFailures.Coverage, snapshot.RecentControlFailures.Retention)
	}
	if got := snapshot.RecentControlFailures.Failures[0]; got.CompletedAt != completed || got.Code != "Internal" || got.Elapsed != "25ms" {
		t.Fatalf("JSON failure = %#v", got)
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/statusz", nil))
	body := recorder.Body.String()
	for _, want := range []string{
		"PostgreSQL pool occupancy", "Operational", "Watch", "2", "3", "10",
		"Recent failed Control RPCs", completed, "Internal", "25ms",
		"Diagnostic sample", "Sample completed", "2026-09-12T04:06:00Z", "Cache age", "0s", "Refresh in progress", "No",
		"as of the diagnostic sample", "replica-local", "resets on process restart", "not a connectivity health check", "not an error rate",
		"&lt;script&gt;bad()&lt;/script&gt;", "&lt;img src=x onerror=&#34;bad()&#34;&gt;",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("HTML missing %q", want)
		}
	}
	for _, unsafe := range []string{"<script>bad()", "<img src=x"} {
		if strings.Contains(body, unsafe) {
			t.Errorf("HTML contains unescaped value %q", unsafe)
		}
	}
}

func TestEmptyFailureHistoryStatesCoverageWithoutClaimingSuccess(t *testing.T) {
	handler := NewHandler(testConfig(), func() ([]*ateapipb.Worker, error) { return nil, nil }, func() bool { return true }, nil, func() []RPCFailure { return nil }, time.Now)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/statusz", nil))
	body := recorder.Body.String()
	if !strings.Contains(body, "No matching failures are retained on this replica") {
		t.Fatalf("empty history does not state retained-sample semantics: %s", body)
	}
	for _, misleading := range []string{"system is healthy", "no rpc failures occurred", "all calls succeeded"} {
		if strings.Contains(strings.ToLower(body), misleading) {
			t.Errorf("empty history contains misleading claim %q", misleading)
		}
	}
}

// TestRenderSeededDashboard can also materialize the actual embedded template
// for local visual review without adding a second rendering path.
func TestRenderSeededDashboard(t *testing.T) {
	handler := NewHandler(testConfig(), func() ([]*ateapipb.Worker, error) {
		return []*ateapipb.Worker{
			worker("research", "general"),
			worker("research", "general"),
			worker("support", "latency-sensitive"),
		}, nil
	}, func() bool { return true }, func() (PoolCounts, PoolCounts) {
		return PoolCounts{AcquiredConns: 7, IdleConns: 13, MaxConns: 40}, PoolCounts{AcquiredConns: 1, IdleConns: 2, MaxConns: 3}
	}, func() []RPCFailure {
		return []RPCFailure{
			{CompletedAt: "2026-09-11T01:38:10Z", Method: "/ateapi.Control/ResumeActor", PrincipalKind: "jwt", PrincipalID: "operator@example.com", Code: "FailedPrecondition", Elapsed: "187ms"},
			{CompletedAt: "2026-09-11T01:37:42Z", Method: "/ateapi.Control/CreateActor", PrincipalKind: "mtls", PrincipalID: "spiffe://cluster.local/ns/ate-system/sa/controller", Code: "Unavailable", Elapsed: "1.204s"},
		}
	}, func() time.Time {
		return testConfig().StartedAt.Add(37*time.Minute + 12*time.Second)
	})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/statusz", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if path := os.Getenv("STATUSZ_RENDER_PATH"); path != "" {
		if err := os.WriteFile(path, recorder.Body.Bytes(), 0o644); err != nil {
			t.Fatalf("write seeded dashboard: %v", err)
		}
	}
}

func requestJSON(t *testing.T, handler http.Handler, target, accept string) Snapshot {
	t.Helper()
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.Header.Set("Accept", accept)
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	var snapshot Snapshot
	if err := json.Unmarshal(recorder.Body.Bytes(), &snapshot); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	return snapshot
}

type jsonHandlerResponse struct {
	code       int
	retryAfter string
	snapshot   Snapshot
	err        error
}

func invokeJSON(handler http.Handler) jsonHandlerResponse {
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/statusz?format=json", nil))
	response := jsonHandlerResponse{code: recorder.Code, retryAfter: recorder.Header().Get("Retry-After")}
	if recorder.Code == http.StatusOK {
		response.err = json.Unmarshal(recorder.Body.Bytes(), &response.snapshot)
	}
	return response
}

func awaitJSONResponse(t *testing.T, response <-chan jsonHandlerResponse) jsonHandlerResponse {
	t.Helper()
	select {
	case got := <-response:
		return got
	case <-time.After(time.Second):
		t.Fatal("status request did not complete")
		return jsonHandlerResponse{}
	}
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{now: now}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(elapsed time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(elapsed)
}

func testConfig() Config {
	return Config{
		Build:     Build{Version: "v1.2.3", Revision: "abc123"},
		StartedAt: time.Date(2026, 9, 11, 1, 2, 3, 0, time.UTC),
		Listeners: Listeners{GRPC: ":443", Metrics: ":9090", Status: ":4040"},
		Drain:     Drain{Delay: "13s", Timeout: "15s"},
		Flags: []Flag{
			{Name: "grpc-listen-addr", Value: ":443", Source: "default"},
			{Name: "postgres-connection-string", Value: "[redacted]", Source: "environment: ATE_API_POSTGRES_CONNECTION_STRING"},
		},
	}
}

func worker(namespace, pool string) *ateapipb.Worker {
	return &ateapipb.Worker{WorkerNamespace: namespace, WorkerPool: pool}
}

func threeDigits(value int) string {
	return string(rune('0'+value/100)) + string(rune('0'+value/10%10)) + string(rune('0'+value%10))
}

func intValue(value *int) int {
	if value == nil {
		return -1
	}
	return *value
}
