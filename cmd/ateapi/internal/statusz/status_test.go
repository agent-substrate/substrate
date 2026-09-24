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
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestSnapshotCountsKnownWorkersWithoutNames(t *testing.T) {
	workers := []*ateapipb.Worker{
		worker("private-namespace", "private-pool"),
		nil,
		worker("another-private-namespace", "another-private-pool"),
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
	if intValue(snapshot.Workers.TotalWorkers) != 2 || snapshot.Workers.Empty {
		t.Fatalf("worker total = %v, empty = %t; want 2, false", snapshot.Workers.TotalWorkers, snapshot.Workers.Empty)
	}
	encoded, _ := json.Marshal(snapshot)
	if strings.Contains(string(encoded), "private-") || strings.Contains(string(encoded), "groups") {
		t.Fatalf("worker names or groups leaked: %s", encoded)
	}
	if !slices.Equal(workers, original) {
		t.Fatal("snapshot assembly reordered the worker-cache slice")
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
				if snapshot.Workers.TotalWorkers == nil || *snapshot.Workers.TotalWorkers != 0 {
					t.Fatalf("ready empty workers = %#v, want explicit zero counts", snapshot.Workers)
				}
			} else if snapshot.Workers.TotalWorkers != nil {
				t.Fatalf("unavailable workers = %#v, want omitted counts", snapshot.Workers)
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
	for _, want := range []string{"ateapi Status", "v1.2.3", "abc123", "Not ready", "2h3m4s", "Configuration", "Known workers", "1 workers"} {
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
	if strings.Contains(recorder.Body.String(), `"diagnostic_sample"`) {
		t.Fatal("JSON contains removed diagnostic sample metadata")
	}
}

func TestHandlerReadsFreshDiagnosticsForEachRequest(t *testing.T) {
	workerCalls := 0
	poolCalls := 0
	failureCalls := 0
	handler := NewHandler(testConfig(), func() ([]*ateapipb.Worker, error) {
		workerCalls++
		workers := []*ateapipb.Worker{worker("tenant", "pool-a")}
		if workerCalls > 1 {
			workers = append(workers, worker("tenant", "pool-b"))
		}
		return workers, nil
	}, func() bool { return true }, func() (PoolCounts, PoolCounts) {
		poolCalls++
		return PoolCounts{AcquiredConns: int32(poolCalls)}, PoolCounts{}
	}, func() []RPCFailure {
		failureCalls++
		return []RPCFailure{{Method: "/ateapi.Control/Call" + threeDigits(failureCalls)}}
	}, func() time.Time { return testConfig().StartedAt.Add(time.Minute) })

	_ = requestJSON(t, handler, "/statusz?format=json", "")
	second := requestJSON(t, handler, "/statusz?format=json", "")
	if workerCalls != 2 || poolCalls != 2 || failureCalls != 2 {
		t.Fatalf("reader calls = [%d %d %d], want [2 2 2]", workerCalls, poolCalls, failureCalls)
	}
	if got := intValue(second.Workers.TotalWorkers); got != 2 {
		t.Fatalf("second request worker count = %d, want 2", got)
	}
	if got := second.PostgreSQLPools.Pools[0].AcquiredConns; got != 2 {
		t.Fatalf("second request acquired connections = %d, want 2", got)
	}
	if got := second.RecentControlFailures.Failures[0].Method; got != "/ateapi.Control/Call002" {
		t.Fatalf("second request failure method = %q, want Call002", got)
	}
}

func TestDashboardRefreshControlReloadsWholePage(t *testing.T) {
	handler := NewHandler(testConfig(), func() ([]*ateapipb.Worker, error) { return nil, nil }, func() bool { return true }, nil, nil, time.Now)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/statusz", nil))
	body := recorder.Body.String()
	for _, want := range []string{
		`id="refresh-interval"`, `onchange="setRefresh()"`, `aria-label="Refresh interval"`,
		`<option value="0">Manual Refresh</option>`,
		`<option value="5">Refresh: 5s</option>`,
		`<option value="10" selected>Refresh: 10s</option>`,
		`<option value="30">Refresh: 30s</option>`,
		`clearTimeout(refreshTimer)`, `window.location.reload()`, `val * 1000`, `setRefresh()`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("HTML refresh control missing %q", want)
		}
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
	failures := []RPCFailure{{
		CompletedAt: completed,
		Method:      "/ateapi.Control/<script>bad()</script>",
		Code:        "Internal",
		Elapsed:     "25ms",
	}}
	handler := NewHandler(testConfig(), func() ([]*ateapipb.Worker, error) { return nil, nil }, func() bool { return true }, func() (PoolCounts, PoolCounts) {
		return PoolCounts{AcquiredConns: 2, IdleConns: 3, MaxConns: 10}, PoolCounts{AcquiredConns: 1, IdleConns: 2, MaxConns: 3}
	}, func() []RPCFailure { return failures }, func() time.Time { return testConfig().StartedAt.Add(time.Minute) })

	snapshot := requestJSON(t, handler, "/statusz?format=json", "")
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
		"read for this request", "replica-local", "resets on process restart", "not a connectivity health check", "not an error rate",
		"&lt;script&gt;bad()&lt;/script&gt;",
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
			{CompletedAt: "2026-09-11T01:38:10Z", Method: "/ateapi.Control/ResumeActor", Code: "FailedPrecondition", Elapsed: "187ms"},
			{CompletedAt: "2026-09-11T01:37:42Z", Method: "/ateapi.Control/CreateActor", Code: "Unavailable", Elapsed: "1.204s"},
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
