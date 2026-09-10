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
	}, func() bool { return true }, func() time.Time {
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
			handler := NewHandler(testConfig(), tt.workers, func() bool { return true }, func() time.Time {
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
	}, func() bool { return false }, func() time.Time {
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
	handler := NewHandler(testConfig(), func() ([]*ateapipb.Worker, error) { return nil, nil }, func() bool { return true }, time.Now)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/statusz?format=json", nil))
	if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
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
	}, func() bool { return true }, func() time.Time {
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
