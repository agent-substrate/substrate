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
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestHandlerFormatsAndEscapesOneSnapshot(t *testing.T) {
	config := testConfig()
	config.Flags = []Flag{{Name: "safe", Value: `<script>alert("x")</script>`, Source: "command line"}}
	handler := NewHandler(config, func() bool { return false }, func() time.Time {
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
	for _, want := range []string{"ateapi Status", "v1.2.3", "abc123", "Not ready", "2h3m4s", "Configuration"} {
		if !strings.Contains(body, want) {
			t.Errorf("HTML missing %q", want)
		}
	}
	if strings.Contains(body, "<script>alert") || !strings.Contains(body, "&lt;script&gt;") {
		t.Fatal("script-shaped flag value was not HTML-escaped")
	}
	for _, deferred := range []string{"Known workers", "PostgreSQL diagnostics", "Recent RPC", "Golden activity", "Outbox"} {
		if strings.Contains(body, deferred) {
			t.Errorf("HTML contains deferred section %q", deferred)
		}
	}
}

func TestHandlerSetsNoStoreOnJSON(t *testing.T) {
	handler := NewHandler(testConfig(), func() bool { return true }, time.Now)
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
	handler := NewHandler(testConfig(), func() bool { return true }, func() time.Time {
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
