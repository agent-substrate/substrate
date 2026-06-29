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

package readyz

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

func TestURL(t *testing.T) {
	tests := []struct {
		name    string
		probe   *ateompb.Readyz
		actorIP string
		want    string
		wantErr bool
	}{
		{
			name:    "default path",
			probe:   &ateompb.Readyz{HttpGet: &ateompb.HTTPGetAction{Port: 8080}},
			actorIP: "169.254.17.2",
			want:    "http://169.254.17.2:8080/readyz",
		},
		{
			name:    "explicit path",
			probe:   &ateompb.Readyz{HttpGet: &ateompb.HTTPGetAction{Path: "/health", Port: 9000}},
			actorIP: "169.254.17.2",
			want:    "http://169.254.17.2:9000/health",
		},
		{
			name:    "path without leading slash is normalized",
			probe:   &ateompb.Readyz{HttpGet: &ateompb.HTTPGetAction{Path: "ready", Port: 80}},
			actorIP: "10.0.0.1",
			want:    "http://10.0.0.1:80/ready",
		},
		{
			name:    "missing httpGet",
			probe:   &ateompb.Readyz{},
			actorIP: "1.2.3.4",
			wantErr: true,
		},
		{
			name:    "port zero",
			probe:   &ateompb.Readyz{HttpGet: &ateompb.HTTPGetAction{Port: 0}},
			actorIP: "1.2.3.4",
			wantErr: true,
		},
		{
			name:    "port too large",
			probe:   &ateompb.Readyz{HttpGet: &ateompb.HTTPGetAction{Port: 70000}},
			actorIP: "1.2.3.4",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := URL(tt.probe, tt.actorIP)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("URL = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestWait_ReturnsOnFirst200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ip, port := splitHostPort(t, srv.URL)
	probe := &ateompb.Readyz{HttpGet: &ateompb.HTTPGetAction{Port: int32(port)}}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := Wait(ctx, "main", probe, ip); err != nil {
		t.Fatalf("Wait returned error: %v", err)
	}
}

func TestWait_WaitsForServerToBecomeReady(t *testing.T) {
	var ready atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "not yet", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ip, port := splitHostPort(t, srv.URL)
	probe := &ateompb.Readyz{HttpGet: &ateompb.HTTPGetAction{Port: int32(port)}}

	flipAt := time.Now().Add(50 * time.Millisecond)
	go func() {
		time.Sleep(time.Until(flipAt))
		ready.Store(true)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	if err := Wait(ctx, "main", probe, ip); err != nil {
		t.Fatalf("Wait returned error: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < 40*time.Millisecond {
		t.Errorf("Wait returned suspiciously early: %v (server became ready at +50ms)", elapsed)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("Wait returned too late: %v (expected ~50ms + poll interval)", elapsed)
	}
}

func TestWait_ContextCancellation(t *testing.T) {
	// Bind a port and immediately close to ensure connect-refused, so the
	// poll loop is exercised but no server ever returns 200.
	port := pickFreePort(t)
	probe := &ateompb.Readyz{HttpGet: &ateompb.HTTPGetAction{Port: int32(port)}}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	err := Wait(ctx, "main", probe, "127.0.0.1")
	if err == nil {
		t.Fatalf("Wait returned nil, expected cancellation error")
	}
}

func TestWaitAll_SkipsContainersWithoutProbe(t *testing.T) {
	// No server bound, but no probes => should return nil immediately.
	containers := []*ateompb.Container{
		{Name: "a"},
		{Name: "b"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := WaitAll(ctx, containers, "127.0.0.1"); err != nil {
		t.Fatalf("WaitAll with no probes returned error: %v", err)
	}
}

func splitHostPort(t *testing.T, raw string) (string, int) {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	host, portStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		t.Fatalf("split host:port: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("port atoi: %v", err)
	}
	return host, port
}

func pickFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}
