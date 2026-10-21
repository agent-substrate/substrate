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

package wakeupprobe

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

func TestURL(t *testing.T) {
	tests := []struct {
		name    string
		probe   *ateworkerpb.WakeupProbe
		actorIP string
		want    string
		wantErr bool
	}{
		{
			name:    "missing path",
			probe:   &ateworkerpb.WakeupProbe{HttpGet: &ateworkerpb.HttpGetAction{Port: 8080}},
			actorIP: "169.254.17.2",
			wantErr: true,
		},
		{
			name:    "explicit path",
			probe:   &ateworkerpb.WakeupProbe{HttpGet: &ateworkerpb.HttpGetAction{Path: "/health", Port: 9000}},
			actorIP: "169.254.17.2",
			want:    "http://169.254.17.2:9000/health",
		},
		{
			name:    "path without leading slash is normalized",
			probe:   &ateworkerpb.WakeupProbe{HttpGet: &ateworkerpb.HttpGetAction{Path: "ready", Port: 80}},
			actorIP: "10.0.0.1",
			want:    "http://10.0.0.1:80/ready",
		},
		{
			name:    "missing httpGet",
			probe:   &ateworkerpb.WakeupProbe{},
			actorIP: "1.2.3.4",
			wantErr: true,
		},
		{
			name:    "port zero",
			probe:   &ateworkerpb.WakeupProbe{HttpGet: &ateworkerpb.HttpGetAction{Port: 0}},
			actorIP: "1.2.3.4",
			wantErr: true,
		},
		{
			name:    "port too large",
			probe:   &ateworkerpb.WakeupProbe{HttpGet: &ateworkerpb.HttpGetAction{Port: 70000}},
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
	probe := &ateworkerpb.WakeupProbe{HttpGet: &ateworkerpb.HttpGetAction{Path: "/readyz", Port: int32(port)}, TimeoutSeconds: 1}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := Wait(ctx, "main", probe, ip, nil); err != nil {
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
	probe := &ateworkerpb.WakeupProbe{HttpGet: &ateworkerpb.HttpGetAction{Path: "/readyz", Port: int32(port)}, TimeoutSeconds: 1}

	flipAt := time.Now().Add(50 * time.Millisecond)
	go func() {
		time.Sleep(time.Until(flipAt))
		ready.Store(true)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	if err := Wait(ctx, "main", probe, ip, nil); err != nil {
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
	probe := &ateworkerpb.WakeupProbe{HttpGet: &ateworkerpb.HttpGetAction{Path: "/readyz", Port: int32(port)}, TimeoutSeconds: 1}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	err := Wait(ctx, "main", probe, "127.0.0.1", nil)
	if err == nil {
		t.Fatalf("Wait returned nil, expected cancellation error")
	}
}

func TestPollTimeout(t *testing.T) {
	tests := []struct {
		name    string
		probe   *ateworkerpb.WakeupProbe
		want    time.Duration
		wantErr bool
	}{
		{
			name:    "unset is rejected",
			probe:   &ateworkerpb.WakeupProbe{},
			wantErr: true,
		},
		{
			name:  "explicit value is honored",
			probe: &ateworkerpb.WakeupProbe{TimeoutSeconds: 300},
			want:  300 * time.Second,
		},
		{
			name:    "negative is rejected",
			probe:   &ateworkerpb.WakeupProbe{TimeoutSeconds: -1},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := pollTimeout(tt.probe)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("pollTimeout = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWait_GivesUpAtProbeTimeout(t *testing.T) {
	// Nothing ever binds this port, so the poll loop runs until the
	// probe's own deadline rather than the package default.
	port := pickFreePort(t)
	probe := &ateworkerpb.WakeupProbe{
		HttpGet:        &ateworkerpb.HttpGetAction{Path: "/readyz", Port: int32(port)},
		TimeoutSeconds: 1,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	err := Wait(ctx, "main", probe, "127.0.0.1", nil)
	if err == nil {
		t.Fatalf("Wait returned nil, expected a timeout error")
	}
	if !strings.Contains(err.Error(), "never returned 200") {
		t.Errorf("Wait error = %v, want a probe-timeout error", err)
	}
	elapsed := time.Since(start)
	if elapsed < time.Second {
		t.Errorf("Wait gave up after %v, before the probe's 1s timeout", elapsed)
	}
	if elapsed > 5*time.Second {
		t.Errorf("Wait took %v; the probe's 1s timeout was not honored", elapsed)
	}
}

func TestWaitAll_SkipsContainersWithoutProbe(t *testing.T) {
	// No server bound, but no probes => should return nil immediately.
	containers := []*ateworkerpb.Container{
		{Name: "a"},
		{Name: "b"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := WaitAll(ctx, containers, "127.0.0.1", nil); err != nil {
		t.Fatalf("WaitAll with no probes returned error: %v", err)
	}
}

func TestWaitFast_RecoversQuicklyFromStalledFirstAttempt(t *testing.T) {
	for _, tc := range []struct {
		name string
		wait func(ctx context.Context, probe *ateworkerpb.WakeupProbe, ip string, dial DialFunc) error
	}{
		{
			name: "WaitAllFast",
			wait: func(ctx context.Context, probe *ateworkerpb.WakeupProbe, ip string, dial DialFunc) error {
				return WaitAllFast(ctx, []*ateworkerpb.Container{{Name: "app", WakeupProbe: probe}}, ip, dial)
			},
		},
		{
			name: "WaitFast",
			wait: func(ctx context.Context, probe *ateworkerpb.WakeupProbe, ip string, dial DialFunc) error {
				return WaitFast(ctx, "app", probe, ip, dial)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()

			ip, port := splitHostPort(t, srv.URL)
			probe := &ateworkerpb.WakeupProbe{
				HttpGet:        &ateworkerpb.HttpGetAction{Path: "/readyz", Port: int32(port)},
				TimeoutSeconds: 2,
			}

			var attempts atomic.Int32
			baseDialer := &net.Dialer{}
			dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
				if attempts.Add(1) == 1 {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return baseDialer.DialContext(ctx, network, addr)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			start := time.Now()
			if err := tc.wait(ctx, probe, ip, dial); err != nil {
				t.Fatalf("%s returned error: %v", tc.name, err)
			}
			elapsed := time.Since(start)
			if elapsed >= RequestTimeout {
				t.Errorf("%s took %v after stalled first dial, want < %v", tc.name, elapsed, RequestTimeout)
			}
			if got := attempts.Load(); got < 2 {
				t.Errorf("dial attempts = %d, want >= 2", got)
			}
		})
	}
}

func TestFastAttemptTimeout(t *testing.T) {
	for attempt, want := range map[int]time.Duration{
		1:  15 * time.Millisecond,
		3:  15 * time.Millisecond,
		4:  40 * time.Millisecond,
		8:  40 * time.Millisecond,
		9:  RequestTimeout,
		20: RequestTimeout,
	} {
		if got := fastAttemptTimeout(attempt); got != want {
			t.Errorf("fastAttemptTimeout(%d) = %v, want %v", attempt, got, want)
		}
	}
}

func BenchmarkWakeupProbe_StalledFirstAttempt(b *testing.B) {
	origLogger := slog.Default()
	slog.SetDefault(slog.New(slog.DiscardHandler))
	b.Cleanup(func() { slog.SetDefault(origLogger) })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	if err != nil {
		b.Fatal(err)
	}
	ip, portStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		b.Fatal(err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		b.Fatal(err)
	}
	probe := &ateworkerpb.WakeupProbe{
		HttpGet:        &ateworkerpb.HttpGetAction{Path: "/readyz", Port: int32(port)},
		TimeoutSeconds: 2,
	}
	baseDialer := &net.Dialer{}

	b.Run("Wait_DefaultTimeout", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			var attempts atomic.Int32
			dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
				if attempts.Add(1) == 1 {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return baseDialer.DialContext(ctx, network, addr)
			}
			if err := Wait(b.Context(), "app", probe, ip, dial); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("WaitFast_AdaptiveTimeout", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			var attempts atomic.Int32
			dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
				if attempts.Add(1) == 1 {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return baseDialer.DialContext(ctx, network, addr)
			}
			if err := WaitFast(b.Context(), "app", probe, ip, dial); err != nil {
				b.Fatal(err)
			}
		}
	})
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
