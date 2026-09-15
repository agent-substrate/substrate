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
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	// A cancelled probe is ateom draining, not the actor failing. Tagging it
	// would attribute a node drain to the workload.
	if errors.Is(err, ateerrors.ReasonWorkloadNotReady) {
		t.Errorf("Wait tagged a cancellation with %v: %v", ateerrors.ReasonWorkloadNotReady, err)
	}
}

func TestOverallTimeout(t *testing.T) {
	tests := []struct {
		name  string
		probe *ateompb.Readyz
		want  time.Duration
	}{
		{
			name:  "unset falls back to the default",
			probe: &ateompb.Readyz{},
			want:  DefaultOverallTimeout,
		},
		{
			name:  "explicit value is honored",
			probe: &ateompb.Readyz{TimeoutSeconds: 300},
			want:  300 * time.Second,
		},
		{
			// A zero deadline could never be met, so it means "unset"
			// rather than "fail immediately".
			name:  "negative falls back to the default",
			probe: &ateompb.Readyz{TimeoutSeconds: -1},
			want:  DefaultOverallTimeout,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := overallTimeout(tt.probe); got != tt.want {
				t.Errorf("overallTimeout = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWait_GivesUpAtProbeTimeout(t *testing.T) {
	// Nothing ever binds this port, so the poll loop runs until the
	// probe's own deadline rather than the package default.
	port := pickFreePort(t)
	probe := &ateompb.Readyz{
		HttpGet:        &ateompb.HTTPGetAction{Port: int32(port)},
		TimeoutSeconds: 1,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	err := Wait(ctx, "main", probe, "127.0.0.1")
	if err == nil {
		t.Fatalf("Wait returned nil, expected a timeout error")
	}
	if !errors.Is(err, ateerrors.ReasonWorkloadNotReady) {
		t.Errorf("Wait error = %v, want it to carry %v", err, ateerrors.ReasonWorkloadNotReady)
	}
	elapsed := time.Since(start)
	if elapsed < time.Second {
		t.Errorf("Wait gave up after %v, before the probe's 1s timeout", elapsed)
	}
	if elapsed > 5*time.Second {
		t.Errorf("Wait took %v; the probe timeout was ignored in favor of the %v default", elapsed, DefaultOverallTimeout)
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

// WaitAll is an ateom RPC boundary, so the reason has to reach atelet as an
// ErrorInfo detail. A %w-wrapped Reason does not: errors.As cannot cross a
// process, and the interceptor flattens a statusless error to a bare
// codes.Internal, which reads back as UNKNOWN.
func TestWaitAll_ReasonSurvivesTheRPCBoundary(t *testing.T) {
	port := pickFreePort(t)
	containers := []*ateompb.Container{{
		Name:   "main",
		Readyz: &ateompb.Readyz{HttpGet: &ateompb.HTTPGetAction{Port: int32(port)}, TimeoutSeconds: 1},
	}}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := WaitAll(ctx, containers, "127.0.0.1")
	if err == nil {
		t.Fatal("WaitAll returned nil, expected a timeout error")
	}

	// What the interceptor does to a handler error, then what atelet reads.
	overWire := fmt.Errorf("while calling ateom.RunWorkload: %w", asHandlerReturns(err))
	if got := ateattr.FailureReason(overWire); got != string(ateerrors.ReasonWorkloadNotReady) {
		t.Errorf("after the RPC hop FailureReason = %q, want %q", got, ateerrors.ReasonWorkloadNotReady)
	}
}

// asHandlerReturns mimics ateinterceptors: a status error in the chain is
// forwarded whole, anything else collapses to codes.Internal with only a message.
func asHandlerReturns(err error) error {
	var statusErr interface{ GRPCStatus() *status.Status }
	if errors.As(err, &statusErr) {
		return statusErr.GRPCStatus().Err()
	}
	return status.Error(codes.Internal, err.Error())
}

func TestWait_TCPConnectsAndCloses(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	probe := &ateompb.Readyz{TcpSocket: &ateompb.TCPSocketAction{Port: int32(listener.Addr().(*net.TCPAddr).Port)}}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := Wait(ctx, "tcp", probe, "127.0.0.1"); err != nil {
		t.Fatal(err)
	}
	if err := listener.(*net.TCPListener).SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	conn, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(conn)
	if err != nil || len(data) != 0 {
		t.Fatalf("probe must close without sending data: data=%q, err=%v", data, err)
	}
}

func TestWaitAll_MixedProbes(t *testing.T) {
	for _, first := range []string{"http", "tcp"} {
		t.Run(first+" ready first", func(t *testing.T) {
			var httpReady atomic.Bool
			httpReady.Store(first == "http")
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if !httpReady.Load() {
					w.WriteHeader(http.StatusServiceUnavailable)
				}
			}))
			defer srv.Close()
			ip, httpPort := splitHostPort(t, srv.URL)
			tcpPort := pickFreePort(t)
			startTCP := func() {
				listener, err := net.Listen("tcp", net.JoinHostPort(ip, strconv.Itoa(tcpPort)))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { listener.Close() })
			}
			if first == "tcp" {
				startTCP()
			}
			containers := []*ateompb.Container{
				{Name: "http", Readyz: &ateompb.Readyz{HttpGet: &ateompb.HTTPGetAction{Port: int32(httpPort)}}},
				{Name: "tcp", Readyz: &ateompb.Readyz{TcpSocket: &ateompb.TCPSocketAction{Port: int32(tcpPort)}}},
				{Name: "no-probe"},
			}
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- WaitAll(ctx, containers, ip) }()
			select {
			case err := <-done:
				t.Fatalf("returned before both listeners were ready: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			if first == "http" {
				startTCP()
			} else {
				httpReady.Store(true)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWait_TCPRefused(t *testing.T) {
	for _, tc := range []struct {
		name   string
		budget time.Duration
		cancel bool
		want   error
	}{
		{name: "probe timeout", budget: 5 * time.Second, want: ateerrors.ReasonWorkloadNotReady},
		{name: "parent deadline", budget: 50 * time.Millisecond, want: context.DeadlineExceeded},
		{name: "parent cancellation", budget: time.Second, cancel: true, want: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := &ateompb.Readyz{TcpSocket: &ateompb.TCPSocketAction{Port: int32(pickFreePort(t))}, TimeoutSeconds: 1}
			ctx, cancel := context.WithTimeout(t.Context(), tc.budget)
			defer cancel()
			if tc.cancel {
				timer := time.AfterFunc(30*time.Millisecond, cancel)
				defer timer.Stop()
			}
			start := time.Now()
			err := Wait(ctx, "tcp", probe, "127.0.0.1")
			if !errors.Is(err, tc.want) {
				t.Fatalf("Wait = %v, want %v", err, tc.want)
			}
			if tc.want != ateerrors.ReasonWorkloadNotReady && errors.Is(err, ateerrors.ReasonWorkloadNotReady) {
				t.Fatalf("parent cancellation tagged as workload failure: %v", err)
			}
			if tc.want == ateerrors.ReasonWorkloadNotReady && time.Since(start) < time.Second {
				t.Fatal("probe failed before configured timeout")
			}
			if time.Since(start) > 2*time.Second {
				t.Fatal("Wait ignored its deadline/cancellation")
			}
		})
	}
}

func TestWait_InvalidProbe(t *testing.T) {
	for _, tc := range []struct {
		name  string
		probe *ateompb.Readyz
	}{
		{"nil", nil},
		{"empty", &ateompb.Readyz{}},
		{"both", &ateompb.Readyz{HttpGet: &ateompb.HTTPGetAction{Port: 80}, TcpSocket: &ateompb.TCPSocketAction{Port: 80}}},
		{"tcp zero port", &ateompb.Readyz{TcpSocket: &ateompb.TCPSocketAction{}}},
		{"tcp negative port", &ateompb.Readyz{TcpSocket: &ateompb.TCPSocketAction{Port: -1}}},
		{"tcp port too large", &ateompb.Readyz{TcpSocket: &ateompb.TCPSocketAction{Port: 65536}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := Wait(t.Context(), "invalid", tc.probe, "127.0.0.1"); err == nil {
				t.Fatal("invalid probe succeeded")
			}
		})
	}
}
