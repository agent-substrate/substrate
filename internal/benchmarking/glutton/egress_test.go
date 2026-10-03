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
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
)

// countingTarget is an upstream that answers every GET with code and counts
// the requests it received.
func countingTarget(t *testing.T, code int) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(code)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(ts.Close)
	return ts, &hits
}

func newEgressService(t *testing.T) *Service {
	t.Helper()
	svc, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(svc.Close)
	return svc
}

// waitForHits polls until the target has received at least n requests.
func waitForHits(t *testing.T, hits *atomic.Int64, n int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for hits.Load() < n {
		if time.Now().After(deadline) {
			t.Fatalf("target got %d requests, want at least %d", hits.Load(), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func useEgress(t *testing.T, svc *Service, req *gluttonpb.UseEgressRequest) {
	t.Helper()
	if _, err := svc.UseEgress(context.Background(), req); err != nil {
		t.Fatalf("UseEgress(%v): %v", req, err)
	}
}

func drainEgress(t *testing.T, svc *Service, stop bool) *gluttonpb.DrainEgressResponse {
	t.Helper()
	resp, err := svc.DrainEgress(context.Background(), &gluttonpb.DrainEgressRequest{Stop: stop})
	if err != nil {
		t.Fatalf("DrainEgress: %v", err)
	}
	return resp
}

// TestUseEgressCallsUntilStopped checks the loop calls the target repeatedly,
// marks only its first call, and makes no call after a stopping drain.
func TestUseEgressCallsUntilStopped(t *testing.T) {
	svc := newEgressService(t)
	target, hits := countingTarget(t, http.StatusOK)

	useEgress(t, svc, &gluttonpb.UseEgressRequest{Url: target.URL, IntervalMs: 10})
	waitForHits(t, hits, 3)
	resp := drainEgress(t, svc, true)

	samples := resp.GetSamples()
	if len(samples) < 3 {
		t.Fatalf("drained %d samples, want at least 3", len(samples))
	}
	if int64(len(samples)) != hits.Load() {
		t.Errorf("drained %d samples, target got %d requests", len(samples), hits.Load())
	}
	for i, s := range samples {
		if s.GetFirst() != (i == 0) {
			t.Errorf("sample %d first = %v, want %v", i, s.GetFirst(), i == 0)
		}
		if s.GetStatusCode() != http.StatusOK || s.GetError() != "" {
			t.Errorf("sample %d = status %d error %q, want 200 and no error", i, s.GetStatusCode(), s.GetError())
		}
		if s.GetLatencyUs() <= 0 {
			t.Errorf("sample %d latency_us = %d, want > 0", i, s.GetLatencyUs())
		}
	}
	if resp.GetDropped() != 0 {
		t.Errorf("dropped = %d, want 0", resp.GetDropped())
	}

	stopped := hits.Load()
	time.Sleep(50 * time.Millisecond)
	if got := hits.Load(); got != stopped {
		t.Errorf("target got %d requests after stop, want 0", got-stopped)
	}
	if got := drainEgress(t, svc, false).GetSamples(); len(got) != 0 {
		t.Errorf("second drain returned %d samples, want 0", len(got))
	}
}

// TestUseEgressConnectionReuse checks reused_conn: with keep-alive every call
// after the first reuses the connection, without it none does.
func TestUseEgressConnectionReuse(t *testing.T) {
	for _, tc := range []struct {
		name              string
		disableKeepAlives bool
	}{
		{name: "keep-alive"},
		{name: "new connection per call", disableKeepAlives: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := newEgressService(t)
			target, hits := countingTarget(t, http.StatusOK)

			useEgress(t, svc, &gluttonpb.UseEgressRequest{
				Url:               target.URL,
				IntervalMs:        10,
				DisableKeepAlives: tc.disableKeepAlives,
			})
			waitForHits(t, hits, 3)
			for i, s := range drainEgress(t, svc, true).GetSamples() {
				want := i > 0 && !tc.disableKeepAlives
				if s.GetReusedConn() != want {
					t.Errorf("sample %d reused_conn = %v, want %v", i, s.GetReusedConn(), want)
				}
			}
		})
	}
}

// TestUseEgressReplaceClearsResults checks that a second UseEgress replaces
// the loop and forgets the old loop's results.
func TestUseEgressReplaceClearsResults(t *testing.T) {
	svc := newEgressService(t)
	oldTarget, oldHits := countingTarget(t, http.StatusOK)
	newTarget, newHits := countingTarget(t, http.StatusNotFound)

	useEgress(t, svc, &gluttonpb.UseEgressRequest{Url: oldTarget.URL, IntervalMs: 10})
	waitForHits(t, oldHits, 2)
	useEgress(t, svc, &gluttonpb.UseEgressRequest{Url: newTarget.URL, IntervalMs: 10})
	oldStopped := oldHits.Load()
	waitForHits(t, newHits, 2)

	samples := drainEgress(t, svc, true).GetSamples()
	if len(samples) == 0 {
		t.Fatal("drained no samples")
	}
	for i, s := range samples {
		if s.GetStatusCode() != http.StatusNotFound {
			t.Errorf("sample %d status = %d, want 404 from the new target", i, s.GetStatusCode())
		}
	}
	if !samples[0].GetFirst() {
		t.Error("first sample of the new loop is not marked first")
	}
	if got := oldHits.Load(); got != oldStopped {
		t.Errorf("old target got %d requests after the replace, want 0", got-oldStopped)
	}
}

// TestUseEgressZeroIntervalStopsAndKeepsResults checks interval_ms=0 stops
// the loop but leaves its results for the next drain.
func TestUseEgressZeroIntervalStopsAndKeepsResults(t *testing.T) {
	svc := newEgressService(t)
	target, hits := countingTarget(t, http.StatusOK)

	useEgress(t, svc, &gluttonpb.UseEgressRequest{Url: target.URL, IntervalMs: 10})
	waitForHits(t, hits, 2)
	useEgress(t, svc, &gluttonpb.UseEgressRequest{})
	stopped := hits.Load()
	time.Sleep(50 * time.Millisecond)

	if got := hits.Load(); got != stopped {
		t.Errorf("target got %d requests after stop, want 0", got-stopped)
	}
	if got := int64(len(drainEgress(t, svc, false).GetSamples())); got != stopped {
		t.Errorf("drained %d samples, want %d", got, stopped)
	}
}

// TestDrainEgressStopWaitsForCallInFlight checks a stopping drain lets the
// call in flight finish and returns its result instead of canceling it.
func TestDrainEgressStopWaitsForCallInFlight(t *testing.T) {
	svc := newEgressService(t)
	entered := make(chan struct{}, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case entered <- struct{}{}:
		default:
		}
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(target.Close)

	useEgress(t, svc, &gluttonpb.UseEgressRequest{Url: target.URL, IntervalMs: 2000})
	<-entered
	samples := drainEgress(t, svc, true).GetSamples()

	if len(samples) != 1 {
		t.Fatalf("drained %d samples, want 1", len(samples))
	}
	if s := samples[0]; s.GetStatusCode() != http.StatusOK || s.GetError() != "" {
		t.Errorf("sample = status %d error %q, want the call to complete with 200", s.GetStatusCode(), s.GetError())
	}
}

// TestEgressCallSlowerThanIntervalTimesOut checks the interval is each call's
// timeout: a slow upstream yields an error and no status.
func TestEgressCallSlowerThanIntervalTimesOut(t *testing.T) {
	svc := newEgressService(t)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	t.Cleanup(target.Close)

	useEgress(t, svc, &gluttonpb.UseEgressRequest{Url: target.URL, IntervalMs: 50})
	samples := drainEgress(t, svc, true).GetSamples()

	if len(samples) == 0 {
		t.Fatal("drained no samples")
	}
	if s := samples[0]; s.GetError() == "" || s.GetStatusCode() != 0 {
		t.Errorf("sample = status %d error %q, want a timeout error and status 0", s.GetStatusCode(), s.GetError())
	}
}

// TestEgressCallUnreachable checks a refused connection yields an error and no
// status.
func TestEgressCallUnreachable(t *testing.T) {
	svc := newEgressService(t)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()

	useEgress(t, svc, &gluttonpb.UseEgressRequest{Url: "http://" + addr + "/", IntervalMs: 1000})
	samples := drainEgress(t, svc, true).GetSamples()

	if len(samples) != 1 {
		t.Fatalf("drained %d samples, want 1", len(samples))
	}
	if s := samples[0]; s.GetError() == "" || s.GetStatusCode() != 0 {
		t.Errorf("sample = status %d error %q, want a connection error and status 0", s.GetStatusCode(), s.GetError())
	}
}

// TestEgressBufferCountsDropped checks results past the buffer cap are counted
// rather than kept.
func TestEgressBufferCountsDropped(t *testing.T) {
	prev := egressBufferCap
	egressBufferCap = 2
	t.Cleanup(func() { egressBufferCap = prev })

	svc := newEgressService(t)
	target, hits := countingTarget(t, http.StatusOK)

	useEgress(t, svc, &gluttonpb.UseEgressRequest{Url: target.URL, IntervalMs: 10})
	waitForHits(t, hits, 4)
	resp := drainEgress(t, svc, true)

	if got := len(resp.GetSamples()); got != 2 {
		t.Errorf("drained %d samples, want the cap of 2", got)
	}
	if want := hits.Load() - 2; resp.GetDropped() != want {
		t.Errorf("dropped = %d, want %d", resp.GetDropped(), want)
	}
}

func TestUseEgressInvalidArgument(t *testing.T) {
	svc := newEgressService(t)
	for _, tc := range []struct {
		name string
		req  *gluttonpb.UseEgressRequest
	}{
		{name: "negative interval", req: &gluttonpb.UseEgressRequest{Url: "http://example.com/", IntervalMs: -1}},
		{name: "empty url", req: &gluttonpb.UseEgressRequest{IntervalMs: 10}},
		{name: "no scheme", req: &gluttonpb.UseEgressRequest{Url: "example.com/", IntervalMs: 10}},
		{name: "unsupported scheme", req: &gluttonpb.UseEgressRequest{Url: "ftp://example.com/", IntervalMs: 10}},
		{name: "no host", req: &gluttonpb.UseEgressRequest{Url: "http:///path", IntervalMs: 10}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.UseEgress(context.Background(), tc.req)
			if status.Code(err) != codes.InvalidArgument {
				t.Errorf("UseEgress error = %v, want InvalidArgument", err)
			}
		})
	}
}
