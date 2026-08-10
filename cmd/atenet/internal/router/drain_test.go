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

package router

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/serverboot"
)

// drainFunc adapts a bare function to the dataplaneDrainer interface.
type drainFunc func(context.Context) error

func (f drainFunc) Drain(ctx context.Context) error { return f(ctx) }

// fakeStopper is a grpcStopper whose GracefulStop blocks until release is
// closed, simulating an in-flight (parked) ext_proc stream.
type fakeStopper struct {
	release        chan struct{}
	gracefulCalled atomic.Bool
	stopCalled     atomic.Bool
}

func newFakeStopper() *fakeStopper {
	return &fakeStopper{release: make(chan struct{})}
}

func (f *fakeStopper) GracefulStop() {
	f.gracefulCalled.Store(true)
	<-f.release
}

func (f *fakeStopper) Stop() { f.stopCalled.Store(true) }

// orderRecorder captures the sequence of drain steps so tests can assert the
// forced ordering: readiness → dataplane → ext_proc → stopRest.
type orderRecorder struct {
	mu    sync.Mutex
	steps []string
}

func (r *orderRecorder) add(step string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.steps = append(r.steps, step)
}

func (r *orderRecorder) get() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.steps...)
}

// TestDrainOnShutdownOrdering asserts the full sequence runs in the forced
// order and that an in-flight ext_proc stream releasing promptly lets the
// drain complete without a force stop.
func TestDrainOnShutdownOrdering(t *testing.T) {
	rec := &orderRecorder{}
	stopper := newFakeStopper()
	readiness := &serverboot.Readiness{}

	// Release the "parked request" shortly after GracefulStop begins.
	go func() {
		for !stopper.gracefulCalled.Load() {
			time.Sleep(5 * time.Millisecond)
		}
		rec.add("extproc-drain-started")
		time.Sleep(20 * time.Millisecond)
		close(stopper.release)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	done := drainOnShutdown(ctx, drainParams{
		readiness: readiness,
		delay:     0,
		dataplane: drainFunc(func(context.Context) error {
			if readiness.Ready() {
				t.Error("dataplane drain ran before the readiness flip")
			}
			rec.add("dataplane")
			return nil
		}),
		dataplaneWindow: time.Second,
		extproc:         stopper,
		timeout:         5 * time.Second,
		stopRest: func() {
			if !stopper.gracefulCalled.Load() {
				t.Error("stopRest ran before the ext_proc drain")
			}
			rec.add("stopRest")
		},
	})

	cancel() // simulate SIGTERM

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("drain did not complete")
	}

	want := []string{"dataplane", "extproc-drain-started", "stopRest"}
	got := rec.get()
	if len(got) != len(want) {
		t.Fatalf("drain steps = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("drain steps = %v, want %v", got, want)
		}
	}
	if stopper.stopCalled.Load() {
		t.Error("force Stop fired although the graceful drain completed in time")
	}
	if readiness.Ready() {
		t.Error("readiness should be not-ready after drain")
	}
}

// TestDrainOnShutdownForceStopsAfterTimeout asserts a stream that never
// finishes is force-stopped at the drain timeout and the sequence still
// completes.
func TestDrainOnShutdownForceStopsAfterTimeout(t *testing.T) {
	stopper := newFakeStopper() // release never closed → GracefulStop blocks forever
	var stopRest atomic.Bool

	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	done := drainOnShutdown(ctx, drainParams{
		readiness:       &serverboot.Readiness{},
		delay:           0,
		dataplane:       nil, // dataplane offers no drain hook (agentgateway shape)
		dataplaneWindow: time.Second,
		extproc:         stopper,
		timeout:         100 * time.Millisecond,
		stopRest:        func() { stopRest.Store(true) },
	})
	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("drain did not force-stop within deadline")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("force stop took too long (%v); expected ~drain timeout", elapsed)
	}
	if !stopper.stopCalled.Load() {
		t.Error("force Stop was not called for the wedged stream")
	}
	if !stopRest.Load() {
		t.Error("stopRest did not run after the force stop")
	}
}

// TestDrainOnShutdownDataplaneFailureContinues asserts an incomplete
// dataplane drain (connections still active at its deadline) does not wedge
// the sequence.
func TestDrainOnShutdownDataplaneFailureContinues(t *testing.T) {
	stopper := newFakeStopper()
	close(stopper.release) // ext_proc idle

	ctx, cancel := context.WithCancel(context.Background())
	done := drainOnShutdown(ctx, drainParams{
		readiness:       &serverboot.Readiness{},
		delay:           0,
		dataplane:       drainFunc(func(ctx context.Context) error { return context.DeadlineExceeded }),
		dataplaneWindow: 50 * time.Millisecond,
		extproc:         stopper,
		timeout:         time.Second,
		stopRest:        func() {},
	})
	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("drain wedged on a failed dataplane drain")
	}
	if !stopper.gracefulCalled.Load() {
		t.Error("ext_proc drain skipped after dataplane drain failure")
	}
}

// fakeEnvoyAdmin is an httptest stand-in for the Envoy admin interface.
type fakeEnvoyAdmin struct {
	mu           sync.Mutex
	posts        []string
	statsCalls   int
	activeSeries []int // successive downstream_cx_active values to report
}

func (f *fakeEnvoyAdmin) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch {
		case r.Method == http.MethodPost:
			f.posts = append(f.posts, r.URL.RequestURI())
		case strings.HasPrefix(r.URL.Path, "/stats"):
			// Report the series value for this poll; once exhausted, repeat the
			// last value so timing-dependent extra polls see a stable count.
			idx := f.statsCalls
			if idx >= len(f.activeSeries) {
				idx = len(f.activeSeries) - 1
			}
			active := f.activeSeries[idx]
			f.statsCalls++
			// Includes an admin line that must be excluded from the sum.
			w.Write([]byte(
				"listener.admin.downstream_cx_active: 1\n" +
					"listener.0.0.0.0_8080.downstream_cx_active: " + itoa(active) + "\n" +
					"http.ingress_http.downstream_cx_active_unrelated: 99\n"))
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// TestEnvoyDrainerDrainsToZero asserts the drainer POSTs the two admin calls
// and polls stats until active connections reach zero, excluding admin
// listeners from the count.
func TestEnvoyDrainerDrainsToZero(t *testing.T) {
	admin := &fakeEnvoyAdmin{activeSeries: []int{2, 1, 0}}
	srv := httptest.NewServer(admin.handler())
	defer srv.Close()

	d := newEnvoyDrainer(strings.TrimPrefix(srv.URL, "http://"))
	d.pollInterval = 5 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := d.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	admin.mu.Lock()
	defer admin.mu.Unlock()
	wantPosts := []string{"/healthcheck/fail", "/drain_listeners?graceful&skip_exit"}
	if len(admin.posts) != len(wantPosts) {
		t.Fatalf("admin POSTs = %v, want %v", admin.posts, wantPosts)
	}
	for i := range wantPosts {
		if admin.posts[i] != wantPosts[i] {
			t.Fatalf("admin POSTs = %v, want %v", admin.posts, wantPosts)
		}
	}
	if admin.statsCalls < 3 {
		t.Errorf("stats polled %d times, want >= 3 (series 2,1,0)", admin.statsCalls)
	}
}

// TestEnvoyDrainerAdminGone asserts an unreachable admin interface (Envoy
// already exited) is treated as a completed drain, quickly.
func TestEnvoyDrainerAdminGone(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	addr := strings.TrimPrefix(srv.URL, "http://")
	srv.Close() // nothing listens there anymore

	d := newEnvoyDrainer(addr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := time.Now()
	if err := d.Drain(ctx); err != nil {
		t.Fatalf("Drain against a gone admin: %v", err)
	}
	if time.Since(start) > time.Second {
		t.Error("Drain against a gone admin should return promptly")
	}
}

// TestEnvoyDrainerDeadlineWithActiveConnections asserts the drainer reports
// the deadline error when connections never reach zero, so the orchestrator
// can log it and continue.
func TestEnvoyDrainerDeadlineWithActiveConnections(t *testing.T) {
	admin := &fakeEnvoyAdmin{activeSeries: []int{5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5}}
	srv := httptest.NewServer(admin.handler())
	defer srv.Close()

	d := newEnvoyDrainer(strings.TrimPrefix(srv.URL, "http://"))
	d.pollInterval = 5 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := d.Drain(ctx); err == nil {
		t.Fatal("Drain with persistent connections should report the deadline")
	}
}

// TestDrainMarkerLifecycle pins the preStop handshake marker semantics: a
// stale marker is removed at startup (emptyDir survives container restarts,
// and a stale marker would release Envoy's preStop the moment a later drain
// begins), the marker is created on drain completion, and an empty path
// disables the handshake entirely.
func TestDrainMarkerLifecycle(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	marker := dir + "/nested/drain-complete"

	// Empty path: both operations are no-ops.
	removeStaleDrainMarker(ctx, "")
	writeDrainMarker(ctx, "")

	// Removing a marker that does not exist is quiet.
	removeStaleDrainMarker(ctx, marker)

	// Writing creates parent directories and the file.
	writeDrainMarker(ctx, marker)
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("marker not created: %v", err)
	}

	// A restart must defuse the stale marker.
	removeStaleDrainMarker(ctx, marker)
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("stale marker not removed: %v", err)
	}
}
