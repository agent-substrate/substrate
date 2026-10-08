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

package objectstoreplugin

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	objectstoresnapshotv1 "github.com/agent-substrate/substrate/pkg/proto/objectstoresnapshotpb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// slowControl is a ControlProvider whose CleanupSnapshot takes delay, and
// closes started when a call arrives.
type slowControl struct {
	objectstoresnapshotv1.UnimplementedControlProviderServer

	delay   time.Duration
	once    sync.Once
	started chan struct{}
}

func newSlowControl(delay time.Duration) *slowControl {
	return &slowControl{delay: delay, started: make(chan struct{})}
}

func (c *slowControl) CleanupSnapshot(ctx context.Context, _ *objectstoresnapshotv1.CleanupSnapshotRequest) (*objectstoresnapshotv1.CleanupSnapshotResponse, error) {
	c.once.Do(func() { close(c.started) })
	select {
	case <-time.After(c.delay):
		return &objectstoresnapshotv1.CleanupSnapshotResponse{}, nil
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

// sidecar serves a ControlProvider on a Unix socket and can be stopped and
// started again on the same path, as kubelet restarts a sidecar container.
type sidecar struct {
	t       *testing.T
	path    string
	control objectstoresnapshotv1.ControlProviderServer

	mu  sync.Mutex
	srv *grpc.Server
}

func newSidecar(t *testing.T, control objectstoresnapshotv1.ControlProviderServer) *sidecar {
	t.Helper()
	// Unix socket paths are length-limited; t.TempDir can exceed that on macOS.
	dir, err := os.MkdirTemp("", "snapplug")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	s := &sidecar{t: t, path: filepath.Join(dir, "plugin.sock"), control: control}
	t.Cleanup(s.stop)
	return s
}

// start may run on any goroutine, so it reports failures with Error.
func (s *sidecar) start() {
	lis, err := Listen(s.path)
	if err != nil {
		s.t.Errorf("Listen: %v", err)
		return
	}
	srv := grpc.NewServer()
	objectstoresnapshotv1.RegisterControlProviderServer(srv, s.control)
	healthpb.RegisterHealthServer(srv, health.NewServer())
	go srv.Serve(lis)
	s.mu.Lock()
	s.srv = srv
	s.mu.Unlock()
}

// stop ends the server and every connection to it at once, as a crashed
// sidecar does.
func (s *sidecar) stop() {
	s.mu.Lock()
	srv := s.srv
	s.srv = nil
	s.mu.Unlock()
	if srv != nil {
		srv.Stop()
	}
}

// dialReady connects to s with the given wait and passes the startup
// readiness check.
func (s *sidecar) dialReady(wait time.Duration) (*grpc.ClientConn, objectstoresnapshotv1.ControlProviderClient) {
	s.t.Helper()
	conn, err := dial(s.path, wait)
	if err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(func() { conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := WaitReady(ctx, conn); err != nil {
		s.t.Fatalf("WaitReady = %v", err)
	}
	return conn, objectstoresnapshotv1.NewControlProviderClient(conn)
}

// lose stops s and waits until conn has seen the plugin go, so the next call
// starts on a lost connection rather than racing the loss.
func (s *sidecar) lose(conn *grpc.ClientConn) {
	s.t.Helper()
	s.stop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for conn.GetState() == connectivity.Ready {
		if !conn.WaitForStateChange(ctx, connectivity.Ready) {
			s.t.Fatal("connection still ready 10s after the plugin stopped")
		}
	}
}

var cleanupReq = &objectstoresnapshotv1.CleanupSnapshotRequest{SnapshotUri: testURI}

// A plugin lost after startup fails a call with Unavailable once the ready
// wait runs out, not when the caller's deadline does.
func TestCallFailsPromptlyAfterPluginLoss(t *testing.T) {
	const wait = 300 * time.Millisecond
	s := newSidecar(t, newSlowControl(0))
	s.start()
	conn, client := s.dialReady(wait)
	s.lose(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	_, err := client.CleanupSnapshot(ctx, cleanupReq)
	elapsed := time.Since(start)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("CleanupSnapshot after plugin loss = %v, want %s", err, codes.Unavailable)
	}
	if elapsed > 5*time.Second {
		t.Errorf("CleanupSnapshot failed after %s, want about %s, well before the caller's 10s deadline", elapsed, wait)
	}
}

// A call that ends with the caller's context while waiting reports the
// context's error, not Unavailable.
func TestCallWaitEndsWithCallerContext(t *testing.T) {
	s := newSidecar(t, newSlowControl(0))
	s.start()
	conn, client := s.dialReady(time.Minute)
	s.lose(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := client.CleanupSnapshot(ctx, cleanupReq)
	if status.Code(err) != codes.DeadlineExceeded {
		t.Errorf("CleanupSnapshot = %v, want %s", err, codes.DeadlineExceeded)
	}
}

// A call that starts on a ready connection runs as long as the plugin takes,
// however short the ready wait.
func TestSlowCallOutlastsReadyWait(t *testing.T) {
	const wait = 100 * time.Millisecond
	s := newSidecar(t, newSlowControl(10*wait))
	s.start()
	_, client := s.dialReady(wait)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := client.CleanupSnapshot(ctx, cleanupReq); err != nil {
		t.Errorf("CleanupSnapshot on a slow, healthy plugin = %v, want success", err)
	}
}

// A plugin that restarts within the ready wait serves the call.
func TestCallRidesThroughPluginRestart(t *testing.T) {
	s := newSidecar(t, newSlowControl(0))
	s.start()
	conn, client := s.dialReady(10 * time.Second)
	s.lose(conn)

	restarted := make(chan struct{})
	go func() {
		defer close(restarted)
		time.Sleep(time.Second)
		s.start()
	}()
	defer func() { <-restarted }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := client.CleanupSnapshot(ctx, cleanupReq); err != nil {
		t.Errorf("CleanupSnapshot across a plugin restart = %v, want success", err)
	}
}

// A call in flight when the plugin dies fails with Unavailable right away.
func TestInFlightCallFailsWhenPluginDies(t *testing.T) {
	control := newSlowControl(time.Minute)
	s := newSidecar(t, control)
	s.start()
	_, client := s.dialReady(time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	errc := make(chan error, 1)
	go func() {
		_, err := client.CleanupSnapshot(ctx, cleanupReq)
		errc <- err
	}()
	<-control.started
	start := time.Now()
	s.stop()
	err := <-errc
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("in-flight CleanupSnapshot = %v, want %s", err, codes.Unavailable)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("in-flight CleanupSnapshot failed %s after the plugin died, want promptly", elapsed)
	}
}

// WaitReady at startup waits for the plugin as long as its context allows,
// not only for the per-call ready wait.
func TestWaitReadyOutlastsReadyWait(t *testing.T) {
	const wait = 100 * time.Millisecond
	s := newSidecar(t, newSlowControl(0))
	conn, err := dial(s.path, wait)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	started := make(chan struct{})
	go func() {
		defer close(started)
		time.Sleep(10 * wait)
		s.start()
	}()
	defer func() { <-started }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := WaitReady(ctx, conn); err != nil {
		t.Errorf("WaitReady for a plugin that starts after the ready wait = %v, want success", err)
	}
}
