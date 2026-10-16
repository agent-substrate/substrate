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

package register

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"

	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/sandboxruntimes"
	"go.ate.dev/substrate/internal/workercapacity"
)

// testActors stands in for the worker's own ceiling, which is a flag in
// production rather than a constant here.
const testActors = 7

func TestFromFiles(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cpu    string
		memory string
		want   *ateletpb.WorkerResources
	}{
		{name: "limits set", cpu: "2000", memory: "4294967296", want: &ateletpb.WorkerResources{Actors: testActors, Resources: &ateletpb.Resources{
			Limits: []*ateletpb.Limits{{Name: "cpu", Quantity: "2"}, {Name: "memory", Quantity: "4Gi"}},
		}}},
		{name: "unparseable is none", cpu: "2Gi", memory: "", want: &ateletpb.WorkerResources{Actors: testActors}},
		{name: "negative is none", cpu: "-1", memory: "-1", want: &ateletpb.WorkerResources{Actors: testActors}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, workercapacity.CPULimitFile), []byte(tc.cpu), 0o600); err != nil {
				t.Fatalf("writing CPU limit: %v", err)
			}
			if err := os.WriteFile(filepath.Join(dir, workercapacity.MemoryLimitFile), []byte(tc.memory), 0o600); err != nil {
				t.Fatalf("writing memory limit: %v", err)
			}

			got := fromDir(dir, testActors)
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("reported capacity mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestFromFilesMissing(t *testing.T) {
	got := fromDir(t.TempDir(), testActors)
	want := &ateletpb.WorkerResources{Actors: testActors}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("unset environment capacity mismatch (-want +got):\n%s", diff)
	}
}

func TestBuildRuntime(t *testing.T) {
	got := buildRuntime("gvisor", sandboxruntimes.Attributes{
		hardware.AttrSandboxRuntimeHash: "sha256:abc",
		hardware.AttrCPUFeatures:        "3f",
		hardware.AttrArchitecture:       runtime.GOARCH,
	})
	want := &ateletpb.SandboxRuntime{
		SandboxClass: "gvisor",
		Version: &ateletpb.VersionedSandboxCompat{
			SchemaVersion: hardware.SchemaVersionV1,
			Attributes: []*ateletpb.AttributeEntry{
				{Key: hardware.AttrArchitecture, Value: runtime.GOARCH},
				{Key: hardware.AttrCPUFeatures, Value: "3f"},
				{Key: hardware.AttrSandboxRuntimeHash, Value: "sha256:abc"},
			},
		},
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("buildRuntime() mismatch (-want +got):\n%s", diff)
	}
}

func TestReconcilerSync(t *testing.T) {
	capacity := &ateletpb.WorkerResources{Actors: testActors}
	v1 := &sandboxruntimes.File{
		DefaultRuntime: sandboxruntimes.Attributes{
			hardware.AttrArchitecture:       runtime.GOARCH,
			hardware.AttrCPUFeatures:        "3",
			hardware.AttrSandboxRuntimeHash: "sha256:v1",
		},
	}
	v2 := &sandboxruntimes.File{
		DefaultRuntime: sandboxruntimes.Attributes{
			hardware.AttrArchitecture:       runtime.GOARCH,
			hardware.AttrCPUFeatures:        "7",
			hardware.AttrSandboxRuntimeHash: "sha256:v2",
		},
		RestorableRuntimes: []sandboxruntimes.Attributes{v1.DefaultRuntime},
	}
	reqV1 := buildRequest(capacity, "gvisor", v1)
	reqV2 := buildRequest(capacity, "gvisor", v2)

	tests := []struct {
		name           string
		setupFile      func(t *testing.T, path string)
		registered     *ateletpb.RegisterWorkerRequest
		registerErr    error
		wantRetry      bool
		wantCalls      []*ateletpb.RegisterWorkerRequest
		wantRegistered *ateletpb.RegisterWorkerRequest
	}{
		{
			// File not written yet: waits for fsnotify rather than spinning in backoff.
			name:      "file missing",
			setupFile: func(*testing.T, string) {},
		},
		{
			name: "corrupt JSON",
			setupFile: func(t *testing.T, path string) {
				if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "empty default runtime",
			setupFile: func(t *testing.T, path string) {
				if _, err := sandboxruntimes.Write(path, &sandboxruntimes.File{}); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "register error asks for retry and keeps prior registered state",
			setupFile: func(t *testing.T, path string) {
				if _, err := sandboxruntimes.Write(path, v1); err != nil {
					t.Fatal(err)
				}
			},
			registerErr: errors.New("worker record does not exist yet"),
			wantRetry:   true,
		},
		{
			name: "InvalidArgument from register is not retried",
			setupFile: func(t *testing.T, path string) {
				if _, err := sandboxruntimes.Write(path, v1); err != nil {
					t.Fatal(err)
				}
			},
			registerErr: status.Error(codes.InvalidArgument, "invalid attribute key"),
			wantRetry:   false,
		},
		{
			name: "first success registers and records request",
			setupFile: func(t *testing.T, path string) {
				if _, err := sandboxruntimes.Write(path, v1); err != nil {
					t.Fatal(err)
				}
			},
			wantCalls:      []*ateletpb.RegisterWorkerRequest{reqV1},
			wantRegistered: reqV1,
		},
		{
			name: "unchanged file is a no-op",
			setupFile: func(t *testing.T, path string) {
				if _, err := sandboxruntimes.Write(path, v1); err != nil {
					t.Fatal(err)
				}
			},
			registered:     reqV1,
			wantRegistered: reqV1,
		},
		{
			name: "updated file registers new default and restorable runtimes",
			setupFile: func(t *testing.T, path string) {
				if _, err := sandboxruntimes.Write(path, v2); err != nil {
					t.Fatal(err)
				}
			},
			registered:     reqV1,
			wantCalls:      []*ateletpb.RegisterWorkerRequest{reqV2},
			wantRegistered: reqV2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), nodepath.SandboxRuntimesFileName)
			tc.setupFile(t, path)

			var calls []*ateletpb.RegisterWorkerRequest
			r := newReconciler(
				ReportConfig{SandboxClass: "gvisor", RuntimesFile: path},
				capacity,
				func() (*sandboxruntimes.File, error) { return sandboxruntimes.Read(path) },
				func(_ context.Context, req *ateletpb.RegisterWorkerRequest) error {
					if tc.registerErr != nil {
						return tc.registerErr
					}
					calls = append(calls, req)
					return nil
				},
			)
			r.registered = tc.registered

			gotRetry := r.sync(context.Background())
			if gotRetry != tc.wantRetry {
				t.Errorf("sync() retry = %v, want %v", gotRetry, tc.wantRetry)
			}
			if diff := cmp.Diff(tc.wantCalls, calls, protocmp.Transform()); diff != "" {
				t.Errorf("register calls mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantRegistered, r.registered, protocmp.Transform()); diff != "" {
				t.Errorf("registered state mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestReconcilerRunBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	file := &sandboxruntimes.File{
		DefaultRuntime: sandboxruntimes.Attributes{hardware.AttrSandboxRuntimeHash: "sha256:v1"},
	}
	changes := make(chan struct{}, 1)

	const (
		initial = 10 * time.Millisecond
		maxB    = 35 * time.Millisecond
		resync  = 5 * time.Minute
	)

	var (
		waits    []time.Duration
		attempts int
	)
	r := newReconciler(
		ReportConfig{SandboxClass: "gvisor", RuntimesFile: "sandbox-runtimes.json"},
		&ateletpb.WorkerResources{Actors: testActors},
		func() (*sandboxruntimes.File, error) { return file, nil },
		func(_ context.Context, _ *ateletpb.RegisterWorkerRequest) error {
			attempts++
			switch attempts {
			case 1, 2, 3:
				// Fail three times: waits should be 10ms, 20ms, 35ms (capped at maxBackoff).
				return errors.New("transient failure")
			case 4:
				// Succeed on attempt 4, then trigger a file update on changes.
				file = &sandboxruntimes.File{
					DefaultRuntime: sandboxruntimes.Attributes{hardware.AttrSandboxRuntimeHash: "sha256:v2"},
				}
				changes <- struct{}{}
				return nil
			case 5:
				// Fail once on the updated file: backoff must have reset to initial (10ms).
				return errors.New("transient failure after reset")
			default:
				cancel()
				return nil
			}
		},
	)
	r.initialBackoff = initial
	r.maxBackoff = maxB
	r.resync = resync
	r.after = func(d time.Duration) <-chan time.Time {
		waits = append(waits, d)
		if d == resync {
			// Resync timer is armed on success; let changes wake the select instead.
			return nil
		}
		ch := make(chan time.Time, 1)
		ch <- time.Now()
		return ch
	}

	if err := r.run(ctx, changes); !errors.Is(err, context.Canceled) {
		t.Fatalf("run() = %v, want context.Canceled", err)
	}
	wantWaits := []time.Duration{
		initial,     // after attempt 1
		2 * initial, // after attempt 2 (doubled)
		maxB,        // after attempt 3 (capped at maxBackoff)
		resync,      // after attempt 4 (succeeded; armed resync timer while changes fired)
		initial,     // after attempt 5 (reset following attempt 4's success)
		resync,      // after attempt 6 (succeeded before cancel)
	}
	if diff := cmp.Diff(wantWaits, waits); diff != "" {
		t.Errorf("timer durations mismatch (-want +got):\n%s", diff)
	}
}

// A change signal that lands while run is backing off after a failed
// registration must not cut the backoff short: with ateapi down, every
// worker on the node would otherwise retry on every signal in lockstep. The
// signal is still acted on once the backoff has elapsed.
func TestReconcilerIgnoresChangesDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	file := &sandboxruntimes.File{
		DefaultRuntime: sandboxruntimes.Attributes{hardware.AttrSandboxRuntimeHash: "sha256:v1"},
	}
	changes := make(chan struct{}, 1)
	// started reports each registration attempt as it begins; release lets
	// the second one return.
	started := make(chan int, 4)
	release := make(chan struct{})
	attempts := 0
	r := newReconciler(
		ReportConfig{SandboxClass: "gvisor", RuntimesFile: "sandbox-runtimes.json"},
		&ateletpb.WorkerResources{Actors: testActors},
		func() (*sandboxruntimes.File, error) { return file, nil },
		func(context.Context, *ateletpb.RegisterWorkerRequest) error {
			attempts++
			started <- attempts
			if attempts == 1 {
				// The change is pending before run reaches its select.
				changes <- struct{}{}
				return errors.New("transient failure")
			}
			<-release
			return nil
		},
	)
	// Unbuffered, so a send succeeds only while run is waiting on the timer.
	backoffTimer := make(chan time.Time)
	armed := make(chan time.Duration, 4)
	r.after = func(d time.Duration) <-chan time.Time {
		armed <- d
		if d == r.initialBackoff {
			return backoffTimer
		}
		return make(chan time.Time) // the resync timer never fires here
	}

	done := make(chan error, 1)
	go func() {
		done <- r.run(ctx, changes)
	}()

	if n := <-started; n != 1 {
		t.Fatalf("first attempt = %d, want 1", n)
	}
	if d := <-armed; d != r.initialBackoff {
		t.Fatalf("armed %v after the failed attempt, want the initial backoff %v", d, r.initialBackoff)
	}
	// Were run listening to changes during the backoff, it would be inside
	// the second attempt now rather than receiving the timer.
	select {
	case n := <-started:
		t.Fatalf("attempt %d started before the backoff elapsed: the change signal bypassed the backoff", n)
	case backoffTimer <- time.Now():
	}
	if n := <-started; n != 2 {
		t.Fatalf("attempt after the backoff = %d, want 2", n)
	}
	close(release)

	// The pending change is consumed once the retry succeeds; the file it
	// announces is what was just registered, so no third attempt follows.
	for _, want := range []time.Duration{r.resync, r.resync} {
		if d := <-armed; d != want {
			t.Fatalf("armed %v after a successful sync, want the resync interval %v", d, want)
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run() = %v, want context.Canceled", err)
	}
	if attempts != 2 {
		t.Errorf("attempts = %d, want 2", attempts)
	}
}

func TestReconcilerRunResync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var reads int
	r := newReconciler(
		ReportConfig{SandboxClass: "gvisor", RuntimesFile: "sandbox-runtimes.json"},
		&ateletpb.WorkerResources{Actors: testActors},
		func() (*sandboxruntimes.File, error) {
			reads++
			if reads >= 2 {
				cancel()
			}
			return &sandboxruntimes.File{
				DefaultRuntime: sandboxruntimes.Attributes{hardware.AttrSandboxRuntimeHash: "sha256:v1"},
			}, nil
		},
		func(context.Context, *ateletpb.RegisterWorkerRequest) error { return nil },
	)
	var armed []time.Duration
	r.after = func(d time.Duration) <-chan time.Time {
		armed = append(armed, d)
		if ctx.Err() != nil {
			return nil // once canceled, let ctx.Done win the select
		}
		ch := make(chan time.Time, 1)
		ch <- time.Now()
		return ch
	}

	// Pass a nil changes channel: only the resync timer can trigger the second read.
	if err := r.run(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("run() = %v, want context.Canceled", err)
	}
	if reads != 2 {
		t.Errorf("read calls = %d, want 2 (initial sync + resync tick)", reads)
	}
	if len(armed) == 0 || armed[0] != resyncInterval {
		t.Errorf("armed timers = %v, want first timer to be resyncInterval (%v)", armed, resyncInterval)
	}
}

func TestWatchFileFallbackToPolling(t *testing.T) {
	origInterval := watchPollInterval
	watchPollInterval = 5 * time.Millisecond
	t.Cleanup(func() { watchPollInterval = origInterval })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Point WatchFilePath at a path whose parent directory does not exist so w.Add
	// fails naturally and triggers the polling fallback.
	missingPath := filepath.Join(t.TempDir(), "missing-parent", nodepath.SandboxRuntimesFileName)
	changes := WatchFilePath(ctx, missingPath)

	select {
	case _, ok := <-changes:
		if !ok {
			t.Fatal("changes channel closed before emitting a polling signal")
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for polling fallback signal")
	}

	cancel()
	for range changes {
	}
}

func TestWatchFileFilterAndErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir := nodepath.WorkerPath("pod-uid")
	target := nodepath.WorkerSandboxRuntimesPath("pod-uid")
	events := make(chan fsnotify.Event)
	errs := make(chan error)
	changes := make(chan struct{}, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		watchFileLoop(ctx, target, events, errs, changes)
	}()

	assertNoSignal := func(t *testing.T, desc string) {
		t.Helper()
		select {
		case <-changes:
			t.Fatalf("unexpected signal after %s", desc)
		case <-time.After(10 * time.Millisecond):
		}
	}
	assertSignal := func(t *testing.T, desc string) {
		t.Helper()
		select {
		case <-changes:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for signal after %s", desc)
		}
	}

	// Unrelated files in the same worker directory must be ignored.
	events <- fsnotify.Event{Name: nodepath.WorkerSocketPath("pod-uid"), Op: fsnotify.Create}
	events <- fsnotify.Event{Name: filepath.Join(dir, ".sandbox-runtimes-123456"), Op: fsnotify.Create | fsnotify.Write}
	assertNoSignal(t, "events on worker.sock and temp file")

	// Non-Create/Write operations on the target file must be ignored.
	events <- fsnotify.Event{Name: target, Op: fsnotify.Chmod}
	events <- fsnotify.Event{Name: target, Op: fsnotify.Remove}
	events <- fsnotify.Event{Name: target, Op: fsnotify.Rename}
	assertNoSignal(t, "Chmod/Remove/Rename on target file")

	// Create and Write on the target file must signal.
	events <- fsnotify.Event{Name: target, Op: fsnotify.Create}
	assertSignal(t, "Create on target file")
	events <- fsnotify.Event{Name: target, Op: fsnotify.Write}
	assertSignal(t, "Write on target file")

	// An error on the watcher's Errors channel must also signal so the caller re-reads.
	errs <- errors.New("fsnotify queue overflow")
	assertSignal(t, "watcher error")

	cancel()
	<-done
}

func TestReconcilerRunWatchesFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	path := filepath.Join(t.TempDir(), nodepath.SandboxRuntimesFileName)
	got := make(chan *ateletpb.RegisterWorkerRequest, 4)
	r := newReconciler(
		ReportConfig{SandboxClass: "gvisor", RuntimesFile: path},
		&ateletpb.WorkerResources{Actors: testActors},
		func() (*sandboxruntimes.File, error) { return sandboxruntimes.Read(path) },
		func(_ context.Context, req *ateletpb.RegisterWorkerRequest) error {
			got <- req
			return nil
		},
	)

	changes := WatchFilePath(ctx, path)
	done := make(chan error, 1)
	go func() {
		done <- r.run(ctx, changes)
	}()

	// Write the file after WatchFilePath has started, as happens when a worker comes up
	// before atelet has reconciled the pod.
	v1 := &sandboxruntimes.File{
		DefaultRuntime: sandboxruntimes.Attributes{
			hardware.AttrArchitecture:       runtime.GOARCH,
			hardware.AttrCPUFeatures:        "3",
			hardware.AttrSandboxRuntimeHash: "sha256:v1",
		},
	}
	if _, err := sandboxruntimes.Write(path, v1); err != nil {
		t.Fatalf("writing v1: %v", err)
	}
	select {
	case req := <-got:
		if diff := cmp.Diff(buildRuntime("gvisor", v1.DefaultRuntime), req.GetDefaultRuntime(), protocmp.Transform()); diff != "" {
			t.Errorf("initial registration default_runtime mismatch (-want +got):\n%s", diff)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for initial registration after file creation")
	}

	// Rewrite the file via atomic rename and verify the watcher triggers a
	// second registration.
	v2 := &sandboxruntimes.File{
		DefaultRuntime: sandboxruntimes.Attributes{
			hardware.AttrArchitecture:       runtime.GOARCH,
			hardware.AttrCPUFeatures:        "7",
			hardware.AttrSandboxRuntimeHash: "sha256:v2",
		},
		RestorableRuntimes: []sandboxruntimes.Attributes{v1.DefaultRuntime},
	}
	if _, err := sandboxruntimes.Write(path, v2); err != nil {
		t.Fatalf("writing v2: %v", err)
	}
	select {
	case req := <-got:
		if diff := cmp.Diff(buildRuntime("gvisor", v2.DefaultRuntime), req.GetDefaultRuntime(), protocmp.Transform()); diff != "" {
			t.Errorf("updated registration default_runtime mismatch (-want +got):\n%s", diff)
		}
		if len(req.GetRestorableRuntimes()) != 1 {
			t.Fatalf("got %d restorable runtimes, want 1", len(req.GetRestorableRuntimes()))
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for registration after file update")
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("run() = %v, want context.Canceled", err)
	}
}

// A misconfiguration must surface rather than spin in the retry loop: the
// caller exits on it, and retrying forever would leave the worker idle and
// silent instead.
func TestReportFailsFastOnBadCredentials(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := Report(ctx, ReportConfig{
		SocketPath:           filepath.Join(t.TempDir(), "atelet.sock"),
		CredentialBundlePath: filepath.Join(t.TempDir(), "does-not-exist.pem"),
		TrustBundlePath:      filepath.Join(t.TempDir(), "also-missing.pem"),
		SandboxClass:         "gvisor",
		RuntimesFile:         filepath.Join(t.TempDir(), nodepath.SandboxRuntimesFileName),
	})
	if err == nil {
		t.Fatal("Report() with unreadable credentials succeeded, want an error the caller can exit on")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Report() retried a permanent failure until the deadline: %v", err)
	}
}

// A runtime without a class or runtimes file path is a configuration bug, so
// Report must fail immediately rather than spin.
func TestReportFailsFastOnMissingConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, tc := range []struct {
		name string
		cfg  ReportConfig
	}{
		{
			name: "missing SandboxClass",
			cfg: ReportConfig{
				SocketPath:   filepath.Join(t.TempDir(), "atelet.sock"),
				RuntimesFile: filepath.Join(t.TempDir(), nodepath.SandboxRuntimesFileName),
			},
		},
		{
			name: "missing RuntimesFile",
			cfg: ReportConfig{
				SocketPath:   filepath.Join(t.TempDir(), "atelet.sock"),
				SandboxClass: "gvisor",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := Report(ctx, tc.cfg)
			if err == nil {
				t.Fatal("Report() succeeded, want an error the caller can exit on")
			}
			if errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("Report() retried a permanent failure until the deadline: %v", err)
			}
		})
	}
}
