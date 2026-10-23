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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"go.ate.dev/substrate/internal/apierror"
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
		{name: "limits set", cpu: "2000", memory: "4294967296", want: &ateletpb.WorkerResources{Actors: testActors, CpuMillicores: 2000, MemoryBytes: 4294967296}},
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

func TestBuildCompat(t *testing.T) {
	got := buildCompat(sandboxruntimes.Attributes{
		hardware.AttrSandboxBundleHash: "sha256:abc",
		hardware.AttrCPUFeatures:       "3f",
		hardware.AttrArchitecture:      runtime.GOARCH,
	})
	want := &ateletpb.VersionedSandboxCompat{
		SchemaVersion: hardware.SchemaVersionV1,
		Attributes: []*ateletpb.AttributeEntry{
			{Key: hardware.AttrArchitecture, Value: runtime.GOARCH},
			{Key: hardware.AttrCPUFeatures, Value: "3f"},
			{Key: hardware.AttrSandboxBundleHash, Value: "sha256:abc"},
		},
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("buildCompat() mismatch (-want +got):\n%s", diff)
	}
}

// gvisorFile is a sandbox runtimes file naming one class, gvisor, with def
// as its default runtime and restorable as the others.
func gvisorFile(def sandboxruntimes.Attributes, restorable ...sandboxruntimes.Attributes) *sandboxruntimes.File {
	c := sandboxruntimes.Class{SandboxClass: "gvisor", DefaultRuntime: sandboxruntimes.Runtime{Version: def}}
	for _, r := range restorable {
		c.RestorableRuntimes = append(c.RestorableRuntimes, sandboxruntimes.Runtime{Version: r})
	}
	return &sandboxruntimes.File{SandboxClasses: []sandboxruntimes.Class{c}}
}

func TestParsePodIPs(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  []string
	}{
		{value: "", want: nil},
		{value: "10.0.0.1", want: []string{"10.0.0.1"}},
		{value: "10.0.0.1,fd00::1", want: []string{"10.0.0.1", "fd00::1"}},
		{value: " 10.0.0.1 , ,fd00::1,", want: []string{"10.0.0.1", "fd00::1"}},
	} {
		if diff := cmp.Diff(tc.want, ParsePodIPs(tc.value)); diff != "" {
			t.Errorf("ParsePodIPs(%q) mismatch (-want +got):\n%s", tc.value, diff)
		}
	}
}

func TestEpochRequire(t *testing.T) {
	var e Epoch
	if _, err := e.Require(); apierror.Code(err) != codes.Unavailable {
		t.Errorf("Require() before registration = %v, want Unavailable", err)
	}
	e.Store(3)
	if got, err := e.Require(); err != nil || got != 3 {
		t.Errorf("Require() after registration = (%d, %v), want (3, nil)", got, err)
	}
}

// The readiness check holds the pod not Ready until the worker process has
// registered, so a restarted one takes no placements before it can serve them.
func TestEpochRegistered(t *testing.T) {
	var e Epoch
	if err := e.Registered(context.Background()); err == nil {
		t.Error("Registered() before registration = nil, want an error")
	}
	e.Store(3)
	if err := e.Registered(context.Background()); err != nil {
		t.Errorf("Registered() after registration = %v, want nil", err)
	}
}

// fakeSupport is a supportClient that fails the first acquireFails acquires
// and fails registration with registerErrs in turn.
type fakeSupport struct {
	acquireFails int
	acquires     int
	registerErrs []error
	registered   []*ateletpb.RegisterWorkerRequest
}

func (f *fakeSupport) AcquireWorkerEpoch(context.Context) (int64, error) {
	f.acquires++
	if f.acquires <= f.acquireFails {
		return 0, status.Error(codes.NotFound, "worker record does not exist yet")
	}
	return int64(f.acquires), nil
}

func (f *fakeSupport) RegisterWorker(_ context.Context, req *ateletpb.RegisterWorkerRequest) error {
	f.registered = append(f.registered, proto.Clone(req).(*ateletpb.RegisterWorkerRequest))
	if len(f.registerErrs) > 0 {
		err := f.registerErrs[0]
		f.registerErrs = f.registerErrs[1:]
		return err
	}
	return nil
}

func (f *fakeSupport) SetWorkerSandboxRuntimes(context.Context, *ateletpb.SetWorkerSandboxRuntimesRequest) error {
	return nil
}

// immediately fires every timer, recording the durations armed.
func immediately(waits *[]time.Duration) func(time.Duration) <-chan time.Time {
	return func(d time.Duration) <-chan time.Time {
		*waits = append(*waits, d)
		ch := make(chan time.Time, 1)
		ch <- time.Now()
		return ch
	}
}

func TestRegisterOnce(t *testing.T) {
	capacity := &ateletpb.WorkerResources{Actors: testActors}
	ips := []string{"10.0.0.1"}

	t.Run("retries acquire and register, resending the same epoch", func(t *testing.T) {
		client := &fakeSupport{
			acquireFails: 2,
			registerErrs: []error{status.Error(codes.DeadlineExceeded, "timed out"), status.Error(codes.Aborted, "conflict")},
		}
		var waits []time.Duration
		epoch, err := registerOnce(context.Background(), client, &ateletpb.RegisterWorkerRequest{Ips: ips, Capacity: capacity}, 10*time.Millisecond, 25*time.Millisecond, immediately(&waits))
		if err != nil {
			t.Fatalf("registerOnce() = %v, want nil", err)
		}
		if epoch != 3 {
			t.Errorf("epoch = %d, want 3 (the first acquire that succeeded)", epoch)
		}
		if client.acquires != 3 {
			t.Errorf("acquires = %d, want 3", client.acquires)
		}
		want := &ateletpb.RegisterWorkerRequest{Epoch: 3, Ips: ips, Capacity: capacity}
		if diff := cmp.Diff([]*ateletpb.RegisterWorkerRequest{want, want, want}, client.registered, protocmp.Transform()); diff != "" {
			t.Errorf("register calls mismatch (-want +got):\n%s", diff)
		}
		wantWaits := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 25 * time.Millisecond, 25 * time.Millisecond}
		if diff := cmp.Diff(wantWaits, waits); diff != "" {
			t.Errorf("backoff mismatch (-want +got):\n%s", diff)
		}
	})

	for _, code := range []codes.Code{codes.FailedPrecondition, codes.InvalidArgument} {
		t.Run(code.String()+" is fatal", func(t *testing.T) {
			client := &fakeSupport{registerErrs: []error{status.Error(code, "refused")}}
			var waits []time.Duration
			_, err := registerOnce(context.Background(), client, &ateletpb.RegisterWorkerRequest{Ips: ips, Capacity: capacity}, time.Millisecond, time.Millisecond, immediately(&waits))
			if status.Code(err) != code {
				t.Errorf("registerOnce() = %v, want %v", err, code)
			}
			if len(client.registered) != 1 {
				t.Errorf("register calls = %d, want 1", len(client.registered))
			}
		})
	}
}

func TestRunStoresEpochBeforeReportingRuntimes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	path := filepath.Join(t.TempDir(), nodepath.SandboxRuntimesFileName)
	if _, err := sandboxruntimes.Write(path, gvisorFile(sandboxruntimes.Attributes{hardware.AttrSandboxBundleHash: "sha256:v1"})); err != nil {
		t.Fatal(err)
	}
	var epoch Epoch
	client := &runtimesRecorder{fakeSupport: &fakeSupport{}, onReport: func(req *ateletpb.SetWorkerSandboxRuntimesRequest) {
		if got := epoch.Load(); got != req.GetEpoch() {
			t.Errorf("epoch stored = %d when runtimes were reported with %d", got, req.GetEpoch())
		}
		cancel()
	}}
	err := run(ctx, ReportConfig{SandboxClass: "gvisor", RuntimesFile: path, PodIPs: []string{"10.0.0.1"}}, &ateletpb.WorkerResources{Actors: testActors}, client, &epoch, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run() = %v, want context.Canceled", err)
	}
	if epoch.Load() != 1 {
		t.Errorf("epoch = %d, want 1", epoch.Load())
	}
}

type runtimesRecorder struct {
	*fakeSupport
	onReport func(*ateletpb.SetWorkerSandboxRuntimesRequest)
}

func (r *runtimesRecorder) SetWorkerSandboxRuntimes(_ context.Context, req *ateletpb.SetWorkerSandboxRuntimesRequest) error {
	r.onReport(req)
	return nil
}

func TestReconcilerSync(t *testing.T) {
	const epoch = 4
	v1 := gvisorFile(sandboxruntimes.Attributes{
		hardware.AttrArchitecture:      runtime.GOARCH,
		hardware.AttrCPUFeatures:       "3",
		hardware.AttrSandboxBundleHash: "sha256:v1",
	})
	v2 := gvisorFile(sandboxruntimes.Attributes{
		hardware.AttrArchitecture:      runtime.GOARCH,
		hardware.AttrCPUFeatures:       "7",
		hardware.AttrSandboxBundleHash: "sha256:v2",
	}, v1.SandboxClasses[0].DefaultRuntime.Version)
	reqV1 := buildRuntimesRequest(epoch, &v1.SandboxClasses[0])
	reqV2 := buildRuntimesRequest(epoch, &v2.SandboxClasses[0])

	tests := []struct {
		name           string
		setupFile      func(t *testing.T, path string)
		registered     *ateletpb.SetWorkerSandboxRuntimesRequest
		registerErr    error
		wantRetry      bool
		wantErr        bool
		wantCalls      []*ateletpb.SetWorkerSandboxRuntimesRequest
		wantRegistered *ateletpb.SetWorkerSandboxRuntimesRequest
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
			name: "no classes",
			setupFile: func(t *testing.T, path string) {
				if _, err := sandboxruntimes.Write(path, &sandboxruntimes.File{}); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			// The file names a class this worker does not implement.
			name: "other class only",
			setupFile: func(t *testing.T, path string) {
				f := &sandboxruntimes.File{SandboxClasses: []sandboxruntimes.Class{{
					SandboxClass:   "microvm",
					DefaultRuntime: v1.SandboxClasses[0].DefaultRuntime,
				}}}
				if _, err := sandboxruntimes.Write(path, f); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "empty default runtime",
			setupFile: func(t *testing.T, path string) {
				if _, err := sandboxruntimes.Write(path, gvisorFile(nil)); err != nil {
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
			// Another worker process registered since: this one must stop.
			name: "FailedPrecondition from report is an error",
			setupFile: func(t *testing.T, path string) {
				if _, err := sandboxruntimes.Write(path, v1); err != nil {
					t.Fatal(err)
				}
			},
			registerErr: status.Error(codes.FailedPrecondition, "epoch superseded"),
			wantErr:     true,
		},
		{
			name: "first success registers and records request",
			setupFile: func(t *testing.T, path string) {
				if _, err := sandboxruntimes.Write(path, v1); err != nil {
					t.Fatal(err)
				}
			},
			wantCalls:      []*ateletpb.SetWorkerSandboxRuntimesRequest{reqV1},
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
			wantCalls:      []*ateletpb.SetWorkerSandboxRuntimesRequest{reqV2},
			wantRegistered: reqV2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), nodepath.SandboxRuntimesFileName)
			tc.setupFile(t, path)

			var calls []*ateletpb.SetWorkerSandboxRuntimesRequest
			r := newReconciler(
				ReportConfig{SandboxClass: "gvisor", RuntimesFile: path},
				epoch,
				func() (*sandboxruntimes.File, error) { return sandboxruntimes.Read(path) },
				func(_ context.Context, req *ateletpb.SetWorkerSandboxRuntimesRequest) error {
					if tc.registerErr != nil {
						return tc.registerErr
					}
					calls = append(calls, req)
					return nil
				},
			)
			r.reported = tc.registered

			gotRetry, err := r.sync(context.Background())
			if gotRetry != tc.wantRetry {
				t.Errorf("sync() retry = %v, want %v", gotRetry, tc.wantRetry)
			}
			if (err != nil) != tc.wantErr {
				t.Errorf("sync() err = %v, want error %v", err, tc.wantErr)
			}
			if diff := cmp.Diff(tc.wantCalls, calls, protocmp.Transform()); diff != "" {
				t.Errorf("register calls mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantRegistered, r.reported, protocmp.Transform()); diff != "" {
				t.Errorf("registered state mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestReconcilerRunBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	file := gvisorFile(sandboxruntimes.Attributes{hardware.AttrSandboxBundleHash: "sha256:v1"})
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
		1,
		func() (*sandboxruntimes.File, error) { return file, nil },
		func(_ context.Context, _ *ateletpb.SetWorkerSandboxRuntimesRequest) error {
			attempts++
			switch attempts {
			case 1, 2, 3:
				// Fail three times: waits should be 10ms, 20ms, 35ms (capped at maxBackoff).
				return errors.New("transient failure")
			case 4:
				// Succeed on attempt 4, then trigger a file update on changes.
				file = gvisorFile(sandboxruntimes.Attributes{hardware.AttrSandboxBundleHash: "sha256:v2"})
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

	file := gvisorFile(sandboxruntimes.Attributes{hardware.AttrSandboxBundleHash: "sha256:v1"})
	changes := make(chan struct{}, 1)
	// started reports each registration attempt as it begins; release lets
	// the second one return.
	started := make(chan int, 4)
	release := make(chan struct{})
	attempts := 0
	r := newReconciler(
		ReportConfig{SandboxClass: "gvisor", RuntimesFile: "sandbox-runtimes.json"},
		1,
		func() (*sandboxruntimes.File, error) { return file, nil },
		func(context.Context, *ateletpb.SetWorkerSandboxRuntimesRequest) error {
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
		1,
		func() (*sandboxruntimes.File, error) {
			reads++
			if reads >= 2 {
				cancel()
			}
			return gvisorFile(sandboxruntimes.Attributes{hardware.AttrSandboxBundleHash: "sha256:v1"}), nil
		},
		func(context.Context, *ateletpb.SetWorkerSandboxRuntimesRequest) error { return nil },
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
	got := make(chan *ateletpb.SetWorkerSandboxRuntimesRequest, 4)
	r := newReconciler(
		ReportConfig{SandboxClass: "gvisor", RuntimesFile: path},
		1,
		func() (*sandboxruntimes.File, error) { return sandboxruntimes.Read(path) },
		func(_ context.Context, req *ateletpb.SetWorkerSandboxRuntimesRequest) error {
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
	v1 := gvisorFile(sandboxruntimes.Attributes{
		hardware.AttrArchitecture:      runtime.GOARCH,
		hardware.AttrCPUFeatures:       "3",
		hardware.AttrSandboxBundleHash: "sha256:v1",
	})
	if _, err := sandboxruntimes.Write(path, v1); err != nil {
		t.Fatalf("writing v1: %v", err)
	}
	select {
	case req := <-got:
		if diff := cmp.Diff(buildRuntimesRequest(r.epoch, &v1.SandboxClasses[0]), req, protocmp.Transform()); diff != "" {
			t.Errorf("initial registration mismatch (-want +got):\n%s", diff)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for initial registration after file creation")
	}

	// Rewrite the file via atomic rename and verify the watcher triggers a
	// second registration.
	v2 := gvisorFile(sandboxruntimes.Attributes{
		hardware.AttrArchitecture:      runtime.GOARCH,
		hardware.AttrCPUFeatures:       "7",
		hardware.AttrSandboxBundleHash: "sha256:v2",
	}, v1.SandboxClasses[0].DefaultRuntime.Version)
	if _, err := sandboxruntimes.Write(path, v2); err != nil {
		t.Fatalf("writing v2: %v", err)
	}
	select {
	case req := <-got:
		if diff := cmp.Diff(buildRuntimesRequest(r.epoch, &v2.SandboxClasses[0]), req, protocmp.Transform()); diff != "" {
			t.Errorf("updated registration mismatch (-want +got):\n%s", diff)
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
		PodIPs:               []string{"10.0.0.1"},
		SandboxClass:         "gvisor",
		RuntimesFile:         filepath.Join(t.TempDir(), nodepath.SandboxRuntimesFileName),
	}, &Epoch{})
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
		{
			name: "missing PodIPs",
			cfg: ReportConfig{
				SocketPath:   filepath.Join(t.TempDir(), "atelet.sock"),
				SandboxClass: "gvisor",
				RuntimesFile: filepath.Join(t.TempDir(), nodepath.SandboxRuntimesFileName),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := Report(ctx, tc.cfg, &Epoch{})
			if err == nil {
				t.Fatal("Report() succeeded, want an error the caller can exit on")
			}
			if errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("Report() retried a permanent failure until the deadline: %v", err)
			}
		})
	}
}
