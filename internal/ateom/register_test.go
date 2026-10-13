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

package ateom

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/sandboxversions"
)

// testActors stands in for the ateom's own ceiling, which is a flag in
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
			if err := os.WriteFile(filepath.Join(dir, CPULimitFile), []byte(tc.cpu), 0o600); err != nil {
				t.Fatalf("writing CPU limit: %v", err)
			}
			if err := os.WriteFile(filepath.Join(dir, MemoryLimitFile), []byte(tc.memory), 0o600); err != nil {
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
		VersionsFile:         filepath.Join(t.TempDir(), "sandbox-versions.json"),
	})
	if err == nil {
		t.Fatal("Report() with unreadable credentials succeeded, want an error the caller can exit on")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Report() retried a permanent failure until the deadline: %v", err)
	}
}

// A runtime without a class is refused by the control plane on every attempt,
// and without a versions file there is nothing to register, so an ateom that
// names neither must exit rather than retry.
func TestReportFailsFastOnMissingConfig(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  ReportConfig
	}{
		{name: "no sandbox class", cfg: ReportConfig{VersionsFile: "/var/lib/ate/ateoms/uid/sandbox-versions.json"}},
		{name: "no versions file", cfg: ReportConfig{SandboxClass: "gvisor"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			tc.cfg.SocketPath = filepath.Join(t.TempDir(), "atelet.sock")

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

// versionsFile is a sandbox versions file for class naming vers.
func versionsFile(class, config string, vers ...*ateletpb.SandboxVersion) *ateletpb.WorkerSandboxVersions {
	return &ateletpb.WorkerSandboxVersions{SandboxClass: class, SandboxConfig: config, Versions: vers}
}

// recordingRegistrar records RegisterWorker requests, failing them with err.
type recordingRegistrar struct {
	mu   sync.Mutex
	got  []*ateletpb.RegisterWorkerRequest
	err  error
	seen chan *ateletpb.RegisterWorkerRequest
}

func (r *recordingRegistrar) register(_ context.Context, req *ateletpb.RegisterWorkerRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.got = append(r.got, req)
	if r.seen != nil {
		r.seen <- req
	}
	return nil
}

func newTestReconciler(t *testing.T, prober *fakeProber, registrar *recordingRegistrar) (*reconciler, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sandbox-versions.json")
	r := &reconciler{
		cfg:      ReportConfig{SandboxClass: "gvisor", VersionsFile: path, Prober: prober},
		capacity: &ateletpb.WorkerResources{Actors: testActors},
		read:     func() (*ateletpb.WorkerSandboxVersions, error) { return sandboxversions.Read(path) },
		register: registrar.register,
		runtimes: newRuntimeCache(),
	}
	return r, path
}

func TestReconcilerSync(t *testing.T) {
	capacity := &ateletpb.WorkerResources{Actors: testActors}
	wantRequest := func(def string, restorable ...string) *ateletpb.RegisterWorkerRequest {
		req := &ateletpb.RegisterWorkerRequest{Capacity: capacity, DefaultRuntime: wantRuntime(def, true)}
		for _, name := range restorable {
			req.RestorableRuntimes = append(req.RestorableRuntimes, wantRuntime(name, true))
		}
		return req
	}
	v1v2 := versionsFile("gvisor", "gvisor-release", testVersion("v2", true), testVersion("v1", false))
	v2v3 := versionsFile("gvisor", "gvisor-next", testVersion("v3", true), testVersion("v2", false))

	// Each step changes the file, then syncs.
	type step struct {
		file *ateletpb.WorkerSandboxVersions // nil removes the file
		raw  string                          // written instead of file when set
		// keep leaves the file as the previous step left it.
		keep        bool
		registerErr error
		probeErrs   map[string]error
		wantRetry   bool
	}
	tests := []struct {
		name  string
		steps []step
		// wantRegistered is every accepted request, in order.
		wantRegistered []*ateletpb.RegisterWorkerRequest
	}{{
		name:  "waits for the file",
		steps: []step{{}},
	}, {
		name:           "registers the file's versions",
		steps:          []step{{file: v1v2}},
		wantRegistered: []*ateletpb.RegisterWorkerRequest{wantRequest("v2", "v1")},
	}, {
		name:           "an unchanged file registers once",
		steps:          []step{{file: v1v2}, {keep: true}},
		wantRegistered: []*ateletpb.RegisterWorkerRequest{wantRequest("v2", "v1")},
	}, {
		name:           "a changed file registers again",
		steps:          []step{{file: v1v2}, {file: v2v3}},
		wantRegistered: []*ateletpb.RegisterWorkerRequest{wantRequest("v2", "v1"), wantRequest("v3", "v2")},
	}, {
		// atelet removed the directory, or the file is being replaced.
		name:           "a file that goes missing keeps the registration",
		steps:          []step{{file: v1v2}, {}},
		wantRegistered: []*ateletpb.RegisterWorkerRequest{wantRequest("v2", "v1")},
	}, {
		name:  "a file for another class is ignored",
		steps: []step{{file: versionsFile("microvm", "microvm-release", testVersion("v2", true))}},
	}, {
		name:  "a corrupt file is ignored",
		steps: []step{{raw: "{not json"}},
	}, {
		name:           "a corrupt file keeps the registration",
		steps:          []step{{file: v1v2}, {raw: "{not json"}},
		wantRegistered: []*ateletpb.RegisterWorkerRequest{wantRequest("v2", "v1")},
	}, {
		// Each retry rereads the file, so the retry registers what is
		// there by then.
		name: "a failed registration is retried with the current file",
		steps: []step{
			{file: v1v2, registerErr: errors.New("worker record does not exist yet"), wantRetry: true},
			{file: v2v3},
		},
		wantRegistered: []*ateletpb.RegisterWorkerRequest{wantRequest("v3", "v2")},
	}, {
		name: "a default that fails to probe is retried and keeps the registration",
		steps: []step{
			{file: v1v2},
			{file: v2v3, probeErrs: map[string]error{"v3": errors.New("runsc crashed")}, wantRetry: true},
			{keep: true},
		},
		wantRegistered: []*ateletpb.RegisterWorkerRequest{wantRequest("v2", "v1"), wantRequest("v3", "v2")},
	}, {
		name: "a default that fails to probe on first registration is retried",
		steps: []step{
			{file: v1v2, probeErrs: map[string]error{"v2": errors.New("runsc crashed")}, wantRetry: true},
			{keep: true},
		},
		wantRegistered: []*ateletpb.RegisterWorkerRequest{wantRequest("v2", "v1")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prober := &fakeProber{}
			registrar := &recordingRegistrar{}
			r, path := newTestReconciler(t, prober, registrar)
			for i, st := range tt.steps {
				switch {
				case st.keep:
				case st.raw != "":
					if err := os.WriteFile(path, []byte(st.raw), 0o600); err != nil {
						t.Fatal(err)
					}
				case st.file != nil:
					if _, err := sandboxversions.Write(path, st.file); err != nil {
						t.Fatal(err)
					}
				default:
					if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
						t.Fatal(err)
					}
				}
				prober.errs = st.probeErrs
				registrar.err = st.registerErr

				if retry := r.sync(context.Background()); retry != st.wantRetry {
					t.Errorf("step %d: sync() retry = %v, want %v", i, retry, st.wantRetry)
				}
			}
			if diff := cmp.Diff(tt.wantRegistered, registrar.got, protocmp.Transform()); diff != "" {
				t.Errorf("registered requests mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// The loop sees atelet's writes without being told: the file appearing after
// startup, and each replacement after that.
func TestReconcilerRunWatchesFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	registrar := &recordingRegistrar{seen: make(chan *ateletpb.RegisterWorkerRequest, 8)}
	r, path := newTestReconciler(t, &fakeProber{}, registrar)
	done := make(chan error, 1)
	go func() { done <- r.run(ctx, watchFile(ctx, path)) }()

	expect := func(def string, restorable ...string) {
		t.Helper()
		want := &ateletpb.RegisterWorkerRequest{Capacity: r.capacity, DefaultRuntime: wantRuntime(def, true)}
		for _, name := range restorable {
			want.RestorableRuntimes = append(want.RestorableRuntimes, wantRuntime(name, true))
		}
		select {
		case got := <-registrar.seen:
			if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
				t.Fatalf("registered request mismatch (-want +got):\n%s", diff)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("no registration after the file changed")
		}
	}
	write := func(v *ateletpb.WorkerSandboxVersions) {
		t.Helper()
		if _, err := sandboxversions.Write(path, v); err != nil {
			t.Fatal(err)
		}
	}

	write(versionsFile("gvisor", "gvisor-release", testVersion("v2", true), testVersion("v1", false)))
	expect("v2", "v1")
	write(versionsFile("gvisor", "gvisor-next", testVersion("v3", true), testVersion("v2", false)))
	expect("v3", "v2")

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("run() = %v, want context.Canceled", err)
	}
	if len(registrar.got) != 2 {
		t.Errorf("registered %d times, want 2", len(registrar.got))
	}
}
