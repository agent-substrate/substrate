//go:build linux

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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc/codes"

	"go.ate.dev/substrate/cmd/ateworker-gvisor/internal/cgroupstats"
	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/actorlog"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateworker"
	"go.ate.dev/substrate/internal/atunnel"
	"go.ate.dev/substrate/internal/atunnel/egress"
	"go.ate.dev/substrate/internal/atunnel/ingress"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

const fakeRunscStatusEnv = "FAKE_RUNSC_SANDBOX_STATUS"

func TestMain(m *testing.M) {
	if filepath.Base(os.Args[0]) == "runsc" {
		runFakeRunsc()
	}
	os.Exit(m.Run())
}

func runFakeRunsc() {
	var subcmd string
	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "-root" {
			i++
			continue
		}
		if !strings.HasPrefix(os.Args[i], "-") {
			subcmd = os.Args[i]
			break
		}
	}
	switch subcmd {
	case "state":
		if os.Args[len(os.Args)-1] != ocispec.SandboxID {
			os.Exit(128)
		}
		status := os.Getenv(fakeRunscStatusEnv)
		if status == "missing" {
			os.Exit(128)
		}
		_ = json.NewEncoder(os.Stdout).Encode(specs.State{Status: specs.ContainerState(status)})
	case "list", "kill", "wait", "delete":
	default:
		fmt.Fprintf(os.Stderr, "unexpected runsc command %q: %v\n", subcmd, os.Args)
		os.Exit(2)
	}
	os.Exit(0)
}

var testActorDirs = &ateworkerpb.ActorDirs{
	RootDir:      "/node/actors/test-actor-123",
	OciBundleDir: "/node/actors/test-actor-123/bundle",
}

// writeSpec builds a workload container's spec from the request, and a
// sandbox spec with no process and no rootfs. It makes the sandbox bundle.
func TestWriteSpec(t *testing.T) {
	root := t.TempDir()
	actorDirs := &ateworkerpb.ActorDirs{RootDir: root, OciBundleDir: filepath.Join(root, "bundles")}
	r := &runsc{
		actorUID:   "uid-a",
		actorDirs:  actorDirs,
		containers: []*ateworkerpb.Container{{Name: "app", ContainerSpec: &ateworkerpb.ContainerSpec{Args: []string{"/app", "serve"}}}},
	}
	// atelet makes the application bundle; writeSpec does not make it.
	if err := r.writeSpec("app"); err == nil {
		t.Error("writeSpec(app) = nil without the bundle atelet makes")
	}
	if err := os.MkdirAll(ociBundlePath(actorDirs, "app"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"app", ocispec.SandboxID} {
		if err := r.writeSpec(name); err != nil {
			t.Fatalf("writeSpec(%q) = %v", name, err)
		}
		b, err := os.ReadFile(filepath.Join(ociBundlePath(actorDirs, name), "config.json"))
		if err != nil {
			t.Fatal(err)
		}
		var spec specs.Spec
		if err := json.Unmarshal(b, &spec); err != nil {
			t.Fatal(err)
		}
		if got := spec.Annotations["io.kubernetes.cri.container-name"]; got != name {
			t.Errorf("%s container-name annotation = %q, want the spec shaped for gVisor", name, got)
		}
		if name == ocispec.SandboxID {
			if spec.Process != nil || spec.Root != nil || spec.Mounts != nil {
				t.Errorf("sandbox spec has process %v, root %v, mounts %v; want none", spec.Process, spec.Root, spec.Mounts)
			}
			continue
		}
		if want := []string{"/app", "serve"}; spec.Process == nil || !slices.Equal(spec.Process.Args, want) {
			t.Errorf("%s process = %+v, want args %v", name, spec.Process, want)
		}
	}
	if err := r.writeSpec("missing"); err == nil {
		t.Error("writeSpec() = nil for a container outside the workload spec")
	}
}

func mustFlagStore(t *testing.T) *flagStore {
	t.Helper()
	cfgPath := filepath.Join(t.TempDir(), "flags.json")
	const testFlagsJSON = `{
  "globalFlags": [
    "-global1",
    "-global2"
  ],
  "commands": {
    "restore": {
      "globalFlags": ["-restore-global"],
      "subcommandFlags": ["-restore-sub1", "-restore-sub2"]
    },
    "start": {
      "globalFlags": ["-start-global"]
    },
    "list": {
      "subcommandFlags": ["-list-sub"]
    }
  }
}`
	if err := os.WriteFile(cfgPath, []byte(testFlagsJSON), 0o600); err != nil {
		t.Fatalf("failed to write test flags.json: %v", err)
	}
	store, err := newFlagStore(t.Context(), cfgPath)
	if err != nil {
		t.Fatalf("newFlagStore(%s) failed: %v", cfgPath, err)
	}
	return store
}

func TestBuildArgs(t *testing.T) {
	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "test-actor-123",
		actorDirs: testActorDirs,
		flags:     mustFlagStore(t),
	}

	for _, tc := range []struct {
		name          string
		command       string
		containerName string
		extraArgs     []string
		want          []string
	}{
		{
			name:          "kill",
			command:       "kill",
			containerName: "",
			extraArgs:     []string{"my-container", "SIGTERM"},
			want: []string{
				"-global1",
				"-global2",
				"-root", "/node/actors/test-actor-123/runsc-state",
				"kill",
				"my-container",
				"SIGTERM",
			},
		},
		{
			name:          "wait",
			command:       "wait",
			containerName: "my-container",
			want: []string{
				"-global1",
				"-global2",
				"-root", "/node/actors/test-actor-123/runsc-state",
				"wait",
				"my-container",
			},
		},
		{
			name:          "wait -restore",
			command:       "wait",
			containerName: "my-container",
			extraArgs:     []string{"-restore"},
			want: []string{
				"-global1",
				"-global2",
				"-root", "/node/actors/test-actor-123/runsc-state",
				"wait",
				"-restore",
				"my-container",
			},
		},
		{
			name:          "wait -fsrestore",
			command:       "wait",
			containerName: "my-container",
			extraArgs:     []string{"-fsrestore"},
			want: []string{
				"-global1",
				"-global2",
				"-root", "/node/actors/test-actor-123/runsc-state",
				"wait",
				"-fsrestore",
				"my-container",
			},
		},
		{
			name:          "list",
			command:       "list",
			containerName: "",
			want: []string{
				"-global1",
				"-global2",
				"-root", "/node/actors/test-actor-123/runsc-state",
				"list",
				"-list-sub",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := r.buildArgs(tc.command, tc.containerName, tc.extraArgs...)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("buildArgs(%q, %q, %v) = %v, want %v", tc.command, tc.containerName, tc.extraArgs, got, tc.want)
			}
		})
	}
}

func TestCheckpointArgs(t *testing.T) {
	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "test-actor-123",
		actorDirs: testActorDirs,
		flags:     mustFlagStore(t),
	}
	checkpointDir := "/node/actors/test-actor-123/checkpoints/snap-1"

	got := r.checkpointArgs(ocispec.SandboxID, checkpointDir, []string{"rootfs/=app:/", "fs/=app:/var/data"})
	want := []string{
		"-global1",
		"-global2",
		"-root", "/node/actors/test-actor-123/runsc-state",
		"checkpoint",
		"-image-path", checkpointDir,
		"-fs-checkpoint-paths", "rootfs/=app:/",
		"-fs-checkpoint-paths", "fs/=app:/var/data",
		ocispec.SandboxID,
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("checkpointArgs() = %v, want %v", got, want)
	}
}

func TestFsCheckpointArgs(t *testing.T) {
	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "test-actor-123",
		actorDirs: testActorDirs,
		flags:     mustFlagStore(t),
	}
	checkpointDir := "/node/actors/test-actor-123/checkpoints/snap-1"

	got := r.fsCheckpointArgs(ocispec.SandboxID, checkpointDir, []string{"rootfs/=app:/", "fs/=app:/var/data"})
	want := []string{
		"-global1",
		"-global2",
		"-root", "/node/actors/test-actor-123/runsc-state",
		"fscheckpoint",
		"-image-path", checkpointDir,
		"-path", "rootfs/=app:/",
		"-path", "fs/=app:/var/data",
		ocispec.SandboxID,
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("fsCheckpointArgs() = %v, want %v", got, want)
	}
}

// Only the sandbox gets -no-root-container: runsc refuses it for any other
// container.
func TestCreateArgs(t *testing.T) {
	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "test-actor-123",
		actorDirs: testActorDirs,
		flags:     mustFlagStore(t),
	}
	for _, tc := range []struct {
		container string
		extra     []string
		want      []string
	}{{
		container: ocispec.SandboxID,
		extra:     []string{"-fs-restore-image-path", "/snap/fs"},
		want: []string{
			"-global1",
			"-global2",
			"-root", "/node/actors/test-actor-123/runsc-state",
			"create",
			"-no-root-container",
			"-bundle", "/node/actors/test-actor-123/bundle/" + ocispec.SandboxID,
			"-pid-file", "/node/actors/test-actor-123/pidfiles/" + ocispec.SandboxID + ".pid",
			"-fs-restore-image-path", "/snap/fs",
			ocispec.SandboxID,
		},
	}, {
		container: "app",
		want: []string{
			"-global1",
			"-global2",
			"-root", "/node/actors/test-actor-123/runsc-state",
			"create",
			"-bundle", "/node/actors/test-actor-123/bundle/app",
			"-pid-file", "/node/actors/test-actor-123/pidfiles/app.pid",
			"app",
		},
	}} {
		if got := r.createArgs(tc.container, tc.extra); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("createArgs(%q) = %v, want %v", tc.container, got, tc.want)
		}
	}
}

func TestRestoreArgs(t *testing.T) {
	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "test-actor-123",
		actorDirs: testActorDirs,
		flags:     mustFlagStore(t),
	}
	checkpointDir := "/node/actors/test-actor-123/checkpoints/snap-1"

	for _, tc := range []struct {
		container string
		want      []string
	}{{
		container: ocispec.SandboxID,
		want: []string{
			"-global1",
			"-global2",
			"-restore-global",
			"-root", "/node/actors/test-actor-123/runsc-state",
			"restore",
			"-restore-sub1",
			"-restore-sub2",
			"-no-root-container",
			"-bundle", "/node/actors/test-actor-123/bundle/" + ocispec.SandboxID,
			"-image-path", checkpointDir,
			"-pid-file", "/node/actors/test-actor-123/pidfiles/" + ocispec.SandboxID + ".pid",
			ocispec.SandboxID,
		},
	}, {
		container: "app",
		want: []string{
			"-global1",
			"-global2",
			"-restore-global",
			"-root", "/node/actors/test-actor-123/runsc-state",
			"restore",
			"-restore-sub1",
			"-restore-sub2",
			"-bundle", "/node/actors/test-actor-123/bundle/app",
			"-image-path", checkpointDir,
			"-pid-file", "/node/actors/test-actor-123/pidfiles/app.pid",
			"app",
		},
	}} {
		if got := r.restoreArgs(tc.container, checkpointDir); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("restoreArgs(%q) = %v, want %v", tc.container, got, tc.want)
		}
	}
}

func TestRestoreWorkloadRejectsEmptyRestoreDir(t *testing.T) {
	s := &AteWorkerService{
		locks:    actorlock.New(),
		inFlight: actorlock.NewInFlight(),
	}

	noDirs := validRestoreRequest()
	noDirs.ActorDirs = nil
	noRestoreDir := validRestoreRequest()
	noRestoreDir.ActorDirs.RestoreDir = ""
	for _, tc := range []struct {
		name string
		req  *ateworkerpb.RestoreWorkloadRequest
		want string
	}{
		{name: "nil actor_dirs", req: noDirs, want: "actor_dirs"},
		{name: "empty restore_dir", req: noRestoreDir, want: "restore_dir"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.RestoreWorkload(context.Background(), tc.req)
			if apierror.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("RestoreWorkload() = %v, want InvalidArgument naming %s", err, tc.want)
			}
		})
	}
}

func stageFakeRunsc(t *testing.T, actorUID string) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := nodepath.ActorSandboxAssetsDir(actorUID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(dir, "runsc")
	if err := os.Link(exe, dst); err != nil {
		b, rerr := os.ReadFile(exe)
		if rerr != nil {
			t.Fatal(rerr)
		}
		if werr := os.WriteFile(dst, b, 0o555); werr != nil {
			t.Fatal(werr)
		}
	}
	return dst
}

func TestCheckpointWorkloadVolumesWithoutDurableDir(t *testing.T) {
	origActorsDir := nodepath.ActorsDir
	nodepath.ActorsDir = t.TempDir()
	t.Cleanup(func() { nodepath.ActorsDir = origActorsDir })
	runscPath := stageFakeRunsc(t, testActor.UID)

	for _, tc := range []struct {
		name      string
		status    string
		oomKilled bool
		wantErr   string
	}{
		{name: "running sandbox writes empty snapshot", status: string(specs.StateRunning)},
		{name: "stopped sandbox fails", status: string(specs.StateStopped), wantErr: "stopped"},
		{name: "missing sandbox fails", status: "missing", wantErr: "runsc state"},
		{name: "oom-killed sandbox fails", status: string(specs.StateRunning), oomKilled: true, wantErr: "memory limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(fakeRunscStatusEnv, tc.status)

			s := NewService(
				&atunnel.Tunnel{Ingress: &ingress.Server{}, Egress: &egress.Proxy{}},
				actorlog.NewActorLogger(io.Discard, false),
				10,
				mustFlagStore(t),
			)
			s.cgroupRoot = t.TempDir()
			s.readSandboxCgroup = func(string) (cgroupstats.Sample, error) {
				return cgroupstats.Sample{}, os.ErrNotExist
			}
			withUsageRecorder(s)
			h := hostWithEpoch(s, time.Now(), true)
			h.ctx, h.cancel = context.WithCancel(context.Background())
			t.Cleanup(h.cancel)
			h.oomKilled.Store(tc.oomKilled)

			root := t.TempDir()
			req := validCheckpointRequest()
			req.Fidelity = ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
			req.RunscPath = runscPath
			req.ActorDirs = &ateworkerpb.ActorDirs{
				RootDir:                   root,
				OciBundleDir:              filepath.Join(root, "bundle"),
				CheckpointDir:             filepath.Join(root, "checkpoint-state"),
				RestoreDir:                filepath.Join(root, "restore-state"),
				DurableDirVolumeMountsDir: filepath.Join(root, "durable-dirs"),
				SystemInfoVolumeRootsDir:  filepath.Join(root, "system-info"),
				ExternalVolumeMountsDir:   filepath.Join(root, "volumes"),
				SandboxAssetsDir:          nodepath.ActorSandboxAssetsDir(testActor.UID),
			}

			resp, err := s.CheckpointWorkload(context.Background(), req)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("CheckpointWorkload() error = %v, want substring %q", err, tc.wantErr)
				}
				if got := ateworker.ReuseCheckpoint(req.ActorDirs.CheckpointDir, req.Fidelity); got != nil {
					t.Fatalf("ReuseCheckpoint() after failed checkpoint = %v, want nil", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("CheckpointWorkload() unexpected error: %v", err)
			}
			if len(resp.GetSnapshotFiles()) != 0 || len(resp.GetDataSnapshotFiles()) != 0 {
				t.Fatalf("CheckpointWorkload() = %v, want empty snapshot files", resp)
			}
			if got := ateworker.ReuseCheckpoint(req.ActorDirs.CheckpointDir, req.Fidelity); got == nil {
				t.Fatal("ReuseCheckpoint() = nil, want recorded empty checkpoint")
			}
		})
	}
}
