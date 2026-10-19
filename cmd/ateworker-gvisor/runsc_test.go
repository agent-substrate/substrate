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
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc/codes"

	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

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

func TestEnsureSpecWritesOnce(t *testing.T) {
	root := t.TempDir()
	actorDirs := &ateworkerpb.ActorDirs{RootDir: root, OciBundleDir: filepath.Join(root, "bundles")}
	r := &runsc{
		actorUID:   "uid-ensure",
		actorDirs:  actorDirs,
		containers: []*ateworkerpb.Container{{Name: "app", ContainerSpec: &ateworkerpb.ContainerSpec{Args: []string{"/app"}}}},
	}
	if err := os.MkdirAll(ociBundlePath(actorDirs, "app"), 0o700); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{ocispec.SandboxID, "app"} {
		if err := r.ensureSpec(name); err != nil {
			t.Fatalf("first ensureSpec(%q) = %v", name, err)
		}
		cfgPath := filepath.Join(ociBundlePath(actorDirs, name), "config.json")
		sentinel := []byte(`{"sentinel":true}`)
		if err := os.WriteFile(cfgPath, sentinel, 0o600); err != nil {
			t.Fatal(err)
		}
		// Second ensureSpec call must be a no-op and preserve the sentinel file.
		if err := r.ensureSpec(name); err != nil {
			t.Fatalf("second ensureSpec(%q) = %v", name, err)
		}
		got, err := os.ReadFile(cfgPath)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, sentinel) {
			t.Errorf("second ensureSpec(%q) overwrote config.json: got %s, want %s", name, got, sentinel)
		}
	}

	if err := r.ensureSpec("missing"); err == nil {
		t.Error("ensureSpec(missing) = nil, want error")
	}
	if r.specsWritten["missing"] {
		t.Error("specsWritten[missing] = true after failed writeSpec")
	}
}

func TestPrepareAppContainers(t *testing.T) {
	root := t.TempDir()
	bundleDir := filepath.Join(root, "bundles")
	actorDirs := &ateworkerpb.ActorDirs{RootDir: root, OciBundleDir: bundleDir}
	containers := []*ateworkerpb.Container{
		{Name: "init", ContainerSpec: &ateworkerpb.ContainerSpec{Args: []string{"/init"}}},
		{Name: "app", ContainerSpec: &ateworkerpb.ContainerSpec{Args: []string{"/app"}}},
	}
	for _, ac := range containers {
		if err := os.MkdirAll(filepath.Join(bundleDir, ac.GetName(), "rootfs"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	r := &runsc{
		actorUID:   "uid-prep",
		actorDirs:  actorDirs,
		containers: containers,
	}

	if err := prepareAppContainers(t.Context(), actorDirs, r, containers); err != nil {
		t.Fatalf("prepareAppContainers() = %v", err)
	}
	for _, ac := range containers {
		bundle := ociBundlePath(actorDirs, ac.GetName())
		if _, err := os.Stat(filepath.Join(bundle, "config.json")); err != nil {
			t.Errorf("%s config.json missing: %v", ac.GetName(), err)
		}
		if _, err := os.Stat(filepath.Join(bundle, "rootfs", "etc", "hosts")); err != nil {
			t.Errorf("%s rootfs/etc/hosts missing: %v", ac.GetName(), err)
		}
		if !r.specsWritten[ac.GetName()] {
			t.Errorf("specsWritten[%q] = false, want true", ac.GetName())
		}
	}

	cancelCtx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := prepareAppContainers(cancelCtx, actorDirs, r, containers); err == nil {
		t.Error("prepareAppContainers with canceled context = nil, want error")
	}
}

func BenchmarkGVisorRestorePrep(b *testing.B) {
	containers := []*ateworkerpb.Container{
		{
			Name:          "init",
			ContainerSpec: &ateworkerpb.ContainerSpec{Args: []string{"/init", "--setup"}},
			DurableDirVolumeMounts: []*ateworkerpb.VolumeMount{
				{Name: "data", MountPath: "/var/data"},
			},
		},
		{
			Name:          "app",
			ContainerSpec: &ateworkerpb.ContainerSpec{Args: []string{"/app", "--serve"}},
			DurableDirVolumeMounts: []*ateworkerpb.VolumeMount{
				{Name: "data", MountPath: "/var/data"},
			},
		},
		{
			Name:          "sidecar",
			ContainerSpec: &ateworkerpb.ContainerSpec{Args: []string{"/sidecar", "--proxy"}},
			DurableDirVolumeMounts: []*ateworkerpb.VolumeMount{
				{Name: "logs", MountPath: "/var/log/app"},
			},
		},
	}
	spec := &ateworkerpb.WorkloadSpec{Containers: containers}
	durableVols := durableVolumeNames(spec)

	setupFixture := func(b *testing.B) (restoreDir string, actorDirs *ateworkerpb.ActorDirs) {
		b.Helper()
		root := b.TempDir()
		bundleDir := filepath.Join(root, "bundles")
		for _, ac := range containers {
			if err := os.MkdirAll(filepath.Join(bundleDir, ac.GetName(), "rootfs"), 0o755); err != nil {
				b.Fatal(err)
			}
		}
		restoreDir = filepath.Join(root, "restore")
		for _, sub := range []string{"", "fs", "rootfs"} {
			if err := os.MkdirAll(filepath.Join(restoreDir, sub), 0o700); err != nil {
				b.Fatal(err)
			}
		}
		files := map[string]int{
			"checkpoint.img":         256 * 1024,
			"pages.img":              8 * 1024 * 1024,
			"rootfs/fscheckpoint.pb": 1024,
			"rootfs/pages.img":       4 * 1024 * 1024,
			"fs/fscheckpoint.pb":     1024,
			"fs/pages.img":           2 * 1024 * 1024,
		}
		for rel, sz := range files {
			if err := os.WriteFile(filepath.Join(restoreDir, rel), bytes.Repeat([]byte{0x5A}, sz), 0o600); err != nil {
				b.Fatal(err)
			}
		}
		actorDirs = &ateworkerpb.ActorDirs{
			RootDir:                   root,
			OciBundleDir:              bundleDir,
			RestoreDir:                restoreDir,
			DurableDirVolumeMountsDir: filepath.Join(root, "durable"),
			SystemInfoVolumeRootsDir:  filepath.Join(root, "sysinfo"),
			ExternalVolumeMountsDir:   filepath.Join(root, "extvols"),
		}
		return restoreDir, actorDirs
	}

	b.Run("Sequential", func(b *testing.B) {
		restoreDir, actorDirs := setupFixture(b)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			rcmd := &runsc{
				actorUID:       "bench-gvisor-actor",
				actorDirs:      actorDirs,
				durableVolumes: durableVols,
				containers:     containers,
			}
			readaheadCheckpointDir(b.Context(), restoreDir)
			// Without ensureSpec, cmdCreate and cmdRestore each called writeSpec.
			if err := rcmd.writeSpec(ocispec.SandboxID); err != nil {
				b.Fatal(err)
			}
			if err := rcmd.writeSpec(ocispec.SandboxID); err != nil {
				b.Fatal(err)
			}
			for _, ac := range containers {
				if err := setupAppRootfs(actorDirs, ac.GetName()); err != nil {
					b.Fatal(err)
				}
				if err := rcmd.writeSpec(ac.GetName()); err != nil {
					b.Fatal(err)
				}
				if err := rcmd.writeSpec(ac.GetName()); err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		restoreDir, actorDirs := setupFixture(b)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			rcmd := &runsc{
				actorUID:       "bench-gvisor-actor",
				actorDirs:      actorDirs,
				durableVolumes: durableVols,
				containers:     containers,
			}
			sandboxSpecDone := make(chan error, 1)
			go func() {
				sandboxSpecDone <- rcmd.ensureSpec(ocispec.SandboxID)
			}()
			appPrepDone := make(chan error, 1)
			go func() {
				appPrepDone <- prepareAppContainers(b.Context(), actorDirs, rcmd, containers)
			}()
			readaheadDone := make(chan struct{})
			go func() {
				defer close(readaheadDone)
				readaheadCheckpointDir(b.Context(), restoreDir)
			}()
			if err := <-sandboxSpecDone; err != nil {
				b.Fatal(err)
			}
			if err := rcmd.ensureSpec(ocispec.SandboxID); err != nil {
				b.Fatal(err)
			}
			if err := rcmd.ensureSpec(ocispec.SandboxID); err != nil {
				b.Fatal(err)
			}
			if err := <-appPrepDone; err != nil {
				b.Fatal(err)
			}
			for _, ac := range containers {
				if err := rcmd.ensureSpec(ac.GetName()); err != nil {
					b.Fatal(err)
				}
				if err := rcmd.ensureSpec(ac.GetName()); err != nil {
					b.Fatal(err)
				}
			}
			<-readaheadDone
		}
	})
}
