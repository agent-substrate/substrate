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
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc/codes"

	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/actorlog"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
)

var testActorDirs = &ateworkerpb.ActorDirs{
	RootDir:      "/node/actors/test-actor-123",
	OciBundleDir: "/node/actors/test-actor-123/bundle",
}

func TestRunscStartupLogAttribution(t *testing.T) {
	for _, onGCE := range []bool{false, true} {
		for _, command := range []string{"create", "start", "restore"} {
			for _, container := range []string{"app", "sidecar"} {
				t.Run(fmt.Sprintf("gce=%t/%s/%s", onGCE, command, container), func(t *testing.T) {
					var buf syncLogBuffer
					logger := actorlog.NewActorLogger(&buf, onGCE)
					attribution := resources.ActorAttribution{Ref: resources.ActorRef{Atespace: "space", Name: "actor-" + container}, UID: "uid-" + container}
					var forwarders sync.WaitGroup
					pipe := func(name string) *os.File {
						pr, pw, err := os.Pipe()
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { pw.Close() })
						forwarders.Go(func() {
							defer pr.Close()
							logger.WrapContainerLogs(pr, attribution, name)
						})
						return pw
					}
					appLogs, sandboxLogs := pipe(container), pipe(ocispec.SandboxID)
					root := t.TempDir()
					dirs := &ateworkerpb.ActorDirs{RootDir: root, OciBundleDir: filepath.Join(root, "bundle")}
					if err := os.MkdirAll(ociBundlePath(dirs, container), 0o700); err != nil {
						t.Fatal(err)
					}
					flags, err := newFlagStore(t.Context(), "kodata/internal_flags.json")
					if err != nil {
						t.Fatal(err)
					}
					r := &runsc{path: "testdata/runsc-logs.sh", actorUID: attribution.UID, actorDirs: dirs, flags: flags, startupLogs: sandboxLogs,
						containers: []*ateworkerpb.Container{{Name: container, ContainerSpec: &ateworkerpb.ContainerSpec{Args: []string{"/app"}}}},
					}
					switch command {
					case "create":
						err = r.cmdCreate(t.Context(), appLogs, container, nil)
					case "start":
						err = r.cmdStart(t.Context(), appLogs, container)
					case "restore":
						err = r.cmdRestore(t.Context(), appLogs, container, filepath.Join(root, "checkpoint"))
					}
					appLogs.Close()
					sandboxLogs.Close()
					forwarders.Wait()
					if err != nil {
						t.Fatal(err)
					}
					want := map[string]string{
						"runsc diagnostic for " + container:                            ocispec.SandboxID,
						"stdout from " + container:                                     container,
						"I1026 19:30:00.000000 123 app.go:1] stderr from " + container: container,
					}
					labelKey := "labels"
					if onGCE {
						labelKey = "logging.googleapis.com/labels"
					}
					for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
						record := decodeRecord(t, []byte(line))
						message, _ := record["message"].(string)
						labels, _ := record[labelKey].(map[string]any)
						if labels[string(ateattr.ActorNameKey)] != attribution.Ref.Name || labels[string(ateattr.AtespaceKey)] != attribution.Ref.Atespace {
							t.Errorf("wrong actor labels: %v", labels)
						}
						containerName, ok := want[message]
						if !ok {
							t.Errorf("unexpected log: %s", line)
							continue
						}
						if got := labels[string(ateattr.ActorContainerNameKey)]; got != containerName {
							t.Errorf("%q container label = %v, want %s", message, got, containerName)
						}
						delete(want, message)
					}
					if len(want) != 0 {
						t.Errorf("missing logs: %v", want)
					}
				})
			}
		}
	}
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
