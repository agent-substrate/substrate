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
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

const (
	qemuConfig        = "[hypervisor.qemu]\nshared_fs = \"virtio-fs\"\n"
	firecrackerConfig = "[hypervisor.firecracker]\n"
)

// testActorDirs returns the ActorDirs atelet would send for the actor, with
// the sandbox assets dir where the validation expects it. The nodepath's actors
// directory is a temp directory for the test.
func testActorDirs(t *testing.T, uid string) *ateworkerpb.ActorDirs {
	t.Helper()
	old := nodepath.ActorsDir
	nodepath.ActorsDir = filepath.Join(t.TempDir(), "actors")
	t.Cleanup(func() { nodepath.ActorsDir = old })
	root := filepath.Join(nodepath.ActorsDir, uid)
	dirs := &ateworkerpb.ActorDirs{
		RootDir:                   root,
		OciBundleDir:              filepath.Join(root, "bundles"),
		CheckpointDir:             filepath.Join(root, "checkpoint"),
		RestoreDir:                filepath.Join(root, "restore"),
		DurableDirVolumeMountsDir: filepath.Join(root, "durable"),
		SystemInfoVolumeRootsDir:  filepath.Join(root, "system-info"),
		ExternalVolumeMountsDir:   filepath.Join(root, "external"),
		SandboxAssetsDir:          nodepath.ActorSandboxAssetsDir(uid),
	}
	if err := os.MkdirAll(dirs.GetSandboxAssetsDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	return dirs
}

// writeAsset writes the kata-config asset into the actor's assets dir and
// returns its path.
func writeAsset(t *testing.T, dirs *ateworkerpb.ActorDirs, content string) string {
	t.Helper()
	path := filepath.Join(dirs.GetSandboxAssetsDir(), kataConfigAsset)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func runRequest(uid string, dirs *ateworkerpb.ActorDirs, assets map[string]string, containers ...*ateworkerpb.Container) *ateworkerpb.RunWorkloadRequest {
	return &ateworkerpb.RunWorkloadRequest{
		Atespace:          "team",
		ActorName:         "agent",
		ActorUid:          uid,
		RuntimeAssetPaths: assets,
		Spec:              &ateworkerpb.WorkloadSpec{Containers: containers},
		ActorDirs:         dirs,
	}
}

func appContainer() *ateworkerpb.Container {
	return &ateworkerpb.Container{
		Name:          "app",
		ContainerSpec: &ateworkerpb.ContainerSpec{Args: []string{"/app"}},
	}
}

func volumeContainer() *ateworkerpb.Container {
	c := appContainer()
	c.DurableDirVolumeMounts = []*ateworkerpb.VolumeMount{{Name: "data", MountPath: "/data"}}
	return c
}

func TestCheckRunRequest(t *testing.T) {
	tests := []struct {
		name       string
		config     string
		containers []*ateworkerpb.Container
		// asset picks the request's asset path, given the one that exists.
		asset    func(t *testing.T, dirs *ateworkerpb.ActorDirs, real string) map[string]string
		wantCode codes.Code
		wantErr  string
		wantHyp  string
	}{
		{
			name:       "qemu without volumes",
			config:     qemuConfig,
			containers: []*ateworkerpb.Container{appContainer()},
			wantHyp:    "qemu",
		},
		{
			name:       "qemu with a volume",
			config:     qemuConfig,
			containers: []*ateworkerpb.Container{volumeContainer()},
			wantHyp:    "qemu",
		},
		{
			name:       "firecracker without volumes",
			config:     firecrackerConfig,
			containers: []*ateworkerpb.Container{appContainer()},
			wantHyp:    "firecracker",
		},
		{
			name:       "firecracker with a volume",
			config:     firecrackerConfig,
			containers: []*ateworkerpb.Container{appContainer(), volumeContainer()},
			wantCode:   codes.FailedPrecondition,
			wantErr:    "firecracker hypervisor shares no host directories",
		},
		{
			name:       "no kata-config asset",
			config:     qemuConfig,
			containers: []*ateworkerpb.Container{appContainer()},
			asset: func(*testing.T, *ateworkerpb.ActorDirs, string) map[string]string {
				return map[string]string{"other": "/x"}
			},
			wantCode: codes.InvalidArgument,
			wantErr:  "Required",
		},
		{
			name:       "asset outside the actor's assets dir",
			config:     qemuConfig,
			containers: []*ateworkerpb.Container{appContainer()},
			asset: func(t *testing.T, _ *ateworkerpb.ActorDirs, _ string) map[string]string {
				p := filepath.Join(t.TempDir(), "kata-config")
				if err := os.WriteFile(p, []byte(qemuConfig), 0o600); err != nil {
					t.Fatal(err)
				}
				return map[string]string{kataConfigAsset: p}
			},
			wantCode: codes.InvalidArgument,
			wantErr:  "must be inside",
		},
		{
			name:       "asset is a symlink",
			config:     qemuConfig,
			containers: []*ateworkerpb.Container{appContainer()},
			asset: func(t *testing.T, dirs *ateworkerpb.ActorDirs, real string) map[string]string {
				link := filepath.Join(dirs.GetSandboxAssetsDir(), "link")
				if err := os.Symlink(real, link); err != nil {
					t.Fatal(err)
				}
				return map[string]string{kataConfigAsset: link}
			},
			wantCode: codes.InvalidArgument,
			wantErr:  "symlink",
		},
		{
			name:       "config without a hypervisor",
			config:     "[runtime]\n",
			containers: []*ateworkerpb.Container{appContainer()},
			wantCode:   codes.FailedPrecondition,
			wantErr:    "want exactly one",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dirs := testActorDirs(t, testUID)
			real := writeAsset(t, dirs, tt.config)
			assets := map[string]string{kataConfigAsset: real}
			if tt.asset != nil {
				assets = tt.asset(t, dirs, real)
			}
			path, config, err := checkRunRequest(runRequest(testUID, dirs, assets, tt.containers...))
			if tt.wantErr != "" {
				if got := apierror.Code(err); got != tt.wantCode || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("checkRunRequest() = %v (%v), want %v containing %q", err, got, tt.wantCode, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("checkRunRequest() = %v", err)
			}
			if path != real || config.hypervisor != tt.wantHyp {
				t.Errorf("checkRunRequest() = %q, %+v, want %q and hypervisor %s", path, config, real, tt.wantHyp)
			}
		})
	}
}

func TestCheckRunRequestRejectsAnotherActorsAssetsDir(t *testing.T) {
	dirs := testActorDirs(t, testUID)
	writeAsset(t, dirs, qemuConfig)
	other := proto.Clone(dirs).(*ateworkerpb.ActorDirs)
	other.SandboxAssetsDir = filepath.Join(nodepath.ActorsDir, "1b1a6a3c-7d5e-4f8a-9c2d-3e4f5a6b7c8d", "sandbox-assets")
	_, _, err := checkRunRequest(runRequest(testUID, other, map[string]string{kataConfigAsset: filepath.Join(other.GetSandboxAssetsDir(), kataConfigAsset)}, appContainer()))
	if got := apierror.Code(err); got != codes.InvalidArgument {
		t.Fatalf("checkRunRequest() = %v (%v), want InvalidArgument", err, got)
	}
}

// Snapshots of any fidelity are refused, and a checkpoint stops the actor's VM,
// since the control plane crashes the actor and releases its worker.
func TestSnapshotsAreUnimplemented(t *testing.T) {
	dirs := testActorDirs(t, testUID)
	spec := &ateworkerpb.WorkloadSpec{Containers: []*ateworkerpb.Container{appContainer()}}
	for _, fidelity := range []ateworkerpb.SnapshotFidelity{
		ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
		ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
		ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
	} {
		t.Run(fidelity.String(), func(t *testing.T) {
			s, _ := newTestService(t, 1)
			f := newFakeShim(syscall.SIGKILL)
			hostWithVM(t, s, f)

			_, err := s.CheckpointWorkload(t.Context(), &ateworkerpb.CheckpointWorkloadRequest{
				Atespace: "team", ActorName: "agent", ActorUid: testUID, Spec: spec, Fidelity: fidelity, ActorDirs: dirs,
			})
			if got := apierror.Code(err); got != codes.Unimplemented || !strings.Contains(err.Error(), "take a "+fidelity.String()+" snapshot") {
				t.Errorf("CheckpointWorkload() = %v (%v), want Unimplemented naming the fidelity", err, got)
			}
			if !slices.Contains(f.log(), "Shutdown "+testUID+" now=true") || s.lookupActor(testUID) != nil {
				t.Errorf("the actor's VM was not stopped and unhosted: %v", f.log())
			}

			_, err = s.RestoreWorkload(t.Context(), &ateworkerpb.RestoreWorkloadRequest{
				Atespace: "team", ActorName: "agent", ActorUid: testUID, Spec: spec, Fidelity: fidelity, ActorDirs: dirs,
			})
			if got := apierror.Code(err); got != codes.Unimplemented || !strings.Contains(err.Error(), "restore a "+fidelity.String()+" snapshot") {
				t.Errorf("RestoreWorkload() = %v (%v), want Unimplemented naming the fidelity", err, got)
			}
		})
	}
}

// A checkpoint for an actor the worker does not host is refused too.
func TestCheckpointOfAnActorNotHosted(t *testing.T) {
	s, _ := newTestService(t, 1)
	_, err := s.CheckpointWorkload(t.Context(), &ateworkerpb.CheckpointWorkloadRequest{
		Atespace: "team", ActorName: "agent", ActorUid: testUID,
		Spec:      &ateworkerpb.WorkloadSpec{Containers: []*ateworkerpb.Container{appContainer()}},
		Fidelity:  ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		ActorDirs: testActorDirs(t, testUID),
	})
	if got := apierror.Code(err); got != codes.Unimplemented {
		t.Errorf("CheckpointWorkload() = %v (%v), want Unimplemented", err, got)
	}
}

// A malformed request is the caller's error, not an unsupported feature.
func TestSnapshotRequestsAreValidatedFirst(t *testing.T) {
	s, _ := newTestService(t, 1)
	if _, err := s.CheckpointWorkload(t.Context(), &ateworkerpb.CheckpointWorkloadRequest{}); apierror.Code(err) != codes.InvalidArgument {
		t.Errorf("CheckpointWorkload() = %v, want InvalidArgument", err)
	}
	if _, err := s.RestoreWorkload(t.Context(), &ateworkerpb.RestoreWorkloadRequest{}); apierror.Code(err) != codes.InvalidArgument {
		t.Errorf("RestoreWorkload() = %v, want InvalidArgument", err)
	}
}

func TestRunWorkloadBeforeRegistration(t *testing.T) {
	s, _ := newTestService(t, 1)
	s.epoch.Store(0)
	dirs := testActorDirs(t, testUID)
	asset := writeAsset(t, dirs, qemuConfig)
	_, err := s.RunWorkload(t.Context(), runRequest(testUID, dirs, map[string]string{kataConfigAsset: asset}, appContainer()))
	if got := apierror.Code(err); got != codes.Unavailable {
		t.Fatalf("RunWorkload() = %v (%v), want Unavailable before the worker registered", err, got)
	}
}

func TestRunWorkloadWhileDraining(t *testing.T) {
	s, tun := newTestService(t, 1)
	s.shuttingDown.Store(true)
	dirs := testActorDirs(t, testUID)
	asset := writeAsset(t, dirs, qemuConfig)
	_, err := s.RunWorkload(t.Context(), runRequest(testUID, dirs, map[string]string{kataConfigAsset: asset}, appContainer()))
	if got := apierror.Code(err); got != codes.Unavailable {
		t.Fatalf("RunWorkload() = %v (%v), want Unavailable", err, got)
	}
	if len(tun.sessions) != 0 {
		t.Errorf("a draining worker set up %d actor networks", len(tun.sessions))
	}
}

func TestRequirePreview(t *testing.T) {
	tests := []struct {
		name    string
		gates   []string
		wantErr bool
	}{
		{"gate on", []string{string(preview.GateKataSandbox)}, false},
		{"gate off", nil, true},
		{"another gate on", []string{string(preview.GateExternalVolumes)}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			preview.InitForTest(t, tt.gates...)
			err := requirePreview()
			if (err != nil) != tt.wantErr {
				t.Fatalf("requirePreview() = %v, want error %v", err, tt.wantErr)
			}
			if err != nil && !strings.Contains(err.Error(), "--preview=KataSandbox") {
				t.Errorf("requirePreview() = %v, want it to name the flag to set", err)
			}
		})
	}
}

func TestHostActorCapacity(t *testing.T) {
	s, tun := newTestService(t, 1)
	ctx := t.Context()
	first, err := s.hostActor(ctx, testAttribution("aaaaaaaa-0000-0000-0000-000000000001"))
	if err != nil {
		t.Fatalf("hostActor() = %v", err)
	}
	_, err = s.hostActor(ctx, testAttribution("aaaaaaaa-0000-0000-0000-000000000002"))
	if got := apierror.Code(err); got != codes.ResourceExhausted {
		t.Fatalf("hostActor() for a second actor = %v (%v), want ResourceExhausted", err, got)
	}
	// The same actor keeps its slot, and the old activation stops being watched.
	again, err := s.hostActor(ctx, testAttribution("aaaaaaaa-0000-0000-0000-000000000001"))
	if err != nil {
		t.Fatalf("hostActor() for the same actor = %v", err)
	}
	if first.ctx.Err() == nil || !first.stopping.Load() {
		t.Error("the replaced activation was not canceled and marked stopping")
	}
	if s.lookupActor("aaaaaaaa-0000-0000-0000-000000000001") != again {
		t.Error("the actor's record was not replaced")
	}
	if err := s.unhostActor(ctx, "aaaaaaaa-0000-0000-0000-000000000001"); err != nil {
		t.Fatalf("unhostActor() = %v", err)
	}
	if _, err := s.hostActor(ctx, testAttribution("aaaaaaaa-0000-0000-0000-000000000002")); err != nil {
		t.Errorf("hostActor() after the slot was freed = %v", err)
	}
	// The refused actor got none: three hostActor calls succeeded.
	if len(tun.sessions) != 3 {
		t.Errorf("networks served = %d, want 3", len(tun.sessions))
	}
}

func TestHostActorNetworkFailureFreesTheSlot(t *testing.T) {
	s, tun := newTestService(t, 1)
	tun.netErr = errors.New("no netns")
	if _, err := s.hostActor(t.Context(), testAttribution(testUID)); err == nil || !strings.Contains(err.Error(), "no netns") {
		t.Fatalf("hostActor() = %v, want the network error", err)
	}
	if s.lookupActor(testUID) != nil {
		t.Error("a failed hostActor left the actor hosted")
	}
	tun.netErr = nil
	if _, err := s.hostActor(t.Context(), testAttribution(testUID)); err != nil {
		t.Errorf("hostActor() after the failure = %v, want the slot free", err)
	}
}
