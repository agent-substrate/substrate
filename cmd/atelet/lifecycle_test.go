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
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"google.golang.org/grpc"
)

// useTempNodeDirs roots atelet's on-node state in temp directories so a test
// can drive the real filesystem layout. Not parallel-safe: the paths are
// process-global.
func useTempNodeDirs(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	origActors, origStatic := ateompath.ActorsDir, ateompath.StaticFilesDir
	ateompath.ActorsDir = filepath.Join(root, "actors")
	ateompath.StaticFilesDir = filepath.Join(root, "static-files")
	t.Cleanup(func() {
		ateompath.ActorsDir, ateompath.StaticFilesDir = origActors, origStatic
	})
}

// fakeAteom is a fake ateom in a worker pod. It writes the files a
// real checkpoint would leave in the checkpoint-state dir, and reads back
// what a restore was handed.
type fakeAteom struct {
	ateompb.UnimplementedAteomServer
	// snapshotFiles are written at checkpoint and reported back to atelet as
	// the exact set the snapshot consists of.
	snapshotFiles map[string]string
	// restored holds the file contents staged into the restore-state dir by
	// the most recent RestoreWorkload.
	restored map[string]string
}

func (f *fakeAteom) RunWorkload(context.Context, *ateompb.RunWorkloadRequest) (*ateompb.RunWorkloadResponse, error) {
	return &ateompb.RunWorkloadResponse{}, nil
}

func (f *fakeAteom) CheckpointWorkload(_ context.Context, req *ateompb.CheckpointWorkloadRequest) (*ateompb.CheckpointWorkloadResponse, error) {
	dir := ateompath.CheckpointStateDir(req.GetActorUid())
	names := make([]string, 0, len(f.snapshotFiles))
	for name, body := range f.snapshotFiles {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return &ateompb.CheckpointWorkloadResponse{SnapshotFiles: names}, nil
}

func (f *fakeAteom) RestoreWorkload(_ context.Context, req *ateompb.RestoreWorkloadRequest) (*ateompb.RestoreWorkloadResponse, error) {
	dir := ateompath.RestoreStateDir(req.GetActorUid())
	f.restored = map[string]string{}
	for name := range f.snapshotFiles {
		body, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		f.restored[name] = string(body)
	}
	return &ateompb.RestoreWorkloadResponse{}, nil
}

func (f *fakeAteom) TerminateWorkload(context.Context, *ateompb.TerminateWorkloadRequest) (*ateompb.TerminateWorkloadResponse, error) {
	return &ateompb.TerminateWorkloadResponse{}, nil
}

// serveFakeAteom serves ateom on a unix socket and points atelet's dialer at
// it. The socket lives in its own short temp dir.
func serveFakeAteom(t *testing.T, f *fakeAteom) {
	t.Helper()
	dir, err := os.MkdirTemp("", "ateom-")
	if err != nil {
		t.Fatalf("creating socket dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	sock := filepath.Join(dir, "ateom.sock")
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listening on %q: %v", sock, err)
	}
	srv := grpc.NewServer()
	ateompb.RegisterAteomServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	orig := ateomSocketPath
	ateomSocketPath = func(string) string { return sock }
	t.Cleanup(func() { ateomSocketPath = orig })
}

// TestLocalSnapshotGC walks an actor through
// run -> pause -> resume -> terminate over atelet's RPC surface and ensures that
// the local snapshot is garbage collected after the actor is terminated.
func TestLocalSnapshotGC(t *testing.T) {
	useTempNodeDirs(t)
	ctx := t.Context()

	const (
		atespace     = "ate-demo"
		actorName    = "counter"
		actorUID     = "actor-uid-1"
		ateomUID     = "ateom-uid-1"
		snapshotName = "pause-snap-1"
	)

	ateom := &fakeAteom{snapshotFiles: map[string]string{"checkpoint.img": "guest-memory"}}
	serveFakeAteom(t, ateom)

	host := imageVolumeTestRegistry(t)
	image := host + "/actor:v1"
	pushTestImage(t, image, singleFileLayer(t, "bin/app", "app"))

	// A single "runsc" asset served from a fake bucket: enough to exercise the
	// content-addressed asset fetch without a gVisor release tarball.
	runsc := []byte("runsc binary")
	s := &AteomHerder{
		ateomDialer:       newAteomDialer(1),
		imageCache:        newImageVolumeStore(t),
		anonGCSClient:     fakeObjectStorage{data: runsc},
		systemInfoVolumes: newSystemInfoVolumeRefresher(nil, nil),
	}
	sandboxAssets := &ateletpb.SandboxAssets{
		SandboxClass: "gvisor",
		PauseImage:   image,
		Assets: map[string]*ateletpb.ArchAssets{
			runtime.GOARCH: {Files: map[string]*ateletpb.AssetFile{
				runscAssetName: {
					Url:    "gs://test-bucket/runsc",
					Sha256: fmt.Sprintf("%x", sha256.Sum256(runsc)),
				},
			}},
		},
	}
	spec := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{{Name: "app", Image: image, Command: []string{"/bin/app"}}},
	}

	if _, err := s.Run(ctx, &ateletpb.RunRequest{
		Atespace:              atespace,
		ActorName:             actorName,
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		TargetAteomUid:        ateomUID,
		SandboxAssets:         sandboxAssets,
		Spec:                  spec,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Pause: a local checkpoint, which leaves the snapshot on this node.
	if _, err := s.Checkpoint(ctx, &ateletpb.CheckpointRequest{
		Atespace:              atespace,
		ActorName:             actorName,
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		TargetAteomUid:        ateomUID,
		Spec:                  spec,
		Scope:                 ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		Type:                  ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL,
		Config: &ateletpb.CheckpointRequest_LocalConfig{
			LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotName: snapshotName},
		},
	}); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	snapshotFile := filepath.Join(ateompath.LocalSnapshotDir(actorUID, snapshotName), "checkpoint.img")
	if _, err := os.Stat(snapshotFile); err != nil {
		t.Fatalf("pause did not write the local snapshot: %v", err)
	}

	// Resume: restores from that local snapshot.
	if _, err := s.Restore(ctx, &ateletpb.RestoreRequest{
		Atespace:              atespace,
		ActorName:             actorName,
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		TargetAteomUid:        ateomUID,
		Spec:                  spec,
		Scope:                 ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		Type:                  ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL,
		Config: &ateletpb.RestoreRequest_LocalConfig{
			LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotName: snapshotName},
		},
	}); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got := ateom.restored["checkpoint.img"]; got != "guest-memory" {
		t.Fatalf("restore staged %q for ateom, want the pause snapshot's %q", got, "guest-memory")
	}

	// Terminate: the actor is gone, and so should its snapshot be.
	if _, err := s.Terminate(ctx, &ateletpb.TerminateRequest{
		Atespace:              atespace,
		ActorName:             actorName,
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		TargetAteomUid:        ateomUID,
		Spec:                  spec,
	}); err != nil {
		t.Fatalf("Terminate: %v", err)
	}

	localDir := ateompath.LocalCheckpointsDir(actorUID)
	if _, err := os.Stat(localDir); !os.IsNotExist(err) {
		leaked, _ := filepath.Glob(filepath.Join(localDir, "*", "*"))
		t.Errorf("local checkpoint dir survived terminate (stat err = %v), leaked files: %v", err, leaked)
	}

	// Terminate is the only chance to reclaim the actor's directory: nothing
	// else on the node deletes it.
	actorDir := ateompath.ActorPath(actorUID)
	if entries, err := os.ReadDir(actorDir); err == nil {
		left := make([]string, 0, len(entries))
		for _, e := range entries {
			left = append(left, e.Name())
		}
		t.Errorf("actor dir %s survived terminate with %d entries: %v", actorDir, len(left), left)
	} else if !os.IsNotExist(err) {
		t.Errorf("reading actor dir %s: %v", actorDir, err)
	}
}

// A deleted worker pod takes its ateom with it and leaves the actor's state on
// the node — gigabytes of it for a durdir actor. Terminate is the only path
// that reclaims those directories, and nothing ever revisits a terminated
// actor's UID, so a Terminate that refuses to run without a live ateom orphans
// them for the life of the node. Both ways the sandbox can be absent are
// covered: the pod's socket gone with it, and an actor that never started one.
func TestTerminateReclaimsStateWithoutALiveSandbox(t *testing.T) {
	const (
		atespace  = "ate-demo"
		actorName = "counter"
		actorUID  = "actor-uid-1"
		ateomUID  = "ateom-uid-1"
	)

	tests := []struct {
		name string
		// setup arranges the absence under test and reports it.
		setup func(t *testing.T)
	}{
		{
			name: "ateom socket gone with its pod",
			setup: func(t *testing.T) {
				orig := ateomSocketPath
				missing := filepath.Join(t.TempDir(), "ateom.sock")
				ateomSocketPath = func(string) string { return missing }
				t.Cleanup(func() { ateomSocketPath = orig })
			},
		},
		{
			// The record is written at Run/Restore: without one, no sandbox
			// was ever started for this actor here, so there is nothing to
			// tear down and no runsc to tear it down with.
			name: "ateom reachable but the actor has no sandbox record",
			setup: func(t *testing.T) {
				serveFakeAteom(t, &fakeAteom{})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			useTempNodeDirs(t)
			ctx := t.Context()
			tt.setup(t)

			// The three directories that each hold a full copy of a durdir
			// actor's payload, which is what makes one actor cost 3 GiB.
			stateDirs := []string{
				ateompath.DurableDirVolumeMountsDir(actorUID),
				ateompath.CheckpointStateDir(actorUID),
				ateompath.RestoreStateDir(actorUID),
			}
			for _, dir := range stateDirs {
				if err := os.MkdirAll(dir, 0o755); err != nil {
					t.Fatalf("seeding %s: %v", dir, err)
				}
				if err := os.WriteFile(filepath.Join(dir, "payload"), []byte("actor state"), 0o600); err != nil {
					t.Fatalf("seeding %s: %v", dir, err)
				}
			}
			snapshotDir := ateompath.LocalSnapshotDir(actorUID, "pause-snap-1")
			if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
				t.Fatalf("seeding %s: %v", snapshotDir, err)
			}

			s := &AteomHerder{
				ateomDialer:       newAteomDialer(1),
				systemInfoVolumes: newSystemInfoVolumeRefresher(nil, nil),
			}
			if _, err := s.Terminate(ctx, &ateletpb.TerminateRequest{
				Atespace:              atespace,
				ActorName:             actorName,
				ActorUid:              actorUID,
				ActorTemplateAtespace: "default",
				ActorTemplateName:     "counter",
				TargetAteomUid:        ateomUID,
				Spec:                  &ateletpb.WorkloadSpec{},
			}); err != nil {
				t.Fatalf("Terminate: %v", err)
			}

			// #1654 made Terminate remove the actor's directory outright
			// rather than emptying the state dirs in place, so the seeded
			// payloads go with it and the whole tree is the assertion.
			if actorDir := ateompath.ActorPath(actorUID); !isGone(actorDir) {
				entries, _ := os.ReadDir(actorDir)
				t.Errorf("%s survived terminate with %d entries, want the actor's state reclaimed", actorDir, len(entries))
			}
			if localDir := ateompath.LocalCheckpointsDir(actorUID); !isGone(localDir) {
				t.Errorf("%s survived terminate, want the local snapshots pruned", localDir)
			}
		})
	}
}

// isGone reports whether path does not exist.
func isGone(path string) bool {
	_, err := os.Stat(path)
	return os.IsNotExist(err)
}
