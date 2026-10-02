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
	"time"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ateletpath"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/wait"
)

// useTempNodeDirs roots atelet's on-node state in temp directories so a test
// can drive the real filesystem layout. Not parallel-safe: the paths are
// process-global.
func useTempNodeDirs(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	origActors, origStatic := nodepath.ActorsDir, nodepath.StaticFilesDir
	nodepath.ActorsDir = filepath.Join(root, "actors")
	nodepath.StaticFilesDir = filepath.Join(root, "static-files")
	t.Cleanup(func() {
		nodepath.ActorsDir, nodepath.StaticFilesDir = origActors, origStatic
	})
}

// fakeAteom is a fake ateom in a worker pod. It writes the files a
// real checkpoint would leave in the checkpoint dir, and reads back what a
// restore was handed. Like a real ateom it takes every actor directory from
// the request, never derived from the actor UID.
type fakeAteom struct {
	ateompb.UnimplementedAteomServer
	// snapshotFiles are written at checkpoint and reported back to atelet as
	// the exact set the snapshot consists of.
	snapshotFiles map[string]string
	// restored holds the file contents staged into the restore dir by the
	// most recent RestoreWorkload.
	restored map[string]string
	// actorDirs records the ActorDirs each RPC arrived with, by RPC name.
	actorDirs map[string]*ateompb.ActorDirs
	// terminateErr, if set, is what TerminateWorkload fails with.
	terminateErr error
}

func (f *fakeAteom) recordActorDirs(rpc string, actorDirs *ateompb.ActorDirs) {
	if f.actorDirs == nil {
		f.actorDirs = map[string]*ateompb.ActorDirs{}
	}
	f.actorDirs[rpc] = actorDirs
}

func (f *fakeAteom) RunWorkload(_ context.Context, req *ateompb.RunWorkloadRequest) (*ateompb.RunWorkloadResponse, error) {
	f.recordActorDirs("RunWorkload", req.GetActorDirs())
	return &ateompb.RunWorkloadResponse{}, nil
}

func (f *fakeAteom) CheckpointWorkload(_ context.Context, req *ateompb.CheckpointWorkloadRequest) (*ateompb.CheckpointWorkloadResponse, error) {
	f.recordActorDirs("CheckpointWorkload", req.GetActorDirs())
	dir := req.GetActorDirs().GetCheckpointDir()
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
	f.recordActorDirs("RestoreWorkload", req.GetActorDirs())
	dir := req.GetActorDirs().GetRestoreDir()
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

func (f *fakeAteom) TerminateWorkload(_ context.Context, req *ateompb.TerminateWorkloadRequest) (*ateompb.TerminateWorkloadResponse, error) {
	f.recordActorDirs("TerminateWorkload", req.GetActorDirs())
	if f.terminateErr != nil {
		return nil, f.terminateErr
	}
	return &ateompb.TerminateWorkloadResponse{}, nil
}

// pointAtAteomSocket points atelet's dialer at a socket in its own short temp
// dir, and returns the socket's path. Nothing serves it yet.
func pointAtAteomSocket(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "ateom-")
	if err != nil {
		t.Fatalf("creating socket dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	sock := filepath.Join(dir, "ateom.sock")
	orig := ateomSocketPath
	ateomSocketPath = func(string) string { return sock }
	t.Cleanup(func() { ateomSocketPath = orig })
	return sock
}

// serveFakeAteomOn serves f on sock until the returned stop is called. stop
// takes ateom down: its connections close and its socket is removed.
func serveFakeAteomOn(t *testing.T, f *fakeAteom, sock string) (stop func()) {
	t.Helper()
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listening on %q: %v", sock, err)
	}
	srv := grpc.NewServer()
	ateompb.RegisterAteomServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return srv.Stop
}

// serveFakeAteom serves ateom on a unix socket and points atelet's dialer at
// it. The socket lives in its own short temp dir.
func serveFakeAteom(t *testing.T, f *fakeAteom) {
	t.Helper()
	serveFakeAteomOn(t, f, pointAtAteomSocket(t))
}

// pointAtDeadAteom points atelet's dialer at the socket of an ateom that has
// exited. leaveSocketAlive keeps the socket file behind, like a crashed ateom
// does on its hostPath.
func pointAtDeadAteom(t *testing.T, leaveSocketAlive bool) {
	t.Helper()
	sock := pointAtAteomSocket(t)
	if leaveSocketAlive {
		lis, err := net.ListenUnix("unix", &net.UnixAddr{Name: sock, Net: "unix"})
		if err != nil {
			t.Fatalf("listening on %q: %v", sock, err)
		}
		lis.SetUnlinkOnClose(false)
		lis.Close()
	}
}

// testRunsc is the runsc binary newAteomHerder's fake bucket serves.
var testRunsc = []byte("runsc binary")

// testSandboxAssets returns gvisor sandbox assets whose runsc asset is
// testRunsc, which newAteomHerder's fake bucket serves.
func testSandboxAssets(pauseImage string) *ateletpb.SandboxAssets {
	return &ateletpb.SandboxAssets{
		SandboxClass: "gvisor",
		PauseImage:   pauseImage,
		Assets: map[string]*ateletpb.ArchAssets{
			runtime.GOARCH: {Files: map[string]*ateletpb.AssetFile{
				runscAssetName: {
					Url:    "gs://test-bucket/runsc",
					Sha256: fmt.Sprintf("%x", sha256.Sum256(testRunsc)),
				},
			}},
		},
	}
}

// newAteomHerder returns an AteomHerder that pulls images into a temp image
// cache and fetches the runsc asset of testSandboxAssets from a fake bucket.
func newAteomHerder(t *testing.T) *AteomHerder {
	t.Helper()
	return &AteomHerder{
		ateomDialer:       newAteomDialer(1),
		imageCache:        newImageVolumeStore(t),
		anonGCSClient:     fakeObjectStorage{data: testRunsc},
		systemInfoVolumes: newSystemInfoVolumeRefresher(nil, nil),
	}
}

// TestTerminateWhenTerminateWorkloadFails pins that a failed TerminateWorkload
// fails Terminate only while ateom is alive. A dead ateom took the sandbox with
// it, so atelet goes on to reclaim the actor's node state.
func TestTerminateWhenTerminateWorkloadFails(t *testing.T) {
	const actorUID = "actor-uid-1"
	sandboxRec, err := recordFromRequest(testSandboxAssets(testPauseImage))
	if err != nil {
		t.Fatalf("recordFromRequest: %v", err)
	}

	tests := []struct {
		name    string
		ateom   func(t *testing.T)
		wantErr bool
	}{
		{
			name: "ateom failure",
			ateom: func(t *testing.T) {
				serveFakeAteom(t, &fakeAteom{terminateErr: status.Error(codes.Internal, "runsc delete failed")})
			},
			wantErr: true,
		},
		{
			// When ateom is dead, we assume the sandbox is also gone and
			// the TerminateWorkload error is ignored.
			name:  "ateom exited and left its socket",
			ateom: func(t *testing.T) { pointAtDeadAteom(t, true) },
		},
		{
			// When ateom is dead, we assume the sandbox is also gone and
			// the TerminateWorkload error is ignored.
			name:  "ateom exited and socket is gone",
			ateom: func(t *testing.T) { pointAtDeadAteom(t, false) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			useTempNodeDirs(t)
			if err := writeSandboxRecord(actorUID, sandboxRec); err != nil {
				t.Fatalf("writeSandboxRecord: %v", err)
			}
			tt.ateom(t)

			_, err := newAteomHerder(t).Terminate(t.Context(), &ateletpb.TerminateRequest{
				Atespace:              "ate-demo",
				ActorName:             "counter",
				ActorUid:              actorUID,
				ActorTemplateAtespace: "default",
				ActorTemplateName:     "counter",
				TargetAteomUid:        "ateom-uid-1",
				Spec:                  &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "app"}}},
			})
			if gotErr := err != nil; gotErr != tt.wantErr {
				t.Fatalf("Terminate() error = %v, wantErr %v", err, tt.wantErr)
			}

			// Removing the actor dir is Terminate's last step.
			_, statErr := os.Stat(ateletpath.ActorPath(actorUID))
			if removed := os.IsNotExist(statErr); removed == tt.wantErr {
				t.Errorf("actor dir removed = %v, want %v (stat err: %v)", removed, !tt.wantErr, statErr)
			}
		})
	}
}

// TestTerminateAfterAteomRestart terminates an actor after the ateom it ran on
// restarted, closing the conn atelet's dialer caches for it. The new ateom
// serves the same socket, and Terminate reaches it on its first attempt.
func TestTerminateAfterAteomRestart(t *testing.T) {
	ctx := t.Context()

	// 1. Serve the first ateom on the socket atelet dials.
	useTempNodeDirs(t)
	sock := pointAtAteomSocket(t)
	stop := serveFakeAteomOn(t, &fakeAteom{}, sock)
	s := newAteomHerder(t)
	image := imageVolumeTestRegistry(t) + "/actor:v1"
	pushTestImage(t, image, singleFileLayer(t, "bin/app", "app"))
	spec := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{{Name: "app", Image: image, Command: []string{"/bin/app"}}},
	}

	// 2. Run an actor on it, leaving the dialer holding a conn to it.
	if _, err := s.Run(ctx, &ateletpb.RunRequest{
		Atespace:              "ate-demo",
		ActorName:             "counter",
		ActorUid:              "actor-uid-1",
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		TargetAteomUid:        "ateom-uid-1",
		SandboxAssets:         testSandboxAssets(image),
		Spec:                  spec,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 3. Restart ateom: stop the first one, closing the cached conn, and serve
	// a new one on the same socket.
	stop()
	restarted := &fakeAteom{}
	serveFakeAteomOn(t, restarted, sock)

	// 4. Terminate the actor. It succeeds on the first attempt.
	if _, err := s.Terminate(ctx, &ateletpb.TerminateRequest{
		Atespace:              "ate-demo",
		ActorName:             "counter",
		ActorUid:              "actor-uid-1",
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		TargetAteomUid:        "ateom-uid-1",
		Spec:                  spec,
	}); err != nil {
		t.Fatalf("Terminate after the restart: %v", err)
	}

	// 5. Verify the terminate reached the restarted ateom.
	if got, want := restarted.actorDirs["TerminateWorkload"], ateletpath.ActorDirs("actor-uid-1"); !proto.Equal(got, want) {
		t.Errorf("restarted ateom's TerminateWorkload carried actor dirs %v, want %v", got, want)
	}
}

// TestTerminateAcrossAteomOutage terminates two actors that ran on an ateom
// that exits and later comes back on the same socket. While it is down,
// Terminate treats the workload as gone with it. Once it is back, the dialer's
// conn can still be backing off from the refused reconnect, and since ateom is
// alive Terminate fails rather than report the workload stopped. A retry
// reaches the new ateom.
func TestTerminateAcrossAteomOutage(t *testing.T) {
	ctx := t.Context()

	// 1. Serve the first ateom on the socket atelet dials.
	useTempNodeDirs(t)
	sock := pointAtAteomSocket(t)
	stop := serveFakeAteomOn(t, &fakeAteom{}, sock)
	s := newAteomHerder(t)
	image := imageVolumeTestRegistry(t) + "/actor:v1"
	pushTestImage(t, image, singleFileLayer(t, "bin/app", "app"))
	spec := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{{Name: "app", Image: image, Command: []string{"/bin/app"}}},
	}

	// 2. Run two actors on it, leaving the dialer holding a conn to it.
	for _, actorUID := range []string{"actor-uid-1", "actor-uid-2"} {
		if _, err := s.Run(ctx, &ateletpb.RunRequest{
			Atespace:              "ate-demo",
			ActorName:             "counter",
			ActorUid:              actorUID,
			ActorTemplateAtespace: "default",
			ActorTemplateName:     "counter",
			TargetAteomUid:        "ateom-uid-1",
			SandboxAssets:         testSandboxAssets(image),
			Spec:                  spec,
		}); err != nil {
			t.Fatalf("Run(%s): %v", actorUID, err)
		}
	}

	// 3. Take ateom down, removing its socket.
	stop()

	// 4. Terminate the first actor while ateom is down. Its workload died with
	// ateom, so Terminate succeeds. The refused reconnect puts the conn in
	// backoff.
	if _, err := s.Terminate(ctx, &ateletpb.TerminateRequest{
		Atespace:              "ate-demo",
		ActorName:             "counter",
		ActorUid:              "actor-uid-1",
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		TargetAteomUid:        "ateom-uid-1",
		Spec:                  spec,
	}); err != nil {
		t.Fatalf("Terminate with ateom down: %v", err)
	}

	// 5. Bring ateom back on the same socket.
	back := &fakeAteom{}
	serveFakeAteomOn(t, back, sock)

	// 6. Retry terminating the second actor until it succeeds. Attempts fail
	// while the conn is still backing off, since ateom is alive and could
	// still host the workload.
	terminateReq := &ateletpb.TerminateRequest{
		Atespace:              "ate-demo",
		ActorName:             "counter",
		ActorUid:              "actor-uid-2",
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		TargetAteomUid:        "ateom-uid-1",
		Spec:                  spec,
	}
	var lastErr error
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		_, lastErr = s.Terminate(ctx, terminateReq)
		return lastErr == nil, nil
	}); err != nil {
		t.Fatalf("Terminate never succeeded once ateom was back: %v", lastErr)
	}

	// 7. Verify the terminate that succeeded reached the returned ateom.
	if got, want := back.actorDirs["TerminateWorkload"], ateletpath.ActorDirs("actor-uid-2"); !proto.Equal(got, want) {
		t.Errorf("returned ateom's TerminateWorkload carried actor dirs %v, want %v", got, want)
	}
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
	snapshotFile := filepath.Join(ateletpath.LocalSnapshotDir(actorUID, snapshotName), "checkpoint.img")
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
		SandboxAssets:         sandboxAssets,
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

	// Every RPC hands ateom the same directory set; the fake already relied
	// on checkpoint_dir and restore_dir above to place and find the snapshot.
	want := ateletpath.ActorDirs(actorUID)
	for _, rpc := range []string{"RunWorkload", "CheckpointWorkload", "RestoreWorkload", "TerminateWorkload"} {
		if got := ateom.actorDirs[rpc]; !proto.Equal(got, want) {
			t.Errorf("%s carried actor actorDirs %v, want %v", rpc, got, want)
		}
	}

	localDir := ateletpath.LocalCheckpointsDir(actorUID)
	if _, err := os.Stat(localDir); !os.IsNotExist(err) {
		leaked, _ := filepath.Glob(filepath.Join(localDir, "*", "*"))
		t.Errorf("local checkpoint dir survived terminate (stat err = %v), leaked files: %v", err, leaked)
	}

	// Terminate is the only chance to reclaim the actor's directory: nothing
	// else on the node deletes it.
	actorDir := ateletpath.ActorPath(actorUID)
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

// TestRestoreUsesRequestSandboxAssets checks that Restore runs the actor with
// the sandbox assets on the request, not the ones recorded in the snapshot
// manifest.
func TestRestoreUsesRequestSandboxAssets(t *testing.T) {
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
	checkpointPause := host + "/pause:v1"
	pushTestImage(t, checkpointPause, singleFileLayer(t, "pause", "pause-v1"))
	restorePause := host + "/pause:v2"
	pushTestImage(t, restorePause, singleFileLayer(t, "pause", "pause-v2"))

	runsc := []byte("runsc binary")
	s := &AteomHerder{
		ateomDialer:       newAteomDialer(1),
		imageCache:        newImageVolumeStore(t),
		anonGCSClient:     fakeObjectStorage{data: runsc},
		systemInfoVolumes: newSystemInfoVolumeRefresher(nil, nil),
	}
	assetsWithPause := func(pause string) *ateletpb.SandboxAssets {
		return &ateletpb.SandboxAssets{
			SandboxClass: "gvisor",
			PauseImage:   pause,
			Assets: map[string]*ateletpb.ArchAssets{
				runtime.GOARCH: {Files: map[string]*ateletpb.AssetFile{
					runscAssetName: {
						Url:    "gs://test-bucket/runsc",
						Sha256: fmt.Sprintf("%x", sha256.Sum256(runsc)),
					},
				}},
			},
		}
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
		SandboxAssets:         assetsWithPause(checkpointPause),
		Spec:                  spec,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

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

	manifest, err := os.ReadFile(filepath.Join(ateletpath.LocalSnapshotDir(actorUID, snapshotName), sandboxManifestName))
	if err != nil {
		t.Fatalf("reading snapshot manifest: %v", err)
	}
	manifestRec, err := unmarshalSandboxRecord(manifest)
	if err != nil {
		t.Fatalf("unmarshalling snapshot manifest: %v", err)
	}
	if manifestRec.PauseImage != checkpointPause {
		t.Fatalf("manifest pause image = %q, want %q", manifestRec.PauseImage, checkpointPause)
	}

	if _, err := s.Restore(ctx, &ateletpb.RestoreRequest{
		Atespace:              atespace,
		ActorName:             actorName,
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		TargetAteomUid:        ateomUID,
		SandboxAssets:         assetsWithPause(restorePause),
		Spec:                  spec,
		Scope:                 ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		Type:                  ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL,
		Config: &ateletpb.RestoreRequest_LocalConfig{
			LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotName: snapshotName},
		},
	}); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	got, err := readSandboxRecord(actorUID)
	if err != nil {
		t.Fatalf("reading on-node sandbox record: %v", err)
	}
	if got.PauseImage != restorePause {
		t.Errorf("restored actor pause image = %q, want the request's %q", got.PauseImage, restorePause)
	}
}
