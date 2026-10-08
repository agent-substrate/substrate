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
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ateletpath"
	"github.com/agent-substrate/substrate/cmd/atelet/internal/trustbundle"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
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

// fakeWorker is a fake worker in a worker pod. It writes the files a
// real checkpoint would leave in the checkpoint dir, and reads back what a
// restore was handed. Like a real worker it takes every actor directory from
// the request, never derived from the actor UID.
type fakeWorker struct {
	ateworkerpb.UnimplementedWorkerServer
	// snapshotFiles are written at checkpoint and reported back to atelet as
	// the exact set the snapshot consists of.
	snapshotFiles map[string]string
	// restored holds the file contents staged into the restore dir by the
	// most recent RestoreWorkload.
	restored map[string]string
	// actorDirs records the ActorDirs each RPC arrived with, by RPC name.
	actorDirs map[string]*ateworkerpb.ActorDirs
	// preserveRestoreDir records the PreserveRestoreDir flag from the most
	// recent RestoreWorkload request.
	preserveRestoreDir bool
}

func (f *fakeWorker) recordActorDirs(rpc string, actorDirs *ateworkerpb.ActorDirs) {
	if f.actorDirs == nil {
		f.actorDirs = map[string]*ateworkerpb.ActorDirs{}
	}
	f.actorDirs[rpc] = actorDirs
}

func (f *fakeWorker) RunWorkload(_ context.Context, req *ateworkerpb.RunWorkloadRequest) (*ateworkerpb.RunWorkloadResponse, error) {
	f.recordActorDirs("RunWorkload", req.GetActorDirs())
	return &ateworkerpb.RunWorkloadResponse{}, nil
}

func (f *fakeWorker) CheckpointWorkload(_ context.Context, req *ateworkerpb.CheckpointWorkloadRequest) (*ateworkerpb.CheckpointWorkloadResponse, error) {
	f.recordActorDirs("CheckpointWorkload", req.GetActorDirs())
	dir := req.GetActorDirs().GetCheckpointDir()
	names := make([]string, 0, len(f.snapshotFiles))
	for name, body := range f.snapshotFiles {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return &ateworkerpb.CheckpointWorkloadResponse{SnapshotFiles: names}, nil
}

func (f *fakeWorker) RestoreWorkload(_ context.Context, req *ateworkerpb.RestoreWorkloadRequest) (*ateworkerpb.RestoreWorkloadResponse, error) {
	f.recordActorDirs("RestoreWorkload", req.GetActorDirs())
	f.preserveRestoreDir = req.GetPreserveRestoreDir()
	dir := req.GetActorDirs().GetRestoreDir()
	f.restored = map[string]string{}
	for name := range f.snapshotFiles {
		body, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		f.restored[name] = string(body)
	}
	return &ateworkerpb.RestoreWorkloadResponse{}, nil
}

func (f *fakeWorker) TerminateWorkload(_ context.Context, req *ateworkerpb.TerminateWorkloadRequest) (*ateworkerpb.TerminateWorkloadResponse, error) {
	f.recordActorDirs("TerminateWorkload", req.GetActorDirs())
	return &ateworkerpb.TerminateWorkloadResponse{}, nil
}

// serveFakeWorker serves worker on a unix socket and points atelet's dialer at
// it. The socket lives in its own short temp dir.
func serveFakeWorker(t *testing.T, f *fakeWorker) {
	t.Helper()
	dir, err := os.MkdirTemp("", "worker-")
	if err != nil {
		t.Fatalf("creating socket dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	sock := filepath.Join(dir, "worker.sock")
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listening on %q: %v", sock, err)
	}
	srv := grpc.NewServer()
	ateworkerpb.RegisterWorkerServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	orig := workerSocketPath
	workerSocketPath = func(string) string { return sock }
	t.Cleanup(func() { workerSocketPath = orig })
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
		workerUID    = "worker-uid-1"
		snapshotName = "pause-snap-1"
	)

	worker := &fakeWorker{snapshotFiles: map[string]string{"checkpoint.img": "guest-memory"}}
	serveFakeWorker(t, worker)

	host := imageVolumeTestRegistry(t)
	image := host + "/actor:v1"
	pushTestImage(t, image, singleFileLayer(t, "bin/app", "app"))

	// A single "runsc" asset served from a fake bucket: enough to exercise the
	// content-addressed asset fetch without a gVisor release tarball.
	runsc := []byte("runsc binary")
	s := &Atelet{
		workerDialer:      newWorkerDialer(1),
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
		TargetWorkerPodUid:    workerUID,
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
		TargetWorkerPodUid:    workerUID,
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
		TargetWorkerPodUid:    workerUID,
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
	if got := worker.restored["checkpoint.img"]; got != "guest-memory" {
		t.Fatalf("restore staged %q for worker, want the pause snapshot's %q", got, "guest-memory")
	}
	if !worker.preserveRestoreDir {
		t.Errorf("RestoreWorkload preserve_restore_dir = false, want true for pure-local restore")
	}
	if entries, err := os.ReadDir(ateletpath.RestoreStateDir(actorUID)); err != nil || len(entries) != 0 {
		t.Fatalf("expected RestoreStateDir to remain empty on local pause restore, got entries=%v err=%v", entries, err)
	}

	// Terminate: the actor is gone, and so should its snapshot be.
	if _, err := s.Terminate(ctx, &ateletpb.TerminateRequest{
		Atespace:              atespace,
		ActorName:             actorName,
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		TargetWorkerPodUid:    workerUID,
		Spec:                  spec,
	}); err != nil {
		t.Fatalf("Terminate: %v", err)
	}

	// Every RPC hands worker the same directory set, except that a local
	// restore points restore_dir at the pause snapshot; the fake already
	// relied on checkpoint_dir and restore_dir above to place and find it.
	for _, rpc := range []string{"RunWorkload", "CheckpointWorkload", "RestoreWorkload", "TerminateWorkload"} {
		want := ateletpath.ActorDirs(actorUID)
		if rpc == "RestoreWorkload" {
			want.RestoreDir = ateletpath.LocalSnapshotDir(actorUID, snapshotName)
		}
		if got := worker.actorDirs[rpc]; !proto.Equal(got, want) {
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
		workerUID    = "worker-uid-1"
		snapshotName = "pause-snap-1"
	)

	worker := &fakeWorker{snapshotFiles: map[string]string{"checkpoint.img": "guest-memory"}}
	serveFakeWorker(t, worker)

	host := imageVolumeTestRegistry(t)
	image := host + "/actor:v1"
	pushTestImage(t, image, singleFileLayer(t, "bin/app", "app"))
	checkpointPause := host + "/pause:v1"
	pushTestImage(t, checkpointPause, singleFileLayer(t, "pause", "pause-v1"))
	restorePause := host + "/pause:v2"
	pushTestImage(t, restorePause, singleFileLayer(t, "pause", "pause-v2"))

	runsc := []byte("runsc binary")
	s := &Atelet{
		workerDialer:      newWorkerDialer(1),
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
		TargetWorkerPodUid:    workerUID,
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
		TargetWorkerPodUid:    workerUID,
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
		TargetWorkerPodUid:    workerUID,
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

// A failed activation preserves a live projection only until it starts resetting
// the actor's directories. Once reset deletes the files, their old registration
// must be fenced, including when reset itself or an early preparation step fails.
func TestActivationFailureBeforeRegistration(t *testing.T) {
	for _, tc := range []struct {
		name        string
		restore     bool
		failure     string
		wantErr     string
		wantPresent bool
	}{
		{name: "Run asset failure preserves projection", failure: "asset", wantErr: "asset unavailable", wantPresent: true},
		{name: "Run partial reset removes registration", failure: "reset", wantErr: "while removing volume dir"},
		{name: "Run record failure removes registration", failure: "record", wantErr: "while recording sandbox assets"},
		{name: "Restore partial reset removes registration", restore: true, failure: "reset", wantErr: "while removing volume dir"},
		{name: "Restore manifest failure removes registration", restore: true, failure: "manifest", wantErr: "while reading local snapshot manifest"},
		{name: "Restore asset failure removes registration", restore: true, failure: "asset", wantErr: "asset unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useTempNodeDirs(t)
			const actorUID, snapshotName = "actor-uid-1", "pause-snap-1"
			ctx := t.Context()
			store := newCTBStore(t)
			certA := string(testCertPEM(t))
			store.set(t, certA)
			refresher := newSystemInfoVolumeRefresher(trustbundle.NewSource(store.lister.Get, nil), nil)
			spec := &ateletpb.WorkloadSpec{Volumes: []*ateletpb.Volume{{
				Name: "trust", Source: &ateletpb.Volume_SystemInfo{SystemInfo: trustVolumeSpec("ca.pem")},
			}}}
			registered, err := refresher.Register(actorUID, resources.ActorRef{Atespace: "team-a", Name: "actor-1"}, systemInfoVolumesFor(actorUID, spec))
			if err != nil {
				t.Fatal(err)
			}
			projectedPath := filepath.Join(ateletpath.SystemInfoVolumeRoot(actorUID, "trust"), "ca.pem")
			if got, err := os.ReadFile(projectedPath); err != nil || string(got) != certA {
				t.Fatalf("initial projection = %q, %v; want cert A", got, err)
			}

			content := []byte("runsc binary")
			assetHash := fmt.Sprintf("%x", sha256.Sum256(content))
			if tc.restore && tc.failure != "manifest" {
				writeLocalSnapshot(t, ateletpath.LocalSnapshotDir(actorUID, snapshotName), sandboxAssetsRecord{
					SandboxClass: "gvisor", PauseImage: testPauseImage,
					Assets:        map[string]assetEntry{"runsc": {URL: "gs://test-bucket/runsc", SHA256: assetHash}},
					SnapshotFiles: []string{"checkpoint.img"},
				}, map[string]string{"checkpoint.img": "guest-memory"})
			}
			if tc.failure == "reset" {
				// Reset refuses to remove a populated external-volume directory,
				// but has already removed the system-info roots by then.
				dir := ateletpath.VolumeHostPath(actorUID, "mounted")
				if err := os.MkdirAll(dir, 0o750); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "keep"), []byte("volume data"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.failure == "record" {
				if err := os.MkdirAll(ateletpath.ActorSandboxAssetsFile(actorUID), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			storage := fakeObjectStorage{data: content}
			if tc.failure == "asset" {
				storage.err = fmt.Errorf("asset unavailable")
			}
			s := &Atelet{anonGCSClient: storage, systemInfoVolumes: refresher}
			assets := &ateletpb.SandboxAssets{
				SandboxClass: "gvisor", PauseImage: testPauseImage,
				Assets: map[string]*ateletpb.ArchAssets{runtime.GOARCH: {Files: map[string]*ateletpb.AssetFile{
					runscAssetName: {Url: "gs://test-bucket/runsc", Sha256: assetHash},
				}}},
			}
			if tc.restore {
				_, err = s.Restore(ctx, &ateletpb.RestoreRequest{
					Atespace: "team-a", ActorName: "actor-1", ActorUid: actorUID, TargetWorkerPodUid: "worker-uid-1",
					SandboxAssets: assets, Spec: spec,
					Scope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL, Type: ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL,
					Config: &ateletpb.RestoreRequest_LocalConfig{LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotName: snapshotName}},
				})
			} else {
				_, err = s.Run(ctx, &ateletpb.RunRequest{
					Atespace: "team-a", ActorName: "actor-1", ActorUid: actorUID, TargetWorkerPodUid: "worker-uid-1",
					SandboxAssets: assets, Spec: spec,
				})
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("activation error = %v, want %q", err, tc.wantErr)
			}

			// Inspect disk and registry before changing the bundle hash: a later
			// rotation can hide a stale registration by recreating deleted files.
			if tc.wantPresent {
				if got, err := os.ReadFile(projectedPath); err != nil || string(got) != certA {
					t.Errorf("pre-reset failure lost projection: %q, %v", got, err)
				}
				if refresher.actors[actorUID] != registered || registered.stale {
					t.Error("pre-reset failure invalidated the existing registration")
				}
			} else {
				if _, err := os.Stat(projectedPath); !os.IsNotExist(err) {
					t.Fatalf("reset did not delete prior projection: stat error = %v", err)
				}
				if got := refresher.actors[actorUID]; got != nil {
					t.Errorf("reset deleted projection but left registration live: %p", got)
				}
				if !registered.stale {
					t.Error("reset left the old registration available to an in-flight refresh")
				}
			}
			certB := string(testCertPEM(t))
			store.set(t, certB)
			if err := refresher.refreshBundle(ctx, trustbundle.EgressName); err != nil {
				t.Fatal(err)
			}
			if tc.wantPresent {
				if got, err := os.ReadFile(projectedPath); err != nil || string(got) != certB {
					t.Errorf("preserved projection stopped refreshing: %q, %v", got, err)
				}
			} else if _, err := os.Stat(projectedPath); !os.IsNotExist(err) {
				t.Errorf("refresh recreated an invalidated projection: stat error = %v", err)
			}
		})
	}
}

func TestRunFailureAfterRegistrationRemovesOwnRegistration(t *testing.T) {
	useTempNodeDirs(t)
	store := newCTBStore(t)
	store.set(t, string(testCertPEM(t)))
	refresher := newSystemInfoVolumeRefresher(trustbundle.NewSource(store.lister.Get, nil), nil)
	content := []byte("runsc binary")
	assetHash := fmt.Sprintf("%x", sha256.Sum256(content))
	s := &Atelet{
		anonGCSClient:     fakeObjectStorage{data: content},
		imageCache:        newImageVolumeStore(t),
		systemInfoVolumes: refresher,
	}
	spec := &ateletpb.WorkloadSpec{
		Volumes: []*ateletpb.Volume{{
			Name:   "trust",
			Source: &ateletpb.Volume_SystemInfo{SystemInfo: trustVolumeSpec("ca.pem")},
		}},
	}
	_, err := s.Run(t.Context(), &ateletpb.RunRequest{
		Atespace:           "team-a",
		ActorName:          "actor-run",
		ActorUid:           "actor-uid-run",
		TargetWorkerPodUid: "worker-uid-1",
		SandboxAssets: &ateletpb.SandboxAssets{
			SandboxClass: "gvisor",
			PauseImage:   "://invalid-image",
			Assets: map[string]*ateletpb.ArchAssets{runtime.GOARCH: {Files: map[string]*ateletpb.AssetFile{
				runscAssetName: {Url: "gs://test-bucket/runsc", Sha256: assetHash},
			}}},
		},
		Spec: spec,
	})
	if err == nil || !strings.Contains(err.Error(), "while creating pause OCI bundle") {
		t.Fatalf("Run error = %v, want the post-registration OCI preparation failure", err)
	}
	if _, err := os.ReadFile(filepath.Join(ateletpath.SystemInfoVolumeRoot("actor-uid-run", "trust"), "ca.pem")); err != nil {
		t.Fatalf("Register did not write its projection before OCI preparation failed: %v", err)
	}
	if got := refresher.actors["actor-uid-run"]; got != nil {
		t.Fatalf("failed Run left its registration live: %p", got)
	}
}

func TestRestoreFailureAfterRegistrationRemovesOwnRegistration(t *testing.T) {
	useTempNodeDirs(t)
	const (
		actorUID     = "actor-uid-restore-after"
		snapshotName = "pause-snap-after"
	)
	store := newCTBStore(t)
	store.set(t, string(testCertPEM(t)))
	refresher := newSystemInfoVolumeRefresher(trustbundle.NewSource(store.lister.Get, nil), nil)
	content := []byte("runsc binary")
	assetHash := fmt.Sprintf("%x", sha256.Sum256(content))
	writeLocalSnapshot(t, ateletpath.LocalSnapshotDir(actorUID, snapshotName), sandboxAssetsRecord{
		SandboxClass:  "gvisor",
		PauseImage:    "://invalid-image",
		Assets:        map[string]assetEntry{"runsc": {URL: "gs://test-bucket/runsc", SHA256: assetHash}},
		SnapshotFiles: []string{"checkpoint.img"},
	}, map[string]string{"checkpoint.img": "guest-memory"})

	spec := &ateletpb.WorkloadSpec{Volumes: []*ateletpb.Volume{{
		Name:   "trust",
		Source: &ateletpb.Volume_SystemInfo{SystemInfo: trustVolumeSpec("ca.pem")},
	}}}
	s := &Atelet{
		anonGCSClient:     fakeObjectStorage{data: content},
		imageCache:        newImageVolumeStore(t),
		systemInfoVolumes: refresher,
	}
	_, err := s.Restore(t.Context(), &ateletpb.RestoreRequest{
		Atespace:           "team-a",
		ActorName:          "actor-restore-after",
		ActorUid:           actorUID,
		TargetWorkerPodUid: "worker-uid-1",
		SandboxAssets: &ateletpb.SandboxAssets{
			SandboxClass: "gvisor",
			PauseImage:   "://invalid-image",
			Assets: map[string]*ateletpb.ArchAssets{runtime.GOARCH: {Files: map[string]*ateletpb.AssetFile{
				runscAssetName: {Url: "gs://test-bucket/runsc", Sha256: assetHash},
			}}},
		},
		Spec:  spec,
		Scope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		Type:  ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL,
		Config: &ateletpb.RestoreRequest_LocalConfig{
			LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotName: snapshotName},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "while creating pause OCI bundle") {
		t.Fatalf("Restore error = %v, want the post-registration OCI preparation failure", err)
	}
	if _, err := os.ReadFile(filepath.Join(ateletpath.SystemInfoVolumeRoot(actorUID, "trust"), "ca.pem")); err != nil {
		t.Fatalf("Register did not write its projection before OCI preparation failed: %v", err)
	}
	if got := refresher.actors[actorUID]; got != nil {
		t.Fatalf("failed Restore left its owned registration live: %p", got)
	}
}

// TestTerminateWithoutTargetWorkerUID verifies that Terminate with an empty
// TargetWorkerPodUid (used when cleaning up the assigned node for a paused or
// crashed actor that no longer has a worker pod) skips dialing worker while
// still pruning local checkpoints and removing actor directories on the node.
func TestTerminateWithoutTargetWorkerUID(t *testing.T) {
	useTempNodeDirs(t)
	ctx := t.Context()

	const (
		atespace     = "ate-demo"
		actorName    = "counter"
		actorUID     = "actor-uid-1"
		snapshotName = "pause-snap-1"
	)

	if err := resetActorDirs(actorUID); err != nil {
		t.Fatalf("resetActorDirs: %v", err)
	}
	snapshotDir := ateletpath.LocalSnapshotDir(actorUID, snapshotName)
	if err := os.MkdirAll(snapshotDir, 0o700); err != nil {
		t.Fatalf("creating local snapshot dir: %v", err)
	}
	snapshotFile := filepath.Join(snapshotDir, "checkpoint.img")
	if err := os.WriteFile(snapshotFile, []byte("guest-memory"), 0o600); err != nil {
		t.Fatalf("writing local snapshot file: %v", err)
	}

	worker := &fakeWorker{}
	serveFakeWorker(t, worker)

	s := &Atelet{
		workerDialer:      newWorkerDialer(1),
		systemInfoVolumes: newSystemInfoVolumeRefresher(nil, nil),
	}
	spec := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{{Name: "app", Image: "example.com/app:v1"}},
	}

	if _, err := s.Terminate(ctx, &ateletpb.TerminateRequest{
		Atespace:              atespace,
		ActorName:             actorName,
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		TargetWorkerPodUid:    "",
		Spec:                  spec,
	}); err != nil {
		t.Fatalf("Terminate: %v", err)
	}

	if got := worker.actorDirs["TerminateWorkload"]; got != nil {
		t.Errorf("TerminateWorkload was called on worker (%v), want skipped when TargetWorkerPodUid is empty", got)
	}
	if _, err := os.Stat(ateletpath.LocalCheckpointsDir(actorUID)); !os.IsNotExist(err) {
		t.Errorf("local checkpoint dir survived terminate: %v", err)
	}
	if _, err := os.Stat(ateletpath.ActorPath(actorUID)); !os.IsNotExist(err) {
		t.Errorf("actor dir survived terminate: %v", err)
	}
}
