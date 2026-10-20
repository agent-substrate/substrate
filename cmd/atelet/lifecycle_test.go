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
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/atelet/internal/actordirs"
	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/cmd/atelet/internal/filecache"
	"go.ate.dev/substrate/cmd/atelet/internal/snapshottracker"
	"go.ate.dev/substrate/cmd/atelet/internal/trustbundle"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/ateinterceptors"
	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/volume"
	"go.ate.dev/substrate/pkg/api/v1alpha1"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
)

// useTempNodeDirs roots atelet's on-node state in temp directories so a test
// can drive the real filesystem layout. Not parallel-safe: the paths are
// process-global.
func useTempNodeDirs(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	origActors, origSnapshots := nodepath.ActorsDir, nodepath.SnapshotsDir
	nodepath.ActorsDir = filepath.Join(root, "actors")
	nodepath.SnapshotsDir = filepath.Join(root, "snapshots")
	t.Cleanup(func() {
		nodepath.ActorsDir, nodepath.SnapshotsDir = origActors, origSnapshots
	})
}

// fakeWorker is a fake worker in a worker pod. It writes the files a
// real checkpoint would leave in the checkpoint dir, and reads back what a
// restore was handed. Like a real worker it takes every actor directory from
// the request, never derived from the actor UID.
type fakeWorker struct {
	ateworkerpb.UnimplementedWorkerServer
	// mu guards the record maps: concurrent RPCs record at the same time.
	mu sync.Mutex
	// snapshotFiles are written at checkpoint and reported back to atelet as
	// the exact set the snapshot consists of.
	snapshotFiles map[string]string
	// restored holds the file contents staged into the restore dir by the
	// most recent RestoreWorkload.
	restored map[string]string
	// restoreErr, if set, fails every RestoreWorkload after it recorded the
	// request, as a worker that could not start the sandbox would.
	restoreErr error
	// actorDirs records the ActorDirs each RPC arrived with, by RPC name.
	actorDirs map[string]*ateworkerpb.ActorDirs
	// preserveRestoreDir records the PreserveRestoreDir flag from the most
	// recent RestoreWorkload request.
	preserveRestoreDir bool
	// workerEpoch is the epoch RunWorkload and RestoreWorkload report.
	workerEpoch int64
	// fidelities records the fidelity each checkpoint or restore arrived
	// with, by RPC name.
	fidelities map[string]ateworkerpb.SnapshotFidelity
	// onCheckpoint, if non-nil, runs inside CheckpointWorkload after snapshot
	// files are written.
	onCheckpoint func()
	// onTerminate, if non-nil, runs inside TerminateWorkload, which fails with
	// the error it returns.
	onTerminate func() error
	// onRestore, if non-nil, runs inside RestoreWorkload after it recorded
	// the request, before it fails with restoreErr or succeeds.
	onRestore func()
	// terminations counts TerminateWorkload calls.
	terminations atomic.Int32
}

func (f *fakeWorker) recordFidelity(rpc string, fidelity ateworkerpb.SnapshotFidelity) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fidelities == nil {
		f.fidelities = map[string]ateworkerpb.SnapshotFidelity{}
	}
	f.fidelities[rpc] = fidelity
}

func (f *fakeWorker) recordActorDirs(rpc string, actorDirs *ateworkerpb.ActorDirs) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.actorDirs == nil {
		f.actorDirs = map[string]*ateworkerpb.ActorDirs{}
	}
	f.actorDirs[rpc] = actorDirs
}

func (f *fakeWorker) RunWorkload(_ context.Context, req *ateworkerpb.RunWorkloadRequest) (*ateworkerpb.RunWorkloadResponse, error) {
	f.recordActorDirs("RunWorkload", req.GetActorDirs())
	return &ateworkerpb.RunWorkloadResponse{WorkerEpoch: f.workerEpoch}, nil
}

func (f *fakeWorker) CheckpointWorkload(_ context.Context, req *ateworkerpb.CheckpointWorkloadRequest) (*ateworkerpb.CheckpointWorkloadResponse, error) {
	f.recordActorDirs("CheckpointWorkload", req.GetActorDirs())
	f.recordFidelity("CheckpointWorkload", req.GetFidelity())
	dir := req.GetActorDirs().GetCheckpointDir()
	names := make([]string, 0, len(f.snapshotFiles))
	for name, body := range f.snapshotFiles {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	if f.onCheckpoint != nil {
		f.onCheckpoint()
	}
	return &ateworkerpb.CheckpointWorkloadResponse{SnapshotFiles: names}, nil
}

func (f *fakeWorker) RestoreWorkload(_ context.Context, req *ateworkerpb.RestoreWorkloadRequest) (*ateworkerpb.RestoreWorkloadResponse, error) {
	f.recordActorDirs("RestoreWorkload", req.GetActorDirs())
	f.recordFidelity("RestoreWorkload", req.GetFidelity())
	f.preserveRestoreDir = req.GetPreserveRestoreDir()
	if f.onRestore != nil {
		f.onRestore()
	}
	if f.restoreErr != nil {
		return nil, f.restoreErr
	}
	dir := req.GetActorDirs().GetRestoreDir()
	f.restored = map[string]string{}
	for name := range f.snapshotFiles {
		body, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		f.restored[name] = string(body)
	}
	return &ateworkerpb.RestoreWorkloadResponse{WorkerEpoch: f.workerEpoch}, nil
}

func (f *fakeWorker) TerminateWorkload(_ context.Context, req *ateworkerpb.TerminateWorkloadRequest) (*ateworkerpb.TerminateWorkloadResponse, error) {
	f.terminations.Add(1)
	f.recordActorDirs("TerminateWorkload", req.GetActorDirs())
	if f.onTerminate != nil {
		if err := f.onTerminate(); err != nil {
			return nil, err
		}
	}
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

// lifecycleSnapshot is the Snapshot the control plane names for an actor's
// checkpoint at generation.
func lifecycleSnapshot(actorUID, name string, generation int32, fidelity ateletpb.SnapshotFidelity) *ateletpb.Snapshot {
	return &ateletpb.Snapshot{
		SnapshotUuid: name,
		Fidelity:     fidelity,
		Object:       &ateletpb.ObjectStorage{SnapshotUri: "gs://test-bucket/atespaces/ate-demo/actors/" + actorUID + "/snapshots/" + name},
		Generation:   generation,
	}
}

// TestLocalSnapshotGC walks an actor through
// run -> deferred checkpoint -> restore -> deferred checkpoint -> terminate
// over atelet's RPC surface and ensures that superseded local snapshots are
// pruned by the next checkpoint and all remaining state is garbage collected
// after the actor is terminated. It runs at every fidelity atelet forwards,
// pinning that the checkpoint and the restore hand worker the fidelity the
// control plane asked for, unchanged.
func TestLocalSnapshotGC(t *testing.T) {
	for _, fidelity := range []ateletpb.SnapshotFidelity{
		ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
		ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
		ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
	} {
		t.Run(fidelity.String(), func(t *testing.T) { runLocalSnapshotGC(t, fidelity, useTempNodeDirs) })
	}
}

// runLocalSnapshotGC runs the TestLocalSnapshotGC flow on the node dirs useDirs
// sets up.
func runLocalSnapshotGC(t *testing.T, fidelity ateletpb.SnapshotFidelity, useDirs func(*testing.T)) {
	useDirs(t)
	ctx := t.Context()

	const (
		atespace      = "ate-demo"
		actorName     = "counter"
		actorUID      = "01234567-89ab-cdef-0123-456789abcdef"
		workerPodUID  = "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f"
		snapshotName  = "pause-snap-1"
		snapshotName2 = "pause-snap-2"
	)

	const workerEpoch = 7
	worker := &fakeWorker{snapshotFiles: map[string]string{"checkpoint.img": "guest-memory"}, workerEpoch: workerEpoch}
	serveFakeWorker(t, worker)

	host := imageVolumeTestRegistry(t)
	image := host + "/actor:v1"
	pinnedImage := pushTestImage(t, image, singleFileLayer(t, "bin/app", "app"))

	release := newGVisorTestRelease(t)
	s := &Atelet{
		workerDialer:      newWorkerDialer(1),
		imageCache:        newImageVolumeStore(t),
		sandboxAssetCache: newTestAssetCache(t),
		anonGCSClient:     fakeObjectStorage{data: release.tarball},
		systemInfoVolumes: newSystemInfoVolumeRefresher(nil, nil),
		snapshots:         snapshottracker.New(t.Context(), nil, clock.RealClock{}),
	}
	sandboxRec := testSandboxRecord(release)
	spec := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{{Name: "app", Image: pinnedImage, Command: []string{"/bin/app"}}},
	}

	advertiseRuntime(t, s, sandboxRec)
	runResp, err := s.Run(ctx, &ateletpb.RunRequest{
		Atespace:              atespace,
		ActorName:             actorName,
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          workerPodUID,
		SandboxRuntime:        runtimeFor(t, sandboxRec),
		Spec:                  spec,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// The control plane stamps the Actor with the epoch of the worker process
	// that started it.
	if got := runResp.GetWorkerEpoch(); got != workerEpoch {
		t.Errorf("Run worker_epoch = %d, want %d", got, workerEpoch)
	}

	// Async suspend: a deferred checkpoint, which leaves the snapshot on this node.
	resp, err := s.Checkpoint(ctx, &ateletpb.CheckpointRequest{
		Atespace:              atespace,
		ActorName:             actorName,
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          workerPodUID,
		Spec:                  spec,
		Snapshot:              lifecycleSnapshot(actorUID, snapshotName, 1, fidelity),
		UploadConfig:          deferredUploadConfig(),
	})
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	snapshotFile := filepath.Join(ateletpath.LocalSnapshotDir(actorUID, snapshotName), "checkpoint.img")
	if _, err := os.Stat(snapshotFile); err != nil {
		t.Fatalf("pause did not write the local snapshot: %v", err)
	}
	// An actor with a local snapshot stays resumable on this node, so it keeps
	// its staged assets, and with them its binaries in the cache.
	if _, err := os.Stat(stagedRunsc(actorUID)); err != nil {
		t.Errorf("paused actor lost its staged runsc: %v", err)
	}
	st := resp.GetState()
	if got := st.GetLatestLocal(); got.GetSnapshot().GetSnapshotUuid() != snapshotName || got.GetLocalCommitTime() == nil {
		t.Errorf("latest_local = %v, want the pause snapshot with its commit time", got)
	}
	if st.GetLatestDurable() != nil {
		t.Errorf("state = %v, want nothing durable", st)
	}

	// Resume: atelet must restore from that local snapshot, not from
	// Snapshot.Object.
	restoreResp, err := s.Restore(ctx, &ateletpb.RestoreRequest{
		Atespace:              atespace,
		ActorName:             actorName,
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          workerPodUID,
		SandboxRuntime:        runtimeFor(t, sandboxRec),
		Spec:                  spec,
		Snapshot:              lifecycleSnapshot(actorUID, snapshotName, 1, fidelity),
	})
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got := restoreResp.GetWorkerEpoch(); got != workerEpoch {
		t.Errorf("Restore worker_epoch = %d, want %d", got, workerEpoch)
	}
	if got := worker.restored["checkpoint.img"]; got != "guest-memory" {
		t.Fatalf("restore staged %q for worker, want the pause snapshot's %q", got, "guest-memory")
	}
	if _, err := os.Stat(stagedRunsc(actorUID)); err != nil {
		t.Errorf("resumed actor has no staged runsc: %v", err)
	}
	for _, rpc := range []string{"CheckpointWorkload", "RestoreWorkload"} {
		if got := worker.fidelities[rpc]; got != toWorkerFidelity(fidelity) {
			t.Errorf("%s carried fidelity %v, want %v", rpc, got, toWorkerFidelity(fidelity))
		}
	}
	if !worker.preserveRestoreDir {
		t.Errorf("RestoreWorkload preserve_restore_dir = false, want true for pure-local restore")
	}
	if entries, err := os.ReadDir(ateletpath.RestoreStateDir(actorUID)); err != nil || len(entries) != 0 {
		t.Fatalf("expected RestoreStateDir to remain empty on local pause restore, got entries=%v err=%v", entries, err)
	}

	// Second deferred checkpoint: must replace, not accumulate alongside, the first snapshot.
	if _, err := s.Checkpoint(ctx, &ateletpb.CheckpointRequest{
		Atespace:              atespace,
		ActorName:             actorName,
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          workerPodUID,
		Spec:                  spec,
		Snapshot:              lifecycleSnapshot(actorUID, snapshotName2, 2, fidelity),
		UploadConfig:          deferredUploadConfig(),
	}); err != nil {
		t.Fatalf("Checkpoint 2: %v", err)
	}
	snapshotFile2 := filepath.Join(ateletpath.LocalSnapshotDir(actorUID, snapshotName2), "checkpoint.img")
	if _, err := os.Stat(ateletpath.LocalSnapshotDir(actorUID, snapshotName)); !os.IsNotExist(err) {
		t.Fatalf("first local snapshot survived second checkpoint: %v", err)
	}
	if _, err := os.Stat(snapshotFile2); err != nil {
		t.Fatalf("second pause did not write the local snapshot: %v", err)
	}

	// Terminate: the actor is gone, and so should its remaining snapshot be.
	if _, err := s.Terminate(ctx, &ateletpb.TerminateRequest{
		Atespace:              atespace,
		ActorName:             actorName,
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          workerPodUID,
		Spec:                  spec,
	}); err != nil {
		t.Fatalf("Terminate: %v", err)
	}

	// Every RPC hands worker the same directory set, except that a local
	// restore points restore_dir at the local snapshot; the fake already
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
	if got := s.snapshots.State(actorUID); !proto.Equal(got, &ateletpb.ActorSnapshotState{}) {
		t.Errorf("snapshot tracker state after terminate = %v, want empty", got)
	}

	// Terminate is the only chance to reclaim the actor's directory: nothing
	// else on the node deletes it.
	assertDirGone(t, "actor dir", ateletpath.ActorPath(actorUID))
	assertDirGone(t, "actor snapshot dir", ateletpath.ActorSnapshotPath(actorUID))
}

// TestRestoreUsesRequestSandboxRuntime checks that Restore runs the actor with
// the sandbox runtime on the request, not the one recorded in the snapshot
// manifest.
func TestRestoreUsesRequestSandboxRuntime(t *testing.T) {
	useTempNodeDirs(t)
	ctx := t.Context()

	const (
		atespace     = "ate-demo"
		actorName    = "counter"
		actorUID     = "01234567-89ab-cdef-0123-456789abcdef"
		workerPodUID = "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f"
		snapshotName = "pause-snap-1"
	)

	worker := &fakeWorker{snapshotFiles: map[string]string{"checkpoint.img": "guest-memory"}}
	serveFakeWorker(t, worker)

	host := imageVolumeTestRegistry(t)
	image := host + "/actor:v1"
	pinnedImage := pushTestImage(t, image, singleFileLayer(t, "bin/app", "app"))

	// The two releases differ in content, so they have distinct bundle hashes.
	release := func(runsc string) gvisorTestRelease {
		tarball := gvisorReleaseTarballWith(t, runsc)
		return gvisorTestRelease{tarball: tarball, entry: assetEntry{URL: "gs://test-bucket/gvisor.tar", SHA256: fmt.Sprintf("%x", sha256.Sum256(tarball))}}
	}
	checkpointRelease, restoreRelease := release("runsc v1"), release("runsc v2")
	s := &Atelet{
		workerDialer:      newWorkerDialer(1),
		imageCache:        newImageVolumeStore(t),
		sandboxAssetCache: newTestAssetCache(t),
		anonGCSClient:     fakeObjectStorage{data: checkpointRelease.tarball},
		systemInfoVolumes: newSystemInfoVolumeRefresher(nil, nil),
		snapshots:         snapshottracker.New(t.Context(), nil, clock.RealClock{}),
	}
	checkpointRec, restoreRec := testSandboxRecord(checkpointRelease), testSandboxRecord(restoreRelease)
	spec := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{{Name: "app", Image: pinnedImage, Command: []string{"/bin/app"}}},
	}

	advertiseRuntime(t, s, checkpointRec)
	if _, err := s.Run(ctx, &ateletpb.RunRequest{
		Atespace:              atespace,
		ActorName:             actorName,
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          workerPodUID,
		SandboxRuntime:        runtimeFor(t, checkpointRec),
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
		WorkerPodUid:          workerPodUID,
		Spec:                  spec,
		Snapshot:              lifecycleSnapshot(actorUID, snapshotName, 1, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY),
		UploadConfig:          &ateletpb.UploadConfig{Mode: ateletpb.UploadMode_UPLOAD_MODE_DEFERRED},
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
	if got, want := manifestRec.Assets[gvisorAssetName].SHA256, checkpointRelease.entry.SHA256; got != want {
		t.Fatalf("manifest gvisor sha256 = %q, want %q", got, want)
	}

	s.anonGCSClient = fakeObjectStorage{data: restoreRelease.tarball}
	advertiseRuntime(t, s, restoreRec)
	restoreReq := &ateletpb.RestoreRequest{
		Atespace:              atespace,
		ActorName:             actorName,
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          workerPodUID,
		SandboxRuntime:        runtimeFor(t, restoreRec),
		Spec:                  spec,
		Snapshot:              lifecycleSnapshot(actorUID, snapshotName, 1, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY),
	}

	// Restore writes the sandbox record before calling RestoreWorkload, so a
	// Restore that fails or times out after reaching the worker still leaves
	// the record for a subsequent Terminate to tear the sandbox down.
	if err := os.Remove(ateletpath.ActorSandboxAssetsFile(actorUID)); err != nil {
		t.Fatalf("removing sandbox record: %v", err)
	}
	worker.restoreErr = status.Error(codes.DeadlineExceeded, "restore timed out")
	if _, err := s.Restore(ctx, restoreReq); err == nil {
		t.Fatal("Restore succeeded when RestoreWorkload failed")
	}
	got, err := readSandboxRecord(actorUID)
	if err != nil {
		t.Fatalf("reading on-node sandbox record after failed RestoreWorkload: %v", err)
	}
	if sha, want := got.Assets[gvisorAssetName].SHA256, restoreRelease.entry.SHA256; sha != want {
		t.Errorf("sandbox record gvisor sha256 after failed RestoreWorkload = %q, want %q", sha, want)
	}

	worker.restoreErr = nil
	if _, err := s.Restore(ctx, restoreReq); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	got, err = readSandboxRecord(actorUID)
	if err != nil {
		t.Fatalf("reading on-node sandbox record: %v", err)
	}
	if sha, want := got.Assets[gvisorAssetName].SHA256, restoreRelease.entry.SHA256; sha != want {
		t.Errorf("restored actor gvisor sha256 = %q, want the request's %q", sha, want)
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
		// publishedVolume leaves an external volume mount point with
		// content in it, as a failed earlier attempt that published the
		// volume would.
		publishedVolume bool
	}{
		{name: "Run asset failure preserves projection", failure: "asset", wantErr: "asset unavailable", wantPresent: true},
		{name: "Run partial reset removes registration", failure: "reset", wantErr: "while creating volumes dir"},
		{name: "Run record failure removes registration", failure: "record", wantErr: "while recording sandbox assets"},
		{name: "Restore partial reset removes registration", restore: true, failure: "reset", wantErr: "while creating volumes dir"},
		{name: "Restore manifest failure removes registration", restore: true, failure: "manifest", wantErr: "no uploaded copy"},
		{name: "Restore asset failure removes registration", restore: true, failure: "asset", wantErr: "asset unavailable"},
		{name: "Run retry keeps a published volume", failure: "record", publishedVolume: true, wantErr: "while recording sandbox assets"},
		{name: "Restore retry keeps a published volume", restore: true, failure: "manifest", publishedVolume: true, wantErr: "no uploaded copy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useTempNodeDirs(t)
			const actorUID, snapshotName = "0f6c2b0e-1b7e-4b3f-9a6a-1d2c3e4f5a61", "pause-snap-1"
			ctx := t.Context()
			store := newCTBStore(t)
			certA := string(testCertPEM(t))
			store.set(t, certA)
			refresher := newSystemInfoVolumeRefresher(trustbundle.NewSource(store.lister.Get, nil), nil)
			spec := &ateletpb.WorkloadSpec{Volumes: []*ateletpb.Volume{{
				Name: "trust", SystemInfo: trustVolumeSpec("ca.pem"),
			}}}
			registered, err := refresher.Register(actorUID, resources.ActorRef{Atespace: "team-a", Name: "actor-1"}, systemInfoVolumesFor(actorUID, spec))
			if err != nil {
				t.Fatal(err)
			}
			projectedPath := filepath.Join(ateletpath.SystemInfoVolumeRoot(actorUID, "trust"), "ca.pem")
			if got, err := os.ReadFile(projectedPath); err != nil || string(got) != certA {
				t.Fatalf("initial projection = %q, %v; want cert A", got, err)
			}

			release := newGVisorTestRelease(t)
			if tc.restore && tc.failure != "manifest" {
				writeLocalSnapshot(t, ateletpath.LocalSnapshotDir(actorUID, snapshotName), sandboxAssetsRecord{
					SandboxClass:  "gvisor",
					Assets:        map[string]assetEntry{gvisorAssetName: release.entry},
					SnapshotFiles: []string{"checkpoint.img"},
				}, map[string]string{"checkpoint.img": "guest-memory"})
			}
			if tc.failure == "reset" {
				// A file where the volumes dir goes fails the reset after it
				// has removed the system-info roots.
				if err := os.WriteFile(ateletpath.ExternalVolumeMountsDir(actorUID), nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.failure == "record" {
				if err := os.MkdirAll(ateletpath.ActorSandboxAssetsFile(actorUID), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			// A non-empty dir fails os.Remove like a live mount (EBUSY)
			// would, so the activation only gets past the reset to fail
			// later with wantErr if the reset leaves volumes/ alone.
			volumeFile := filepath.Join(ateletpath.ExternalVolumeHostPath(actorUID, "data"), "data.txt")
			if tc.publishedVolume {
				if err := os.MkdirAll(filepath.Dir(volumeFile), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(volumeFile, []byte("volume data"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			// The live activation's staged binary: a Run that fails before
			// it swaps its own set in must leave it, and a Run that fails
			// after must still leave nothing half-staged behind.
			liveAsset := stagedRunsc(actorUID)
			if err := os.MkdirAll(filepath.Dir(liveAsset), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(liveAsset, []byte("live runsc"), 0o555); err != nil {
				t.Fatal(err)
			}
			storage := fakeObjectStorage{data: release.tarball}
			if tc.failure == "asset" {
				storage.err = fmt.Errorf("asset unavailable")
			}
			// The object store is empty: a restore with no local copy finds
			// no uploaded copy either.
			s := newPluginHerder(t, &recordingObjectStorage{})
			s.anonGCSClient = storage
			s.systemInfoVolumes = refresher
			rec := testSandboxRecord(release)
			advertiseRuntime(t, s, rec)
			if tc.restore {
				_, err = s.Restore(ctx, &ateletpb.RestoreRequest{
					Atespace: "team-a", ActorName: "actor-1", ActorUid: actorUID, WorkerPodUid: "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f",
					SandboxRuntime: runtimeFor(t, rec), Spec: spec,
					Snapshot: lifecycleSnapshot(actorUID, snapshotName, 1, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY),
				})
			} else {
				_, err = s.Run(ctx, &ateletpb.RunRequest{
					Atespace: "team-a", ActorName: "actor-1", ActorUid: actorUID, WorkerPodUid: "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f",
					SandboxRuntime: runtimeFor(t, rec), Spec: spec,
				})
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("activation error = %v, want %q", err, tc.wantErr)
			}
			if _, err := os.Lstat(ateletpath.SandboxAssetsStagingDir(actorUID)); !os.IsNotExist(err) {
				t.Errorf("failed activation left the staging dir behind (lstat err = %v)", err)
			}
			if tc.publishedVolume {
				if got, err := os.ReadFile(volumeFile); err != nil || string(got) != "volume data" {
					t.Errorf("activation touched the published volume: %q, %v", got, err)
				}
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
				if got, err := os.ReadFile(liveAsset); err != nil || string(got) != "live runsc" {
					t.Errorf("pre-reset failure lost the live activation's staged asset: %q, %v", got, err)
				}
			} else {
				// Past the reset, the record still describes the previous
				// activation (or nothing), so no links of this one may
				// remain for a later Terminate to take for that record's:
				// at most the previous activation's own, untouched.
				entries, err := os.ReadDir(ateletpath.SandboxAssetsDir(actorUID))
				if err != nil && !os.IsNotExist(err) {
					t.Fatal(err)
				}
				for _, e := range entries {
					if got, err := os.ReadFile(liveAsset); e.Name() != gvisorAssetName || err != nil || string(got) != "live runsc" {
						t.Errorf("failed activation left %q behind in the assets dir", e.Name())
					}
				}
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
	release := newGVisorTestRelease(t)
	const actorUID = "0f6c2b0e-1b7e-4b3f-9a6a-1d2c3e4f5a62"
	s := &Atelet{
		sandboxAssetCache: newTestAssetCache(t),
		anonGCSClient:     fakeObjectStorage{data: release.tarball},
		imageCache:        newImageVolumeStore(t),
		systemInfoVolumes: refresher,
		snapshots:         snapshottracker.New(t.Context(), nil, clock.RealClock{}),
	}
	spec := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{{Name: "app", Image: missingImage(t)}},
		Volumes: []*ateletpb.Volume{{
			Name:       "trust",
			SystemInfo: trustVolumeSpec("ca.pem"),
		}},
	}
	rec := testSandboxRecord(release)
	advertiseRuntime(t, s, rec)
	_, err := s.Run(t.Context(), &ateletpb.RunRequest{
		Atespace:       "team-a",
		ActorName:      "actor-run",
		ActorUid:       actorUID,
		WorkerPodUid:   "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f",
		SandboxRuntime: runtimeFor(t, rec),
		Spec:           spec,
	})
	if err == nil || !strings.Contains(err.Error(), `while creating "app" OCI bundle`) {
		t.Fatalf("Run error = %v, want the post-registration OCI preparation failure", err)
	}
	if _, err := os.ReadFile(filepath.Join(ateletpath.SystemInfoVolumeRoot(actorUID, "trust"), "ca.pem")); err != nil {
		t.Fatalf("Register did not write its projection before OCI preparation failed: %v", err)
	}
	if got := refresher.actors[actorUID]; got != nil {
		t.Fatalf("failed Run left its registration live: %p", got)
	}
}

// missingImage returns a digest-pinned reference that passes request
// validation but names a manifest the local test registry does not serve, so
// the pull fails only once OCI bundle preparation starts.
func missingImage(t *testing.T) string {
	t.Helper()
	return imageVolumeTestRegistry(t) + "/app@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
}

func TestRestoreFailureAfterRegistrationRemovesOwnRegistration(t *testing.T) {
	useTempNodeDirs(t)
	const (
		actorUID     = "0f6c2b0e-1b7e-4b3f-9a6a-1d2c3e4f5a63"
		snapshotName = "pause-snap-after"
	)
	image := missingImage(t)
	store := newCTBStore(t)
	store.set(t, string(testCertPEM(t)))
	refresher := newSystemInfoVolumeRefresher(trustbundle.NewSource(store.lister.Get, nil), nil)
	release := newGVisorTestRelease(t)
	writeLocalSnapshot(t, ateletpath.LocalSnapshotDir(actorUID, snapshotName), sandboxAssetsRecord{
		SandboxClass:  "gvisor",
		Assets:        map[string]assetEntry{gvisorAssetName: release.entry},
		SnapshotFiles: []string{"checkpoint.img"},
	}, map[string]string{"checkpoint.img": "guest-memory"})

	spec := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{{Name: "app", Image: image}},
		Volumes: []*ateletpb.Volume{{
			Name:       "trust",
			SystemInfo: trustVolumeSpec("ca.pem"),
		}},
	}
	s := &Atelet{
		sandboxAssetCache: newTestAssetCache(t),
		anonGCSClient:     fakeObjectStorage{data: release.tarball},
		imageCache:        newImageVolumeStore(t),
		systemInfoVolumes: refresher,
		snapshots:         snapshottracker.New(t.Context(), nil, clock.RealClock{}),
	}
	rec := testSandboxRecord(release)
	advertiseRuntime(t, s, rec)
	_, err := s.Restore(t.Context(), &ateletpb.RestoreRequest{
		Atespace:       "team-a",
		ActorName:      "actor-restore-after",
		ActorUid:       actorUID,
		WorkerPodUid:   "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f",
		SandboxRuntime: runtimeFor(t, rec),
		Spec:           spec,
		Snapshot:       lifecycleSnapshot(actorUID, snapshotName, 1, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY),
	})
	if err == nil || !strings.Contains(err.Error(), `while creating "app" OCI bundle`) {
		t.Fatalf("Restore error = %v, want the post-registration OCI preparation failure", err)
	}
	if _, err := os.ReadFile(filepath.Join(ateletpath.SystemInfoVolumeRoot(actorUID, "trust"), "ca.pem")); err != nil {
		t.Fatalf("Register did not write its projection before OCI preparation failed: %v", err)
	}
	if got := refresher.actors[actorUID]; got != nil {
		t.Fatalf("failed Restore left its owned registration live: %p", got)
	}
}

// TestRestorePrefersLocalAndFallsBackToObjectStorage checks that Restore reads
// a usable node-local checkpoint in place even when snapshot.object is also
// populated, and falls back to staging from snapshot.object when the local copy
// is absent or unusable (corrupt manifest, missing listed file, or non-regular
// file).
func TestRestorePrefersLocalAndFallsBackToObjectStorage(t *testing.T) {
	const (
		atespace     = "ate-demo"
		actorName    = "counter"
		actorUID     = "01234567-89ab-cdef-0123-456789abcdef"
		workerPodUID = "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f"
		snapshotName = "pause-snap-1"
	)

	worker := &fakeWorker{snapshotFiles: map[string]string{"checkpoint.img": ""}}
	serveFakeWorker(t, worker)

	host := imageVolumeTestRegistry(t)
	pinnedImage := pushTestImage(t, host+"/actor:v1", singleFileLayer(t, "bin/app", "app"))

	release := newGVisorTestRelease(t)
	sandboxRec := testSandboxRecord(release)
	spec := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{{Name: "app", Image: pinnedImage, Command: []string{"/bin/app"}}},
	}
	manifestRec := sandboxAssetsRecord{
		SandboxClass:  "gvisor",
		Assets:        map[string]assetEntry{gvisorAssetName: release.entry},
		SnapshotFiles: []string{"checkpoint.img"},
	}

	// Upload a committed copy to object storage once; every subtest restores
	// with snapshot.object pointing at it.
	objStore := &recordingObjectStorage{}
	s := newPluginHerder(t, objStore)
	s.workerDialer = newWorkerDialer(1)
	s.imageCache = newImageVolumeStore(t)
	s.anonGCSClient = fakeObjectStorage{data: release.tarball}
	s.systemInfoVolumes = newSystemInfoVolumeRefresher(nil, nil)

	uri, err := resources.ParseSnapshotURI(pausedSnapshotURI)
	if err != nil {
		t.Fatal(err)
	}
	remoteSrcDir := t.TempDir()
	writeLocalSnapshot(t, remoteSrcDir, manifestRec, map[string]string{"checkpoint.img": "object-storage-memory"})
	if err := s.uploadSnapshot(t.Context(), testActor, uri, remoteSrcDir, &manifestRec); err != nil {
		t.Fatalf("uploadSnapshot: %v", err)
	}

	for _, tc := range []struct {
		name      string
		prepLocal func(t *testing.T, localDir string)
		wantLocal bool
	}{{
		name: "usable local copy is preferred over object storage",
		prepLocal: func(t *testing.T, localDir string) {
			writeLocalSnapshot(t, localDir, manifestRec, map[string]string{"checkpoint.img": "local-memory"})
		},
		wantLocal: true,
	}, {
		name:      "absent local copy falls back to object storage",
		prepLocal: func(*testing.T, string) {},
		wantLocal: false,
	}, {
		name: "local copy with missing listed file falls back to object storage",
		prepLocal: func(t *testing.T, localDir string) {
			writeLocalSnapshot(t, localDir, manifestRec, nil)
		},
		wantLocal: false,
	}, {
		name: "local copy with corrupt manifest falls back to object storage",
		prepLocal: func(t *testing.T, localDir string) {
			if err := os.MkdirAll(localDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(localDir, sandboxManifestName), []byte("{not-json"), 0o600); err != nil {
				t.Fatal(err)
			}
		},
		wantLocal: false,
	}, {
		name: "local copy with symlinked snapshot file falls back to object storage",
		prepLocal: func(t *testing.T, localDir string) {
			writeLocalSnapshot(t, localDir, manifestRec, nil)
			target := filepath.Join(t.TempDir(), "outside.img")
			if err := os.WriteFile(target, []byte("outside"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, filepath.Join(localDir, "checkpoint.img")); err != nil {
				t.Fatal(err)
			}
		},
		wantLocal: false,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			useTempNodeDirs(t)
			localDir := ateletpath.LocalSnapshotDir(actorUID, snapshotName)
			tc.prepLocal(t, localDir)
			advertiseRuntime(t, s, sandboxRec)

			if _, err := s.Restore(t.Context(), &ateletpb.RestoreRequest{
				Atespace:              atespace,
				ActorName:             actorName,
				ActorUid:              actorUID,
				ActorTemplateAtespace: "default",
				ActorTemplateName:     "counter",
				WorkerPodUid:          workerPodUID,
				SandboxRuntime:        runtimeFor(t, sandboxRec),
				Spec:                  spec,
				Snapshot: &ateletpb.Snapshot{
					SnapshotUuid: snapshotName,
					Generation:   1,
					Fidelity:     ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
					Object:       &ateletpb.ObjectStorage{SnapshotUri: pausedSnapshotURI},
				},
			}); err != nil {
				t.Fatalf("Restore: %v", err)
			}

			wantContent := "object-storage-memory"
			wantRestoreDir := ateletpath.RestoreStateDir(actorUID)
			if tc.wantLocal {
				wantContent = "local-memory"
				wantRestoreDir = localDir
			}
			if got := worker.restored["checkpoint.img"]; got != wantContent {
				t.Errorf("restored checkpoint.img = %q, want %q", got, wantContent)
			}
			if worker.preserveRestoreDir != tc.wantLocal {
				t.Errorf("RestoreWorkload preserve_restore_dir = %v, want %v", worker.preserveRestoreDir, tc.wantLocal)
			}
			if got := worker.actorDirs["RestoreWorkload"].GetRestoreDir(); got != wantRestoreDir {
				t.Errorf("RestoreWorkload restore_dir = %q, want %q", got, wantRestoreDir)
			}
			if tc.wantLocal {
				if entries, err := os.ReadDir(ateletpath.RestoreStateDir(actorUID)); err != nil || len(entries) != 0 {
					t.Errorf("RestoreStateDir entries = %v, err = %v; want empty on local restore", entries, err)
				}
			}
		})
	}
}

// TestRestoreFailureRemovesStagedSnapshotFiles pins what a shared restore
// that fails after its snapshot files were staged leaves behind: nothing in
// the restore-state directory, so the links into the snapshot cache do not
// hold its entries until a later lifecycle call resets the directory.
func TestRestoreFailureRemovesStagedSnapshotFiles(t *testing.T) {
	useTempNodeDirs(t)
	const (
		actorUID     = "0f6c2b0e-1b7e-4b3f-9a6a-1d2c3e4f5a66"
		snapshotName = "pause-snap-1"
	)
	worker := &fakeWorker{snapshotFiles: map[string]string{"checkpoint.img": ""}, restoreErr: status.Error(codes.Internal, "sandbox would not start")}
	serveFakeWorker(t, worker)
	host := imageVolumeTestRegistry(t)
	pinnedImage := pushTestImage(t, host+"/actor:v1", singleFileLayer(t, "bin/app", "app"))
	release := newGVisorTestRelease(t)
	sandboxRec := testSandboxRecord(release)
	manifestRec := sandboxAssetsRecord{
		SandboxClass:  "gvisor",
		Assets:        map[string]assetEntry{gvisorAssetName: release.entry},
		SnapshotFiles: []string{"checkpoint.img"},
	}
	s := newPluginHerder(t, &recordingObjectStorage{})
	s.workerDialer = newWorkerDialer(1)
	s.imageCache = newImageVolumeStore(t)
	s.anonGCSClient = fakeObjectStorage{data: release.tarball}
	s.systemInfoVolumes = newSystemInfoVolumeRefresher(nil, nil)
	// No min age, so the only thing standing between a staged entry and a
	// pass is the restore's link.
	cache, err := filecache.New(filepath.Join(t.TempDir(), "cache"), filecache.WithMinAge(0))
	if err != nil {
		t.Fatal(err)
	}
	s.snapshotCache = cache
	uri, err := resources.ParseSnapshotURI(pausedSnapshotURI)
	if err != nil {
		t.Fatal(err)
	}
	srcDir := t.TempDir()
	writeLocalSnapshot(t, srcDir, manifestRec, map[string]string{"checkpoint.img": "shared-memory"})
	if err := s.uploadSnapshot(t.Context(), testActor, uri, srcDir, &manifestRec); err != nil {
		t.Fatalf("uploadSnapshot: %v", err)
	}
	advertiseRuntime(t, s, sandboxRec)

	_, err = s.Restore(t.Context(), &ateletpb.RestoreRequest{
		Atespace:              "ate-demo",
		ActorName:             "counter",
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f",
		SandboxRuntime:        runtimeFor(t, sandboxRec),
		Spec:                  &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "app", Image: pinnedImage, Command: []string{"/bin/app"}}}},
		Snapshot: &ateletpb.Snapshot{
			SnapshotUuid: snapshotName,
			Generation:   1,
			Fidelity:     ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			Object:       &ateletpb.ObjectStorage{SnapshotUri: pausedSnapshotURI, Sharing: ateletpb.SnapshotSharing_SNAPSHOT_SHARING_SHARED},
		},
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("Restore = %v, want the worker's Internal", err)
	}
	if worker.actorDirs["RestoreWorkload"] == nil {
		t.Fatal("the worker was never asked: the failure must come after the snapshot files were staged")
	}
	if entries, err := os.ReadDir(ateletpath.RestoreStateDir(actorUID)); err == nil && len(entries) != 0 {
		t.Errorf("restore-state holds %v after a failed restore, want nothing", entries)
	}
	stats, err := cache.EvictUnused(t.Context(), 1<<40, true)
	if err != nil {
		t.Fatal(err)
	}
	// Two entries, the manifest and checkpoint.img, both unlinked.
	if stats.Retired != 2 || stats.SkippedLinked != 0 {
		t.Errorf("after the failed restore a pass would retire %d entries and skip %d linked, want 2 and 0: the staged link must be gone", stats.Retired, stats.SkippedLinked)
	}
}

// TestRestoreWithoutUsableLocalCopy checks that a Restore with no usable local
// copy, naming a snapshot.object that was never uploaded, fails as NotFound
// and wraps the local copy's problem.
func TestRestoreWithoutUsableLocalCopy(t *testing.T) {
	const (
		actorUID     = "0f6c2b0e-1b7e-4b3f-9a6a-1d2c3e4f5a65"
		snapshotName = "pause-snap-1"
	)
	release := newGVisorTestRelease(t)
	rec := testSandboxRecord(release)
	for _, tc := range []struct {
		name string
		// unusableLocal writes a manifest that lists a file that is not there.
		unusableLocal bool
		wantCause     error
	}{{
		name:      "no local copy",
		wantCause: os.ErrNotExist,
	}, {
		name:          "unusable local copy",
		unusableLocal: true,
		wantCause:     errUnusableLocalSnapshot,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			useTempNodeDirs(t)
			if tc.unusableLocal {
				writeLocalSnapshot(t, ateletpath.LocalSnapshotDir(actorUID, snapshotName), sandboxAssetsRecord{
					SandboxClass: "gvisor", SnapshotFiles: []string{"checkpoint.img"},
				}, nil)
			}
			// The object store is empty: nothing was ever uploaded.
			s := newPluginHerder(t, &recordingObjectStorage{})
			s.systemInfoVolumes = newSystemInfoVolumeRefresher(nil, nil)
			advertiseRuntime(t, s, rec)
			_, err := s.Restore(t.Context(), &ateletpb.RestoreRequest{
				Atespace: "team-a", ActorName: "actor-1", ActorUid: actorUID,
				WorkerPodUid:   "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f",
				SandboxRuntime: runtimeFor(t, rec),
				Spec:           &ateletpb.WorkloadSpec{},
				Snapshot:       lifecycleSnapshot(actorUID, snapshotName, 1, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY),
			})
			if code := apierror.Code(err); code != codes.NotFound {
				t.Fatalf("Restore error = %v (code %v), want NotFound", err, code)
			}
			if !strings.Contains(err.Error(), "no uploaded copy") {
				t.Errorf("Restore error = %v, want it to say %q", err, "no uploaded copy")
			}
			if !errors.Is(err, tc.wantCause) {
				t.Errorf("Restore error = %v, want it to wrap %v", err, tc.wantCause)
			}
		})
	}
}

// TestTerminateWithoutWorkerPodUID verifies that Terminate with an empty
// WorkerPodUid (used when cleaning up the assigned node for a SUSPENDED or
// crashed actor that no longer has a worker pod) skips dialing worker while
// still pruning local checkpoints and removing actor directories on the node.
func TestTerminateWithoutWorkerPodUID(t *testing.T) {
	useTempNodeDirs(t)
	ctx := t.Context()

	const (
		atespace     = "ate-demo"
		actorName    = "counter"
		actorUID     = "0f6c2b0e-1b7e-4b3f-9a6a-1d2c3e4f5a64"
		snapshotName = "pause-snap-1"
	)

	if err := actordirs.ResetActorDirs(actorUID); err != nil {
		t.Fatalf("ResetActorDirs: %v", err)
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
		sandboxAssetCache: newTestAssetCache(t),
		systemInfoVolumes: newSystemInfoVolumeRefresher(nil, nil),
		snapshots:         snapshottracker.New(t.Context(), nil, clock.RealClock{}),
	}
	spec := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{{Name: "app", Image: "example.com/app@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}},
	}

	if _, err := s.Terminate(ctx, &ateletpb.TerminateRequest{
		Atespace:              atespace,
		ActorName:             actorName,
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          "",
		Spec:                  spec,
	}); err != nil {
		t.Fatalf("Terminate: %v", err)
	}

	if got := worker.actorDirs["TerminateWorkload"]; got != nil {
		t.Errorf("TerminateWorkload was called on worker (%v), want skipped when WorkerPodUid is empty", got)
	}
	if _, err := os.Stat(ateletpath.LocalCheckpointsDir(actorUID)); !os.IsNotExist(err) {
		t.Errorf("local checkpoint dir survived terminate: %v", err)
	}
	if _, err := os.Stat(ateletpath.ActorSnapshotPath(actorUID)); !os.IsNotExist(err) {
		t.Errorf("actor snapshot dir survived terminate: %v", err)
	}
	if _, err := os.Stat(ateletpath.ActorPath(actorUID)); !os.IsNotExist(err) {
		t.Errorf("actor dir survived terminate: %v", err)
	}
}

// A deleted worker pod takes its worker with it and leaves the actor's state on
// the node. The worker delete reclaims it with a Terminate that still names
// that worker, so one that refuses to run without a live worker leaves the
// state until the actor is deleted or reverted. All three ways the sandbox can
// be absent are covered: the pod's socket gone with it, a socket that outlived
// its process (Unavailable from TerminateWorkload), and an actor with no
// sandbox record on disk.
func TestTerminateReclaimsStateWithoutALiveSandbox(t *testing.T) {
	const (
		atespace     = "ate-demo"
		actorName    = "counter"
		actorUID     = "01234567-89ab-cdef-0123-456789abcdef"
		workerPodUID = "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f"
	)

	tests := []struct {
		name string
		// setup arranges the absence under test.
		setup func(t *testing.T, s *Atelet)
	}{
		{
			name: "worker socket gone with its pod",
			setup: func(t *testing.T, _ *Atelet) {
				orig := workerSocketPath
				missing := filepath.Join(t.TempDir(), "worker.sock")
				workerSocketPath = func(string) string { return missing }
				t.Cleanup(func() { workerSocketPath = orig })
				// A sandbox record whose asset is not in the cache would fail
				// ensureSandboxAssets (anonGCSClient is nil) if Terminate read
				// the record before checking whether the worker socket exists.
				if err := writeSandboxRecord(actorUID, &sandboxAssetsRecord{
					SandboxClass: "gvisor",
					Assets:       map[string]assetEntry{gvisorAssetName: {URL: "gs://test-bucket/gvisor-release.tar.gz", SHA256: strings.Repeat("a", 64)}},
				}); err != nil {
					t.Fatalf("writing sandbox record: %v", err)
				}
			},
		},
		{
			// After an ungraceful exit the socket can outlive the pod, or the
			// worker can shut down while TerminateWorkload is in flight;
			// TerminateWorkload then fails with Unavailable.
			name: "worker socket outlived its process",
			setup: func(t *testing.T, s *Atelet) {
				release := newGVisorTestRelease(t)
				s.anonGCSClient = fakeObjectStorage{data: release.tarball}
				rec := testSandboxRecord(release)
				if err := writeSandboxRecord(actorUID, rec); err != nil {
					t.Fatalf("writing sandbox record: %v", err)
				}
				serveFakeWorker(t, &fakeWorker{
					onTerminate: func() error {
						return status.Error(codes.Unavailable, "worker shutting down")
					},
				})
			},
		},
		{
			// Run and Restore write the record before starting the workload,
			// and an earlier Terminate removes it with the actor directory.
			// Without one, no sandbox was started.
			name: "worker reachable but the actor has no sandbox record",
			setup: func(t *testing.T, _ *Atelet) {
				serveFakeWorker(t, &fakeWorker{})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			useTempNodeDirs(t)
			ctx := t.Context()

			stateDirs := []string{
				ateletpath.DurableDirVolumeMountsDir(actorUID),
				ateletpath.CheckpointStateDir(actorUID),
				ateletpath.RestoreStateDir(actorUID),
			}
			for _, dir := range stateDirs {
				if err := os.MkdirAll(dir, 0o755); err != nil {
					t.Fatalf("seeding %s: %v", dir, err)
				}
				if err := os.WriteFile(filepath.Join(dir, "payload"), []byte("actor state"), 0o600); err != nil {
					t.Fatalf("seeding %s: %v", dir, err)
				}
			}
			snapshotDir := ateletpath.LocalSnapshotDir(actorUID, "pause-snap-1")
			if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
				t.Fatalf("seeding %s: %v", snapshotDir, err)
			}

			s := &Atelet{
				workerDialer:      newWorkerDialer(1),
				sandboxAssetCache: newTestAssetCache(t),
				systemInfoVolumes: newSystemInfoVolumeRefresher(nil, nil),
				snapshots:         snapshottracker.New(t.Context(), nil, clock.RealClock{}),
			}
			tt.setup(t, s)
			if _, err := s.Terminate(ctx, &ateletpb.TerminateRequest{
				Atespace:              atespace,
				ActorName:             actorName,
				ActorUid:              actorUID,
				ActorTemplateAtespace: "default",
				ActorTemplateName:     "counter",
				WorkerPodUid:          workerPodUID,
				Spec:                  &ateletpb.WorkloadSpec{},
			}); err != nil {
				t.Fatalf("Terminate: %v", err)
			}

			// Terminate removes both the actor and snapshot directories
			// outright rather than emptying the state dirs in place, so the
			// seeded payloads go with them and both trees are the assertion.
			assertDirGone(t, "actor dir", ateletpath.ActorPath(actorUID))
			assertDirGone(t, "actor snapshot dir", ateletpath.ActorSnapshotPath(actorUID))
		})
	}
}

// TestTerminateCanBeRetried pins that a repeated Terminate succeeds, both when
// a single caller retries and when DeleteActor or RevertActor sends a second
// Terminate after DeleteWorker's best-effort Terminate.
func TestTerminateCanBeRetried(t *testing.T) {
	for _, tt := range []struct {
		name string
		// failFirst fails the first Terminate after its worker teardown, the way
		// a volume that has not finished unmounting does.
		failFirst bool
		// secondWithoutWorker sends the second Terminate with an empty
		// WorkerPodUid, as DeleteActor and RevertActor do after DeleteWorker
		// has already cleared the actor's worker assignment.
		secondWithoutWorker bool
		// wantTeardowns is how many TerminateWorkload calls worker sees. A retry
		// tears down again only when it names a worker and the sandbox record
		// survived the first call.
		wantTeardowns int32
	}{
		{name: "after a terminate that succeeded", wantTeardowns: 1},
		{name: "after a terminate that failed past the teardown", failFirst: true, wantTeardowns: 2},
		{name: "delete or revert after a worker-delete terminate that succeeded", secondWithoutWorker: true, wantTeardowns: 1},
		{name: "delete or revert after a worker-delete terminate that failed partway", failFirst: true, secondWithoutWorker: true, wantTeardowns: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := runPluginActor(t)
			ctx := t.Context()

			snap := a.snapshot("pause-snap-1", 1)
			if _, err := a.checkpoint(t, snap, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}

			// A populated volume directory, which resetActorDirs refuses to
			// delete through.
			straggler := filepath.Join(ateletpath.ExternalVolumeHostPath(pluginActorUID, "data"), "file")
			if tt.failFirst {
				if err := os.MkdirAll(filepath.Dir(straggler), 0o755); err != nil {
					t.Fatalf("seeding %s: %v", straggler, err)
				}
				if err := os.WriteFile(straggler, []byte("still mounted"), 0o600); err != nil {
					t.Fatalf("seeding %s: %v", straggler, err)
				}
			}

			firstReq := &ateletpb.TerminateRequest{
				Atespace:              "ate-demo",
				ActorName:             "counter",
				ActorUid:              pluginActorUID,
				ActorTemplateAtespace: "default",
				ActorTemplateName:     "counter",
				WorkerPodUid:          pluginWorkerPodUID,
				Spec:                  a.spec,
			}
			_, err := a.s.Terminate(ctx, firstReq)
			if tt.failFirst {
				if err == nil {
					t.Fatal("first Terminate succeeded through a populated volume directory")
				}
				// The unmount finishes before the second Terminate.
				if err := os.Remove(straggler); err != nil {
					t.Fatalf("removing %s: %v", straggler, err)
				}
			} else if err != nil {
				t.Fatalf("first Terminate: %v", err)
			}

			secondReq := proto.Clone(firstReq).(*ateletpb.TerminateRequest)
			if tt.secondWithoutWorker {
				secondReq.WorkerPodUid = ""
			}
			if _, err := a.s.Terminate(ctx, secondReq); err != nil {
				t.Fatalf("second Terminate: %v", err)
			}
			if got := a.worker.terminations.Load(); got != tt.wantTeardowns {
				t.Errorf("worker saw %d TerminateWorkload calls, want %d", got, tt.wantTeardowns)
			}
			assertDirGone(t, "actor dir", ateletpath.ActorPath(pluginActorUID))
			assertDirGone(t, "actor snapshot dir", ateletpath.ActorSnapshotPath(pluginActorUID))
		})
	}
}

// TestTerminateWorkerFailurePreservesState checks that when a live worker
// returns Internal from TerminateWorkload, Terminate fails without resetting
// actor directories or pruning local snapshots.
func TestTerminateWorkerFailurePreservesState(t *testing.T) {
	a := runPluginActor(t)
	snap := a.snapshot("pause-snap-1", 1)
	if _, err := a.checkpoint(t, snap, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	durableFile := filepath.Join(ateletpath.DurableDirVolumeMountPoint(pluginActorUID, "work"), "payload")
	if err := os.MkdirAll(filepath.Dir(durableFile), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(durableFile, []byte("runtime-payload"), 0o600); err != nil {
		t.Fatal(err)
	}

	a.worker.onTerminate = func() error { return apierror.Internal("sandbox busy") }
	_, err := a.s.Terminate(t.Context(), &ateletpb.TerminateRequest{
		Atespace:              "ate-demo",
		ActorName:             "counter",
		ActorUid:              pluginActorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          pluginWorkerPodUID,
		Spec:                  a.spec,
	})
	if got := apierror.Code(err); got != codes.Internal {
		t.Fatalf("Terminate error = %v (code %v), want Internal", err, got)
	}
	if _, err := os.Stat(durableFile); err != nil {
		t.Errorf("durable-dir payload removed after failed TerminateWorkload: %v", err)
	}
	snapshotFile := filepath.Join(ateletpath.LocalSnapshotDir(pluginActorUID, "pause-snap-1"), "checkpoint.img")
	if _, err := os.Stat(snapshotFile); err != nil {
		t.Errorf("local snapshot removed after failed TerminateWorkload: %v", err)
	}
}

// pluginActorUID is the actor runPluginActor runs.
const (
	pluginActorUID     = "01234567-89ab-cdef-0123-456789abcdef"
	pluginWorkerPodUID = "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f"
)

// pluginActor is an actor running on a fake worker, on a herder whose snapshot
// plugin stores into store.
type pluginActor struct {
	s      *Atelet
	worker *fakeWorker
	store  *recordingObjectStorage
	rec    *sandboxAssetsRecord
	spec   *ateletpb.WorkloadSpec
}

func runPluginActor(t *testing.T) *pluginActor {
	t.Helper()
	useTempNodeDirs(t)

	a := &pluginActor{
		worker: &fakeWorker{snapshotFiles: map[string]string{"checkpoint.img": "guest-memory"}},
		store:  &recordingObjectStorage{},
	}
	serveFakeWorker(t, a.worker)

	host := imageVolumeTestRegistry(t)
	image := host + "/actor:v1"
	pinnedImage := pushTestImage(t, image, singleFileLayer(t, "bin/app", "app"))

	release := newGVisorTestRelease(t)
	a.s = newPluginHerder(t, a.store)
	a.s.workerDialer = newWorkerDialer(1)
	a.s.imageCache = newImageVolumeStore(t)
	a.s.sandboxAssetCache = newTestAssetCache(t)
	a.s.anonGCSClient = fakeObjectStorage{data: release.tarball}
	a.s.systemInfoVolumes = newSystemInfoVolumeRefresher(nil, nil)
	a.rec = testSandboxRecord(release)
	a.spec = &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{{Name: "app", Image: pinnedImage, Command: []string{"/bin/app"}}},
	}

	advertiseRuntime(t, a.s, a.rec)
	if _, err := a.s.Run(t.Context(), &ateletpb.RunRequest{
		Atespace:              "ate-demo",
		ActorName:             "counter",
		ActorUid:              pluginActorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          pluginWorkerPodUID,
		SandboxRuntime:        runtimeFor(t, a.rec),
		Spec:                  a.spec,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return a
}

// snapshot is the Snapshot the control plane names for the actor's checkpoint
// at generation.
func (a *pluginActor) snapshot(name string, generation int32) *ateletpb.Snapshot {
	return lifecycleSnapshot(pluginActorUID, name, generation, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY)
}

func (a *pluginActor) checkpoint(t *testing.T, snap *ateletpb.Snapshot, mode ateletpb.UploadMode) (*ateletpb.CheckpointResponse, error) {
	t.Helper()
	return a.s.Checkpoint(t.Context(), &ateletpb.CheckpointRequest{
		Atespace:              "ate-demo",
		ActorName:             "counter",
		ActorUid:              pluginActorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          pluginWorkerPodUID,
		Spec:                  a.spec,
		Snapshot:              snap,
		UploadConfig:          &ateletpb.UploadConfig{Mode: mode},
	})
}

// restoreLocal resumes the actor from its local snapshot named name, taken at
// generation. The request names the snapshot's upload location too, as the
// control plane's does.
func (a *pluginActor) restoreLocal(t *testing.T, name string, generation int32) {
	t.Helper()
	if _, err := a.s.Restore(t.Context(), &ateletpb.RestoreRequest{
		Atespace:              "ate-demo",
		ActorName:             "counter",
		ActorUid:              pluginActorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          pluginWorkerPodUID,
		SandboxRuntime:        runtimeFor(t, a.rec),
		Spec:                  a.spec,
		Snapshot:              a.snapshot(name, generation),
	}); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if !a.worker.preserveRestoreDir {
		t.Fatalf("Restore of %s read object storage, want its local copy restored in place", name)
	}
}

// promote promotes snap and returns the actor's state the response reports.
func (a *pluginActor) promote(t *testing.T, snap *ateletpb.Snapshot) *ateletpb.ActorSnapshotState {
	t.Helper()
	resp, err := a.s.PromoteSnapshot(t.Context(), &ateletpb.PromoteSnapshotRequest{
		Atespace:              "ate-demo",
		ActorName:             "counter",
		ActorUid:              pluginActorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		Snapshot:              snap,
	})
	if err != nil {
		t.Fatalf("PromoteSnapshot: %v", err)
	}
	return resp.GetState()
}

// manifestKey is where the object store keeps the manifest of the actor's
// snapshot named name.
func manifestKey(name string) string {
	return "test-bucket/atespaces/ate-demo/actors/" + pluginActorUID + "/snapshots/" + name + "/" + sandboxManifestName
}

// TestImmediateCheckpointKeepsLocalCopy suspends a running actor: the
// checkpoint uploads the snapshot before it returns and keeps a local copy on
// the node.
func TestImmediateCheckpointKeepsLocalCopy(t *testing.T) {
	a := runPluginActor(t)
	snap := a.snapshot("suspend-snap-1", 1)

	resp, err := a.checkpoint(t, snap, ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE)
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if !slices.Contains(a.store.keys(), manifestKey("suspend-snap-1")) {
		t.Errorf("objects = %v, want the snapshot uploaded", a.store.keys())
	}
	if _, err := os.Stat(ateletpath.LocalSnapshotDir(pluginActorUID, "suspend-snap-1")); err != nil {
		t.Errorf("local snapshot after an immediate checkpoint: stat = %v, want it kept", err)
	}
	st := resp.GetState()
	if got := st.GetLatestLocal(); !proto.Equal(got.GetSnapshot(), snap) || got.GetLocalCommitTime() == nil || got.GetDurableCompleteTime() != nil {
		t.Errorf("latest_local = %v, want %v with its commit time and no upload time", got, snap)
	}
	if got := st.GetLatestDurable(); !proto.Equal(got.GetSnapshot(), snap) || got.GetLocalCommitTime() == nil || got.GetDurableCompleteTime() == nil {
		t.Errorf("latest_durable = %v, want %v with its commit and upload times", got, snap)
	}
}

// TestImmediateCheckpointWithoutLocalCopy suspends a running actor whose
// snapshot cannot be stored locally: the snapshot is durable, so the
// checkpoint still succeeds, and reports no local snapshot. A retry of that
// checkpoint answers from the durable snapshot without checkpointing or
// uploading again, and finishes the first call's cleanup.
func TestImmediateCheckpointWithoutLocalCopy(t *testing.T) {
	a := runPluginActor(t)
	var workerCheckpoints atomic.Int32
	a.worker.onCheckpoint = func() { workerCheckpoints.Add(1) }
	snap := a.snapshot("suspend-snap-1", 1)
	// A file where the local checkpoint directory belongs fails the move.
	if err := os.WriteFile(ateletpath.LocalCheckpointsDir(pluginActorUID), nil, 0o600); err != nil {
		t.Fatal(err)
	}

	resp, err := a.checkpoint(t, snap, ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE)
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if !slices.Contains(a.store.keys(), manifestKey("suspend-snap-1")) {
		t.Errorf("objects = %v, want the snapshot uploaded", a.store.keys())
	}
	st := resp.GetState()
	if st.GetLatestLocal() != nil {
		t.Errorf("latest_local = %v, want unset without a local copy", st.GetLatestLocal())
	}
	if got := st.GetLatestDurable(); !proto.Equal(got.GetSnapshot(), snap) || got.GetDurableCompleteTime() == nil {
		t.Errorf("latest_durable = %v, want %v with its upload time", got, snap)
	}
	if got := a.s.snapshots.State(pluginActorUID); !proto.Equal(got, st) {
		t.Errorf("tracked state = %v, want %v, as Checkpoint returned", got, st)
	}

	a.store.mu.Lock()
	a.store.putErr = errors.New("retried Checkpoint uploaded a snapshot that was already durable")
	a.store.mu.Unlock()
	// What the first call's cleanup leaves behind when it fails.
	leftover := filepath.Join(ateletpath.OCIBundleDir(pluginActorUID), "leftover")
	if err := os.MkdirAll(filepath.Dir(leftover), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(leftover, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	retryResp, err := a.checkpoint(t, snap, ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE)
	if err != nil {
		t.Fatalf("retried Checkpoint: %v", err)
	}
	if n := workerCheckpoints.Load(); n != 1 {
		t.Errorf("worker checkpointed the actor %d times, want once", n)
	}
	if !proto.Equal(retryResp.GetState(), st) {
		t.Errorf("retried Checkpoint state = %v, want %v", retryResp.GetState(), st)
	}
	if _, err := os.Stat(leftover); !os.IsNotExist(err) {
		t.Errorf("leftover in the bundle dir after the retry: stat = %v, want it cleaned up", err)
	}
}

// TestStaleCheckpointRetryLeavesRestoredActorAlone retries a Checkpoint
// after the actor was restored from the snapshot it committed, from its local
// copy or, once atelet detached it, from its upload: atelet answers from that
// snapshot without checkpointing or cleaning up, since the actor's
// directories and volumes belong to the restored activation now.
func TestStaleCheckpointRetryLeavesRestoredActorAlone(t *testing.T) {
	for _, tc := range []struct {
		name   string
		detach bool
	}{
		{name: "restored from the local copy"},
		{name: "restored from the upload", detach: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := runPluginActor(t)
			a.s.snapshots = snapshottracker.New(t.Context(), &trackerStore{s: a.s}, clock.RealClock{})
			var workerCheckpoints atomic.Int32
			a.worker.onCheckpoint = func() { workerCheckpoints.Add(1) }
			snap := a.snapshot("pause-snap-1", 1)
			if _, err := a.checkpoint(t, snap, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}
			if tc.detach {
				if _, err := a.s.snapshots.Detach(t.Context(), pluginActorUID); err != nil {
					t.Fatalf("Detach: %v", err)
				}
			}
			// Restore reads the local copy if there is one, and otherwise the
			// upload.
			if _, err := a.s.Restore(t.Context(), &ateletpb.RestoreRequest{
				Atespace:              "ate-demo",
				ActorName:             "counter",
				ActorUid:              pluginActorUID,
				ActorTemplateAtespace: "default",
				ActorTemplateName:     "counter",
				WorkerPodUid:          pluginWorkerPodUID,
				SandboxRuntime:        runtimeFor(t, a.rec),
				Spec:                  a.spec,
				Snapshot:              snap,
			}); err != nil {
				t.Fatalf("Restore: %v", err)
			}
			inUse := filepath.Join(ateletpath.OCIBundleDir(pluginActorUID), "in-use")
			if err := os.WriteFile(inUse, nil, 0o600); err != nil {
				t.Fatal(err)
			}

			resp, err := a.checkpoint(t, snap, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
			if err != nil {
				t.Fatalf("stale retry of the Checkpoint: %v", err)
			}
			if n := workerCheckpoints.Load(); n != 1 {
				t.Errorf("worker checkpointed the actor %d times, want once", n)
			}
			st := resp.GetState()
			committed := st.GetLatestLocal().GetSnapshot()
			if tc.detach {
				committed = st.GetLatestDurable().GetSnapshot()
			}
			if !proto.Equal(committed, snap) {
				t.Errorf("state = %v, want %v as committed", st, snap)
			}
			if _, err := os.Stat(inUse); err != nil {
				t.Errorf("restored activation's bundle dir after the stale retry: stat = %v, want it untouched", err)
			}
		})
	}
}

// TestRetriedCheckpointAnswersFromCommittedSnapshot retries a Checkpoint
// that committed its snapshot, as the control plane does when the first
// call's response is lost: atelet answers without checkpointing the worker
// again, which has no workload left to checkpoint, and finishes the first
// call's cleanup. A retry under UPLOAD_MODE_IMMEDIATE uploads the snapshot
// first if the first Checkpoint deferred its upload, and a retry under
// UPLOAD_MODE_DEFERRED after atelet detached the snapshot returns no local
// snapshot. A retry records no checkpoint duration.
func TestRetriedCheckpointAnswersFromCommittedSnapshot(t *testing.T) {
	const (
		deferred  = ateletpb.UploadMode_UPLOAD_MODE_DEFERRED
		immediate = ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE
	)
	for _, tc := range []struct {
		name         string
		first, retry ateletpb.UploadMode
		detachFirst  bool
		wantLocal    bool
		wantDurable  bool
	}{
		{name: "deferred", first: deferred, retry: deferred, wantLocal: true},
		{name: "immediate", first: immediate, retry: immediate, wantLocal: true, wantDurable: true},
		{name: "deferred then immediate", first: deferred, retry: immediate, wantLocal: true, wantDurable: true},
		{name: "immediate then deferred", first: immediate, retry: deferred, wantLocal: true, wantDurable: true},
		{name: "deferred after detach", first: deferred, retry: deferred, detachFirst: true, wantDurable: true},
		{name: "immediate after detach", first: deferred, retry: immediate, detachFirst: true, wantDurable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := runPluginActor(t)
			a.s.snapshots = snapshottracker.New(t.Context(), &trackerStore{s: a.s}, clock.RealClock{})
			var workerCheckpoints atomic.Int32
			a.worker.onCheckpoint = func() { workerCheckpoints.Add(1) }
			snap := a.snapshot("snap-1", 1)
			if _, err := a.checkpoint(t, snap, tc.first); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}
			if tc.detachFirst {
				if _, err := a.s.snapshots.Detach(t.Context(), pluginActorUID); err != nil {
					t.Fatalf("Detach: %v", err)
				}
			}
			// What the first call's cleanup leaves behind when it fails.
			leftover := filepath.Join(ateletpath.OCIBundleDir(pluginActorUID), "leftover")
			if err := os.MkdirAll(filepath.Dir(leftover), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(leftover, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			inst, reader := newTestInstruments(t)
			a.s.instruments = inst

			resp, err := a.checkpoint(t, snap, tc.retry)
			if err != nil {
				t.Fatalf("retried Checkpoint: %v", err)
			}
			if n := workerCheckpoints.Load(); n != 1 {
				t.Errorf("worker checkpointed the actor %d times, want once", n)
			}
			if _, err := os.Stat(leftover); !os.IsNotExist(err) {
				t.Errorf("leftover in the bundle dir after the retry: stat = %v, want it cleaned up", err)
			}
			if _, err := os.Stat(ateletpath.OCIBundleDir(pluginActorUID)); (err == nil) != tc.wantLocal {
				t.Errorf("bundle dir exists = %v (err = %v), want %v", err == nil, err, tc.wantLocal)
			}
			st := resp.GetState()
			if got := st.GetLatestLocal().GetSnapshot(); proto.Equal(got, snap) != tc.wantLocal {
				t.Errorf("latest_local = %v, want local = %v", got, tc.wantLocal)
			}
			if got := st.GetLatestDurable().GetSnapshot(); proto.Equal(got, snap) != tc.wantDurable {
				t.Errorf("latest_durable = %v, want durable = %v", got, tc.wantDurable)
			}
			if got := slices.Contains(a.store.keys(), manifestKey("snap-1")); got != tc.wantDurable {
				t.Errorf("snapshot uploaded = %v, want %v", got, tc.wantDurable)
			}
			if got := a.s.snapshots.State(pluginActorUID); !proto.Equal(got, st) {
				t.Errorf("tracked state = %v, want %v, as the retry returned", got, st)
			}
			var rm metricdata.ResourceMetrics
			if err := reader.Collect(t.Context(), &rm); err != nil {
				t.Fatalf("collect: %v", err)
			}
			if len(rm.ScopeMetrics) != 0 {
				t.Error("the retry recorded a checkpoint duration, want none")
			}
		})
	}
}

// TestTerminateWaitsForInFlightRPC checks that Terminate waits on the
// per-actor lock while Checkpoint or PromoteSnapshot is in flight, then cleans
// up the actor's tracked state and local files once that RPC returns.
func TestTerminateWaitsForInFlightRPC(t *testing.T) {
	waitTerminateBehind := func(t *testing.T, a *pluginActor) (block func(), finish func()) {
		t.Helper()
		var once sync.Once
		started := make(chan struct{})
		release := make(chan struct{})
		block = func() {
			once.Do(func() {
				close(started)
				<-release
			})
		}
		finish = func() {
			t.Helper()
			<-started
			termDone := make(chan error, 1)
			go func() {
				_, err := a.s.Terminate(t.Context(), &ateletpb.TerminateRequest{
					Atespace:     "ate-demo",
					ActorName:    "counter",
					ActorUid:     pluginActorUID,
					WorkerPodUid: pluginWorkerPodUID,
					Spec:         a.spec,
				})
				termDone <- err
			}()
			select {
			case err := <-termDone:
				t.Fatalf("Terminate returned %v while another RPC held the actor lock, want it to wait", err)
			case <-time.After(50 * time.Millisecond):
			}
			close(release)
			if err := <-termDone; err != nil {
				t.Fatalf("Terminate: %v", err)
			}
		}
		return block, finish
	}

	assertCleanedUp := func(t *testing.T, a *pluginActor) {
		t.Helper()
		if st := a.s.snapshots.State(pluginActorUID); !proto.Equal(st, &ateletpb.ActorSnapshotState{}) {
			t.Errorf("tracked state = %v, want none", st)
		}
		if _, err := os.Stat(ateletpath.ActorSnapshotStateFile(pluginActorUID)); !os.IsNotExist(err) {
			t.Errorf("snapshot state file: stat = %v, want none", err)
		}
		if _, err := os.Stat(ateletpath.LocalSnapshotDir(pluginActorUID, "snap-1")); !os.IsNotExist(err) {
			t.Errorf("local snapshot: stat = %v, want none", err)
		}
		if _, err := os.Stat(ateletpath.ActorPath(pluginActorUID)); !os.IsNotExist(err) {
			t.Errorf("actor dir after Terminate: stat = %v, want none", err)
		}
	}

	for _, mode := range []ateletpb.UploadMode{
		ateletpb.UploadMode_UPLOAD_MODE_DEFERRED,
		ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			a := runPluginActor(t)
			block, finish := waitTerminateBehind(t, a)
			a.worker.onCheckpoint = block
			cpDone := make(chan error, 1)
			go func() {
				_, err := a.checkpoint(t, a.snapshot("snap-1", 1), mode)
				cpDone <- err
			}()
			finish()
			if err := <-cpDone; err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}
			assertCleanedUp(t, a)
		})
	}

	t.Run("PromoteSnapshot", func(t *testing.T) {
		a := runPluginActor(t)
		snap := a.snapshot("snap-1", 1)
		if _, err := a.checkpoint(t, snap, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}

		block, finish := waitTerminateBehind(t, a)
		a.store.onPut = block
		promoteDone := make(chan error, 1)
		go func() {
			_, err := a.s.PromoteSnapshot(t.Context(), &ateletpb.PromoteSnapshotRequest{
				Atespace:              "ate-demo",
				ActorName:             "counter",
				ActorUid:              pluginActorUID,
				ActorTemplateAtespace: "default",
				ActorTemplateName:     "counter",
				Snapshot:              snap,
			})
			promoteDone <- err
		}()
		finish()
		if err := <-promoteDone; err != nil {
			t.Fatalf("PromoteSnapshot: %v", err)
		}
		assertCleanedUp(t, a)
	})
}

// TestFailedCheckpointKeepsLocalSnapshot checkpoints a resumed actor again, and
// fails before the new snapshot commits: the local snapshot stays on disk and
// recorded as the actor's latest local snapshot, so the actor can still be
// restored from it.
func TestFailedCheckpointKeepsLocalSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name          string
		snap          string
		mode          ateletpb.UploadMode
		fail          func(t *testing.T, a *pluginActor)
		wantNoNewSnap bool
	}{
		{
			name: "suspend upload fails",
			snap: "suspend-snap-2",
			mode: ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE,
			fail: func(t *testing.T, a *pluginActor) {
				a.store.putErr = errors.New("object store unavailable")
			},
		},
		{
			name: "pause move fails",
			snap: "pause-snap-2",
			mode: ateletpb.UploadMode_UPLOAD_MODE_DEFERRED,
			fail: func(t *testing.T, a *pluginActor) {
				// A file where the new snapshot's directory belongs fails the move.
				if err := os.WriteFile(ateletpath.LocalSnapshotDir(pluginActorUID, "pause-snap-2"), nil, 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "missing snapshot file",
			snap: "pause-snap-2",
			mode: ateletpb.UploadMode_UPLOAD_MODE_DEFERRED,
			fail: func(t *testing.T, a *pluginActor) {
				var once sync.Once
				a.worker.onCheckpoint = func() {
					once.Do(func() {
						if err := os.Remove(filepath.Join(ateletpath.CheckpointStateDir(pluginActorUID), "checkpoint.img")); err != nil {
							t.Error(err)
						}
					})
				}
			},
			wantNoNewSnap: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := runPluginActor(t)
			if _, err := a.checkpoint(t, a.snapshot("pause-snap-1", 1), ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err != nil {
				t.Fatalf("Checkpoint (pause): %v", err)
			}
			a.restoreLocal(t, "pause-snap-1", 1)

			tc.fail(t, a)
			if _, err := a.checkpoint(t, a.snapshot(tc.snap, 2), tc.mode); err == nil {
				t.Fatal("Checkpoint succeeded, want it to fail")
			}
			if tc.wantNoNewSnap {
				if _, err := os.Stat(ateletpath.LocalSnapshotDir(pluginActorUID, tc.snap)); !os.IsNotExist(err) {
					t.Errorf("failed move created local snapshot %q: stat = %v, want not exist", tc.snap, err)
				}
			}
			if _, err := os.Stat(ateletpath.LocalSnapshotDir(pluginActorUID, "pause-snap-1")); err != nil {
				t.Errorf("pause snapshot after the failed checkpoint: stat = %v, want it kept", err)
			}
			if got := a.s.snapshots.State(pluginActorUID).GetLatestLocal().GetSnapshot().GetSnapshotUuid(); got != "pause-snap-1" {
				t.Errorf("latest_local = %q, want pause-snap-1", got)
			}
			a.restoreLocal(t, "pause-snap-1", 1)
			if entries, err := os.ReadDir(ateletpath.CheckpointStateDir(pluginActorUID)); err != nil || len(entries) != 0 {
				t.Errorf("checkpoint-state after restore: entries = %v, err = %v, want empty", entries, err)
			}
		})
	}
}

// TestPromoteSnapshotRecordsUpload promotes a suspended actor's local snapshot:
// the promote records the uploaded snapshot as the latest durable one, with the
// checkpoint's commit time, and keeps the local copy on the node. A retry
// reports the same state.
func TestPromoteSnapshotRecordsUpload(t *testing.T) {
	a := runPluginActor(t)
	snap := a.snapshot("pause-snap-1", 1)
	resp, err := a.checkpoint(t, snap, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	commitTime := resp.GetState().GetLatestLocal().GetLocalCommitTime()

	st := a.promote(t, snap)
	if _, err := os.Stat(ateletpath.LocalSnapshotDir(pluginActorUID, "pause-snap-1")); err != nil {
		t.Errorf("local snapshot after promote: stat = %v, want it kept", err)
	}
	if got := st.GetLatestLocal(); !proto.Equal(got.GetSnapshot(), snap) || !proto.Equal(got.GetLocalCommitTime(), commitTime) || got.GetDurableCompleteTime() != nil {
		t.Errorf("latest_local = %v, want %v committed at %v, with no upload time", got, snap, commitTime)
	}
	if got := st.GetLatestDurable(); !proto.Equal(got.GetSnapshot(), snap) || !proto.Equal(got.GetLocalCommitTime(), commitTime) || got.GetDurableCompleteTime() == nil {
		t.Errorf("latest_durable = %v, want %v committed at %v, with its upload time", got, snap, commitTime)
	}
	if got := a.s.snapshots.State(pluginActorUID); !proto.Equal(got, st) {
		t.Errorf("tracked state = %v, want %v, as PromoteSnapshot returned", got, st)
	}

	a.store.putErr = errors.New("retried promote should not upload")
	if got := a.promote(t, snap); !proto.Equal(got, st) {
		t.Errorf("state after a retried promote = %v, want %v", got, st)
	}

	mismatch := proto.CloneOf(snap)
	mismatch.Generation = snap.GetGeneration() + 1
	if _, err := a.s.PromoteSnapshot(t.Context(), &ateletpb.PromoteSnapshotRequest{
		Atespace:              "ate-demo",
		ActorName:             "counter",
		ActorUid:              pluginActorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		Snapshot:              mismatch,
	}); apierror.Code(err) != codes.FailedPrecondition {
		t.Errorf("PromoteSnapshot with mismatched generation = %v, want FailedPrecondition", err)
	}
}

// TestConcurrentRunsShareOneAssetDownload runs two actors that need the same
// asset at the same time: the asset cache downloads it once, and each actor
// gets its own hard link to the one cached copy.
func TestConcurrentRunsShareOneAssetDownload(t *testing.T) {
	useTempNodeDirs(t)
	ctx := t.Context()

	worker := &fakeWorker{}
	serveFakeWorker(t, worker)

	host := imageVolumeTestRegistry(t)
	pinnedImage := pushTestImage(t, host+"/actor:v1", singleFileLayer(t, "bin/app", "app"))

	release := newGVisorTestRelease(t)
	var gets atomic.Int32
	s := &Atelet{
		workerDialer:      newWorkerDialer(2),
		imageCache:        newImageVolumeStore(t),
		sandboxAssetCache: newTestAssetCache(t),
		anonGCSClient:     fakeObjectStorage{data: release.tarball, gets: &gets, delay: 200 * time.Millisecond},
		systemInfoVolumes: newSystemInfoVolumeRefresher(nil, nil),
		snapshots:         snapshottracker.New(t.Context(), nil, clock.RealClock{}),
	}
	sandboxRec := testSandboxRecord(release)
	spec := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{{Name: "app", Image: pinnedImage, Command: []string{"/bin/app"}}},
	}

	advertiseRuntime(t, s, sandboxRec)
	actorUIDs := []string{"01234567-89ab-cdef-0123-456789abcde1", "01234567-89ab-cdef-0123-456789abcde2"}
	var wg sync.WaitGroup
	errs := make([]error, len(actorUIDs))
	for i, uid := range actorUIDs {
		wg.Go(func() {
			_, errs[i] = s.Run(ctx, &ateletpb.RunRequest{
				Atespace:              "ate-demo",
				ActorName:             "counter-" + uid[len(uid)-1:],
				ActorUid:              uid,
				ActorTemplateAtespace: "default",
				ActorTemplateName:     "counter",
				WorkerPodUid:          "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f",
				SandboxRuntime:        runtimeFor(t, sandboxRec),
				Spec:                  spec,
			})
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("Run %d: %v", i, err)
		}
	}
	if gets.Load() != 1 {
		t.Errorf("asset downloaded %d times for two concurrent Runs, want 1", gets.Load())
	}

	var infos []os.FileInfo
	for _, uid := range actorUIDs {
		staged := stagedRunsc(uid)
		got, err := os.ReadFile(staged)
		if err != nil || string(got) != "runsc bytes" {
			t.Fatalf("actor %s staged runsc = %q, %v; want the release's runsc", uid, got, err)
		}
		fi, err := os.Stat(staged)
		if err != nil {
			t.Fatal(err)
		}
		infos = append(infos, fi)
	}
	if !os.SameFile(infos[0], infos[1]) {
		t.Error("the two actors' runsc links are not one inode")
	}
	for _, uid := range actorUIDs {
		if _, err := os.Lstat(ateletpath.SandboxAssetsStagingDir(uid)); !os.IsNotExist(err) {
			t.Errorf("actor %s: staging dir left behind after Run (lstat err = %v)", uid, err)
		}
	}
}

// TestCommitSandboxAssetsChecksPathsBeforeMoving pins the order in
// commitSandboxAssets: a path outside the staging dir fails the commit
// before anything moves, so the staged set stays for Run's cleanup and the
// assets dir, which the sandbox record still describes, is untouched.
func TestCommitSandboxAssetsChecksPathsBeforeMoving(t *testing.T) {
	useTempNodeDirs(t)
	const actorUID = "01234567-89ab-cdef-0123-456789abcdef"
	staging, final := ateletpath.SandboxAssetsStagingDir(actorUID), ateletpath.SandboxAssetsDir(actorUID)
	runsc := filepath.Join(gvisorAssetName, "runsc")
	for _, dir := range []string{filepath.Join(staging, gvisorAssetName), filepath.Join(final, gvisorAssetName)} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(staging, runsc), []byte("new"), 0o555); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(final, runsc), []byte("previous"), 0o555); err != nil {
		t.Fatal(err)
	}
	paths := map[string]string{gvisorAssetName: filepath.Join(staging, runsc), "stray": "/elsewhere/kernel"}
	if err := commitSandboxAssets(actorUID, staging, paths); err == nil {
		t.Fatal("commitSandboxAssets accepted a path outside the staging dir")
	}
	if got, err := os.ReadFile(filepath.Join(final, runsc)); err != nil || string(got) != "previous" {
		t.Errorf("assets dir changed by a refused commit: %q, %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(staging, runsc)); err != nil {
		t.Errorf("staged set gone after a refused commit: %v", err)
	}
	if paths[gvisorAssetName] != filepath.Join(staging, runsc) {
		t.Errorf("paths rewritten by a refused commit: %v", paths)
	}

	delete(paths, "stray")
	if err := commitSandboxAssets(actorUID, staging, paths); err != nil {
		t.Fatalf("commitSandboxAssets: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(final, runsc)); err != nil || string(got) != "new" {
		t.Errorf("assets dir after commit = %q, %v; want the staged set", got, err)
	}
	if paths[gvisorAssetName] != filepath.Join(final, runsc) {
		t.Errorf("paths after commit = %v, want them under %s", paths, final)
	}
	if _, err := os.Lstat(staging); !os.IsNotExist(err) {
		t.Errorf("staging dir remains after commit (lstat err = %v)", err)
	}
}

func TestTrackerStoreUploadLocalSnapshot(t *testing.T) {
	a := runPluginActor(t)
	snap := a.snapshot("pause-snap-1", 1)
	if _, err := a.checkpoint(t, snap, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	st := &trackerStore{s: a.s}

	// An upload asking for a fidelity other than the checkpoint captured is
	// rejected before uploading.
	noFidelity := proto.CloneOf(snap)
	noFidelity.Fidelity = ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED
	if _, err := st.UploadLocalSnapshot(t.Context(), pluginActorUID, snapshottracker.SnapshotRef{Snapshot: noFidelity}); apierror.Code(err) != codes.FailedPrecondition {
		t.Fatalf("UploadLocalSnapshot with another fidelity: err = %v, want FailedPrecondition", err)
	}

	// First upload writes to object storage and reports uploaded = true.
	uploaded, err := st.UploadLocalSnapshot(t.Context(), pluginActorUID, snapshottracker.SnapshotRef{Snapshot: snap})
	if err != nil || !uploaded {
		t.Fatalf("UploadLocalSnapshot = (%v, %v), want (true, nil)", uploaded, err)
	}

	// Once the local copy is pruned, a retry sees the remote manifest and
	// reports uploaded = false.
	if err := snapshottracker.PruneLocalCheckpoints(t.Context(), pluginActorUID); err != nil {
		t.Fatalf("PruneLocalCheckpoints: %v", err)
	}
	uploaded, err = st.UploadLocalSnapshot(t.Context(), pluginActorUID, snapshottracker.SnapshotRef{Snapshot: snap})
	if err != nil || uploaded {
		t.Fatalf("UploadLocalSnapshot after prune = (%v, %v), want (false, nil)", uploaded, err)
	}
}

// TestRestoredActorIsNotDetached restores an actor from its local copy:
// once the actor runs on the node, atelet refuses to detach it.
func TestRestoredActorIsNotDetached(t *testing.T) {
	a := runPluginActor(t)
	a.s.snapshots = snapshottracker.New(t.Context(), &trackerStore{s: a.s}, clock.RealClock{})
	if _, err := a.checkpoint(t, a.snapshot("pause-snap-1", 1), ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err != nil {
		t.Fatalf("Checkpoint (pause): %v", err)
	}
	a.restoreLocal(t, "pause-snap-1", 1)
	if _, err := a.s.snapshots.Detach(t.Context(), pluginActorUID); apierror.Code(err) != codes.Aborted {
		t.Errorf("Detach of the restored actor = %v, want Aborted", err)
	}
}

// TestFailedRestoreUnschedulesDetach fails a restore of an actor from its local
// snapshot after it began: as after a restore that succeeds, atelet keeps the
// actor's local snapshot, and does not detach the actor until its next
// checkpoint.
func TestFailedRestoreUnschedulesDetach(t *testing.T) {
	a := runPluginActor(t)
	a.s.snapshots = snapshottracker.New(t.Context(), &trackerStore{s: a.s}, clock.RealClock{})
	if _, err := a.checkpoint(t, a.snapshot("pause-snap-1", 1), ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err != nil {
		t.Fatalf("Checkpoint (pause): %v", err)
	}
	// A snapshot with neither a local copy nor an uploaded copy fails the
	// restore once it has begun.
	_, err := a.s.Restore(t.Context(), &ateletpb.RestoreRequest{
		Atespace:              "ate-demo",
		ActorName:             "counter",
		ActorUid:              pluginActorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          pluginWorkerPodUID,
		SandboxRuntime:        runtimeFor(t, a.rec),
		Spec:                  a.spec,
		Snapshot:              a.snapshot("missing-snap", 2),
	})
	if apierror.Code(err) != codes.NotFound {
		t.Fatalf("Restore of a missing snapshot = %v, want NotFound", err)
	}
	if got := a.s.snapshots.State(pluginActorUID).GetLatestLocal().GetSnapshot().GetSnapshotUuid(); got != "pause-snap-1" {
		t.Errorf("latest_local = %q after the failed restore, want pause-snap-1 kept", got)
	}
	if _, err := a.s.snapshots.Detach(t.Context(), pluginActorUID); apierror.Code(err) != codes.Aborted {
		t.Errorf("Detach after the failed restore = %v, want Aborted", err)
	}
}

// TestRestoreAfterDetachReadsUpload resumes a suspended actor that atelet
// detached before the control plane recorded the upload, so the control plane
// still sends the Restore here: atelet restores the actor from the upload the
// request names.
func TestRestoreAfterDetachReadsUpload(t *testing.T) {
	a := runPluginActor(t)
	a.s.snapshots = snapshottracker.New(t.Context(), &trackerStore{s: a.s}, clock.RealClock{})
	snap := a.snapshot("pause-snap-1", 1)
	if _, err := a.checkpoint(t, snap, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err != nil {
		t.Fatalf("Checkpoint (pause): %v", err)
	}
	// A Detach call uploads and prunes as a background detach does.
	st, err := a.s.snapshots.Detach(t.Context(), pluginActorUID)
	if err != nil {
		t.Fatalf("Detach: %v", err)
	}
	if st.GetLatestLocal() != nil || st.GetLatestDurable().GetSnapshot().GetSnapshotUuid() != "pause-snap-1" {
		t.Fatalf("state after detach = %v, want pause-snap-1 durable and nothing local", st)
	}

	if _, err := a.s.Restore(t.Context(), &ateletpb.RestoreRequest{
		Atespace:              "ate-demo",
		ActorName:             "counter",
		ActorUid:              pluginActorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          pluginWorkerPodUID,
		SandboxRuntime:        runtimeFor(t, a.rec),
		Spec:                  a.spec,
		Snapshot:              snap,
	}); err != nil {
		t.Fatalf("Restore of the detached snapshot: %v", err)
	}
	if a.worker.preserveRestoreDir {
		t.Error("Restore restored a local copy in place, want the upload staged")
	}
	if got := a.worker.restored["checkpoint.img"]; got != "guest-memory" {
		t.Errorf("restored checkpoint.img = %q, want the paused snapshot's %q", got, "guest-memory")
	}
}

func detachActorRequest() *ateletpb.DetachActorRequest {
	return &ateletpb.DetachActorRequest{Atespace: "ate-demo", ActorName: "counter", ActorUid: pluginActorUID}
}

// TestDetachActorDetachesPausedActor detaches a suspended actor through the
// RPC: atelet uploads its local snapshot, deletes the local copy, returns the
// new state, and removes the actor's directory once the detach report is
// acknowledged.
func TestDetachActorDetachesPausedActor(t *testing.T) {
	a := runPluginActor(t)
	a.s.snapshots = snapshottracker.New(t.Context(), &trackerStore{s: a.s}, clock.RealClock{},
		snapshottracker.WithReporter(&trackerReporter{workers: newFakeReportService()}))
	a.s.snapshots.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := a.s.snapshots.Wait(ctx); err != nil {
			t.Errorf("Wait: %v", err)
		}
	})
	if _, err := a.checkpoint(t, a.snapshot("pause-snap-1", 1), ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err != nil {
		t.Fatalf("Checkpoint (pause): %v", err)
	}
	localDir := ateletpath.LocalSnapshotDir(pluginActorUID, "pause-snap-1")
	if _, err := os.Stat(localDir); err != nil {
		t.Fatalf("local snapshot dir after the pause: %v", err)
	}
	resp, err := a.s.DetachActor(t.Context(), detachActorRequest())
	if err != nil {
		t.Fatalf("DetachActor: %v", err)
	}
	if st := resp.GetState(); st.GetLatestLocal() != nil || st.GetLatestDurable().GetSnapshot().GetSnapshotUuid() != "pause-snap-1" {
		t.Errorf("state = %v, want pause-snap-1 durable and nothing local", st)
	}
	if _, err := os.Stat(localDir); !os.IsNotExist(err) {
		t.Errorf("local snapshot dir after DetachActor: stat = %v, want it deleted", err)
	}
	waitFor(t, "the actor dir to be removed after the detach report", func() bool {
		return !exists(ateletpath.ActorPath(pluginActorUID))
	})
	resp, err = a.s.DetachActor(t.Context(), detachActorRequest())
	if err != nil {
		t.Fatalf("second DetachActor: %v", err)
	}
	if !proto.Equal(resp.GetState(), &ateletpb.ActorSnapshotState{}) {
		t.Errorf("second DetachActor state = %v, want empty", resp.GetState())
	}
}

// TestDetachActorWithoutLocalSnapshot detaches an actor atelet holds no local
// snapshot for: DetachActor does nothing and returns an empty state.
func TestDetachActorWithoutLocalSnapshot(t *testing.T) {
	a := runPluginActor(t)
	a.s.snapshots = snapshottracker.New(t.Context(), &trackerStore{s: a.s}, clock.RealClock{})
	resp, err := a.s.DetachActor(t.Context(), detachActorRequest())
	if err != nil {
		t.Fatalf("DetachActor: %v", err)
	}
	if !proto.Equal(resp.GetState(), &ateletpb.ActorSnapshotState{}) {
		t.Errorf("state = %v, want empty", resp.GetState())
	}
}

// TestDetachActorRejectsRestoredActor detaches an actor restored from its
// local snapshot: DetachActor fails with Aborted.
func TestDetachActorRejectsRestoredActor(t *testing.T) {
	a := runPluginActor(t)
	a.s.snapshots = snapshottracker.New(t.Context(), &trackerStore{s: a.s}, clock.RealClock{})
	if _, err := a.checkpoint(t, a.snapshot("pause-snap-1", 1), ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err != nil {
		t.Fatalf("Checkpoint (pause): %v", err)
	}
	a.restoreLocal(t, "pause-snap-1", 1)
	if _, err := a.s.DetachActor(t.Context(), detachActorRequest()); apierror.Code(err) != codes.Aborted {
		t.Errorf("DetachActor of the restored actor = %v, want Aborted", err)
	}
}

// TestDetachActorValidatesRequest sends a DetachActor request without an
// actor UID: atelet rejects it with InvalidArgument.
func TestDetachActorValidatesRequest(t *testing.T) {
	a := runPluginActor(t)
	a.s.snapshots = snapshottracker.New(t.Context(), &trackerStore{s: a.s}, clock.RealClock{})
	req := detachActorRequest()
	req.ActorUid = ""
	if _, err := a.s.DetachActor(t.Context(), req); apierror.Code(err) != codes.InvalidArgument {
		t.Errorf("DetachActor without actor_uid = %v, want InvalidArgument", err)
	}
}

// TestDetachActorWaitsForInFlightCheckpoint checks that DetachActor waits on
// the per-actor lock while a Checkpoint is in flight, then detaches the
// snapshot that Checkpoint committed.
func TestDetachActorWaitsForInFlightCheckpoint(t *testing.T) {
	a := runPluginActor(t)
	a.s.snapshots = snapshottracker.New(t.Context(), &trackerStore{s: a.s}, clock.RealClock{})
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	a.worker.onCheckpoint = func() {
		once.Do(func() {
			close(started)
			<-release
		})
	}
	cpErr := make(chan error, 1)
	go func() {
		_, err := a.checkpoint(t, a.snapshot("pause-snap-1", 1), ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
		cpErr <- err
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the Checkpoint to reach the worker")
	}

	type detachResult struct {
		resp *ateletpb.DetachActorResponse
		err  error
	}
	detached := make(chan detachResult, 1)
	go func() {
		resp, err := a.s.DetachActor(t.Context(), detachActorRequest())
		detached <- detachResult{resp, err}
	}()
	select {
	case r := <-detached:
		t.Fatalf("DetachActor returned (%v, %v) while a Checkpoint held the actor lock, want it to wait", r.resp, r.err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if err := <-cpErr; err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	r := <-detached
	if r.err != nil {
		t.Fatalf("DetachActor: %v", r.err)
	}
	if st := r.resp.GetState(); st.GetLatestLocal() != nil || st.GetLatestDurable().GetSnapshot().GetSnapshotUuid() != "pause-snap-1" {
		t.Errorf("state = %v, want pause-snap-1 durable and nothing local", st)
	}
}

// TestInvalidRunKeepsDetachScheduled runs an actor with a request that
// names no sandbox runtime: Run fails before it begins the start, so the
// actor's detach stays scheduled.
func TestInvalidRunKeepsDetachScheduled(t *testing.T) {
	a := runPluginActor(t)
	a.s.snapshots = snapshottracker.New(t.Context(), &trackerStore{s: a.s}, clock.RealClock{})
	if _, err := a.checkpoint(t, a.snapshot("pause-snap-1", 1), ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err != nil {
		t.Fatalf("Checkpoint (pause): %v", err)
	}
	_, err := a.s.Run(t.Context(), &ateletpb.RunRequest{
		Atespace:              "ate-demo",
		ActorName:             "counter",
		ActorUid:              pluginActorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          pluginWorkerPodUID,
		Spec:                  a.spec,
	})
	if apierror.Code(err) != codes.InvalidArgument {
		t.Fatalf("Run without sandbox_runtime = %v, want InvalidArgument", err)
	}
	if _, err := a.s.snapshots.Detach(t.Context(), pluginActorUID); err != nil {
		t.Errorf("Detach after the invalid Run = %v, want the detach still scheduled", err)
	}
}

// blockingTrackerStore is a snapshottracker.Store whose uploads report on
// started, then block until their context ends.
type blockingTrackerStore struct{ started chan struct{} }

func (s *blockingTrackerStore) UploadLocalSnapshot(ctx context.Context, _ string, _ snapshottracker.SnapshotRef) (bool, error) {
	s.started <- struct{}{}
	<-ctx.Done()
	return false, ctx.Err()
}

// startBlockedDetach checkpoints the actor locally as pause-snap-1, and starts
// a Detach of it whose upload blocks until the test ends.
func startBlockedDetach(t *testing.T, a *pluginActor) {
	t.Helper()
	store := &blockingTrackerStore{started: make(chan struct{}, 1)}
	a.s.snapshots = snapshottracker.New(t.Context(), store, clock.RealClock{})
	if _, err := a.checkpoint(t, a.snapshot("pause-snap-1", 1), ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err != nil {
		t.Fatalf("Checkpoint (pause): %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	detached := make(chan struct{})
	go func() {
		defer close(detached)
		_, _ = a.s.snapshots.Detach(ctx, pluginActorUID)
	}()
	t.Cleanup(func() {
		cancel()
		<-detached
		// The detach outlives the call, until the tracker's context ends.
		waitCtx, cancelWait := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelWait()
		if err := a.s.snapshots.Wait(waitCtx); err != nil {
			t.Errorf("detach still running after the test: %v", err)
		}
	})
	select {
	case <-store.started:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the detach's upload to start")
	}
}

// TestCheckpointRecordsDetachWait gives up a Checkpoint while it waits for the
// actor's detach in progress: the checkpoint's duration, the wait included, is
// still recorded.
func TestCheckpointRecordsDetachWait(t *testing.T) {
	a := runPluginActor(t)
	startBlockedDetach(t, a)
	inst, reader := newTestInstruments(t)
	a.s.instruments = inst

	gaveUp, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := a.s.Checkpoint(gaveUp, &ateletpb.CheckpointRequest{
		Atespace:              "ate-demo",
		ActorName:             "counter",
		ActorUid:              pluginActorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          pluginWorkerPodUID,
		Spec:                  a.spec,
		Snapshot:              a.snapshot("pause-snap-2", 2),
		UploadConfig:          &ateletpb.UploadConfig{Mode: ateletpb.UploadMode_UPLOAD_MODE_DEFERRED},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Checkpoint that gave up waiting for the detach = %v, want %v", err, context.Canceled)
	}
	if _, ok := phaseValues(t, collectHistogram(t, reader, checkpointDurationMetric))[ateattr.SnapshotPhaseTotal]; !ok {
		t.Error("no total duration recorded for the checkpoint")
	}
}

// TestPromoteSnapshotRecordsDetachWait gives up a PromoteSnapshot while it
// waits for the actor's detach in progress: the promote's duration, the wait
// included, is still recorded.
func TestPromoteSnapshotRecordsDetachWait(t *testing.T) {
	a := runPluginActor(t)
	startBlockedDetach(t, a)
	inst, reader := newTestInstruments(t)
	a.s.instruments = inst

	gaveUp, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := a.s.PromoteSnapshot(gaveUp, &ateletpb.PromoteSnapshotRequest{
		Atespace:              "ate-demo",
		ActorName:             "counter",
		ActorUid:              pluginActorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		Snapshot:              a.snapshot("pause-snap-1", 1),
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("PromoteSnapshot that gave up waiting for the detach = %v, want %v", err, context.Canceled)
	}
	if _, ok := phaseValues(t, collectHistogram(t, reader, checkpointDurationMetric))[ateattr.SnapshotPhaseTotal]; !ok {
		t.Error("no total duration recorded for the promote")
	}
}

// TestPromoteSnapshotFoundAlreadyUploaded promotes an asynchronously suspended
// actor's snapshot whose upload committed before the promote, unrecorded, and
// whose local copy is gone since: the promote records the snapshot as durable,
// without an upload time, since it wrote nothing.
func TestPromoteSnapshotFoundAlreadyUploaded(t *testing.T) {
	a := runPluginActor(t)
	snap := a.snapshot("pause-snap-1", 1)
	if _, err := a.checkpoint(t, snap, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if _, err := (&trackerStore{s: a.s}).UploadLocalSnapshot(t.Context(), pluginActorUID, snapshottracker.SnapshotRef{Snapshot: snap}); err != nil {
		t.Fatalf("UploadLocalSnapshot: %v", err)
	}
	if err := snapshottracker.PruneLocalCheckpoints(t.Context(), pluginActorUID); err != nil {
		t.Fatalf("PruneLocalCheckpoints: %v", err)
	}

	if got := a.promote(t, snap).GetLatestDurable(); !proto.Equal(got.GetSnapshot(), snap) || got.GetDurableCompleteTime() != nil {
		t.Errorf("latest_durable = %v, want %v with no upload time", got, snap)
	}
}

// TestPromoteSnapshotAfterDetach verifies that after an actor is detached
// (its entire directory removed), PromoteSnapshot still succeeds via its
// remote manifest check in object storage (with UploadLocalSnapshot returning
// uploaded = false).
func TestPromoteSnapshotAfterDetach(t *testing.T) {
	a := runPluginActor(t)
	store := &trackerStore{s: a.s}
	a.s.snapshots = snapshottracker.New(t.Context(), store, clock.RealClock{},
		snapshottracker.WithReporter(&trackerReporter{workers: newFakeReportService()}))
	a.s.snapshots.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := a.s.snapshots.Wait(ctx); err != nil {
			t.Errorf("Wait: %v", err)
		}
	})
	snap := a.snapshot("pause-snap-1", 1)
	if _, err := a.checkpoint(t, snap, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err != nil {
		t.Fatalf("Checkpoint (pause): %v", err)
	}

	// Detach uploads the snapshot and removes the actor directory once reported.
	st, err := a.s.snapshots.Detach(t.Context(), pluginActorUID)
	if err != nil {
		t.Fatalf("Detach: %v", err)
	}
	if st.GetLatestLocal() != nil || st.GetLatestDurable() == nil {
		t.Fatalf("state = %v, want durable set and local nil", st)
	}
	waitFor(t, "the actor dir to be removed after the detach report", func() bool {
		return !exists(ateletpath.ActorPath(pluginActorUID))
	})

	// TrackerStore.UploadLocalSnapshot after detach probes object storage and
	// reports uploaded = false.
	uploaded, err := store.UploadLocalSnapshot(t.Context(), pluginActorUID, snapshottracker.SnapshotRef{Snapshot: snap})
	if err != nil || uploaded {
		t.Fatalf("UploadLocalSnapshot after detach = (%v, %v), want (false, nil)", uploaded, err)
	}

	// PromoteSnapshot after detach finds the local snapshot gone, probes object
	// storage for the manifest uploaded during detach, and succeeds without
	// re-uploading.
	if got := a.promote(t, snap).GetLatestDurable(); !proto.Equal(got.GetSnapshot(), snap) || got.GetDurableCompleteTime() != nil {
		t.Errorf("latest_durable = %v, want %v with no upload time", got, snap)
	}
}

// testSandboxRecord is the record of a gVisor runtime whose only asset is
// release, as the syncer plans it from a SandboxConfig version.
func testSandboxRecord(release gvisorTestRelease) *sandboxAssetsRecord {
	return &sandboxAssetsRecord{
		SandboxClass: "gvisor",
		Assets:       map[string]assetEntry{gvisorAssetName: release.entry},
	}
}

// advertiseRuntime makes the runtime that rec describes one a worker on this
// node advertises, as the syncer would have after planning that worker's
// pod, so a Run or Restore carrying runtimeFor(rec) resolves. It gives s an
// index if it has none.
func advertiseRuntime(t *testing.T, s *Atelet, rec *sandboxAssetsRecord) {
	t.Helper()
	if s.runtimes == nil {
		s.runtimes = newRuntimeIndex()
	}
	hash := bundleHash(rec)
	s.runtimes.register("test-worker-pod", advertisedClass{
		class:  v1alpha1.SandboxClass(rec.SandboxClass),
		config: "test-config",
		hashed: []hashedVersion{{versionPlan: versionPlan{name: "v1", isDefault: true, rec: rec}, hash: hash}},
	})
}

// TestRunAndRestoreRejectUnadvertisedRuntime pins that an actor placed for a
// runtime no worker on this node advertises is refused by Run and Restore
// alike, before anything is fetched or staged, and that either request
// without a runtime is invalid.
func TestRunAndRestoreRejectUnadvertisedRuntime(t *testing.T) {
	useTempNodeDirs(t)
	const (
		actorUID     = "01234567-89ab-cdef-0123-456789abcdef"
		workerPodUID = "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f"
	)
	release := newGVisorTestRelease(t)
	var gets atomic.Int32
	s := &Atelet{
		runtimes:          newRuntimeIndex(),
		sandboxAssetCache: newTestAssetCache(t),
		anonGCSClient:     fakeObjectStorage{data: release.tarball, gets: &gets},
		systemInfoVolumes: newSystemInfoVolumeRefresher(nil, nil),
	}
	rec := testSandboxRecord(release)

	for _, tc := range []struct {
		name string
		call func(t *testing.T, rt *ateletpb.ActorSandboxRuntime) error
	}{{
		name: "Run",
		call: func(t *testing.T, rt *ateletpb.ActorSandboxRuntime) error {
			_, err := s.Run(t.Context(), &ateletpb.RunRequest{
				Atespace: "team-a", ActorName: "actor-1", ActorUid: actorUID, WorkerPodUid: workerPodUID,
				SandboxRuntime: rt, Spec: &ateletpb.WorkloadSpec{},
			})
			return err
		},
	}, {
		name: "Restore",
		call: func(t *testing.T, rt *ateletpb.ActorSandboxRuntime) error {
			_, err := s.Restore(t.Context(), &ateletpb.RestoreRequest{
				Atespace: "team-a", ActorName: "actor-1", ActorUid: actorUID, WorkerPodUid: workerPodUID,
				SandboxRuntime: rt, Spec: &ateletpb.WorkloadSpec{},
				Snapshot: lifecycleSnapshot(actorUID, "pause-snap-1", 1, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY),
			})
			return err
		},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call(t, runtimeFor(t, rec))
			if apierror.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "not advertised") {
				t.Fatalf("%s for an unadvertised runtime = %v (code %s), want %s naming the runtime", tc.name, err, apierror.Code(err), codes.FailedPrecondition)
			}
			if gets.Load() != 0 {
				t.Errorf("assets fetched %d times for a refused %s, want 0", gets.Load(), tc.name)
			}
			if _, err := os.Lstat(ateletpath.ActorPath(actorUID)); !os.IsNotExist(err) {
				t.Errorf("refused %s created the actor dir (lstat err = %v)", tc.name, err)
			}

			// A request that names no runtime is invalid.
			if err := tc.call(t, nil); apierror.Code(err) != codes.InvalidArgument {
				t.Errorf("%s without sandbox_runtime = %v (code %s), want %s", tc.name, err, apierror.Code(err), codes.InvalidArgument)
			}
		})
	}

	// Once a worker advertises it, the same runtime resolves.
	advertiseRuntime(t, s, rec)
	if _, err := s.advertisedRuntime(t.Context(), runtimeFor(t, rec)); err != nil {
		t.Errorf("advertisedRuntime after advertising: %v", err)
	}
}

// runtimeFor returns the ActorSandboxRuntime ate-api-server sends for an actor
// placed on a worker that advertises the runtime rec describes: the worker's
// registration, whose sandbox_bundle_hash is the hash of rec.
func runtimeFor(t *testing.T, rec *sandboxAssetsRecord) *ateletpb.ActorSandboxRuntime {
	t.Helper()
	hash := bundleHash(rec)
	return &ateletpb.ActorSandboxRuntime{
		SandboxClass: rec.SandboxClass,
		Version: &ateletpb.VersionedSandboxCompat{
			SchemaVersion: hardware.SchemaVersionV1,
			Attributes: []*ateletpb.AttributeEntry{
				{Key: hardware.AttrArchitecture, Value: runtime.GOARCH},
				{Key: hardware.AttrSandboxBundleHash, Value: hash},
			},
		},
	}
}

type gatingTrackerStore struct {
	inner snapshottracker.Store
	// objects, if set, fails every upload after the first gated one.
	objects *recordingObjectStorage
	started chan struct{}
	release chan struct{}
	// canceled, if set, receives once an upload's context ends, and that
	// upload still waits for release before it returns, as an object-store
	// call that is finishing would.
	canceled chan struct{}
}

func (s *gatingTrackerStore) UploadLocalSnapshot(ctx context.Context, actorUID string, ref snapshottracker.SnapshotRef) (bool, error) {
	s.started <- struct{}{}
	select {
	case <-s.release:
	case <-ctx.Done():
		if s.canceled != nil {
			s.canceled <- struct{}{}
			<-s.release
		}
		return false, ctx.Err()
	}
	uploaded, err := s.inner.UploadLocalSnapshot(ctx, actorUID, ref)
	if s.objects == nil {
		return uploaded, err
	}
	// Fail any second upload attempt so PromoteSnapshot proves it skips
	// re-uploading once the background upload made the snapshot durable.
	s.objects.mu.Lock()
	delete(s.objects.objects, manifestKey(ref.Snapshot.GetSnapshotUuid()))
	s.objects.putErr = errors.New("promote re-uploaded a snapshot already uploaded in the background")
	s.objects.mu.Unlock()
	return uploaded, err
}

// stepToUpload advances clk to due and waits for the background upload to
// start. The tracker's queue arms its timer from a clock reading taken just
// before, so a step that lands in between leaves the timer late; further
// one-second steps get past it.
func stepToUpload(t *testing.T, clk *clocktesting.FakeClock, due time.Time, started <-chan struct{}) {
	t.Helper()
	clk.Step(due.Sub(clk.Now()))
	waitFor(t, "the background upload to start", func() bool {
		select {
		case <-started:
			return true
		case <-time.After(10 * time.Millisecond):
			clk.Step(time.Second)
			return false
		}
	})
}

// TestPromoteSnapshotWaitsForBackgroundUpload promotes a snapshot while its
// background upload is in flight: PromoteSnapshot waits for the background
// upload to finish and does not upload the snapshot a second time.
func TestPromoteSnapshotWaitsForBackgroundUpload(t *testing.T) {
	a := runPluginActor(t)
	snap := a.snapshot("pause-snap-1", 1)
	clk := clocktesting.NewFakeClock(time.Now())
	store := &gatingTrackerStore{
		inner:   &trackerStore{s: a.s},
		objects: a.store,
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	a.s.snapshots = snapshottracker.New(t.Context(), store, clk,
		snapshottracker.WithUploadDelay(time.Second),
		snapshottracker.WithDetachDelay(time.Hour))
	a.s.snapshots.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := a.s.snapshots.Wait(ctx); err != nil {
			t.Errorf("Wait: %v", err)
		}
	})

	cpResp, err := a.checkpoint(t, snap, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	stepToUpload(t, clk, cpResp.GetState().GetUploadDueTime().AsTime(), store.started)

	type promoteResult struct {
		state *ateletpb.ActorSnapshotState
		err   error
	}
	resCh := make(chan promoteResult, 1)
	go func() {
		resp, err := a.s.PromoteSnapshot(t.Context(), &ateletpb.PromoteSnapshotRequest{
			Atespace:              "ate-demo",
			ActorName:             "counter",
			ActorUid:              pluginActorUID,
			ActorTemplateAtespace: "default",
			ActorTemplateName:     "counter",
			Snapshot:              snap,
		})
		resCh <- promoteResult{resp.GetState(), err}
	}()
	select {
	case res := <-resCh:
		t.Fatalf("PromoteSnapshot returned (%v, %v) while background upload was in progress", res.state, res.err)
	case <-time.After(50 * time.Millisecond):
	}

	close(store.release)
	res := <-resCh
	if res.err != nil {
		t.Fatalf("PromoteSnapshot: %v", res.err)
	}
	if got := res.state.GetLatestDurable(); !proto.Equal(got.GetSnapshot(), snap) || got.GetDurableCompleteTime() == nil {
		t.Errorf("latest_durable = %v, want %v with the background upload's time", got, snap)
	}
}

// TestCheckpointKeepsSnapshotBeingUploaded checkpoints a resumed actor locally
// while the background upload of its previous local snapshot still runs: the
// checkpoint's prune keeps that snapshot for the upload, which deletes it once
// it ends.
func TestCheckpointKeepsSnapshotBeingUploaded(t *testing.T) {
	a := runPluginActor(t)
	clk := clocktesting.NewFakeClock(time.Now())
	store := &gatingTrackerStore{
		inner:   &trackerStore{s: a.s},
		objects: a.store,
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	a.s.snapshots = snapshottracker.New(t.Context(), store, clk,
		snapshottracker.WithUploadDelay(time.Second),
		snapshottracker.WithDetachDelay(time.Hour))
	a.s.snapshots.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := a.s.snapshots.Wait(ctx); err != nil {
			t.Errorf("Wait: %v", err)
		}
	})

	cpResp, err := a.checkpoint(t, a.snapshot("pause-snap-1", 1), ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
	if err != nil {
		t.Fatalf("Checkpoint (pause-snap-1): %v", err)
	}
	stepToUpload(t, clk, cpResp.GetState().GetUploadDueTime().AsTime(), store.started)

	a.restoreLocal(t, "pause-snap-1", 1)
	if _, err := a.checkpoint(t, a.snapshot("pause-snap-2", 2), ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err != nil {
		t.Fatalf("Checkpoint (pause-snap-2): %v", err)
	}
	snap1Dir := ateletpath.LocalSnapshotDir(pluginActorUID, "pause-snap-1")
	if _, err := os.Stat(snap1Dir); err != nil {
		t.Fatalf("pause-snap-1 during its upload: stat = %v, want it kept", err)
	}

	close(store.release)
	waitFor(t, "pause-snap-1 to be deleted once its upload ends", func() bool {
		_, err := os.Stat(snap1Dir)
		return os.IsNotExist(err)
	})
	if _, err := os.Stat(ateletpath.LocalSnapshotDir(pluginActorUID, "pause-snap-2")); err != nil {
		t.Errorf("pause-snap-2 after the upload of pause-snap-1: stat = %v, want it kept", err)
	}
}

// TestImmediateCheckpointCancelsBackgroundUpload suspends a resumed actor with
// an immediate upload while the background upload of its previous local
// snapshot still runs: the checkpoint checkpoints the worker without waiting
// for that upload, then cancels it, and returns only once it has stopped, so
// the control plane can delete the older snapshot's objects.
func TestImmediateCheckpointCancelsBackgroundUpload(t *testing.T) {
	a := runPluginActor(t)
	clk := clocktesting.NewFakeClock(time.Now())
	store := &gatingTrackerStore{
		inner:    &trackerStore{s: a.s},
		started:  make(chan struct{}, 1),
		release:  make(chan struct{}),
		canceled: make(chan struct{}, 1),
	}
	a.s.snapshots = snapshottracker.New(t.Context(), store, clk,
		snapshottracker.WithUploadDelay(time.Second),
		snapshottracker.WithDetachDelay(time.Hour))
	a.s.snapshots.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := a.s.snapshots.Wait(ctx); err != nil {
			t.Errorf("Wait: %v", err)
		}
	})
	// A canceled upload holds on until released; release it before Wait
	// even when the test stops early.
	release := sync.OnceFunc(func() { close(store.release) })
	t.Cleanup(release)

	cpResp, err := a.checkpoint(t, a.snapshot("pause-snap-1", 1), ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
	if err != nil {
		t.Fatalf("Checkpoint (pause-snap-1): %v", err)
	}
	stepToUpload(t, clk, cpResp.GetState().GetUploadDueTime().AsTime(), store.started)
	a.restoreLocal(t, "pause-snap-1", 1)
	a.worker.mu.Lock()
	delete(a.worker.actorDirs, "CheckpointWorkload")
	a.worker.mu.Unlock()

	type checkpointResult struct {
		state *ateletpb.ActorSnapshotState
		err   error
	}
	resCh := make(chan checkpointResult, 1)
	go func() {
		resp, err := a.checkpoint(t, a.snapshot("suspend-snap-2", 2), ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE)
		resCh <- checkpointResult{resp.GetState(), err}
	}()
	select {
	case <-store.canceled:
	case res := <-resCh:
		t.Fatalf("immediate Checkpoint returned (%v, %v) without canceling the background upload", res.state, res.err)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the immediate Checkpoint to cancel the background upload")
	}
	a.worker.mu.Lock()
	_, checkpointed := a.worker.actorDirs["CheckpointWorkload"]
	a.worker.mu.Unlock()
	if !checkpointed {
		t.Error("background upload canceled before the worker was checkpointed, want the checkpoint not to wait for it")
	}
	select {
	case res := <-resCh:
		t.Fatalf("immediate Checkpoint returned (%v, %v) before the canceled upload stopped", res.state, res.err)
	case <-time.After(50 * time.Millisecond):
	}

	release()
	var res checkpointResult
	select {
	case res = <-resCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the immediate Checkpoint once the canceled upload stopped")
	}
	if res.err != nil {
		t.Fatalf("Checkpoint (suspend-snap-2): %v", res.err)
	}
	if got := res.state.GetLatestDurable().GetSnapshot().GetSnapshotUuid(); got != "suspend-snap-2" {
		t.Errorf("latest_durable = %q, want suspend-snap-2", got)
	}
	if res.state.GetUploading() != nil {
		t.Errorf("uploading = %v, want no upload in progress once the checkpoint returns", res.state.GetUploading())
	}
}

// resumedWithPendingUpload runs a plugin actor, checkpoints it into pause-snap-1
// with a deferred upload, and resumes it from that snapshot's local copy
// before the upload is due. It returns the actor, its gated tracker store, and
// a func that steps the clock past the upload's due time and reports whether
// the upload starts within a moment.
func resumedWithPendingUpload(t *testing.T) (*pluginActor, *gatingTrackerStore, func() bool) {
	t.Helper()
	a := runPluginActor(t)
	clk := clocktesting.NewFakeClock(time.Now())
	store := &gatingTrackerStore{
		inner:   &trackerStore{s: a.s},
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	a.s.snapshots = snapshottracker.New(t.Context(), store, clk,
		snapshottracker.WithUploadDelay(time.Second),
		snapshottracker.WithDetachDelay(time.Hour))
	a.s.snapshots.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := a.s.snapshots.Wait(ctx); err != nil {
			t.Errorf("Wait: %v", err)
		}
	})
	// An upload that starts holds on until released; release it before Wait.
	t.Cleanup(func() { close(store.release) })

	cpResp, err := a.checkpoint(t, a.snapshot("pause-snap-1", 1), ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
	if err != nil {
		t.Fatalf("Checkpoint (pause-snap-1): %v", err)
	}
	due := cpResp.GetState().GetUploadDueTime().AsTime()
	a.restoreLocal(t, "pause-snap-1", 1)
	// Step on in one-second steps past the due time, in case the queue's timer
	// was armed late (see stepToUpload).
	stepPastDue := func() bool {
		clk.Step(due.Sub(clk.Now()))
		for range 5 {
			select {
			case <-store.started:
				return true
			case <-time.After(20 * time.Millisecond):
				clk.Step(time.Second)
			}
		}
		return false
	}
	return a, store, stepPastDue
}

// TestImmediateCheckpointHoldsOffBackgroundUpload checkpoints an actor resumed
// from its local snapshot while that snapshot's background upload comes due:
// under UPLOAD_MODE_IMMEDIATE the upload does not start, since the checkpoint
// would only cancel it, while under UPLOAD_MODE_DEFERRED it starts.
func TestImmediateCheckpointHoldsOffBackgroundUpload(t *testing.T) {
	for _, tc := range []struct {
		mode        ateletpb.UploadMode
		snapshot    string
		wantStarted bool
	}{
		{mode: ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE, snapshot: "suspend-snap-2"},
		{mode: ateletpb.UploadMode_UPLOAD_MODE_DEFERRED, snapshot: "pause-snap-2", wantStarted: true},
	} {
		t.Run(tc.mode.String(), func(t *testing.T) {
			a, _, stepPastDue := resumedWithPendingUpload(t)
			var started atomic.Bool
			a.worker.onCheckpoint = func() { started.Store(stepPastDue()) }

			if _, err := a.checkpoint(t, a.snapshot(tc.snapshot, 2), tc.mode); err != nil {
				t.Fatalf("Checkpoint (%s): %v", tc.snapshot, err)
			}
			if got := started.Load(); got != tc.wantStarted {
				t.Errorf("background upload of pause-snap-1 started during the checkpoint = %v, want %v", got, tc.wantStarted)
			}
		})
	}
}

// TestTerminateHoldsOffBackgroundUpload terminates an actor resumed from its
// local snapshot, whose background upload comes due while the worker tears the
// actor down: the upload does not start, since Terminate would only cancel it.
// When the worker fails to tear the actor down, Terminate fails before it
// forgets the actor, and the upload, still due, starts once Terminate returns.
func TestTerminateHoldsOffBackgroundUpload(t *testing.T) {
	for _, tc := range []struct {
		name string
		// teardownErr is what the worker's TerminateWorkload fails with.
		teardownErr error
	}{
		{name: "Terminate succeeds"},
		{name: "Terminate fails", teardownErr: status.Error(codes.Internal, "teardown failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, store, stepPastDue := resumedWithPendingUpload(t)
			var started atomic.Bool
			a.worker.onTerminate = func() error {
				started.Store(stepPastDue())
				return tc.teardownErr
			}

			_, err := a.s.Terminate(t.Context(), &ateletpb.TerminateRequest{
				Atespace:              "ate-demo",
				ActorName:             "counter",
				ActorUid:              pluginActorUID,
				ActorTemplateAtespace: "default",
				ActorTemplateName:     "counter",
				WorkerPodUid:          pluginWorkerPodUID,
				Spec:                  a.spec,
			})
			switch {
			case tc.teardownErr == nil && err != nil:
				t.Fatalf("Terminate: %v", err)
			case tc.teardownErr != nil && err == nil:
				t.Fatal("Terminate succeeded, want it to fail with the worker's teardown")
			}
			if started.Load() {
				t.Error("background upload of pause-snap-1 started during Terminate, want none")
			}
			if tc.teardownErr == nil {
				select {
				case <-store.started:
					t.Error("background upload of pause-snap-1 started during Terminate, want none")
				default:
				}
				return
			}
			select {
			case <-store.started:
			case <-time.After(10 * time.Second):
				t.Fatal("background upload of pause-snap-1 did not start once Terminate failed")
			}
		})
	}
}

// TestCheckpointRejectsStaleGeneration checks that Checkpoint fails with
// Aborted when given a generation below the actor's newest snapshot
// or equal to it under another snapshot_uuid, before calling worker, pruning
// local checkpoints, or changing tracked state.
func TestCheckpointRejectsStaleGeneration(t *testing.T) {
	a := runPluginActor(t)
	snap2 := a.snapshot("pause-snap-2", 2)
	resp, err := a.checkpoint(t, snap2, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	wantState := resp.GetState()
	a.restoreLocal(t, "pause-snap-2", 2)
	delete(a.worker.actorDirs, "CheckpointWorkload")

	for _, stale := range []*ateletpb.Snapshot{
		a.snapshot("stale-snap-1", 1),
		a.snapshot("other-snap-2", 2),
	} {
		_, err := ateinterceptors.InternalServerUnaryInterceptor(t.Context(), nil,
			&grpc.UnaryServerInfo{FullMethod: ateletpb.Atelet_Checkpoint_FullMethodName},
			func(context.Context, any) (any, error) {
				return a.checkpoint(t, stale, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
			})
		if code := status.Code(err); code != codes.Aborted {
			t.Errorf("Checkpoint(%v) error = %v (code %v), want Aborted", stale, err, code)
		}
		if got := a.worker.actorDirs["CheckpointWorkload"]; got != nil {
			t.Errorf("Checkpoint(%v) called worker.CheckpointWorkload (%v), want rejected before worker", stale, got)
		}
		if _, err := os.Stat(ateletpath.LocalSnapshotDir(pluginActorUID, "pause-snap-2")); err != nil {
			t.Errorf("local snapshot pause-snap-2 after rejected Checkpoint(%v): stat = %v, want preserved", stale, err)
		}
		if got := a.s.snapshots.State(pluginActorUID); !proto.Equal(got, wantState) {
			t.Errorf("tracked state after rejected Checkpoint(%v) = %v, want %v", stale, got, wantState)
		}
	}
}

// TestPromoteSnapshotRejectsStaleGeneration checks that PromoteSnapshot fails
// with Aborted when given a generation below the actor's newest
// snapshot or equal to it under another snapshot_uuid, before uploading to
// object storage, pruning local checkpoints, or changing tracked state.
func TestPromoteSnapshotRejectsStaleGeneration(t *testing.T) {
	a := runPluginActor(t)
	snap2 := a.snapshot("pause-snap-2", 2)
	resp, err := a.checkpoint(t, snap2, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	wantState := resp.GetState()

	for _, stale := range []*ateletpb.Snapshot{
		a.snapshot("stale-snap-1", 1),
		a.snapshot("other-snap-2", 2),
	} {
		writeLocalSnapshot(t, ateletpath.LocalSnapshotDir(pluginActorUID, stale.GetSnapshotUuid()), sandboxAssetsRecord{
			SandboxClass:  "gvisor",
			SnapshotFiles: []string{"checkpoint.img"},
			Fidelity:      "MEMORY",
		}, map[string]string{"checkpoint.img": "stale-memory"})

		req := &ateletpb.PromoteSnapshotRequest{
			Atespace:              "ate-demo",
			ActorName:             "counter",
			ActorUid:              pluginActorUID,
			ActorTemplateAtespace: "default",
			ActorTemplateName:     "counter",
			Snapshot:              stale,
		}
		_, err := ateinterceptors.InternalServerUnaryInterceptor(t.Context(), req,
			&grpc.UnaryServerInfo{FullMethod: ateletpb.Atelet_PromoteSnapshot_FullMethodName},
			func(ctx context.Context, _ any) (any, error) { return a.s.PromoteSnapshot(ctx, req) })
		if code := status.Code(err); code != codes.Aborted {
			t.Errorf("PromoteSnapshot(%v) error = %v (code %v), want Aborted", stale, err, code)
		}
		if got := a.store.keys(); len(got) != 0 {
			t.Errorf("objects after rejected PromoteSnapshot(%v) = %v, want nothing uploaded", stale, got)
		}
		if _, err := os.Stat(ateletpath.LocalSnapshotDir(pluginActorUID, "pause-snap-2")); err != nil {
			t.Errorf("local snapshot pause-snap-2 after rejected PromoteSnapshot(%v): stat = %v, want preserved", stale, err)
		}
		if got := a.s.snapshots.State(pluginActorUID); !proto.Equal(got, wantState) {
			t.Errorf("tracked state after rejected PromoteSnapshot(%v) = %v, want %v", stale, got, wantState)
		}
	}
}

// TestCheckpointInFlightRejectsOlderGeneration checks that while Checkpoint is
// running on the worker for generation 3, an out-of-order Checkpoint or
// PromoteSnapshot for generation 2 waits on the per-actor RPC lock and then
// fails with Aborted.
func TestCheckpointInFlightRejectsOlderGeneration(t *testing.T) {
	a := runPluginActor(t)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	a.worker.onCheckpoint = func() {
		once.Do(func() {
			close(started)
			<-release
		})
	}

	cpErr := make(chan error, 1)
	go func() {
		_, err := a.checkpoint(t, a.snapshot("pause-snap-3", 3), ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
		cpErr <- err
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for Checkpoint(pause-snap-3) to reach worker")
	}

	stale := a.snapshot("pause-snap-2", 2)
	staleCpErr := make(chan error, 1)
	go func() {
		_, err := a.checkpoint(t, stale, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
		staleCpErr <- err
	}()
	stalePromoteErr := make(chan error, 1)
	go func() {
		_, err := a.s.PromoteSnapshot(t.Context(), &ateletpb.PromoteSnapshotRequest{
			Atespace:              "ate-demo",
			ActorName:             "counter",
			ActorUid:              pluginActorUID,
			ActorTemplateAtespace: "default",
			ActorTemplateName:     "counter",
			Snapshot:              stale,
		})
		stalePromoteErr <- err
	}()
	select {
	case err := <-staleCpErr:
		t.Fatalf("Checkpoint(%v) returned %v while generation 3 was in flight, want it to wait", stale, err)
	case err := <-stalePromoteErr:
		t.Fatalf("PromoteSnapshot(%v) returned %v while generation 3 was in flight, want it to wait", stale, err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if err := <-cpErr; err != nil {
		t.Fatalf("Checkpoint(pause-snap-3): %v", err)
	}
	if err := <-staleCpErr; apierror.Code(err) != codes.Aborted {
		t.Errorf("Checkpoint(%v) after generation 3 committed = %v, want Aborted", stale, err)
	}
	if err := <-stalePromoteErr; apierror.Code(err) != codes.Aborted {
		t.Errorf("PromoteSnapshot(%v) after generation 3 committed = %v, want Aborted", stale, err)
	}
	if got := a.s.snapshots.State(pluginActorUID).GetLatestLocal().GetSnapshot().GetSnapshotUuid(); got != "pause-snap-3" {
		t.Errorf("latest_local = %q, want pause-snap-3", got)
	}
}

// TestCheckpointSkipsStaleCommitAndKeepsNewerLocal checks that when a stalled
// Checkpoint(gen=2) reaches CommitLocal and PruneLocalCheckpoints after a newer
// snapshot (gen=3) has already committed locally, gen=3 stays tracked and its
// local directory stays on disk while gen=2's local copy is pruned.
func TestCheckpointSkipsStaleCommitAndKeepsNewerLocal(t *testing.T) {
	a := runPluginActor(t)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	a.worker.onCheckpoint = func() {
		once.Do(func() {
			close(started)
			<-release
		})
	}

	type cpResult struct {
		resp *ateletpb.CheckpointResponse
		err  error
	}
	resCh := make(chan cpResult, 1)
	go func() {
		resp, err := a.checkpoint(t, a.snapshot("pause-snap-2", 2), ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
		resCh <- cpResult{resp, err}
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for Checkpoint(pause-snap-2) to reach worker")
	}

	snap3 := a.snapshot("pause-snap-3", 3)
	snap3Dir := ateletpath.LocalSnapshotDir(pluginActorUID, snap3.GetSnapshotUuid())
	writeLocalSnapshot(t, snap3Dir, sandboxAssetsRecord{
		SandboxClass:  "gvisor",
		SnapshotFiles: []string{"checkpoint.img"},
		Fidelity:      "MEMORY",
	}, map[string]string{"checkpoint.img": "newer-memory"})
	wantState := a.s.snapshots.CommitLocal(pluginActorUID, snapshottracker.SnapshotRef{
		Snapshot:   snap3,
		CommitTime: time.Now(),
	})

	close(release)
	res := <-resCh
	if res.err != nil {
		t.Fatalf("Checkpoint(pause-snap-2): %v", res.err)
	}
	if !proto.Equal(res.resp.GetState(), wantState) {
		t.Errorf("Checkpoint(pause-snap-2) state = %v, want %v (keeping pause-snap-3)", res.resp.GetState(), wantState)
	}
	if _, err := os.Stat(snap3Dir); err != nil {
		t.Errorf("newer local snapshot pause-snap-3: stat = %v, want kept", err)
	}
	if _, err := os.Stat(ateletpath.LocalSnapshotDir(pluginActorUID, "pause-snap-2")); !os.IsNotExist(err) {
		t.Errorf("stale local snapshot pause-snap-2: stat = %v, want pruned", err)
	}
}

func TestWorkerRestoreError(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(t.Context())
	cancel()

	for _, tc := range []struct {
		name     string
		ctx      context.Context
		err      error
		wantCode codes.Code
	}{
		{
			name:     "worker Unavailable maps to apierror.Unavailable",
			ctx:      t.Context(),
			err:      status.Error(codes.Unavailable, "while reseeding guest CRNG: context deadline exceeded"),
			wantCode: codes.Unavailable,
		},
		{
			// The worker reports any step's internal timeout this way, not
			// only one before the workload served traffic.
			name:     "worker DeadlineExceeded with live caller context maps to codes.Internal",
			ctx:      t.Context(),
			err:      status.Error(codes.DeadlineExceeded, "context deadline exceeded"),
			wantCode: codes.Internal,
		},
		{
			name:     "worker Canceled with live caller context maps to codes.Internal",
			ctx:      t.Context(),
			err:      status.Error(codes.Canceled, "context canceled"),
			wantCode: codes.Internal,
		},
		{
			name:     "worker Internal maps to codes.Internal",
			ctx:      t.Context(),
			err:      status.Error(codes.Internal, "corrupt snapshot config"),
			wantCode: codes.Internal,
		},
		{
			name:     "caller context canceled reports Canceled",
			ctx:      canceledCtx,
			err:      status.Error(codes.Internal, "worker aborted"),
			wantCode: codes.Canceled,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := workerRestoreError(tc.ctx, tc.err)
			if code := apierror.Code(got); code != tc.wantCode {
				t.Errorf("apierror.Code(workerRestoreError()) = %v, want %v (err: %v)", code, tc.wantCode, got)
			}
			if !errors.Is(got, tc.err) {
				t.Errorf("workerRestoreError() = %v, want it to wrap %v", got, tc.err)
			}
		})
	}
}

// withExternalVolumes gives the actor's spec the external volumes ids, served
// by a fake volume plugin, and returns the plugin.
func (a *pluginActor) withExternalVolumes(ids ...string) *fakeWorkerPlugin {
	volPlugin := &fakeWorkerPlugin{}
	a.s.volumePlugins = map[string]volume.VolumePluginWorkerPlane{
		"mock-driver": volPlugin,
	}
	a.spec.Volumes = nil
	for _, id := range ids {
		a.spec.Volumes = append(a.spec.Volumes, &ateletpb.Volume{
			Name: "ext-" + id,
			External: &ateletpb.ExternalVolumeSource{
				StorageVolumeId: id,
				VolumeType:      "mock-driver",
			},
		})
	}
	return volPlugin
}

// restoreRequest is the control plane's request to restore the actor from
// snap.
func (a *pluginActor) restoreRequest(t *testing.T, snap *ateletpb.Snapshot) *ateletpb.RestoreRequest {
	t.Helper()
	return &ateletpb.RestoreRequest{
		Atespace:              "ate-demo",
		ActorName:             "counter",
		ActorUid:              pluginActorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          pluginWorkerPodUID,
		SandboxRuntime:        runtimeFor(t, a.rec),
		Spec:                  a.spec,
		Snapshot:              snap,
	}
}

// checkpointed checkpoints the actor as snapshot name and returns the
// snapshot.
func (a *pluginActor) checkpointed(t *testing.T, name string, generation int32) *ateletpb.Snapshot {
	t.Helper()
	snap := a.snapshot(name, generation)
	if _, err := a.checkpoint(t, snap, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	return snap
}

var errWorkerGuestNotReady = status.Error(codes.Unavailable, "while reseeding guest CRNG: context deadline exceeded")

func TestRestoreStopsRetryingAfterConsecutiveUnavailableFailures(t *testing.T) {
	a := runPluginActor(t)
	snap := a.checkpointed(t, "pause-snap-1", 1)
	restoreReq := a.restoreRequest(t, snap)
	restoreCode := func() codes.Code {
		_, err := a.s.Restore(t.Context(), restoreReq)
		return apierror.Code(err)
	}
	failRetryably := func(times int, after string) {
		t.Helper()
		a.worker.restoreErr = errWorkerGuestNotReady
		for i := 1; i <= times; i++ {
			if got := restoreCode(); got != codes.Unavailable {
				t.Fatalf("Restore %d %s: code = %v, want %v", i, after, got, codes.Unavailable)
			}
		}
	}

	failRetryably(maxConsecutiveRetryableRestoreFailures-1, "at first")
	if got := restoreCode(); got != codes.Internal {
		t.Fatalf("Restore %d code = %v, want %v", maxConsecutiveRetryableRestoreFailures, got, codes.Internal)
	}

	// Giving up starts the count over, and so does a successful restore.
	failRetryably(maxConsecutiveRetryableRestoreFailures-1, "after giving up")
	a.worker.restoreErr = nil
	if got := restoreCode(); got != codes.OK {
		t.Fatalf("successful Restore code = %v, want %v", got, codes.OK)
	}
	failRetryably(maxConsecutiveRetryableRestoreFailures-1, "after a success")

	// Restoring another snapshot starts a new streak.
	restoreReq = a.restoreRequest(t, a.checkpointed(t, "pause-snap-2", 2))
	failRetryably(maxConsecutiveRetryableRestoreFailures-1, "of another snapshot")

	// So does a failure that crashes the actor: the control plane's crash
	// handling does not call Terminate, so nothing else would clear the count
	// before the actor is restored here again.
	a.worker.restoreErr = status.Error(codes.Internal, "corrupt snapshot config")
	if got := restoreCode(); got != codes.Internal {
		t.Fatalf("Restore with a worker Internal error: code = %v, want %v", got, codes.Internal)
	}
	failRetryably(maxConsecutiveRetryableRestoreFailures-1, "after a crash")

	// So does a failure in atelet's own steps before the worker call, here a
	// volume that does not mount.
	volPlugin := a.withExternalVolumes("vol-1")
	volPlugin.mountErr = errors.New("attach failed")
	if got := restoreCode(); got != codes.Internal {
		t.Fatalf("Restore with a failing volume mount: code = %v, want %v", got, codes.Internal)
	}
	volPlugin.mountErr = nil
	failRetryably(maxConsecutiveRetryableRestoreFailures-1, "after a failed mount")

	// An attempt atelet stopped waiting on leaves the actor RESUMING, and the
	// count as is, so the next retryable failure is the last.
	ctx, cancel := context.WithCancel(t.Context())
	a.worker.onRestore = cancel
	_, err := a.s.Restore(ctx, restoreReq)
	if got := apierror.Code(err); got != codes.Canceled {
		t.Fatalf("Restore that atelet stopped waiting on: code = %v, want %v (err: %v)", got, codes.Canceled, err)
	}
	a.worker.onRestore = nil
	if got := restoreCode(); got != codes.Internal {
		t.Fatalf("Restore after a canceled attempt: code = %v, want %v", got, codes.Internal)
	}
}

// TestCheckpointManifestBootID checks that Checkpoint stamps the node's boot ID
// into the local manifest only, omitting it from the uploaded manifest.
func TestCheckpointManifestBootID(t *testing.T) {
	a := runPluginActor(t)
	a.s.bootID = "boot-1"
	snap := a.snapshot("suspend-snap-1", 1)

	if _, err := a.checkpoint(t, snap, ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	localBytes, err := os.ReadFile(filepath.Join(ateletpath.LocalSnapshotDir(pluginActorUID, "suspend-snap-1"), sandboxManifestName))
	if err != nil {
		t.Fatalf("reading local manifest: %v", err)
	}
	localRec, err := unmarshalSandboxRecord(localBytes)
	if err != nil {
		t.Fatalf("unmarshalling local manifest: %v", err)
	}
	if localRec.BootID != "boot-1" {
		t.Errorf("local manifest bootId = %q, want %q", localRec.BootID, "boot-1")
	}

	remoteBytes := a.store.objects[manifestKey("suspend-snap-1")]
	if len(remoteBytes) == 0 {
		t.Fatal("no remote manifest uploaded")
	}
	if strings.Contains(string(remoteBytes), `"bootId"`) {
		t.Errorf("remote manifest %s carries bootId, want none", remoteBytes)
	}

	localDir := ateletpath.LocalSnapshotDir(pluginActorUID, "suspend-snap-1")
	noWorkers := []snapshottracker.Option{
		snapshottracker.WithUploadWorkers(0),
		snapshottracker.WithDetachWorkers(0),
		snapshottracker.WithReportWorkers(0),
	}
	sameBoot := snapshottracker.New(t.Context(), nil, clock.RealClock{}, append(noWorkers, snapshottracker.WithBootCheck(localSnapshotBootCheck("boot-1")))...)
	sameBoot.Start()
	if _, err := os.Stat(localDir); err != nil {
		t.Fatalf("local snapshot after restart on same boot: stat = %v, want kept", err)
	}
	if got := sameBoot.State(pluginActorUID).GetLatestLocal().GetSnapshot().GetSnapshotUuid(); got != "suspend-snap-1" {
		t.Errorf("latest_local after restart on same boot = %q, want %q", got, "suspend-snap-1")
	}

	newBoot := snapshottracker.New(t.Context(), nil, clock.RealClock{}, append(noWorkers, snapshottracker.WithBootCheck(localSnapshotBootCheck("boot-2")))...)
	newBoot.Start()
	if _, err := os.Stat(localDir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("local snapshot after restart on new boot: stat = %v, want not exist", err)
	}
	if got := newBoot.State(pluginActorUID).GetLatestLocal(); got != nil {
		t.Errorf("latest_local after restart on new boot = %v, want nil", got)
	}
	if got := newBoot.State(pluginActorUID).GetLatestDurable().GetSnapshot().GetSnapshotUuid(); got != "suspend-snap-1" {
		t.Errorf("latest_durable after restart on new boot = %q, want %q", got, "suspend-snap-1")
	}
}

// TestRestoreLocalSnapshotFromEarlierBoot checks that Restore never reads a
// node-local snapshot whose manifest carries a boot ID from before the node
// last rebooted: it restores from object storage when an uploaded copy
// exists, and fails with NotFound wrapping errUnusableLocalSnapshot otherwise.
func TestRestoreLocalSnapshotFromEarlierBoot(t *testing.T) {
	const (
		atespace     = "ate-demo"
		actorName    = "counter"
		actorUID     = "01234567-89ab-cdef-0123-456789abcdef"
		workerPodUID = "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f"
		snapshotName = "pause-snap-1"
	)

	worker := &fakeWorker{snapshotFiles: map[string]string{"checkpoint.img": ""}}
	serveFakeWorker(t, worker)

	host := imageVolumeTestRegistry(t)
	pinnedImage := pushTestImage(t, host+"/actor:v1", singleFileLayer(t, "bin/app", "app"))

	release := newGVisorTestRelease(t)
	sandboxRec := testSandboxRecord(release)
	spec := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{{Name: "app", Image: pinnedImage, Command: []string{"/bin/app"}}},
	}
	baseManifest := func() sandboxAssetsRecord {
		return sandboxAssetsRecord{
			SandboxClass:  "gvisor",
			Assets:        map[string]assetEntry{gvisorAssetName: release.entry},
			SnapshotFiles: []string{"checkpoint.img"},
			BootID:        "boot-1",
		}
	}

	newHerder := func(t *testing.T, objStore *recordingObjectStorage) *Atelet {
		t.Helper()
		s := newPluginHerder(t, objStore)
		s.bootID = "boot-2"
		s.workerDialer = newWorkerDialer(1)
		s.imageCache = newImageVolumeStore(t)
		s.anonGCSClient = fakeObjectStorage{data: release.tarball}
		s.systemInfoVolumes = newSystemInfoVolumeRefresher(nil, nil)
		advertiseRuntime(t, s, sandboxRec)
		return s
	}

	restoreReq := &ateletpb.RestoreRequest{
		Atespace:              atespace,
		ActorName:             actorName,
		ActorUid:              actorUID,
		ActorTemplateAtespace: "default",
		ActorTemplateName:     "counter",
		WorkerPodUid:          workerPodUID,
		SandboxRuntime:        runtimeFor(t, sandboxRec),
		Spec:                  spec,
		Snapshot: &ateletpb.Snapshot{
			SnapshotUuid: snapshotName,
			Generation:   1,
			Fidelity:     ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			Object:       &ateletpb.ObjectStorage{SnapshotUri: pausedSnapshotURI},
		},
	}

	t.Run("uploaded copy exists restores from object storage", func(t *testing.T) {
		useTempNodeDirs(t)
		objStore := &recordingObjectStorage{}
		s := newHerder(t, objStore)

		uri, err := resources.ParseSnapshotURI(pausedSnapshotURI)
		if err != nil {
			t.Fatal(err)
		}
		remoteRec := baseManifest()
		remoteSrcDir := t.TempDir()
		writeLocalSnapshot(t, remoteSrcDir, remoteRec, map[string]string{"checkpoint.img": "object-storage-memory"})
		if err := s.uploadSnapshot(t.Context(), testActor, uri, remoteSrcDir, &remoteRec); err != nil {
			t.Fatalf("uploadSnapshot: %v", err)
		}

		localDir := ateletpath.LocalSnapshotDir(actorUID, snapshotName)
		writeLocalSnapshot(t, localDir, baseManifest(), map[string]string{"checkpoint.img": "local-memory"})

		if _, err := s.Restore(t.Context(), restoreReq); err != nil {
			t.Fatalf("Restore: %v", err)
		}
		if got := worker.restored["checkpoint.img"]; got != "object-storage-memory" {
			t.Errorf("restored checkpoint.img = %q, want %q", got, "object-storage-memory")
		}
		if worker.preserveRestoreDir {
			t.Error("preserve_restore_dir = true, want false when restoring from object storage")
		}
	})

	t.Run("no uploaded copy fails with NotFound", func(t *testing.T) {
		useTempNodeDirs(t)
		s := newHerder(t, &recordingObjectStorage{})
		localDir := ateletpath.LocalSnapshotDir(actorUID, snapshotName)
		writeLocalSnapshot(t, localDir, baseManifest(), map[string]string{"checkpoint.img": "local-memory"})

		_, err := s.Restore(t.Context(), restoreReq)
		if code := apierror.Code(err); code != codes.NotFound {
			t.Fatalf("Restore error = %v (code %v), want NotFound", err, code)
		}
		if !errors.Is(err, errUnusableLocalSnapshot) {
			t.Errorf("Restore error = %v, want it to wrap %v", err, errUnusableLocalSnapshot)
		}
	})
}
