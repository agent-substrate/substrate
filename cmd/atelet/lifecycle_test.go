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

func TestRestoreFailureBeforeRegistrationPreservesExistingRegistration(t *testing.T) {
	useTempNodeDirs(t)
	ctx := t.Context()
	const (
		actorUID     = "actor-uid-restore"
		snapshotName = "pause-snap-1"
	)

	store := newCTBStore(t)
	certA := string(testCertPEM(t))
	store.set(t, certA)
	refresher := newSystemInfoVolumeRefresher(store.lister, nil)
	registeredDir := t.TempDir()
	registerTrustVolume(t, refresher, registeredDir, actorUID)
	registered := refresher.actors[actorUID]

	assetHash := fmt.Sprintf("%x", sha256.Sum256([]byte("missing asset")))
	writeLocalSnapshot(t, ateompath.LocalSnapshotDir(actorUID, snapshotName), sandboxAssetsRecord{
		SandboxClass:  "gvisor",
		PauseImage:    testPauseImage,
		Assets:        map[string]assetEntry{"runsc": {URL: "gs://test-bucket/runsc", SHA256: assetHash}},
		SnapshotFiles: []string{"checkpoint.img"},
	}, map[string]string{"checkpoint.img": "guest-memory"})

	spec := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{{Name: "app"}},
		Volumes: []*ateletpb.Volume{{
			Name:   "trust",
			Source: &ateletpb.Volume_SystemInfo{SystemInfo: trustVolumeSpec("ca.pem")},
		}},
	}
	s := &AteomHerder{
		anonGCSClient:     fakeObjectStorage{err: fmt.Errorf("asset unavailable")},
		systemInfoVolumes: refresher,
	}
	_, err := s.Restore(ctx, &ateletpb.RestoreRequest{
		Atespace:       "team-a",
		ActorName:      "actor-restore",
		ActorUid:       actorUID,
		TargetAteomUid: "ateom-uid-1",
		Spec:           spec,
		Scope:          ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		Type:           ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL,
		Config: &ateletpb.RestoreRequest_LocalConfig{
			LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotName: snapshotName},
		},
	})
	if err == nil {
		t.Fatal("Restore succeeded with an unavailable sandbox asset")
	}
	if got := refresher.actors[actorUID]; got != registered {
		t.Fatalf("failed Restore removed the existing registration: got %p, want %p", got, registered)
	}

	certB := string(testCertPEM(t))
	store.set(t, certB)
	if err := refresher.refreshBundle(ctx, EgressTrustBundleName); err != nil {
		t.Fatalf("refreshBundle: %v", err)
	}
	if got := readProjected(t, registeredDir, actorUID, "trust", "ca.pem"); got != certB {
		t.Fatalf("existing registration stopped refreshing after failed Restore: got %q, want cert B", got)
	}
}

func TestRunFailureAfterRegistrationRemovesOwnRegistration(t *testing.T) {
	useTempNodeDirs(t)
	store := newCTBStore(t)
	store.set(t, string(testCertPEM(t)))
	refresher := newSystemInfoVolumeRefresher(store.lister, nil)
	content := []byte("runsc binary")
	assetHash := fmt.Sprintf("%x", sha256.Sum256(content))
	s := &AteomHerder{
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
		Atespace:       "team-a",
		ActorName:      "actor-run",
		ActorUid:       "actor-uid-run",
		TargetAteomUid: "ateom-uid-1",
		SandboxAssets: &ateletpb.SandboxAssets{
			SandboxClass: "gvisor",
			PauseImage:   "://invalid-image",
			Assets: map[string]*ateletpb.ArchAssets{runtime.GOARCH: {Files: map[string]*ateletpb.AssetFile{
				runscAssetName: {Url: "gs://test-bucket/runsc", Sha256: assetHash},
			}}},
		},
		Spec: spec,
	})
	if err == nil {
		t.Fatal("Run succeeded with an invalid pause image")
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
	refresher := newSystemInfoVolumeRefresher(store.lister, nil)
	content := []byte("runsc binary")
	assetHash := fmt.Sprintf("%x", sha256.Sum256(content))
	writeLocalSnapshot(t, ateompath.LocalSnapshotDir(actorUID, snapshotName), sandboxAssetsRecord{
		SandboxClass:  "gvisor",
		PauseImage:    "://invalid-image",
		Assets:        map[string]assetEntry{"runsc": {URL: "gs://test-bucket/runsc", SHA256: assetHash}},
		SnapshotFiles: []string{"checkpoint.img"},
	}, map[string]string{"checkpoint.img": "guest-memory"})

	spec := &ateletpb.WorkloadSpec{Volumes: []*ateletpb.Volume{{
		Name:   "trust",
		Source: &ateletpb.Volume_SystemInfo{SystemInfo: trustVolumeSpec("ca.pem")},
	}}}
	s := &AteomHerder{
		anonGCSClient:     fakeObjectStorage{data: content},
		imageCache:        newImageVolumeStore(t),
		systemInfoVolumes: refresher,
	}
	_, err := s.Restore(t.Context(), &ateletpb.RestoreRequest{
		Atespace:       "team-a",
		ActorName:      "actor-restore-after",
		ActorUid:       actorUID,
		TargetAteomUid: "ateom-uid-1",
		Spec:           spec,
		Scope:          ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		Type:           ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL,
		Config: &ateletpb.RestoreRequest_LocalConfig{
			LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotName: snapshotName},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "while creating pause OCI bundle") {
		t.Fatalf("Restore error = %v, want the post-registration OCI preparation failure", err)
	}
	if got := refresher.actors[actorUID]; got != nil {
		t.Fatalf("failed Restore left its owned registration live: %p", got)
	}
}
