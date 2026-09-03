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
	"crypto/sha256"
	"encoding/json"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
		ateomDialer:   newAteomDialer(1),
		imageCache:    newImageVolumeStore(t),
		anonGCSClient: fakeObjectStorage{data: runsc},
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
		SandboxConfigRef: &ateletpb.SandboxConfigRef{Name: "gvisor-prod", Uid: "sandbox-uid-1", ResourceVersion: "42"},
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

	// Run records the sandbox assets (with their SandboxConfig reference)
	// on-node; Checkpoint and Terminate read them back from there.
	rec, err := readSandboxRecord(actorUID)
	if err != nil {
		t.Fatalf("Run left no readable on-node sandbox record: %v", err)
	}
	if want := (sandboxConfigRef{Name: "gvisor-prod", UID: "sandbox-uid-1", ResourceVersion: "42"}); rec.SandboxConfigRef != want {
		t.Errorf("record SandboxConfig ref = %+v, want %+v", rec.SandboxConfigRef, want)
	}

	// Pause: a local checkpoint, which leaves the snapshot on this node.
	checkpointResp, err := s.Checkpoint(ctx, &ateletpb.CheckpointRequest{
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
	})
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	// The checkpoint reports the on-node record's SandboxConfig reference so
	// the control plane can stamp it on the snapshot record it finalizes.
	if got := checkpointResp.GetSandboxConfigRef(); got.GetName() != "gvisor-prod" || got.GetUid() != "sandbox-uid-1" || got.GetResourceVersion() != "42" {
		t.Errorf("Checkpoint response SandboxConfig ref = %s/%s@%s, want gvisor-prod/sandbox-uid-1@42", got.GetName(), got.GetUid(), got.GetResourceVersion())
	}
	snapshotFile := filepath.Join(ateompath.LocalSnapshotDir(actorUID, snapshotName), "checkpoint.img")
	if _, err := os.Stat(snapshotFile); err != nil {
		t.Fatalf("pause did not write the local snapshot: %v", err)
	}

	// The pause manifest carries the SandboxConfig reference, not the assets.
	manifest, err := os.ReadFile(filepath.Join(ateompath.LocalSnapshotDir(actorUID, snapshotName), sandboxManifestName))
	if err != nil {
		t.Fatalf("reading pause manifest: %v", err)
	}
	for _, key := range []string{`"assets"`, `"pauseImage"`} {
		if bytes.Contains(manifest, []byte(key)) {
			t.Errorf("pause manifest contains %s: %s", key, manifest)
		}
	}
	man, err := unmarshalSandboxManifest(manifest)
	if err != nil {
		t.Fatalf("parsing pause manifest: %v", err)
	}
	if want := (&sandboxConfigRef{Name: "gvisor-prod", UID: "sandbox-uid-1", ResourceVersion: "42"}); man.SandboxConfigRef == nil || *man.SandboxConfigRef != *want {
		t.Errorf("pause manifest SandboxConfig ref = %+v, want %+v", man.SandboxConfigRef, want)
	}

	// Resume: restores from that local snapshot, with the sandbox assets
	// resolved and sent by the control plane.
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
		SandboxAssets: sandboxAssets,
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
}

// TestRestoreSandboxAssetsSource pins that Restore takes its sandbox assets
// only from the request — verified against the manifest's SandboxConfig
// reference — and rejects requests without them.
func TestRestoreSandboxAssetsSource(t *testing.T) {
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

	runsc := []byte("runsc binary")
	s := &AteomHerder{
		ateomDialer:   newAteomDialer(1),
		imageCache:    newImageVolumeStore(t),
		anonGCSClient: fakeObjectStorage{data: runsc},
	}
	assetFiles := map[string]*ateletpb.AssetFile{
		runscAssetName: {
			Url:    "gs://test-bucket/runsc",
			Sha256: fmt.Sprintf("%x", sha256.Sum256(runsc)),
		},
	}
	sandboxAssets := &ateletpb.SandboxAssets{
		SandboxClass:     "gvisor",
		PauseImage:       image,
		Assets:           map[string]*ateletpb.ArchAssets{runtime.GOARCH: {Files: assetFiles}},
		SandboxConfigRef: &ateletpb.SandboxConfigRef{Name: "gvisor-prod", Uid: "sandbox-uid-1", ResourceVersion: "42"},
	}
	spec := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{{Name: "app", Image: image, Command: []string{"/bin/app"}}},
	}

	// Run + pause to lay a local snapshot (with an asset-less manifest) out
	// on the node.
	if _, err := s.Run(ctx, &ateletpb.RunRequest{
		Atespace: atespace, ActorName: actorName, ActorUid: actorUID,
		ActorTemplateAtespace: "default", ActorTemplateName: "counter",
		TargetAteomUid: ateomUID, SandboxAssets: sandboxAssets, Spec: spec,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, err := s.Checkpoint(ctx, &ateletpb.CheckpointRequest{
		Atespace: atespace, ActorName: actorName, ActorUid: actorUID,
		ActorTemplateAtespace: "default", ActorTemplateName: "counter",
		TargetAteomUid: ateomUID, Spec: spec,
		Scope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		Type:  ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL,
		Config: &ateletpb.CheckpointRequest_LocalConfig{
			LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotName: snapshotName},
		},
	}); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	manifestPath := filepath.Join(ateompath.LocalSnapshotDir(actorUID, snapshotName), sandboxManifestName)
	strippedManifest, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("reading pause manifest: %v", err)
	}
	writeManifest := func(t *testing.T, man *snapshotManifest) {
		t.Helper()
		b, err := json.Marshal(man)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(manifestPath, b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	restoreReq := func(sa *ateletpb.SandboxAssets) *ateletpb.RestoreRequest {
		return &ateletpb.RestoreRequest{
			Atespace: atespace, ActorName: actorName, ActorUid: actorUID,
			ActorTemplateAtespace: "default", ActorTemplateName: "counter",
			TargetAteomUid: ateomUID, Spec: spec,
			Scope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
			Type:  ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL,
			Config: &ateletpb.RestoreRequest_LocalConfig{
				LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotName: snapshotName},
			},
			SandboxAssets: sa,
		}
	}

	t.Run("request without sandbox_assets is rejected outright", func(t *testing.T) {
		_, err := s.Restore(ctx, restoreReq(nil))
		if got := status.Code(err); got != codes.InvalidArgument {
			t.Fatalf("status.Code = %v (err %v), want InvalidArgument", got, err)
		}
	})

	t.Run("request assets restore the snapshot", func(t *testing.T) {
		if _, err := s.Restore(ctx, restoreReq(sandboxAssets)); err != nil {
			t.Fatalf("Restore: %v", err)
		}
	})

	t.Run("manifest revision mismatch is rejected", func(t *testing.T) {
		// Same SandboxConfig object, updated in place since the checkpoint:
		// the request's assets no longer describe the binaries the memory
		// image was captured under.
		man, err := unmarshalSandboxManifest(strippedManifest)
		if err != nil {
			t.Fatal(err)
		}
		man.SandboxConfigRef.ResourceVersion = "41"
		writeManifest(t, man)
		_, err = s.Restore(ctx, restoreReq(sandboxAssets))
		if got := status.Code(err); got != codes.FailedPrecondition {
			t.Fatalf("status.Code = %v (err %v), want FailedPrecondition", got, err)
		}
	})

	t.Run("manifest ref mismatch is rejected", func(t *testing.T) {
		man, err := unmarshalSandboxManifest(strippedManifest)
		if err != nil {
			t.Fatal(err)
		}
		man.SandboxConfigRef.UID = "some-other-uid"
		writeManifest(t, man)
		_, err = s.Restore(ctx, restoreReq(sandboxAssets))
		if got := status.Code(err); got != codes.FailedPrecondition {
			t.Fatalf("status.Code = %v (err %v), want FailedPrecondition", got, err)
		}
	})

	t.Run("manifest ref mismatch is allowed for a DATA restore", func(t *testing.T) {
		// A DATA restore cold-boots the guest from the spec, so the request's
		// assets may legitimately come from a different SandboxConfig than the
		// snapshot's (a repointed actor). The mismatched manifest written by
		// the previous subtest is still in place.
		req := restoreReq(sandboxAssets)
		req.Scope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA
		if _, err := s.Restore(ctx, req); err != nil {
			t.Fatalf("Restore: %v", err)
		}
	})

	t.Run("old-format manifest assets are never read back", func(t *testing.T) {
		// An old-format manifest still carries the asset set, but the type no
		// longer declares those keys, so an asset-less request must still fail.
		legacy := fmt.Sprintf(`{"sandboxClass":"gvisor","pauseImage":%q,"assets":{%q:{"url":"gs://test-bucket/runsc","sha256":"%x"}},"snapshotFiles":["checkpoint.img"],"scope":"full"}`,
			image, runscAssetName, sha256.Sum256(runsc))
		if err := os.WriteFile(manifestPath, []byte(legacy), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := s.Restore(ctx, restoreReq(nil))
		if got := status.Code(err); got != codes.InvalidArgument {
			t.Fatalf("status.Code = %v (err %v), want InvalidArgument", got, err)
		}
		// With request assets the same old-format snapshot restores fine (it
		// records no SandboxConfig reference, so there is nothing to match).
		if _, err := s.Restore(ctx, restoreReq(sandboxAssets)); err != nil {
			t.Fatalf("Restore of old-format snapshot with request assets: %v", err)
		}
	})
}
