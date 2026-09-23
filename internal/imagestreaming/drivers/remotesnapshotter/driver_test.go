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

package remotesnapshotter

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/imagestreaming"
	"github.com/agent-substrate/substrate/internal/proto/snapshots"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type mockSnapshotsServer struct {
	snapshots.UnimplementedSnapshotsServer
	mu          sync.Mutex
	prepareFunc func(context.Context, *snapshots.PrepareSnapshotRequest) (*snapshots.PrepareSnapshotResponse, error)
	viewFunc    func(context.Context, *snapshots.ViewSnapshotRequest) (*snapshots.ViewSnapshotResponse, error)
	commitFunc  func(context.Context, *snapshots.CommitSnapshotRequest) (*emptypb.Empty, error)
	removeFunc  func(context.Context, *snapshots.RemoveSnapshotRequest) (*emptypb.Empty, error)

	preparedKeys  []string
	committedKeys map[string]bool
	viewedKeys    []string
	removedKeys   []string
	mountsByKey   map[string][]*snapshots.Mount
}

func (m *mockSnapshotsServer) Prepare(ctx context.Context, req *snapshots.PrepareSnapshotRequest) (*snapshots.PrepareSnapshotResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.preparedKeys = append(m.preparedKeys, req.GetKey())
	var resp *snapshots.PrepareSnapshotResponse
	var err error
	if m.prepareFunc != nil {
		resp, err = m.prepareFunc(ctx, req)
	} else {
		resp = &snapshots.PrepareSnapshotResponse{
			Mounts: []*snapshots.Mount{
				{
					Type:   "overlay",
					Source: "/var/lib/mock/" + req.GetKey(),
				},
			},
		}
	}
	if err == nil && resp != nil {
		if m.mountsByKey == nil {
			m.mountsByKey = make(map[string][]*snapshots.Mount)
		}
		m.mountsByKey[req.GetKey()] = resp.Mounts
	}
	return resp, err
}

func (m *mockSnapshotsServer) Commit(ctx context.Context, req *snapshots.CommitSnapshotRequest) (*emptypb.Empty, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.committedKeys == nil {
		m.committedKeys = make(map[string]bool)
	}
	m.committedKeys[req.GetName()] = true
	if m.mountsByKey == nil {
		m.mountsByKey = make(map[string][]*snapshots.Mount)
	}
	if mounts, ok := m.mountsByKey[req.GetKey()]; ok {
		m.mountsByKey[req.GetName()] = mounts
	}
	if m.commitFunc != nil {
		return m.commitFunc(ctx, req)
	}
	return &emptypb.Empty{}, nil
}

func (m *mockSnapshotsServer) View(ctx context.Context, req *snapshots.ViewSnapshotRequest) (*snapshots.ViewSnapshotResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.viewedKeys = append(m.viewedKeys, req.GetKey())
	if m.viewFunc != nil {
		return m.viewFunc(ctx, req)
	}
	if m.committedKeys != nil && m.committedKeys[req.GetParent()] {
		if mounts, ok := m.mountsByKey[req.GetParent()]; ok {
			return &snapshots.ViewSnapshotResponse{Mounts: mounts}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "snapshot not found")
}

func (m *mockSnapshotsServer) Remove(ctx context.Context, req *snapshots.RemoveSnapshotRequest) (*emptypb.Empty, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removedKeys = append(m.removedKeys, req.GetKey())
	if m.removeFunc != nil {
		return m.removeFunc(ctx, req)
	}
	return &emptypb.Empty{}, nil
}

func setupTestRemoteSnapshotter(t *testing.T, provider string) (*mockSnapshotsServer, *Driver) {
	t.Helper()
	serverDir := t.TempDir()
	sockPath := filepath.Join(serverDir, "test-snapshotter.sock")

	srv := &mockSnapshotsServer{}
	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("failed to listen on test socket: %v", err)
	}
	grpcServer := grpc.NewServer()
	snapshots.RegisterSnapshotsServer(grpcServer, srv)
	go func() {
		_ = grpcServer.Serve(lis)
	}()

	t.Cleanup(func() {
		grpcServer.Stop()
	})

	workDir := t.TempDir()
	var driver *Driver
	switch provider {
	case ProviderRiptide:
		driver, err = NewRiptide(
			WithSocketPath(sockPath),
			WithWorkDir(workDir),
			WithListableTimeout(500*time.Millisecond),
			WithListableInterval(10*time.Millisecond),
		)
	case ProviderSOCI:
		driver, err = NewSOCI(
			WithSocketPath(sockPath),
			WithWorkDir(workDir),
			WithListableTimeout(500*time.Millisecond),
			WithListableInterval(10*time.Millisecond),
		)
	default:
		driver, err = New(
			WithSocketPath(sockPath),
			WithWorkDir(workDir),
			WithListableTimeout(500*time.Millisecond),
			WithListableInterval(10*time.Millisecond),
		)
	}
	if err != nil {
		t.Fatalf("New driver error: %v", err)
	}
	t.Cleanup(func() {
		_ = driver.Close()
	})

	return srv, driver
}

func TestRegistration(t *testing.T) {
	ctx := context.Background()
	streamer, err := imagestreaming.Get(ctx, ProviderRemoteSnapshotter, imagestreaming.Config{})
	if err != nil {
		t.Fatalf("imagestreaming.Get(%q) error = %v", ProviderRemoteSnapshotter, err)
	}
	if streamer.Name() != ProviderRemoteSnapshotter {
		t.Errorf("streamer.Name() = %q, want %q", streamer.Name(), ProviderRemoteSnapshotter)
	}
}

func TestCanStream(t *testing.T) {
	_, driver := setupTestRemoteSnapshotter(t, ProviderRiptide)
	ctx := context.Background()

	can, err := driver.CanStream(ctx, &imagestreaming.StreamRequest{ImageRef: "example.com/test:v1"})
	if err != nil {
		t.Fatalf("CanStream error: %v", err)
	}
	if !can {
		t.Errorf("CanStream = false, want true when daemon socket exists")
	}

	missingDriver, _ := New(WithSocketPath("/non/existent/test.sock"))
	canMissing, err := missingDriver.CanStream(ctx, &imagestreaming.StreamRequest{ImageRef: "example.com/test:v1"})
	if err != nil {
		t.Fatalf("CanStream error on missing sock: %v", err)
	}
	if canMissing {
		t.Errorf("CanStream = true, want false when socket is missing")
	}
}

func TestPrepareAndReleaseLayers(t *testing.T) {
	srv, driver := setupTestRemoteSnapshotter(t, ProviderRiptide)
	ctx := context.Background()

	diff1 := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	diff2 := "sha256:2222222222222222222222222222222222222222222222222222222222222222"

	driver.imageResolver = func(ctx context.Context, ref string, auth *imagestreaming.AuthConfig) (string, *v1.Config, []string, []string, error) {
		return "sha256:testdigest", &v1.Config{Cmd: []string{"/entrypoint"}}, []string{diff1, diff2}, []string{"sha256:blob1", "sha256:blob2"}, nil
	}

	mount1Dir := t.TempDir()
	mount2Dir := t.TempDir()
	// Create dummy files inside mountDirs so os.ReadDir succeeds.
	if err := os.WriteFile(filepath.Join(mount1Dir, "init"), []byte("bin"), 0o755); err != nil {
		t.Fatalf("failed to create dummy file in mount1: %v", err)
	}
	if err := os.WriteFile(filepath.Join(mount2Dir, "app"), []byte("code"), 0o755); err != nil {
		t.Fatalf("failed to create dummy file in mount2: %v", err)
	}

	callCount := 0
	srv.prepareFunc = func(ctx context.Context, req *snapshots.PrepareSnapshotRequest) (*snapshots.PrepareSnapshotResponse, error) {
		callCount++
		src := mount1Dir
		if callCount == 2 {
			src = mount2Dir
		}
		return &snapshots.PrepareSnapshotResponse{
			Mounts: []*snapshots.Mount{
				{
					Type:   "overlay",
					Source: "overlay",
					Options: []string{
						"lowerdir=" + src,
					},
				},
			},
		}, nil
	}

	req := &imagestreaming.StreamRequest{ImageRef: "us-docker.pkg.dev/proj/repo/image:tag"}
	res, err := driver.PrepareLayers(ctx, req)
	if err != nil {
		t.Fatalf("PrepareLayers error: %v", err)
	}

	if res.ImageDigest != "sha256:testdigest" {
		t.Errorf("got digest %q, want sha256:testdigest", res.ImageDigest)
	}
	if len(res.LayerDirs) != 2 {
		t.Fatalf("got %d LayerDirs, want 2", len(res.LayerDirs))
	}

	// Verify layer wrappers: layerDir/fs symlink and finalized marker
	for i, ldir := range res.LayerDirs {
		fsLink := filepath.Join(ldir, "fs")
		target, err := os.Readlink(fsLink)
		if err != nil {
			t.Errorf("layer %d fs symlink readlink error: %v", i, err)
		}
		expectedTarget := mount1Dir
		if i == 1 {
			expectedTarget = mount2Dir
		}
		if target != expectedTarget {
			t.Errorf("layer %d symlink target = %q, want %q", i, target, expectedTarget)
		}

		finalizedMarker := filepath.Join(ldir, "finalized")
		if _, err := os.Stat(finalizedMarker); err != nil {
			t.Errorf("layer %d missing finalized marker: %v", i, err)
		}
	}

	// Test reference counting: second PrepareLayers reuses warm mounts in memory.
	res2, err := driver.PrepareLayers(ctx, req)
	if err != nil {
		t.Fatalf("second PrepareLayers error: %v", err)
	}
	if len(res2.LayerDirs) != 2 {
		t.Fatalf("res2 len(LayerDirs) = %d, want 2", len(res2.LayerDirs))
	}
	if callCount != 2 {
		t.Errorf("Prepare RPC called %d times, expected 2 (cached lease should have avoided extra RPCs)", callCount)
	}

	// First release: refCount drops to 1, mounts remain active.
	if err := driver.ReleaseLayers(ctx, req); err != nil {
		t.Fatalf("first ReleaseLayers error: %v", err)
	}
	srv.mu.Lock()
	if len(srv.removedKeys) != 0 {
		t.Errorf("expected 0 removed snapshots after first release, got %d", len(srv.removedKeys))
	}
	srv.mu.Unlock()

	// Second release: refCount drops to 0, snapshots are removed and workDir cleaned up.
	if err := driver.ReleaseLayers(ctx, req); err != nil {
		t.Fatalf("second ReleaseLayers error: %v", err)
	}
	srv.mu.Lock()
	if len(srv.removedKeys) != 2 {
		t.Errorf("expected 2 removed snapshots after final release, got %d", len(srv.removedKeys))
	}
	srv.mu.Unlock()

	for _, ldir := range res.LayerDirs {
		if _, err := os.Stat(ldir); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("expected layerDir %s to be deleted after final release, got err: %v", ldir, err)
		}
	}
}

func TestPrepareLayers_ViewFallbackOnAlreadyExists(t *testing.T) {
	srv, driver := setupTestRemoteSnapshotter(t, ProviderSOCI)
	ctx := context.Background()

	diff := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	driver.imageResolver = func(ctx context.Context, ref string, auth *imagestreaming.AuthConfig) (string, *v1.Config, []string, []string, error) {
		return "sha256:digest", &v1.Config{}, []string{diff}, []string{"sha256:blob"}, nil
	}

	mountDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(mountDir, "file"), []byte("test"), 0o644)

	srv.prepareFunc = func(ctx context.Context, req *snapshots.PrepareSnapshotRequest) (*snapshots.PrepareSnapshotResponse, error) {
		return nil, status.Error(codes.AlreadyExists, "snapshot already exists")
	}
	srv.viewFunc = func(ctx context.Context, req *snapshots.ViewSnapshotRequest) (*snapshots.ViewSnapshotResponse, error) {
		return &snapshots.ViewSnapshotResponse{
			Mounts: []*snapshots.Mount{
				{
					Type:   "overlay",
					Source: mountDir,
				},
			},
		}, nil
	}

	req := &imagestreaming.StreamRequest{ImageRef: "example.com/existing:latest"}
	res, err := driver.PrepareLayers(ctx, req)
	if err != nil {
		t.Fatalf("PrepareLayers error when snapshot already exists: %v", err)
	}
	if len(res.LayerDirs) != 1 {
		t.Fatalf("got %d LayerDirs, want 1", len(res.LayerDirs))
	}

	srv.mu.Lock()
	if len(srv.viewedKeys) != 1 {
		t.Errorf("View called %d times, want 1", len(srv.viewedKeys))
	}
	srv.mu.Unlock()
}

func TestReconcileLeases(t *testing.T) {
	_, driver := setupTestRemoteSnapshotter(t, ProviderRiptide)
	ctx := context.Background()

	mountDir := t.TempDir()
	active := []*imagestreaming.ActiveLease{
		{
			ImageRef:    "example.com/app:v1",
			ImageDigest: "sha256:digest1",
			LayerDirs:   []string{mountDir},
			RefCount:    3,
		},
	}

	if err := driver.ReconcileLeases(ctx, active); err != nil {
		t.Fatalf("ReconcileLeases error: %v", err)
	}

	driver.mu.Lock()
	lease, ok := driver.leases["example.com/app:v1"]
	driver.mu.Unlock()

	if !ok {
		t.Fatalf("expected lease for example.com/app:v1 to exist after reconciliation")
	}
	if lease.refCount != 3 {
		t.Errorf("got refCount = %d, want 3", lease.refCount)
	}
	if len(lease.layers) != 1 || lease.layers[0] != mountDir {
		t.Errorf("got layers %v, want [%s]", lease.layers, mountDir)
	}
}
