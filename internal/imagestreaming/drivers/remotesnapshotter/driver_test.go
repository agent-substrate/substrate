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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/imagestreaming"
	"github.com/agent-substrate/substrate/internal/proto/snapshots"
	"github.com/google/go-containerregistry/pkg/authn"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// mockSnapshotsServer is a fake remote snapshotter. By default, Prepare
// provides the layers in remoteMounts the way a remote snapshotter does: it
// commits them under the chain ID from the containerd.io/snapshot.ref label
// and returns AlreadyExists. It declines every other layer by returning
// mounts with a nil error.
type mockSnapshotsServer struct {
	snapshots.UnimplementedSnapshotsServer
	mu          sync.Mutex
	prepareFunc func(context.Context, *snapshots.PrepareSnapshotRequest) (*snapshots.PrepareSnapshotResponse, error)
	viewFunc    func(context.Context, *snapshots.ViewSnapshotRequest) (*snapshots.ViewSnapshotResponse, error)
	commitFunc  func(context.Context, *snapshots.CommitSnapshotRequest) (*emptypb.Empty, error)
	removeFunc  func(context.Context, *snapshots.RemoveSnapshotRequest) (*emptypb.Empty, error)
	statFunc    func(context.Context, *snapshots.StatSnapshotRequest) (*snapshots.StatSnapshotResponse, error)

	// remoteMounts maps the chain IDs the fake can provide to their mounts.
	remoteMounts map[string][]*snapshots.Mount

	preparedKeys  []string
	committedKeys map[string]bool
	commitNames   []string
	statKeys      []string
	viewedKeys    []string
	removedKeys   []string
	mountsByKey   map[string][]*snapshots.Mount
}

func (m *mockSnapshotsServer) Prepare(ctx context.Context, req *snapshots.PrepareSnapshotRequest) (*snapshots.PrepareSnapshotResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.preparedKeys = append(m.preparedKeys, req.GetKey())
	if m.prepareFunc != nil {
		return m.prepareFunc(ctx, req)
	}
	target := req.GetLabels()["containerd.io/snapshot.ref"]
	if mounts, ok := m.remoteMounts[target]; ok {
		m.commitLocked(target, mounts)
		return nil, status.Errorf(codes.AlreadyExists, "target snapshot %q: already exists", target)
	}
	return &snapshots.PrepareSnapshotResponse{
		Mounts: []*snapshots.Mount{{Type: "bind", Source: "/var/lib/mock/" + req.GetKey()}},
	}, nil
}

func (m *mockSnapshotsServer) Commit(ctx context.Context, req *snapshots.CommitSnapshotRequest) (*emptypb.Empty, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commitNames = append(m.commitNames, req.GetName())
	if m.commitFunc != nil {
		return m.commitFunc(ctx, req)
	}
	return &emptypb.Empty{}, nil
}

func (m *mockSnapshotsServer) Stat(ctx context.Context, req *snapshots.StatSnapshotRequest) (*snapshots.StatSnapshotResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statKeys = append(m.statKeys, req.GetKey())
	if m.statFunc != nil {
		return m.statFunc(ctx, req)
	}
	if !m.committedKeys[req.GetKey()] {
		return nil, status.Errorf(codes.NotFound, "snapshot %v does not exist", req.GetKey())
	}
	return &snapshots.StatSnapshotResponse{Info: &snapshots.Info{Name: req.GetKey()}}, nil
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

// commitLocked records a committed snapshot. The caller must hold m.mu.
func (m *mockSnapshotsServer) commitLocked(name string, mounts []*snapshots.Mount) {
	if m.committedKeys == nil {
		m.committedKeys = make(map[string]bool)
	}
	if m.mountsByKey == nil {
		m.mountsByKey = make(map[string][]*snapshots.Mount)
	}
	m.committedKeys[name] = true
	m.mountsByKey[name] = mounts
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

	chain := computeChainIDs([]string{diff1, diff2})
	srv.mu.Lock()
	srv.remoteMounts = map[string][]*snapshots.Mount{
		chain[0].ChainID: {{Type: "overlay", Source: "overlay", Options: []string{"lowerdir=" + mount1Dir}}},
		chain[1].ChainID: {{Type: "overlay", Source: "overlay", Options: []string{"lowerdir=" + mount2Dir}}},
	}
	srv.mu.Unlock()

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
	srv.mu.Lock()
	if len(srv.preparedKeys) != 2 {
		t.Errorf("Prepare RPC called %d times, expected 2 (cached lease should have avoided extra RPCs)", len(srv.preparedKeys))
	}
	if len(srv.commitNames) != 0 {
		t.Errorf("Commit called for %v, want no commits", srv.commitNames)
	}
	srv.mu.Unlock()

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

// TestPrepareLayers_SnapshotterOutcomes covers each result of the remote
// snapshotter protocol for a two-layer image.
func TestPrepareLayers_SnapshotterOutcomes(t *testing.T) {
	diffIDs := []string{
		"sha256:1111111111111111111111111111111111111111111111111111111111111111",
		"sha256:2222222222222222222222222222222222222222222222222222222222222222",
	}
	chain := computeChainIDs(diffIDs)
	c0, c1 := chain[0].ChainID, chain[1].ChainID

	tests := []struct {
		name       string
		committed  []string // chain IDs already committed on the node
		remote     []string // chain IDs the snapshotter can provide
		prepareErr error    // returned by every Prepare when set
		statErr    error    // returned by every Stat when set

		wantErr     error // matched with errors.Is when set
		wantFailure bool  // want an error other than ErrNotStreamable
		wantStats   []string
		// Snapshot keys, without the per-run prefix.
		wantPrepared []string
		wantViewed   []string
		wantRemoved  []string
	}{
		{
			name:       "layers already committed",
			committed:  []string{c0, c1},
			wantStats:  []string{c0, c1},
			wantViewed: []string{"l0-view", "l1-view"},
		},
		{
			name:         "prepare returns AlreadyExists",
			remote:       []string{c0, c1},
			wantStats:    []string{c0, c0, c1, c1},
			wantPrepared: []string{"l0-prep", "l1-prep"},
			wantViewed:   []string{"l0-view", "l1-view"},
		},
		{
			name:         "prepare declines the first layer",
			wantErr:      imagestreaming.ErrNotStreamable,
			wantStats:    []string{c0},
			wantPrepared: []string{"l0-prep"},
			wantRemoved:  []string{"l0-prep"},
		},
		{
			name:         "prepare declines a later layer",
			remote:       []string{c0},
			wantErr:      imagestreaming.ErrNotStreamable,
			wantStats:    []string{c0, c0, c1},
			wantPrepared: []string{"l0-prep", "l1-prep"},
			wantViewed:   []string{"l0-view"},
			wantRemoved:  []string{"l1-prep", "l0-view"},
		},
		{
			name:         "prepare fails",
			prepareErr:   status.Error(codes.Internal, "commit failed"),
			wantFailure:  true,
			wantStats:    []string{c0},
			wantPrepared: []string{"l0-prep"},
			wantRemoved:  []string{"l0-prep"},
		},
		{
			name:         "prepare returns AlreadyExists without committing the chain ID",
			prepareErr:   status.Error(codes.AlreadyExists, "key exists"),
			wantFailure:  true,
			wantStats:    []string{c0, c0},
			wantPrepared: []string{"l0-prep"},
		},
		{
			name:        "stat fails",
			statErr:     status.Error(codes.Unavailable, "remount failed"),
			wantFailure: true,
			wantStats:   []string{c0},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv, driver := setupTestRemoteSnapshotter(t, ProviderRemoteSnapshotter)
			ctx := context.Background()
			driver.imageResolver = func(ctx context.Context, ref string, auth *imagestreaming.AuthConfig) (string, *v1.Config, []string, []string, error) {
				return "sha256:digest", &v1.Config{}, diffIDs, []string{"sha256:blob0", "sha256:blob1"}, nil
			}

			mountDirs := map[string]string{c0: t.TempDir(), c1: t.TempDir()}
			bindMount := func(chainID string) []*snapshots.Mount {
				return []*snapshots.Mount{{Type: "bind", Source: mountDirs[chainID], Options: []string{"ro", "rbind"}}}
			}
			srv.mu.Lock()
			srv.remoteMounts = map[string][]*snapshots.Mount{}
			for _, c := range tc.remote {
				srv.remoteMounts[c] = bindMount(c)
			}
			for _, c := range tc.committed {
				srv.commitLocked(c, bindMount(c))
			}
			if tc.prepareErr != nil {
				srv.prepareFunc = func(context.Context, *snapshots.PrepareSnapshotRequest) (*snapshots.PrepareSnapshotResponse, error) {
					return nil, tc.prepareErr
				}
			}
			if tc.statErr != nil {
				srv.statFunc = func(context.Context, *snapshots.StatSnapshotRequest) (*snapshots.StatSnapshotResponse, error) {
					return nil, tc.statErr
				}
			}
			srv.mu.Unlock()

			ref := "example.com/app:v1"
			res, err := driver.PrepareLayers(ctx, &imagestreaming.StreamRequest{ImageRef: ref})
			switch {
			case tc.wantErr != nil:
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("PrepareLayers error = %v, want %v", err, tc.wantErr)
				}
			case tc.wantFailure:
				if err == nil || errors.Is(err, imagestreaming.ErrNotStreamable) {
					t.Fatalf("PrepareLayers error = %v, want an error other than ErrNotStreamable", err)
				}
			case err != nil:
				t.Fatalf("PrepareLayers error: %v", err)
			default:
				if len(res.LayerDirs) != 2 {
					t.Fatalf("got %d LayerDirs, want 2", len(res.LayerDirs))
				}
				for i, c := range []string{c0, c1} {
					target, err := os.Readlink(filepath.Join(res.LayerDirs[i], "fs"))
					if err != nil || target != mountDirs[c] {
						t.Errorf("layer %d fs symlink = %q (err %v), want %q", i, target, err, mountDirs[c])
					}
				}
			}

			srv.mu.Lock()
			defer srv.mu.Unlock()
			if !slices.Equal(srv.statKeys, tc.wantStats) {
				t.Errorf("Stat keys = %v, want %v", srv.statKeys, tc.wantStats)
			}
			if got := keySuffixes(srv.preparedKeys); !slices.Equal(got, tc.wantPrepared) {
				t.Errorf("Prepare keys = %v, want %v", got, tc.wantPrepared)
			}
			if got := keySuffixes(srv.viewedKeys); !slices.Equal(got, tc.wantViewed) {
				t.Errorf("View keys = %v, want %v", got, tc.wantViewed)
			}
			// A removed chain ID would show up here unstripped and fail the comparison.
			if got := keySuffixes(srv.removedKeys); !slices.Equal(got, tc.wantRemoved) {
				t.Errorf("Remove keys = %v, want %v", got, tc.wantRemoved)
			}
			if len(srv.commitNames) != 0 {
				t.Errorf("Commit called for %v, want no commits", srv.commitNames)
			}

			if err != nil {
				imageWorkDir := filepath.Join(driver.workDir, sanitizePathKey(ref))
				if _, statErr := os.Stat(imageWorkDir); !errors.Is(statErr, os.ErrNotExist) {
					t.Errorf("image work dir after failure: got err %v, want it removed", statErr)
				}
				driver.mu.Lock()
				_, leased := driver.leases[ref]
				driver.mu.Unlock()
				if leased {
					t.Errorf("lease for %s recorded after failure", ref)
				}
			}
		})
	}
}

// keySuffixes strips the per-run prefix from snapshot keys, leaving
// "l<N>-prep" or "l<N>-view".
func keySuffixes(keys []string) []string {
	var out []string
	for _, k := range keys {
		out = append(out, k[strings.LastIndex(k, "-l")+1:])
	}
	return out
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

type staticTestKeychain struct {
	authn.Keychain
	resolved []string
}

func (k *staticTestKeychain) Resolve(target authn.Resource) (authn.Authenticator, error) {
	k.resolved = append(k.resolved, target.String())
	return authn.Anonymous, nil
}

func TestWithKeychain_AttachesToDriver(t *testing.T) {
	kc := &staticTestKeychain{}
	d, err := New(WithKeychain(kc))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if d.keychain != kc {
		t.Errorf("got driver keychain = %v, want %v", d.keychain, kc)
	}
}

func TestNewFromConfig_PropagatesKeychainFromContext(t *testing.T) {
	kc := &staticTestKeychain{}
	ctx := imagestreaming.WithKeychainContext(context.Background(), kc)
	d, err := NewFromConfig(ctx, ProviderRemoteSnapshotter, imagestreaming.Config{})
	if err != nil {
		t.Fatalf("NewFromConfig failed: %v", err)
	}
	if d.keychain != kc {
		t.Errorf("got driver keychain = %v, want %v", d.keychain, kc)
	}
}
