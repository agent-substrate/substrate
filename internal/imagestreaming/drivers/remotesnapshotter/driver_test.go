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
	"github.com/agent-substrate/substrate/internal/proto/riptidekeychain"
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

func TestProbe(t *testing.T) {
	ctx := context.Background()

	t.Run("reachable returns NotFound", func(t *testing.T) {
		srv, driver := setupTestRemoteSnapshotter(t, ProviderRiptide)
		if err := driver.Probe(ctx); err != nil {
			t.Fatalf("Probe() = %v, want nil", err)
		}
		srv.mu.Lock()
		gotKeys := slices.Clone(srv.statKeys)
		srv.mu.Unlock()
		if !slices.Equal(gotKeys, []string{probeKey}) {
			t.Errorf("Stat keys = %v, want [%s]", gotKeys, probeKey)
		}
		// Probe before any PrepareLayers uses a temporary connection and
		// leaves d.snapshotsClient unset.
		driver.mu.Lock()
		cachedClient := driver.snapshotsClient
		driver.mu.Unlock()
		if cachedClient != nil {
			t.Errorf("driver.snapshotsClient = %v after cold Probe, want nil", cachedClient)
		}
	})

	t.Run("snapshotter error surfaces", func(t *testing.T) {
		srv, driver := setupTestRemoteSnapshotter(t, ProviderRiptide)
		srv.mu.Lock()
		srv.statFunc = func(context.Context, *snapshots.StatSnapshotRequest) (*snapshots.StatSnapshotResponse, error) {
			return nil, status.Error(codes.Unavailable, "daemon shutting down")
		}
		srv.mu.Unlock()

		if err := driver.Probe(ctx); status.Code(err) != codes.Unavailable {
			t.Fatalf("Probe() = %v, want Unavailable", err)
		}
	})

	t.Run("dead socket fails within context deadline", func(t *testing.T) {
		sockPath := filepath.Join(t.TempDir(), "dead.sock")
		ln, err := net.Listen("unix", sockPath)
		if err != nil {
			t.Fatalf("net.Listen: %v", err)
		}
		_ = ln.Close()

		d, err := New(WithName(ProviderRiptide), WithSocketPath(sockPath))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		probeCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		if err := d.Probe(probeCtx); err == nil {
			t.Fatal("Probe() on closed socket = nil, want error")
		}
	})
}

func TestReconcileLeases_RestoresMetadataAndSweepsOrphans(t *testing.T) {
	ctx := context.Background()
	srv, driver := setupTestRemoteSnapshotter(t, ProviderRiptide)

	mountActive := t.TempDir()
	mountOrphan := t.TempDir()
	diffActive := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	diffOrphan := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	srv.remoteMounts = map[string][]*snapshots.Mount{
		diffActive: {{Type: "bind", Source: mountActive}},
		diffOrphan: {{Type: "bind", Source: mountOrphan}},
	}

	activeRef := "example.com/active:v1"
	orphanRef := "example.com/orphan:v1"

	driver.imageResolver = func(_ context.Context, ref string, _ *imagestreaming.AuthConfig) (string, *v1.Config, []string, []string, error) {
		switch ref {
		case activeRef:
			return "sha256:1111", &v1.Config{Cmd: []string{"/active-app"}}, []string{diffActive}, []string{"sha256:l1"}, nil
		case orphanRef:
			return "sha256:2222", &v1.Config{Cmd: []string{"/orphan-app"}}, []string{diffOrphan}, []string{"sha256:l2"}, nil
		default:
			return "", nil, nil, nil, errors.New("unexpected ref")
		}
	}

	activeRes, err := driver.PrepareLayers(ctx, &imagestreaming.StreamRequest{ImageRef: activeRef})
	if err != nil {
		t.Fatalf("PrepareLayers(active): %v", err)
	}
	orphanRes, err := driver.PrepareLayers(ctx, &imagestreaming.StreamRequest{ImageRef: orphanRef})
	if err != nil {
		t.Fatalf("PrepareLayers(orphan): %v", err)
	}
	activeWorkDir := filepath.Dir(activeRes.LayerDirs[0])
	orphanWorkDir := filepath.Dir(orphanRes.LayerDirs[0])

	// Simulate an atelet restart with a fresh Driver instance sharing workDir and socket.
	restarted, err := NewRiptide(
		WithSocketPath(driver.socket),
		WithWorkDir(driver.workDir),
		WithImageResolver(func(context.Context, string, *imagestreaming.AuthConfig) (string, *v1.Config, []string, []string, error) {
			return "", nil, nil, nil, errors.New("imageResolver must not be called for warm reconciled lease")
		}),
	)
	if err != nil {
		t.Fatalf("NewRiptide: %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close() })

	// Only activeRef is still mounted by a surviving actor; orphanRef has no actor.
	if err := restarted.ReconcileLeases(ctx, []*imagestreaming.ActiveLease{{
		ImageRef:    activeRef,
		ImageDigest: activeRes.ImageDigest,
		LayerDirs:   activeRes.LayerDirs,
		RefCount:    1,
	}}); err != nil {
		t.Fatalf("ReconcileLeases: %v", err)
	}

	// 1. Orphan workDir should be swept and its view snapshot removed.
	if _, err := os.Stat(orphanWorkDir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("orphanWorkDir %s still exists after ReconcileLeases: err=%v", orphanWorkDir, err)
	}
	srv.mu.Lock()
	removedAfterSweep := slices.Clone(srv.removedKeys)
	srv.mu.Unlock()
	if len(removedAfterSweep) != 1 || !strings.Contains(removedAfterSweep[0], "orphan") {
		t.Errorf("removedKeys after sweep = %v, want 1 orphan view key", removedAfterSweep)
	}

	// 2. Active lease should have restored Config so a warm PrepareLayers succeeds without calling imageResolver.
	warmRes, err := restarted.PrepareLayers(ctx, &imagestreaming.StreamRequest{ImageRef: activeRef})
	if err != nil {
		t.Fatalf("warm PrepareLayers after reconcile: %v", err)
	}
	if warmRes.Config == nil || !slices.Equal(warmRes.Config.Cmd, []string{"/active-app"}) {
		t.Errorf("warm PrepareLayers Config = %+v, want Cmd=[/active-app]", warmRes.Config)
	}

	// 3. Releasing both references should remove the active view snapshot and delete activeWorkDir.
	if err := restarted.ReleaseLayers(ctx, &imagestreaming.StreamRequest{ImageRef: activeRef}); err != nil {
		t.Fatalf("first ReleaseLayers: %v", err)
	}
	if _, err := os.Stat(activeWorkDir); err != nil {
		t.Fatalf("activeWorkDir removed while refCount=1: %v", err)
	}
	if err := restarted.ReleaseLayers(ctx, &imagestreaming.StreamRequest{ImageRef: activeRef}); err != nil {
		t.Fatalf("second ReleaseLayers: %v", err)
	}
	if _, err := os.Stat(activeWorkDir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("activeWorkDir %s still exists after final ReleaseLayers: err=%v", activeWorkDir, err)
	}
	srv.mu.Lock()
	removedFinal := slices.Clone(srv.removedKeys)
	srv.mu.Unlock()
	if len(removedFinal) != 2 {
		t.Errorf("removedKeys after final release = %v, want 2 view keys (orphan + active)", removedFinal)
	}
}

func TestDeclineTTL_SkipsRepeatLookupUntilExpired(t *testing.T) {
	ctx := context.Background()
	srv, driver := setupTestRemoteSnapshotter(t, ProviderRiptide)
	driver.declineTTL = 60 * time.Millisecond

	diffID := "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	var resolverCalls int
	driver.imageResolver = func(_ context.Context, _ string, _ *imagestreaming.AuthConfig) (string, *v1.Config, []string, []string, error) {
		resolverCalls++
		return "sha256:declined", &v1.Config{}, []string{diffID}, []string{"sha256:l1"}, nil
	}

	req := &imagestreaming.StreamRequest{ImageRef: "docker.io/library/unstreamable:v1"}

	// 1. First attempt is declined by the snapshotter.
	if _, err := driver.PrepareLayers(ctx, req); !errors.Is(err, imagestreaming.ErrNotStreamable) {
		t.Fatalf("first PrepareLayers error = %v, want ErrNotStreamable", err)
	}
	if resolverCalls != 1 {
		t.Fatalf("resolverCalls = %d, want 1", resolverCalls)
	}
	srv.mu.Lock()
	prepCount1 := len(srv.preparedKeys)
	srv.mu.Unlock()
	if prepCount1 != 1 {
		t.Fatalf("preparedKeys = %d, want 1", prepCount1)
	}

	// 2. While the decline TTL is active, CanStream returns false and
	// PrepareLayers returns ErrNotStreamable without calling imageResolver or Prepare.
	can, err := driver.CanStream(ctx, req)
	if err != nil {
		t.Fatalf("CanStream error: %v", err)
	}
	if can {
		t.Errorf("CanStream = true while decline TTL active, want false")
	}
	if _, err := driver.PrepareLayers(ctx, req); !errors.Is(err, imagestreaming.ErrNotStreamable) {
		t.Fatalf("cached PrepareLayers error = %v, want ErrNotStreamable", err)
	}
	if resolverCalls != 1 {
		t.Errorf("resolverCalls after cached decline = %d, want 1 (no extra registry lookup)", resolverCalls)
	}
	srv.mu.Lock()
	prepCount2 := len(srv.preparedKeys)
	srv.mu.Unlock()
	if prepCount2 != 1 {
		t.Errorf("preparedKeys after cached decline = %d, want 1", prepCount2)
	}

	// 3. Once the decline TTL expires, CanStream returns true and PrepareLayers tries again.
	time.Sleep(80 * time.Millisecond)
	can, err = driver.CanStream(ctx, req)
	if err != nil {
		t.Fatalf("CanStream after TTL expiry error: %v", err)
	}
	if !can {
		t.Errorf("CanStream = false after decline TTL expired, want true")
	}
	if _, err := driver.PrepareLayers(ctx, req); !errors.Is(err, imagestreaming.ErrNotStreamable) {
		t.Fatalf("PrepareLayers after TTL expiry error = %v, want ErrNotStreamable", err)
	}
	if resolverCalls != 2 {
		t.Errorf("resolverCalls after TTL expiry = %d, want 2", resolverCalls)
	}
}

func TestCanStream_ChecksLivenessAfterClientCached(t *testing.T) {
	ctx := context.Background()
	srv, driver := setupTestRemoteSnapshotter(t, ProviderRiptide)

	mountDir := t.TempDir()
	diffID := "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	srv.remoteMounts = map[string][]*snapshots.Mount{
		diffID: {{Type: "bind", Source: mountDir}},
	}
	driver.imageResolver = func(_ context.Context, _ string, _ *imagestreaming.AuthConfig) (string, *v1.Config, []string, []string, error) {
		return "sha256:ok", &v1.Config{Cmd: []string{"/app"}}, []string{diffID}, []string{"sha256:l1"}, nil
	}

	// Warm up driver.snapshotsClient via PrepareLayers.
	if _, err := driver.PrepareLayers(ctx, &imagestreaming.StreamRequest{ImageRef: "example.com/warm:v1"}); err != nil {
		t.Fatalf("PrepareLayers: %v", err)
	}
	driver.mu.Lock()
	hasClient := driver.snapshotsClient != nil
	driver.mu.Unlock()
	if !hasClient {
		t.Fatal("expected driver.snapshotsClient to be cached after PrepareLayers")
	}

	// Simulate the snapshotter daemon becoming unavailable after the client was cached.
	srv.mu.Lock()
	srv.statFunc = func(context.Context, *snapshots.StatSnapshotRequest) (*snapshots.StatSnapshotResponse, error) {
		return nil, status.Error(codes.Unavailable, "daemon unhealthy")
	}
	srv.mu.Unlock()

	can, err := driver.CanStream(ctx, &imagestreaming.StreamRequest{ImageRef: "example.com/other:v1"})
	if err != nil {
		t.Fatalf("CanStream error: %v", err)
	}
	if can {
		t.Errorf("CanStream = true when cached client's daemon is Unavailable, want false")
	}
}

func TestPrepareLayers_ConcurrentDeduplication(t *testing.T) {
	ctx := context.Background()
	srv, driver := setupTestRemoteSnapshotter(t, ProviderRiptide)

	mountDir := t.TempDir()
	diffID := "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	srv.remoteMounts = map[string][]*snapshots.Mount{
		diffID: {{Type: "bind", Source: mountDir}},
	}

	const numCallers = 8
	resolverEntered := make(chan struct{})
	releaseResolver := make(chan struct{})
	var resolverMu sync.Mutex
	var resolverCalls int

	driver.imageResolver = func(_ context.Context, _ string, _ *imagestreaming.AuthConfig) (string, *v1.Config, []string, []string, error) {
		resolverMu.Lock()
		resolverCalls++
		callNum := resolverCalls
		resolverMu.Unlock()
		if callNum == 1 {
			close(resolverEntered)
			<-releaseResolver
		}
		return "sha256:concurrent", &v1.Config{Cmd: []string{"/concurrent-app"}}, []string{diffID}, []string{"sha256:l1"}, nil
	}

	ref := "example.com/concurrent:v1"
	results := make([]*imagestreaming.StreamResult, numCallers)
	errs := make([]error, numCallers)

	var wg sync.WaitGroup
	for i := range numCallers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx] = driver.PrepareLayers(ctx, &imagestreaming.StreamRequest{ImageRef: ref})
		}(i)
	}

	// Wait until the leader goroutine is inside imageResolver, give the other
	// goroutines a moment to queue on d.inflight[ref], then release the leader.
	<-resolverEntered
	time.Sleep(20 * time.Millisecond)
	close(releaseResolver)
	wg.Wait()

	for i := range numCallers {
		if errs[i] != nil {
			t.Fatalf("caller %d PrepareLayers error: %v", i, errs[i])
		}
		if results[i] == nil || results[i].Config == nil || !slices.Equal(results[i].Config.Cmd, []string{"/concurrent-app"}) {
			t.Errorf("caller %d result = %+v, want Cmd=[/concurrent-app]", i, results[i])
		}
	}

	if resolverCalls != 1 {
		t.Errorf("resolverCalls = %d, want 1", resolverCalls)
	}
	srv.mu.Lock()
	prepCalls := len(srv.preparedKeys)
	viewCalls := len(srv.viewedKeys)
	srv.mu.Unlock()
	if prepCalls != 1 || viewCalls != 1 {
		t.Errorf("Prepare/View calls = %d/%d, want 1/1", prepCalls, viewCalls)
	}

	driver.mu.Lock()
	refCount := driver.leases[ref].refCount
	workDir := driver.leases[ref].workDir
	driver.mu.Unlock()
	if refCount != numCallers {
		t.Fatalf("lease refCount = %d, want %d", refCount, numCallers)
	}

	// Releasing numCallers-1 times keeps the lease active; the final release cleans up.
	for i := range numCallers - 1 {
		if err := driver.ReleaseLayers(ctx, &imagestreaming.StreamRequest{ImageRef: ref}); err != nil {
			t.Fatalf("ReleaseLayers(%d): %v", i, err)
		}
	}
	if _, err := os.Stat(workDir); err != nil {
		t.Fatalf("workDir removed with 1 active reference remaining: %v", err)
	}
	if err := driver.ReleaseLayers(ctx, &imagestreaming.StreamRequest{ImageRef: ref}); err != nil {
		t.Fatalf("final ReleaseLayers: %v", err)
	}
	if _, err := os.Stat(workDir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("workDir %s still exists after final release: err=%v", workDir, err)
	}
}

func TestPrepareLayers_CRILabels(t *testing.T) {
	ctx := context.Background()
	srv, driver := setupTestRemoteSnapshotter(t, ProviderRiptide)

	diff0 := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	diff1 := "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	chain := computeChainIDs([]string{diff0, diff1})

	mount0 := t.TempDir()
	mount1 := t.TempDir()
	driver.imageResolver = func(context.Context, string, *imagestreaming.AuthConfig) (string, *v1.Config, []string, []string, error) {
		return "sha256:manifestdigest", &v1.Config{}, []string{diff0, diff1}, []string{"sha256:blob0", "sha256:blob1"}, nil
	}

	var gotLabels []map[string]string
	srv.prepareFunc = func(_ context.Context, req *snapshots.PrepareSnapshotRequest) (*snapshots.PrepareSnapshotResponse, error) {
		gotLabels = append(gotLabels, req.GetLabels())
		target := req.GetLabels()["containerd.io/snapshot.ref"]
		switch target {
		case chain[0].ChainID:
			srv.commitLocked(target, []*snapshots.Mount{{Type: "bind", Source: mount0}})
		case chain[1].ChainID:
			srv.commitLocked(target, []*snapshots.Mount{{Type: "bind", Source: mount1}})
		}
		return nil, status.Errorf(codes.AlreadyExists, "target snapshot %q: already exists", target)
	}

	req := &imagestreaming.StreamRequest{ImageRef: "us-docker.pkg.dev/proj/repo/image:tag"}
	if _, err := driver.PrepareLayers(ctx, req); err != nil {
		t.Fatalf("PrepareLayers error: %v", err)
	}
	if len(gotLabels) != 2 {
		t.Fatalf("got %d Prepare calls, want 2", len(gotLabels))
	}

	wantImageRef := "us-docker.pkg.dev/proj/repo/image@sha256:manifestdigest"
	if got := gotLabels[0]["containerd.io/snapshot/cri.image-ref"]; got != wantImageRef {
		t.Errorf("layer 0 cri.image-ref = %q, want %q", got, wantImageRef)
	}
	if got := gotLabels[0]["containerd.io/snapshot/cri.image-layers"]; got != "sha256:blob0,sha256:blob1" {
		t.Errorf("layer 0 cri.image-layers = %q, want %q", got, "sha256:blob0,sha256:blob1")
	}
	if got := gotLabels[1]["containerd.io/snapshot/cri.image-layers"]; got != "sha256:blob1" {
		t.Errorf("layer 1 cri.image-layers = %q, want shrinking suffix %q", got, "sha256:blob1")
	}
}

func TestPrepareLayers_ConcurrentSharedBaseLayerLocking(t *testing.T) {
	ctx := context.Background()
	srv, driver := setupTestRemoteSnapshotter(t, ProviderRiptide)

	baseDiff := "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	topDiffA := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	topDiffB := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	chainA := computeChainIDs([]string{baseDiff, topDiffA})
	chainB := computeChainIDs([]string{baseDiff, topDiffB})

	mountBase := t.TempDir()
	mountTopA := t.TempDir()
	mountTopB := t.TempDir()

	driver.imageResolver = func(_ context.Context, ref string, _ *imagestreaming.AuthConfig) (string, *v1.Config, []string, []string, error) {
		switch ref {
		case "example.com/app-a:v1":
			return "sha256:digesta", &v1.Config{}, []string{baseDiff, topDiffA}, []string{"sha256:base", "sha256:topa"}, nil
		case "example.com/app-b:v1":
			return "sha256:digestb", &v1.Config{}, []string{baseDiff, topDiffB}, []string{"sha256:base", "sha256:topb"}, nil
		default:
			return "", nil, nil, nil, errors.New("unexpected ref")
		}
	}

	var basePrepareCalls int
	srv.prepareFunc = func(_ context.Context, req *snapshots.PrepareSnapshotRequest) (*snapshots.PrepareSnapshotResponse, error) {
		target := req.GetLabels()["containerd.io/snapshot.ref"]
		switch target {
		case chainA[0].ChainID:
			basePrepareCalls++
			// Hold the first Prepare on the shared base layer briefly so the
			// second image pull would race Prepare if chainID locking were missing.
			srv.mu.Unlock()
			time.Sleep(30 * time.Millisecond)
			srv.mu.Lock()
			srv.commitLocked(target, []*snapshots.Mount{{Type: "bind", Source: mountBase}})
		case chainA[1].ChainID:
			srv.commitLocked(target, []*snapshots.Mount{{Type: "bind", Source: mountTopA}})
		case chainB[1].ChainID:
			srv.commitLocked(target, []*snapshots.Mount{{Type: "bind", Source: mountTopB}})
		}
		return nil, status.Errorf(codes.AlreadyExists, "target snapshot %q: already exists", target)
	}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	refs := []string{"example.com/app-a:v1", "example.com/app-b:v1"}
	for i, r := range refs {
		wg.Add(1)
		go func(idx int, imageRef string) {
			defer wg.Done()
			_, errs[idx] = driver.PrepareLayers(ctx, &imagestreaming.StreamRequest{ImageRef: imageRef})
		}(i, r)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("PrepareLayers(%s) error: %v", refs[i], err)
		}
	}
	srv.mu.Lock()
	gotBasePrepares := basePrepareCalls
	srv.mu.Unlock()
	if gotBasePrepares != 1 {
		t.Errorf("shared base layer Prepare called %d times, want 1 (per-chainID lock should prevent duplicate Prepare)", gotBasePrepares)
	}
}

func TestPrepareLayers_SelfHealsEvictedLeaseLayers(t *testing.T) {
	ctx := context.Background()
	srv, driver := setupTestRemoteSnapshotter(t, ProviderRiptide)

	diff0 := "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	mount1 := filepath.Join(t.TempDir(), "snap-1")
	mount2 := filepath.Join(t.TempDir(), "snap-2")
	if err := os.MkdirAll(mount1, 0o755); err != nil {
		t.Fatalf("MkdirAll(mount1): %v", err)
	}
	if err := os.MkdirAll(mount2, 0o755); err != nil {
		t.Fatalf("MkdirAll(mount2): %v", err)
	}

	srv.remoteMounts = map[string][]*snapshots.Mount{
		diff0: {{Type: "bind", Source: mount1}},
	}
	driver.imageResolver = func(context.Context, string, *imagestreaming.AuthConfig) (string, *v1.Config, []string, []string, error) {
		return "sha256:digest", &v1.Config{}, []string{diff0}, []string{"sha256:blob0"}, nil
	}

	req := &imagestreaming.StreamRequest{ImageRef: "example.com/app:v1"}
	if _, err := driver.PrepareLayers(ctx, req); err != nil {
		t.Fatalf("first PrepareLayers error: %v", err)
	}

	// Simulate external host containerd GC removing the snapshotter view directory.
	if err := os.RemoveAll(mount1); err != nil {
		t.Fatalf("RemoveAll(mount1): %v", err)
	}
	srv.mu.Lock()
	delete(srv.committedKeys, diff0)
	delete(srv.mountsByKey, diff0)
	srv.remoteMounts[diff0] = []*snapshots.Mount{{Type: "bind", Source: mount2}}
	srv.mu.Unlock()

	// Second PrepareLayers must detect the dangling layer-0/fs symlink and re-prepare.
	res2, err := driver.PrepareLayers(ctx, req)
	if err != nil {
		t.Fatalf("second PrepareLayers after eviction error: %v", err)
	}
	target, err := os.Readlink(filepath.Join(res2.LayerDirs[0], "fs"))
	if err != nil {
		t.Fatalf("Readlink(layer-0/fs): %v", err)
	}
	if target != mount2 {
		t.Errorf("healed layer-0/fs points to %q, want %q", target, mount2)
	}
}

type mockRiptideKeychainServer struct {
	riptidekeychain.UnimplementedKeychainServer
	mu      sync.Mutex
	updates []*riptidekeychain.UpdateCredsRequest
}

func (s *mockRiptideKeychainServer) UpdateCreds(_ context.Context, req *riptidekeychain.UpdateCredsRequest) (*riptidekeychain.UpdateCredsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updates = append(s.updates, req)
	return &riptidekeychain.UpdateCredsResponse{}, nil
}

func setupMockRiptideKeychainServer(t *testing.T) (*mockRiptideKeychainServer, string) {
	t.Helper()
	sockPath := filepath.Join(t.TempDir(), "keychain.sock")
	srv := &mockRiptideKeychainServer{}
	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen on keychain socket: %v", err)
	}
	grpcServer := grpc.NewServer()
	riptidekeychain.RegisterKeychainServer(grpcServer, srv)
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
	})
	return srv, sockPath
}

func TestPrepareLayers_RiptideKeychainUpdateCreds(t *testing.T) {
	ctx := context.Background()
	snapSrv, driver := setupTestRemoteSnapshotter(t, ProviderRiptide)
	kcSrv, kcSock := setupMockRiptideKeychainServer(t)
	driver.keychainSocket = kcSock

	diff0 := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	mount0 := t.TempDir()
	driver.imageResolver = func(context.Context, string, *imagestreaming.AuthConfig) (string, *v1.Config, []string, []string, error) {
		return "sha256:manifestdigest", &v1.Config{}, []string{diff0}, []string{"sha256:blob0"}, nil
	}

	var credsPresentAtPrepare bool
	snapSrv.prepareFunc = func(_ context.Context, req *snapshots.PrepareSnapshotRequest) (*snapshots.PrepareSnapshotResponse, error) {
		kcSrv.mu.Lock()
		credsPresentAtPrepare = len(kcSrv.updates) == 1 &&
			kcSrv.updates[0].GetImage() == req.GetLabels()["containerd.io/snapshot/cri.image-ref"]
		kcSrv.mu.Unlock()
		target := req.GetLabels()["containerd.io/snapshot.ref"]
		snapSrv.commitLocked(target, []*snapshots.Mount{{Type: "bind", Source: mount0}})
		return nil, status.Errorf(codes.AlreadyExists, "target snapshot %q: already exists", target)
	}

	req := &imagestreaming.StreamRequest{
		ImageRef: "us-docker.pkg.dev/proj/repo/private:v1",
		AuthConfig: &imagestreaming.AuthConfig{
			Username: "_json_key",
			Password: "secret-token-1",
		},
	}
	if _, err := driver.PrepareLayers(ctx, req); err != nil {
		t.Fatalf("PrepareLayers cold error: %v", err)
	}
	if !credsPresentAtPrepare {
		t.Fatal("expected Riptide Keychain.UpdateCreds to be called with canonical cri.image-ref BEFORE Snapshots.Prepare")
	}

	kcSrv.mu.Lock()
	if len(kcSrv.updates) != 1 {
		t.Fatalf("got %d UpdateCreds calls, want 1", len(kcSrv.updates))
	}
	got := kcSrv.updates[0]
	kcSrv.mu.Unlock()

	wantRef := "us-docker.pkg.dev/proj/repo/private@sha256:manifestdigest"
	if got.GetImage() != wantRef {
		t.Errorf("UpdateCreds image = %q, want %q", got.GetImage(), wantRef)
	}
	if got.GetAuth().GetUsername() != "_json_key" || got.GetAuth().GetPassword() != "secret-token-1" {
		t.Errorf("UpdateCreds auth = %+v, want username=_json_key password=secret-token-1", got.GetAuth())
	}
	if got.GetAuth().GetServerAddress() != "us-docker.pkg.dev" {
		t.Errorf("UpdateCreds ServerAddress = %q, want %q", got.GetAuth().GetServerAddress(), "us-docker.pkg.dev")
	}

	// Warm lease hit with rotated credentials should refresh gcfsd's keychain without re-running Prepare.
	reqWarm := &imagestreaming.StreamRequest{
		ImageRef: "us-docker.pkg.dev/proj/repo/private:v1",
		AuthConfig: &imagestreaming.AuthConfig{
			Username: "_json_key",
			Password: "secret-token-2",
		},
	}
	if _, err := driver.PrepareLayers(ctx, reqWarm); err != nil {
		t.Fatalf("PrepareLayers warm error: %v", err)
	}
	kcSrv.mu.Lock()
	defer kcSrv.mu.Unlock()
	if len(kcSrv.updates) != 2 {
		t.Fatalf("got %d UpdateCreds calls after warm PrepareLayers, want 2", len(kcSrv.updates))
	}
	if kcSrv.updates[1].GetAuth().GetPassword() != "secret-token-2" {
		t.Errorf("warm UpdateCreds password = %q, want secret-token-2", kcSrv.updates[1].GetAuth().GetPassword())
	}
}

type basicAuthTestKeychain struct {
	username string
	password string
}

func (k *basicAuthTestKeychain) Resolve(_ authn.Resource) (authn.Authenticator, error) {
	return &authn.Basic{Username: k.username, Password: k.password}, nil
}

func TestPrepareLayers_RiptideKeychainFromDriverKeychain(t *testing.T) {
	ctx := context.Background()
	snapSrv, driver := setupTestRemoteSnapshotter(t, ProviderRiptide)
	kcSrv, kcSock := setupMockRiptideKeychainServer(t)
	driver.keychainSocket = kcSock
	driver.keychain = &basicAuthTestKeychain{username: "k8s-pull-user", password: "k8s-pull-pass"}

	diff0 := "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	mount0 := t.TempDir()
	snapSrv.remoteMounts = map[string][]*snapshots.Mount{
		diff0: {{Type: "bind", Source: mount0}},
	}
	driver.imageResolver = func(context.Context, string, *imagestreaming.AuthConfig) (string, *v1.Config, []string, []string, error) {
		return "sha256:kcmanifest", &v1.Config{}, []string{diff0}, []string{"sha256:blob0"}, nil
	}

	if _, err := driver.PrepareLayers(ctx, &imagestreaming.StreamRequest{ImageRef: "ghcr.io/org/private:v2"}); err != nil {
		t.Fatalf("PrepareLayers error: %v", err)
	}

	kcSrv.mu.Lock()
	defer kcSrv.mu.Unlock()
	if len(kcSrv.updates) != 1 {
		t.Fatalf("got %d UpdateCreds calls, want 1", len(kcSrv.updates))
	}
	got := kcSrv.updates[0]
	if got.GetImage() != "ghcr.io/org/private@sha256:kcmanifest" {
		t.Errorf("UpdateCreds image = %q, want ghcr.io/org/private@sha256:kcmanifest", got.GetImage())
	}
	if got.GetAuth().GetUsername() != "k8s-pull-user" || got.GetAuth().GetPassword() != "k8s-pull-pass" {
		t.Errorf("UpdateCreds auth = %+v, want k8s-pull-user/k8s-pull-pass", got.GetAuth())
	}
	if got.GetAuth().GetServerAddress() != "ghcr.io" {
		t.Errorf("UpdateCreds ServerAddress = %q, want ghcr.io", got.GetAuth().GetServerAddress())
	}
}

func TestNewSOCI_DoesNotConfigureRiptideKeychainSocket(t *testing.T) {
	_, sociDriver := setupTestRemoteSnapshotter(t, ProviderSOCI)
	if sociDriver.keychainSocket != "" {
		t.Errorf("sociDriver.keychainSocket = %q, want empty string", sociDriver.keychainSocket)
	}
}

func TestPrepareLayers_CachesResolvedMetadataAcrossZeroToOneTransitions(t *testing.T) {
	ctx := context.Background()
	srv, driver := setupTestRemoteSnapshotter(t, ProviderRiptide)
	kcSrv, kcSock := setupMockRiptideKeychainServer(t)
	driver.keychainSocket = kcSock

	diff0 := "sha256:9999999999999999999999999999999999999999999999999999999999999999"
	mount0 := t.TempDir()
	srv.remoteMounts = map[string][]*snapshots.Mount{
		diff0: {{Type: "bind", Source: mount0}},
	}

	var resolverCalls int
	driver.imageResolver = func(context.Context, string, *imagestreaming.AuthConfig) (string, *v1.Config, []string, []string, error) {
		resolverCalls++
		if resolverCalls > 1 {
			return "", nil, nil, nil, errors.New("UNAUTHORIZED: token expired")
		}
		return "sha256:cachedmanifest", &v1.Config{Cmd: []string{"/cached-app"}}, []string{diff0}, []string{"sha256:blob0"}, nil
	}

	req1 := &imagestreaming.StreamRequest{
		ImageRef: "us-docker.pkg.dev/proj/repo/private:v1",
		AuthConfig: &imagestreaming.AuthConfig{
			Username: "oauth2accesstoken",
			Password: "token-1",
		},
	}

	// First 0 -> 1 transition resolves image metadata and commits chainID in snapshotter.
	res1, err := driver.PrepareLayers(ctx, req1)
	if err != nil {
		t.Fatalf("first PrepareLayers error: %v", err)
	}
	if resolverCalls != 1 {
		t.Fatalf("resolverCalls = %d, want 1", resolverCalls)
	}

	// Release drops refCount 1 -> 0 and removes view snapshot + workDir.
	if err := driver.ReleaseLayers(ctx, req1); err != nil {
		t.Fatalf("ReleaseLayers error: %v", err)
	}
	if _, err := os.Stat(filepath.Dir(res1.LayerDirs[0])); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected workDir removed after ReleaseLayers, got err=%v", err)
	}

	// Second 0 -> 1 transition must reuse cached resolvedImageMeta without
	// calling imageResolver over the network, while still pushing credentials
	// to Riptide keychain and creating a fresh view from the committed chainID.
	req2 := &imagestreaming.StreamRequest{
		ImageRef: "us-docker.pkg.dev/proj/repo/private:v1",
		AuthConfig: &imagestreaming.AuthConfig{
			Username: "oauth2accesstoken",
			Password: "token-2",
		},
	}
	res2, err := driver.PrepareLayers(ctx, req2)
	if err != nil {
		t.Fatalf("second PrepareLayers after ReleaseLayers error: %v", err)
	}
	if resolverCalls != 1 {
		t.Errorf("resolverCalls after 0->1 re-prepare = %d, want 1 (cached metadata must avoid network fetch)", resolverCalls)
	}
	if res2.Config == nil || !slices.Equal(res2.Config.Cmd, []string{"/cached-app"}) {
		t.Errorf("res2.Config = %+v, want Cmd=[/cached-app]", res2.Config)
	}
	kcSrv.mu.Lock()
	defer kcSrv.mu.Unlock()
	if len(kcSrv.updates) != 2 || kcSrv.updates[1].GetAuth().GetPassword() != "token-2" {
		t.Errorf("keychain updates = %+v, want 2 updates with latest password token-2", kcSrv.updates)
	}
}

func TestReleaseAndPrepareLayers_ConcurrentZeroTransitionNoRace(t *testing.T) {
	ctx := context.Background()
	srv, driver := setupTestRemoteSnapshotter(t, ProviderRiptide)

	diff0 := "sha256:8888888888888888888888888888888888888888888888888888888888888888"
	mount0 := t.TempDir()
	srv.remoteMounts = map[string][]*snapshots.Mount{
		diff0: {{Type: "bind", Source: mount0}},
	}
	driver.imageResolver = func(context.Context, string, *imagestreaming.AuthConfig) (string, *v1.Config, []string, []string, error) {
		return "sha256:racedigest", &v1.Config{Cmd: []string{"/race-app"}}, []string{diff0}, []string{"sha256:blob0"}, nil
	}

	req := &imagestreaming.StreamRequest{ImageRef: "example.com/race:v1"}
	if _, err := driver.PrepareLayers(ctx, req); err != nil {
		t.Fatalf("initial PrepareLayers error: %v", err)
	}

	removeEntered := make(chan struct{})
	releaseRemove := make(chan struct{})
	var once sync.Once
	srv.mu.Lock()
	srv.removeFunc = func(context.Context, *snapshots.RemoveSnapshotRequest) (*emptypb.Empty, error) {
		once.Do(func() {
			close(removeEntered)
			srv.mu.Unlock()
			<-releaseRemove
			srv.mu.Lock()
		})
		return &emptypb.Empty{}, nil
	}
	srv.mu.Unlock()

	var (
		wg      sync.WaitGroup
		prepRes *imagestreaming.StreamResult
		prepErr error
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = driver.ReleaseLayers(ctx, req)
	}()

	// Wait until ReleaseLayers has dropped refCount to 0 and is inside client.Remove
	// (before os.RemoveAll(lease.workDir)).
	<-removeEntered

	// Start a concurrent PrepareLayers (0 -> 1) for the same image while ReleaseLayers
	// is still tearing down the old lease.
	wg.Add(1)
	go func() {
		defer wg.Done()
		prepRes, prepErr = driver.PrepareLayers(ctx, req)
	}()

	time.Sleep(20 * time.Millisecond)
	close(releaseRemove)
	wg.Wait()

	if prepErr != nil {
		t.Fatalf("concurrent PrepareLayers error: %v", prepErr)
	}
	if prepRes == nil || len(prepRes.LayerDirs) != 1 {
		t.Fatalf("concurrent PrepareLayers result = %+v, want 1 LayerDir", prepRes)
	}
	// Verify that ReleaseLayers's os.RemoveAll(workDir) did NOT delete the new lease's wrapper directory.
	fsLink := filepath.Join(prepRes.LayerDirs[0], "fs")
	target, err := os.Readlink(fsLink)
	if err != nil {
		t.Fatalf("new lease layer-0/fs missing after concurrent ReleaseLayers: %v", err)
	}
	if target != mount0 {
		t.Errorf("new lease layer-0/fs = %q, want %q", target, mount0)
	}
}
