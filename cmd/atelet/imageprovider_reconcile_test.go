// Copyright 2026 The Agent Substrate Authors
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
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"

	providerclient "github.com/ai-on-gke/image-provider-plugin/client"
	providermock "github.com/ai-on-gke/image-provider-plugin/client/mock"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/preview"
)

// writeTestBundle writes spec as the overlay spec of actorUID's container
// bundle under actorsDir.
func writeTestBundle(t *testing.T, actorsDir, actorUID, container string, spec *imagecache.OverlaySpec) {
	t.Helper()
	dir := filepath.Join(actorsDir, actorUID, "bundles", container)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := imagecache.WriteSpec(dir, spec); err != nil {
		t.Fatal(err)
	}
}

// The plugin learns every lease the bundles record, and nothing about images
// from the cache.
func TestReconcileProviderLeases(t *testing.T) {
	actorsDir := t.TempDir()
	const app, tools, local = "registry.example/app:1", "registry.example/tools:1", "registry.example/local:1"
	writeTestBundle(t, actorsDir, "actor-1", "main", &imagecache.OverlaySpec{
		ImageRef: app, ImageDigest: "sha256:aaa", LeaseID: "actor-1/main", LowerDirs: []string{"/plugin/app/0"},
		ImageVolumes: []imagecache.ImageVolumeOverlay{{
			Name: "tools", ImageRef: tools, ImageDigest: "sha256:bbb", LeaseID: "actor-1/main/tools", LowerDirs: []string{"/plugin/tools/0"},
		}},
	})
	writeTestBundle(t, actorsDir, "actor-2", "main", &imagecache.OverlaySpec{
		ImageRef: app, ImageDigest: "sha256:aaa", LeaseID: "actor-2/main", LowerDirs: []string{"/plugin/app/0"},
	})
	writeTestBundle(t, actorsDir, "actor-3", "main", &imagecache.OverlaySpec{
		ImageRef: local, ImageDigest: "sha256:ccc", StoreLayers: []string{"/cache/ccc"},
	})

	provider := providermock.New("nydus")
	s := &Atelet{imageProvider: provider}
	if err := s.reconcileProviderLeases(t.Context(), actorsDir); err != nil {
		t.Fatalf("reconcileProviderLeases: %v", err)
	}
	want := [][]*providerclient.ActiveLease{{
		{LeaseID: "actor-1/main", ImageRef: app, ImageDigest: "sha256:aaa"},
		{LeaseID: "actor-1/main/tools", ImageRef: tools, ImageDigest: "sha256:bbb"},
		{LeaseID: "actor-2/main", ImageRef: app, ImageDigest: "sha256:aaa"},
	}}
	if diff := cmp.Diff(want, provider.ReconcileCalls); diff != "" {
		t.Errorf("ReconcileImages calls mismatch (-want +got):\n%s", diff)
	}
}

// A reconcile replaces the plugin's whole lease set, so an unreadable spec
// must stop it rather than drop the leases that spec records.
func TestReconcileProviderLeasesUnreadableSpec(t *testing.T) {
	actorsDir := t.TempDir()
	writeTestBundle(t, actorsDir, "actor-1", "main", &imagecache.OverlaySpec{
		ImageRef: "registry.example/app:1", ImageDigest: "sha256:aaa", LeaseID: "actor-1/main", LowerDirs: []string{"/plugin/app/0"},
	})
	bad := filepath.Join(actorsDir, "actor-2", "bundles", "main")
	if err := os.MkdirAll(bad, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bad, imagecache.OverlaySpecFileName), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}

	provider := providermock.New("riptide")
	s := &Atelet{imageProvider: provider}
	if err := s.reconcileProviderLeases(t.Context(), actorsDir); err == nil {
		t.Fatal("reconcileProviderLeases succeeded with an unreadable spec")
	}
	if len(provider.ReconcileCalls) != 0 {
		t.Errorf("ReconcileImages calls = %v, want none", provider.ReconcileCalls)
	}
}

// A reconcile sends the lease a bundle is taking without waiting for the
// pull, the spec's lease once the bundle records it, and nothing once the
// bundle releases it, even if the release fails.
func TestReconcileProviderLeasesWithBundles(t *testing.T) {
	preview.InitForTest(t, string(preview.GateImageProviderPlugin))
	useTempNodeDirs(t)
	const ref = "registry.example/app:1"
	provider := providermock.New("nydus")
	s := &Atelet{imageProvider: provider, imageCache: newImageVolumeStore(t)}
	rootfs := streamedRootfs(t)
	provider.PrepareImageFunc = func(ctx context.Context, _ *providerclient.PrepareRequest) (*providerclient.PrepareResult, error) {
		if err := s.reconcileProviderLeases(ctx, nodepath.ActorsDir); err != nil {
			return nil, err
		}
		return streamedResult(rootfs), nil
	}
	provider.ReleaseImageFunc = func(context.Context, *providerclient.ReleaseRequest) error {
		return errors.New("plugin unavailable")
	}

	if _, _, err := prepareOCIDirectory(t.Context(), s.imageSource(), "actor-1", "app", ref, nil, nil); err != nil {
		t.Fatalf("prepareOCIDirectory: %v", err)
	}
	if err := s.reconcileProviderLeases(t.Context(), nodepath.ActorsDir); err != nil {
		t.Fatalf("reconcileProviderLeases: %v", err)
	}
	s.releaseProviderLeases(t.Context(), "actor-1")
	if err := s.reconcileProviderLeases(t.Context(), nodepath.ActorsDir); err != nil {
		t.Fatalf("reconcileProviderLeases: %v", err)
	}

	want := [][]*providerclient.ActiveLease{
		{{LeaseID: "actor-1/app", ImageRef: ref}},
		{{LeaseID: "actor-1/app", ImageRef: ref, ImageDigest: testStreamedDigest}},
		nil,
	}
	if diff := cmp.Diff(want, provider.ReconcileCalls, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("ReconcileImages calls mismatch (-want +got):\n%s", diff)
	}
}

// A restarted plugin listens on a new socket file. atelet must reconcile
// then, also when the socket is back before the next check.
func TestNewProviderSocket(t *testing.T) {
	// Unix socket paths are short, so the socket goes in its own temp dir.
	dir, err := os.MkdirTemp("", "ipp-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "p.sock")
	listen := func() os.FileInfo {
		t.Helper()
		os.Remove(sock)
		lis, err := net.Listen("unix", sock)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { lis.Close() })
		info := providerSocketInfo(sock)
		if info == nil {
			t.Fatalf("providerSocketInfo(%q) = nil for a listening socket", sock)
		}
		return info
	}
	if info := providerSocketInfo(sock); info != nil {
		t.Fatalf("providerSocketInfo(%q) = %v before the socket exists", sock, info)
	}
	first := listen()
	same := providerSocketInfo(sock)
	second := listen()

	for _, tc := range []struct {
		name      string
		last, cur os.FileInfo
		want      bool
	}{
		{"no socket", nil, nil, false},
		{"socket appears", nil, first, true},
		{"same socket", first, same, false},
		{"socket replaced", first, second, true},
		{"socket gone", first, nil, false},
	} {
		if got := newProviderSocket(tc.last, tc.cur); got != tc.want {
			t.Errorf("%s: newProviderSocket = %v, want %v", tc.name, got, tc.want)
		}
	}
}
