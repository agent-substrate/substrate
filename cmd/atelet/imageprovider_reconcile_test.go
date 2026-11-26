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
	"slices"
	"strings"
	"testing"
	"time"

	imageprovider "github.com/ai-on-gke/image-provider-plugin/api/imageprovider/v1alpha1"
	providerclient "github.com/ai-on-gke/image-provider-plugin/client"
	providermock "github.com/ai-on-gke/image-provider-plugin/client/mock"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/testing/protocmp"
	clocktesting "k8s.io/utils/clock/testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
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
// from the cache. atelet records the lost leases that a spec records, asks
// again to suspend their actors, and marks the auth keys the plugin asks to
// refresh.
func TestReconcileProviderLeases(t *testing.T) {
	const app, tools, local = "registry.example/app:1", "registry.example/tools:1", "registry.example/local:1"
	const appKey, toolsKey = "registry.example/app", "registry.example/tools"
	for _, tc := range []struct {
		name        string
		result      *providerclient.ReconcileResult
		wantLost    map[string]bool
		wantRefresh bool
		// wantNeedsRefresh lists the auth keys that need a refresh after
		// the reconcile. Both start as AUTH_REFRESH_NOT_NEEDED.
		wantNeedsRefresh []string
	}{{
		name:   "plugin restores every lease",
		result: &providerclient.ReconcileResult{},
	}, {
		name: "lost leases",
		result: &providerclient.ReconcileResult{LostLeases: []providerclient.LostLease{
			{LeaseID: "actor-2/main", Message: "image gone"},
			// No spec records it, so it is a lease being taken.
			{LeaseID: "actor-9/main", Message: "no record"},
		}},
		wantLost: map[string]bool{"actor-2/main": true},
	}, {
		name:             "auth refresh required",
		result:           &providerclient.ReconcileResult{AuthRefreshRequired: []string{toolsKey}},
		wantRefresh:      true,
		wantNeedsRefresh: []string{toolsKey},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			actorsDir := t.TempDir()
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
			provider.ReconcileFunc = func(context.Context, []string) (*providerclient.ReconcileResult, error) {
				return tc.result, nil
			}
			s := &Atelet{imageProvider: provider, drain: newDrainSuspender(newFakeDrainWorkers(), clocktesting.NewFakePassiveClock(time.Unix(0, 0)))}
			// actor-2 was asked to suspend before.
			s.drain.actors["actor-2"] = &drainSuspend{asked: true}
			s.providerAuth.prepared(appKey, imageprovider.AuthRefresh_AUTH_REFRESH_NOT_NEEDED)
			s.providerAuth.prepared(toolsKey, imageprovider.AuthRefresh_AUTH_REFRESH_NOT_NEEDED)
			refresh, err := s.reconcileProviderLeases(t.Context(), actorsDir)
			if err != nil {
				t.Fatalf("reconcileProviderLeases: %v", err)
			}
			want := [][]string{{"actor-1/main", "actor-1/main/tools", "actor-2/main"}}
			if diff := cmp.Diff(want, provider.ReconcileCalls); diff != "" {
				t.Errorf("ReconcileImages calls mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantLost, s.providerLeases.lostLeases(), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("lost leases mismatch (-want +got):\n%s", diff)
			}
			if asked := s.drain.actors["actor-2"].asked; asked == tc.wantLost["actor-2/main"] {
				t.Errorf("actor-2 asked = %v after the reconcile, want %v", asked, !tc.wantLost["actor-2/main"])
			}
			if refresh != tc.wantRefresh {
				t.Errorf("reconcileProviderLeases refresh = %v, want %v", refresh, tc.wantRefresh)
			}
			var needs []string
			for _, k := range []string{appKey, toolsKey} {
				if need, _ := s.providerAuth.needsRefresh(k); need {
					needs = append(needs, k)
				}
			}
			if diff := cmp.Diff(tc.wantNeedsRefresh, needs); diff != "" {
				t.Errorf("auth keys that need a refresh mismatch (-want +got):\n%s", diff)
			}
		})
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
	if _, err := s.reconcileProviderLeases(t.Context(), actorsDir); err == nil {
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
		if _, err := s.reconcileProviderLeases(ctx, nodepath.ActorsDir); err != nil {
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
	if _, err := s.reconcileProviderLeases(t.Context(), nodepath.ActorsDir); err != nil {
		t.Fatalf("reconcileProviderLeases: %v", err)
	}
	s.releaseProviderLeases(t.Context(), "actor-1")
	if _, err := s.reconcileProviderLeases(t.Context(), nodepath.ActorsDir); err != nil {
		t.Fatalf("reconcileProviderLeases: %v", err)
	}

	want := [][]string{{"actor-1/app"}, {"actor-1/app"}, nil}
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

type fakeStatsWorkerServer struct {
	ateworkerpb.UnimplementedWorkerServer
	samples []*ateworkerpb.WorkloadStatsSample
}

func (f *fakeStatsWorkerServer) GetActiveWorkloadStats(context.Context, *ateworkerpb.GetActiveWorkloadStatsRequest) (*ateworkerpb.GetActiveWorkloadStatsResponse, error) {
	return &ateworkerpb.GetActiveWorkloadStatsResponse{Samples: f.samples}, nil
}

// While the plugin drains, only running actors that hold a lease are asked to
// suspend, once each. Otherwise only those that hold a lost lease are.
func TestStartProviderSuspends(t *testing.T) {
	actorsDir, workersDir := t.TempDir(), t.TempDir()
	writeTestBundle(t, actorsDir, "actor-streamed", "main", &imagecache.OverlaySpec{
		ImageRef: "registry.example/app:1", StoreLayers: []string{"/cache/aaa"},
		ImageVolumes: []imagecache.ImageVolumeOverlay{{Name: "tools", ImageRef: "registry.example/tools:1", LeaseID: "actor-streamed/main/tools", LowerDirs: []string{"/plugin/0"}}},
	})
	writeTestBundle(t, actorsDir, "actor-other", "main", &imagecache.OverlaySpec{
		ImageRef: "registry.example/app:1", LeaseID: "actor-other/main", LowerDirs: []string{"/plugin/1"},
	})
	writeTestBundle(t, actorsDir, "actor-local", "main", &imagecache.OverlaySpec{
		ImageRef: "registry.example/local:1", StoreLayers: []string{"/cache/ccc"},
	})

	const podUID = "worker-pod-1"
	if err := os.MkdirAll(filepath.Join(workersDir, podUID), 0o700); err != nil {
		t.Fatal(err)
	}
	// Unix socket paths are short, so the socket goes in its own temp dir.
	sockDir, err := os.MkdirTemp("", "worker-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	sock := filepath.Join(sockDir, "w.sock")
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	ateworkerpb.RegisterWorkerServer(srv, &fakeStatsWorkerServer{samples: []*ateworkerpb.WorkloadStatsSample{
		{Atespace: "ns", ActorName: "streamed", ActorUid: "actor-streamed"},
		{Atespace: "ns", ActorName: "other", ActorUid: "actor-other"},
		{Atespace: "ns", ActorName: "local", ActorUid: "actor-local"},
	}})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	origSocketPath := workerSocketPath
	workerSocketPath = func(string) string { return sock }
	t.Cleanup(func() { workerSocketPath = origSocketPath })

	request := func(name string) *ateapi.RequestActorSuspendRequest {
		return &ateapi.RequestActorSuspendRequest{
			Worker:   &ateapi.ObjectRef{Name: podUID},
			Actor:    &ateapi.ObjectRef{Atespace: "ns", Name: name},
			ActorUid: "actor-" + name,
		}
	}
	for _, tc := range []struct {
		name     string
		draining bool
		lost     []string
		want     []*ateapi.RequestActorSuspendRequest
	}{{
		name: "serving plugin with every lease",
	}, {
		name:     "draining plugin",
		draining: true,
		want:     []*ateapi.RequestActorSuspendRequest{request("other"), request("streamed")},
	}, {
		name: "lost lease",
		lost: []string{"actor-streamed/main/tools"},
		want: []*ateapi.RequestActorSuspendRequest{request("streamed")},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			workers := newFakeDrainWorkers()
			s := &Atelet{workerDialer: newWorkerDialer(1), drain: newDrainSuspender(workers, clocktesting.NewFakePassiveClock(time.Unix(0, 0)))}
			if len(tc.lost) > 0 {
				s.providerLeases.lost = map[string]bool{}
				for _, id := range tc.lost {
					s.providerLeases.lost[id] = true
				}
			}
			for i, want := range []int{len(tc.want), 0} {
				got, wait := s.startProviderSuspends(t.Context(), actorsDir, workersDir, tc.draining)
				wait()
				if got != want {
					t.Errorf("startProviderSuspends call %d started %d, want %d", i, got, want)
				}
			}
			slices.SortFunc(workers.requests, func(a, b *ateapi.RequestActorSuspendRequest) int {
				return strings.Compare(a.GetActorUid(), b.GetActorUid())
			})
			if diff := cmp.Diff(tc.want, workers.requests, protocmp.Transform(), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("RequestActorSuspend calls mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
