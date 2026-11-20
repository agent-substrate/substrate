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
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"

	providerclient "github.com/ai-on-gke/image-provider-plugin/client"
	providermock "github.com/ai-on-gke/image-provider-plugin/client/mock"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/imagecache"
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

// fakeSuspendWorkers records RequestActorSuspend calls and fails the first
// failFirst of them.
type fakeSuspendWorkers struct {
	ateapi.WorkerServiceClient
	mu        sync.Mutex
	failFirst int
	requests  []*ateapi.RequestActorSuspendRequest
}

func (f *fakeSuspendWorkers) RequestActorSuspend(_ context.Context, req *ateapi.RequestActorSuspendRequest, _ ...grpc.CallOption) (*ateapi.RequestActorSuspendResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, req)
	if len(f.requests) <= f.failFirst {
		return nil, status.Error(codes.Unavailable, "control plane unavailable")
	}
	return &ateapi.RequestActorSuspendResponse{}, nil
}

// Only running actors that hold a lease are suspended, once each, and a
// failed request is retried.
func TestSuspendStreamedActors(t *testing.T) {
	actorsDir, workersDir := t.TempDir(), t.TempDir()
	writeTestBundle(t, actorsDir, "actor-streamed", "main", &imagecache.OverlaySpec{
		ImageRef: "registry.example/app:1", StoreLayers: []string{"/cache/aaa"},
		ImageVolumes: []imagecache.ImageVolumeOverlay{{Name: "tools", ImageRef: "registry.example/tools:1", LeaseID: "actor-streamed/main/tools", LowerDirs: []string{"/plugin/0"}}},
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
		{Atespace: "ns", ActorName: "local", ActorUid: "actor-local"},
	}})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	origSocketPath := workerSocketPath
	workerSocketPath = func(string) string { return sock }
	t.Cleanup(func() { workerSocketPath = origSocketPath })

	workers := &fakeSuspendWorkers{failFirst: 1}
	s := &Atelet{workerDialer: newWorkerDialer(1), workers: workers, drainSuspended: map[string]bool{}}

	for i, want := range []int{0, 1, 0} {
		if got := s.suspendStreamedActors(t.Context(), actorsDir, workersDir); got != want {
			t.Errorf("suspendStreamedActors call %d = %d, want %d", i, got, want)
		}
	}
	want := &ateapi.RequestActorSuspendRequest{
		Worker:   &ateapi.ObjectRef{Name: podUID},
		Actor:    &ateapi.ObjectRef{Atespace: "ns", Name: "streamed"},
		ActorUid: "actor-streamed",
	}
	if diff := cmp.Diff([]*ateapi.RequestActorSuspendRequest{want, want}, workers.requests, protocmp.Transform()); diff != "" {
		t.Errorf("RequestActorSuspend calls mismatch (-want +got):\n%s", diff)
	}
}
