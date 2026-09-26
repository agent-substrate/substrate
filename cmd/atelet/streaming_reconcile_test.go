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
	"os"
	"path/filepath"
	"testing"

	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/imagestreaming"
	"github.com/agent-substrate/substrate/internal/imagestreaming/mock"
)

func TestScanActiveStreamedLeases(t *testing.T) {
	actorsDir := t.TempDir()

	// Actor 1: Two bundles referencing the same streamed image (refcount should be 2)
	a1Bundle1 := filepath.Join(actorsDir, "actor-1", "bundles", "c1")
	a1Bundle2 := filepath.Join(actorsDir, "actor-1", "bundles", "c2")
	if err := os.MkdirAll(a1Bundle1, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(a1Bundle2, 0o755); err != nil {
		t.Fatal(err)
	}

	streamedImg1 := "us-docker.pkg.dev/test/image:v1"
	streamedDigest1 := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	streamedLayers1 := []string{"/run/ate/streaming/riptide/image-v1/layer-0", "/run/ate/streaming/riptide/image-v1/layer-1"}

	if err := imagecache.WriteSpec(a1Bundle1, &imagecache.OverlaySpec{
		ImageRef:    streamedImg1,
		ImageDigest: streamedDigest1,
		Layers:      streamedLayers1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := imagecache.WriteSpec(a1Bundle2, &imagecache.OverlaySpec{
		ImageRef:    streamedImg1,
		ImageDigest: streamedDigest1,
		Layers:      streamedLayers1,
	}); err != nil {
		t.Fatal(err)
	}

	// Actor 2: One non-streamed bundle, and one streamed bundle (SOCI)
	a2Bundle1 := filepath.Join(actorsDir, "actor-2", "bundles", "non-streamed")
	a2Bundle2 := filepath.Join(actorsDir, "actor-2", "bundles", "soci-streamed")
	if err := os.MkdirAll(a2Bundle1, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(a2Bundle2, 0o755); err != nil {
		t.Fatal(err)
	}

	// Non-streamed rootfs spec, but with a streamed ImageVolume
	streamedVolRef := "us-docker.pkg.dev/test/vol-image:v1"
	streamedVolDigest := "sha256:3333333333333333333333333333333333333333333333333333333333333333"
	streamedVolLayers := []string{"/var/lib/ateom-gvisor/streaming/riptide/vol-v1/layer-0"}
	if err := imagecache.WriteSpec(a2Bundle1, &imagecache.OverlaySpec{
		ImageRef:    "docker.io/library/busybox:latest",
		ImageDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Layers:      []string{"/var/lib/atelet/image-cache/layers/layer0"},
		ImageVolumes: []imagecache.ImageVolumeOverlay{{
			Name:        "weights",
			ImageRef:    streamedVolRef,
			ImageDigest: streamedVolDigest,
			Layers:      streamedVolLayers,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	// SOCI streamed image spec
	streamedImg2 := "public.ecr.aws/test/gpu:v2"
	streamedDigest2 := "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	streamedLayers2 := []string{"/run/soci-snapshotter-grpc/mounts/layer0"}
	if err := imagecache.WriteSpec(a2Bundle2, &imagecache.OverlaySpec{
		ImageRef:    streamedImg2,
		ImageDigest: streamedDigest2,
		Layers:      streamedLayers2,
	}); err != nil {
		t.Fatal(err)
	}

	leases, err := scanActiveStreamedLeases(actorsDir)
	if err != nil {
		t.Fatalf("scanActiveStreamedLeases failed: %v", err)
	}

	if len(leases) != 3 {
		t.Fatalf("got %d active leases, want 3", len(leases))
	}

	leaseMap := make(map[string]*imagestreaming.ActiveLease)
	for _, l := range leases {
		leaseMap[l.ImageRef] = l
	}

	l1, ok1 := leaseMap[streamedImg1]
	if !ok1 {
		t.Fatalf("missing lease for %s", streamedImg1)
	}
	if l1.RefCount != 2 {
		t.Errorf("lease 1 RefCount = %d, want 2", l1.RefCount)
	}
	if l1.ImageDigest != streamedDigest1 {
		t.Errorf("lease 1 ImageDigest = %s, want %s", l1.ImageDigest, streamedDigest1)
	}
	if len(l1.LayerDirs) != 2 {
		t.Errorf("lease 1 LayerDirs length = %d, want 2", len(l1.LayerDirs))
	}

	l2, ok2 := leaseMap[streamedImg2]
	if !ok2 {
		t.Fatalf("missing lease for %s", streamedImg2)
	}
	if l2.RefCount != 1 {
		t.Errorf("lease 2 RefCount = %d, want 1", l2.RefCount)
	}

	lVol, okVol := leaseMap[streamedVolRef]
	if !okVol {
		t.Fatalf("missing lease for image volume %s", streamedVolRef)
	}
	if lVol.RefCount != 1 {
		t.Errorf("volume lease RefCount = %d, want 1", lVol.RefCount)
	}
}

func TestReconcileStreamingLeases(t *testing.T) {
	ctx := context.Background()
	actorsDir := t.TempDir()

	// Even with 0 active actors on disk, ReconcileLeases must be called so
	// the driver sweeps any orphaned streaming workDirs.
	emptyStreamer := mock.New()
	emptyStreamer.NameVal = "riptide"
	if err := reconcileStreamingLeases(ctx, emptyStreamer, actorsDir); err != nil {
		t.Fatalf("reconcileStreamingLeases(empty) failed: %v", err)
	}
	if len(emptyStreamer.ReconcileLeasesCalls) != 1 || len(emptyStreamer.ReconcileLeasesCalls[0]) != 0 {
		t.Fatalf("ReconcileLeasesCalls on empty actorsDir = %v, want 1 call with 0 leases", emptyStreamer.ReconcileLeasesCalls)
	}

	aBundle := filepath.Join(actorsDir, "act-1", "bundles", "c1")
	if err := os.MkdirAll(aBundle, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := imagecache.WriteSpec(aBundle, &imagecache.OverlaySpec{
		ImageRef:    "test.registry/stream/img:v1",
		ImageDigest: "sha256:abc",
		Layers:      []string{"/run/ate/streaming/riptide/img-v1/layer-0"},
	}); err != nil {
		t.Fatal(err)
	}

	mockStreamer := mock.New()
	mockStreamer.NameVal = "riptide"

	if err := reconcileStreamingLeases(ctx, mockStreamer, actorsDir); err != nil {
		t.Fatalf("reconcileStreamingLeases failed: %v", err)
	}

	if len(mockStreamer.ReconcileLeasesCalls) != 1 {
		t.Fatalf("ReconcileLeases called %d times, want 1", len(mockStreamer.ReconcileLeasesCalls))
	}
	call := mockStreamer.ReconcileLeasesCalls[0]
	if len(call) != 1 {
		t.Fatalf("ReconcileLeases got %d leases, want 1", len(call))
	}
	if call[0].ImageRef != "test.registry/stream/img:v1" {
		t.Errorf("got imageRef %q, want test.registry/stream/img:v1", call[0].ImageRef)
	}
	if call[0].RefCount != 1 {
		t.Errorf("got refCount %d, want 1", call[0].RefCount)
	}
}
