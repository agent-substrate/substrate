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
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/imagestreaming"
	"github.com/agent-substrate/substrate/internal/imagestreaming/mock"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestEnsureContainerImage_StreamerSuccess(t *testing.T) {
	ctx := context.Background()
	m := mock.New()
	m.CanStreamFunc = func(ctx context.Context, req *imagestreaming.StreamRequest) (bool, error) {
		return true, nil
	}
	m.PrepareLayersFunc = func(ctx context.Context, req *imagestreaming.StreamRequest) (*imagestreaming.StreamResult, error) {
		return &imagestreaming.StreamResult{
			ImageDigest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
			Config: &v1.Config{
				Cmd: []string{"/bin/sh"},
			},
			LayerDirs: []string{"/streamed/layer1", "/streamed/layer2"},
		}, nil
	}

	img, err := ensureContainerImage(ctx, nil, m, nil, "example.com/test:latest")
	if err != nil {
		t.Fatalf("ensureContainerImage: %v", err)
	}

	if want := "sha256:1111111111111111111111111111111111111111111111111111111111111111"; img.Digest.String() != want {
		t.Errorf("img.Digest = %q, want %q", img.Digest.String(), want)
	}
	if !slices.Equal(img.LayerDirs, []string{"/streamed/layer1", "/streamed/layer2"}) {
		t.Errorf("img.LayerDirs = %v, want [/streamed/layer1, /streamed/layer2]", img.LayerDirs)
	}
	if len(img.Config.Cmd) != 1 || img.Config.Cmd[0] != "/bin/sh" {
		t.Errorf("img.Config.Cmd = %v, want [/bin/sh]", img.Config.Cmd)
	}

	if len(m.CanStreamCalls) != 1 || m.CanStreamCalls[0].ImageRef != "example.com/test:latest" {
		t.Errorf("unexpected CanStreamCalls: %+v", m.CanStreamCalls)
	}
	if len(m.PrepareLayersCalls) != 1 || m.PrepareLayersCalls[0].ImageRef != "example.com/test:latest" {
		t.Errorf("unexpected PrepareLayersCalls: %+v", m.PrepareLayersCalls)
	}
}

func TestEnsureContainerImage_UnsupportedFallback(t *testing.T) {
	ctx := context.Background()
	regHost := imageVolumeTestRegistry(t)
	ref := regHost + "/fallback-unsupported:v1"
	pushTestImage(t, ref, singleFileLayer(t, "file.txt", "hello"))

	cacheDir := t.TempDir()
	store, err := imagecache.New(cacheDir)
	if err != nil {
		t.Fatalf("imagecache.New: %v", err)
	}

	m := mock.New()
	m.CanStreamFunc = func(ctx context.Context, req *imagestreaming.StreamRequest) (bool, error) {
		return false, nil
	}

	img, err := ensureContainerImage(ctx, store, m, nil, ref)
	if err != nil {
		t.Fatalf("ensureContainerImage fallback: %v", err)
	}
	if len(img.LayerDirs) == 0 {
		t.Fatal("expected cached layer dirs, got none")
	}
	if len(m.PrepareLayersCalls) != 0 {
		t.Errorf("PrepareLayers called %d times, want 0", len(m.PrepareLayersCalls))
	}
}

func TestEnsureContainerImage_ErrorFallback(t *testing.T) {
	ctx := context.Background()
	regHost := imageVolumeTestRegistry(t)
	ref := regHost + "/fallback-err:v1"
	pushTestImage(t, ref, singleFileLayer(t, "file.txt", "hello"))

	cacheDir := t.TempDir()
	store, err := imagecache.New(cacheDir)
	if err != nil {
		t.Fatalf("imagecache.New: %v", err)
	}

	m := mock.New()
	m.CanStreamFunc = func(ctx context.Context, req *imagestreaming.StreamRequest) (bool, error) {
		return true, nil
	}
	m.PrepareLayersFunc = func(ctx context.Context, req *imagestreaming.StreamRequest) (*imagestreaming.StreamResult, error) {
		return nil, errors.New("daemon connection failed")
	}

	img, err := ensureContainerImage(ctx, store, m, nil, ref)
	if err != nil {
		t.Fatalf("ensureContainerImage fallback on error: %v", err)
	}
	if len(img.LayerDirs) == 0 {
		t.Fatal("expected cached layer dirs after fallback, got none")
	}
}

func TestEnsureContainerImage_NilStreamer(t *testing.T) {
	ctx := context.Background()
	regHost := imageVolumeTestRegistry(t)
	ref := regHost + "/nil-streamer:v1"
	pushTestImage(t, ref, singleFileLayer(t, "file.txt", "hello"))

	cacheDir := t.TempDir()
	store, err := imagecache.New(cacheDir)
	if err != nil {
		t.Fatalf("imagecache.New: %v", err)
	}

	img, err := ensureContainerImage(ctx, store, nil, nil, ref)
	if err != nil {
		t.Fatalf("ensureContainerImage with nil streamer: %v", err)
	}
	if len(img.LayerDirs) == 0 {
		t.Fatal("expected cached layer dirs, got none")
	}
}

func TestPrepareOCIDirectory_WithStreaming(t *testing.T) {
	root := t.TempDir()
	origActors := ateompath.ActorsDir
	ateompath.ActorsDir = filepath.Join(root, "actors")
	t.Cleanup(func() {
		ateompath.ActorsDir = origActors
	})

	ctx := context.Background()
	m := mock.New()
	m.CanStreamFunc = func(ctx context.Context, req *imagestreaming.StreamRequest) (bool, error) {
		return true, nil
	}
	m.PrepareLayersFunc = func(ctx context.Context, req *imagestreaming.StreamRequest) (*imagestreaming.StreamResult, error) {
		return &imagestreaming.StreamResult{
			ImageDigest: "sha256:2222222222222222222222222222222222222222222222222222222222222222",
			Config: &v1.Config{
				Cmd: []string{"/app"},
			},
			LayerDirs: []string{"/streamed/app-layer"},
		}, nil
	}

	actorUID := "actor-test-123"
	containerName := "app"
	err := prepareOCIDirectory(
		ctx,
		nil, // imageCache is nil; streaming handles it completely!
		m,
		nil, // instruments is nil
		actorUID,
		containerName,
		"test.example.com/app:v1",
		[]string{"/app"},
		nil,
		nil,
		"/proc/1/ns/net",
		nil,
		nil,
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("prepareOCIDirectory: %v", err)
	}

	bundlePath := ateompath.OCIBundlePath(actorUID, containerName)
	spec, err := imagecache.ReadSpec(bundlePath)
	if err != nil {
		t.Fatalf("imagecache.ReadSpec: %v", err)
	}
	if spec == nil {
		t.Fatal("expected rootfs-overlay.json to exist")
	}
	if spec.ImageDigest != "sha256:2222222222222222222222222222222222222222222222222222222222222222" {
		t.Errorf("spec.ImageDigest = %q, want expected digest", spec.ImageDigest)
	}
	if !slices.Equal(spec.Layers, []string{"/streamed/app-layer"}) {
		t.Errorf("spec.Layers = %v, want [/streamed/app-layer]", spec.Layers)
	}
}

func TestInitImageStreamer_NoneAndEmpty(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"none", "", "   "} {
		s, err := initImageStreamer(ctx, strings.TrimSpace(mode), "")
		if err != nil {
			t.Fatalf("initImageStreamer(%q): %v", mode, err)
		}
		if s != nil {
			t.Errorf("initImageStreamer(%q) = %v, want nil", mode, s)
		}
	}
}

func TestInitImageStreamer_ExplicitProvider(t *testing.T) {
	ctx := context.Background()
	dummyStreamer := mock.New()
	dummyStreamer.NameVal = "custom-test"

	imagestreaming.Register("custom-test", func(ctx context.Context, cfg imagestreaming.Config) (imagestreaming.ImageStreamer, error) {
		if cfg[imagestreaming.SocketPathKey] != "/run/custom.sock" {
			return nil, fmt.Errorf("unexpected socket: %v", cfg[imagestreaming.SocketPathKey])
		}
		return dummyStreamer, nil
	})

	s, err := initImageStreamer(ctx, "custom-test", "/run/custom.sock")
	if err != nil {
		t.Fatalf("initImageStreamer: %v", err)
	}
	if s == nil || s.Name() != "custom-test" {
		t.Errorf("unexpected streamer: %v", s)
	}
}

func TestInitImageStreamer_UnknownProvider(t *testing.T) {
	ctx := context.Background()
	_, err := initImageStreamer(ctx, "unknown-provider-xyz", "")
	if err == nil {
		t.Fatal("expected error for unknown provider, got nil")
	}
}

func TestInitImageStreamer_AutoNoSockets(t *testing.T) {
	ctx := context.Background()
	s, err := initImageStreamer(ctx, "auto", "")
	if err != nil {
		t.Fatalf("initImageStreamer(auto): %v", err)
	}
	if s != nil {
		t.Logf("Detected daemon on test host: %s", s.Name())
	}
}

func TestReleaseStreamedLayers_CheckpointAndTerminate(t *testing.T) {
	ctx := context.Background()
	m := mock.New()

	s := &AteomHerder{
		imageStreamer: m,
	}

	actorRef := resources.ActorRef{Atespace: "test", Name: "actor1"}
	spec := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{
			{Name: "c1", Image: "registry.example.com/c1:v1"},
			{Name: "c2", Image: "registry.example.com/c2:v1"},
		},
	}

	// Verify release on checkpoint (Approach 1: active-only leases)
	s.releaseStreamedLayers(ctx, actorRef, spec)

	if len(m.ReleaseLayersCalls) != 2 {
		t.Fatalf("ReleaseLayers called %d times on checkpoint, want 2", len(m.ReleaseLayersCalls))
	}
	if m.ReleaseLayersCalls[0].ImageRef != "registry.example.com/c1:v1" {
		t.Errorf("call 0: %q, want registry.example.com/c1:v1", m.ReleaseLayersCalls[0].ImageRef)
	}
	if m.ReleaseLayersCalls[1].ImageRef != "registry.example.com/c2:v1" {
		t.Errorf("call 1: %q, want registry.example.com/c2:v1", m.ReleaseLayersCalls[1].ImageRef)
	}

	// Verify release on terminate is also safe and invokes releaseStreamedLayers
	s.releaseStreamedLayers(ctx, actorRef, spec)
	if len(m.ReleaseLayersCalls) != 4 {
		t.Fatalf("ReleaseLayers total calls %d, want 4", len(m.ReleaseLayersCalls))
	}
}

func TestImageStreaming_MetricsRecording(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	inst, err := NewInstruments(mp.Meter("atelet"))
	if err != nil {
		t.Fatalf("NewInstruments: %v", err)
	}

	m := mock.New()
	m.NameVal = "test-provider"
	m.CanStreamFunc = func(ctx context.Context, req *imagestreaming.StreamRequest) (bool, error) {
		return true, nil
	}
	m.PrepareLayersFunc = func(ctx context.Context, req *imagestreaming.StreamRequest) (*imagestreaming.StreamResult, error) {
		return &imagestreaming.StreamResult{
			ImageDigest: "sha256:3333333333333333333333333333333333333333333333333333333333333333",
			Config: &v1.Config{
				Cmd: []string{"/app"},
			},
			LayerDirs: []string{"/layer/1"},
		}, nil
	}

	_, err = ensureContainerImage(ctx, nil, m, inst, "example.com/app:v1")
	if err != nil {
		t.Fatalf("ensureContainerImage: %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("reader.Collect: %v", err)
	}

	var foundRequests, foundDuration bool
	for _, sm := range rm.ScopeMetrics {
		for _, metric := range sm.Metrics {
			if metric.Name == imageStreamingRequestsMetric {
				foundRequests = true
				if metric.Unit != "{request}" {
					t.Errorf("requests unit = %q, want {request}", metric.Unit)
				}
				sum, ok := metric.Data.(metricdata.Sum[int64])
				if !ok {
					t.Fatalf("requests data is not Sum[int64]")
				}
				if len(sum.DataPoints) != 1 {
					t.Fatalf("requests data points count = %d, want 1", len(sum.DataPoints))
				}
				dp := sum.DataPoints[0]
				if dp.Value != 1 {
					t.Errorf("requests count = %d, want 1", dp.Value)
				}
				provider, _ := dp.Attributes.Value(ateattr.ImageStreamingProviderKey)
				outcome, _ := dp.Attributes.Value(ateattr.ImageStreamingOutcomeKey)
				if provider.AsString() != "test-provider" {
					t.Errorf("provider = %q, want test-provider", provider.AsString())
				}
				if outcome.AsString() != ateattr.ImageStreamingOutcomeSuccess {
					t.Errorf("outcome = %q, want %q", outcome.AsString(), ateattr.ImageStreamingOutcomeSuccess)
				}
			} else if metric.Name == imageStreamingDurationMetric {
				foundDuration = true
				if metric.Unit != "s" {
					t.Errorf("duration unit = %q, want s", metric.Unit)
				}
				hist, ok := metric.Data.(metricdata.Histogram[float64])
				if !ok {
					t.Fatalf("duration data is not Histogram[float64]")
				}
				if len(hist.DataPoints) != 1 {
					t.Fatalf("duration data points count = %d, want 1", len(hist.DataPoints))
				}
			}
		}
	}
	if !foundRequests {
		t.Errorf("metric %s not collected", imageStreamingRequestsMetric)
	}
	if !foundDuration {
		t.Errorf("metric %s not collected", imageStreamingDurationMetric)
	}
}
