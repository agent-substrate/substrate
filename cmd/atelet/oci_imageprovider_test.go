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
	"os"
	"path/filepath"
	"slices"
	"testing"

	providerclient "github.com/ai-on-gke/image-provider-plugin/client"
	providermock "github.com/ai-on-gke/image-provider-plugin/client/mock"
	"github.com/google/go-containerregistry/pkg/authn"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/proto/ateletpb"
)

const testStreamedDigest = "sha256:1111111111111111111111111111111111111111111111111111111111111111"

// streamedLayerDir makes a lowerdir as a plugin returns it.
func streamedLayerDir(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}

func streamedResult(layerDir string) *providerclient.PrepareResult {
	return &providerclient.PrepareResult{
		ImageDigest:  testStreamedDigest,
		Config:       &v1.Config{Entrypoint: []string{"/bin/streamed"}},
		LayerDirs:    []string{layerDir},
		ProviderName: "nydus",
	}
}

// providerOutcome returns the outcome ate.imageprovider.requests recorded, or
// "" if it recorded nothing.
func providerOutcome(t *testing.T, reader *sdkmetric.ManualReader) string {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != imageProviderRequestsMetric {
				continue
			}
			sum := m.Data.(metricdata.Sum[int64])
			if len(sum.DataPoints) != 1 {
				t.Fatalf("%s has %d datapoints, want 1", m.Name, len(sum.DataPoints))
			}
			v, _ := sum.DataPoints[0].Attributes.Value(ateattr.ImageProviderOutcomeKey)
			return v.AsString()
		}
	}
	return ""
}

func TestImageSourceEnsure(t *testing.T) {
	host := imageVolumeTestRegistry(t)
	ref := pushTestImage(t, host+"/app:v1", singleFileLayer(t, "app-bin", "hello"))
	keychain := authn.NewMultiKeychain()

	for _, tc := range []struct {
		name string
		// noProvider leaves imageSource.provider nil.
		noProvider bool
		// cached pulls ref into the cache first.
		cached     bool
		canProvide func(context.Context, *providerclient.PrepareRequest) (bool, error)
		prepare    func(context.Context, *providerclient.PrepareRequest) (*providerclient.PrepareResult, error)
		// result is what prepare returns when it is nil.
		result       func(layerDir string) *providerclient.PrepareResult
		wantStreamed bool
		wantOutcome  string
		wantPrepares int
		wantReleases int
	}{{
		name:       "no provider",
		noProvider: true,
	}, {
		name:         "provider serves the image",
		result:       streamedResult,
		wantStreamed: true,
		wantOutcome:  ateattr.ImageProviderOutcomeSuccess,
		wantPrepares: 1,
	}, {
		name:   "digest ref already cached",
		cached: true,
		result: streamedResult,
	}, {
		name:        "provider declines",
		canProvide:  func(context.Context, *providerclient.PrepareRequest) (bool, error) { return false, nil },
		wantOutcome: ateattr.ImageProviderOutcomeFallback,
	}, {
		name: "CanProvide fails",
		canProvide: func(context.Context, *providerclient.PrepareRequest) (bool, error) {
			return false, errors.New("unavailable")
		},
		wantOutcome: ateattr.ImageProviderOutcomeFallback,
	}, {
		name: "PrepareImage not supported",
		prepare: func(context.Context, *providerclient.PrepareRequest) (*providerclient.PrepareResult, error) {
			return nil, providerclient.ErrNotSupported
		},
		wantOutcome:  ateattr.ImageProviderOutcomeFallback,
		wantPrepares: 1,
		wantReleases: 1,
	}, {
		name: "PrepareImage fails",
		prepare: func(context.Context, *providerclient.PrepareRequest) (*providerclient.PrepareResult, error) {
			return nil, errors.New("boom")
		},
		wantOutcome:  ateattr.ImageProviderOutcomeFallback,
		wantPrepares: 1,
		wantReleases: 1,
	}, {
		name: "result without config is released",
		result: func(layerDir string) *providerclient.PrepareResult {
			res := streamedResult(layerDir)
			res.Config = nil
			return res
		},
		wantOutcome:  ateattr.ImageProviderOutcomeFallback,
		wantPrepares: 1,
		wantReleases: 1,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			inst, reader := newTestInstruments(t)
			images := imageSource{cache: newImageVolumeStore(t), keychain: keychain, instruments: inst}
			if tc.cached {
				if _, err := images.cache.EnsureImage(ctx, ref); err != nil {
					t.Fatal(err)
				}
			}
			provider := providermock.New("nydus")
			provider.CanProvideFunc = tc.canProvide
			layerDir := streamedLayerDir(t)
			provider.PrepareImageFunc = tc.prepare
			if tc.prepare == nil && tc.result != nil {
				provider.PrepareImageFunc = func(ctx context.Context, _ *providerclient.PrepareRequest) (*providerclient.PrepareResult, error) {
					if providerclient.KeychainFromContext(ctx) == nil {
						t.Error("PrepareImage got no keychain in its context")
					}
					return tc.result(layerDir), nil
				}
			}
			if !tc.noProvider {
				images.provider = provider
			}

			const leaseID = "actor-1/app"
			img, err := images.ensure(ctx, ref, leaseID)
			if err != nil {
				t.Fatalf("ensure: %v", err)
			}
			wantLease := ""
			if tc.wantStreamed {
				wantLease = leaseID
			}
			if img.LeaseID != wantLease {
				t.Errorf("LeaseID = %q, want %q", img.LeaseID, wantLease)
			}
			for _, r := range provider.PrepareCalls {
				if r.LeaseID != leaseID {
					t.Errorf("PrepareImage lease = %q, want %q", r.LeaseID, leaseID)
				}
			}
			// A failed PrepareImage returns no digest to release with.
			wantDigest := testStreamedDigest
			if tc.prepare != nil {
				wantDigest = ""
			}
			for _, r := range provider.ReleaseCalls {
				if r.LeaseID != leaseID || r.ImageDigest != wantDigest {
					t.Errorf("released %+v, want lease %q on %q", r, leaseID, wantDigest)
				}
			}
			if tc.wantStreamed {
				if !slices.Equal(img.ProviderDirs, []string{layerDir}) || len(img.LayerDirs) != 0 || img.Digest.String() != testStreamedDigest {
					t.Errorf("image = %+v, want the provider's layers and digest", img)
				}
			} else if _, err := os.Stat(filepath.Join(img.LayerDirs[0], "fs", "app-bin")); err != nil {
				t.Errorf("image is not from the cache: %v", err)
			}
			if got := len(provider.PrepareCalls); got != tc.wantPrepares {
				t.Errorf("PrepareImage calls = %d, want %d", got, tc.wantPrepares)
			}
			if got := len(provider.ReleaseCalls); got != tc.wantReleases {
				t.Errorf("ReleaseImage calls = %d, want %d", got, tc.wantReleases)
			}
			if got := providerOutcome(t, reader); got != tc.wantOutcome {
				t.Errorf("recorded outcome = %q, want %q", got, tc.wantOutcome)
			}
		})
	}
}

// prepareOCIDirectory records the provider's leases in the overlay spec, and
// returns them when it fails.
func TestPrepareOCIDirectoryProviderLeases(t *testing.T) {
	useTempNodeDirs(t)
	host := imageVolumeTestRegistry(t)
	ref := pushTestImage(t, host+"/app:v1", singleFileLayer(t, "app-bin", "hello"))
	volRef := pushTestImage(t, host+"/models:v1", singleFileLayer(t, "weights", "w"))
	volumes := []*ateletpb.Volume{{Name: "models", Image: &ateletpb.ImageVolumeSource{Reference: volRef}}}
	mounts := []*ateletpb.VolumeMount{{Name: "models", MountPath: "/models"}}

	layerDir := streamedLayerDir(t)
	provider := providermock.New("nydus")
	provider.PrepareImageFunc = func(context.Context, *providerclient.PrepareRequest) (*providerclient.PrepareResult, error) {
		return streamedResult(layerDir), nil
	}
	images := imageSource{cache: newImageVolumeStore(t), provider: provider}

	// Two containers of one actor take a lease each on the same images.
	for _, ctr := range []string{"app", "sidecar"} {
		if _, _, err := prepareOCIDirectory(t.Context(), images, "actor-1", ctr, ref, volumes, mounts); err != nil {
			t.Fatalf("prepareOCIDirectory(%s): %v", ctr, err)
		}
		spec, err := imagecache.ReadSpec(ateletpath.OCIBundlePath("actor-1", ctr))
		if err != nil {
			t.Fatal(err)
		}
		if want := "actor-1/" + ctr; spec.ImageRef != ref || spec.LeaseID != want {
			t.Errorf("spec image = %q lease %q, want %q lease %q", spec.ImageRef, spec.LeaseID, ref, want)
		}
		if want := "actor-1/" + ctr + "/models"; len(spec.ImageVolumes) != 1 || spec.ImageVolumes[0].ImageRef != volRef || spec.ImageVolumes[0].LeaseID != want {
			t.Errorf("spec image volumes = %+v, want %q lease %q", spec.ImageVolumes, volRef, want)
		}
	}
	if len(provider.ReleaseCalls) != 0 {
		t.Errorf("ReleaseImage calls = %d after success, want 0", len(provider.ReleaseCalls))
	}

	// A WORKDIR that is not absolute fails the container after both images
	// were prepared, so both leases go back.
	badRef := pushTestImageWithConfig(t, host+"/bad:v1", v1.Config{WorkingDir: "relative"}, singleFileLayer(t, "x", "x"))
	provider.PrepareImageFunc = func(_ context.Context, req *providerclient.PrepareRequest) (*providerclient.PrepareResult, error) {
		res := streamedResult(layerDir)
		if req.ImageRef == badRef {
			res.Config = &v1.Config{WorkingDir: "relative"}
		}
		return res, nil
	}
	if _, _, err := prepareOCIDirectory(t.Context(), images, "actor-2", "app", badRef, volumes, mounts); err == nil {
		t.Fatal("prepareOCIDirectory with a relative WORKDIR succeeded, want an error")
	}
	var released []string
	for _, r := range provider.ReleaseCalls {
		released = append(released, r.LeaseID+"@"+r.ImageDigest)
	}
	slices.Sort(released)
	if want := []string{"actor-2/app/models@" + testStreamedDigest, "actor-2/app@" + testStreamedDigest}; !slices.Equal(released, want) {
		t.Errorf("released %v, want %v", released, want)
	}
}
