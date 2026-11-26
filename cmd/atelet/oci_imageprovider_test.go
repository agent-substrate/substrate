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
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	providerclient "github.com/ai-on-gke/image-provider-plugin/client"
	providermock "github.com/ai-on-gke/image-provider-plugin/client/mock"
	"github.com/google/go-containerregistry/pkg/authn"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/utils/clock"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/cmd/atelet/internal/snapshottracker"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/proto/ateletpb"
)

const testStreamedDigest = "sha256:1111111111111111111111111111111111111111111111111111111111111111"

// streamedRootfs makes a rootfs as a plugin returns it.
func streamedRootfs(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}

func streamedResult(rootfs string) *providerclient.PrepareResult {
	return &providerclient.PrepareResult{
		ImageDigest:  testStreamedDigest,
		Config:       &v1.Config{Entrypoint: []string{"/bin/streamed"}},
		Rootfs:       rootfs,
		ProviderName: "nydus",
	}
}

// providerOutcome returns the outcome and error.type ate.imageprovider.requests
// recorded, or "" if it recorded nothing.
func providerOutcome(t *testing.T, reader *sdkmetric.ManualReader) (outcome, errorType string) {
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
			attrs := sum.DataPoints[0].Attributes
			v, _ := attrs.Value(ateattr.ImageProviderOutcomeKey)
			e, _ := attrs.Value(ateattr.ErrorTypeKey)
			return v.AsString(), e.AsString()
		}
	}
	return "", ""
}

func TestImageSourceEnsure(t *testing.T) {
	host := imageVolumeTestRegistry(t)
	ref := pushTestImage(t, host+"/app:v1", singleFileLayer(t, "app-bin", "hello"))
	_, refDigest, _ := strings.Cut(ref, "@")
	// pinnedResult is the provider's result for ref.
	pinnedResult := func(rootfs string) *providerclient.PrepareResult {
		res := streamedResult(rootfs)
		res.ImageDigest = refDigest
		return res
	}
	keychain := authn.NewMultiKeychain()

	for _, tc := range []struct {
		name string
		// noProvider leaves imageSource.provider nil.
		noProvider bool
		// gateOff leaves the ImageProviderPlugin preview gate off.
		gateOff bool
		// cached pulls ref into the cache first.
		cached     bool
		canProvide func(context.Context, *providerclient.PrepareRequest) (bool, error)
		prepare    func(context.Context, *providerclient.PrepareRequest) (*providerclient.PrepareResult, error)
		// result is what prepare returns when it is nil.
		result        func(rootfs string) *providerclient.PrepareResult
		wantStreamed  bool
		wantOutcome   string
		wantErrorType string
		wantPrepares  int
		wantReleases  int
	}{{
		name:       "no provider",
		noProvider: true,
	}, {
		name:    "gate off",
		gateOff: true,
		result:  pinnedResult,
	}, {
		name:         "provider serves the image",
		result:       pinnedResult,
		wantStreamed: true,
		wantOutcome:  ateattr.ImageProviderOutcomeSuccess,
		wantPrepares: 1,
	}, {
		name:   "digest ref already cached",
		cached: true,
		result: pinnedResult,
	}, {
		name: "no plugin socket is not a request",
		canProvide: func(context.Context, *providerclient.PrepareRequest) (bool, error) {
			return false, providerclient.ErrNoPlugin
		},
	}, {
		name:        "provider declines",
		canProvide:  func(context.Context, *providerclient.PrepareRequest) (bool, error) { return false, nil },
		wantOutcome: ateattr.ImageProviderOutcomeDeclined,
	}, {
		name: "CanProvide fails",
		canProvide: func(context.Context, *providerclient.PrepareRequest) (bool, error) {
			return false, status.Error(codes.Unavailable, "plugin restarting")
		},
		wantOutcome:   ateattr.ImageProviderOutcomeError,
		wantErrorType: "Unavailable",
	}, {
		name: "PrepareImage not supported",
		prepare: func(context.Context, *providerclient.PrepareRequest) (*providerclient.PrepareResult, error) {
			return nil, providerclient.ErrNotSupported
		},
		wantOutcome:  ateattr.ImageProviderOutcomeDeclined,
		wantPrepares: 1,
		wantReleases: 1,
	}, {
		name: "PrepareImage fails",
		prepare: func(context.Context, *providerclient.PrepareRequest) (*providerclient.PrepareResult, error) {
			return nil, fmt.Errorf("calling PrepareImage on image provider: %w", status.Error(codes.Internal, "boom"))
		},
		wantOutcome:   ateattr.ImageProviderOutcomeError,
		wantErrorType: "Internal",
		wantPrepares:  1,
		wantReleases:  1,
	}, {
		name: "result without config is released",
		result: func(rootfs string) *providerclient.PrepareResult {
			res := pinnedResult(rootfs)
			res.Config = nil
			return res
		},
		wantOutcome:   ateattr.ImageProviderOutcomeError,
		wantErrorType: "invalid_result",
		wantPrepares:  1,
		wantReleases:  1,
	}, {
		name:          "digest that ref does not pin is released",
		result:        streamedResult,
		wantOutcome:   ateattr.ImageProviderOutcomeError,
		wantErrorType: "invalid_result",
		wantPrepares:  1,
		wantReleases:  1,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.gateOff {
				preview.InitForTest(t)
			} else {
				preview.InitForTest(t, string(preview.GateImageProviderPlugin))
			}
			ctx := t.Context()
			inst, reader := newTestInstruments(t)
			images := imageSource{cache: newImageVolumeStore(t), keychain: keychain, leases: &providerLeaseTracker{}, authModes: &providerAuthModes{}, instruments: inst}
			if tc.cached {
				if _, err := images.cache.EnsureImage(ctx, ref); err != nil {
					t.Fatal(err)
				}
			}
			provider := providermock.New("nydus")
			provider.CanProvideFunc = func(ctx context.Context, req *providerclient.PrepareRequest) (bool, error) {
				if providerclient.KeychainFromContext(ctx) != nil {
					t.Error("CanProvide got a keychain in its context")
				}
				if _, ok := ctx.Deadline(); !ok {
					t.Error("CanProvide got no deadline")
				}
				if tc.canProvide != nil {
					return tc.canProvide(ctx, req)
				}
				return true, nil
			}
			rootfs := streamedRootfs(t)
			provider.PrepareImageFunc = tc.prepare
			if tc.prepare == nil && tc.result != nil {
				provider.PrepareImageFunc = func(ctx context.Context, _ *providerclient.PrepareRequest) (*providerclient.PrepareResult, error) {
					if providerclient.KeychainFromContext(ctx) == nil {
						t.Error("PrepareImage got no keychain in its context")
					}
					if _, ok := ctx.Deadline(); !ok {
						t.Error("PrepareImage got no deadline")
					}
					return tc.result(rootfs), nil
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
			wantAuthKey, err := providerclient.AuthKey(ref)
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range provider.PrepareCalls {
				if r.LeaseID != leaseID || r.AuthKey != wantAuthKey {
					t.Errorf("PrepareImage lease = %q auth key %q, want %q %q", r.LeaseID, r.AuthKey, leaseID, wantAuthKey)
				}
			}
			for _, r := range provider.ReleaseCalls {
				if r.LeaseID != leaseID {
					t.Errorf("released %+v, want lease %q", r, leaseID)
				}
			}
			if tc.wantStreamed {
				if img.ProviderRootfs != rootfs || len(img.LayerDirs) != 0 || img.Digest.String() != refDigest {
					t.Errorf("image = %+v, want the provider's rootfs and digest", img)
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
			if outcome, errorType := providerOutcome(t, reader); outcome != tc.wantOutcome || errorType != tc.wantErrorType {
				t.Errorf("recorded outcome = %q error.type %q, want %q %q", outcome, errorType, tc.wantOutcome, tc.wantErrorType)
			}
		})
	}
}

// prepareOCIDirectory records the provider's leases in the overlay spec, and
// returns them when it fails.
func TestPrepareOCIDirectoryProviderLeases(t *testing.T) {
	preview.InitForTest(t, string(preview.GateImageProviderPlugin))
	useTempNodeDirs(t)
	// The provider serves every image, so none needs to be in a registry. Tag
	// refs pin no digest for the provider's result to match.
	const ref, volRef, badRef = "registry.example/app:v1", "registry.example/models:v1", "registry.example/bad:v1"
	volumes := []*ateletpb.Volume{{Name: "models", Image: &ateletpb.ImageVolumeSource{Reference: volRef}}}
	mounts := []*ateletpb.VolumeMount{{Name: "models", MountPath: "/models"}}

	rootfs := streamedRootfs(t)
	provider := providermock.New("nydus")
	provider.PrepareImageFunc = func(context.Context, *providerclient.PrepareRequest) (*providerclient.PrepareResult, error) {
		return streamedResult(rootfs), nil
	}
	images := imageSource{cache: newImageVolumeStore(t), provider: provider, leases: &providerLeaseTracker{}, authModes: &providerAuthModes{}}

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
		if !slices.Equal(spec.LowerDirs, []string{rootfs}) {
			t.Errorf("spec lowerdirs = %v, want [%s]", spec.LowerDirs, rootfs)
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
	provider.PrepareImageFunc = func(_ context.Context, req *providerclient.PrepareRequest) (*providerclient.PrepareResult, error) {
		res := streamedResult(rootfs)
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
		released = append(released, r.LeaseID)
	}
	slices.Sort(released)
	if want := []string{"actor-2/app", "actor-2/app/models"}; !slices.Equal(released, want) {
		t.Errorf("released %v, want %v", released, want)
	}
}

// releaseProviderLeases returns the leases the actor's overlay specs record,
// once.
func TestReleaseProviderLeases(t *testing.T) {
	useTempNodeDirs(t)
	const actorUID = "actor-1"
	for name, spec := range map[string]*imagecache.OverlaySpec{
		"app": {
			ImageRef:    "registry.example/app:1",
			ImageDigest: "sha256:aaa",
			LeaseID:     "actor-1/app",
			LowerDirs:   []string{"/plugin/layer-0"},
			ImageVolumes: []imagecache.ImageVolumeOverlay{
				{Name: "models", ImageRef: "registry.example/models:1", ImageDigest: "sha256:bbb", LeaseID: "actor-1/app/models", LowerDirs: []string{"/plugin/layer-1"}},
				{Name: "tools", ImageRef: "registry.example/tools:1", StoreLayers: []string{"/cache/layer-2"}},
			},
		},
		"sidecar": {ImageRef: "registry.example/sidecar:1", StoreLayers: []string{"/cache/layer-3"}},
	} {
		bundle := ateletpath.OCIBundlePath(actorUID, name)
		if err := os.MkdirAll(bundle, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := imagecache.WriteSpec(bundle, spec); err != nil {
			t.Fatal(err)
		}
	}
	provider := providermock.New("nydus")
	s := &Atelet{imageProvider: provider}

	s.releaseProviderLeases(t.Context(), actorUID)
	var released []string
	for _, r := range provider.ReleaseCalls {
		released = append(released, r.LeaseID)
	}
	slices.Sort(released)
	if want := []string{"actor-1/app", "actor-1/app/models"}; !slices.Equal(released, want) {
		t.Errorf("released %v, want %v", released, want)
	}

	s.releaseProviderLeases(t.Context(), actorUID)
	if got := len(provider.ReleaseCalls); got != 2 {
		t.Errorf("ReleaseImage calls = %d after a second release, want 2", got)
	}

	// The spec with leases is gone. The cache-only spec still roots its
	// layers.
	for name, want := range map[string]bool{"app": false, "sidecar": true} {
		spec, err := imagecache.ReadSpec(ateletpath.OCIBundlePath(actorUID, name))
		if err != nil {
			t.Fatal(err)
		}
		if got := spec != nil; got != want {
			t.Errorf("%s spec present = %v, want %v", name, got, want)
		}
	}
}

// A Run or Restore whose worker call fails keeps its provider leases and
// specs, since the sandbox may have started. Terminate or the next reset
// releases them.
func TestActivationFailureAfterWorkerCallKeepsProviderLeases(t *testing.T) {
	for _, rpc := range []string{"RunWorkload", "RestoreWorkload"} {
		t.Run(rpc, func(t *testing.T) {
			preview.InitForTest(t, string(preview.GateImageProviderPlugin))
			useTempNodeDirs(t)
			const (
				actorUID     = "0f6c2b0e-1b7e-4b3f-9a6a-1d2c3e4f5a64"
				workerPodUID = "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f"
				snapshotName = "pause-snap-1"
			)
			worker := &fakeWorker{
				runErr:     status.Error(codes.DeadlineExceeded, "run timed out"),
				restoreErr: status.Error(codes.DeadlineExceeded, "restore timed out"),
			}
			serveFakeWorker(t, worker)
			release := newGVisorTestRelease(t)
			rootfs := streamedRootfs(t)
			provider := providermock.New("nydus")
			provider.PrepareImageFunc = func(context.Context, *providerclient.PrepareRequest) (*providerclient.PrepareResult, error) {
				return streamedResult(rootfs), nil
			}
			s := &Atelet{
				workerDialer:      newWorkerDialer(1),
				imageCache:        newImageVolumeStore(t),
				imageProvider:     provider,
				sandboxAssetCache: newTestAssetCache(t),
				anonGCSClient:     fakeObjectStorage{data: release.tarball},
				systemInfoVolumes: newSystemInfoVolumeRefresher(nil, nil),
				snapshots:         snapshottracker.New(t.Context(), nil, clock.RealClock{}),
			}
			rec := testSandboxRecord(release)
			advertiseRuntime(t, s, rec)
			spec := &ateletpb.WorkloadSpec{
				Containers: []*ateletpb.Container{{Name: "app", Image: "registry.example/app@" + testStreamedDigest}},
			}

			var err error
			if rpc == "RunWorkload" {
				_, err = s.Run(t.Context(), &ateletpb.RunRequest{
					Atespace:       "team-a",
					ActorName:      "actor-a",
					ActorUid:       actorUID,
					WorkerPodUid:   workerPodUID,
					SandboxRuntime: runtimeFor(t, rec),
					Spec:           spec,
				})
			} else {
				writeLocalSnapshot(t, ateletpath.LocalSnapshotDir(actorUID, snapshotName), sandboxAssetsRecord{
					SandboxClass:  "gvisor",
					Assets:        map[string]assetEntry{gvisorAssetName: release.entry},
					SnapshotFiles: []string{"checkpoint.img"},
				}, map[string]string{"checkpoint.img": "guest-memory"})
				_, err = s.Restore(t.Context(), &ateletpb.RestoreRequest{
					Atespace:       "team-a",
					ActorName:      "actor-a",
					ActorUid:       actorUID,
					WorkerPodUid:   workerPodUID,
					SandboxRuntime: runtimeFor(t, rec),
					Spec:           spec,
					Snapshot:       lifecycleSnapshot(actorUID, snapshotName, 1, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY),
				})
			}
			if err == nil {
				t.Fatalf("%s failed but the activation succeeded", rpc)
			}
			if worker.actorDirs[rpc] == nil {
				t.Fatalf("activation failed before %s: %v", rpc, err)
			}
			if len(provider.ReleaseCalls) != 0 {
				t.Errorf("ReleaseImage calls = %d, want 0", len(provider.ReleaseCalls))
			}
			got, err := imagecache.ReadSpec(ateletpath.OCIBundlePath(actorUID, "app"))
			if err != nil {
				t.Fatal(err)
			}
			if want := actorUID + "/app"; got == nil || got.LeaseID != want {
				t.Errorf("overlay spec = %+v, want one recording lease %q", got, want)
			}
		})
	}
}
