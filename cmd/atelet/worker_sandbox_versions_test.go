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
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http/httptest"
	"net/url"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/agent-substrate/substrate/internal/hardware"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/sandboxversions"
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/objectstorage"
)

// pushPauseImage pushes a tiny image to ref and returns its manifest digest,
// so tests can later assert a cache hit by digest with the registry gone.
func pushPauseImage(t *testing.T, ref string) v1.Hash {
	t.Helper()
	img, err := mutate.AppendLayers(empty.Image, singleFileLayer(t, "pause", "pause bytes"))
	if err != nil {
		t.Fatalf("mutate.AppendLayers: %v", err)
	}
	digest, err := img.Digest()
	if err != nil {
		t.Fatalf("img.Digest: %v", err)
	}
	tag, err := name.ParseReference(ref, name.Insecure)
	if err != nil {
		t.Fatalf("name.ParseReference(%q): %v", ref, err)
	}
	if err := remote.Write(tag, img); err != nil {
		t.Fatalf("remote.Write(%q): %v", ref, err)
	}
	return digest
}

func gvisorConfig(name, url, sha string) *v1alpha1.SandboxConfig {
	return &v1alpha1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.SandboxConfigSpec{
			SandboxClass:   v1alpha1.SandboxClassGvisor,
			DefaultVersion: "v1",
			Versions: []v1alpha1.SandboxVersionConfig{{
				Name:       "v1",
				PauseImage: "registry.k8s.io/pause@sha256:abc",
				Assets: map[string]map[string]v1alpha1.AssetFile{
					runtime.GOARCH: {
						runscAssetName: {URL: url, SHA256: sha},
					},
				},
			}},
		},
	}
}

// recordingFetcher stands in for the AteomHerder's asset fetch, recording
// every record asked for and placing each asset at /static/<sha256>.
type recordingFetcher struct {
	mu  sync.Mutex
	got []*sandboxAssetsRecord
	// err fails every fetch.
	err error
	// failSHA fails a record with an asset of this sha256.
	failSHA string
}

func (f *recordingFetcher) ensureSandboxAssets(_ context.Context, rec *sandboxAssetsRecord) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, rec)
	if f.err != nil {
		return nil, f.err
	}
	paths := make(map[string]string, len(rec.Assets))
	for name, a := range rec.Assets {
		if a.SHA256 == f.failSHA {
			return nil, errors.New("bucket unavailable")
		}
		paths[name] = "/static/" + a.SHA256
	}
	return paths, nil
}

// recordingPuller stands in for the image cache, recording every pause image
// asked for.
type recordingPuller struct {
	mu  sync.Mutex
	got []string
	// failRef fails the pull of this image.
	failRef string
}

func (p *recordingPuller) EnsureImage(_ context.Context, ref string) (*imagecache.Image, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.got = append(p.got, ref)
	if ref == p.failRef {
		return nil, errors.New("registry unavailable")
	}
	return &imagecache.Image{Digest: pauseDigest(ref)}, nil
}

// pauseDigest is the digest recordingPuller resolves ref to.
func pauseDigest(ref string) v1.Hash {
	return v1.Hash{Algorithm: "sha256", Hex: fmt.Sprintf("%x", sha256.Sum256([]byte(ref)))}
}

// multiVersionConfig is a gVisor config whose default version "v2" is listed
// after an Enabled "v1", a Disabled "v0", and an Enabled "other-arch" with no
// assets for this node.
func multiVersionConfig() *v1alpha1.SandboxConfig {
	version := func(name, sha string, state *v1alpha1.SandboxVersionState) v1alpha1.SandboxVersionConfig {
		return v1alpha1.SandboxVersionConfig{
			Name:       name,
			State:      state,
			PauseImage: "pause-" + name,
			Assets: map[string]map[string]v1alpha1.AssetFile{
				runtime.GOARCH: {gvisorAssetName: {URL: "gs://bucket/" + name, SHA256: sha}},
			},
		}
	}
	otherArch := version("other-arch", "sha-other", nil)
	otherArch.Assets = map[string]map[string]v1alpha1.AssetFile{"other-arch": otherArch.Assets[runtime.GOARCH]}
	return &v1alpha1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "gvisor-release"},
		Spec: v1alpha1.SandboxConfigSpec{
			SandboxClass:   v1alpha1.SandboxClassGvisor,
			DefaultVersion: "v2",
			Versions: []v1alpha1.SandboxVersionConfig{
				version("v0", "sha-v0", ptr.To(v1alpha1.SandboxVersionStateDisabled)),
				version("v1", "sha-v1", ptr.To(v1alpha1.SandboxVersionStateEnabled)),
				otherArch,
				version("v2", "sha-v2", nil),
			},
		},
	}
}

// hangingObjectStorage blocks GetObject until the caller's context ends,
// simulating a bucket read that stalls without failing.
type hangingObjectStorage struct{}

func (hangingObjectStorage) GetObject(ctx context.Context, _, _ string) (io.ReadCloser, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (hangingObjectStorage) PutObject(_ context.Context, _, _ string, _ io.Reader) error { return nil }

func TestRecordForArch(t *testing.T) {
	sha := fmt.Sprintf("%x", sha256.Sum256([]byte("runsc")))
	tests := []struct {
		name   string
		assets map[string]map[string]v1alpha1.AssetFile
		want   *sandboxAssetsRecord
	}{{
		name:   "assets for this architecture",
		assets: map[string]map[string]v1alpha1.AssetFile{runtime.GOARCH: {runscAssetName: {URL: "gs://bucket/runsc", SHA256: sha}}},
		want: &sandboxAssetsRecord{
			SandboxClass: string(v1alpha1.SandboxClassGvisor),
			PauseImage:   "registry.k8s.io/pause@sha256:abc",
			Assets:       map[string]assetEntry{runscAssetName: {URL: "gs://bucket/runsc", SHA256: sha}},
		},
	}, {
		name:   "assets for another architecture only",
		assets: map[string]map[string]v1alpha1.AssetFile{"other-arch": {runscAssetName: {URL: "gs://bucket/runsc", SHA256: sha}}},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := gvisorConfig("gvisor-default", "", "")
			cfg.Spec.Versions[0].Assets = tt.assets
			got, ok := recordForArch(cfg, &cfg.Spec.Versions[0])
			if ok != (tt.want != nil) {
				t.Fatalf("recordForArch() ok = %v, want %v", ok, tt.want != nil)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("recordForArch() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRuntimeHash(t *testing.T) {
	gvisor := map[string]assetEntry{gvisorAssetName: {URL: "gs://bucket/gvisor", SHA256: "d547d81401461fd1c679c5c4fa0a6c2b8ef7dc3c22ce23c9e25dcc4c69cfd06f"}}
	pause := "sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4"

	// Snapshots record the hash, so a change to any of these values strands
	// every snapshot taken before it.
	tests := []struct {
		name        string
		rec         *sandboxAssetsRecord
		pauseDigest string
		want        string
	}{{
		name:        "gvisor asset and pause image",
		rec:         &sandboxAssetsRecord{SandboxClass: "gvisor", PauseImage: "registry.k8s.io/pause:3.10.2@" + pause, Assets: gvisor},
		pauseDigest: pause,
		want:        "sha256:7197896bb429f0b824accbc635a38c9ed5666c160070b833bbdc6e036ccd4f76",
	}, {
		// Only the digests count, not the version's other fields.
		name:        "another URL and pause image ref",
		rec:         &sandboxAssetsRecord{SandboxClass: "gvisor", PauseImage: "mirror.example.com/pause@" + pause, Assets: map[string]assetEntry{gvisorAssetName: {URL: "gs://other/gvisor", SHA256: gvisor[gvisorAssetName].SHA256}}},
		pauseDigest: pause,
		want:        "sha256:7197896bb429f0b824accbc635a38c9ed5666c160070b833bbdc6e036ccd4f76",
	}, {
		name: "no pause image",
		rec:  &sandboxAssetsRecord{SandboxClass: "microvm", Assets: gvisor},
		want: "sha256:f86e6f2bd8e25ac31a22e51e6e57e0eb6741eb0b0df77082989cc1e40f906fe1",
	}, {
		name:        "the asset under another name",
		rec:         &sandboxAssetsRecord{SandboxClass: "gvisor", Assets: map[string]assetEntry{runscAssetName: gvisor[gvisorAssetName]}},
		pauseDigest: pause,
		want:        "sha256:91e241c70c94d67b16127f1ba9c239e76ed1e91cb99fd9587b071cb8ccf7d3d8",
	}, {
		name: "several assets",
		rec: &sandboxAssetsRecord{SandboxClass: "microvm", Assets: map[string]assetEntry{
			"kernel": {SHA256: "aaaa"},
			"rootfs": {SHA256: "bbbb"},
		}},
		want: "sha256:fc253c8ca0d24c485ef86a377c0cd2cf97fbf922e9cabf290971c55f051158ba",
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := runtimeHash(tt.rec, tt.pauseDigest); got != tt.want {
				t.Errorf("runtimeHash() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPoolConfigName(t *testing.T) {
	tests := []struct {
		name string
		pool *v1alpha1.WorkerPool
		want string
	}{{
		name: "confirmed config of the pool's class",
		pool: resolvedPool("pool", "gvisor-default"),
		want: "gvisor-default",
	}, {
		name: "not resolved yet",
		pool: resolvedPool("pool", ""),
	}, {
		name: "only a pending config",
		pool: func() *v1alpha1.WorkerPool {
			pool := resolvedPool("pool", "")
			pool.Status.SandboxClasses[0].PendingConfigRef = &v1alpha1.SandboxConfigReference{Name: "gvisor-next"}
			return pool
		}(),
	}, {
		name: "status resolved another class than the pool's",
		pool: func() *v1alpha1.WorkerPool {
			pool := resolvedPool("pool", "gvisor-default")
			pool.Spec.SandboxClasses[0].Name = v1alpha1.SandboxClassMicroVM
			return pool
		}(),
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := poolConfigName(tt.pool); got != tt.want {
				t.Errorf("poolConfigName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestWorkerSandboxResolverResolve(t *testing.T) {
	cfg := gvisorConfig("gvisor-default", "gs://bucket/runsc", "abc")

	tests := []struct {
		name          string
		objs          []any
		unsynced      bool
		pod           *corev1.Pod
		wantConfig    string
		wantErr       bool
		wantPermanent bool
	}{{
		name:       "the config the pool resolved for the pool's class",
		objs:       []any{resolvedPool("pool", "gvisor-default"), cfg},
		pod:        workerPod("pod-a", "pool"),
		wantConfig: "gvisor-default",
	}, {
		// Startup never waits on these informers.
		name:     "caches not synced",
		objs:     []any{resolvedPool("pool", "gvisor-default"), cfg},
		unsynced: true,
		pod:      workerPod("pod-a", "pool"),
		wantErr:  true,
	}, {
		name:          "pod belongs to no pool",
		objs:          []any{resolvedPool("pool", "gvisor-default"), cfg},
		pod:           workerPod("pod-a", ""),
		wantErr:       true,
		wantPermanent: true,
	}, {
		name:    "pool not found",
		objs:    []any{cfg},
		pod:     workerPod("pod-a", "pool"),
		wantErr: true,
	}, {
		name:    "pool has not resolved a config",
		objs:    []any{resolvedPool("pool", ""), cfg},
		pod:     workerPod("pod-a", "pool"),
		wantErr: true,
	}, {
		// A pending config is one the atecontroller has claimed but not yet
		// confirmed exists and is not being deleted.
		name: "pool is only switching to a config",
		objs: []any{func() *v1alpha1.WorkerPool {
			pool := resolvedPool("pool", "")
			pool.Status.SandboxClasses[0].PendingConfigRef = &v1alpha1.SandboxConfigReference{Name: "gvisor-default"}
			return pool
		}(), cfg},
		pod:     workerPod("pod-a", "pool"),
		wantErr: true,
	}, {
		// The class comes from the pool's spec, not from the status entry.
		name: "status resolved another class than the pool's",
		objs: []any{func() *v1alpha1.WorkerPool {
			pool := resolvedPool("pool", "gvisor-default")
			pool.Spec.SandboxClasses[0].Name = v1alpha1.SandboxClassMicroVM
			return pool
		}(), cfg},
		pod:     workerPod("pod-a", "pool"),
		wantErr: true,
	}, {
		name:    "config not found",
		objs:    []any{resolvedPool("pool", "gvisor-missing"), cfg},
		pod:     workerPod("pod-a", "pool"),
		wantErr: true,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestSyncer(t, nil, tt.objs...)
			if tt.unsynced {
				s.resolver.synced = func() bool { return false }
			}
			class, got, err := s.resolver.resolve(tt.pod)
			if (err != nil) != tt.wantErr {
				t.Fatalf("resolve() error = %v, want error %v", err, tt.wantErr)
			}
			if errors.Is(err, errPermanent) != tt.wantPermanent {
				t.Errorf("resolve() error = %v, want permanent %v", err, tt.wantPermanent)
			}
			if err != nil {
				return
			}
			if got.Name != tt.wantConfig || class != v1alpha1.SandboxClassGvisor {
				t.Errorf("resolve() = %q, %q, want %q, %q", class, got.Name, v1alpha1.SandboxClassGvisor, tt.wantConfig)
			}
		})
	}
}

// wantRuntime is the runtime planAndFetch builds from multiVersionConfig's
// version name under recordingPuller.
func wantRuntime(name string) sandboxversions.Attributes {
	rec := &sandboxAssetsRecord{Assets: map[string]assetEntry{gvisorAssetName: {SHA256: "sha-" + name}}}
	return sandboxversions.Attributes{hardware.AttrSandboxRuntimeHash: runtimeHash(rec, pauseDigest("pause-"+name).String())}
}

// wantVersions is the file content for multiVersionConfig.
func wantVersions() *sandboxversions.File {
	return &sandboxversions.File{
		DefaultRuntime:     wantRuntime("v2"),
		RestorableRuntimes: []sandboxversions.Attributes{wantRuntime("v1")},
	}
}

// planAndFetch runs the plan and fetch steps of syncPod for pod.
func planAndFetch(s *workerSandboxSyncer, pod *corev1.Pod) (*sandboxversions.File, error) {
	plan, err := s.resolver.plan(pod)
	if err != nil {
		return nil, err
	}
	return s.fetch(context.Background(), plan)
}

func TestPlanAndFetch(t *testing.T) {
	defaultNoArch := multiVersionConfig()
	defaultNoArch.Spec.Versions[3].Assets = map[string]map[string]v1alpha1.AssetFile{"other-arch": defaultNoArch.Spec.Versions[3].Assets[runtime.GOARCH]}

	onlyDefault := &sandboxversions.File{DefaultRuntime: wantRuntime("v2")}

	// sameAsDefault has v1 run the default version's binaries.
	sameAsDefault := multiVersionConfig()
	sameAsDefault.Spec.Versions[1].PauseImage = sameAsDefault.Spec.Versions[3].PauseImage
	sameAsDefault.Spec.Versions[1].Assets = sameAsDefault.Spec.Versions[3].Assets

	// v1Twice adds an Enabled "v3" running v1's binaries.
	v1Twice := multiVersionConfig()
	v3 := *v1Twice.Spec.Versions[1].DeepCopy()
	v3.Name = "v3"
	v1Twice.Spec.Versions = append(v1Twice.Spec.Versions, v3)

	tests := []struct {
		name     string
		cfg      *v1alpha1.SandboxConfig
		fetchErr error
		// failSHA fails only the version whose asset has this sha256.
		failSHA string
		// failRef fails only the pull of this pause image.
		failRef       string
		want          *sandboxversions.File
		wantErr       bool
		wantPermanent bool
		wantFetched   int
		wantPulled    []string
	}{{
		// Disabled versions and versions with nothing for this node's
		// architecture are left out; the default leads.
		name:        "default first, then the other enabled versions",
		cfg:         multiVersionConfig(),
		want:        wantVersions(),
		wantFetched: 2,
		wantPulled:  []string{"pause-v1", "pause-v2"},
	}, {
		// The proto keeps default_runtime out of restorable_runtimes.
		name:        "a version with the default's binaries is left out",
		cfg:         sameAsDefault,
		want:        onlyDefault,
		wantFetched: 2,
		wantPulled:  []string{"pause-v2", "pause-v2"},
	}, {
		name:        "versions with equal binaries are listed once",
		cfg:         v1Twice,
		want:        wantVersions(),
		wantFetched: 3,
		wantPulled:  []string{"pause-v1", "pause-v1", "pause-v2"},
	}, {
		name:          "default version has no assets for this architecture",
		cfg:           defaultNoArch,
		wantErr:       true,
		wantPermanent: true,
	}, {
		// No worker of the pool could start actors on this node.
		name:        "a failed default version asset writes no file",
		cfg:         multiVersionConfig(),
		failSHA:     "sha-v2",
		wantErr:     true,
		wantFetched: 2,
		wantPulled:  []string{"pause-v1"},
	}, {
		name:        "a failed default version pause image writes no file",
		cfg:         multiVersionConfig(),
		failRef:     "pause-v2",
		wantErr:     true,
		wantFetched: 2,
		wantPulled:  []string{"pause-v1", "pause-v2"},
	}, {
		// The syncer writes the file and retries.
		name:        "a failed non-default version asset is left out and surfaced",
		cfg:         multiVersionConfig(),
		failSHA:     "sha-v1",
		want:        onlyDefault,
		wantErr:     true,
		wantFetched: 2,
		wantPulled:  []string{"pause-v2"},
	}, {
		name:        "a failed non-default version pause image is left out and surfaced",
		cfg:         multiVersionConfig(),
		failRef:     "pause-v1",
		want:        onlyDefault,
		wantErr:     true,
		wantFetched: 2,
		wantPulled:  []string{"pause-v1", "pause-v2"},
	}, {
		name:        "surfaces a failed fetch",
		cfg:         multiVersionConfig(),
		fetchErr:    errors.New("bucket unavailable"),
		wantErr:     true,
		wantFetched: 2,
	}, {
		name:    "surfaces an unresolved config",
		wantErr: true,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objs := []any{resolvedPool("pool", "gvisor-release")}
			if tt.cfg != nil {
				objs = append(objs, tt.cfg)
			}
			fetcher := &recordingFetcher{err: tt.fetchErr, failSHA: tt.failSHA}
			s := newTestSyncer(t, fetcher, objs...)
			puller := &recordingPuller{failRef: tt.failRef}
			s.images = puller

			got, err := planAndFetch(s, workerPod("pod-a", "pool"))
			if (err != nil) != tt.wantErr {
				t.Fatalf("planAndFetch() error = %v, want error %v", err, tt.wantErr)
			}
			if errors.Is(err, errPermanent) != tt.wantPermanent {
				t.Errorf("planAndFetch() error = %v, want permanent %v", err, tt.wantPermanent)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("planAndFetch() mismatch (-want +got):\n%s", diff)
			}
			if len(fetcher.got) != tt.wantFetched {
				t.Errorf("fetched %d versions, want %d", len(fetcher.got), tt.wantFetched)
			}
			// The versions are fetched concurrently, so the order is not fixed.
			slices.Sort(puller.got)
			if diff := cmp.Diff(tt.wantPulled, puller.got); diff != "" {
				t.Errorf("pulled pause images mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestFetchPausePull runs the pause image pull against a real image cache and
// registry: the image lands in the cache once the version's assets are
// fetched.
func TestFetchPausePull(t *testing.T) {
	origDir := nodepath.StaticFilesDir
	nodepath.StaticFilesDir = t.TempDir()
	t.Cleanup(func() { nodepath.StaticFilesDir = origDir })

	content := []byte("runsc binary bytes")

	tests := []struct {
		name    string
		storage objectstorage.ObjectStorage
		// sha is the runsc digest in the config. Each case uses its own so
		// the shared static-files cache never satisfies the fetch.
		sha         string
		wantErr     bool
		wantInCache bool
	}{{
		name:        "asset fetch succeeds",
		storage:     fakeObjectStorage{data: content},
		sha:         fmt.Sprintf("%x", sha256.Sum256(content)),
		wantInCache: true,
	}, {
		// The version failed, so its pause image is not pulled.
		name:    "asset fetch fails",
		storage: fakeObjectStorage{err: errors.New("bucket unavailable")},
		sha:     fmt.Sprintf("%x", sha256.Sum256([]byte("other runsc"))),
		wantErr: true,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
			defer srv.Close()
			u, err := url.Parse(srv.URL)
			if err != nil {
				t.Fatalf("parsing registry URL: %v", err)
			}
			pauseRef := u.Host + "/pause:3.10"
			pauseDigest := pushPauseImage(t, pauseRef)
			cfg := gvisorConfig("gvisor-release", "gs://bucket/runsc", tt.sha)
			cfg.Spec.Versions[0].PauseImage = pauseRef
			s := newTestSyncer(t, nil, workerPod("pod-a", "pool"), resolvedPool("pool", "gvisor-release"), cfg)
			s.assets = &AteomHerder{anonGCSClient: tt.storage}
			store, err := imagecache.New(t.TempDir())
			if err != nil {
				t.Fatalf("imagecache.New: %v", err)
			}
			s.images = store

			got, err := planAndFetch(s, workerPod("pod-a", "pool"))
			if (err != nil) != tt.wantErr {
				t.Fatalf("planAndFetch() error = %v, want error %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				// The hash covers the digest the tag resolved to.
				rec, _ := recordForArch(cfg, &cfg.Spec.Versions[0])
				want := &sandboxversions.File{DefaultRuntime: sandboxversions.Attributes{hardware.AttrSandboxRuntimeHash: runtimeHash(rec, pauseDigest.String())}}
				if diff := cmp.Diff(want, got); diff != "" {
					t.Errorf("planAndFetch() mismatch (-want +got):\n%s", diff)
				}
			}
			// With the registry gone, only a cache hit can satisfy a digest
			// ref.
			srv.Close()
			if _, err := store.EnsureImage(context.Background(), u.Host+"/pause@"+pauseDigest.String()); (err == nil) != tt.wantInCache {
				t.Errorf("pause image in the cache = %v (%v), want %v", err == nil, err, tt.wantInCache)
			}
		})
	}
}

// TestFetchTimeout verifies one pod's fetches are bounded by
// syncTimeout: the queue has one worker, so a fetch that never returned would
// block every other worker on the node.
func TestFetchTimeout(t *testing.T) {
	origDir, origTimeout := nodepath.StaticFilesDir, syncTimeout
	nodepath.StaticFilesDir = t.TempDir()
	syncTimeout = 50 * time.Millisecond
	t.Cleanup(func() { nodepath.StaticFilesDir, syncTimeout = origDir, origTimeout })

	cfg := gvisorConfig("gvisor-release", "gs://bucket/runsc", fmt.Sprintf("%x", sha256.Sum256([]byte("hung runsc"))))
	cfg.Spec.Versions[0].PauseImage = ""
	s := newTestSyncer(t, nil, workerPod("pod-a", "pool"), resolvedPool("pool", "gvisor-release"), cfg)
	s.assets = &AteomHerder{anonGCSClient: hangingObjectStorage{}}

	done := make(chan error, 1)
	go func() {
		_, err := planAndFetch(s, workerPod("pod-a", "pool"))
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Error("planAndFetch returned nil despite the download hanging")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("planAndFetch never returned; a hung download would block the syncer forever")
	}
}
