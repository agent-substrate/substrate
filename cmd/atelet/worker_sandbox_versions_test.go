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
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/cmd/atelet/internal/filecache"
	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/sandboxruntimes"
)

// multiVersionConfig is a gVisor config whose default version "v2" is listed
// after an Enabled "v1", a Disabled "v0", and an Enabled "other-arch" with no
// assets for this node.
func multiVersionConfig() *atev1.SandboxConfig {
	version := func(name, sha string, state *atev1.SandboxVersionState) atev1.SandboxVersionConfig {
		return atev1.SandboxVersionConfig{
			Name:  name,
			State: state,
			Assets: map[string]map[string]atev1.AssetFile{
				runtime.GOARCH: {gvisorAssetName: {URL: "gs://bucket/" + name, SHA256: sha}},
			},
		}
	}
	otherArch := version("other-arch", "sha-other", nil)
	otherArch.Assets = map[string]map[string]atev1.AssetFile{"other-arch": otherArch.Assets[runtime.GOARCH]}
	return &atev1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "gvisor-release"},
		Spec: atev1.SandboxConfigSpec{
			SandboxClass:   atev1.SandboxClassGvisor,
			DefaultVersion: "v2",
			Versions: []atev1.SandboxVersionConfig{
				version("v0", "sha-v0", ptr.To(atev1.SandboxVersionStateDisabled)),
				version("v1", "sha-v1", ptr.To(atev1.SandboxVersionStateEnabled)),
				otherArch,
				version("v2", "sha-v2", nil),
			},
		},
	}
}

func TestRecordForArch(t *testing.T) {
	sha := fmt.Sprintf("%x", sha256.Sum256([]byte("gvisor release")))
	tests := []struct {
		name   string
		assets map[string]map[string]atev1.AssetFile
		want   *sandboxAssetsRecord
	}{{
		name:   "assets for this architecture",
		assets: map[string]map[string]atev1.AssetFile{runtime.GOARCH: {gvisorAssetName: {URL: "gs://bucket/gvisor", SHA256: sha}}},
		want: &sandboxAssetsRecord{
			SandboxClass: string(atev1.SandboxClassGvisor),
			Assets:       map[string]assetEntry{gvisorAssetName: {URL: "gs://bucket/gvisor", SHA256: sha}},
		},
	}, {
		name:   "assets for another architecture only",
		assets: map[string]map[string]atev1.AssetFile{"other-arch": {gvisorAssetName: {URL: "gs://bucket/gvisor", SHA256: sha}}},
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

func TestBundleHash(t *testing.T) {
	gvisor := map[string]assetEntry{gvisorAssetName: {URL: "gs://bucket/gvisor", SHA256: "d547d81401461fd1c679c5c4fa0a6c2b8ef7dc3c22ce23c9e25dcc4c69cfd06f"}}

	// Snapshots record the hash, so a change to any of these values strands
	// every snapshot taken before it.
	tests := []struct {
		name string
		rec  *sandboxAssetsRecord
		want string
	}{{
		name: "gvisor asset",
		rec:  &sandboxAssetsRecord{SandboxClass: "gvisor", Assets: gvisor},
		want: "sha256:f86e6f2bd8e25ac31a22e51e6e57e0eb6741eb0b0df77082989cc1e40f906fe1",
	}, {
		// Only the digests count, not the version's other fields.
		name: "another URL and class",
		rec:  &sandboxAssetsRecord{SandboxClass: "microvm", Assets: map[string]assetEntry{gvisorAssetName: {URL: "gs://other/gvisor", SHA256: gvisor[gvisorAssetName].SHA256}}},
		want: "sha256:f86e6f2bd8e25ac31a22e51e6e57e0eb6741eb0b0df77082989cc1e40f906fe1",
	}, {
		name: "the asset under another name",
		rec:  &sandboxAssetsRecord{SandboxClass: "gvisor", Assets: map[string]assetEntry{"renamed": gvisor[gvisorAssetName]}},
		want: "sha256:510948cc43e084528a7c76c1627c5bfb2d140be0b620f33282c03fc2debd65af",
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
			if got := bundleHash(tt.rec); got != tt.want {
				t.Errorf("bundleHash() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestWorkerSandboxResolverResolve(t *testing.T) {
	cfg := gvisorConfig("gvisor-default", "gs://bucket/gvisor", "abc")

	tests := []struct {
		name       string
		objs       []any
		pod        *corev1.Pod
		wantConfig string
		wantErr    bool
		// wantPermanent and wantUnresolved are the error's class, which
		// decides whether finish drops it, caps its retries, or retries it
		// without a cap.
		wantPermanent  bool
		wantUnresolved bool
	}{{
		name:       "the config the pool resolved for the pool's class",
		objs:       []any{resolvedPool("pool", "gvisor-default"), cfg},
		pod:        workerPod("pod-a", "pool"),
		wantConfig: "gvisor-default",
	}, {
		name:          "pod belongs to no pool",
		objs:          []any{resolvedPool("pool", "gvisor-default"), cfg},
		pod:           workerPod("pod-a", ""),
		wantErr:       true,
		wantPermanent: true,
	}, {
		name:           "pool not found",
		objs:           []any{cfg},
		pod:            workerPod("pod-a", "pool"),
		wantErr:        true,
		wantUnresolved: true,
	}, {
		name:           "pool has not resolved a config",
		objs:           []any{resolvedPool("pool", ""), cfg},
		pod:            workerPod("pod-a", "pool"),
		wantErr:        true,
		wantUnresolved: true,
	}, {
		// A pending config is one the atecontroller has claimed but not yet
		// confirmed exists and is not being deleted.
		name: "pool is only switching to a config",
		objs: []any{func() *atev1.WorkerPool {
			pool := resolvedPool("pool", "")
			pool.Status.SandboxClasses[0].PendingConfigRef = &atev1.SandboxConfigReference{Name: "gvisor-default"}
			return pool
		}(), cfg},
		pod:            workerPod("pod-a", "pool"),
		wantErr:        true,
		wantUnresolved: true,
	}, {
		// The class comes from the pool's spec, not from the status entry.
		name: "status resolved another class than the pool's",
		objs: []any{func() *atev1.WorkerPool {
			pool := resolvedPool("pool", "gvisor-default")
			pool.Spec.SandboxClasses[0].Name = atev1.SandboxClassMicroVM
			return pool
		}(), cfg},
		pod:            workerPod("pod-a", "pool"),
		wantErr:        true,
		wantUnresolved: true,
	}, {
		name:           "config not found",
		objs:           []any{resolvedPool("pool", "gvisor-missing"), cfg},
		pod:            workerPod("pod-a", "pool"),
		wantErr:        true,
		wantUnresolved: true,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestSyncer(t, tt.objs...)
			got, err := s.resolver.resolve(tt.pod)
			if (err != nil) != tt.wantErr {
				t.Fatalf("resolve() error = %v, want error %v", err, tt.wantErr)
			}
			if errors.Is(err, errPermanent) != tt.wantPermanent {
				t.Errorf("resolve() error = %v, want permanent %v", err, tt.wantPermanent)
			}
			if errors.Is(err, errUnresolved) != tt.wantUnresolved {
				t.Errorf("resolve() error = %v, want unresolved %v", err, tt.wantUnresolved)
			}
			if err != nil {
				return
			}
			if got.Name != tt.wantConfig {
				t.Errorf("resolve() = %q, want %q", got.Name, tt.wantConfig)
			}
		})
	}
}

// wantRuntime is the runtime buildFile builds from multiVersionConfig's
// version with the given name when runsc reports testCPUFeatures() ("3").
func wantRuntime(name string) sandboxruntimes.Attributes {
	return wantRuntimeWithFeatures(name, "3")
}

func wantRuntimeWithFeatures(name, cpuFeatures string) sandboxruntimes.Attributes {
	rec := &sandboxAssetsRecord{Assets: map[string]assetEntry{gvisorAssetName: {SHA256: "sha-" + name}}}
	return sandboxruntimes.Attributes{
		hardware.AttrArchitecture:      runtime.GOARCH,
		hardware.AttrCPUFeatures:       cpuFeatures,
		hardware.AttrSandboxBundleHash: bundleHash(rec),
	}
}

// classFile is the file content of a worker in a pool of the given class:
// one entry, with the given default and restorable runtimes.
func classFile(class atev1.SandboxClass, def sandboxruntimes.Attributes, restorable ...sandboxruntimes.Attributes) *sandboxruntimes.File {
	c := sandboxruntimes.Class{SandboxClass: string(class), DefaultRuntime: sandboxruntimes.Runtime{Version: def}}
	for _, r := range restorable {
		c.RestorableRuntimes = append(c.RestorableRuntimes, sandboxruntimes.Runtime{Version: r})
	}
	return &sandboxruntimes.File{SandboxClasses: []sandboxruntimes.Class{c}}
}

// gvisorFile is classFile for a gVisor pool.
func gvisorFile(def sandboxruntimes.Attributes, restorable ...sandboxruntimes.Attributes) *sandboxruntimes.File {
	return classFile(atev1.SandboxClassGvisor, def, restorable...)
}

// wantVersions is the file content for multiVersionConfig.
func wantVersions() *sandboxruntimes.File {
	return gvisorFile(wantRuntime("v2"), wantRuntime("v1"))
}

// planAndBuild runs the plan and build steps of syncPod for pod.
func planAndBuild(s *workerSandboxSyncer, pod *corev1.Pod) (*sandboxruntimes.File, error) {
	plan, err := s.resolver.plan(pod)
	if err != nil {
		return nil, err
	}
	return s.buildFile(context.Background(), plan, hashVersions(plan))
}

func TestPlanAndBuild(t *testing.T) {
	defaultNoArch := multiVersionConfig()
	defaultNoArch.Spec.Versions[3].Assets = map[string]map[string]atev1.AssetFile{"other-arch": defaultNoArch.Spec.Versions[3].Assets[runtime.GOARCH]}

	onlyDefault := gvisorFile(wantRuntime("v2"))

	// sameAsDefault has v1 run the default version's binaries.
	sameAsDefault := multiVersionConfig()
	sameAsDefault.Spec.Versions[1].Assets = sameAsDefault.Spec.Versions[3].Assets

	// v1Twice adds an Enabled "v3" running v1's binaries.
	v1Twice := multiVersionConfig()
	v3 := *v1Twice.Spec.Versions[1].DeepCopy()
	v3.Name = "v3"
	v1Twice.Spec.Versions = append(v1Twice.Spec.Versions, v3)

	unsupportedClass := multiVersionConfig()
	unsupportedClass.Spec.SandboxClass = "unknown"

	noGVisorAsset := multiVersionConfig()
	noGVisorAsset.Spec.Versions[3].Assets = map[string]map[string]atev1.AssetFile{
		runtime.GOARCH: {"other": {URL: "gs://bucket/other", SHA256: "sha-other"}},
	}

	tests := []struct {
		name          string
		cfg           *atev1.SandboxConfig
		want          *sandboxruntimes.File
		wantErr       bool
		wantPermanent bool
	}{{
		// Disabled versions and versions with nothing for this node's
		// architecture are left out; the default leads.
		name: "default first, then the other enabled versions",
		cfg:  multiVersionConfig(),
		want: wantVersions(),
	}, {
		// The proto keeps default_runtime out of restorable_runtimes.
		name: "a version with the default's binaries is left out",
		cfg:  sameAsDefault,
		want: onlyDefault,
	}, {
		name: "versions with equal binaries are listed once",
		cfg:  v1Twice,
		want: wantVersions(),
	}, {
		name:          "default version has no assets for this architecture",
		cfg:           defaultNoArch,
		wantErr:       true,
		wantPermanent: true,
	}, {
		name:          "unsupported sandbox class",
		cfg:           unsupportedClass,
		wantErr:       true,
		wantPermanent: true,
	}, {
		name:          "gvisor version has no gvisor asset",
		cfg:           noGVisorAsset,
		wantErr:       true,
		wantPermanent: true,
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
			s := newTestSyncer(t, objs...)

			got, err := planAndBuild(s, workerPod("pod-a", "pool"))
			if (err != nil) != tt.wantErr {
				t.Fatalf("planAndBuild() error = %v, want error %v", err, tt.wantErr)
			}
			if errors.Is(err, errPermanent) != tt.wantPermanent {
				t.Errorf("planAndBuild() error = %v, want permanent %v", err, tt.wantPermanent)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("planAndBuild() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPlanAndBuildMicroVM(t *testing.T) {
	pool := resolvedPoolOf(atev1.SandboxClassMicroVM, "microvm-pool", "microvm-release")
	version := func(name, sha string) atev1.SandboxVersionConfig {
		return atev1.SandboxVersionConfig{
			Name: name,
			Assets: map[string]map[string]atev1.AssetFile{
				runtime.GOARCH: {"kernel": {URL: "gs://bucket/kernel-" + name, SHA256: sha}},
			},
		}
	}
	cfg := &atev1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "microvm-release"},
		Spec: atev1.SandboxConfigSpec{
			SandboxClass:   atev1.SandboxClassMicroVM,
			DefaultVersion: "v2",
			Versions: []atev1.SandboxVersionConfig{
				version("v1", "sha-v1"),
				version("v2", "sha-v2"),
			},
		},
	}
	wantMicroVM := func(sha string) sandboxruntimes.Attributes {
		rec := &sandboxAssetsRecord{Assets: map[string]assetEntry{"kernel": {SHA256: sha}}}
		return sandboxruntimes.Attributes{
			hardware.AttrArchitecture:      runtime.GOARCH,
			hardware.AttrCPUVendor:         testCPUVendor,
			hardware.AttrCPUModel:          testCPUModel,
			hardware.AttrSandboxBundleHash: bundleHash(rec),
		}
	}

	s := newTestSyncer(t, pool, cfg)
	var fetched []string
	var failSHA string
	s.assets = &fakeAssetFetcher{
		fn: func(ctx context.Context, rec *sandboxAssetsRecord) (map[string]string, error) {
			sha := rec.Assets["kernel"].SHA256
			fetched = append(fetched, sha)
			if sha == failSHA {
				return nil, fmt.Errorf("bad sha256 for %s", sha)
			}
			return newTestAssetFetcher().ensureSandboxAssets(ctx, rec, "")
		},
	}
	probeCalls := 0
	probeErr := errors.New("broken /proc/cpuinfo")
	s.probeCPUIdentity = func() (string, string, error) {
		probeCalls++
		if probeErr != nil {
			return "", "", probeErr
		}
		return testCPUVendor, testCPUModel, nil
	}

	// A failed probe is not cached: the sync fails with a retryable error and
	// the next one probes again.
	if _, err := planAndBuild(s, workerPod("pod-a", "microvm-pool")); !errors.Is(err, probeErr) || errors.Is(err, errPermanent) {
		t.Fatalf("planAndBuild() on probe failure = %v, want %v and not permanent", err, probeErr)
	}

	// A failed fetch on the default version fails the build without writing.
	probeErr = nil
	failSHA = "sha-v2"
	if _, err := planAndBuild(s, workerPod("pod-a", "microvm-pool")); err == nil || errors.Is(err, errPermanent) || errors.Is(err, errIncomplete) {
		t.Fatalf("planAndBuild() on default asset fetch failure = %v, want retryable error", err)
	}

	// A failed fetch on a restorable version returns the default runtime
	// alongside errIncomplete, leaving the unverified restorable version out.
	failSHA = "sha-v1"
	partial, err := planAndBuild(s, workerPod("pod-a", "microvm-pool"))
	if !errors.Is(err, errIncomplete) || errors.Is(err, errPermanent) {
		t.Fatalf("planAndBuild() on restorable asset fetch failure = %v, want errIncomplete", err)
	}
	if diff := cmp.Diff(classFile(atev1.SandboxClassMicroVM, wantMicroVM("sha-v2")), partial); diff != "" {
		t.Errorf("planAndBuild() partial file mismatch (-want +got):\n%s", diff)
	}

	// Once a restorable version's assets are verified, the check is memoized
	// by bundle hash alongside the host CPU identity probe.
	failSHA = ""
	want := classFile(atev1.SandboxClassMicroVM, wantMicroVM("sha-v2"), wantMicroVM("sha-v1"))
	got, err := planAndBuild(s, workerPod("pod-a", "microvm-pool"))
	if err != nil {
		t.Fatalf("planAndBuild(pod-a) error = %v", err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("planAndBuild(pod-a) mismatch (-want +got):\n%s", diff)
	}

	// Syncing pod-b stages the default (v2) again and reuses the memoized check
	// for the restorable version (v1) without fetching v1 again.
	failSHA = "sha-v1"
	got, err = planAndBuild(s, workerPod("pod-b", "microvm-pool"))
	if err != nil {
		t.Fatalf("planAndBuild(pod-b) error = %v", err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("planAndBuild(pod-b) mismatch (-want +got):\n%s", diff)
	}
	if probeCalls != 2 {
		t.Errorf("probe calls = %d, want 2: one that failed, one that succeeded and was reused", probeCalls)
	}
	// The probe-failure build fetches nothing. The default-failure build
	// fetches v2, the restorable-failure build fetches v2 then fails on v1,
	// pod-a fetches v2 and v1 (memoizing v1), and pod-b fetches only the
	// default v2.
	wantFetched := []string{"sha-v2", "sha-v2", "sha-v1", "sha-v2", "sha-v1", "sha-v2"}
	if diff := cmp.Diff(wantFetched, fetched); diff != "" {
		t.Errorf("fetched assets mismatch (-want +got):\n%s", diff)
	}
}

// TestSyncPodMicroVMAssetCacheAndEviction runs syncPod for a microVM pool
// against the real sandboxAssetCache and runtimePrewarmer:
//   - buildFile stages each version's assets into scratchDir and cleans up;
//   - the prewarmer links only the default version under the worker's directory,
//     so EvictUnused reclaims the unlinked restorable version;
//   - once memoized, syncing a new worker pod does not re-download the evicted
//     restorable version, and re-syncing while the bucket is failing keeps the
//     restorable version in sandbox-runtimes.json;
//   - promoting the evicted restorable version to default while the bucket is
//     failing fails the sync and leaves the file and the old default's
//     directory as they were.
func TestSyncPodMicroVMAssetCacheAndEviction(t *testing.T) {
	ctx := context.Background()
	v1Bytes, v2Bytes := []byte("kernel v1"), []byte("kernel v2")
	v1SHA := fmt.Sprintf("%x", sha256.Sum256(v1Bytes))
	v2SHA := fmt.Sprintf("%x", sha256.Sum256(v2Bytes))

	pool := resolvedPoolOf(atev1.SandboxClassMicroVM, "microvm-pool", "microvm-release")
	version := func(name, sha string) atev1.SandboxVersionConfig {
		return atev1.SandboxVersionConfig{
			Name: name,
			Assets: map[string]map[string]atev1.AssetFile{
				runtime.GOARCH: {"kernel": {URL: "gs://assets/kernel-" + name, SHA256: sha}},
			},
		}
	}
	cfg := &atev1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "microvm-release"},
		Spec: atev1.SandboxConfigSpec{
			SandboxClass:   atev1.SandboxClassMicroVM,
			DefaultVersion: "v2",
			Versions: []atev1.SandboxVersionConfig{
				version("v1", v1SHA),
				version("v2", v2SHA),
			},
		},
	}
	wantMicroVM := func(sha string) sandboxruntimes.Attributes {
		rec := &sandboxAssetsRecord{Assets: map[string]assetEntry{"kernel": {SHA256: sha}}}
		return sandboxruntimes.Attributes{
			hardware.AttrArchitecture:      runtime.GOARCH,
			hardware.AttrCPUVendor:         testCPUVendor,
			hardware.AttrCPUModel:          testCPUModel,
			hardware.AttrSandboxBundleHash: bundleHash(rec),
		}
	}
	want := classFile(atev1.SandboxClassMicroVM, wantMicroVM(v2SHA), wantMicroVM(v1SHA))

	root := t.TempDir()
	storage := &bucketStorage{
		objects: map[string][]byte{
			"assets/kernel-v1": v1Bytes,
			"assets/kernel-v2": v2Bytes,
		},
	}
	herder := &Atelet{
		sandboxAssetCache: newTestAssetCacheAt(t, root, filecache.WithMinAge(0)),
		anonGCSClient:     storage,
	}
	s := newTestSyncer(t, pool, cfg)
	s.assets = herder
	s.workersDir = filepath.Join(root, "workers")
	s.scratchDir = filepath.Join(root, "scratch")
	if err := os.MkdirAll(s.scratchDir, 0o700); err != nil {
		t.Fatal(err)
	}
	recPrewarm := &recordingPrewarm{}
	s.prewarm = recPrewarm

	if err := s.syncPod(ctx, workerPod("pod-a", "microvm-pool")); err != nil {
		t.Fatalf("syncPod(pod-a): %v", err)
	}
	if left, err := os.ReadDir(s.scratchDir); err != nil || len(left) != 0 {
		t.Errorf("scratchDir entries after syncPod = %v, %v; want empty", left, err)
	}
	if got := storage.gets.Load(); got != 2 {
		t.Fatalf("bucket gets after first sync = %d, want 2", got)
	}
	if n := cachedAssets(t, root); n != 2 {
		t.Fatalf("cached assets after first sync = %d, want 2", n)
	}

	// Link the default version (v2) into pod-a's default-runtime directory via
	// the prewarmer, leaving the restorable version (v1) unlinked.
	p := newRuntimePrewarmer(s.index, herder)
	for _, item := range recPrewarm.items() {
		p.process(ctx, item)
	}
	stats, err := herder.sandboxAssetCache.EvictUnused(ctx, 1<<40, false)
	if err != nil {
		t.Fatalf("EvictUnused: %v", err)
	}
	if stats.Retired != 1 || stats.SkippedLinked != 1 || cachedAssets(t, root) != 1 {
		t.Fatalf("after EvictUnused: stats=%+v, cached=%d; want 1 retired (v1) and 1 linked kept (v2)", stats, cachedAssets(t, root))
	}

	// Take the bucket offline and verify that syncing a new pod (pod-b) and
	// re-syncing pod-a neither re-download v1 nor drop it from the written file.
	storage.mu.Lock()
	clear(storage.objects)
	storage.mu.Unlock()

	for _, uid := range []string{"pod-b", "pod-a"} {
		if err := s.syncPod(ctx, workerPod(uid, "microvm-pool")); err != nil {
			t.Fatalf("syncPod(%s) after eviction with bucket offline: %v", uid, err)
		}
		got, err := sandboxruntimes.Read(s.runtimesFile(uid))
		if err != nil {
			t.Fatalf("sandboxruntimes.Read(%s): %v", uid, err)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("sandbox-runtimes.json for %s mismatch (-want +got):\n%s", uid, diff)
		}
	}
	if got := storage.gets.Load(); got != 2 {
		t.Errorf("bucket gets after syncing pod-b and re-syncing pod-a = %d, want 2 (no re-downloads)", got)
	}

	// Promoting the evicted v1 to default while the bucket is offline must fail
	// the sync and keep pod-a's previous file and default-runtime pin in place.
	cfg.Spec.DefaultVersion = "v1"
	if err := s.syncPod(ctx, workerPod("pod-a", "microvm-pool")); err == nil {
		t.Fatal("syncPod(pod-a) after promoting evicted v1 to default with bucket offline = nil, want error")
	}
	got, err := sandboxruntimes.Read(s.runtimesFile("pod-a"))
	if err != nil {
		t.Fatalf("sandboxruntimes.Read(pod-a): %v", err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("sandbox-runtimes.json for pod-a after failed default promotion (-want +got):\n%s", diff)
	}
	v2Hash := want.SandboxClasses[0].DefaultRuntime.Version[hardware.AttrSandboxBundleHash]
	if !exists(s.defaultRuntimeDir("pod-a", v2Hash)) {
		t.Error("a failed sync removed pod-a's default-runtime directory for v2")
	}
	if n := cachedAssets(t, root); n != 1 {
		t.Errorf("cached assets after failed default promotion = %d, want 1 (v2 still linked)", n)
	}
	if got := storage.gets.Load(); got != 3 {
		t.Errorf("bucket gets after promoting evicted v1 to default = %d, want 3 (the default is checked, not memoized)", got)
	}
}

func TestGVisorCPUFeaturesMemoizationAndErrors(t *testing.T) {
	feats := testCPUFeatures()

	t.Run("a version with an earlier version's binaries in another bucket is neither fetched nor probed", func(t *testing.T) {
		// bundleHash leaves URLs out, so v3 is v1's runtime. The syncer must
		// see that from the spec alone, before touching v3's bucket.
		cfg := multiVersionConfig()
		v3 := *cfg.Spec.Versions[1].DeepCopy()
		v3.Name = "v3"
		v3.Assets[runtime.GOARCH][gvisorAssetName] = atev1.AssetFile{URL: "gs://other-bucket/v1", SHA256: "sha-v1"}
		cfg.Spec.Versions = append(cfg.Spec.Versions, v3)

		s := newTestSyncer(t, resolvedPool("pool", "gvisor-release"), cfg)
		s.assets = &fakeAssetFetcher{
			fn: func(ctx context.Context, rec *sandboxAssetsRecord) (map[string]string, error) {
				if url := rec.Assets[gvisorAssetName].URL; strings.HasPrefix(url, "gs://other-bucket/") {
					return nil, fmt.Errorf("fetched a duplicate runtime from %s", url)
				}
				return newTestAssetFetcher().ensureSandboxAssets(ctx, rec, "")
			},
		}
		got, err := planAndBuild(s, workerPod("pod-a", "pool"))
		if err != nil {
			t.Fatalf("planAndBuild() error = %v", err)
		}
		if diff := cmp.Diff(wantVersions(), got); diff != "" {
			t.Errorf("planAndBuild() mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("memoizes runsc cpu-features by asset digest across versions and syncs", func(t *testing.T) {
		cfg := multiVersionConfig()
		// Add a third version "v3" that shares v1's gvisor asset digest ("sha-v1")
		// but has one more asset, so it has a distinct bundle hash while it
		// uses the same runsc binary.
		v3 := *cfg.Spec.Versions[1].DeepCopy()
		v3.Name = "v3"
		v3.Assets[runtime.GOARCH]["extra"] = atev1.AssetFile{URL: "gs://bucket/extra", SHA256: "sha-extra"}
		cfg.Spec.Versions = append(cfg.Spec.Versions, v3)

		s := newTestSyncer(t, resolvedPool("pool", "gvisor-release"), cfg)
		s.assets = &fakeAssetFetcher{
			fn: func(_ context.Context, rec *sandboxAssetsRecord) (map[string]string, error) {
				return map[string]string{gvisorAssetName: "/cache/" + rec.Assets[gvisorAssetName].SHA256 + "/runsc"}, nil
			},
		}
		execCalls := make(map[string]int)
		s.runCPUFeatures = func(_ context.Context, runscPath string) ([]string, error) {
			execCalls[runscPath]++
			if runscPath == "/cache/sha-v2/runsc" {
				return feats, nil // bits 0 and 1 -> "3"
			}
			return feats[:1], nil // bit 0 only -> "1"
		}

		got, err := planAndBuild(s, workerPod("pod-a", "pool"))
		if err != nil {
			t.Fatalf("first planAndBuild() error = %v", err)
		}
		v3Rec := &sandboxAssetsRecord{Assets: map[string]assetEntry{gvisorAssetName: {SHA256: "sha-v1"}, "extra": {SHA256: "sha-extra"}}}
		want := gvisorFile(
			wantRuntimeWithFeatures("v2", "3"),
			wantRuntimeWithFeatures("v1", "1"),
			sandboxruntimes.Attributes{
				hardware.AttrArchitecture:      runtime.GOARCH,
				hardware.AttrCPUFeatures:       "1",
				hardware.AttrSandboxBundleHash: bundleHash(v3Rec),
			},
		)
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("planAndBuild() mismatch (-want +got):\n%s", diff)
		}

		// Syncing again must answer from the memo alone: no fetch, no runsc.
		s.assets = &fakeAssetFetcher{
			fn: func(context.Context, *sandboxAssetsRecord) (map[string]string, error) {
				return nil, errors.New("fetched a runsc whose cpu_features are memoized")
			},
		}
		if _, err := planAndBuild(s, workerPod("pod-b", "pool")); err != nil {
			t.Fatalf("second planAndBuild() error = %v", err)
		}
		wantCalls := map[string]int{
			"/cache/sha-v2/runsc": 1,
			"/cache/sha-v1/runsc": 1,
		}
		if diff := cmp.Diff(wantCalls, execCalls); diff != "" {
			t.Errorf("runCPUFeatures call counts mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("transient asset fetch or runsc execution error is retriable", func(t *testing.T) {
		s := newTestSyncer(t, resolvedPool("pool", "gvisor-release"), multiVersionConfig())
		s.assets = &fakeAssetFetcher{
			fn: func(context.Context, *sandboxAssetsRecord) (map[string]string, error) {
				return nil, errors.New("temporary download failure")
			},
		}
		if _, err := planAndBuild(s, workerPod("pod-a", "pool")); err == nil || errors.Is(err, errPermanent) {
			t.Errorf("planAndBuild() on asset fetch error = %v, want non-permanent error", err)
		}

		s = newTestSyncer(t, resolvedPool("pool", "gvisor-release"), multiVersionConfig())
		s.runCPUFeatures = func(context.Context, string) ([]string, error) {
			return nil, errors.New("runsc failed")
		}
		if _, err := planAndBuild(s, workerPod("pod-a", "pool")); err == nil || errors.Is(err, errPermanent) {
			t.Errorf("planAndBuild() on runCPUFeatures error = %v, want non-permanent error", err)
		}

		// On a restorable version alone, buildFile returns the default runtime
		// alongside errIncomplete so syncPod writes the partial file right away
		// and requeues for the rest.
		s = newTestSyncer(t, resolvedPool("pool", "gvisor-release"), multiVersionConfig())
		s.assets = &fakeAssetFetcher{
			fn: func(_ context.Context, rec *sandboxAssetsRecord) (map[string]string, error) {
				if sha := rec.Assets[gvisorAssetName].SHA256; sha != "sha-v2" {
					return nil, fmt.Errorf("temporary download failure for %s", sha)
				}
				return map[string]string{gvisorAssetName: "sha-v2"}, nil
			},
		}
		s.runCPUFeatures = func(context.Context, string) ([]string, error) { return feats, nil }
		got, err := planAndBuild(s, workerPod("pod-a", "pool"))
		if !errors.Is(err, errIncomplete) || errors.Is(err, errPermanent) {
			t.Errorf("planAndBuild() on a restorable version's fetch error = %v, want errIncomplete and not errPermanent", err)
		}
		want := gvisorFile(wantRuntime("v2"))
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("planAndBuild() partial file mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("unknown CPU feature on default version is permanent, on non-default is skipped", func(t *testing.T) {
		s := newTestSyncer(t, resolvedPool("pool", "gvisor-release"), multiVersionConfig())
		s.runCPUFeatures = func(context.Context, string) ([]string, error) {
			return []string{"not_a_known_feature"}, nil
		}
		if _, err := planAndBuild(s, workerPod("pod-a", "pool")); !errors.Is(err, errPermanent) {
			t.Errorf("planAndBuild() with unknown feature on default = %v, want errPermanent", err)
		}

		s = newTestSyncer(t, resolvedPool("pool", "gvisor-release"), multiVersionConfig())
		s.assets = &fakeAssetFetcher{
			fn: func(_ context.Context, rec *sandboxAssetsRecord) (map[string]string, error) {
				return map[string]string{gvisorAssetName: rec.Assets[gvisorAssetName].SHA256}, nil
			},
		}
		s.runCPUFeatures = func(_ context.Context, runscPath string) ([]string, error) {
			if runscPath == "sha-v2" {
				return feats, nil
			}
			return []string{"future_unknown_feature"}, nil
		}
		got, err := planAndBuild(s, workerPod("pod-a", "pool"))
		if err != nil {
			t.Fatalf("planAndBuild() with unknown feature on non-default error = %v", err)
		}
		want := gvisorFile(wantRuntime("v2"))
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("planAndBuild() mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestRunRunscCPUFeatures(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	script := func(name, body string) string {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte("#!/bin/sh\n"+body), 0o755); err != nil {
			t.Fatal(err)
		}
		return p
	}

	got, err := runRunscCPUFeatures(ctx, script("ok", "echo pni,avx2\n"))
	if err != nil {
		t.Fatalf("runRunscCPUFeatures() error = %v", err)
	}
	if diff := cmp.Diff([]string{"pni", "avx2"}, got); diff != "" {
		t.Errorf("runRunscCPUFeatures() mismatch (-want +got):\n%s", diff)
	}

	// A binary that runs and fails, or prints nothing, will do so again:
	// permanent, with its stderr in the error.
	_, err = runRunscCPUFeatures(ctx, script("fail", "echo boom >&2\nexit 1\n"))
	if !errors.Is(err, errPermanent) || !strings.Contains(err.Error(), "boom") {
		t.Errorf("runRunscCPUFeatures() on a failing binary = %v, want permanent error quoting boom", err)
	}
	if _, err := runRunscCPUFeatures(ctx, script("empty", "exit 0\n")); !errors.Is(err, errPermanent) {
		t.Errorf("runRunscCPUFeatures() on empty output = %v, want permanent error", err)
	}

	// A binary that cannot be started, or is cut off, is retried.
	if _, err := runRunscCPUFeatures(ctx, filepath.Join(dir, "missing")); err == nil || errors.Is(err, errPermanent) {
		t.Errorf("runRunscCPUFeatures() on a missing binary = %v, want non-permanent error", err)
	}
	origTimeout := cpuFeaturesTimeout
	cpuFeaturesTimeout = 50 * time.Millisecond
	t.Cleanup(func() { cpuFeaturesTimeout = origTimeout })
	// exec, so the kill reaches the process holding stdout.
	_, err = runRunscCPUFeatures(ctx, script("hang", "exec sleep 10\n"))
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errPermanent) {
		t.Errorf("runRunscCPUFeatures() on a hanging binary = %v, want context.DeadlineExceeded", err)
	}
}

// TestSyncPodAdvertisesWhatItWrote pins when a worker's index entry changes:
// with its file. A sync that fails before the write leaves the entry of the
// file still on disk, and one that writes a partial file names only the
// runtimes in it, while every hashed runtime resolves either way.
func TestSyncPodAdvertisesWhatItWrote(t *testing.T) {
	ctx := context.Background()
	pod := workerPod("pod-a", "pool")
	hashOf := func(name string) string { return wantRuntime(name)[hardware.AttrSandboxBundleHash] }
	advertised := func(t *testing.T, s *workerSandboxSyncer) *classRuntime {
		t.Helper()
		wr, ok := s.index.worker("pod-a")
		if !ok {
			t.Fatal("pod-a has no index entry")
		}
		cr, ok := wr.class(atev1.SandboxClassGvisor)
		if !ok {
			t.Fatal("pod-a advertises nothing for gvisor")
		}
		return cr
	}

	t.Run("a failed sync keeps the entry of the file on disk", func(t *testing.T) {
		s := newTestSyncer(t, resolvedPool("pool", "gvisor-release"), multiVersionConfig())
		if err := s.syncPod(ctx, pod); err != nil {
			t.Fatalf("syncPod: %v", err)
		}
		before := advertised(t, s)
		if before.defaultHash != hashOf("v2") || !slices.Equal(before.hashes, []string{hashOf("v2"), hashOf("v1")}) {
			t.Fatalf("after a full sync pod-a advertises default %s, hashes %v; want v2 and [v2 v1]", before.defaultHash, before.hashes)
		}

		// The default's probe now fails permanently: nothing is written.
		s.mu.Lock()
		s.cpuFeatures = map[string]string{}
		s.mu.Unlock()
		s.runCPUFeatures = func(context.Context, string) ([]string, error) { return []string{"not_a_known_feature"}, nil }
		if err := s.syncPod(ctx, pod); !errors.Is(err, errPermanent) {
			t.Fatalf("syncPod with the default's probe failing = %v, want errPermanent", err)
		}
		if after := advertised(t, s); after.defaultHash != before.defaultHash || !slices.Equal(after.hashes, before.hashes) {
			t.Errorf("a sync that wrote nothing changed pod-a's entry: default %s, hashes %v", after.defaultHash, after.hashes)
		}
	})

	t.Run("a partial file advertises only its runtimes", func(t *testing.T) {
		s := newTestSyncer(t, resolvedPool("pool", "gvisor-release"), multiVersionConfig())
		s.assets = &fakeAssetFetcher{
			fn: func(_ context.Context, rec *sandboxAssetsRecord) (map[string]string, error) {
				if sha := rec.Assets[gvisorAssetName].SHA256; sha != "sha-v2" {
					return nil, fmt.Errorf("temporary download failure for %s", sha)
				}
				return map[string]string{gvisorAssetName: "sha-v2"}, nil
			},
		}
		if err := s.syncPod(ctx, pod); !errors.Is(err, errIncomplete) {
			t.Fatalf("syncPod with a restorable version failing = %v, want errIncomplete", err)
		}
		if cr := advertised(t, s); cr.defaultHash != hashOf("v2") || !slices.Equal(cr.hashes, []string{hashOf("v2")}) {
			t.Errorf("pod-a advertises default %s, hashes %v; want only the written v2", cr.defaultHash, cr.hashes)
		}
		if _, ok := s.index.lookup(hashOf("v1")); !ok {
			t.Error("the runtime left out of the partial file does not resolve; it must, before the retry writes it")
		}
	})
}
