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
	"runtime"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/agent-substrate/substrate/internal/hardware"
	"github.com/agent-substrate/substrate/internal/sandboxversions"
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

// pauseDigest is the digest of the pause image of multiVersionConfig's
// version with the given name.
func pauseDigest(name string) string {
	return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte("pause-"+name)))
}

// multiVersionConfig is a gVisor config whose default version "v2" is listed
// after an Enabled "v1", a Disabled "v0", and an Enabled "other-arch" with no
// assets for this node.
func multiVersionConfig() *v1alpha1.SandboxConfig {
	version := func(name, sha string, state *v1alpha1.SandboxVersionState) v1alpha1.SandboxVersionConfig {
		return v1alpha1.SandboxVersionConfig{
			Name:       name,
			State:      state,
			PauseImage: "registry.k8s.io/pause@" + pauseDigest(name),
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
			s := newTestSyncer(t, tt.objs...)
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

func TestVersionRuntimeHash(t *testing.T) {
	gvisor := map[string]assetEntry{gvisorAssetName: {URL: "gs://bucket/gvisor", SHA256: "d547d81401461fd1c679c5c4fa0a6c2b8ef7dc3c22ce23c9e25dcc4c69cfd06f"}}
	pause := "sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4"

	tests := []struct {
		name       string
		pauseImage string
		want       string
		wantErr    bool
	}{{
		name:       "pause image pinned by digest",
		pauseImage: "registry.k8s.io/pause:3.10.2@" + pause,
		want:       runtimeHash(&sandboxAssetsRecord{Assets: gvisor}, pause),
	}, {
		name:       "pause image pinned by digest only",
		pauseImage: "gcr.io/gke-release/pause@" + pause,
		want:       runtimeHash(&sandboxAssetsRecord{Assets: gvisor}, pause),
	}, {
		name: "no pause image",
		want: runtimeHash(&sandboxAssetsRecord{Assets: gvisor}, ""),
	}, {
		// The CRD rejects this shape.
		name:       "pause image by tag",
		pauseImage: "registry.k8s.io/pause:3.10.2",
		wantErr:    true,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := versionRuntimeHash(&sandboxAssetsRecord{SandboxClass: "gvisor", PauseImage: tt.pauseImage, Assets: gvisor})
			if (err != nil) != tt.wantErr {
				t.Fatalf("versionRuntimeHash() error = %v, want error %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("versionRuntimeHash() = %q, want %q", got, tt.want)
			}
		})
	}
}

// wantRuntime is the runtime buildFile builds from multiVersionConfig's
// version with the given name.
func wantRuntime(name string) sandboxversions.Attributes {
	rec := &sandboxAssetsRecord{Assets: map[string]assetEntry{gvisorAssetName: {SHA256: "sha-" + name}}}
	return sandboxversions.Attributes{hardware.AttrSandboxRuntimeHash: runtimeHash(rec, pauseDigest(name))}
}

// wantVersions is the file content for multiVersionConfig.
func wantVersions() *sandboxversions.File {
	return &sandboxversions.File{
		DefaultRuntime:     wantRuntime("v2"),
		RestorableRuntimes: []sandboxversions.Attributes{wantRuntime("v1")},
	}
}

// planAndBuild runs the plan and build steps of syncPod for pod.
func planAndBuild(s *workerSandboxSyncer, pod *corev1.Pod) (*sandboxversions.File, error) {
	plan, err := s.resolver.plan(pod)
	if err != nil {
		return nil, err
	}
	return buildFile(context.Background(), plan)
}

func TestPlanAndBuild(t *testing.T) {
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

	// The CRD rejects a pause image not pinned by digest.
	tagged := func(i int) *v1alpha1.SandboxConfig {
		cfg := multiVersionConfig()
		cfg.Spec.Versions[i].PauseImage = "registry.k8s.io/pause:3.10.2"
		return cfg
	}

	tests := []struct {
		name          string
		cfg           *v1alpha1.SandboxConfig
		want          *sandboxversions.File
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
		// No worker of the pool could start actors on this node.
		name:          "default version pause image not pinned by digest",
		cfg:           tagged(3),
		wantErr:       true,
		wantPermanent: true,
	}, {
		name: "non-default version pause image not pinned by digest is left out",
		cfg:  tagged(1),
		want: onlyDefault,
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
