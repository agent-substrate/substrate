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

package controlapi

import (
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

var testSandboxAssets = map[string]map[string]atev1alpha1.AssetFile{
	"amd64": {
		"runsc":                    {URL: "https://assets/amd64/runsc", SHA256: "aaa"},
		"containerd-shim-runsc-v1": {URL: "https://assets/amd64/shim", SHA256: "bbb"},
	},
	"arm64": {
		"runsc": {URL: "https://assets/arm64/runsc", SHA256: "ccc"},
	},
}

func newSandboxConfig(name string, class atev1alpha1.SandboxClass, isDefault bool, assets map[string]map[string]atev1alpha1.AssetFile) *atev1alpha1.SandboxConfig {
	return &atev1alpha1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: atev1alpha1.SandboxConfigSpec{
			SandboxClass: class,
			Default:      isDefault,
			Assets:       assets,
		},
	}
}

func newSandboxConfigLister(t *testing.T, configs ...*atev1alpha1.SandboxConfig) listersv1alpha1.SandboxConfigLister {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, sc := range configs {
		if err := indexer.Add(sc); err != nil {
			t.Fatalf("add SandboxConfig %q to indexer: %v", sc.Name, err)
		}
	}
	return listersv1alpha1.NewSandboxConfigLister(indexer)
}

func newWorkerPoolLister(t *testing.T, pools ...*atev1alpha1.WorkerPool) listersv1alpha1.WorkerPoolLister {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, wp := range pools {
		if err := indexer.Add(wp); err != nil {
			t.Fatalf("add WorkerPool %s/%s to indexer: %v", wp.Namespace, wp.Name, err)
		}
	}
	return listersv1alpha1.NewWorkerPoolLister(indexer)
}

func newWorkerPool(namespace, name string, class atev1alpha1.SandboxClass, configName string) *atev1alpha1.WorkerPool {
	return &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: atev1alpha1.WorkerPoolSpec{
			SandboxClass:      class,
			SandboxConfigName: configName,
		},
	}
}

func wantAteletAssets(class string) *ateletpb.SandboxAssets {
	return &ateletpb.SandboxAssets{
		SandboxClass: class,
		Assets: map[string]*ateletpb.ArchAssets{
			"amd64": {Files: map[string]*ateletpb.AssetFile{
				"runsc":                    {Url: "https://assets/amd64/runsc", Sha256: "aaa"},
				"containerd-shim-runsc-v1": {Url: "https://assets/amd64/shim", Sha256: "bbb"},
			}},
			"arm64": {Files: map[string]*ateletpb.AssetFile{
				"runsc": {Url: "https://assets/arm64/runsc", Sha256: "ccc"},
			}},
		},
	}
}

func TestResolveSandboxAssets(t *testing.T) {
	tests := []struct {
		name    string
		pools   []*atev1alpha1.WorkerPool
		configs []*atev1alpha1.SandboxConfig
		want    *ateletpb.SandboxAssets
		wantErr string
	}{{
		name:    "named config matching class",
		pools:   []*atev1alpha1.WorkerPool{newWorkerPool("ns", "pool", atev1alpha1.SandboxClassMicroVM, "mv-cfg")},
		configs: []*atev1alpha1.SandboxConfig{newSandboxConfig("mv-cfg", atev1alpha1.SandboxClassMicroVM, false, testSandboxAssets)},
		want:    wantAteletAssets("microvm"),
	}, {
		name:    "empty pool class defaults to gvisor",
		pools:   []*atev1alpha1.WorkerPool{newWorkerPool("ns", "pool", "", "gv-cfg")},
		configs: []*atev1alpha1.SandboxConfig{newSandboxConfig("gv-cfg", atev1alpha1.SandboxClassGvisor, false, testSandboxAssets)},
		want:    wantAteletAssets("gvisor"),
	}, {
		name:  "unnamed config falls back to class default",
		pools: []*atev1alpha1.WorkerPool{newWorkerPool("ns", "pool", atev1alpha1.SandboxClassGvisor, "")},
		configs: []*atev1alpha1.SandboxConfig{
			newSandboxConfig("gv-default", atev1alpha1.SandboxClassGvisor, true, testSandboxAssets),
			newSandboxConfig("mv-default", atev1alpha1.SandboxClassMicroVM, true, nil),
		},
		want: wantAteletAssets("gvisor"),
	}, {
		name:    "worker pool not found",
		wantErr: "while getting WorkerPool ns/pool",
	}, {
		name:    "named config not found",
		pools:   []*atev1alpha1.WorkerPool{newWorkerPool("ns", "pool", "", "missing")},
		wantErr: `while getting SandboxConfig "missing"`,
	}, {
		name:    "named config class mismatch",
		pools:   []*atev1alpha1.WorkerPool{newWorkerPool("ns", "pool", atev1alpha1.SandboxClassGvisor, "mv-cfg")},
		configs: []*atev1alpha1.SandboxConfig{newSandboxConfig("mv-cfg", atev1alpha1.SandboxClassMicroVM, false, nil)},
		wantErr: `SandboxConfig "mv-cfg" has class "microvm" but WorkerPool ns/pool is class "gvisor"`,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveSandboxAssets(newWorkerPoolLister(t, tt.pools...), newSandboxConfigLister(t, tt.configs...), "ns", "pool")
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("resolveSandboxAssets error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveSandboxAssets: %v", err)
			}
			if diff := cmp.Diff(tt.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("resolveSandboxAssets mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestResolveSandboxAssetsCarriesPauseImage pins that the pause image travels
// with the sandbox binaries — it is resolved from the pool's SandboxConfig, not
// from the ActorTemplate — for both the named and the class-default config.
func TestResolveSandboxAssetsCarriesPauseImage(t *testing.T) {
	const (
		defaultPause = "registry.k8s.io/pause@sha256:default"
		namedPause   = "gcr.io/gke-release/pause@sha256:named"
	)
	defaultConfig := newSandboxConfig("gvisor-default", atev1alpha1.SandboxClassGvisor, true, testSandboxAssets)
	defaultConfig.Spec.PauseImage = defaultPause
	namedConfig := newSandboxConfig("gvisor-custom", atev1alpha1.SandboxClassGvisor, false, testSandboxAssets)
	namedConfig.Spec.PauseImage = namedPause

	tests := []struct {
		name           string
		configName     string
		wantPauseImage string
	}{
		{name: "class default", wantPauseImage: defaultPause},
		{name: "named config", configName: "gvisor-custom", wantPauseImage: namedPause},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			poolLister := newWorkerPoolLister(t, newWorkerPool("worker-ns", "pool1", atev1alpha1.SandboxClassGvisor, tt.configName))
			configLister := newSandboxConfigLister(t, defaultConfig, namedConfig)

			got, err := resolveSandboxAssets(poolLister, configLister, "worker-ns", "pool1")
			if err != nil {
				t.Fatalf("resolveSandboxAssets() error: %v", err)
			}
			if got.GetPauseImage() != tt.wantPauseImage {
				t.Errorf("pause image = %q, want %q", got.GetPauseImage(), tt.wantPauseImage)
			}
		})
	}
}

func TestDefaultSandboxConfig(t *testing.T) {
	tests := []struct {
		name     string
		configs  []*atev1alpha1.SandboxConfig
		class    atev1alpha1.SandboxClass
		wantName string
		wantErr  string
	}{{
		name: "single default for class wins over other classes and non-defaults",
		configs: []*atev1alpha1.SandboxConfig{
			newSandboxConfig("gv-default", atev1alpha1.SandboxClassGvisor, true, nil),
			newSandboxConfig("gv-extra", atev1alpha1.SandboxClassGvisor, false, nil),
			newSandboxConfig("mv-default", atev1alpha1.SandboxClassMicroVM, true, nil),
		},
		class:    atev1alpha1.SandboxClassGvisor,
		wantName: "gv-default",
	}, {
		name: "no default for class",
		configs: []*atev1alpha1.SandboxConfig{
			newSandboxConfig("gv-extra", atev1alpha1.SandboxClassGvisor, false, nil),
			newSandboxConfig("mv-default", atev1alpha1.SandboxClassMicroVM, true, nil),
		},
		class:   atev1alpha1.SandboxClassGvisor,
		wantErr: `no default SandboxConfig for class "gvisor"`,
	}, {
		name: "multiple defaults for class",
		configs: []*atev1alpha1.SandboxConfig{
			newSandboxConfig("gv-a", atev1alpha1.SandboxClassGvisor, true, nil),
			newSandboxConfig("gv-b", atev1alpha1.SandboxClassGvisor, true, nil),
		},
		class:   atev1alpha1.SandboxClassGvisor,
		wantErr: `multiple default SandboxConfigs for class "gvisor"`,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := defaultSandboxConfig(newSandboxConfigLister(t, tt.configs...), tt.class)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("defaultSandboxConfig error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("defaultSandboxConfig: %v", err)
			}
			if got.Name != tt.wantName {
				t.Errorf("defaultSandboxConfig = %q, want %q", got.Name, tt.wantName)
			}
		})
	}
}

func TestResolveTemplateVersionSandbox(t *testing.T) {
	wantGvisor := &ateapipb.SandboxAssets{
		SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR,
		Assets: map[string]*ateapipb.ArchAssets{
			"amd64": {Files: map[string]*ateapipb.AssetFile{
				"runsc":                    {Url: "https://assets/amd64/runsc", Sha256: "aaa"},
				"containerd-shim-runsc-v1": {Url: "https://assets/amd64/shim", Sha256: "bbb"},
			}},
			"arm64": {Files: map[string]*ateapipb.AssetFile{
				"runsc": {Url: "https://assets/arm64/runsc", Sha256: "ccc"},
			}},
		},
	}
	tests := []struct {
		name    string
		cfg     *ateapipb.SandboxConfig
		configs []*atev1alpha1.SandboxConfig
		want    *ateapipb.SandboxAssets
		wantErr string
	}{{
		name:    "gvisor config resolves and freezes assets",
		cfg:     &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR, ConfigName: "gv-cfg"},
		configs: []*atev1alpha1.SandboxConfig{newSandboxConfig("gv-cfg", atev1alpha1.SandboxClassGvisor, false, testSandboxAssets)},
		want:    wantGvisor,
	}, {
		name:    "unspecified class treated as gvisor",
		cfg:     &ateapipb.SandboxConfig{ConfigName: "gv-cfg"},
		configs: []*atev1alpha1.SandboxConfig{newSandboxConfig("gv-cfg", atev1alpha1.SandboxClassGvisor, false, testSandboxAssets)},
		want:    wantGvisor,
	}, {
		name:    "microvm config resolves with microvm class",
		cfg:     &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM, ConfigName: "mv-cfg"},
		configs: []*atev1alpha1.SandboxConfig{newSandboxConfig("mv-cfg", atev1alpha1.SandboxClassMicroVM, false, nil)},
		want:    &ateapipb.SandboxAssets{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM, Assets: map[string]*ateapipb.ArchAssets{}},
	}, {
		name:    "config not found",
		cfg:     &ateapipb.SandboxConfig{ConfigName: "missing"},
		wantErr: `while getting SandboxConfig "missing"`,
	}, {
		name:    "class mismatch",
		cfg:     &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM, ConfigName: "gv-cfg"},
		configs: []*atev1alpha1.SandboxConfig{newSandboxConfig("gv-cfg", atev1alpha1.SandboxClassGvisor, false, nil)},
		wantErr: `SandboxConfig "gv-cfg" has class "gvisor" but the version asks for class "microvm"`,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveTemplateVersionSandbox(newSandboxConfigLister(t, tt.configs...), tt.cfg)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("resolveTemplateVersionSandbox error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveTemplateVersionSandbox: %v", err)
			}
			if diff := cmp.Diff(tt.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("resolveTemplateVersionSandbox mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSandboxClassFromProto(t *testing.T) {
	tests := []struct {
		in   ateapipb.SandboxClass
		want atev1alpha1.SandboxClass
	}{
		{ateapipb.SandboxClass_SANDBOX_CLASS_UNSPECIFIED, atev1alpha1.SandboxClassGvisor},
		{ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR, atev1alpha1.SandboxClassGvisor},
		{ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM, atev1alpha1.SandboxClassMicroVM},
	}
	for _, tt := range tests {
		if got := sandboxClassFromProto(tt.in); got != tt.want {
			t.Errorf("sandboxClassFromProto(%v) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestSandboxAssetsAPIProto(t *testing.T) {
	sc := newSandboxConfig("gv-cfg", atev1alpha1.SandboxClassGvisor, false, testSandboxAssets)
	want := &ateapipb.SandboxAssets{
		SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR,
		Assets: map[string]*ateapipb.ArchAssets{
			"amd64": {Files: map[string]*ateapipb.AssetFile{
				"runsc":                    {Url: "https://assets/amd64/runsc", Sha256: "aaa"},
				"containerd-shim-runsc-v1": {Url: "https://assets/amd64/shim", Sha256: "bbb"},
			}},
			"arm64": {Files: map[string]*ateapipb.AssetFile{
				"runsc": {Url: "https://assets/arm64/runsc", Sha256: "ccc"},
			}},
		},
	}
	if diff := cmp.Diff(want, sandboxAssetsAPIProto(atev1alpha1.SandboxClassGvisor, sc), protocmp.Transform()); diff != "" {
		t.Errorf("sandboxAssetsAPIProto mismatch (-want +got):\n%s", diff)
	}

	got := sandboxAssetsAPIProto(atev1alpha1.SandboxClassMicroVM, newSandboxConfig("mv-cfg", atev1alpha1.SandboxClassMicroVM, false, nil))
	if got.SandboxClass != ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM {
		t.Errorf("SandboxClass = %v, want SANDBOX_CLASS_MICROVM", got.SandboxClass)
	}
	if got.Assets == nil || len(got.Assets) != 0 {
		t.Errorf("Assets = %v, want non-nil empty map", got.Assets)
	}
}

func TestSandboxAssetsProto(t *testing.T) {
	sc := newSandboxConfig("gv-cfg", atev1alpha1.SandboxClassGvisor, false, testSandboxAssets)
	if diff := cmp.Diff(wantAteletAssets("gvisor"), sandboxAssetsProto(atev1alpha1.SandboxClassGvisor, sc), protocmp.Transform()); diff != "" {
		t.Errorf("sandboxAssetsProto mismatch (-want +got):\n%s", diff)
	}

	got := sandboxAssetsProto(atev1alpha1.SandboxClassMicroVM, newSandboxConfig("mv-cfg", atev1alpha1.SandboxClassMicroVM, false, nil))
	if got.SandboxClass != "microvm" {
		t.Errorf("SandboxClass = %q, want microvm", got.SandboxClass)
	}
	if got.Assets == nil || len(got.Assets) != 0 {
		t.Errorf("Assets = %v, want non-nil empty map", got.Assets)
	}
}
