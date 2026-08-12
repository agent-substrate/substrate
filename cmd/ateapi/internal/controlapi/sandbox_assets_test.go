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
	"testing"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

// listerFor builds listers over the given objects, using the same key
// functions the informers use.
func listersFor(t *testing.T, pools []*atev1alpha1.WorkerPool, configs []*atev1alpha1.SandboxConfig) (listersv1alpha1.WorkerPoolLister, listersv1alpha1.SandboxConfigLister) {
	t.Helper()
	poolIdx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	for _, p := range pools {
		if err := poolIdx.Add(p); err != nil {
			t.Fatalf("adding WorkerPool: %v", err)
		}
	}
	configIdx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, c := range configs {
		if err := configIdx.Add(c); err != nil {
			t.Fatalf("adding SandboxConfig: %v", err)
		}
	}
	return listersv1alpha1.NewWorkerPoolLister(poolIdx), listersv1alpha1.NewSandboxConfigLister(configIdx)
}

func testAssets() map[string]map[string]atev1alpha1.AssetFile {
	return map[string]map[string]atev1alpha1.AssetFile{
		"amd64": {"gvisor": {URL: "gs://bucket/gvisor.tar.bz2", SHA256: "abc"}},
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
	defaultConfig := &atev1alpha1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "gvisor-default"},
		Spec: atev1alpha1.SandboxConfigSpec{
			SandboxClass: atev1alpha1.SandboxClassGvisor,
			Default:      true,
			PauseImage:   defaultPause,
			Assets:       testAssets(),
		},
	}
	namedConfig := &atev1alpha1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "gvisor-custom"},
		Spec: atev1alpha1.SandboxConfigSpec{
			SandboxClass: atev1alpha1.SandboxClassGvisor,
			PauseImage:   namedPause,
			Assets:       testAssets(),
		},
	}

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
			pool := &atev1alpha1.WorkerPool{
				ObjectMeta: metav1.ObjectMeta{Name: "pool1", Namespace: "worker-ns"},
				Spec:       atev1alpha1.WorkerPoolSpec{SandboxConfigName: tt.configName},
			}
			poolLister, configLister := listersFor(t, []*atev1alpha1.WorkerPool{pool},
				[]*atev1alpha1.SandboxConfig{defaultConfig, namedConfig})

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
