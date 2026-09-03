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
	"time"

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
		ObjectMeta: metav1.ObjectMeta{
			Name:        "gvisor-default",
			Annotations: map[string]string{atev1alpha1.IsDefaultAnnotation: "true"},
		},
		Spec: atev1alpha1.SandboxConfigSpec{
			SandboxClass: atev1alpha1.SandboxClassGvisor,
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

// defaultAt builds a SandboxConfig with the given is-default annotation value
// and creation time.
func defaultAt(name string, class atev1alpha1.SandboxClass, annotation string, created time.Time) *atev1alpha1.SandboxConfig {
	return &atev1alpha1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Annotations:       map[string]string{atev1alpha1.IsDefaultAnnotation: annotation},
			CreationTimestamp: metav1.NewTime(created),
		},
		Spec: atev1alpha1.SandboxConfigSpec{
			SandboxClass: class,
			PauseImage:   "registry.k8s.io/pause@sha256:x",
			Assets:       testAssets(),
		},
	}
}

// TestDefaultSandboxConfig pins the StorageClass-style default semantics:
// only "true" counts, the newest default of the class wins, and none is an
// error.
func TestDefaultSandboxConfig(t *testing.T) {
	t0 := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		configs []*atev1alpha1.SandboxConfig
		want    string
		wantErr string
	}{{
		name:    "single default",
		configs: []*atev1alpha1.SandboxConfig{defaultAt("only", atev1alpha1.SandboxClassGvisor, "true", t0)},
		want:    "only",
	}, {
		name: "most recently created wins",
		configs: []*atev1alpha1.SandboxConfig{
			defaultAt("old", atev1alpha1.SandboxClassGvisor, "true", t0),
			defaultAt("new", atev1alpha1.SandboxClassGvisor, "true", t0.Add(time.Hour)),
		},
		want: "new",
	}, {
		name: "timestamp tie breaks by smaller name",
		configs: []*atev1alpha1.SandboxConfig{
			defaultAt("bbb", atev1alpha1.SandboxClassGvisor, "true", t0),
			defaultAt("aaa", atev1alpha1.SandboxClassGvisor, "true", t0),
		},
		want: "aaa",
	}, {
		name: "other classes' defaults are ignored",
		configs: []*atev1alpha1.SandboxConfig{
			defaultAt("gv", atev1alpha1.SandboxClassGvisor, "true", t0),
			defaultAt("mv", atev1alpha1.SandboxClassMicroVM, "true", t0.Add(time.Hour)),
		},
		want: "gv",
	}, {
		name: "only the exact value true counts",
		configs: []*atev1alpha1.SandboxConfig{
			defaultAt("upper", atev1alpha1.SandboxClassGvisor, "True", t0.Add(time.Hour)),
			defaultAt("one", atev1alpha1.SandboxClassGvisor, "1", t0.Add(2*time.Hour)),
			defaultAt("lower", atev1alpha1.SandboxClassGvisor, "true", t0),
		},
		want: "lower",
	}, {
		name: "no default is an error",
		configs: []*atev1alpha1.SandboxConfig{
			defaultAt("off", atev1alpha1.SandboxClassGvisor, "false", t0),
		},
		wantErr: "no default SandboxConfig",
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, configLister := listersFor(t, nil, tt.configs)
			got, err := defaultSandboxConfig(configLister, atev1alpha1.SandboxClassGvisor)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("defaultSandboxConfig() error = %v, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("defaultSandboxConfig() error: %v", err)
			}
			if got.Name != tt.want {
				t.Errorf("defaultSandboxConfig() = %q, want %q", got.Name, tt.want)
			}
		})
	}
}
