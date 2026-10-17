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

	atev1alpha1 "go.ate.dev/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "go.ate.dev/substrate/pkg/client/listers/api/v1alpha1"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"
)

// sandboxConfigListerFor builds a lister over the given SandboxConfigs, using
// the same key function the informers use.
func sandboxConfigListerFor(t *testing.T, configs []*atev1alpha1.SandboxConfig) listersv1alpha1.SandboxConfigLister {
	t.Helper()
	configIdx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, c := range configs {
		if err := configIdx.Add(c); err != nil {
			t.Fatalf("adding SandboxConfig: %v", err)
		}
	}
	return listersv1alpha1.NewSandboxConfigLister(configIdx)
}

// workerPoolListerFor builds a lister over the given WorkerPools, using the
// same key function the informers use.
func workerPoolListerFor(t *testing.T, pools []*atev1alpha1.WorkerPool) listersv1alpha1.WorkerPoolLister {
	t.Helper()
	poolIdx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, p := range pools {
		if err := poolIdx.Add(p); err != nil {
			t.Fatalf("adding WorkerPool: %v", err)
		}
	}
	return listersv1alpha1.NewWorkerPoolLister(poolIdx)
}

// resolvedWorkerPool is a gVisor WorkerPool whose controller-confirmed
// SandboxConfig is configName; "" is a pool that has not resolved one yet.
func resolvedWorkerPool(namespace, name, configName string) *atev1alpha1.WorkerPool {
	pool := &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: atev1alpha1.WorkerPoolSpec{
			SandboxClasses: []atev1alpha1.WorkerPoolSandboxClass{{Name: atev1alpha1.SandboxClassGvisor}},
		},
	}
	status := atev1alpha1.WorkerPoolSandboxClassStatus{Name: atev1alpha1.SandboxClassGvisor}
	if configName != "" {
		status.ConfigRef = &atev1alpha1.SandboxConfigReference{Name: configName}
	}
	pool.Status.SandboxClasses = []atev1alpha1.WorkerPoolSandboxClassStatus{status}
	return pool
}

func testAssets() map[string]map[string]atev1alpha1.AssetFile {
	return map[string]map[string]atev1alpha1.AssetFile{
		"amd64": {"gvisor": {URL: "gs://bucket/gvisor.tar.bz2", SHA256: "abc"}},
	}
}

// TestResolveSandboxAssets pins the pool-side resolution: the SandboxConfig
// the assigned worker's pool resolved is taken at its default version, a
// missing default version, an unresolved pool, a missing pool or config is an
// error, and the pause image travels with the sandbox binaries.
func TestResolveSandboxAssets(t *testing.T) {
	const namedPause = "gcr.io/gke-release/pause@sha256:named"
	namedConfig := &atev1alpha1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "gvisor-custom"},
		Spec: atev1alpha1.SandboxConfigSpec{
			SandboxClass:   atev1alpha1.SandboxClassGvisor,
			DefaultVersion: "v2",
			Versions: []atev1alpha1.SandboxVersionConfig{{
				Name:       "v1",
				PauseImage: "gcr.io/gke-release/pause@sha256:old",
				Assets:     testAssets(),
			}, {
				Name:       "v2",
				PauseImage: namedPause,
				Assets:     testAssets(),
			}},
		},
	}
	// A config whose defaultVersion names no entry; the CRD rejects this, but
	// an object stored before the versions schema reads back this way.
	noDefaultConfig := &atev1alpha1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "gvisor-no-default"},
		Spec:       atev1alpha1.SandboxConfigSpec{SandboxClass: atev1alpha1.SandboxClassGvisor},
	}
	// A config whose defaultVersion names a Disabled entry; the CRD rejects
	// this too, and it must not resolve to the disabled version.
	disabledDefaultConfig := &atev1alpha1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "gvisor-disabled-default"},
		Spec: atev1alpha1.SandboxConfigSpec{
			SandboxClass:   atev1alpha1.SandboxClassGvisor,
			DefaultVersion: "v1",
			Versions: []atev1alpha1.SandboxVersionConfig{{
				Name:       "v1",
				State:      ptr.To(atev1alpha1.SandboxVersionStateDisabled),
				PauseImage: namedPause,
				Assets:     testAssets(),
			}},
		},
	}
	pools := []*atev1alpha1.WorkerPool{
		resolvedWorkerPool("worker-ns", "custom", "gvisor-custom"),
		resolvedWorkerPool("worker-ns", "no-default", "gvisor-no-default"),
		resolvedWorkerPool("worker-ns", "disabled-default", "gvisor-disabled-default"),
		resolvedWorkerPool("worker-ns", "missing-config", "does-not-exist"),
		resolvedWorkerPool("worker-ns", "unresolved", ""),
	}
	assignment := func(pool string) *ateapipb.WorkerAssignment {
		return &ateapipb.WorkerAssignment{WorkerNamespace: "worker-ns", WorkerPool: pool}
	}

	tests := []struct {
		name           string
		assignment     *ateapipb.WorkerAssignment
		wantPauseImage string
		wantErr        string
	}{{
		name:           "pool with a resolved config",
		assignment:     assignment("custom"),
		wantPauseImage: namedPause,
	}, {
		name:       "config without its default version",
		assignment: assignment("no-default"),
		wantErr:    `SandboxConfig "gvisor-no-default" has no enabled version ""`,
	}, {
		name:       "config whose default version is disabled",
		assignment: assignment("disabled-default"),
		wantErr:    `SandboxConfig "gvisor-disabled-default" has no enabled version "v1"`,
	}, {
		name:       "pool whose config is missing",
		assignment: assignment("missing-config"),
		wantErr:    `SandboxConfig "does-not-exist" of WorkerPool worker-ns/missing-config not found`,
	}, {
		name:       "pool that has not resolved a config",
		assignment: assignment("unresolved"),
		wantErr:    `WorkerPool worker-ns/unresolved has not resolved a SandboxConfig`,
	}, {
		name:       "missing pool",
		assignment: assignment("does-not-exist"),
		wantErr:    `WorkerPool worker-ns/does-not-exist not found`,
	}, {
		name:       "assignment without a pool",
		assignment: &ateapipb.WorkerAssignment{},
		wantErr:    "names no worker pool",
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			poolLister := workerPoolListerFor(t, pools)
			configLister := sandboxConfigListerFor(t, []*atev1alpha1.SandboxConfig{namedConfig, noDefaultConfig, disabledDefaultConfig})

			got, err := resolveSandboxAssets(poolLister, configLister, tt.assignment)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("resolveSandboxAssets() error = %v, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveSandboxAssets() error: %v", err)
			}
			if got.GetPauseImage() != tt.wantPauseImage {
				t.Errorf("pause image = %q, want %q", got.GetPauseImage(), tt.wantPauseImage)
			}
		})
	}
}
