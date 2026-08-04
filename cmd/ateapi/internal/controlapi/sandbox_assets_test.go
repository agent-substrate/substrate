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

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
)

const (
	defaultRunscSHA = "f18a948bf9c8bbb54eb998549a3a8d719a1c7de2efbe8fdd2ff0ee5fecd06f19"
	pinnedRunscSHA  = "62eee121f8c188e347c428acc96f111568ede3be37b906046b6f28bbe2cc40c0"
)

func testWorkerPoolLister(t *testing.T, pools ...*atev1alpha1.WorkerPool) listersv1alpha1.WorkerPoolLister {
	t.Helper()
	idx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	for _, p := range pools {
		if err := idx.Add(p); err != nil {
			t.Fatalf("adding WorkerPool %q to indexer: %v", p.Name, err)
		}
	}
	return listersv1alpha1.NewWorkerPoolLister(idx)
}

func testSandboxConfigLister(t *testing.T, configs ...*atev1alpha1.SandboxConfig) listersv1alpha1.SandboxConfigLister {
	t.Helper()
	idx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	for _, c := range configs {
		if err := idx.Add(c); err != nil {
			t.Fatalf("adding SandboxConfig %q to indexer: %v", c.Name, err)
		}
	}
	return listersv1alpha1.NewSandboxConfigLister(idx)
}

func testWorkerPool(name, configName string, class atev1alpha1.SandboxClass) *atev1alpha1.WorkerPool {
	return &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ate-demo"},
		Spec: atev1alpha1.WorkerPoolSpec{
			SandboxClass:      class,
			SandboxConfigName: configName,
		},
	}
}

func testGvisorConfig(name, sha string, isDefault bool) *atev1alpha1.SandboxConfig {
	return &atev1alpha1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: atev1alpha1.SandboxConfigSpec{
			SandboxClass: atev1alpha1.SandboxClassGvisor,
			Default:      isDefault,
			Assets: map[string]map[string]atev1alpha1.AssetFile{
				"amd64": {"runsc": {URL: "gs://bucket/" + name + "/runsc", SHA256: sha}},
			},
		},
	}
}

// TestResolveSandboxAssetsNamedConfig covers the spec.sandboxConfigName branch:
// a pool naming a SandboxConfig gets that config's binaries, not the cluster
// default's. This is how a pool opts into a non-default runsc (see
// hack/gvisor-hack/), so a regression here would silently boot actors on the
// default binary instead of the pinned one — the assets still resolve, so
// nothing errors; the pool just runs the wrong sandbox.
func TestResolveSandboxAssetsNamedConfig(t *testing.T) {
	wpLister := testWorkerPoolLister(t, testWorkerPool("pinned-pool", "pinned", atev1alpha1.SandboxClassGvisor))
	scLister := testSandboxConfigLister(t,
		testGvisorConfig("gvisor-default", defaultRunscSHA, true),
		testGvisorConfig("pinned", pinnedRunscSHA, false),
	)

	got, err := resolveSandboxAssets(wpLister, scLister, "ate-demo", "pinned-pool")
	if err != nil {
		t.Fatalf("resolveSandboxAssets() error = %v", err)
	}

	want := &ateletpb.SandboxAssets{
		SandboxClass: string(atev1alpha1.SandboxClassGvisor),
		Assets: map[string]*ateletpb.ArchAssets{
			"amd64": {Files: map[string]*ateletpb.AssetFile{
				"runsc": {Url: "gs://bucket/pinned/runsc", Sha256: pinnedRunscSHA},
			}},
		},
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("resolveSandboxAssets() mismatch (-want +got):\n%s", diff)
	}
}

// TestResolveSandboxAssetsDefaultConfig covers the no-name branch: an empty
// sandboxConfigName falls back to the single default for the pool's class.
func TestResolveSandboxAssetsDefaultConfig(t *testing.T) {
	wpLister := testWorkerPoolLister(t, testWorkerPool("plain-pool", "", ""))
	scLister := testSandboxConfigLister(t,
		testGvisorConfig("gvisor-default", defaultRunscSHA, true),
		testGvisorConfig("pinned", pinnedRunscSHA, false),
	)

	got, err := resolveSandboxAssets(wpLister, scLister, "ate-demo", "plain-pool")
	if err != nil {
		t.Fatalf("resolveSandboxAssets() error = %v", err)
	}
	// An empty spec.sandboxClass defaults to gvisor.
	if got.GetSandboxClass() != string(atev1alpha1.SandboxClassGvisor) {
		t.Errorf("SandboxClass = %q, want %q", got.GetSandboxClass(), atev1alpha1.SandboxClassGvisor)
	}
	if sha := got.GetAssets()["amd64"].GetFiles()["runsc"].GetSha256(); sha != defaultRunscSHA {
		t.Errorf("runsc sha256 = %q, want the default config's %q", sha, defaultRunscSHA)
	}
}

func TestResolveSandboxAssetsErrors(t *testing.T) {
	microvmDefault := &atev1alpha1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "microvm-config"},
		Spec: atev1alpha1.SandboxConfigSpec{
			SandboxClass: atev1alpha1.SandboxClassMicroVM,
			Assets:       map[string]map[string]atev1alpha1.AssetFile{"amd64": {"cloud-hypervisor": {URL: "gs://bucket/ch", SHA256: defaultRunscSHA}}},
		},
	}

	tests := []struct {
		name    string
		pool    *atev1alpha1.WorkerPool
		configs []*atev1alpha1.SandboxConfig
		wantErr string
	}{{
		// The class gate: naming a micro-VM config from a gVisor pool must fail
		// rather than hand atelet an asset set the gVisor backend cannot use.
		name:    "named config has the wrong class",
		pool:    testWorkerPool("pool", "microvm-config", atev1alpha1.SandboxClassGvisor),
		configs: []*atev1alpha1.SandboxConfig{microvmDefault},
		wantErr: "is class",
	}, {
		name:    "named config does not exist",
		pool:    testWorkerPool("pool", "absent", atev1alpha1.SandboxClassGvisor),
		configs: []*atev1alpha1.SandboxConfig{testGvisorConfig("gvisor-default", defaultRunscSHA, true)},
		wantErr: "while getting SandboxConfig",
	}, {
		// Why hack/gvisor-hack/deploy.sh demotes the incumbent default before
		// applying a new one with spec.default: true.
		name: "two defaults for one class",
		pool: testWorkerPool("pool", "", atev1alpha1.SandboxClassGvisor),
		configs: []*atev1alpha1.SandboxConfig{
			testGvisorConfig("gvisor-default", defaultRunscSHA, true),
			testGvisorConfig("gvisor-hack", pinnedRunscSHA, true),
		},
		wantErr: "multiple default SandboxConfigs",
	}, {
		name:    "no default for the class",
		pool:    testWorkerPool("pool", "", atev1alpha1.SandboxClassGvisor),
		configs: []*atev1alpha1.SandboxConfig{testGvisorConfig("not-default", pinnedRunscSHA, false)},
		wantErr: "no default SandboxConfig",
	}, {
		name:    "worker pool does not exist",
		pool:    testWorkerPool("other-pool", "", atev1alpha1.SandboxClassGvisor),
		configs: []*atev1alpha1.SandboxConfig{testGvisorConfig("gvisor-default", defaultRunscSHA, true)},
		wantErr: "while getting WorkerPool",
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wpLister := testWorkerPoolLister(t, tt.pool)
			scLister := testSandboxConfigLister(t, tt.configs...)

			_, err := resolveSandboxAssets(wpLister, scLister, "ate-demo", "pool")
			if err == nil {
				t.Fatalf("resolveSandboxAssets() succeeded, want error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("resolveSandboxAssets() error = %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}
