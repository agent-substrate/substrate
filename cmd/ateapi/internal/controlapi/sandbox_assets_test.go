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
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// TestResolveSandboxAssetsCarriesConfigRef pins that the resolved assets name
// the SandboxConfig object they came from (name + UID + resourceVersion).
func TestResolveSandboxAssetsCarriesConfigRef(t *testing.T) {
	config := &atev1alpha1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "gvisor-prod", UID: "sandbox-uid-1", ResourceVersion: "42"},
		Spec: atev1alpha1.SandboxConfigSpec{
			SandboxClass: atev1alpha1.SandboxClassGvisor,
			Default:      true,
			PauseImage:   "registry.k8s.io/pause@sha256:abc",
			Assets:       testAssets(),
		},
	}
	pool := &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool1", Namespace: "worker-ns"},
	}
	poolLister, configLister := listersFor(t, []*atev1alpha1.WorkerPool{pool}, []*atev1alpha1.SandboxConfig{config})

	got, err := resolveSandboxAssets(poolLister, configLister, "worker-ns", "pool1")
	if err != nil {
		t.Fatalf("resolveSandboxAssets() error: %v", err)
	}
	ref := got.GetSandboxConfigRef()
	if ref.GetName() != "gvisor-prod" || ref.GetUid() != "sandbox-uid-1" || ref.GetResourceVersion() != "42" {
		t.Errorf("SandboxConfig ref = %s/%s@%s, want gvisor-prod/sandbox-uid-1@42", ref.GetName(), ref.GetUid(), ref.GetResourceVersion())
	}
}

// TestResolveSandboxAssetsByRef pins that the named SandboxConfig is used
// only while its UID still matches; a missing or re-created object is a
// FailedPrecondition.
func TestResolveSandboxAssetsByRef(t *testing.T) {
	config := &atev1alpha1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "gvisor-prod", UID: "sandbox-uid-1"},
		Spec: atev1alpha1.SandboxConfigSpec{
			SandboxClass: atev1alpha1.SandboxClassGvisor,
			PauseImage:   "registry.k8s.io/pause@sha256:abc",
			Assets:       testAssets(),
		},
	}
	_, configLister := listersFor(t, nil, []*atev1alpha1.SandboxConfig{config})

	tests := []struct {
		name     string
		ref      *ateapipb.SandboxConfigRef
		wantCode codes.Code
	}{
		{name: "match", ref: &ateapipb.SandboxConfigRef{Name: "gvisor-prod", Uid: "sandbox-uid-1"}, wantCode: codes.OK},
		{name: "object gone", ref: &ateapipb.SandboxConfigRef{Name: "gvisor-gone", Uid: "sandbox-uid-1"}, wantCode: codes.FailedPrecondition},
		{name: "uid mismatch (object recreated under the same name)", ref: &ateapipb.SandboxConfigRef{Name: "gvisor-prod", Uid: "sandbox-uid-0"}, wantCode: codes.FailedPrecondition},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveSandboxAssetsByRef(configLister, tt.ref)
			if code := status.Code(err); code != tt.wantCode {
				t.Fatalf("status.Code = %v (err %v), want %v", code, err, tt.wantCode)
			}
			if tt.wantCode != codes.OK {
				return
			}
			if got.GetSandboxConfigRef().GetName() != "gvisor-prod" || got.GetSandboxConfigRef().GetUid() != "sandbox-uid-1" {
				t.Errorf("SandboxConfig ref = %s/%s, want gvisor-prod/sandbox-uid-1", got.GetSandboxConfigRef().GetName(), got.GetSandboxConfigRef().GetUid())
			}
		})
	}
}
