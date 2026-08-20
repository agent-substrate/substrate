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
	"context"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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

// Mirrors manifests/ate-install/sandboxconfig-gvisor.yaml.
const (
	testGvisorURL    = "gs://gvisor/releases/release/20260803/x86_64/gvisor.tar.bz2"
	testGvisorSHA256 = "9e7a5fcc2cbd28c9cd4af910a9327abcf07a8efcce242c285b860d79010c2db5"
)

func testAssets() map[string]map[string]atev1alpha1.AssetFile {
	return map[string]map[string]atev1alpha1.AssetFile{
		"amd64": {"gvisor": {URL: testGvisorURL, SHA256: testGvisorSHA256}},
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

			got, err := resolveSandboxAssetsByWorkerPool(poolLister, configLister, "worker-ns", "pool1")
			if err != nil {
				t.Fatalf("resolveSandboxAssets() error: %v", err)
			}
			if got.GetPauseImage() != tt.wantPauseImage {
				t.Errorf("pause image = %q, want %q", got.GetPauseImage(), tt.wantPauseImage)
			}
		})
	}
}

// TestResolveBootSandboxAssets pins how a cold boot picks its sandbox assets:
// an actor bound to a stored ActorTemplate boots with the assets frozen on
// the template's status — never touching the pool's SandboxConfig (the
// listers stay nil, so that path would panic) — and fails FailedPrecondition
// on a dangling reference or unfrozen assets; an actor without the reference
// keeps resolving from the pool.
func TestResolveBootSandboxAssets(t *testing.T) {
	// Mirrors manifests/microvm/sandboxconfig-microvm.yaml.tmpl (amd64).
	const (
		frozenPause     = "registry.k8s.io/pause:3.10.2@sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4"
		kataKernelURL   = "gs://ate-cluster-assets/kata-assets/vmlinux"
		kataKernelSHA   = "6abc48fa83c58e3db314037105d04ec00c1bc80d85eb2dfb2e2854f414573bca"
		cloudHypervisor = "gs://ate-cluster-assets/kata-assets/cloud-hypervisor"
		cloudHypSHA     = "448af3d4e59b22c2987f7df94c213ad40fb53a10d437e42b5ee6c4fce7c29ecc"
		// The gvisor default's in-project mirror, per
		// manifests/ate-install/sandboxconfig-gvisor.yaml.
		poolPause = "gcr.io/gke-release/pause@sha256:bcbd57ba5653580ec647b16d8163cdd1112df3609129b01f912a8032e48265da"
	)
	frozen := &ateapipb.SandboxAssets{
		SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM,
		PauseImage:   frozenPause,
		Assets: map[string]*ateapipb.ArchAssets{
			"amd64": {Files: map[string]*ateapipb.AssetFile{
				"cloud-hypervisor": {Url: cloudHypervisor, Sha256: cloudHypSHA},
				"kata-kernel":      {Url: kataKernelURL, Sha256: kataKernelSHA},
			}},
		},
	}

	tests := []struct {
		name string
		// template is seeded into the store before the call; nil seeds none.
		template *ateapipb.ActorTemplate
		// actorTemplateRef is the actor's stored-template reference; nil
		// exercises the CRD-actor fallback.
		actorTemplateRef *ateapipb.ObjectRef
		// withPoolListers wires pool/config listers serving poolPause.
		withPoolListers bool
		wantCode        codes.Code
		want            *ateletpb.SandboxAssets
	}{
		{
			name: "frozen template assets win",
			template: &ateapipb.ActorTemplate{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "ate-1", Name: "tmpl-1"},
				Status:   &ateapipb.ActorTemplateStatus{SandboxAssets: frozen},
			},
			actorTemplateRef: &ateapipb.ObjectRef{Atespace: "ate-1", Name: "tmpl-1"},
			want: &ateletpb.SandboxAssets{
				SandboxClass: string(atev1alpha1.SandboxClassMicroVM),
				PauseImage:   frozenPause,
				Assets: map[string]*ateletpb.ArchAssets{
					"amd64": {Files: map[string]*ateletpb.AssetFile{
						"cloud-hypervisor": {Url: cloudHypervisor, Sha256: cloudHypSHA},
						"kata-kernel":      {Url: kataKernelURL, Sha256: kataKernelSHA},
					}},
				},
			},
		},
		{
			name:             "template not found",
			actorTemplateRef: &ateapipb.ObjectRef{Atespace: "ate-1", Name: "missing"},
			wantCode:         codes.FailedPrecondition,
		},
		{
			name: "assets not frozen",
			template: &ateapipb.ActorTemplate{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "ate-1", Name: "no-assets"},
			},
			actorTemplateRef: &ateapipb.ObjectRef{Atespace: "ate-1", Name: "no-assets"},
			wantCode:         codes.FailedPrecondition,
		},
		{
			name:            "no template ref falls back to pool",
			withPoolListers: true,
			want: &ateletpb.SandboxAssets{
				SandboxClass: string(atev1alpha1.SandboxClassGvisor),
				PauseImage:   poolPause,
				Assets: map[string]*ateletpb.ArchAssets{
					"amd64": {Files: map[string]*ateletpb.AssetFile{
						"gvisor": {Url: testGvisorURL, Sha256: testGvisorSHA256},
					}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()

			if tt.template != nil {
				if _, err := st.CreateActorTemplate(ctx, tt.template); err != nil {
					t.Fatalf("seed ActorTemplate: %v", err)
				}
			}
			var poolLister listersv1alpha1.WorkerPoolLister
			var configLister listersv1alpha1.SandboxConfigLister
			if tt.withPoolListers {
				pool := &atev1alpha1.WorkerPool{
					ObjectMeta: metav1.ObjectMeta{Name: "pool1", Namespace: "worker-ns"},
				}
				config := &atev1alpha1.SandboxConfig{
					ObjectMeta: metav1.ObjectMeta{Name: "gvisor-default"},
					Spec: atev1alpha1.SandboxConfigSpec{
						SandboxClass: atev1alpha1.SandboxClassGvisor,
						Default:      true,
						PauseImage:   poolPause,
						Assets:       testAssets(),
					},
				}
				poolLister, configLister = listersFor(t, []*atev1alpha1.WorkerPool{pool}, []*atev1alpha1.SandboxConfig{config})
			}

			w := NewActorWorkflow(st, nil, nil, nil, poolLister, configLister, nil, nil, "", nil)
			actor := &ateapipb.Actor{ActorTemplate: tt.actorTemplateRef}
			got, err := w.resolveSandboxAssets(ctx, actor, "worker-ns", "pool1")
			if tt.wantCode != codes.OK {
				if code := status.Code(err); code != tt.wantCode {
					t.Fatalf("status.Code = %v (err %v), want %v", code, err, tt.wantCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveBootSandboxAssets() error: %v", err)
			}
			if !proto.Equal(got, tt.want) {
				t.Errorf("resolveBootSandboxAssets() = %v, want %v", got, tt.want)
			}
		})
	}
}
