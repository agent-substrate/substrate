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

package steps

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	k8stesting "k8s.io/client-go/testing"

	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
)

// Single-component redeploys apply ate-otel-config on its own. It must be the
// ConfigMap the full install renders, so the kind overlay's endpoint (or one a
// provider's component sets) survives, and nothing else from that render: the
// components render adds, such as the fake data plane, add workloads.
func TestApplyOtelConfigAppliesOnlyTheRenderedConfigMap(t *testing.T) {
	root := repoRoot(t)
	for _, tc := range []struct {
		name         string
		cfg          config.Config
		wantEndpoint string
	}{
		{name: "base", cfg: config.Config{Router: config.RouterEnvoy}},
		{name: "agentgateway", cfg: config.Config{Router: config.RouterAgentgateway}},
		{
			name:         "kind",
			cfg:          config.Config{Router: config.RouterEnvoy, Kind: true},
			wantEndpoint: "http://opentelemetry-collector.otel-system.svc:4317",
		},
		{
			name:         "kind agentgateway",
			cfg:          config.Config{Router: config.RouterAgentgateway, Kind: true},
			wantEndpoint: "http://opentelemetry-collector.otel-system.svc:4317",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.cfg.Root = root
			tc.cfg.FakeDataPlane = true
			tc.cfg.E2ETuning = true
			tc.cfg.CordonSystemInfraComponents = true
			k, dc := fakeApplyKube(t)
			e := &Env{Cfg: &tc.cfg, Kube: k}
			if err := e.applyOtelConfig(t.Context()); err != nil {
				t.Fatalf("applyOtelConfig() error = %v", err)
			}

			var applied []map[string]any
			for _, a := range dc.Actions() {
				p, ok := a.(k8stesting.PatchAction)
				if !ok || p.GetPatchType() != types.ApplyPatchType {
					continue
				}
				var obj map[string]any
				if err := json.Unmarshal(p.GetPatch(), &obj); err != nil {
					t.Fatalf("decoding the applied object: %v", err)
				}
				applied = append(applied, obj)
			}
			if len(applied) != 1 {
				t.Fatalf("applied %d objects, want only ConfigMap %s", len(applied), otelConfigMap)
			}
			obj := applied[0]
			meta, _ := obj["metadata"].(map[string]any)
			if obj["kind"] != "ConfigMap" || meta["name"] != otelConfigMap || meta["namespace"] != NamespaceAteSystem {
				t.Fatalf("applied %v %v, want ConfigMap %s/%s", obj["kind"], meta, NamespaceAteSystem, otelConfigMap)
			}
			data, _ := obj["data"].(map[string]any)
			got, _ := data[otelEndpointKey].(string)
			if got != tc.wantEndpoint {
				t.Errorf("%s = %q, want %q", otelEndpointKey, got, tc.wantEndpoint)
			}
		})
	}
}

// A render without ate-otel-config must fail by name rather than apply
// nothing, which would leave every component's envFrom pointing at a stale or
// missing ConfigMap.
func TestApplyOtelConfigFailsWithoutTheConfigMap(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, installDir, "base")
	if err := os.MkdirAll(base, 0o755); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		"kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n- other.yaml\n",
		"other.yaml":         "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: other\n  namespace: ate-system\n",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(base, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	k, dc := fakeApplyKube(t)
	e := &Env{Cfg: &config.Config{Root: root, Router: config.RouterEnvoy}, Kube: k}
	err := e.applyOtelConfig(t.Context())
	if err == nil || !strings.Contains(err.Error(), NamespaceAteSystem+"/"+otelConfigMap) {
		t.Fatalf("applyOtelConfig() error = %v, want it to name %s/%s", err, NamespaceAteSystem, otelConfigMap)
	}
	if n := len(dc.Actions()); n != 0 {
		t.Errorf("applyOtelConfig() sent %d requests after failing, want none", n)
	}
}

// fakeApplyKube is a kube.Client whose dynamic client records applies, with
// discovery for ConfigMaps only, so applying any other kind fails.
func fakeApplyKube(t *testing.T) (*kube.Client, *dynamicfake.FakeDynamicClient) {
	t.Helper()
	cs := fake.NewClientset()
	cs.Fake.Resources = []*metav1.APIResourceList{
		{GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: "configmaps", Kind: "ConfigMap", Namespaced: true}}},
	}
	dc := dynamicfake.NewSimpleDynamicClient(clientgoscheme.Scheme)
	// The fake's object tracker cannot serve a server-side apply, so accept
	// it here; the action is still recorded for the test to inspect.
	dc.PrependReactor("patch", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap"}}, nil
	})
	return kube.NewFromClients(nil, cs, dc, cs.Discovery()), dc
}
