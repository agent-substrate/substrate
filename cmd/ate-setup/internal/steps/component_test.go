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
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
)

// A component redeploy on kind must carry the overlay's patch for it (the
// rustfs settings on the object-store plugin) and nothing from the other
// components.
func TestRenderComponentCarriesOverlayPatches(t *testing.T) {
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatalf("resolving repo root: %v", err)
	}
	env := &Env{Cfg: &config.Config{Root: root, Kind: true}}
	manifest, err := env.renderComponent("ate-api-server.yaml")
	if err != nil {
		t.Fatalf("renderComponent: %v", err)
	}
	objs, err := kube.DecodeManifestBytes(manifest)
	if err != nil {
		t.Fatalf("decoding the render: %v", err)
	}

	var dep *appsv1.Deployment
	for _, obj := range objs {
		switch obj.GetKind() {
		case "DaemonSet", "ConfigMap", "StatefulSet":
			t.Errorf("render carries %s %s, which is not part of ate-api-server.yaml", obj.GetKind(), obj.GetName())
		case "Deployment":
			if obj.GetName() != "ate-api-server" {
				t.Errorf("render carries Deployment %s", obj.GetName())
				continue
			}
			dep = &appsv1.Deployment{}
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, dep); err != nil {
				t.Fatalf("converting the Deployment: %v", err)
			}
		}
	}
	if dep == nil {
		t.Fatal("render has no ate-api-server Deployment")
	}
	for _, c := range dep.Spec.Template.Spec.InitContainers {
		if c.Name != "objectstore-plugin" {
			continue
		}
		for _, e := range c.Env {
			if e.Name == "AWS_ENDPOINT_URL" && e.Value == "http://rustfs.ate-system.svc:9000" {
				return
			}
		}
		t.Fatalf("objectstore-plugin env = %v, want the kind overlay's AWS_ENDPOINT_URL", c.Env)
	}
	t.Fatal("Deployment has no objectstore-plugin init container")
}
