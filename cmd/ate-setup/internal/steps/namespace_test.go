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
	"context"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
)

type namespaceTestResolver struct{}

func (namespaceTestResolver) ResolvePath(_ context.Context, path string) ([]byte, error) {
	return kube.ReadPath(path)
}

func (namespaceTestResolver) ResolveBytes(_ context.Context, manifest []byte) ([]byte, error) {
	return manifest, nil
}

func TestEnvNamespace(t *testing.T) {
	tests := []struct {
		name string
		cfg  *config.Config
		want string
	}{
		{"falls back to the canonical namespace when unset", &config.Config{}, NamespaceAteSystem},
		{"uses the configured namespace", &config.Config{Namespace: "substrate-dev"}, "substrate-dev"},
		{"tolerates a nil config", nil, NamespaceAteSystem},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Env{Cfg: tt.cfg}
			if got := e.Namespace(); got != tt.want {
				t.Errorf("Namespace() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestInstallNamespaces(t *testing.T) {
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{
		Root: root, Namespace: "substrate-demo", PodcertNamespace: "cert-demo",
	}
	e := &Env{Cfg: cfg, resolver: namespaceTestResolver{}}
	for _, path := range []string{
		cfg.Manifest("pod-certificate-controller.yaml"),
		cfg.Manifest("ate-api-server.yaml"),
		cfg.Manifest("postgres", "postgres.yaml"),
		cfg.Path(SystemOverlay(cfg)),
		cfg.Path("manifests/ate-install/kind"),
		cfg.Path("manifests/ate-install/agentgateway"),
		cfg.Path("manifests/ate-install/kind-agentgateway"),
		cfg.Manifest("podcert-size10"),
	} {
		t.Run(path, func(t *testing.T) {
			manifest, err := e.renderResolve(t.Context(), path)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(manifest), "podcertificate-controller-system") || strings.Contains(string(manifest), "ate-system") {
				t.Fatal("rendered manifest still refers to a canonical namespace")
			}
			if _, err := kube.DecodeManifestBytes(manifest); err != nil {
				t.Fatalf("relocated manifest is invalid: %v", err)
			}
			if filepath.Base(path) == "ate-controller.yaml" || filepath.Base(path) == "atelet.yaml" {
				for _, want := range []string{"api.substrate-demo.svc", "namespace: substrate-demo"} {
					if !strings.Contains(string(manifest), want) {
						t.Errorf("rendered %s missing %q", path, want)
					}
				}
			}
		})
	}
	if got := e.PodcertNamespace(); got != "cert-demo" {
		t.Errorf("PodcertNamespace() = %q", got)
	}
	objs, err := e.installPathObjects(cfg.Manifest("pod-certificate-controller.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	binding := false
	for _, obj := range objs {
		if obj.GetKind() == "Deployment" && obj.GetName() == "podcertificate-controller" {
			found = true
			if obj.GetNamespace() != "cert-demo" {
				t.Errorf("controller namespace = %q", obj.GetNamespace())
			}
		}
		if obj.GetKind() == "ClusterRoleBinding" {
			subjects, found, err := unstructured.NestedSlice(obj.Object, "subjects")
			if err != nil {
				t.Fatal(err)
			}
			if found && len(subjects) > 0 {
				subject := subjects[0].(map[string]any)
				if subject["namespace"] != "cert-demo" {
					t.Errorf("ClusterRoleBinding %s subject namespace = %v", obj.GetName(), subject["namespace"])
				}
				binding = true
			}
		}
	}
	if !found {
		t.Fatal("missing controller Deployment")
	}
	if !binding {
		t.Fatal("missing podcert ClusterRoleBinding")
	}
	if err := setPodcertWorkersPerSigner(objs, e.PodcertNamespace(), 8); err != nil {
		t.Fatalf("size10 worker override in relocated namespace: %v", err)
	}
}
