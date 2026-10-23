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

package kustomize

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const testDeployment = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: demo
spec:
  template:
    spec:
      containers:
      - name: web
        image: example.com/web
`

const testConfigMap = `apiVersion: v1
kind: ConfigMap
metadata:
  name: settings
  namespace: demo
data:
  key: value
`

// testComponent labels every Deployment it finds and nothing else, so the
// tests can tell a composed render from a plain one.
const testComponent = `apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component
patches:
  - target:
      kind: Deployment
    patch: |-
      apiVersion: apps/v1
      kind: Deployment
      metadata:
        name: overridden-by-target
        labels:
          composed: "true"
`

// fixture writes a component and two manifests into separate directories, so
// the relative paths Compose computes have to cross directory boundaries.
func fixture(t *testing.T) (component, deployment, configMap string) {
	t.Helper()
	root := t.TempDir()
	component = filepath.Join(root, "components", "label")
	manifests := filepath.Join(root, "manifests")
	for _, dir := range []string{component, manifests} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for path, content := range map[string]string{
		filepath.Join(component, "kustomization.yaml"): testComponent,
		filepath.Join(manifests, "deployment.yaml"):    testDeployment,
		filepath.Join(manifests, "configmap.yaml"):     testConfigMap,
	} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return component, filepath.Join(manifests, "deployment.yaml"), filepath.Join(manifests, "configmap.yaml")
}

func TestComposeAppliesComponentToFile(t *testing.T) {
	component, deployment, _ := fixture(t)

	out, err := Compose(deployment, component)
	if err != nil {
		t.Fatalf("Compose() error = %v", err)
	}
	if !strings.Contains(string(out), `composed: "true"`) {
		t.Errorf("Compose() output lacks the component's label:\n%s", out)
	}
	if !strings.Contains(string(out), "name: web") {
		t.Errorf("Compose() output lost the resource:\n%s", out)
	}
}

// The install wraps every control plane apply, including streams with none
// of the workloads a component targets. Those must pass through unchanged
// rather than fail on "no matches".
func TestComposeLeavesUnmatchedStreamAlone(t *testing.T) {
	component, _, configMap := fixture(t)

	out, err := Compose(configMap, component)
	if err != nil {
		t.Fatalf("Compose() error = %v", err)
	}
	if strings.Contains(string(out), "composed") {
		t.Errorf("Compose() patched a resource the component does not target:\n%s", out)
	}
	if !strings.Contains(string(out), "key: value") {
		t.Errorf("Compose() output lost the resource:\n%s", out)
	}
}

func TestComposeAppliesComponentToDirectory(t *testing.T) {
	component, deployment, _ := fixture(t)
	dir := filepath.Dir(deployment)
	kustomization := "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - deployment.yaml\n  - configmap.yaml\n"
	if err := os.WriteFile(filepath.Join(dir, "kustomization.yaml"), []byte(kustomization), 0o600); err != nil {
		t.Fatal(err)
	}

	out, err := Compose(dir, component)
	if err != nil {
		t.Fatalf("Compose() error = %v", err)
	}
	if !strings.Contains(string(out), `composed: "true"`) {
		t.Errorf("Compose() output lacks the component's label:\n%s", out)
	}
	if !strings.Contains(string(out), "name: settings") {
		t.Errorf("Compose() output lost the second resource:\n%s", out)
	}
}

func TestComposeBytes(t *testing.T) {
	component, _, _ := fixture(t)

	out, err := ComposeBytes([]byte(testDeployment), component)
	if err != nil {
		t.Fatalf("ComposeBytes() error = %v", err)
	}
	if !strings.Contains(string(out), `composed: "true"`) {
		t.Errorf("ComposeBytes() output lacks the component's label:\n%s", out)
	}
}

func TestComposeReportsMissingComponent(t *testing.T) {
	_, deployment, _ := fixture(t)

	if _, err := Compose(deployment, filepath.Join(t.TempDir(), "absent")); err == nil {
		t.Error("Compose() = nil error for a missing component, want one")
	}
}

func TestComponentDirs(t *testing.T) {
	const component = "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\n"
	root := t.TempDir()
	for name, content := range map[string]string{
		"component/kustomization.yaml":     testComponent,
		"second/kustomization.yaml":        testComponent,
		"yml/kustomization.yml":            testComponent,
		"bare/Kustomization":               testComponent,
		"both/kustomization.yaml":          testComponent,
		"both/kustomization.yml":           testComponent,
		"kustomization/kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\n",
		"invalid/kustomization.yaml":       "kind: [\n",
		"untargeted/kustomization.yaml":    "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\npatches:\n  - path: patch.yaml\n",
		"strategic/kustomization.yaml":     "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\npatchesStrategicMerge:\n  - patch.yaml\n",
		"replacements/kustomization.yaml":  component + "replacements:\n  - path: replacement.yaml\n",
		"resources/kustomization.yaml":     component + "resources:\n  - extra.yaml\n",
		"replicas/kustomization.yaml":      component + "replicas:\n  - name: ate-api-server\n    count: 3\n",
		"configmap/kustomization.yaml":     component + "configMapGenerator:\n  - name: extra\n",
		"secret/kustomization.yaml":        component + "secretGenerator:\n  - name: extra\n",
		"prefix/kustomization.yaml":        component + "namePrefix: acme-\n",
		"suffix/kustomization.yaml":        component + "nameSuffix: -acme\n",
		"namespace/kustomization.yaml":     component + "namespace: acme\n",
		"labels/kustomization.yaml":        component + "labels:\n  - pairs:\n      team: platform\n",
		"selectors/kustomization.yaml":     component + "labels:\n  - pairs:\n      team: platform\n    includeSelectors: true\n",
		"nested/kustomization.yaml":        component + "components:\n  - ../component\n",
		"nestedbad/kustomization.yaml":     component + "components:\n  - ../resources\n",
		"cycle/kustomization.yaml":         component + "components:\n  - ../cycle2\n",
		"cycle2/kustomization.yaml":        component + "components:\n  - ../cycle\n",
	} {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(root, "empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Chdir(root)

	for _, tc := range []struct {
		name  string
		dirs  []string
		want  []string
		error string
	}{
		{name: "none"},
		{
			name: "relative paths become absolute, in order",
			dirs: []string{"second", filepath.Join(root, "component")},
			want: []string{filepath.Join(root, "second"), filepath.Join(root, "component")},
		},
		{name: "not a component", dirs: []string{"component", "kustomization"}, error: `kind "Kustomization", want Component`},
		{
			name: "every kustomization file name",
			dirs: []string{"yml", "bare"},
			want: []string{filepath.Join(root, "yml"), filepath.Join(root, "bare")},
		},
		{name: "no kustomization file", dirs: []string{"empty"}, error: "no kustomization file"},
		{name: "two kustomization files", dirs: []string{"both"}, error: "has both kustomization.yaml and kustomization.yml"},
		{name: "invalid YAML", dirs: []string{"invalid"}, error: "while parsing kustomization.yaml"},
		{name: "patch without a target", dirs: []string{"untargeted"}, error: "patch 0 has no target"},
		{name: "deprecated field", dirs: []string{"strategic"}, error: "uses deprecated fields"},
		{name: "replacements", dirs: []string{"replacements"}, error: "replacements is not supported"},
		{name: "resources", dirs: []string{"resources"}, error: "resources is not supported"},
		{name: "replicas", dirs: []string{"replicas"}, error: "replicas is not supported"},
		{name: "configMapGenerator", dirs: []string{"configmap"}, error: "configMapGenerator is not supported"},
		{name: "secretGenerator", dirs: []string{"secret"}, error: "secretGenerator is not supported"},
		{name: "namePrefix", dirs: []string{"prefix"}, error: "namePrefix is not supported"},
		{name: "nameSuffix", dirs: []string{"suffix"}, error: "nameSuffix is not supported"},
		{name: "namespace", dirs: []string{"namespace"}, error: "namespace is not supported"},
		{name: "labels without selectors", dirs: []string{"labels"}, want: []string{filepath.Join(root, "labels")}},
		{name: "labels with selectors", dirs: []string{"selectors"}, error: "labels 0 sets includeSelectors"},
		{name: "nested component", dirs: []string{"nested"}, want: []string{filepath.Join(root, "nested")}},
		{name: "nested component checked", dirs: []string{"nestedbad"}, error: "nested component " + filepath.Join(root, "resources") + ": resources is not supported"},
		{name: "nested cycle", dirs: []string{"cycle"}, error: "includes itself"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ComponentDirs(tc.dirs)
			if tc.error != "" {
				if err == nil || !strings.Contains(err.Error(), tc.error) {
					t.Fatalf("ComponentDirs(%q) error = %v, want one containing %q", tc.dirs, err, tc.error)
				}
				return
			}
			if err != nil {
				t.Fatalf("ComponentDirs(%q): %v", tc.dirs, err)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("ComponentDirs(%q) = %q, want %q", tc.dirs, got, tc.want)
			}
		})
	}
}
