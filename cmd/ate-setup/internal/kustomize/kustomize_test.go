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
