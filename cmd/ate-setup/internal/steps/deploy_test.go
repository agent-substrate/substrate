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

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
)

// podcertObjects loads the podcertificate-controller manifest as
// DeployPodCertificateController renders it, before image resolution.
func podcertObjects(t *testing.T, clusterSize string) []*unstructured.Unstructured {
	t.Helper()
	e := &Env{Cfg: &config.Config{Root: repoRoot(t), ClusterSize: clusterSize}}
	path := e.Cfg.Manifest("pod-certificate-controller.yaml")
	if e.Cfg.Size10() {
		path = e.Cfg.Manifest("podcert-size10")
	}
	manifest, err := e.render(path)
	if err != nil {
		t.Fatalf("render(%s): %v", path, err)
	}
	objs, err := kube.DecodeManifestBytes(manifest)
	if err != nil {
		t.Fatalf("decoding the podcertificate-controller manifest: %v", err)
	}
	return objs
}

// workersPerSigner returns every WORKERS_PER_SIGNER value on the controller
// container, so a duplicated entry shows up as more than one.
func workersPerSigner(t *testing.T, objs []*unstructured.Unstructured) []any {
	t.Helper()
	dep := findObject(objs, "Deployment", "podcertificate-controller")
	if dep == nil {
		t.Fatal("manifest has no deployment/podcertificate-controller")
	}
	containers, _, _ := unstructured.NestedSlice(dep.Object, "spec", "template", "spec", "containers")
	env, _, _ := unstructured.NestedSlice(containers[0].(map[string]any), "env")
	var values []any
	for _, v := range env {
		if envVar := v.(map[string]any); envVar["name"] == "WORKERS_PER_SIGNER" {
			values = append(values, envVar["value"])
		}
	}
	return values
}

// Runs over the real manifests, so a renamed variable or Deployment fails here
// rather than on an install that silently keeps one worker.
func TestSetPodcertWorkersPerSigner(t *testing.T) {
	for _, size := range []string{config.ClusterSizeSize0, config.ClusterSizeSize10} {
		t.Run(size, func(t *testing.T) {
			objs := podcertObjects(t, size)
			if got := workersPerSigner(t, objs); len(got) != 1 {
				t.Fatalf("base manifest WORKERS_PER_SIGNER = %v, want exactly one entry", got)
			}
			if err := setPodcertWorkersPerSigner(objs, 8); err != nil {
				t.Fatalf("setPodcertWorkersPerSigner: %v", err)
			}
			if got := workersPerSigner(t, objs); len(got) != 1 || got[0] != "8" {
				t.Errorf("WORKERS_PER_SIGNER = %v, want [8]", got)
			}
		})
	}
}

func TestSetPodcertWorkersPerSignerAppendsWhenAbsent(t *testing.T) {
	dep := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata":   map[string]any{"namespace": NamespacePodCert, "name": "podcertificate-controller"},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
			"containers": []any{map[string]any{"name": "controller"}},
		}}},
	}}
	objs := []*unstructured.Unstructured{dep}
	if err := setPodcertWorkersPerSigner(objs, 3); err != nil {
		t.Fatalf("setPodcertWorkersPerSigner: %v", err)
	}
	if got := workersPerSigner(t, objs); len(got) != 1 || got[0] != "3" {
		t.Errorf("WORKERS_PER_SIGNER = %v, want [3]", got)
	}
}

func TestSetPodcertWorkersPerSignerRejectsMissingDeployment(t *testing.T) {
	var withoutDeployment []*unstructured.Unstructured
	for _, obj := range podcertObjects(t, config.ClusterSizeSize0) {
		if obj.GetKind() != "Deployment" {
			withoutDeployment = append(withoutDeployment, obj)
		}
	}
	if err := setPodcertWorkersPerSigner(withoutDeployment, 2); err == nil {
		t.Error("setPodcertWorkersPerSigner succeeded without a Deployment, want an error")
	}
}
