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
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
	apiv1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/assetfetch"
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

func setGvisorAssetURL(t *testing.T, obj *unstructured.Unstructured, versionName, arch, assetName, assetURL string) {
	t.Helper()
	versions, found, err := unstructured.NestedSlice(obj.Object, "spec", "versions")
	if err != nil || !found {
		t.Fatalf("reading spec.versions: found=%v err=%v", found, err)
	}
	for versionIndex, rawVersion := range versions {
		version := rawVersion.(map[string]any)
		if version["name"] != versionName {
			continue
		}
		assets := version["assets"].(map[string]any)
		files := assets[arch].(map[string]any)
		asset := files[assetName].(map[string]any)
		asset["url"] = assetURL
		files[assetName] = asset
		assets[arch] = files
		version["assets"] = assets
		versions[versionIndex] = version
		if err := unstructured.SetNestedSlice(obj.Object, versions, "spec", "versions"); err != nil {
			t.Fatalf("updating spec.versions: %v", err)
		}
		return
	}
	t.Fatalf("asset %s/%s/%s not found", versionName, arch, assetName)
}

const testGvisorAssetSHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestGvisorSandboxConfigObjectsRewritesAssetBaseURL(t *testing.T) {
	const mirror = "s3://substrate-assets/gvisor/releases/"
	root := repoRoot(t)
	baseObjects, err := (&Env{Cfg: &config.Config{Root: root}}).gvisorSandboxConfigObjects()
	if err != nil {
		t.Fatalf("loading the default SandboxConfig: %v", err)
	}
	wantObjects := make([]*unstructured.Unstructured, len(baseObjects))
	for i, obj := range baseObjects {
		wantObjects[i] = obj.DeepCopy()
	}
	wantConfig := findObject(wantObjects, sandboxConfigKind, defaultGvisorSandboxConfigName)
	if wantConfig == nil {
		t.Fatalf("objects have no sandboxconfig/%s", defaultGvisorSandboxConfigName)
	}
	sandboxConfig := &apiv1alpha1.SandboxConfig{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(wantConfig.Object, sandboxConfig); err != nil {
		t.Fatalf("decoding the default SandboxConfig: %v", err)
	}
	rewritten := 0
	for _, version := range sandboxConfig.Spec.Versions {
		for arch, assets := range version.Assets {
			for name, asset := range assets {
				if strings.HasPrefix(asset.URL, bundledGvisorReleaseBaseURL) {
					setGvisorAssetURL(t, wantConfig, version.Name, arch, name,
						mirror+strings.TrimPrefix(asset.URL, bundledGvisorReleaseBaseURL))
					rewritten++
				}
			}
		}
	}
	if rewritten == 0 {
		t.Fatal("the real bundled SandboxConfig has no public release URLs")
	}

	mirroredObjects, err := (&Env{Cfg: &config.Config{
		Root: root, GvisorAssetBaseURL: mirror,
	}}).gvisorSandboxConfigObjects()
	if err != nil {
		t.Fatalf("loading the mirrored SandboxConfig: %v", err)
	}
	if diff := cmp.Diff(wantObjects, mirroredObjects); diff != "" {
		t.Errorf("mirrored objects differ from the base outside the expected URLs (-want +got):\n%s", diff)
	}

	mirroredConfig := findObject(mirroredObjects, sandboxConfigKind, defaultGvisorSandboxConfigName)
	if mirroredConfig == nil {
		t.Fatalf("mirrored objects have no sandboxconfig/%s", defaultGvisorSandboxConfigName)
	}
	if got := mirroredConfig.GetAnnotations()[apiv1alpha1.SandboxConfigClassDefaultAnnotation]; got != "true" {
		t.Errorf("is-class-default annotation = %q, want true", got)
	}
}

func gvisorSandboxConfigObject(t *testing.T, versions ...apiv1alpha1.SandboxVersionConfig) *unstructured.Unstructured {
	t.Helper()
	config := &apiv1alpha1.SandboxConfig{
		TypeMeta:   metav1.TypeMeta{APIVersion: apiv1alpha1.GroupVersion.String(), Kind: sandboxConfigKind},
		ObjectMeta: metav1.ObjectMeta{Name: defaultGvisorSandboxConfigName},
		Spec: apiv1alpha1.SandboxConfigSpec{
			SandboxClass: apiv1alpha1.SandboxClassGvisor,
			Versions:     versions,
		},
	}
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(config)
	if err != nil {
		t.Fatalf("encoding test SandboxConfig: %v", err)
	}
	unstructured.RemoveNestedField(raw, "metadata", "creationTimestamp")
	return &unstructured.Unstructured{Object: raw}
}

func TestRewriteBundledGvisorAssetURLsCoversEveryVersion(t *testing.T) {
	const mirror = "s3://substrate-assets/gvisor/releases/"
	obj := gvisorSandboxConfigObject(t,
		apiv1alpha1.SandboxVersionConfig{
			Name: "v1",
			Assets: map[string]map[string]apiv1alpha1.AssetFile{
				"amd64": {
					"gvisor": {URL: bundledGvisorReleaseBaseURL + "nightly/v1/gvisor.tar.zstd", SHA256: testGvisorAssetSHA256},
					"other":  {URL: "s3://already-internal/other", SHA256: testGvisorAssetSHA256},
				},
			},
		},
		apiv1alpha1.SandboxVersionConfig{
			Name: "v2",
			Assets: map[string]map[string]apiv1alpha1.AssetFile{
				"arm64": {"gvisor": {URL: bundledGvisorReleaseBaseURL + "nightly/v2/gvisor.tar.zstd", SHA256: testGvisorAssetSHA256}},
			},
		},
	)
	want := obj.DeepCopy()
	setGvisorAssetURL(t, want, "v1", "amd64", "gvisor", mirror+"nightly/v1/gvisor.tar.zstd")
	setGvisorAssetURL(t, want, "v2", "arm64", "gvisor", mirror+"nightly/v2/gvisor.tar.zstd")
	if err := rewriteBundledGvisorAssetURLs([]*unstructured.Unstructured{obj}, mirror); err != nil {
		t.Fatalf("rewriteBundledGvisorAssetURLs: %v", err)
	}
	if diff := cmp.Diff(want.Object, obj.Object); diff != "" {
		t.Errorf("rewritten object lost or changed fields outside the expected URLs (-want +got):\n%s", diff)
	}
}

func TestRewriteBundledGvisorAssetURLsRejectsRemainingPublicURL(t *testing.T) {
	obj := gvisorSandboxConfigObject(t, apiv1alpha1.SandboxVersionConfig{
		Name: "v1",
		Assets: map[string]map[string]apiv1alpha1.AssetFile{
			"amd64": {
				"gvisor": {URL: bundledGvisorReleaseBaseURL + "nightly/v1/gvisor.tar.zstd", SHA256: testGvisorAssetSHA256},
				"helper": {URL: bundledGvisorBucketBaseURL + "builds/helper", SHA256: testGvisorAssetSHA256},
			},
		},
	})
	err := rewriteBundledGvisorAssetURLs([]*unstructured.Unstructured{obj}, "s3://mirror/releases/")
	if err == nil || !strings.Contains(err.Error(), "still uses the public gVisor bucket") {
		t.Fatalf("rewriteBundledGvisorAssetURLs error = %v, want remaining public bucket error", err)
	}
}

func TestRewriteBundledGvisorAssetURLsFailsClosed(t *testing.T) {
	obj := gvisorSandboxConfigObject(t, apiv1alpha1.SandboxVersionConfig{
		Name: "v1",
		Assets: map[string]map[string]apiv1alpha1.AssetFile{
			"amd64": {"gvisor": {URL: "s3://already-internal/gvisor.tar.zstd", SHA256: testGvisorAssetSHA256}},
		},
	})
	err := rewriteBundledGvisorAssetURLs([]*unstructured.Unstructured{obj}, "s3://mirror/releases/")
	if err == nil || !strings.Contains(err.Error(), "has no asset URL beginning") {
		t.Fatalf("rewriteBundledGvisorAssetURLs error = %v, want no matching release URL error", err)
	}
}

func TestRewriteBundledGvisorAssetURLsRejectsLongResult(t *testing.T) {
	obj := gvisorSandboxConfigObject(t, apiv1alpha1.SandboxVersionConfig{
		Name: "v1",
		Assets: map[string]map[string]apiv1alpha1.AssetFile{
			"amd64": {"gvisor": {URL: bundledGvisorReleaseBaseURL + "gvisor.tar.zstd", SHA256: testGvisorAssetSHA256}},
		},
	})
	base := "s3://bucket/" + strings.Repeat("x", assetfetch.MaxURIBytes) + "/"
	err := rewriteBundledGvisorAssetURLs([]*unstructured.Unstructured{obj}, base)
	if err == nil || !strings.Contains(err.Error(), "more than the limit") {
		t.Fatalf("rewriteBundledGvisorAssetURLs error = %v, want URI length error", err)
	}
	if strings.Contains(err.Error(), "rpc error:") {
		t.Errorf("rewriteBundledGvisorAssetURLs error contains gRPC status wrapping: %q", err)
	}
}

func TestRewriteBundledGvisorAssetURLsRejectsPluginInvalidResult(t *testing.T) {
	for _, tc := range []struct {
		baseURL, wantReason string
	}{
		{"s3://bucket/x%2F/", "empty, '.'-terminated or space-padded path segment"},
		{"s3://bucket/atespaces/team/tags/", "stored under snapshot"},
		{"s3://bucket:9000/x/", "must name a bucket without a port"},
	} {
		t.Run(tc.baseURL, func(t *testing.T) {
			obj := gvisorSandboxConfigObject(t, apiv1alpha1.SandboxVersionConfig{
				Name: "v1",
				Assets: map[string]map[string]apiv1alpha1.AssetFile{
					"amd64": {"gvisor": {URL: bundledGvisorReleaseBaseURL + "nightly/v1/gvisor.tar.zstd", SHA256: testGvisorAssetSHA256}},
				},
			})
			err := rewriteBundledGvisorAssetURLs([]*unstructured.Unstructured{obj}, tc.baseURL)
			if err == nil || !strings.Contains(err.Error(), tc.wantReason) {
				t.Fatalf("rewriteBundledGvisorAssetURLs error = %v, want %q", err, tc.wantReason)
			}
		})
	}
}

// Runs over the real generated manifests: EnsureCRDs and DeployCRDs act on what
// generatedCRDs returns, so it has to hold every CRD file and no RBAC.
func TestGeneratedCRDsLeavesOutRBAC(t *testing.T) {
	e := &Env{Cfg: &config.Config{Root: repoRoot(t)}}
	crds, err := e.generatedCRDs()
	if err != nil {
		t.Fatalf("loading the generated CRDs: %v", err)
	}
	for _, crd := range crds {
		if crd.GetKind() != "CustomResourceDefinition" {
			t.Errorf("generatedCRDs returned %s/%s", crd.GetKind(), crd.GetName())
		}
	}
	files, err := filepath.Glob(filepath.Join(e.Cfg.Manifest("generated"), "ate.dev_*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(crds) != len(files) {
		t.Errorf("generatedCRDs returned %d CRDs, want one per CRD file: %v", len(crds), files)
	}
}

// A component redeploy creates only the CRDs the cluster lacks. One that is
// already there, even at an older schema, is left to DeployCRDs.
func TestMissingCRDsLeavesPresentOnesAlone(t *testing.T) {
	crd := func(name string) *unstructured.Unstructured {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(crdGVK)
		obj.SetName(name)
		return obj
	}
	crds := []*unstructured.Unstructured{crd("workerpools.ate.dev"), crd("sandboxconfigs.ate.dev"), crd("csidriverconfigs.ate.dev")}
	present := map[string]bool{"workerpools.ate.dev": true, "sandboxconfigs.ate.dev": true}
	missing, err := missingCRDs(crds, func(name string) (bool, error) { return present[name], nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 1 || missing[0].GetName() != "csidriverconfigs.ate.dev" {
		var names []string
		for _, m := range missing {
			names = append(names, m.GetName())
		}
		t.Errorf("missingCRDs = %v, want only csidriverconfigs.ate.dev", names)
	}
}
