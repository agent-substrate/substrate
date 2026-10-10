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

package v1alpha1

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

const validSHA256 = "a397be1abc2420d26bce6c70e6e2ff96c73aaaab929756c56f5e2089ea842b63"

// vapManifestPath is the SandboxConfig ValidatingAdmissionPolicy shipped with
// the install — loaded here so the test guards the policy we actually ship.
const vapManifestPath = "../../../manifests/ate-install/sandboxconfig-validation.yaml"

const validPauseImage = "registry.k8s.io/pause:3.10.2@sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4"

// sandboxVersion returns a version with a pause image iff the class needs one.
func sandboxVersion(name string, class SandboxClass, assets map[string]map[string]AssetFile) SandboxVersionConfig {
	v := SandboxVersionConfig{Name: name, Assets: assets}
	if class == SandboxClassGvisor {
		v.PauseImage = validPauseImage
	}
	return v
}

// sandboxConfig returns a config with a single default version "v1".
func sandboxConfig(name string, class SandboxClass, assets map[string]map[string]AssetFile) *SandboxConfig {
	return &SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: SandboxConfigSpec{
			SandboxClass:   class,
			DefaultVersion: "v1",
			Versions:       []SandboxVersionConfig{sandboxVersion("v1", class, assets)},
		},
	}
}

// withPauseImage overrides the pause image of the first version on an
// otherwise-valid config.
func withPauseImage(sc *SandboxConfig, image string) *SandboxConfig {
	sc.Spec.Versions[0].PauseImage = image
	return sc
}

// withVersions appends versions to a config.
func withVersions(sc *SandboxConfig, versions ...SandboxVersionConfig) *SandboxConfig {
	sc.Spec.Versions = append(sc.Spec.Versions, versions...)
	return sc
}

// withDefaultVersion overrides the default version of a config.
func withDefaultVersion(sc *SandboxConfig, name string) *SandboxConfig {
	sc.Spec.DefaultVersion = name
	return sc
}

// gvisorAssets returns a valid gVisor asset set for amd64.
func gvisorAssets() map[string]map[string]AssetFile {
	return map[string]map[string]AssetFile{"amd64": {"gvisor": gvisorAsset()}}
}

func runscAsset() AssetFile { return AssetFile{URL: "gs://bucket/runsc", SHA256: validSHA256} }

func gvisorAsset() AssetFile {
	return AssetFile{URL: "gs://bucket/gvisor.tar.bz2", SHA256: validSHA256}
}

// microVMAssets returns a full, valid micro-VM asset set for one architecture:
// the four assets the policy requires. The overlay rootfs serves the OCI image
// over virtio-fs, so virtiofsd is part of the set.
func microVMAssets() map[string]AssetFile {
	a := AssetFile{URL: "gs://bucket/asset", SHA256: validSHA256}
	return map[string]AssetFile{
		"cloud-hypervisor": a,
		"virtiofsd":        a,
		"kata-kernel":      a,
		"kata-image":       a,
	}
}

// applyVAP installs the shipped ValidatingAdmissionPolicy + binding into the
// envtest API server and waits for the apiserver to actually enforce it (policy
// activation is asynchronous), confirmed by a sentinel that must be denied.
func applyVAP(t *testing.T, ctx context.Context) {
	t.Helper()
	raw, err := os.ReadFile(vapManifestPath)
	if err != nil {
		t.Fatalf("read VAP manifest: %v", err)
	}
	for _, doc := range strings.Split(string(raw), "\n---") {
		obj := map[string]any{}
		if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
			t.Fatalf("decode VAP doc: %v", err)
		}
		if len(obj) == 0 {
			continue // comment-only / empty document
		}
		u := &unstructured.Unstructured{Object: obj}
		if err := k8sClient.Create(ctx, u); err != nil && !strings.Contains(err.Error(), "already exists") {
			t.Fatalf("create %s %q: %v", u.GetKind(), u.GetName(), err)
		}
	}

	// Wait until the policy is enforced: a gvisor config missing runsc (valid
	// per the CRD schema) must be denied.
	i := 0
	err = wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		i++
		sc := sandboxConfig(fmt.Sprintf("vap-warmup-%d", i), SandboxClassGvisor,
			map[string]map[string]AssetFile{"amd64": {"notrunsc": runscAsset()}})
		createErr := k8sClient.Create(ctx, sc)
		if createErr == nil {
			_ = k8sClient.Delete(ctx, sc) // policy not active yet; clean up and retry
			return false, nil
		}
		return strings.Contains(createErr.Error(), "runsc"), nil
	})
	if err != nil {
		t.Fatalf("VAP did not become active: %v", err)
	}
}

func TestSandboxConfigValidation(t *testing.T) {
	ctx := t.Context()
	applyVAP(t, ctx)

	tests := []struct {
		name    string
		sc      *SandboxConfig
		wantErr bool
		errMsg  string
	}{{
		name:    "valid gvisor with release tarball",
		sc:      sandboxConfig("ok-gvisor-tarball", SandboxClassGvisor, map[string]map[string]AssetFile{"amd64": {"gvisor": gvisorAsset()}, "arm64": {"gvisor": gvisorAsset()}}),
		wantErr: false,
	}, {
		name:    "valid gvisor with legacy runsc",
		sc:      sandboxConfig("ok-gvisor", SandboxClassGvisor, map[string]map[string]AssetFile{"amd64": {"runsc": runscAsset()}, "arm64": {"runsc": runscAsset()}}),
		wantErr: false,
	}, {
		name:    "valid gvisor mixing tarball and legacy per arch",
		sc:      sandboxConfig("ok-gvisor-mixed", SandboxClassGvisor, map[string]map[string]AssetFile{"amd64": {"gvisor": gvisorAsset()}, "arm64": {"runsc": runscAsset()}}),
		wantErr: false,
	}, {
		name:    "valid microvm with full asset set",
		sc:      sandboxConfig("ok-microvm", "microvm", map[string]map[string]AssetFile{"amd64": microVMAssets()}),
		wantErr: false,
	}, {
		name:    "valid microvm arm64 asset set",
		sc:      sandboxConfig("ok-microvm-arm64", "microvm", map[string]map[string]AssetFile{"arm64": microVMAssets()}),
		wantErr: false,
	}, {
		name: "microvm missing an asset",
		sc: sandboxConfig("bad-microvm", "microvm", map[string]map[string]AssetFile{"amd64": func() map[string]AssetFile {
			m := microVMAssets()
			delete(m, "kata-image")
			return m
		}()}),
		wantErr: true,
		errMsg:  "microvm SandboxConfig must define",
	}, {
		name: "microvm missing virtiofsd",
		sc: sandboxConfig("bad-microvm-novfsd", "microvm", map[string]map[string]AssetFile{"amd64": func() map[string]AssetFile {
			m := microVMAssets()
			delete(m, "virtiofsd")
			return m
		}()}),
		wantErr: true,
		errMsg:  "microvm SandboxConfig must define",
	}, {
		name:    "gvisor arch missing gvisor and runsc",
		sc:      sandboxConfig("bad-no-runsc", SandboxClassGvisor, map[string]map[string]AssetFile{"amd64": {"notrunsc": runscAsset()}}),
		wantErr: true,
		errMsg:  "runsc",
	}, {
		name:    "gvisor one arch missing gvisor and runsc",
		sc:      sandboxConfig("bad-mixed-arch", SandboxClassGvisor, map[string]map[string]AssetFile{"amd64": {"gvisor": gvisorAsset()}, "arm64": {"notrunsc": runscAsset()}}),
		wantErr: true,
		errMsg:  "runsc",
	}, {
		name:    "gvisor with no assets",
		sc:      sandboxConfig("bad-empty", SandboxClassGvisor, nil),
		wantErr: true,
		errMsg:  "runsc",
	}, {
		name:    "asset missing url",
		sc:      sandboxConfig("bad-no-url", SandboxClassGvisor, map[string]map[string]AssetFile{"amd64": {"runsc": {SHA256: validSHA256}}}),
		wantErr: true,
		errMsg:  "url",
	}, {
		name:    "asset missing sha256",
		sc:      sandboxConfig("bad-no-sha", SandboxClassGvisor, map[string]map[string]AssetFile{"amd64": {"runsc": {URL: "gs://bucket/runsc"}}}),
		wantErr: true,
		errMsg:  "sha256",
	}, {
		name:    "asset sha256 not 64 hex",
		sc:      sandboxConfig("bad-sha", SandboxClassGvisor, map[string]map[string]AssetFile{"amd64": {"runsc": {URL: "gs://bucket/runsc", SHA256: "deadbeef"}}}),
		wantErr: true,
		errMsg:  "sha256",
	}, {
		name:    "gvisor missing pauseImage",
		sc:      withPauseImage(sandboxConfig("bad-no-pause", SandboxClassGvisor, map[string]map[string]AssetFile{"amd64": {"gvisor": gvisorAsset()}}), ""),
		wantErr: true,
		errMsg:  "pauseImage is required on every version for gvisor",
	}, {
		name:    "microvm with pauseImage",
		sc:      withPauseImage(sandboxConfig("bad-microvm-pause", SandboxClassMicroVM, map[string]map[string]AssetFile{"amd64": microVMAssets()}), validPauseImage),
		wantErr: true,
		errMsg:  "pauseImage is required on every version for gvisor",
	}, {
		name:    "unpinned pauseImage",
		sc:      withPauseImage(sandboxConfig("bad-unpinned-pause", SandboxClassGvisor, map[string]map[string]AssetFile{"amd64": {"gvisor": gvisorAsset()}}), "registry.k8s.io/pause:3.10.2"),
		wantErr: true,
		errMsg:  "All images must include a digest",
	}, {
		name:    "valid gvisor with two versions",
		sc:      withVersions(sandboxConfig("ok-two-versions", SandboxClassGvisor, gvisorAssets()), sandboxVersion("v2", SandboxClassGvisor, gvisorAssets())),
		wantErr: false,
	}, {
		name: "valid non-default disabled version",
		sc: withVersions(sandboxConfig("ok-disabled-old", SandboxClassGvisor, gvisorAssets()), func() SandboxVersionConfig {
			v := sandboxVersion("v0", SandboxClassGvisor, gvisorAssets())
			v.State = ptr.To(SandboxVersionStateDisabled)
			return v
		}()),
		wantErr: false,
	}, {
		name:    "defaultVersion not in versions",
		sc:      withDefaultVersion(sandboxConfig("bad-default-missing", SandboxClassGvisor, gvisorAssets()), "v9"),
		wantErr: true,
		errMsg:  "defaultVersion must name an entry in versions",
	}, {
		name: "defaultVersion disabled",
		sc: func() *SandboxConfig {
			sc := sandboxConfig("bad-default-disabled", SandboxClassGvisor, gvisorAssets())
			sc.Spec.Versions[0].State = ptr.To(SandboxVersionStateDisabled)
			return sc
		}(),
		wantErr: true,
		errMsg:  "must not be Disabled",
	}, {
		name:    "no versions",
		sc:      &SandboxConfig{ObjectMeta: metav1.ObjectMeta{Name: "bad-no-versions"}, Spec: SandboxConfigSpec{SandboxClass: SandboxClassGvisor, DefaultVersion: "v1", Versions: []SandboxVersionConfig{}}},
		wantErr: true,
		errMsg:  "spec.versions",
	}, {
		name:    "duplicate version names",
		sc:      withVersions(sandboxConfig("bad-dup-versions", SandboxClassGvisor, gvisorAssets()), sandboxVersion("v1", SandboxClassGvisor, gvisorAssets())),
		wantErr: true,
		errMsg:  "Duplicate value",
	}, {
		name: "too many versions",
		sc: func() *SandboxConfig {
			sc := sandboxConfig("bad-many-versions", SandboxClassGvisor, gvisorAssets())
			for i := range 16 {
				sc.Spec.Versions = append(sc.Spec.Versions, sandboxVersion(fmt.Sprintf("extra-%d", i), SandboxClassGvisor, gvisorAssets()))
			}
			return sc
		}(),
		wantErr: true,
		errMsg:  "spec.versions",
	}, {
		name: "version name not a DNS label",
		sc: func() *SandboxConfig {
			sc := sandboxConfig("bad-version-name", SandboxClassGvisor, gvisorAssets())
			sc.Spec.Versions[0].Name = "V_1"
			sc.Spec.DefaultVersion = "V_1"
			return sc
		}(),
		wantErr: true,
		errMsg:  "spec.versions[0].name",
	}, {
		name: "gvisor second version missing pauseImage",
		sc: withVersions(sandboxConfig("bad-v2-no-pause", SandboxClassGvisor, gvisorAssets()), func() SandboxVersionConfig {
			v := sandboxVersion("v2", SandboxClassGvisor, gvisorAssets())
			v.PauseImage = ""
			return v
		}()),
		wantErr: true,
		errMsg:  "pauseImage is required on every version for gvisor",
	}, {
		name: "microvm second version with pauseImage",
		sc: withVersions(sandboxConfig("bad-microvm-v2-pause", SandboxClassMicroVM, map[string]map[string]AssetFile{"amd64": microVMAssets()}), func() SandboxVersionConfig {
			v := sandboxVersion("v2", SandboxClassMicroVM, map[string]map[string]AssetFile{"amd64": microVMAssets()})
			v.PauseImage = validPauseImage
			return v
		}()),
		wantErr: true,
		errMsg:  "pauseImage is required on every version for gvisor",
	}, {
		name:    "gvisor second version missing gvisor and runsc",
		sc:      withVersions(sandboxConfig("bad-v2-no-runsc", SandboxClassGvisor, gvisorAssets()), sandboxVersion("v2", SandboxClassGvisor, map[string]map[string]AssetFile{"amd64": {"notrunsc": runscAsset()}})),
		wantErr: true,
		errMsg:  "runsc",
	}, {
		name: "microvm second version missing an asset",
		sc: withVersions(sandboxConfig("bad-microvm-v2", SandboxClassMicroVM, map[string]map[string]AssetFile{"amd64": microVMAssets()}), sandboxVersion("v2", SandboxClassMicroVM, map[string]map[string]AssetFile{"amd64": func() map[string]AssetFile {
			m := microVMAssets()
			delete(m, "kata-kernel")
			return m
		}()})),
		wantErr: true,
		errMsg:  "microvm SandboxConfig must define",
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := k8sClient.Create(ctx, tt.sc)
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("Create() unexpected error: %v", err)
				}
				t.Cleanup(func() { _ = k8sClient.Delete(ctx, tt.sc, &client.DeleteOptions{}) })
				return
			}
			if err == nil {
				_ = k8sClient.Delete(ctx, tt.sc)
				t.Fatalf("Create() succeeded, want denied")
			}
			if tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
				t.Errorf("Create() error = %q, want it to contain %q", err.Error(), tt.errMsg)
			}
		})
	}
}

func TestSandboxConfigUpdateRemovingDefaultVersion(t *testing.T) {
	ctx := t.Context()
	sc := withVersions(sandboxConfig("update-remove-default", SandboxClassGvisor, gvisorAssets()), sandboxVersion("v2", SandboxClassGvisor, gvisorAssets()))
	if err := k8sClient.Create(ctx, sc); err != nil {
		t.Fatalf("Create() unexpected error: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, sc) })

	// Dropping the version defaultVersion names must be denied.
	sc.Spec.Versions = sc.Spec.Versions[1:]
	err := k8sClient.Update(ctx, sc)
	if err == nil {
		t.Fatalf("Update() removing the default version succeeded, want denied")
	}
	if !strings.Contains(err.Error(), "defaultVersion must name an entry in versions") {
		t.Errorf("Update() error = %q, want it to mention defaultVersion", err.Error())
	}

	// Repointing defaultVersion in the same update is allowed.
	sc.Spec.DefaultVersion = "v2"
	if err := k8sClient.Update(ctx, sc); err != nil {
		t.Fatalf("Update() repointing defaultVersion: %v", err)
	}
}

// TestShippedSandboxConfigManifests guards the SandboxConfigs the install ships
// against the CRD schema and the shipped ValidatingAdmissionPolicy.
func TestShippedSandboxConfigManifests(t *testing.T) {
	ctx := t.Context()
	applyVAP(t, ctx)

	for _, path := range []string{
		"../../../manifests/ate-install/sandboxconfig-gvisor.yaml",
		"../../../manifests/microvm/sandboxconfig-microvm.yaml.tmpl",
	} {
		t.Run(path, func(t *testing.T) {
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read manifest: %v", err)
			}
			raw = []byte(strings.ReplaceAll(string(raw), "${BUCKET_NAME}", "test-bucket"))
			sc := &SandboxConfig{}
			if err := yaml.UnmarshalStrict(raw, sc); err != nil {
				t.Fatalf("decode manifest: %v", err)
			}
			if sc.Annotations[SandboxConfigClassDefaultAnnotation] != "true" {
				t.Errorf("manifest is not annotated %s=true", SandboxConfigClassDefaultAnnotation)
			}
			if err := k8sClient.Create(ctx, sc); err != nil {
				t.Fatalf("Create() unexpected error: %v", err)
			}
			t.Cleanup(func() { _ = k8sClient.Delete(ctx, sc) })
		})
	}
}
