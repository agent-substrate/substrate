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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

const validSHA256 = "a397be1abc2420d26bce6c70e6e2ff96c73aaaab929756c56f5e2089ea842b63"

// vapManifestPath is the SandboxConfig ValidatingAdmissionPolicy shipped with
// the install — loaded here so the test guards the policy we actually ship.
const vapManifestPath = "../../../manifests/ate-install/sandboxconfig-validation.yaml"

func sandboxConfig(name string, class SandboxClass, assets map[string]map[string]AssetFile) *SandboxConfig {
	return &SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: SandboxConfigSpec{
			SandboxClass: class,
			Assets:       assets,
		},
	}
}

func runscAsset() AssetFile { return AssetFile{URL: "gs://bucket/runsc", SHA256: validSHA256} }

// microVMAssets returns a full, valid micro-VM asset set for one architecture:
// the four assets the policy requires. ateom owns the cloud-hypervisor boot and
// gives the actor a writable virtio-blk rootfs, so the set has no kata-shim or
// virtiofsd.
func microVMAssets() map[string]AssetFile {
	a := AssetFile{URL: "gs://bucket/asset", SHA256: validSHA256}
	return map[string]AssetFile{
		"cloud-hypervisor": a,
		"kata-kernel":      a,
		"kata-image":       a,
		"kata-config":      a,
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
		name:    "valid gvisor with runsc",
		sc:      sandboxConfig("ok-gvisor", SandboxClassGvisor, map[string]map[string]AssetFile{"amd64": {"runsc": runscAsset()}, "arm64": {"runsc": runscAsset()}}),
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
		name:    "gvisor arch missing runsc",
		sc:      sandboxConfig("bad-no-runsc", SandboxClassGvisor, map[string]map[string]AssetFile{"amd64": {"notrunsc": runscAsset()}}),
		wantErr: true,
		errMsg:  "runsc",
	}, {
		name:    "gvisor one arch missing runsc",
		sc:      sandboxConfig("bad-mixed-arch", SandboxClassGvisor, map[string]map[string]AssetFile{"amd64": {"runsc": runscAsset()}, "arm64": {"notrunsc": runscAsset()}}),
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
