// Copyright 2026 The Agent Substrate Authors
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
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	k8sruntime "k8s.io/apimachinery/pkg/runtime"

	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
)

func TestPrepareKindGVisorConfig(t *testing.T) {
	manifest := filepath.Join(repoRoot(t), "manifests", "ate-install", "sandboxconfig-gvisor.yaml")
	config, downloads, err := prepareKindGVisorConfig(manifest, t.TempDir(), "test-bucket", runtime.GOARCH)
	if err != nil {
		t.Fatal(err)
	}
	if len(downloads) == 0 {
		t.Fatal("prepareKindGVisorConfig() returned no downloads")
	}

	var typed atev1.SandboxConfig
	if err := k8sruntime.DefaultUnstructuredConverter.FromUnstructured(config.Object, &typed); err != nil {
		t.Fatal(err)
	}
	for _, download := range downloads {
		if !strings.HasPrefix(download.url, "https://storage.googleapis.com/gvisor/") {
			t.Errorf("download URL = %q", download.url)
		}
		if !strings.HasPrefix(download.asset.key, gvisorAssetsPrefix+"/"+runtime.GOARCH+"/") || !strings.HasSuffix(download.asset.key, "/"+path.Base(download.url)) {
			t.Errorf("asset key = %q", download.asset.key)
		}
	}
	for _, version := range typed.Spec.Versions {
		if version.State != nil && *version.State == atev1.SandboxVersionStateDisabled {
			continue
		}
		asset := version.Assets[runtime.GOARCH]["gvisor"]
		if want := "s3://test-bucket/" + gvisorAssetsPrefix + "/" + runtime.GOARCH + "/" + asset.SHA256 + "/gvisor.tar.zstd"; asset.URL != want {
			t.Errorf("version %s URL = %q, want %q", version.Name, asset.URL, want)
		}
	}
}
