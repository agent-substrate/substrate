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
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"

	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
	"go.ate.dev/substrate/cmd/ate-setup/internal/log"
)

const gvisorAssetsPrefix = "gvisor-assets"

type gVisorPrepared struct {
	bucket string
	config *unstructured.Unstructured
	assets []rustfsAsset
}

type gVisorDownload struct {
	url    string
	sha256 string
	asset  rustfsAsset
}

func (e *Env) prepareKindGVisorDeps(ctx context.Context) (*gVisorPrepared, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	bucket, err := e.microVMBucket()
	if err != nil {
		return nil, err
	}
	config, downloads, err := prepareKindGVisorConfig(
		e.Cfg.Manifest("sandboxconfig-gvisor.yaml"),
		e.Cfg.Path("bin", gvisorAssetsPrefix),
		bucket,
		runtime.GOARCH,
	)
	if err != nil {
		return nil, err
	}
	assets := make([]rustfsAsset, 0, len(downloads))
	for _, download := range downloads {
		if err := ensureDownloadedAsset(ctx, download.url, download.asset.path, download.sha256); err != nil {
			return nil, err
		}
		assets = append(assets, download.asset)
	}
	return &gVisorPrepared{bucket: bucket, config: config, assets: assets}, nil
}

func prepareKindGVisorConfig(manifestPath, outDir, bucket, arch string) (*unstructured.Unstructured, []gVisorDownload, error) {
	objects, err := kube.LoadPath(manifestPath)
	if err != nil {
		return nil, nil, err
	}
	if len(objects) != 1 {
		return nil, nil, fmt.Errorf("%s decoded to %d objects, want 1", manifestPath, len(objects))
	}
	var config atev1.SandboxConfig
	if err := k8sruntime.DefaultUnstructuredConverter.FromUnstructured(objects[0].Object, &config); err != nil {
		return nil, nil, fmt.Errorf("decoding %s: %w", manifestPath, err)
	}

	var downloads []gVisorDownload
	for i := range config.Spec.Versions {
		version := &config.Spec.Versions[i]
		if version.State != nil && *version.State == atev1.SandboxVersionStateDisabled {
			continue
		}
		assets, ok := version.Assets[arch]
		if !ok {
			return nil, nil, fmt.Errorf("%s version %q has no assets for arch %q", manifestPath, version.Name, arch)
		}
		asset, ok := assets["gvisor"]
		if !ok || asset.SHA256 == "" {
			return nil, nil, fmt.Errorf("%s version %q arch %q has no gvisor asset", manifestPath, version.Name, arch)
		}
		downloadURL, err := publicGCSURL(asset.URL)
		if err != nil {
			return nil, nil, fmt.Errorf("%s version %q arch %q: %w", manifestPath, version.Name, arch, err)
		}
		key := fmt.Sprintf("%s/%s/%s/%s", gvisorAssetsPrefix, arch, asset.SHA256, path.Base(asset.URL))
		localPath := filepath.Join(outDir, arch, asset.SHA256)
		asset.URL = fmt.Sprintf("s3://%s/%s", bucket, key)
		assets["gvisor"] = asset
		version.Assets[arch] = assets
		downloads = append(downloads, gVisorDownload{
			url:    downloadURL,
			sha256: asset.SHA256,
			asset:  rustfsAsset{path: localPath, key: key, label: version.Name + "/" + arch + "/gvisor"},
		})
	}
	if len(downloads) == 0 {
		return nil, nil, fmt.Errorf("%s has no enabled gvisor assets for arch %q", manifestPath, arch)
	}
	object, err := k8sruntime.DefaultUnstructuredConverter.ToUnstructured(&config)
	if err != nil {
		return nil, nil, fmt.Errorf("encoding %s: %w", manifestPath, err)
	}
	return &unstructured.Unstructured{Object: object}, downloads, nil
}

func publicGCSURL(raw string) (string, error) {
	object, ok := strings.CutPrefix(raw, "gs://")
	if !ok || object == "" {
		return "", fmt.Errorf("gvisor asset URL %q is not a gs:// URL", raw)
	}
	return "https://storage.googleapis.com/" + object, nil
}

func ensureDownloadedAsset(ctx context.Context, url, dstPath, wantSHA string) error {
	if got, err := fileSHA256(dstPath); err == nil && got == wantSHA {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dstPath), ".download-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if err := tmp.Close(); err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmpPath) }()
	log.Infof("Downloading %s...", url)
	if err := downloadToFile(ctx, url, tmpPath, 0o644); err != nil {
		return err
	}
	gotSHA, err := fileSHA256(tmpPath)
	if err != nil {
		return err
	}
	if gotSHA != wantSHA {
		return fmt.Errorf("asset %s sha256 = %s, want %s", url, gotSHA, wantSHA)
	}
	if err := os.Rename(tmpPath, dstPath); err != nil {
		return err
	}
	return nil
}

func (e *Env) applyKindGVisorSandboxConfig(ctx context.Context, prep *gVisorPrepared) error {
	log.Infof("Staging gVisor assets to in-cluster rustfs bucket %s (%s/)...", prep.bucket, gvisorAssetsPrefix)
	if err := e.stageAssetsToRustfs(ctx, prep.bucket, gvisorAssetsPrefix, prep.assets); err != nil {
		return err
	}
	return e.Kube.Apply(ctx, []*unstructured.Unstructured{prep.config})
}
