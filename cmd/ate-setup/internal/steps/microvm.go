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
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"

	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
	"go.ate.dev/substrate/cmd/ate-setup/internal/log"
	"go.ate.dev/substrate/cmd/ate-setup/internal/render"
)

const (
	// MicroVMStageTimeout bounds the wait for the in-cluster rustfs Deployment
	// and bucket-init Job on Kind.
	MicroVMStageTimeout = 300 * time.Second

	// DefaultSandboxClasses is the default value of --sandbox-classes.
	DefaultSandboxClasses = "gvisor,microvm"

	// Keep defaultKataVersion at 4.1.0 or later. Cloud Hypervisor's restore
	// handshake hangs against virtiofsd v1.13.3 (bundled through Kata 4.0.0),
	// and Kata 4.1.0 is the first release with virtiofsd v1.14.0's vhost-user
	// snapshot-restore (REPLY_ACK) fix.
	defaultKataVersion            = "4.3.1"
	defaultCloudHypervisorVersion = "v53.0"

	assetStampFile               = ".asset-versions"
	defaultKindMicroVMBucket     = "ate-snapshots"
	maxGCSBucketNameLength       = 63
	microvmAssetsPrefix          = "microvm-assets"
	microvmSandboxConfigTemplate = "manifests/ate-install/sandboxconfig-microvm.yaml.tmpl"
	microvmSandboxConfigName     = "microvm"

	// Keep in sync with manifests/ate-install/kind/rustfs.yaml.
	rustfsAWSCLIImage = "amazon/aws-cli:2.38.5@sha256:c8156a0e74e13929a42d7e60f0d4e14e7d28d980cd21822505d4bff80e5460c1"

	kataSharePrefix      = "opt/kata/share/kata-containers/"
	kataKernelSymlink    = "opt/kata/share/kata-containers/vmlinux.container"
	kataRootfsSymlink    = "opt/kata/share/kata-containers/kata-containers.img"
	kataLibexecVirtiofsd = "opt/kata/libexec/virtiofsd"

	maxMicroVMAssetBytes int64 = 8 << 30
)

var (
	microvmArches = []string{"amd64", "arm64"}
	microvmAssets = []string{"cloud-hypervisor", "virtiofsd", "vmlinux", "rootfs.img"}

	// microvmTemplateAssetFiles maps each asset key in
	// manifests/ate-install/sandboxconfig-microvm.yaml.tmpl to the local file
	// name assembled under bin/microvm-assets/<arch>/.
	microvmTemplateAssetFiles = map[string]string{
		"cloud-hypervisor": "cloud-hypervisor",
		"virtiofsd":        "virtiofsd",
		"kata-kernel":      "vmlinux",
		"kata-image":       "rootfs.img",
	}
)

// microvmReleaseBaseURL is overridden in unit tests to point at an httptest.Server.
var microvmReleaseBaseURL = "https://github.com"

// ValidateSandboxClasses reports whether classes is a valid --sandbox-classes value.
func ValidateSandboxClasses(classes string) error {
	_, _, err := parseSandboxClasses(classes)
	return err
}

func parseSandboxClasses(classes string) (gvisor, microvm bool, err error) {
	if strings.TrimSpace(classes) == "" {
		return true, true, nil
	}
	for _, raw := range strings.Split(classes, ",") {
		switch c := strings.TrimSpace(raw); c {
		case "gvisor":
			gvisor = true
		case "microvm":
			microvm = true
		default:
			return false, false, fmt.Errorf("unknown sandbox class %q (valid options: gvisor, microvm)", c)
		}
	}
	return gvisor, microvm, nil
}

// microVMPrepared holds the validated target bucket and per-architecture
// asset digests assembled by prepareMicroVMDeps before the cluster is touched.
type microVMPrepared struct {
	bucket  string
	digests map[string]map[string]string
}

// microVMArches returns the architectures to assemble and stage: only the host
// architecture on Kind (where nodes always match runtime.GOARCH), and both
// amd64 and arm64 on non-Kind clusters.
func (e *Env) microVMArches() []string {
	if e.Cfg != nil && e.Cfg.Kind {
		return []string{runtime.GOARCH}
	}
	return microvmArches
}

// prepareMicroVMDeps validates the microVM prerequisites, assembles the guest
// assets locally, and on non-Kind clusters uploads them to GCS before any
// cluster resources are modified.
func (e *Env) prepareMicroVMDeps(ctx context.Context) (*microVMPrepared, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	log.Step("prepare_microvm_deps")

	bucket, err := e.checkMicroVMDeps()
	if err != nil {
		return nil, err
	}
	digests, err := loadMicroVMAssetDigests(e.Cfg.Manifest("sandboxconfig-microvm.yaml.tmpl"))
	if err != nil {
		return nil, err
	}

	for _, arch := range e.microVMArches() {
		outDir := e.Cfg.Path("bin", microvmAssetsPrefix, arch)
		wantStamp := microvmAssetStamp(arch, defaultKataVersion, defaultCloudHypervisorVersion)
		needAssemble, stale := microvmAssetsNeedAssemble(outDir, wantStamp, digests[arch])
		if stale {
			log.Infof("Asset set in %s is stale; re-assembling.", outDir)
		}
		if needAssemble {
			log.Infof("Assembling microVM assets into %s (arch=%s)...", outDir, arch)
			if err := assembleMicroVMAssets(ctx, outDir, arch, defaultKataVersion, defaultCloudHypervisorVersion, digests[arch]); err != nil {
				return nil, err
			}
		} else {
			log.Infof("Assets already present in %s; skipping assemble.", outDir)
		}
	}

	if !e.Cfg.Kind {
		log.Infof("Uploading assets to gs://%s/%s/ ...", bucket, microvmAssetsPrefix)
		if err := e.stageMicroVMAssetsToGCS(ctx, bucket, digests); err != nil {
			return nil, err
		}
	}
	return &microVMPrepared{bucket: bucket, digests: digests}, nil
}

// applyMicroVMSandboxConfig stages the assembled microVM guest assets into
// in-cluster rustfs on Kind and applies the cluster-wide `microvm` SandboxConfig.
func (e *Env) applyMicroVMSandboxConfig(ctx context.Context, prep *microVMPrepared) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	log.Step("deploy_microvm_sandboxconfig")

	if e.Cfg.Kind {
		log.Infof("Staging assets to in-cluster rustfs bucket %s (%s/)...", prep.bucket, microvmAssetsPrefix)
		if err := e.stageMicroVMAssetsToRustfs(ctx, prep.bucket, prep.digests); err != nil {
			return err
		}
	}

	log.Infof("Applying microvm SandboxConfig from %s...", microvmSandboxConfigTemplate)
	rendered, err := render.Template(
		e.Cfg.Manifest("sandboxconfig-microvm.yaml.tmpl"),
		map[string]string{
			"BUCKET_SCHEME":     e.microVMBucketScheme(),
			"ASSET_BUCKET_NAME": prep.bucket,
		},
		nil,
	)
	if err != nil {
		return err
	}
	if err := e.Kube.ApplyBytes(ctx, rendered); err != nil {
		return err
	}
	log.Infof("Done. Installed cluster-wide microvm SandboxConfig.")
	return nil
}

// loadMicroVMAssetDigests reads the expected per-architecture SHA-256 digests
// for microvmAssets from the default version of sandboxconfig-microvm.yaml.tmpl.
func loadMicroVMAssetDigests(tmplPath string) (map[string]map[string]string, error) {
	rendered, err := render.Template(
		tmplPath,
		map[string]string{
			"BUCKET_SCHEME":     "gs",
			"ASSET_BUCKET_NAME": "placeholder",
		},
		nil,
	)
	if err != nil {
		return nil, err
	}
	objs, err := kube.DecodeManifestBytes(rendered)
	if err != nil {
		return nil, err
	}
	if len(objs) != 1 {
		return nil, fmt.Errorf("%s decoded to %d objects, want 1", tmplPath, len(objs))
	}
	var sc atev1.SandboxConfig
	if err := k8sruntime.DefaultUnstructuredConverter.FromUnstructured(objs[0].Object, &sc); err != nil {
		return nil, fmt.Errorf("decoding %s: %w", tmplPath, err)
	}
	defaultVer, ok := sc.Spec.DefaultVersionConfig()
	if !ok {
		return nil, fmt.Errorf("%s has no enabled version matching defaultVersion %q", tmplPath, sc.Spec.DefaultVersion)
	}

	out := make(map[string]map[string]string, len(microvmArches))
	for _, arch := range microvmArches {
		archAssets, ok := defaultVer.Assets[arch]
		if !ok {
			return nil, fmt.Errorf("%s default version %q has no assets for arch %q", tmplPath, defaultVer.Name, arch)
		}
		files := make(map[string]string, len(microvmTemplateAssetFiles))
		for tmplKey, fileName := range microvmTemplateAssetFiles {
			af, ok := archAssets[tmplKey]
			if !ok || af.SHA256 == "" {
				return nil, fmt.Errorf("%s default version %q arch %q missing sha256 for %q", tmplPath, defaultVer.Name, arch, tmplKey)
			}
			wantSuffix := fmt.Sprintf("/%s/%s/%s", microvmAssetsPrefix, arch, af.SHA256)
			if !strings.HasSuffix(af.URL, wantSuffix) {
				return nil, fmt.Errorf("%s default version %q arch %q %q url %q does not end with %q", tmplPath, defaultVer.Name, arch, tmplKey, af.URL, wantSuffix)
			}
			files[fileName] = af.SHA256
		}
		out[arch] = files
	}
	return out, nil
}

// checkMicroVMDeps validates the asset bucket configuration and staging CLI
// tool before cluster mutation or archive downloads begin.
func (e *Env) checkMicroVMDeps() (string, error) {
	bucket, err := e.microVMBucket()
	if err != nil {
		return "", err
	}
	if e.Cfg.Kind {
		if _, err := exec.LookPath("docker"); err != nil {
			return "", fmt.Errorf("'docker' is required to run the S3 client but was not found in PATH: %w", err)
		}
	} else {
		if _, err := exec.LookPath("gcloud"); err != nil {
			return "", fmt.Errorf("'gcloud' is required to upload microVM assets to GCS but was not found in PATH: %w", err)
		}
	}
	return bucket, nil
}

// microVMBucket returns the bucket where microVM guest assets are staged: the
// shared rustfs bucket on Kind, and <BUCKET_NAME>-assets on GKE (where atelet
// has read access only to the asset bucket).
func (e *Env) microVMBucket() (string, error) {
	if e.Cfg.Kind {
		if e.Cfg.BucketName != "" {
			return e.Cfg.BucketName, nil
		}
		return defaultKindMicroVMBucket, nil
	}
	if e.Cfg.BucketName == "" {
		return "", fmt.Errorf("BUCKET_NAME (--storage-bucket-name or config.storage.bucketName) must be set to stage microVM assets on non-Kind clusters, or pass --sandbox-classes=gvisor")
	}
	assetBucket := e.Cfg.BucketName + "-assets"
	if len(assetBucket) > maxGCSBucketNameLength {
		return "", fmt.Errorf("bucket name %q is too long: the asset bucket %q would have %d characters, and GCS allows %d", e.Cfg.BucketName, assetBucket, len(assetBucket), maxGCSBucketNameLength)
	}
	return assetBucket, nil
}

// microVMBucketScheme returns the URL scheme for the node's object-store
// plugin: s3 on Kind (rustfs) and gs on GKE.
func (e *Env) microVMBucketScheme() string {
	if e.Cfg.Kind {
		return "s3"
	}
	return "gs"
}

func cloudHypervisorAssetName(arch string) (string, error) {
	switch arch {
	case "arm64":
		return "cloud-hypervisor-static-aarch64", nil
	case "amd64":
		return "cloud-hypervisor-static", nil
	default:
		return "", fmt.Errorf("unsupported arch %q", arch)
	}
}

func microvmAssetStamp(arch, kataVer, chVer string) string {
	return fmt.Sprintf("arch=%s\nkata=%s\ncloud-hypervisor=%s\n",
		arch, kataVer, chVer)
}

func checkMicroVMAssets(outDir string) error {
	for _, f := range microvmAssets {
		if fi, err := os.Stat(filepath.Join(outDir, f)); err != nil || !fi.Mode().IsRegular() {
			return fmt.Errorf("missing asset %s/%s", outDir, f)
		}
	}
	return nil
}

func verifyMicroVMAssetDigests(outDir string, digests map[string]string) error {
	if len(digests) == 0 {
		return fmt.Errorf("no pinned microVM asset digests")
	}
	var mismatches []string
	for _, f := range microvmAssets {
		p := filepath.Join(outDir, f)
		got, err := fileSHA256(p)
		if err != nil {
			return err
		}
		if want := digests[f]; got != want {
			mismatches = append(mismatches, fmt.Sprintf("%s: got %s, want %s", f, got, want))
		}
	}
	if len(mismatches) > 0 {
		return fmt.Errorf("microVM asset sha256 mismatch in %s (pinned in %s):\n  %s",
			outDir, microvmSandboxConfigTemplate, strings.Join(mismatches, "\n  "))
	}
	return nil
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func microvmAssetsNeedAssemble(outDir, wantStamp string, digests map[string]string) (needAssemble, stale bool) {
	if err := checkMicroVMAssets(outDir); err != nil {
		return true, false
	}
	got, err := os.ReadFile(filepath.Join(outDir, assetStampFile))
	if err != nil || string(got) != wantStamp {
		return true, true
	}
	if err := verifyMicroVMAssetDigests(outDir, digests); err != nil {
		return true, true
	}
	return false, false
}

// assembleMicroVMAssets downloads the Kata static tarball and Cloud Hypervisor
// binary for arch into outDir, verifies the SHA-256 digests against digests,
// and writes the .asset-versions stamp last.
func assembleMicroVMAssets(ctx context.Context, outDir, arch, kataVer, chVer string, digests map[string]string) error {
	chAsset, err := cloudHypervisorAssetName(arch)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("while creating %s: %w", outDir, err)
	}

	// Remove any existing stamp before mutating files in outDir so a failed
	// run leaves an unstamped directory that the next run will re-assemble.
	stampPath := filepath.Join(outDir, assetStampFile)
	_ = os.Remove(stampPath)

	workDir, err := os.MkdirTemp(outDir, ".assemble-*")
	if err != nil {
		return fmt.Errorf("while creating temporary directory in %s: %w", outDir, err)
	}
	defer func() { _ = os.RemoveAll(workDir) }()

	baseURL := strings.TrimSuffix(microvmReleaseBaseURL, "/")
	kataURL := fmt.Sprintf("%s/kata-containers/kata-containers/releases/download/%s/kata-static-%s-%s.tar.zst",
		baseURL, kataVer, kataVer, arch)
	log.Infof(">> Downloading kata-static %s (%s)...", kataVer, arch)
	if err := downloadAndExtractKataAssets(ctx, kataURL, workDir, outDir); err != nil {
		return err
	}

	chURL := fmt.Sprintf("%s/cloud-hypervisor/cloud-hypervisor/releases/download/%s/%s", baseURL, chVer, chAsset)
	log.Infof(">> Downloading cloud-hypervisor %s (%s)...", chVer, chAsset)
	chTmp := filepath.Join(workDir, "cloud-hypervisor")
	if err := downloadToFile(ctx, chURL, chTmp, 0o755); err != nil {
		return err
	}
	if err := os.Rename(chTmp, filepath.Join(outDir, "cloud-hypervisor")); err != nil {
		return err
	}

	if err := checkMicroVMAssets(outDir); err != nil {
		return err
	}
	if err := verifyMicroVMAssetDigests(outDir, digests); err != nil {
		return err
	}
	if err := os.WriteFile(stampPath, []byte(microvmAssetStamp(arch, kataVer, chVer)), 0o644); err != nil {
		return fmt.Errorf("while writing %s: %w", stampPath, err)
	}
	log.Infof(">> Assets assembled in %s.", outDir)
	return nil
}

func downloadAndExtractKataAssets(ctx context.Context, kataURL, workDir, outDir string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, kataURL, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("while downloading %s: %w", kataURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("while downloading %s: unexpected HTTP status %s", kataURL, resp.Status)
	}

	links, err := extractKataTarZst(ctx, resp.Body, workDir)
	if err != nil {
		return fmt.Errorf("while extracting %s: %w", kataURL, err)
	}

	for _, item := range []struct {
		archivePath string
		outName     string
		mode        fs.FileMode
	}{
		{archivePath: kataKernelSymlink, outName: "vmlinux", mode: 0o644},
		// rootfs.img is attached over virtio-pmem and mounted with DAX (cmd/ateworker-microvm/run.go).
		// The image must meet the following requirements:
		//   - a size that is a multiple of 2 MiB (cloud-hypervisor)
		//   - partition 1 aligned to the guest page size
		//   - a filesystem block size equal to the guest page size
		// The kata-containers.img used here is built for this, and vmlinux.container uses 4 KiB pages.
		// See https://github.com/kata-containers/kata-containers/blob/main/tools/osbuilder/image-builder/image_builder.sh
		{archivePath: kataRootfsSymlink, outName: "rootfs.img", mode: 0o644},
		{archivePath: kataLibexecVirtiofsd, outName: "virtiofsd", mode: 0o755},
	} {
		resolved := item.archivePath
		for range 8 {
			target, ok := links[resolved]
			if !ok {
				break
			}
			resolved = target
		}
		src := filepath.Join(workDir, filepath.FromSlash(resolved))
		if fi, err := os.Stat(src); err != nil || !fi.Mode().IsRegular() {
			return fmt.Errorf("while resolving %s in kata archive: missing regular file %s", item.archivePath, resolved)
		}
		if err := os.Chmod(src, item.mode); err != nil {
			return err
		}
		if err := os.Rename(src, filepath.Join(outDir, item.outName)); err != nil {
			return err
		}
	}
	return nil
}

// extractKataTarZst decompresses a zstd-compressed tar stream, writes regular
// files under opt/kata/share/kata-containers/ and opt/kata/libexec/virtiofsd
// into workDir, and returns a map of symlink paths to their cleaned archive
// targets.
func extractKataTarZst(ctx context.Context, r io.Reader, workDir string) (map[string]string, error) {
	zr, err := zstd.NewReader(bufio.NewReader(r))
	if err != nil {
		return nil, fmt.Errorf("creating zstd reader: %w", err)
	}
	defer zr.Close()

	links := map[string]string{}
	tr := tar.NewReader(zr)
	var totalBytes int64
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		hdr, err := tr.Next()
		if err == io.EOF {
			return links, nil
		}
		if err != nil {
			return nil, err
		}

		name := strings.TrimPrefix(path.Clean(hdr.Name), "/")
		if name == "." || name == "" {
			continue
		}
		if !filepath.IsLocal(filepath.FromSlash(name)) {
			return nil, fmt.Errorf("tar entry %q escapes the extraction directory", hdr.Name)
		}
		if !strings.HasPrefix(name, kataSharePrefix) && name != kataLibexecVirtiofsd {
			continue
		}

		switch hdr.Typeflag {
		case tar.TypeReg:
			totalBytes += hdr.Size
			if totalBytes > maxMicroVMAssetBytes {
				return nil, fmt.Errorf("kata archive exceeds %d-byte limit", maxMicroVMAssetBytes)
			}
			dst := filepath.Join(workDir, filepath.FromSlash(name))
			if err := writeStreamToFile(dst, 0o600, tr); err != nil {
				return nil, err
			}
		case tar.TypeSymlink:
			if path.IsAbs(hdr.Linkname) {
				return nil, fmt.Errorf("tar symlink %q has absolute target %q", hdr.Name, hdr.Linkname)
			}
			target := path.Clean(path.Join(path.Dir(name), hdr.Linkname))
			if !filepath.IsLocal(filepath.FromSlash(target)) {
				return nil, fmt.Errorf("tar symlink %q target %q escapes the extraction directory", hdr.Name, hdr.Linkname)
			}
			links[name] = target
		}
	}
}

func writeStreamToFile(dstPath string, mode fs.FileMode, r io.Reader) error {
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(dstPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	n, err := io.Copy(f, io.LimitReader(r, maxMicroVMAssetBytes+1))
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if n > maxMicroVMAssetBytes {
		return fmt.Errorf("extracted file exceeds %d-byte limit", maxMicroVMAssetBytes)
	}
	return closeErr
}

func downloadToFile(ctx context.Context, url, dstPath string, mode fs.FileMode) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("while downloading %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("while downloading %s: unexpected HTTP status %s", url, resp.Status)
	}
	return writeStreamToFile(dstPath, mode, resp.Body)
}

func (e *Env) stageMicroVMAssetsToRustfs(ctx context.Context, bucket string, digests map[string]map[string]string) error {
	ns := e.Namespace()
	timeout := e.Cfg.WaitTimeout(MicroVMStageTimeout)
	log.Infof(">> Waiting for rustfs in namespace %s...", ns)
	if err := e.Kube.RolloutStatus(ctx, kube.KindDeployment, ns, "rustfs", timeout); err != nil {
		return err
	}
	if err := e.Kube.WaitJobComplete(ctx, ns, "rustfs-bucket-init", timeout); err != nil {
		return err
	}

	node, err := e.rustfsNodeName(ctx, ns)
	if err != nil {
		return err
	}
	svc, err := e.Kube.Typed.CoreV1().Services(ns).Get(ctx, "rustfs", metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("while getting service %s/rustfs: %w", ns, err)
	}
	if svc.Spec.ClusterIP == "" || svc.Spec.ClusterIP == corev1.ClusterIPNone {
		return fmt.Errorf("service %s/rustfs has no ClusterIP", ns)
	}
	endpoint := "http://" + net.JoinHostPort(svc.Spec.ClusterIP, "9000")

	// Credentials and region are fixed in manifests/ate-install/kind/rustfs.yaml.
	awsCLIArgs := func(args ...string) []string {
		return append([]string{
			"run", "--rm", "-i",
			"--network", "container:" + node,
			"-e", "AWS_ACCESS_KEY_ID=rustfsadmin",
			"-e", "AWS_SECRET_ACCESS_KEY=rustfsadmin",
			"-e", "AWS_REGION=us-east-1",
			"-e", "AWS_ENDPOINT_URL=" + endpoint,
			rustfsAWSCLIImage,
		}, args...)
	}
	awsCLI := func(stdin io.Reader, args ...string) error {
		return e.runCmd(ctx, stdin, "docker", awsCLIArgs(args...)...)
	}

	// rustfs-bucket-init creates only defaultKindMicroVMBucket.
	if bucket != defaultKindMicroVMBucket {
		if err := awsCLI(nil, "s3api", "head-bucket", "--bucket", bucket); err != nil {
			if err := awsCLI(nil, "s3api", "create-bucket", "--bucket", bucket); err != nil {
				return fmt.Errorf("while creating bucket %s in rustfs: %w", bucket, err)
			}
		}
	}

	prefixURL := fmt.Sprintf("s3://%s/%s/", bucket, microvmAssetsPrefix)
	// s3 ls exits non-zero when the prefix has no objects yet; cp surfaces real errors.
	lsOut, _ := e.runCmdOutput(ctx, "docker", awsCLIArgs("s3", "ls", "--recursive", prefixURL)...)
	existing := fieldsSet(lsOut)

	log.Infof(">> Uploading assets to %s via %s (netns of %s)...", prefixURL, endpoint, node)
	for _, arch := range e.microVMArches() {
		outDir := e.Cfg.Path("bin", microvmAssetsPrefix, arch)
		for _, f := range microvmAssets {
			sha := digests[arch][f]
			key := fmt.Sprintf("%s/%s/%s", microvmAssetsPrefix, arch, sha)
			if existing[key] {
				log.Infof("   %s/%s (%s) already in s3://%s; skipping.", arch, f, sha, bucket)
				continue
			}
			log.Infof("   %s/%s -> %s", arch, f, key)
			file, err := os.Open(filepath.Join(outDir, f))
			if err != nil {
				return err
			}
			dst := fmt.Sprintf("s3://%s/%s", bucket, key)
			err = awsCLI(file, "s3", "cp", "-", dst)
			_ = file.Close()
			if err != nil {
				return fmt.Errorf("while uploading %s/%s to rustfs: %w", arch, f, err)
			}
		}
	}

	log.Infof(">> Done. Verify:")
	return awsCLI(nil, "s3", "ls", "--recursive", prefixURL)
}

// rustfsNodeName returns the node hosting the rustfs pod so the S3 upload
// runs in that node's network namespace, which rustfs's NetworkPolicy admits.
func (e *Env) rustfsNodeName(ctx context.Context, ns string) (string, error) {
	pods, err := e.Kube.Typed.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: "app=rustfs",
	})
	if err != nil {
		return "", fmt.Errorf("while listing rustfs pods: %w", err)
	}
	for _, p := range pods.Items {
		if p.DeletionTimestamp != nil || p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		if p.Spec.NodeName != "" {
			return p.Spec.NodeName, nil
		}
	}
	return e.kindNodeName(ctx)
}

func (e *Env) stageMicroVMAssetsToGCS(ctx context.Context, bucket string, digests map[string]map[string]string) error {
	var projectFlag []string
	if e.Cfg.ProjectID != "" {
		projectFlag = []string{"--project=" + e.Cfg.ProjectID}
	}

	prefixURL := fmt.Sprintf("gs://%s/%s/", bucket, microvmAssetsPrefix)
	lsArgs := append(append([]string{"storage", "ls", "--recursive"}, projectFlag...), prefixURL)
	// gcloud storage ls exits non-zero when the prefix has no objects yet; cp surfaces real errors.
	lsOut, _ := e.runCmdOutput(ctx, "gcloud", lsArgs...)
	existing := fieldsSet(lsOut)

	for _, arch := range e.microVMArches() {
		outDir := e.Cfg.Path("bin", microvmAssetsPrefix, arch)
		for _, f := range microvmAssets {
			sha := digests[arch][f]
			dst := fmt.Sprintf("gs://%s/%s/%s/%s", bucket, microvmAssetsPrefix, arch, sha)
			if existing[dst] {
				log.Infof("   %s/%s (%s) already in gs://%s; skipping.", arch, f, sha, bucket)
				continue
			}
			log.Infof("   %s/%s -> %s", arch, f, dst)
			args := append(append([]string{"storage", "cp"}, projectFlag...), filepath.Join(outDir, f), dst)
			if err := e.runCmd(ctx, nil, "gcloud", args...); err != nil {
				return fmt.Errorf("while uploading %s/%s to GCS: %w", arch, f, err)
			}
		}
	}

	log.Infof(">> Done. Verify:")
	return e.runCmd(ctx, nil, "gcloud", lsArgs...)
}

func fieldsSet(s string) map[string]bool {
	fields := strings.Fields(s)
	out := make(map[string]bool, len(fields))
	for _, f := range fields {
		out[f] = true
	}
	return out
}

func (e *Env) runCmd(ctx context.Context, stdin io.Reader, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	if e.Cfg != nil {
		cmd.Dir = e.Cfg.Root
		cmd.Env = e.Cfg.ScriptEnv()
	}
	cmd.Stdin = stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (e *Env) runCmdOutput(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	if e.Cfg != nil {
		cmd.Dir = e.Cfg.Root
		cmd.Env = e.Cfg.ScriptEnv()
	}
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = io.Discard
	err := cmd.Run()
	return out.String(), err
}
