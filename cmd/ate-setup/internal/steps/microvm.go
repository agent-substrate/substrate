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
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"

	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
	"go.ate.dev/substrate/cmd/ate-setup/internal/log"
	"go.ate.dev/substrate/cmd/ate-setup/internal/render"
)

const (
	// MicroVMStageTimeout bounds the wait for the in-cluster rustfs Deployment
	// and bucket-init Job on Kind.
	MicroVMStageTimeout = 300 * time.Second

	defaultMicroVMAssetVersion    = "v1"
	defaultKataVersion            = "4.1.0"
	defaultCloudHypervisorVersion = "v53.0"

	assetStampFile               = ".asset-versions"
	defaultKindMicroVMBucket     = "ate-snapshots"
	maxGCSBucketNameLength       = 63
	microvmAssetsPrefix          = "microvm-assets"
	microvmSandboxConfigTemplate = "manifests/ate-install/sandboxconfig-microvm.yaml.tmpl"
	microvmSandboxConfigName     = "microvm"

	// Keep in sync with manifests/ate-install/kind/rustfs.yaml.
	rustfsAWSCLIImage = "amazon/aws-cli:2.17.0@sha256:643507c10ada7964ca6157b3d799f030b90577643da9955d319a77399ed80d73"

	kataSharePrefix      = "opt/kata/share/kata-containers/"
	kataKernelSymlink    = "opt/kata/share/kata-containers/vmlinux.container"
	kataRootfsSymlink    = "opt/kata/share/kata-containers/kata-containers.img"
	kataLibexecVirtiofsd = "opt/kata/libexec/virtiofsd"

	maxMicroVMAssetBytes int64 = 8 << 30
)

var (
	microvmArches = []string{"amd64", "arm64"}
	microvmAssets = []string{"cloud-hypervisor", "virtiofsd", "vmlinux", "rootfs.img"}

	// microvmAssetDigests pins the expected sha256 digest of each assembled
	// file in microvmAssets, matching manifests/ate-install/sandboxconfig-microvm.yaml.tmpl.
	microvmAssetDigests = map[string]map[string]string{
		"arm64": {
			"cloud-hypervisor": "f192b510eea1c710cbc439d716bb0573c223fc463dbe3e6523788a2b7ef62850",
			"virtiofsd":        "3aa9240045d2261930ef04138fe05cf288f94c867e63e5d0845758bac5e3d458",
			"vmlinux":          "4960f6cf781274eaa445fc9f88bb8c5ce951649278bc9caa13afb1e99cfe80e6",
			"rootfs.img":       "7e3bdd500eb92de17c377bd84f8ed57e50d6bb68c7ec08c88f5e1916cd49f30d",
		},
		"amd64": {
			"cloud-hypervisor": "448af3d4e59b22c2987f7df94c213ad40fb53a10d437e42b5ee6c4fce7c29ecc",
			"virtiofsd":        "15b2e72a78cc08a9bd8a6943e89fb69c88cb3cbeb63069efceade835342ac7d4",
			"vmlinux":          "8e9dbc3a6e4c26adb089d23d31201edebce905c7f3e31aa67b32bdd41df1b86f",
			"rootfs.img":       "96497f64da1de9c7473fef46c3d29ddd0805d334731cf9d903a21a5b2c33cefb",
		},
	}
)

// microvmReleaseBaseURL is overridden in unit tests to point at an httptest.Server.
var microvmReleaseBaseURL = "https://github.com"

// DeployMicroVMDeps installs the microVM prerequisites: the guest assets for
// each supported architecture and the cluster-wide `microvm` SandboxConfig.
func (e *Env) DeployMicroVMDeps(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	log.Step("deploy_microvm_deps")

	bucket, err := e.microVMBucket()
	if err != nil {
		return err
	}

	for _, arch := range microvmArches {
		outDir := e.Cfg.Path("bin", microvmAssetsPrefix, defaultMicroVMAssetVersion, arch)
		wantStamp := microvmAssetStamp(arch, defaultKataVersion, defaultCloudHypervisorVersion)
		needAssemble, stale := microvmAssetsNeedAssemble(outDir, wantStamp)
		if stale {
			log.Infof("Asset set in %s is stale (version stamp mismatch); re-assembling.", outDir)
		}
		if needAssemble {
			log.Infof("Assembling microVM assets into %s (arch=%s)...", outDir, arch)
			if err := assembleMicroVMAssets(ctx, outDir, arch, defaultKataVersion, defaultCloudHypervisorVersion); err != nil {
				return err
			}
		} else {
			log.Infof("Assets already present in %s; skipping assemble.", outDir)
			if err := verifyMicroVMAssetDigests(outDir, arch); err != nil {
				return err
			}
		}
	}

	if e.Cfg.Kind {
		log.Infof("Staging assets to in-cluster rustfs bucket %s (%s/%s/)...", bucket, microvmAssetsPrefix, defaultMicroVMAssetVersion)
		if err := e.stageMicroVMAssetsToRustfs(ctx, bucket); err != nil {
			return err
		}
	} else {
		log.Infof("Uploading assets to gs://%s/%s/%s/ ...", bucket, microvmAssetsPrefix, defaultMicroVMAssetVersion)
		if err := e.stageMicroVMAssetsToGCS(ctx, bucket); err != nil {
			return err
		}
	}

	log.Infof("Applying microvm SandboxConfig from %s...", microvmSandboxConfigTemplate)
	rendered, err := render.Template(
		e.Cfg.Manifest("sandboxconfig-microvm.yaml.tmpl"),
		map[string]string{
			"BUCKET_SCHEME":     e.microVMBucketScheme(),
			"ASSET_BUCKET_NAME": bucket,
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

// DeleteMicroVMDeps removes the cluster-wide `microvm` SandboxConfig, leaving
// staged bucket assets in place.
func (e *Env) DeleteMicroVMDeps(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	log.Step("delete_microvm_deps")
	log.Infof("Deleting microvm SandboxConfig...")

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(atev1.GroupVersion.WithKind("SandboxConfig"))
	obj.SetName(microvmSandboxConfigName)
	if err := e.Kube.DeleteOne(ctx, obj); err != nil {
		return err
	}
	if bucket, err := e.microVMBucket(); err == nil {
		log.Infof("Done. (Bucket assets at %s://%s/%s/ left in place.)", e.microVMBucketScheme(), bucket, microvmAssetsPrefix)
	} else {
		log.Infof("Done.")
	}
	return nil
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
		return "", fmt.Errorf("BUCKET_NAME (--storage-bucket-name or config.storage.bucketName) must be set to stage microVM assets on non-Kind clusters, or pass --skip-microvm-deps")
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

func verifyMicroVMAssetDigests(outDir, arch string) error {
	digests, ok := microvmAssetDigests[arch]
	if !ok {
		return fmt.Errorf("no pinned microVM asset digests for arch %q", arch)
	}
	for _, f := range microvmAssets {
		p := filepath.Join(outDir, f)
		got, err := fileSHA256(p)
		if err != nil {
			return err
		}
		if want := digests[f]; got != want {
			return fmt.Errorf("microVM asset %s sha256 = %s, want %s (pinned in %s)", p, got, want, microvmSandboxConfigTemplate)
		}
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

func microvmAssetsNeedAssemble(outDir, wantStamp string) (needAssemble, stale bool) {
	if err := checkMicroVMAssets(outDir); err != nil {
		return true, false
	}
	got, err := os.ReadFile(filepath.Join(outDir, assetStampFile))
	if err != nil || string(got) != wantStamp {
		return true, true
	}
	return false, false
}

// assembleMicroVMAssets downloads the Kata static tarball and Cloud Hypervisor
// binary for arch into outDir, verifies the SHA-256 digests against
// microvmAssetDigests, and writes the .asset-versions stamp last.
func assembleMicroVMAssets(ctx context.Context, outDir, arch, kataVer, chVer string) error {
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
	if err := downloadToFile(ctx, chURL, filepath.Join(outDir, "cloud-hypervisor"), 0o755); err != nil {
		return err
	}

	log.Infof(">> Assets assembled in %s:", outDir)
	if err := checkMicroVMAssets(outDir); err != nil {
		return err
	}
	if err := verifyMicroVMAssetDigests(outDir, arch); err != nil {
		return err
	}
	if err := os.WriteFile(stampPath, []byte(microvmAssetStamp(arch, kataVer, chVer)), 0o644); err != nil {
		return fmt.Errorf("while writing %s: %w", stampPath, err)
	}

	log.Infof(">> sha256 (paste all four into the %s block in %s):", arch, microvmSandboxConfigTemplate)
	for _, f := range microvmAssets {
		sum, err := fileSHA256(filepath.Join(outDir, f))
		if err != nil {
			return err
		}
		log.Infof("%s  %s", sum, f)
	}
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

func (e *Env) stageMicroVMAssetsToRustfs(ctx context.Context, bucket string) error {
	if _, err := exec.LookPath("docker"); err != nil {
		return fmt.Errorf("'docker' is required to run the S3 client but was not found in PATH: %w", err)
	}

	ns := e.Namespace()
	timeout := e.Cfg.WaitTimeout(MicroVMStageTimeout)
	log.Infof(">> Waiting for rustfs in namespace %s...", ns)
	if err := e.Kube.RolloutStatus(ctx, kube.KindDeployment, ns, "rustfs", timeout); err != nil {
		return err
	}
	if err := e.waitJobComplete(ctx, ns, "rustfs-bucket-init", timeout); err != nil {
		return err
	}

	node, err := e.kindNodeName(ctx)
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
	endpoint := fmt.Sprintf("http://%s:9000", svc.Spec.ClusterIP)

	// Credentials and region are fixed in manifests/ate-install/kind/rustfs.yaml.
	awsCLI := func(stdin io.Reader, args ...string) error {
		dockerArgs := append([]string{
			"run", "--rm", "-i",
			"--network", "container:" + node,
			"-e", "AWS_ACCESS_KEY_ID=rustfsadmin",
			"-e", "AWS_SECRET_ACCESS_KEY=rustfsadmin",
			"-e", "AWS_REGION=us-east-1",
			"-e", "AWS_ENDPOINT_URL=" + endpoint,
			rustfsAWSCLIImage,
		}, args...)
		return e.runCmd(ctx, stdin, "docker", dockerArgs...)
	}

	log.Infof(">> Uploading assets to s3://%s/%s/%s/ via %s (netns of %s)...", bucket, microvmAssetsPrefix, defaultMicroVMAssetVersion, endpoint, node)
	for _, arch := range microvmArches {
		outDir := e.Cfg.Path("bin", microvmAssetsPrefix, defaultMicroVMAssetVersion, arch)
		for _, f := range microvmAssets {
			log.Infof("   %s/%s", arch, f)
			file, err := os.Open(filepath.Join(outDir, f))
			if err != nil {
				return err
			}
			dst := fmt.Sprintf("s3://%s/%s/%s/%s/%s", bucket, microvmAssetsPrefix, defaultMicroVMAssetVersion, arch, f)
			err = awsCLI(file, "s3", "cp", "-", dst)
			_ = file.Close()
			if err != nil {
				return fmt.Errorf("while uploading %s/%s to rustfs: %w", arch, f, err)
			}
		}
	}

	log.Infof(">> Done. Verify:")
	return awsCLI(nil, "s3", "ls", "--recursive", fmt.Sprintf("s3://%s/%s/", bucket, microvmAssetsPrefix))
}

func (e *Env) waitJobComplete(ctx context.Context, namespace, name string, timeout time.Duration) error {
	defer log.Elapsed(time.Now(), fmt.Sprintf("wait for job/%s", name))
	return wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		job, err := e.Kube.Typed.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		for _, cond := range job.Status.Conditions {
			if cond.Type == batchv1.JobComplete && cond.Status == corev1.ConditionTrue {
				return true, nil
			}
			if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
				return false, fmt.Errorf("job %s/%s failed: %s", namespace, name, cond.Message)
			}
		}
		return false, nil
	})
}

func (e *Env) stageMicroVMAssetsToGCS(ctx context.Context, bucket string) error {
	var projectFlag []string
	if e.Cfg.ProjectID != "" {
		projectFlag = []string{"--project=" + e.Cfg.ProjectID}
	}

	for _, arch := range microvmArches {
		outDir := e.Cfg.Path("bin", microvmAssetsPrefix, defaultMicroVMAssetVersion, arch)
		for _, f := range microvmAssets {
			log.Infof("   %s/%s", arch, f)
			dst := fmt.Sprintf("gs://%s/%s/%s/%s/%s", bucket, microvmAssetsPrefix, defaultMicroVMAssetVersion, arch, f)
			args := append(append([]string{"storage", "cp"}, projectFlag...), filepath.Join(outDir, f), dst)
			if err := e.runCmd(ctx, nil, "gcloud", args...); err != nil {
				return fmt.Errorf("while uploading %s/%s to GCS: %w", arch, f, err)
			}
		}
	}

	log.Infof(">> Done. Verify:")
	lsArgs := append(append([]string{"storage", "ls", "--recursive"}, projectFlag...), fmt.Sprintf("gs://%s/%s/", bucket, microvmAssetsPrefix))
	return e.runCmd(ctx, nil, "gcloud", lsArgs...)
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
