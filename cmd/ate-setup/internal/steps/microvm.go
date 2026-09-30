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
	"cmp"
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
	"runtime"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/render"
)

const (
	// MicroVMStageTimeout bounds the wait for the in-cluster rustfs Deployment
	// and bucket-init Job on Kind.
	MicroVMStageTimeout = 300 * time.Second

	defaultKataVersion            = "4.1.0"
	defaultCloudHypervisorVersion = "v53.0"
	// virtiofsdVersion is the version bundled inside kata-static for
	// defaultKataVersion (not env-overridable; verified before stamping).
	virtiofsdVersion = "1.14.0"

	assetStampFile               = ".asset-versions"
	defaultMicroVMBucket         = "ate-snapshots"
	microvmSandboxConfigTemplate = "manifests/microvm/sandboxconfig-microvm.yaml.tmpl"
	microvmSandboxConfigName     = "microvm"

	// Keep in sync with manifests/ate-install/kind/rustfs.yaml.
	rustfsAWSCLIImage = "amazon/aws-cli:2.17.0@sha256:643507c10ada7964ca6157b3d799f030b90577643da9955d319a77399ed80d73"

	kataSharePrefix      = "opt/kata/share/kata-containers/"
	kataKernelSymlink    = "opt/kata/share/kata-containers/vmlinux.container"
	kataRootfsSymlink    = "opt/kata/share/kata-containers/kata-containers.img"
	kataLibexecVirtiofsd = "opt/kata/libexec/virtiofsd"

	maxMicroVMAssetBytes int64 = 8 << 30
)

var microvmAssets = []string{"cloud-hypervisor", "virtiofsd", "vmlinux", "rootfs.img"}

var sandboxConfigGVK = schema.GroupVersionKind{
	Group:   "ate.dev",
	Version: "v1alpha1",
	Kind:    "SandboxConfig",
}

// microvmReleaseBaseURL is overridden in unit tests to point at an httptest.Server.
var microvmReleaseBaseURL = "https://github.com"

// DeployMicroVMDeps installs the micro-VM prerequisites: the guest assets and
// the cluster-wide `microvm` SandboxConfig.
func (e *Env) DeployMicroVMDeps(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	log.Step("deploy_microvm_deps")

	arch := e.microvmArch()
	if _, err := cloudHypervisorAssetName(arch); err != nil {
		return err
	}
	outDir := cmp.Or(e.getenv("OUT"), e.Cfg.Path("bin", "microvm-assets", arch))
	bucket := cmp.Or(e.Cfg.BucketName, defaultMicroVMBucket)
	kataVer := cmp.Or(e.getenv("KATA_VER"), defaultKataVersion)
	chVer := cmp.Or(e.getenv("CH_VER"), defaultCloudHypervisorVersion)

	wantStamp := microvmAssetStamp(arch, kataVer, chVer)
	needAssemble, stale := microvmAssetsNeedAssemble(outDir, wantStamp)
	if stale {
		log.Infof("Asset set in %s is stale (version stamp mismatch); re-assembling.", outDir)
	}
	if needAssemble {
		log.Infof("Assembling micro-VM assets into %s (ARCH=%s)...", outDir, arch)
		if err := assembleMicroVMAssets(ctx, outDir, arch, kataVer, chVer); err != nil {
			return err
		}
	} else {
		log.Infof("Assets already present in %s; skipping assemble.", outDir)
	}

	if e.Cfg.Kind {
		log.Infof("Staging assets to in-cluster rustfs bucket %s (kata-assets/)...", bucket)
		if err := e.stageMicroVMAssetsToRustfs(ctx, outDir, bucket); err != nil {
			return err
		}
	} else {
		log.Infof("Uploading assets to gs://%s/kata-assets/ ...", bucket)
		if err := e.stageMicroVMAssetsToGCS(ctx, outDir, bucket); err != nil {
			return err
		}
	}

	log.Infof("Applying microvm SandboxConfig from %s...", microvmSandboxConfigTemplate)
	rendered, err := render.Template(
		e.Cfg.Path("manifests", "microvm", "sandboxconfig-microvm.yaml.tmpl"),
		map[string]string{"BUCKET_NAME": bucket},
		nil,
	)
	if err != nil {
		return err
	}
	if err := e.Kube.ApplyBytes(ctx, rendered); err != nil {
		return err
	}
	log.Infof("Done. ActorTemplates must reference this SandboxConfig by name (sandboxConfig.configName: microvm).")
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
	obj.SetGroupVersionKind(sandboxConfigGVK)
	obj.SetName(microvmSandboxConfigName)
	if err := e.Kube.DeleteOne(ctx, obj); err != nil {
		return err
	}
	log.Infof("Done. (Bucket assets at gs://%s/kata-assets/ left in place.)", cmp.Or(e.Cfg.BucketName, defaultMicroVMBucket))
	return nil
}

func (e *Env) microvmArch() string {
	if arch := e.getenv("ARCH"); arch != "" {
		return arch
	}
	if e.Cfg != nil && e.Cfg.KODefaultPlatforms != "" {
		if _, arch, ok := strings.Cut(e.Cfg.KODefaultPlatforms, "/"); ok {
			return arch
		}
		return e.Cfg.KODefaultPlatforms
	}
	return runtime.GOARCH
}

func (e *Env) getenv(key string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	if e.Cfg == nil {
		return ""
	}
	prefix := key + "="
	for _, kv := range e.Cfg.ScriptEnv() {
		if val, ok := strings.CutPrefix(kv, prefix); ok {
			return val
		}
	}
	return ""
}

func cloudHypervisorAssetName(arch string) (string, error) {
	switch arch {
	case "arm64":
		return "cloud-hypervisor-static-aarch64", nil
	case "amd64":
		return "cloud-hypervisor-static", nil
	default:
		return "", fmt.Errorf("unsupported ARCH=%s", arch)
	}
}

func microvmAssetStamp(arch, kataVer, chVer string) string {
	return fmt.Sprintf("arch=%s\nkata=%s\ncloud-hypervisor=%s\nvirtiofsd=%s\n",
		arch, kataVer, chVer, virtiofsdVersion)
}

func checkMicroVMAssets(outDir string) error {
	for _, f := range microvmAssets {
		if fi, err := os.Stat(filepath.Join(outDir, f)); err != nil || !fi.Mode().IsRegular() {
			return fmt.Errorf("missing asset %s/%s", outDir, f)
		}
	}
	return nil
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
// binary for arch into outDir, verifies the bundled virtiofsd version when
// runnable on the host, and writes the .asset-versions stamp last.
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
	if err := verifyVirtiofsdVersion(ctx, filepath.Join(outDir, "virtiofsd"), kataVer, virtiofsdVersion); err != nil {
		return err
	}
	if err := os.WriteFile(stampPath, []byte(microvmAssetStamp(arch, kataVer, chVer)), 0o644); err != nil {
		return fmt.Errorf("while writing %s: %w", stampPath, err)
	}

	log.Infof(">> sha256 (paste all four into the per-arch block in %s):", microvmSandboxConfigTemplate)
	for _, f := range microvmAssets {
		file, err := os.Open(filepath.Join(outDir, f))
		if err != nil {
			return err
		}
		h := sha256.New()
		_, copyErr := io.Copy(h, file)
		_ = file.Close()
		if copyErr != nil {
			return copyErr
		}
		log.Infof("%x  %s", h.Sum(nil), f)
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

func verifyVirtiofsdVersion(ctx context.Context, virtiofsdPath, kataVer, wantVersion string) error {
	out, err := exec.CommandContext(ctx, virtiofsdPath, "--version").Output()
	if err != nil {
		return ctx.Err()
	}
	firstLine, _, _ := strings.Cut(string(out), "\n")
	fields := strings.Fields(firstLine)
	if len(fields) < 2 {
		return nil
	}
	if got := fields[1]; got != wantVersion {
		return fmt.Errorf("kata %s bundles virtiofsd %s, not %s: update virtiofsdVersion", kataVer, got, wantVersion)
	}
	log.Infof("virtiofsd %s", fields[1])
	return nil
}

func (e *Env) stageMicroVMAssetsToRustfs(ctx context.Context, outDir, bucket string) error {
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

	awsCLI := func(stdin io.Reader, args ...string) error {
		dockerArgs := append([]string{
			"run", "--rm", "-i",
			"--network", "container:" + node,
			"-e", "AWS_ACCESS_KEY_ID=" + cmp.Or(e.getenv("AWS_ACCESS_KEY_ID"), "rustfsadmin"),
			"-e", "AWS_SECRET_ACCESS_KEY=" + cmp.Or(e.getenv("AWS_SECRET_ACCESS_KEY"), "rustfsadmin"),
			"-e", "AWS_REGION=" + cmp.Or(e.getenv("AWS_REGION"), "us-east-1"),
			"-e", "AWS_ENDPOINT_URL=" + endpoint,
			rustfsAWSCLIImage,
		}, args...)
		return e.runCmd(ctx, stdin, "docker", dockerArgs...)
	}

	log.Infof(">> Uploading assets to s3://%s/kata-assets/ via %s (netns of %s)...", bucket, endpoint, node)
	for _, f := range microvmAssets {
		log.Infof("   %s", f)
		file, err := os.Open(filepath.Join(outDir, f))
		if err != nil {
			return err
		}
		err = awsCLI(file, "s3", "cp", "-", fmt.Sprintf("s3://%s/kata-assets/%s", bucket, f))
		_ = file.Close()
		if err != nil {
			return fmt.Errorf("while uploading %s to rustfs: %w", f, err)
		}
	}

	log.Infof(">> Done. Verify:")
	return awsCLI(nil, "s3", "ls", fmt.Sprintf("s3://%s/kata-assets/", bucket))
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

func (e *Env) stageMicroVMAssetsToGCS(ctx context.Context, outDir, bucket string) error {
	var projectFlag []string
	if e.Cfg.ProjectID != "" {
		projectFlag = []string{"--project=" + e.Cfg.ProjectID}
	}

	for _, f := range microvmAssets {
		log.Infof("   %s", f)
		args := append(append([]string{"storage", "cp"}, projectFlag...),
			filepath.Join(outDir, f), fmt.Sprintf("gs://%s/kata-assets/%s", bucket, f))
		if err := e.runCmd(ctx, nil, "gcloud", args...); err != nil {
			return fmt.Errorf("while uploading %s to GCS: %w", f, err)
		}
	}

	log.Infof(">> Done. Verify:")
	lsArgs := append(append([]string{"storage", "ls"}, projectFlag...), fmt.Sprintf("gs://%s/kata-assets/", bucket))
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
