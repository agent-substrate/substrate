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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
	"go.ate.dev/substrate/cmd/ate-setup/internal/log"
	"go.ate.dev/substrate/cmd/ate-setup/internal/render"
)

func silenceStepLog(t *testing.T) {
	t.Helper()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(os.Stdout) })
}

func sha256HexString(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func TestVerifyMicroVMAssetDigests(t *testing.T) {
	origDigests := microvmAssetDigests["amd64"]
	microvmAssetDigests["amd64"] = map[string]string{
		"cloud-hypervisor": sha256HexString("ch-bytes"),
		"virtiofsd":        sha256HexString("virtiofsd-bytes"),
		"vmlinux":          sha256HexString("vmlinux-bytes"),
		"rootfs.img":       sha256HexString("rootfs-bytes"),
	}
	t.Cleanup(func() { microvmAssetDigests["amd64"] = origDigests })

	writeValidAssets := func(t *testing.T, dir string) {
		t.Helper()
		for name, body := range map[string]string{
			"cloud-hypervisor": "ch-bytes",
			"virtiofsd":        "virtiofsd-bytes",
			"vmlinux":          "vmlinux-bytes",
			"rootfs.img":       "rootfs-bytes",
		} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}

	t.Run("matching digests succeed", func(t *testing.T) {
		dir := t.TempDir()
		writeValidAssets(t, dir)
		if err := verifyMicroVMAssetDigests(dir, "amd64"); err != nil {
			t.Fatalf("verifyMicroVMAssetDigests() = %v, want nil", err)
		}
	})

	t.Run("mismatched digest fails", func(t *testing.T) {
		dir := t.TempDir()
		writeValidAssets(t, dir)
		if err := os.WriteFile(filepath.Join(dir, "vmlinux"), []byte("corrupted"), 0o644); err != nil {
			t.Fatal(err)
		}
		err := verifyMicroVMAssetDigests(dir, "amd64")
		if err == nil || !strings.Contains(err.Error(), "sha256 =") {
			t.Fatalf("verifyMicroVMAssetDigests() = %v, want sha256 mismatch error", err)
		}
	})

	t.Run("missing file fails", func(t *testing.T) {
		dir := t.TempDir()
		writeValidAssets(t, dir)
		if err := os.Remove(filepath.Join(dir, "rootfs.img")); err != nil {
			t.Fatal(err)
		}
		if err := verifyMicroVMAssetDigests(dir, "amd64"); err == nil {
			t.Fatal("verifyMicroVMAssetDigests() succeeded with missing file, want error")
		}
	})

	t.Run("unknown arch fails", func(t *testing.T) {
		if err := verifyMicroVMAssetDigests(t.TempDir(), "riscv64"); err == nil {
			t.Fatal("verifyMicroVMAssetDigests() succeeded with unknown arch, want error")
		}
	})
}

func TestMicroVMAssetsNeedAssemble(t *testing.T) {
	stamp := microvmAssetStamp("amd64", defaultKataVersion, defaultCloudHypervisorVersion)
	const wantStamp = "arch=amd64\nkata=4.1.0\ncloud-hypervisor=v53.0\n"
	if stamp != wantStamp {
		t.Fatalf("microvmAssetStamp() =\n%q\nwant\n%q", stamp, wantStamp)
	}

	writeAllAssets := func(t *testing.T, dir string) {
		t.Helper()
		for _, f := range microvmAssets {
			if err := os.WriteFile(filepath.Join(dir, f), []byte(f), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}

	t.Run("missing file triggers assemble without stale warning", func(t *testing.T) {
		dir := t.TempDir()
		for _, f := range microvmAssets[:3] {
			if err := os.WriteFile(filepath.Join(dir, f), []byte(f), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		need, stale := microvmAssetsNeedAssemble(dir, stamp)
		if !need || stale {
			t.Errorf("microvmAssetsNeedAssemble() = (%v, %v), want (true, false)", need, stale)
		}
	})

	t.Run("missing stamp with all files present reports stale", func(t *testing.T) {
		dir := t.TempDir()
		writeAllAssets(t, dir)
		need, stale := microvmAssetsNeedAssemble(dir, stamp)
		if !need || !stale {
			t.Errorf("microvmAssetsNeedAssemble() = (%v, %v), want (true, true)", need, stale)
		}
	})

	t.Run("mismatched stamp reports stale", func(t *testing.T) {
		dir := t.TempDir()
		writeAllAssets(t, dir)
		oldStamp := microvmAssetStamp("amd64", "4.0.0", defaultCloudHypervisorVersion)
		if err := os.WriteFile(filepath.Join(dir, assetStampFile), []byte(oldStamp), 0o644); err != nil {
			t.Fatal(err)
		}
		need, stale := microvmAssetsNeedAssemble(dir, stamp)
		if !need || !stale {
			t.Errorf("microvmAssetsNeedAssemble() = (%v, %v), want (true, true)", need, stale)
		}
	})

	t.Run("matching stamp skips assemble", func(t *testing.T) {
		dir := t.TempDir()
		writeAllAssets(t, dir)
		if err := os.WriteFile(filepath.Join(dir, assetStampFile), []byte(stamp), 0o644); err != nil {
			t.Fatal(err)
		}
		need, stale := microvmAssetsNeedAssemble(dir, stamp)
		if need || stale {
			t.Errorf("microvmAssetsNeedAssemble() = (%v, %v), want (false, false)", need, stale)
		}
	})
}

type tarTestEntry struct {
	name     string
	typeflag byte
	linkname string
	body     string
	mode     int64
}

func buildKataTarZst(t *testing.T, entries []tarTestEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	tw := tar.NewWriter(zw)
	for _, e := range entries {
		mode := e.mode
		if mode == 0 {
			mode = 0o644
		}
		hdr := &tar.Header{
			Name:     e.name,
			Typeflag: e.typeflag,
			Linkname: e.linkname,
			Size:     int64(len(e.body)),
			Mode:     mode,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if e.typeflag == tar.TypeReg {
			if _, err := tw.Write([]byte(e.body)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func validKataArchive(t *testing.T, virtiofsdScript string) []byte {
	t.Helper()
	// Place the vmlinux.container symlink BEFORE its target file to verify
	// extraction does not depend on entry order in the tar stream.
	return buildKataTarZst(t, []tarTestEntry{
		{
			name:     "opt/kata/share/kata-containers/vmlinux.container",
			typeflag: tar.TypeSymlink,
			linkname: "vmlinux-6.12.47-173",
		},
		{
			name:     "opt/kata/share/kata-containers/vmlinux-6.12.47-173",
			typeflag: tar.TypeReg,
			body:     "fake-kernel-bytes",
		},
		{
			name:     "opt/kata/share/kata-containers/kata-containers-ubuntu.img",
			typeflag: tar.TypeReg,
			body:     "fake-rootfs-bytes",
		},
		{
			name:     "opt/kata/share/kata-containers/kata-containers.img",
			typeflag: tar.TypeSymlink,
			linkname: "kata-containers-ubuntu.img",
		},
		{
			name:     "opt/kata/libexec/virtiofsd",
			typeflag: tar.TypeReg,
			body:     virtiofsdScript,
			mode:     0o755,
		},
		{
			name:     "opt/kata/bin/qemu-system-x86_64",
			typeflag: tar.TypeReg,
			body:     "ignored-qemu-binary",
		},
	})
}

func serveFakeReleases(t *testing.T, archive []byte, chBody string, failCH bool) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/kata-containers/kata-containers/releases/download/4.1.0/kata-static-4.1.0-amd64.tar.zst":
			_, _ = w.Write(archive)
		case "/cloud-hypervisor/cloud-hypervisor/releases/download/v53.0/cloud-hypervisor-static":
			if failCH {
				http.Error(w, "boom", http.StatusInternalServerError)
				return
			}
			_, _ = w.Write([]byte(chBody))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	origURL := microvmReleaseBaseURL
	microvmReleaseBaseURL = srv.URL
	t.Cleanup(func() { microvmReleaseBaseURL = origURL })
}

func TestAssembleMicroVMAssets(t *testing.T) {
	silenceStepLog(t)

	const virtiofsdBody = "fake-virtiofsd-binary"
	const chBody = "fake-cloud-hypervisor-binary"
	origDigests := microvmAssetDigests["amd64"]
	microvmAssetDigests["amd64"] = map[string]string{
		"cloud-hypervisor": sha256HexString(chBody),
		"virtiofsd":        sha256HexString(virtiofsdBody),
		"vmlinux":          sha256HexString("fake-kernel-bytes"),
		"rootfs.img":       sha256HexString("fake-rootfs-bytes"),
	}
	t.Cleanup(func() { microvmAssetDigests["amd64"] = origDigests })

	serveFakeReleases(t, validKataArchive(t, virtiofsdBody), chBody, false)

	outDir := t.TempDir()
	if err := assembleMicroVMAssets(t.Context(), outDir, "amd64", "4.1.0", "v53.0"); err != nil {
		t.Fatalf("assembleMicroVMAssets() = %v", err)
	}

	for name, wantBody := range map[string]string{
		"vmlinux":          "fake-kernel-bytes",
		"rootfs.img":       "fake-rootfs-bytes",
		"virtiofsd":        virtiofsdBody,
		"cloud-hypervisor": chBody,
	} {
		got, err := os.ReadFile(filepath.Join(outDir, name))
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}
		if string(got) != wantBody {
			t.Errorf("%s content = %q, want %q", name, got, wantBody)
		}
	}

	for _, execName := range []string{"virtiofsd", "cloud-hypervisor"} {
		fi, err := os.Stat(filepath.Join(outDir, execName))
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm()&0o111 == 0 {
			t.Errorf("%s mode = %v, want executable bit set", execName, fi.Mode())
		}
	}

	stamp, err := os.ReadFile(filepath.Join(outDir, assetStampFile))
	if err != nil {
		t.Fatalf("reading %s: %v", assetStampFile, err)
	}
	if want := microvmAssetStamp("amd64", "4.1.0", "v53.0"); string(stamp) != want {
		t.Errorf("stamp = %q, want %q", stamp, want)
	}
}

func TestAssembleMicroVMAssetsClearsStampOnFailure(t *testing.T) {
	silenceStepLog(t)

	serveFakeReleases(t, validKataArchive(t, "fake-virtiofsd-binary"), "", true)

	outDir := t.TempDir()
	stampPath := filepath.Join(outDir, assetStampFile)
	if err := os.WriteFile(stampPath, []byte("pre-existing-stamp\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := assembleMicroVMAssets(t.Context(), outDir, "amd64", "4.1.0", "v53.0"); err == nil {
		t.Fatal("assembleMicroVMAssets() succeeded when cloud-hypervisor download returned 500")
	}
	if _, err := os.Stat(stampPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("stamp file after failed assemble: err = %v, want ErrNotExist", err)
	}
}

func TestAssembleMicroVMAssetsRejectsDigestMismatchBeforeStamping(t *testing.T) {
	silenceStepLog(t)

	serveFakeReleases(t, validKataArchive(t, "bad-virtiofsd-bytes"), "ch", false)

	outDir := t.TempDir()
	err := assembleMicroVMAssets(t.Context(), outDir, "amd64", "4.1.0", "v53.0")
	if err == nil || !strings.Contains(err.Error(), "sha256 =") {
		t.Fatalf("assembleMicroVMAssets() = %v, want sha256 mismatch error", err)
	}
	if _, statErr := os.Stat(filepath.Join(outDir, assetStampFile)); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("stamp written despite digest mismatch: %v", statErr)
	}
}

func TestExtractKataTarZstRejectsTraversal(t *testing.T) {
	for _, tc := range []struct {
		name  string
		entry tarTestEntry
	}{
		{
			name: "dotdot path",
			entry: tarTestEntry{
				name:     "opt/kata/share/kata-containers/../../../../../etc/passwd",
				typeflag: tar.TypeReg,
				body:     "bad",
			},
		},
		{
			name: "symlink escaping workDir",
			entry: tarTestEntry{
				name:     "opt/kata/share/kata-containers/vmlinux.container",
				typeflag: tar.TypeSymlink,
				linkname: "../../../../../../etc/passwd",
			},
		},
		{
			name: "absolute symlink target",
			entry: tarTestEntry{
				name:     "opt/kata/share/kata-containers/vmlinux.container",
				typeflag: tar.TypeSymlink,
				linkname: "/etc/passwd",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := buildKataTarZst(t, []tarTestEntry{tc.entry})
			_, err := extractKataTarZst(t.Context(), bytes.NewReader(data), t.TempDir())
			if err == nil {
				t.Fatal("extractKataTarZst() succeeded on escaping entry, want error")
			}
		})
	}
}

func TestMicroVMSandboxConfigTemplate(t *testing.T) {
	tmplPath := filepath.Join(repoRoot(t), microvmSandboxConfigTemplate)
	for _, tc := range []struct {
		scheme string
		bucket string
	}{
		{scheme: "gs", bucket: "my-test-bucket"},
		{scheme: "s3", bucket: defaultKindMicroVMBucket},
	} {
		t.Run(tc.scheme, func(t *testing.T) {
			rendered, err := render.Template(tmplPath, map[string]string{
				"BUCKET_SCHEME":     tc.scheme,
				"ASSET_BUCKET_NAME": tc.bucket,
			}, nil)
			if err != nil {
				t.Fatalf("render.Template(%s) = %v", tmplPath, err)
			}
			if strings.Contains(string(rendered), "${") {
				t.Errorf("rendered manifest still contains unexpanded placeholder:\n%s", rendered)
			}
			for _, arch := range microvmArches {
				for _, asset := range microvmAssets {
					wantURL := tc.scheme + "://" + tc.bucket + "/kata-assets/" + arch + "/" + asset
					if !strings.Contains(string(rendered), wantURL) {
						t.Errorf("rendered manifest missing expected bucket URL %q:\n%s", wantURL, rendered)
					}
					wantDigest := microvmAssetDigests[arch][asset]
					if !strings.Contains(string(rendered), wantDigest) {
						t.Errorf("rendered manifest missing expected %s/%s digest %q:\n%s", arch, asset, wantDigest, rendered)
					}
				}
			}
			objs, err := kube.DecodeManifestBytes(rendered)
			if err != nil {
				t.Fatalf("DecodeManifestBytes() = %v", err)
			}
			if len(objs) != 1 || objs[0].GetKind() != "SandboxConfig" || objs[0].GetName() != microvmSandboxConfigName {
				t.Errorf("decoded objects = %v, want single SandboxConfig/%s", objs, microvmSandboxConfigName)
			}
		})
	}
}

func TestDeployMicroVMDepsRequiresBucketOnNonKind(t *testing.T) {
	silenceStepLog(t)

	env := &Env{Cfg: &config.Config{Root: t.TempDir(), Kind: false}}
	err := env.DeployMicroVMDeps(t.Context())
	if err == nil || !strings.Contains(err.Error(), "BUCKET_NAME") {
		t.Fatalf("DeployMicroVMDeps() on non-Kind without BucketName = %v, want BUCKET_NAME error", err)
	}
}

func TestMicroVMBucket(t *testing.T) {
	for _, tc := range []struct {
		name       string
		cfg        config.Config
		wantBucket string
		wantScheme string
		wantErr    bool
	}{
		{
			name:       "Kind defaults to ate-snapshots over s3",
			cfg:        config.Config{Kind: true},
			wantBucket: defaultKindMicroVMBucket,
			wantScheme: "s3",
		},
		{
			name:       "Kind honors explicit BucketName",
			cfg:        config.Config{Kind: true, BucketName: "custom-kind-bucket"},
			wantBucket: "custom-kind-bucket",
			wantScheme: "s3",
		},
		{
			name:       "Non-Kind requires explicit BucketName",
			cfg:        config.Config{Kind: false},
			wantScheme: "gs",
			wantErr:    true,
		},
		{
			name:       "Non-Kind uses <BucketName>-assets over gs",
			cfg:        config.Config{Kind: false, BucketName: "my-gke-bucket"},
			wantBucket: "my-gke-bucket-assets",
			wantScheme: "gs",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := &Env{Cfg: &tc.cfg}
			gotBucket, err := env.microVMBucket()
			if (err != nil) != tc.wantErr {
				t.Fatalf("microVMBucket() error = %v, wantErr %v", err, tc.wantErr)
			}
			if gotBucket != tc.wantBucket {
				t.Errorf("microVMBucket() = %q, want %q", gotBucket, tc.wantBucket)
			}
			if gotScheme := env.microVMBucketScheme(); gotScheme != tc.wantScheme {
				t.Errorf("microVMBucketScheme() = %q, want %q", gotScheme, tc.wantScheme)
			}
		})
	}
}

func TestWaitJobComplete(t *testing.T) {
	silenceStepLog(t)

	t.Run("returns nil when JobComplete is True", func(t *testing.T) {
		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Namespace: NamespaceAteSystem, Name: "rustfs-bucket-init"},
			Status: batchv1.JobStatus{
				Conditions: []batchv1.JobCondition{
					{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
				},
			},
		}
		e := &Env{Kube: fakeKube(t, job)}
		if err := e.waitJobComplete(t.Context(), NamespaceAteSystem, "rustfs-bucket-init", time.Second); err != nil {
			t.Errorf("waitJobComplete() = %v, want nil", err)
		}
	})

	t.Run("returns error immediately when JobFailed is True", func(t *testing.T) {
		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Namespace: NamespaceAteSystem, Name: "rustfs-bucket-init"},
			Status: batchv1.JobStatus{
				Conditions: []batchv1.JobCondition{
					{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Message: "BackoffLimitExceeded"},
				},
			},
		}
		e := &Env{Kube: fakeKube(t, job)}
		err := e.waitJobComplete(t.Context(), NamespaceAteSystem, "rustfs-bucket-init", time.Second)
		if err == nil || !strings.Contains(err.Error(), "BackoffLimitExceeded") {
			t.Errorf("waitJobComplete() = %v, want BackoffLimitExceeded error", err)
		}
	})
}

func TestMicroVMDepsStepsAreSeparableFromContext(t *testing.T) {
	silenceStepLog(t)

	env := &Env{Cfg: &config.Config{Root: t.TempDir(), Kind: true}}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := env.DeployMicroVMDeps(ctx); err == nil {
		t.Error("DeployMicroVMDeps() with a cancelled context = nil, want an error")
	}
	if err := env.DeleteMicroVMDeps(ctx); err == nil {
		t.Error("DeleteMicroVMDeps() with a cancelled context = nil, want an error")
	}
}
