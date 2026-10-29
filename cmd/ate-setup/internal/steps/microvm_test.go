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
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
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

func testAssetDigests() map[string]string {
	return map[string]string{
		"cloud-hypervisor": sha256HexString("ch-bytes"),
		"virtiofsd":        sha256HexString("virtiofsd-bytes"),
		"vmlinux":          sha256HexString("vmlinux-bytes"),
		"rootfs.img":       sha256HexString("rootfs-bytes"),
	}
}

func writeValidTestAssets(t *testing.T, dir string) {
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

func TestVerifyMicroVMAssetDigests(t *testing.T) {
	digests := testAssetDigests()

	t.Run("matching digests succeed", func(t *testing.T) {
		dir := t.TempDir()
		writeValidTestAssets(t, dir)
		if err := verifyMicroVMAssetDigests(dir, digests); err != nil {
			t.Fatalf("verifyMicroVMAssetDigests() = %v, want nil", err)
		}
	})

	t.Run("mismatched digests report all mismatches at once", func(t *testing.T) {
		dir := t.TempDir()
		writeValidTestAssets(t, dir)
		if err := os.WriteFile(filepath.Join(dir, "vmlinux"), []byte("bad-kernel"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "rootfs.img"), []byte("bad-rootfs"), 0o644); err != nil {
			t.Fatal(err)
		}
		err := verifyMicroVMAssetDigests(dir, digests)
		if err == nil {
			t.Fatal("verifyMicroVMAssetDigests() = nil, want mismatch error")
		}
		for _, wantSubstr := range []string{
			"vmlinux: got " + sha256HexString("bad-kernel") + ", want " + digests["vmlinux"],
			"rootfs.img: got " + sha256HexString("bad-rootfs") + ", want " + digests["rootfs.img"],
		} {
			if !strings.Contains(err.Error(), wantSubstr) {
				t.Errorf("verifyMicroVMAssetDigests() = %q, missing %q", err, wantSubstr)
			}
		}
	})

	t.Run("missing file fails", func(t *testing.T) {
		dir := t.TempDir()
		writeValidTestAssets(t, dir)
		if err := os.Remove(filepath.Join(dir, "rootfs.img")); err != nil {
			t.Fatal(err)
		}
		if err := verifyMicroVMAssetDigests(dir, digests); err == nil {
			t.Fatal("verifyMicroVMAssetDigests() succeeded with missing file, want error")
		}
	})

	t.Run("empty digests map fails", func(t *testing.T) {
		if err := verifyMicroVMAssetDigests(t.TempDir(), nil); err == nil {
			t.Fatal("verifyMicroVMAssetDigests() succeeded with nil digests, want error")
		}
	})
}

func TestMicroVMAssetsNeedAssemble(t *testing.T) {
	stamp := microvmAssetStamp("amd64", defaultKataVersion, defaultCloudHypervisorVersion)
	const wantStamp = "arch=amd64\nkata=4.3.1\ncloud-hypervisor=v53.0\n"
	if stamp != wantStamp {
		t.Fatalf("microvmAssetStamp() =\n%q\nwant\n%q", stamp, wantStamp)
	}
	digests := testAssetDigests()

	t.Run("missing file triggers assemble without stale warning", func(t *testing.T) {
		dir := t.TempDir()
		for _, f := range microvmAssets[:3] {
			if err := os.WriteFile(filepath.Join(dir, f), []byte(f), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		need, stale := microvmAssetsNeedAssemble(dir, stamp, digests)
		if !need || stale {
			t.Errorf("microvmAssetsNeedAssemble() = (%v, %v), want (true, false)", need, stale)
		}
	})

	t.Run("missing stamp with all files present reports stale", func(t *testing.T) {
		dir := t.TempDir()
		writeValidTestAssets(t, dir)
		need, stale := microvmAssetsNeedAssemble(dir, stamp, digests)
		if !need || !stale {
			t.Errorf("microvmAssetsNeedAssemble() = (%v, %v), want (true, true)", need, stale)
		}
	})

	t.Run("mismatched stamp reports stale", func(t *testing.T) {
		dir := t.TempDir()
		writeValidTestAssets(t, dir)
		oldStamp := microvmAssetStamp("amd64", "4.0.0", defaultCloudHypervisorVersion)
		if err := os.WriteFile(filepath.Join(dir, assetStampFile), []byte(oldStamp), 0o644); err != nil {
			t.Fatal(err)
		}
		need, stale := microvmAssetsNeedAssemble(dir, stamp, digests)
		if !need || !stale {
			t.Errorf("microvmAssetsNeedAssemble() = (%v, %v), want (true, true)", need, stale)
		}
	})

	t.Run("matching stamp with corrupted file digest reports stale", func(t *testing.T) {
		dir := t.TempDir()
		writeValidTestAssets(t, dir)
		if err := os.WriteFile(filepath.Join(dir, assetStampFile), []byte(stamp), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "vmlinux"), []byte("corrupted"), 0o644); err != nil {
			t.Fatal(err)
		}
		need, stale := microvmAssetsNeedAssemble(dir, stamp, digests)
		if !need || !stale {
			t.Errorf("microvmAssetsNeedAssemble() = (%v, %v), want (true, true)", need, stale)
		}
	})

	t.Run("matching stamp and matching digests skip assemble", func(t *testing.T) {
		dir := t.TempDir()
		writeValidTestAssets(t, dir)
		if err := os.WriteFile(filepath.Join(dir, assetStampFile), []byte(stamp), 0o644); err != nil {
			t.Fatal(err)
		}
		need, stale := microvmAssetsNeedAssemble(dir, stamp, digests)
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
		case "/kata-containers/kata-containers/releases/download/4.3.1/kata-static-4.3.1-amd64.tar.zst":
			_, _ = w.Write(archive)
		case "/cloud-hypervisor/cloud-hypervisor/releases/download/v53.0/cloud-hypervisor-static":
			if failCH {
				w.Header().Set("Content-Length", "100")
				_, _ = w.Write([]byte("partial"))
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
				panic(http.ErrAbortHandler)
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
	digests := map[string]string{
		"cloud-hypervisor": sha256HexString(chBody),
		"virtiofsd":        sha256HexString(virtiofsdBody),
		"vmlinux":          sha256HexString("fake-kernel-bytes"),
		"rootfs.img":       sha256HexString("fake-rootfs-bytes"),
	}

	serveFakeReleases(t, validKataArchive(t, virtiofsdBody), chBody, false)

	outDir := t.TempDir()
	if err := assembleMicroVMAssets(t.Context(), outDir, "amd64", "4.3.1", "v53.0", digests); err != nil {
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
	if want := microvmAssetStamp("amd64", "4.3.1", "v53.0"); string(stamp) != want {
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

	if err := assembleMicroVMAssets(t.Context(), outDir, "amd64", "4.3.1", "v53.0", testAssetDigests()); err == nil {
		t.Fatal("assembleMicroVMAssets() succeeded when cloud-hypervisor download aborted mid-stream")
	}
	if _, err := os.Stat(stampPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("stamp file after failed assemble: err = %v, want ErrNotExist", err)
	}
	if _, err := os.Stat(filepath.Join(outDir, "cloud-hypervisor")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("partial cloud-hypervisor after aborted download: err = %v, want ErrNotExist", err)
	}
}

func TestAssembleMicroVMAssetsRejectsDigestMismatchBeforeStamping(t *testing.T) {
	silenceStepLog(t)

	serveFakeReleases(t, validKataArchive(t, "bad-virtiofsd-bytes"), "ch", false)

	outDir := t.TempDir()
	err := assembleMicroVMAssets(t.Context(), outDir, "amd64", "4.3.1", "v53.0", testAssetDigests())
	if err == nil || !strings.Contains(err.Error(), "sha256 mismatch") {
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
	digests, err := loadMicroVMAssetDigests(tmplPath)
	if err != nil {
		t.Fatalf("loadMicroVMAssetDigests(%s) = %v", tmplPath, err)
	}
	for _, arch := range microvmArches {
		for _, asset := range microvmAssets {
			d := digests[arch][asset]
			if len(d) != 64 {
				t.Errorf("digests[%s][%s] = %q, want 64-char sha256 hex", arch, asset, d)
			}
		}
	}
	for _, asset := range microvmAssets {
		if digests["amd64"][asset] == digests["arm64"][asset] {
			t.Errorf("amd64 and arm64 share the same digest for %s: %s", asset, digests["amd64"][asset])
		}
	}

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
					wantDigest := digests[arch][asset]
					wantURL := tc.scheme + "://" + tc.bucket + "/" + microvmAssetsPrefix + "/" + arch + "/" + wantDigest
					if !strings.Contains(string(rendered), wantURL) {
						t.Errorf("rendered manifest missing expected bucket URL %q:\n%s", wantURL, rendered)
					}
				}
			}
			objs, err := kube.DecodeManifestBytes(rendered)
			if err != nil {
				t.Fatalf("DecodeManifestBytes() = %v", err)
			}
			wantGVK := atev1.GroupVersion.WithKind("SandboxConfig")
			if len(objs) != 1 || objs[0].GroupVersionKind() != wantGVK || objs[0].GetName() != microvmSandboxConfigName {
				t.Errorf("decoded objects = %v, want single %v/%s", objs, wantGVK, microvmSandboxConfigName)
			}
		})
	}
}

func TestMicroVMArches(t *testing.T) {
	kindEnv := &Env{Cfg: &config.Config{Kind: true}}
	if got, want := kindEnv.microVMArches(), []string{runtime.GOARCH}; !reflect.DeepEqual(got, want) {
		t.Errorf("Kind microVMArches() = %v, want %v", got, want)
	}
	gkeEnv := &Env{Cfg: &config.Config{Kind: false}}
	if got, want := gkeEnv.microVMArches(), []string{"amd64", "arm64"}; !reflect.DeepEqual(got, want) {
		t.Errorf("non-Kind microVMArches() = %v, want %v", got, want)
	}
}

func TestParseSandboxClasses(t *testing.T) {
	for _, tc := range []struct {
		raw         string
		wantGvisor  bool
		wantMicroVM bool
		wantErr     bool
	}{
		{raw: "", wantGvisor: true, wantMicroVM: true},
		{raw: "gvisor,microvm", wantGvisor: true, wantMicroVM: true},
		{raw: "microvm,gvisor", wantGvisor: true, wantMicroVM: true},
		{raw: "gvisor", wantGvisor: true, wantMicroVM: false},
		{raw: "microvm", wantGvisor: false, wantMicroVM: true},
		{raw: "kata", wantErr: true},
		{raw: "gvisor,", wantErr: true},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			gvisor, microvm, err := parseSandboxClasses(tc.raw)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseSandboxClasses(%q) err = %v, wantErr %v", tc.raw, err, tc.wantErr)
			}
			if gvisor != tc.wantGvisor || microvm != tc.wantMicroVM {
				t.Errorf("parseSandboxClasses(%q) = (%v, %v), want (%v, %v)", tc.raw, gvisor, microvm, tc.wantGvisor, tc.wantMicroVM)
			}
		})
	}
}

func TestStageMicroVMAssetsToGCS(t *testing.T) {
	silenceStepLog(t)

	tmplPath := filepath.Join(repoRoot(t), microvmSandboxConfigTemplate)
	digests, err := loadMicroVMAssetDigests(tmplPath)
	if err != nil {
		t.Fatal(err)
	}

	root := t.TempDir()
	binDir := t.TempDir()
	callsFile := filepath.Join(t.TempDir(), "gcloud-calls.txt")
	existingObj := "gs://test-bucket-assets/" + microvmAssetsPrefix + "/amd64/" + digests["amd64"]["vmlinux"]
	script := "#!/bin/sh\necho \"$*\" >> " + callsFile + "\nif [ \"$2\" = \"ls\" ]; then\n  echo \"" + existingObj + "\"\nfi\n"
	if err := os.WriteFile(filepath.Join(binDir, "gcloud"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	for _, arch := range microvmArches {
		dir := filepath.Join(root, "bin", microvmAssetsPrefix, arch)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		for _, f := range microvmAssets {
			if err := os.WriteFile(filepath.Join(dir, f), []byte(f), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}

	env := &Env{Cfg: &config.Config{Root: root, BucketName: "test-bucket"}}
	if err := env.stageMicroVMAssetsToGCS(t.Context(), "test-bucket-assets", digests); err != nil {
		t.Fatalf("stageMicroVMAssetsToGCS() = %v", err)
	}

	gotBytes, err := os.ReadFile(callsFile)
	if err != nil {
		t.Fatal(err)
	}
	got := string(gotBytes)
	for _, arch := range microvmArches {
		for _, f := range microvmAssets {
			src := filepath.Join(root, "bin", microvmAssetsPrefix, arch, f)
			dst := "gs://test-bucket-assets/" + microvmAssetsPrefix + "/" + arch + "/" + digests[arch][f]
			want := "storage cp " + src + " " + dst
			if dst == existingObj {
				if strings.Contains(got, want) {
					t.Errorf("expected existing object %q to be skipped, but found %q in:\n%s", dst, want, got)
				}
				continue
			}
			if !strings.Contains(got, want) {
				t.Errorf("missing expected gcloud upload %q in:\n%s", want, got)
			}
		}
	}
}

func TestStageMicroVMAssetsToRustfs(t *testing.T) {
	silenceStepLog(t)

	tmplPath := filepath.Join(repoRoot(t), microvmSandboxConfigTemplate)
	digests, err := loadMicroVMAssetDigests(tmplPath)
	if err != nil {
		t.Fatal(err)
	}

	root := t.TempDir()
	binDir := t.TempDir()
	callsFile := filepath.Join(t.TempDir(), "docker-calls.txt")
	existingKey := microvmAssetsPrefix + "/" + runtime.GOARCH + "/" + digests[runtime.GOARCH]["vmlinux"]
	script := "#!/bin/sh\necho \"$*\" >> " + callsFile + "\ncase \"$*\" in\n  *\"s3 ls --recursive\"*)\n    echo \"2026-10-28 00:00:00 123 " + existingKey + "\"\n    ;;\nesac\n"
	if err := os.WriteFile(filepath.Join(binDir, "docker"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	archDir := filepath.Join(root, "bin", microvmAssetsPrefix, runtime.GOARCH)
	if err := os.MkdirAll(archDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range microvmAssets {
		if err := os.WriteFile(filepath.Join(archDir, f), []byte(f), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	replicas := int32(1)
	cs := fake.NewClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "kind-worker"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "kind-worker2"}},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "rustfs", Namespace: "ate-system", Generation: 1},
			Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
			Status: appsv1.DeploymentStatus{
				ObservedGeneration: 1,
				Replicas:           1,
				UpdatedReplicas:    1,
				ReadyReplicas:      1,
				AvailableReplicas:  1,
			},
		},
		&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: "rustfs-bucket-init", Namespace: "ate-system"},
			Status: batchv1.JobStatus{
				Conditions: []batchv1.JobCondition{{
					Type:   batchv1.JobComplete,
					Status: corev1.ConditionTrue,
				}},
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "rustfs-old-failed",
				Namespace: "ate-system",
				Labels:    map[string]string{"app": "rustfs"},
			},
			Spec:   corev1.PodSpec{NodeName: "kind-worker"},
			Status: corev1.PodStatus{Phase: corev1.PodFailed},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "rustfs-abc",
				Namespace: "ate-system",
				Labels:    map[string]string{"app": "rustfs"},
			},
			Spec: corev1.PodSpec{NodeName: "kind-worker2"},
		},
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "rustfs", Namespace: "ate-system"},
			Spec:       corev1.ServiceSpec{ClusterIP: "fd00:10:96::1234"},
		},
	)

	env := &Env{
		Cfg:  &config.Config{Root: root, Kind: true},
		Kube: kube.NewFromClients(nil, cs, nil, cs.Discovery()),
	}
	if err := env.stageMicroVMAssetsToRustfs(t.Context(), defaultKindMicroVMBucket, digests); err != nil {
		t.Fatalf("stageMicroVMAssetsToRustfs() = %v", err)
	}

	gotBytes, err := os.ReadFile(callsFile)
	if err != nil {
		t.Fatal(err)
	}
	got := string(gotBytes)
	if !strings.Contains(got, "--network container:kind-worker2") {
		t.Errorf("expected rustfs pod's node (kind-worker2) in docker args, got:\n%s", got)
	}
	if !strings.Contains(got, "AWS_ENDPOINT_URL=http://[fd00:10:96::1234]:9000") {
		t.Errorf("expected bracketed IPv6 endpoint URL in docker args, got:\n%s", got)
	}
	for _, f := range microvmAssets {
		key := microvmAssetsPrefix + "/" + runtime.GOARCH + "/" + digests[runtime.GOARCH][f]
		want := "s3 cp - s3://" + defaultKindMicroVMBucket + "/" + key
		if key == existingKey {
			if strings.Contains(got, want) {
				t.Errorf("expected existing object %q to be skipped, but found %q in:\n%s", key, want, got)
			}
			continue
		}
		if !strings.Contains(got, want) {
			t.Errorf("missing expected rustfs upload %q in:\n%s", want, got)
		}
	}
}

func TestPrepareMicroVMDepsRequiresBucketOnNonKind(t *testing.T) {
	silenceStepLog(t)

	cs := fake.NewClientset()
	env := &Env{
		Cfg:  &config.Config{Root: t.TempDir(), Kind: false},
		Kube: kube.NewFromClients(nil, cs, nil, cs.Discovery()),
	}
	if _, err := env.prepareMicroVMDeps(t.Context()); err == nil || !strings.Contains(err.Error(), "BUCKET_NAME") {
		t.Fatalf("prepareMicroVMDeps() on non-Kind without BucketName = %v, want BUCKET_NAME error", err)
	}
	err := env.DeploySandboxConfig(t.Context(), DefaultSandboxClasses)
	if err == nil || !strings.Contains(err.Error(), "BUCKET_NAME") {
		t.Fatalf("DeploySandboxConfig() on non-Kind without BucketName = %v, want BUCKET_NAME error", err)
	}
	if got := len(cs.Actions()); got != 0 {
		t.Errorf("DeploySandboxConfig() made %d API calls before preflight failed, want 0", got)
	}
}

func TestCheckMicroVMDepsRequiresStageTool(t *testing.T) {
	t.Setenv("PATH", t.TempDir())

	t.Run("Kind requires docker", func(t *testing.T) {
		env := &Env{Cfg: &config.Config{Kind: true}}
		if _, err := env.checkMicroVMDeps(); err == nil || !strings.Contains(err.Error(), "docker") {
			t.Fatalf("checkMicroVMDeps() on Kind without docker = %v, want docker error", err)
		}
	})

	t.Run("Non-Kind requires gcloud", func(t *testing.T) {
		env := &Env{Cfg: &config.Config{Kind: false, BucketName: "test-bucket"}}
		if _, err := env.checkMicroVMDeps(); err == nil || !strings.Contains(err.Error(), "gcloud") {
			t.Fatalf("checkMicroVMDeps() on non-Kind without gcloud = %v, want gcloud error", err)
		}
	})
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
		{
			name:       "Non-Kind accepts 56-character BucketName (63-character asset bucket)",
			cfg:        config.Config{Kind: false, BucketName: strings.Repeat("a", 56)},
			wantBucket: strings.Repeat("a", 56) + "-assets",
			wantScheme: "gs",
		},
		{
			name:       "Non-Kind rejects BucketName whose asset bucket exceeds 63 characters",
			cfg:        config.Config{Kind: false, BucketName: strings.Repeat("a", 57)},
			wantScheme: "gs",
			wantErr:    true,
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

func TestMicroVMDepsStepsAreSeparableFromContext(t *testing.T) {
	silenceStepLog(t)

	env := &Env{Cfg: &config.Config{Root: t.TempDir(), Kind: true}}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := env.prepareMicroVMDeps(ctx); err == nil {
		t.Error("prepareMicroVMDeps() with a cancelled context = nil, want an error")
	}
	if err := env.applyMicroVMSandboxConfig(ctx, &microVMPrepared{}); err == nil {
		t.Error("applyMicroVMSandboxConfig() with a cancelled context = nil, want an error")
	}
}
