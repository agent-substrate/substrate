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
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/render"
)

func silenceStepLog(t *testing.T) {
	t.Helper()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(os.Stdout) })
}

func TestMicroVMConfigResolution(t *testing.T) {
	t.Run("ARCH env beats KO_DEFAULTPLATFORMS and GOARCH", func(t *testing.T) {
		t.Setenv("ARCH", "arm64")
		e := &Env{Cfg: &config.Config{KODefaultPlatforms: "linux/amd64"}}
		if got := e.microvmArch(); got != "arm64" {
			t.Errorf("microvmArch() = %q, want arm64", got)
		}
	})

	t.Run("KO_DEFAULTPLATFORMS suffix beats GOARCH", func(t *testing.T) {
		t.Setenv("ARCH", "")
		e := &Env{Cfg: &config.Config{KODefaultPlatforms: "linux/arm64"}}
		if got := e.microvmArch(); got != "arm64" {
			t.Errorf("microvmArch() = %q, want arm64", got)
		}
	})

	t.Run("falls back to runtime.GOARCH", func(t *testing.T) {
		t.Setenv("ARCH", "")
		e := &Env{Cfg: &config.Config{}}
		if got := e.microvmArch(); got != runtime.GOARCH {
			t.Errorf("microvmArch() = %q, want %q", got, runtime.GOARCH)
		}
	})
}

func TestMicroVMAssetsNeedAssemble(t *testing.T) {
	stamp := microvmAssetStamp("amd64", defaultKataVersion, defaultCloudHypervisorVersion)
	const wantStamp = "arch=amd64\nkata=4.1.0\ncloud-hypervisor=v53.0\nvirtiofsd=1.14.0\n"
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

	virtiofsdBody := "#!/bin/sh\necho 'virtiofsd " + virtiofsdVersion + "'\n"
	const chBody = "fake-cloud-hypervisor-binary"
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

	serveFakeReleases(t, validKataArchive(t, "#!/bin/sh\necho 'virtiofsd "+virtiofsdVersion+"'\n"), "", true)

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

func TestAssembleMicroVMAssetsRejectsVirtiofsdVersionMismatch(t *testing.T) {
	silenceStepLog(t)

	serveFakeReleases(t, validKataArchive(t, "#!/bin/sh\necho 'virtiofsd 1.13.3'\n"), "ch", false)

	outDir := t.TempDir()
	err := assembleMicroVMAssets(t.Context(), outDir, "amd64", "4.1.0", "v53.0")
	if err == nil || !strings.Contains(err.Error(), "1.13.3") {
		t.Fatalf("assembleMicroVMAssets() = %v, want virtiofsd 1.13.3 mismatch error", err)
	}
	if _, statErr := os.Stat(filepath.Join(outDir, assetStampFile)); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("stamp written despite virtiofsd mismatch: %v", statErr)
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
	rendered, err := render.Template(tmplPath, map[string]string{"BUCKET_NAME": "my-test-bucket"}, nil)
	if err != nil {
		t.Fatalf("render.Template(%s) = %v", tmplPath, err)
	}
	if strings.Contains(string(rendered), "${") {
		t.Errorf("rendered manifest still contains unexpanded placeholder:\n%s", rendered)
	}
	if !strings.Contains(string(rendered), "gs://my-test-bucket/kata-assets/cloud-hypervisor") {
		t.Errorf("rendered manifest missing expected bucket URL:\n%s", rendered)
	}
	objs, err := kube.DecodeManifestBytes(rendered)
	if err != nil {
		t.Fatalf("DecodeManifestBytes() = %v", err)
	}
	if len(objs) != 1 || objs[0].GetKind() != "SandboxConfig" || objs[0].GetName() != microvmSandboxConfigName {
		t.Errorf("decoded objects = %v, want single SandboxConfig/%s", objs, microvmSandboxConfigName)
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

// Deploying benchmarks onto micro-VM has to bring the SandboxConfig with it:
// the workloads reference it by name.
func TestDeployBenchmarksInstallsMicroVMDeps(t *testing.T) {
	silenceStepLog(t)

	// Passing an unsupported ARCH makes DeployMicroVMDeps return an error
	// immediately, proving DeployBenchmarks invoked it before deploy_locust.sh.
	t.Setenv("ARCH", "unsupported-arch")
	env := &Env{Cfg: &config.Config{Root: t.TempDir()}}

	err := env.DeployBenchmarks(t.Context(), BenchmarkOptions{
		WorkerCount:  1,
		SandboxClass: config.SandboxClassMicrovm,
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported ARCH=unsupported-arch") {
		t.Fatalf("DeployBenchmarks() = %v, want error from DeployMicroVMDeps", err)
	}
}

// Confirm the opposite: a gvisor benchmark run must not touch the cluster-wide
// micro-VM SandboxConfig.
func TestDeleteBenchmarksLeavesMicroVMDepsAloneForGvisor(t *testing.T) {
	silenceStepLog(t)

	root := t.TempDir()
	locust := filepath.Join(root, deployLocustScript)
	if err := os.MkdirAll(filepath.Dir(locust), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(locust, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	// Kube is nil: if DeleteBenchmarks called DeleteMicroVMDeps for gvisor, it
	// would dereference e.Kube and panic.
	env := &Env{Cfg: &config.Config{Root: root}}
	if err := env.DeleteBenchmarks(t.Context(), BenchmarkOptions{
		WorkerCount:  1,
		SandboxClass: config.SandboxClassGvisor,
	}); err != nil {
		t.Fatalf("DeleteBenchmarks(gvisor) = %v, want nil", err)
	}
}

func TestMicroVMDepsStepsAreSeparableFromContext(t *testing.T) {
	silenceStepLog(t)

	env := &Env{Cfg: &config.Config{Root: t.TempDir()}}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := env.DeployMicroVMDeps(ctx); err == nil {
		t.Error("DeployMicroVMDeps() with a cancelled context = nil, want an error")
	}
	if err := env.DeleteMicroVMDeps(ctx); err == nil {
		t.Error("DeleteMicroVMDeps() with a cancelled context = nil, want an error")
	}
}
