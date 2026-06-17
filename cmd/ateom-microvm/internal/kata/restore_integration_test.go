//go:build linux

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

package kata

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
	"golang.org/x/sys/unix"
)

// TestKataRestore exercises the full restore cycle against real kata + CH:
// boot a busybox CH micro-VM via the shim, capture the virtio-fs shared dir
// from the shim's mount namespace, pause+snapshot, tear the original down, then
// RESTORE by reconstructing the shared dir + relaunching virtiofsd + CH
// --restore + resume. A post-restore pause/resume roundtrip proves the restored
// guest is live. Gated behind KATA_INTEGRATION=1.
//
// Env (in addition to KATA_INTEGRATION / KATA_ROOTFS_SRC / KATA_SHIM):
//
//	KATA_CH=<path>          cloud-hypervisor (default /usr/local/bin/cloud-hypervisor)
//	KATA_VIRTIOFSD=<path>   virtiofsd with migration support (default /usr/local/bin/virtiofsd-patched)
func TestKataRestore(t *testing.T) {
	if os.Getenv("KATA_INTEGRATION") != "1" {
		t.Skip("set KATA_INTEGRATION=1 to run (requires kata + /dev/kvm)")
	}
	rootfsSrc := os.Getenv("KATA_ROOTFS_SRC")
	if rootfsSrc == "" {
		t.Fatal("KATA_ROOTFS_SRC is required")
	}
	shim := envOr("KATA_SHIM", "/opt/kata/bin/containerd-shim-kata-v2")
	chBin := envOr("KATA_CH", "/usr/local/bin/cloud-hypervisor")
	vfsdBin := envOr("KATA_VIRTIOFSD", "/usr/local/bin/virtiofsd-patched")

	id := fmt.Sprintf("ateomchv-rst-%d", os.Getpid())
	work := filepath.Join("/tmp", id)
	bundle := filepath.Join(work, "bundle")
	rootfs := filepath.Join(bundle, "rootfs")
	if err := os.MkdirAll(rootfs, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(work) })
	if out, err := exec.Command("cp", "-a", rootfsSrc+"/.", rootfs+"/").CombinedOutput(); err != nil {
		t.Fatalf("copying rootfs: %v: %s", err, out)
	}
	writeBundleConfig(t, bundle, id)
	drainLogFifo(t, bundle)

	s := &Shim{
		Binary:       shim,
		ID:           id,
		Bundle:       bundle,
		Namespace:    "default",
		GRPCAddress:  filepath.Join(work, "fake-containerd.sock"),
		TTRPCAddress: filepath.Join(work, "fake-containerd.sock.ttrpc"),
		Diagnostics:  testWriter{t},
		Debug:        true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	var vfsdCmd, chCmd *exec.Cmd
	t.Cleanup(func() {
		for _, c := range []*exec.Cmd{chCmd, vfsdCmd} {
			if c != nil && c.Process != nil {
				_ = c.Process.Kill()
			}
		}
		_ = os.RemoveAll(VMDir(id))
		_ = os.RemoveAll(SharedDir(id))
	})

	// --- Boot the original incarnation. ---
	if err := s.Bootstrap(ctx); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if _, err := s.Create(ctx, CreateOptions{}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Log("original micro-VM running")

	// --- Capture shared dir (from the shim mountns) + snapshot. ---
	tarPath := filepath.Join(work, "shared-dir.tar")
	if err := s.CaptureSharedDir(ctx, tarPath); err != nil {
		t.Fatalf("CaptureSharedDir: %v", err)
	}
	t.Logf("captured shared dir -> %s", tarPath)

	client := ch.NewClient(CLHSocketPath(id))
	if err := client.WaitReady(ctx, 10*time.Second); err != nil {
		t.Fatalf("CH WaitReady: %v", err)
	}
	if err := client.Pause(ctx); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	snapDir := filepath.Join(work, "snapshot")
	if err := client.Snapshot(ctx, snapDir); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	t.Logf("snapshot -> %s", snapDir)

	// --- Tear the original down fast (kill CH + shim; skip agent-pinging stop). ---
	_ = client.Shutdown(ctx)
	if pid, err := s.ShimPID(); err == nil {
		_ = unix.Kill(pid, unix.SIGKILL)
	}
	_ = s.Close()
	time.Sleep(time.Second)

	// --- RESTORE (same id; bypasses the shim). ---
	if err := ReconstructSharedDir(ctx, tarPath, id); err != nil {
		t.Fatalf("ReconstructSharedDir: %v", err)
	}
	if err := os.MkdirAll(VMDir(id), 0o700); err != nil {
		t.Fatal(err)
	}
	vfsdCmd, err := StartVirtiofsd(ctx, VirtiofsdOptions{
		Binary:     vfsdBin,
		SocketPath: VirtiofsdSocketPath(id),
		SharedDir:  SharedDir(id),
		Log:        testWriter{t},
	})
	if err != nil {
		t.Fatalf("StartVirtiofsd: %v", err)
	}

	var rclient *ch.Client
	chCmd, rclient, err = ch.Restore(ctx, ch.RestoreOptions{
		Binary:    chBin,
		APISocket: filepath.Join(VMDir(id), "clh-api-restore.sock"),
		SourceDir: snapDir,
		Stdout:    testWriter{t},
		Stderr:    testWriter{t},
	})
	if err != nil {
		t.Fatalf("ch.Restore: %v", err)
	}
	t.Log("CH restored from snapshot")

	if err := rclient.Resume(ctx); err != nil {
		t.Fatalf("Resume after restore: %v", err)
	}

	// Liveness: a pause/resume roundtrip on the restored VM proves it is live.
	if err := rclient.Pause(ctx); err != nil {
		t.Errorf("post-restore Pause (liveness): %v", err)
	}
	if err := rclient.Resume(ctx); err != nil {
		t.Errorf("post-restore Resume (liveness): %v", err)
	}
	t.Log("RESTORE OK: guest resumed and is live on a fresh CH process")

	// Teardown the restored incarnation.
	_ = rclient.Shutdown(ctx)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
