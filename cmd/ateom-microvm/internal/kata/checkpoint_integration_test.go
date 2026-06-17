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

// TestKataCheckpoint boots a busybox container in a CH micro-VM via the kata
// shim, then exercises the checkpoint path: drive CH's REST api-socket (the
// one kata created) to pause + snapshot, and verify a portable, sparse snapshot
// directory is produced. Gated behind KATA_INTEGRATION=1 (see kata_integration_test.go).
func TestKataCheckpoint(t *testing.T) {
	if os.Getenv("KATA_INTEGRATION") != "1" {
		t.Skip("set KATA_INTEGRATION=1 to run (requires kata + /dev/kvm)")
	}
	rootfsSrc := os.Getenv("KATA_ROOTFS_SRC")
	if rootfsSrc == "" {
		t.Fatal("KATA_ROOTFS_SRC is required")
	}
	shim := os.Getenv("KATA_SHIM")
	if shim == "" {
		shim = "/opt/kata/bin/containerd-shim-kata-v2"
	}

	id := fmt.Sprintf("ateomchv-ckpt-%d", os.Getpid())
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

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	defer func() {
		_ = s.Kill(ctx, 9, true)
		sc, c := context.WithTimeout(context.Background(), 15*time.Second)
		_ = s.Shutdown(sc, true)
		c()
		_ = s.Close()
		cc, c2 := context.WithTimeout(context.Background(), 15*time.Second)
		_ = s.CleanupAction(cc)
		c2()
	}()

	if err := s.Bootstrap(ctx); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if _, err := s.Create(ctx, CreateOptions{}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Log("micro-VM running; checkpointing via CH REST api-socket")

	// --- M2: drive the CH api-socket kata created. ---
	client := ch.NewClient(CLHSocketPath(id))
	if err := client.WaitReady(ctx, 10*time.Second); err != nil {
		t.Fatalf("CH WaitReady: %v", err)
	}
	if err := client.Pause(ctx); err != nil {
		t.Fatalf("CH Pause: %v", err)
	}
	snapDir := filepath.Join(work, "snapshot")
	if err := client.Snapshot(ctx, snapDir); err != nil {
		t.Fatalf("CH Snapshot: %v", err)
	}
	t.Logf("snapshot written to %s", snapDir)

	// Verify the snapshot dir: config.json + state.json + a sparse memory file.
	for _, f := range []string{"config.json", "state.json"} {
		if _, err := os.Stat(filepath.Join(snapDir, f)); err != nil {
			t.Errorf("expected snapshot file %q: %v", f, err)
		}
	}
	entries, err := os.ReadDir(snapDir)
	if err != nil {
		t.Fatalf("reading snapshot dir: %v", err)
	}
	var memFile string
	for _, e := range entries {
		t.Logf("snapshot entry: %s", e.Name())
		if e.Name() != "config.json" && e.Name() != "state.json" {
			memFile = e.Name()
		}
	}
	if memFile == "" {
		t.Fatal("no memory file in snapshot dir")
	}
	apparent, actual := fileSizes(t, filepath.Join(snapDir, memFile))
	t.Logf("memory file %q: apparent=%d bytes, actual(on-disk)=%d bytes", memFile, apparent, actual)
	if actual >= apparent {
		t.Errorf("memory file not sparse: actual %d >= apparent %d (need shared=on / sparse snapshot)", actual, apparent)
	}
	if apparent == 0 {
		t.Errorf("memory file is empty")
	}
	t.Log("checkpoint produced a portable, sparse snapshot")
}

// drainLogFifo creates the bundle "log" fifo the kata shim writes to and drains
// it to t.Log so the shim's diagnostics are visible (and it doesn't block).
func drainLogFifo(t *testing.T, bundle string) {
	t.Helper()
	logFifo := filepath.Join(bundle, "log")
	if err := unix.Mkfifo(logFifo, 0o700); err != nil {
		t.Fatalf("mkfifo log: %v", err)
	}
	go func() {
		f, err := os.OpenFile(logFifo, os.O_RDONLY, 0)
		if err != nil {
			return
		}
		defer f.Close()
		buf := make([]byte, 4096)
		for {
			n, err := f.Read(buf)
			if n > 0 {
				t.Logf("[shim-log] %s", buf[:n])
			}
			if err != nil {
				return
			}
		}
	}()
}

// fileSizes returns a file's apparent size and its actual on-disk size (from
// st_blocks), so a caller can detect sparseness.
func fileSizes(t *testing.T, path string) (apparent, actual int64) {
	t.Helper()
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		t.Fatalf("stat %q: %v", path, err)
	}
	return st.Size, st.Blocks * 512
}
