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

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/roottest"
	"golang.org/x/sys/unix"
)

// This exercises the load-bearing filesystem boundary in split microVM
// startup with a real virtiofsd: the daemon starts on an empty share, then an
// application overlay mounted later must become visible in its mount namespace.
func TestPreparedRootfsBecomesVisibleToVirtiofsd(t *testing.T) {
	roottest.Require(t, "creating an isolated mount namespace and overlay mount")
	virtiofsd := os.Getenv("VIRTIOFSD_TEST_BINARY")
	if virtiofsd == "" {
		t.Skip("set VIRTIOFSD_TEST_BINARY to run the real virtiofsd integration test")
	}
	if _, err := os.Stat(virtiofsd); err != nil {
		t.Fatalf("VIRTIOFSD_TEST_BINARY: %v", err)
	}

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	originalMountNS, err := os.Open("/proc/thread-self/ns/mnt")
	if err != nil {
		t.Fatalf("open original mount namespace: %v", err)
	}
	defer originalMountNS.Close()
	if err := unix.Unshare(unix.CLONE_NEWNS); err != nil {
		t.Fatalf("unshare mount namespace: %v", err)
	}
	defer func() {
		if err := unix.Setns(int(originalMountNS.Fd()), unix.CLONE_NEWNS); err != nil {
			t.Errorf("restore original mount namespace: %v", err)
		}
	}()
	if err := unix.Mount("", "/", "", unix.MS_REC|unix.MS_PRIVATE, ""); err != nil {
		t.Fatalf("make test mount namespace private: %v", err)
	}
	if err := os.MkdirAll("/run/kata-containers", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := ensureSharedPropagation(t.Context(), "/run/kata-containers"); err != nil {
		t.Fatal(err)
	}

	id := fmt.Sprintf("prepared-rootfs-test-%d", os.Getpid())
	kata.CleanupSandboxState(t.Context(), id)
	defer kata.CleanupSandboxState(t.Context(), id)
	defer os.RemoveAll(rootfsUpperDir(id))
	if err := os.MkdirAll(kata.VMDir(id), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := resetRootfsUpperDir(id); err != nil {
		t.Fatal(err)
	}

	s := &AteomService{}
	vfsd, err := s.startRootfsShare(t.Context(), resolvedRuntime{virtiofsd: virtiofsd}, id)
	if err != nil {
		t.Fatalf("startRootfsShare: %v", err)
	}
	defer func() {
		if vfsd.Process != nil {
			_ = vfsd.Process.Kill()
			_, _ = vfsd.Process.Wait()
		}
	}()

	lower := t.TempDir()
	const proof = "mounted after virtiofsd started"
	if err := os.WriteFile(filepath.Join(lower, "proof.txt"), []byte(proof), 0o644); err != nil {
		t.Fatal(err)
	}
	ctrs := []actorContainer{{name: "app", bundleRootfs: lower}}
	if err := s.stageMergedRootfsMounts(t.Context(), id, ctrs, nil); err != nil {
		t.Fatalf("stageMergedRootfsMounts: %v", err)
	}
	defer kata.UnmountMergedRootfs(id, "app")

	// virtiofsd pivots its root to the shared directory. Reading through proc
	// therefore observes its private mount namespace, not this test's namespace.
	proofPath := filepath.Join(fmt.Sprintf("/proc/%d/root", vfsd.Process.Pid), "app", "rootfs", "proof.txt")
	deadline := time.Now().Add(2 * time.Second)
	for {
		got, err := os.ReadFile(proofPath)
		if err == nil {
			if string(got) != proof {
				t.Fatalf("proof content = %q, want %q", got, proof)
			}
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("late rootfs mount never appeared inside virtiofsd: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
