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
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"golang.org/x/sys/unix"

	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/roottest"
)

func TestIsSandboxProcess(t *testing.T) {
	const id = "0e16c0fa-5c06-4b48-8742-4068635c08c1"
	// atelet stages each actor's VMM under that actor's own directory.
	staged := nodepath.ActorsDir + "/" + id + "/sandbox-assets/cloud-hypervisor"
	const other = "7d3b1a9c-2e4f-4a6b-9c8d-0f1e2d3c4b5a"
	otherStaged := nodepath.ActorsDir + "/" + other + "/sandbox-assets/cloud-hypervisor"
	argv := func(args ...string) string { return strings.Join(args, "\x00") + "\x00" }
	for _, tc := range []struct {
		name    string
		cmdline string
		exe     string
		want    bool
	}{
		{"staged VMM", argv(staged, "--api-socket", "/run/vc/vm/"+id+"/clh-api.sock"), staged, true},
		{"staged virtiofsd", argv(staged, "--socket-path=/run/vc/vm/"+id+"/virtiofsd.sock"), staged, true},
		{"VMM by name", argv("cloud-hypervisor", "--api-socket", "/run/vc/vm/"+id+"/clh-api.sock"), "/usr/bin/cloud-hypervisor", true},
		{"another sandbox", argv(otherStaged, "--api-socket", "/run/vc/vm/"+other+"/clh-api.sock"), otherStaged, false},
		{"another sandbox's VMM naming the id", argv(otherStaged, "--api-socket", "/run/vc/vm/"+other+"/clh-api.sock", "--log", "/tmp/"+id), otherStaged, false},
		{"unrelated process naming the id", argv("/bin/sh", "-c", "echo "+id), "/bin/busybox", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isSandboxProcess(id, tc.cmdline, tc.exe); got != tc.want {
				t.Errorf("isSandboxProcess() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCleanupSandboxStateOpts_AbsentAndPresentDirs(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	sharedBase := filepath.Join(tmp, "shared-sandboxes")
	vmBase := filepath.Join(tmp, "vc-vm")
	fakeMountinfo := filepath.Join(tmp, "mountinfo")
	if err := os.WriteFile(fakeMountinfo, []byte("22 26 0:20 / /sys rw - sysfs sysfs rw\n"), 0o600); err != nil {
		t.Fatalf("WriteFile(fakeMountinfo): %v", err)
	}

	const id = "actor-clean"
	sharedDir := filepath.Join(sharedBase, id)
	vmDir := filepath.Join(vmBase, id)
	dirs := []string{sharedDir, vmDir}

	// 1. Neither directory exists: cleanupSandboxDirs is a two-lstat no-op.
	cleanupSandboxDirs(ctx, id, dirs, CleanupOptions{CgroupKilled: false})

	// 2. Create leftover sandbox directories and leaf subdirectories; verify
	// fast cleanup skips descending into unmounted leaf dirs (rootfs, durable,
	// csi, system-info, volumes/<name>) and removes both sandbox directories.
	for _, sub := range []string{
		filepath.Join(sharedDir, "shared", "c_main", "rootfs", "etc"),
		filepath.Join(sharedDir, "shared", "c_main", "volumes", "agent", "bin"),
		filepath.Join(sharedDir, "shared", "durable", "vol0"),
		filepath.Join(sharedDir, "shared", "csi", "vol1"),
		filepath.Join(sharedDir, "shared", "system-info", "info"),
		vmDir,
	} {
		if err := os.MkdirAll(sub, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q): %v", sub, err)
		}
	}
	if err := os.WriteFile(filepath.Join(vmDir, "virtiofsd.sock"), []byte("s"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	pts, err := sandboxMountsUnderStatx(sharedDir)
	if err != nil {
		t.Fatalf("sandboxMountsUnderStatx(sharedDir): %v", err)
	}
	if len(pts) != 0 {
		t.Fatalf("sandboxMountsUnderStatx(sharedDir) = %v, want empty", pts)
	}

	cleanupSandboxDirs(ctx, id, dirs, CleanupOptions{CgroupKilled: true})
	for _, d := range dirs {
		if _, err := os.Stat(d); !os.IsNotExist(err) {
			t.Errorf("expected %q to be removed, stat err = %v", d, err)
		}
	}

	// 3. Verify unmountAndRemoveMountinfo also cleans up directories.
	if err := os.MkdirAll(sharedDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(sharedDir): %v", err)
	}
	unmountAndRemoveMountinfo(ctx, []string{sharedDir}, fakeMountinfo)
	if _, err := os.Stat(sharedDir); !os.IsNotExist(err) {
		t.Errorf("expected %q to be removed by unmountAndRemoveMountinfo, stat err = %v", sharedDir, err)
	}
}

func TestCleanupSandboxStateOpts_DetachesMountsBeforeRemove(t *testing.T) {
	roottest.Require(t, "mount/unmount")

	ctx := context.Background()
	tmp := t.TempDir()
	sharedBase := filepath.Join(tmp, "shared-sandboxes")
	vmBase := filepath.Join(tmp, "vc-vm")

	const id = "actor-mounted"
	sharedDir := filepath.Join(sharedBase, id)
	vmDir := filepath.Join(vmBase, id)
	dirs := []string{sharedDir, vmDir}

	// Simulate atelet's durable-dir volume source on the host, bind-mounted into
	// the sandbox's shared tree, plus a container rootfs bind mount.
	durableSrc := filepath.Join(tmp, "atelet-durable")
	rootfsSrc := filepath.Join(tmp, "bundle-rootfs")
	for _, d := range []string{durableSrc, rootfsSrc} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q): %v", d, err)
		}
	}
	durableFile := filepath.Join(durableSrc, "actor-data.txt")
	if err := os.WriteFile(durableFile, []byte("precious"), 0o644); err != nil {
		t.Fatalf("WriteFile(durableFile): %v", err)
	}

	durableMount := filepath.Join(sharedDir, "shared", "durable")
	rootfsMount := filepath.Join(sharedDir, "shared", "c_main", "rootfs")
	for _, d := range []string{durableMount, rootfsMount, vmDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q): %v", d, err)
		}
	}
	if err := unix.Mount(durableSrc, durableMount, "", unix.MS_BIND, ""); err != nil {
		t.Fatalf("bind mount durable: %v", err)
	}
	t.Cleanup(func() { _ = unix.Unmount(durableMount, unix.MNT_DETACH) })
	if err := unix.Mount(rootfsSrc, rootfsMount, "", unix.MS_BIND, ""); err != nil {
		t.Fatalf("bind mount rootfs: %v", err)
	}
	t.Cleanup(func() { _ = unix.Unmount(rootfsMount, unix.MNT_DETACH) })

	pts, err := sandboxMountsUnderStatx(sharedDir)
	if err != nil {
		t.Fatalf("sandboxMountsUnderStatx(sharedDir): %v", err)
	}
	slices.Sort(pts)
	want := []string{durableMount, rootfsMount}
	slices.Sort(want)
	if !slices.Equal(pts, want) {
		t.Fatalf("sandboxMountsUnderStatx(sharedDir) = %v, want %v", pts, want)
	}

	cleanupSandboxDirs(ctx, id, dirs, CleanupOptions{CgroupKilled: true})

	for _, d := range dirs {
		if _, err := os.Stat(d); !os.IsNotExist(err) {
			t.Errorf("expected %q to be removed, stat err = %v", d, err)
		}
	}
	// The bind-mounted durable source data must remain untouched.
	if got, err := os.ReadFile(durableFile); err != nil || string(got) != "precious" {
		t.Errorf("durable source file after cleanupSandboxDirs = %q (%v), want \"precious\"", got, err)
	}
}

func BenchmarkCleanupSandboxState(b *testing.B) {
	ctx := context.Background()
	tmp := b.TempDir()
	sharedBase := filepath.Join(tmp, "shared-sandboxes")
	vmBase := filepath.Join(tmp, "vc-vm")

	// Construct a synthetic 5,000-line mountinfo table representative of a
	// dense node with hundreds of active sandboxes and long overlay options.
	var sb strings.Builder
	lowerChain := strings.Repeat("/var/lib/ate/image-cache/layers/sha256/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef/fs:", 16)
	for i := range 5000 {
		fmt.Fprintf(&sb, "%d 100 0:250 / /run/kata-containers/shared/sandboxes/actor-%04d/shared/c_main/rootfs rw,relatime shared:%d - overlay overlay rw,lowerdir=%s,upperdir=/var/lib/ate/actors/actor-%04d/rootfs-upper/main/fs,workdir=/var/lib/ate/actors/actor-%04d/rootfs-upper/main/work\n",
			1000+i, i, 500+i, lowerChain, i, i)
	}
	mountinfoFile := filepath.Join(tmp, "mountinfo")
	if err := os.WriteFile(mountinfoFile, []byte(sb.String()), 0o600); err != nil {
		b.Fatalf("WriteFile(mountinfo): %v", err)
	}

	// Construct a synthetic /proc tree with 500 numeric PID directories
	// representative of active VMM, virtiofsd, and system processes on a node.
	fakeProc := filepath.Join(tmp, "proc")
	for i := range 500 {
		pidDir := filepath.Join(fakeProc, fmt.Sprintf("%d", 2000+i))
		if err := os.MkdirAll(pidDir, 0o755); err != nil {
			b.Fatalf("MkdirAll(%q): %v", pidDir, err)
		}
		actorID := fmt.Sprintf("other-actor-%04d", i)
		cmdline := fmt.Sprintf("/var/lib/ate/actors/%s/sandbox-assets/cloud-hypervisor\x00--api-socket\x00/run/vc/vm/%s/clh-api.sock\x00", actorID, actorID)
		if err := os.WriteFile(filepath.Join(pidDir, "cmdline"), []byte(cmdline), 0o644); err != nil {
			b.Fatalf("WriteFile(cmdline): %v", err)
		}
		if err := os.Symlink(fmt.Sprintf("/var/lib/ate/actors/%s/sandbox-assets/cloud-hypervisor", actorID), filepath.Join(pidDir, "exe")); err != nil {
			b.Fatalf("Symlink(exe): %v", err)
		}
	}

	const benchActor = "bench-actor"
	dirs := []string{filepath.Join(sharedBase, benchActor), filepath.Join(vmBase, benchActor)}
	opts := CleanupOptions{CgroupKilled: true}

	b.Run("fast=true", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			cleanupSandboxDirs(ctx, benchActor, dirs, opts)
		}
	})

	b.Run("fast=false", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			unmountAndRemoveMountinfo(ctx, dirs, mountinfoFile)
			killOrphanedSandboxProcesses(ctx, benchActor, fakeProc)
		}
	})
}
