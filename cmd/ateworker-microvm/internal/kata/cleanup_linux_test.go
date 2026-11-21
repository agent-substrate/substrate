//go:build linux

// Copyright 2026 The Agent Substrate Authors
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

func TestCleanupSandboxDirs_AbsentAndPresentDirs(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	const id = "actor-clean"
	sharedDir := filepath.Join(tmp, "shared-sandboxes", id)
	vmDir := filepath.Join(tmp, "vc-vm", id)
	dirs := []string{sharedDir, vmDir}

	// Unmounted leaf dirs (rootfs, durable, csi, system-info, volumes/<name>)
	// hold no mounts, and both dirs are removed.
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

	pts, err := sandboxMountsUnder(sharedDir)
	if err != nil {
		t.Fatalf("sandboxMountsUnder(sharedDir): %v", err)
	}
	if len(pts) != 0 {
		t.Fatalf("sandboxMountsUnder(sharedDir) = %v, want empty", pts)
	}

	cleanupSandboxDirs(ctx, id, dirs, true)
	for _, d := range dirs {
		if _, err := os.Stat(d); !os.IsNotExist(err) {
			t.Errorf("expected %q to be removed, stat err = %v", d, err)
		}
	}
}

func TestCleanupSandboxDirs_KeepsDirWhenMountsUnknown(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a mode 0 dir")
	}
	ctx := context.Background()
	const id = "actor-unreadable"
	sharedDir := filepath.Join(t.TempDir(), id)
	unreadable := filepath.Join(sharedDir, "shared")
	if err := os.MkdirAll(unreadable, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.Chmod(unreadable, 0); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(unreadable, 0o755) })

	cleanupSandboxDirs(ctx, id, []string{sharedDir}, true)
	if _, err := os.Stat(sharedDir); err != nil {
		t.Errorf("Stat(%q) = %v, want the dir left in place", sharedDir, err)
	}
}

func TestCleanupSandboxDirs_DetachesMountsBeforeRemove(t *testing.T) {
	roottest.Require(t, "mount/unmount")

	ctx := context.Background()
	tmp := t.TempDir()
	const id = "actor-mounted"
	sharedDir := filepath.Join(tmp, "shared-sandboxes", id)
	vmDir := filepath.Join(tmp, "vc-vm", id)
	dirs := []string{sharedDir, vmDir}

	// Simulate atelet's durable-dir volume source on the host, bind-mounted
	// twice (stacked) into the sandbox's shared tree, plus container rootfs and
	// image volume bind mounts.
	durableSrc := filepath.Join(tmp, "atelet-durable")
	rootfsSrc := filepath.Join(tmp, "bundle-rootfs")
	volumeSrc := filepath.Join(tmp, "image-volume")
	for _, d := range []string{durableSrc, rootfsSrc, volumeSrc} {
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
	volumeMount := filepath.Join(sharedDir, "shared", "c_main", "volumes", "agent")
	for _, d := range []string{durableMount, rootfsMount, volumeMount, vmDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q): %v", d, err)
		}
	}
	for _, m := range []struct{ src, dst string }{
		{durableSrc, durableMount},
		{durableSrc, durableMount},
		{rootfsSrc, rootfsMount},
		{volumeSrc, volumeMount},
	} {
		if err := unix.Mount(m.src, m.dst, "", unix.MS_BIND, ""); err != nil {
			t.Fatalf("bind mount %q: %v", m.dst, err)
		}
		t.Cleanup(func() { _ = unix.Unmount(m.dst, unix.MNT_DETACH) })
	}

	pts, err := sandboxMountsUnder(sharedDir)
	if err != nil {
		t.Fatalf("sandboxMountsUnder(sharedDir): %v", err)
	}
	slices.Sort(pts)
	want := []string{durableMount, rootfsMount, volumeMount}
	slices.Sort(want)
	if !slices.Equal(pts, want) {
		t.Fatalf("sandboxMountsUnder(sharedDir) = %v, want %v", pts, want)
	}

	// The first sweep detaches the top durable bind, which uncovers the one
	// stacked under it, so it leaves sharedDir in place.
	cleanupSandboxDirs(ctx, id, dirs, true)
	if _, err := os.Stat(sharedDir); err != nil {
		t.Fatalf("Stat(%q) after the first sweep = %v, want the dir left in place", sharedDir, err)
	}
	cleanupSandboxDirs(ctx, id, dirs, true)

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

// mountinfoMountsUnder returns the mount points at or below dirs listed in
// mountinfoPath. It is the scan CleanupSandboxState ran before it used statx,
// kept as the benchmark baseline.
func mountinfoMountsUnder(mountinfoPath string, dirs []string) ([]string, error) {
	b, err := os.ReadFile(mountinfoPath)
	if err != nil {
		return nil, err
	}
	var points []string
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		mp := fields[4]
		for _, d := range dirs {
			if mp == d || strings.HasPrefix(mp, d+"/") {
				points = append(points, mp)
				break
			}
		}
	}
	return points, nil
}

// BenchmarkCleanupSandboxState compares cleanup against the mountinfo and
// /proc scans it replaced, on a synthetic 5,000-line mountinfo and a 500-PID
// /proc tree. "absent" is a teardown whose sandbox dirs are gone and whose
// cgroup kill succeeded. "present" compares only mount discovery over a
// sandbox tree with no mounts.
func BenchmarkCleanupSandboxState(b *testing.B) {
	ctx := context.Background()
	tmp := b.TempDir()

	// A dense node: hundreds of sandboxes with long overlay options.
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

	// Other actors' VMMs, none of which match the benchmark's sandbox id.
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

	const id = "bench-actor"
	absent := []string{filepath.Join(tmp, "absent", "shared-sandboxes", id), filepath.Join(tmp, "absent", "vc-vm", id)}
	present := []string{filepath.Join(tmp, "present", "shared-sandboxes", id), filepath.Join(tmp, "present", "vc-vm", id)}
	for _, sub := range []string{
		filepath.Join(present[0], "shared", "c_main", "rootfs"),
		filepath.Join(present[0], "shared", "c_main", "volumes", "agent"),
		filepath.Join(present[0], "shared", "durable"),
		filepath.Join(present[0], "shared", "csi"),
		filepath.Join(present[0], "shared", "system-info"),
		present[1],
	} {
		if err := os.MkdirAll(sub, 0o755); err != nil {
			b.Fatalf("MkdirAll(%q): %v", sub, err)
		}
	}

	b.Run("absent/scan", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := mountinfoMountsUnder(mountinfoFile, absent); err != nil {
				b.Fatal(err)
			}
			for _, d := range absent {
				_ = os.RemoveAll(d)
			}
			killOrphanedSandboxProcesses(ctx, id, fakeProc)
		}
	})
	b.Run("absent/lstat", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			cleanupSandboxDirs(ctx, id, absent, true)
		}
	})
	b.Run("present/mountinfo", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := mountinfoMountsUnder(mountinfoFile, present); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("present/statx", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			for _, d := range present {
				if _, err := sandboxMountsUnder(d); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}
