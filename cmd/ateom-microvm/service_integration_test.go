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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/vishvananda/netns"
)

// TestServiceRunBlkRootfs covers the owned-boot cold-run path: ateom boots
// cloud-hypervisor itself and gives the actor a writable boot-time virtio-blk
// rootfs (/dev/vdb), then drives the kata-agent to start the container. It
// exercises only run (no checkpoint/restore). Unlike TestServiceE2E it MUST pass
// the guest kernel + image + base-config asset paths, because owned-boot builds
// the CH vm.create itself rather than reading configuration.toml.
//
// Gated behind KATA_INTEGRATION=1. Required env:
//
//	KATA_ROOTFS_SRC=<dir>   a populated actor rootfs (becomes /dev/vdb)
//	KATA_KERNEL=<path>      guest kernel (vmlinux.container)
//	KATA_IMAGE=<path>       guest OS image (kata-containers.img, /dev/vda)
//	KATA_CONFIG=<path>      a stock kata clh configuration.toml (for kernel_params + sizing)
//
// Optional: KATA_CH / KATA_VIRTIOFSD (defaults provided). Run as root on a host
// with kata + /dev/kvm + mkfs.ext4 (e2fsprogs):
//
//	sudo KATA_INTEGRATION=1 KATA_ROOTFS_SRC=/path/to/rootfs KATA_KERNEL=... KATA_IMAGE=... \
//	  KATA_CONFIG=... ./ateom-microvm.test -test.v -test.run BlkRootfs
func TestServiceRunBlkRootfs(t *testing.T) {
	if os.Getenv("KATA_INTEGRATION") != "1" {
		t.Skip("set KATA_INTEGRATION=1 to run (requires kata + /dev/kvm + root + e2fsprogs)")
	}
	rootfsSrc := os.Getenv("KATA_ROOTFS_SRC")
	if rootfsSrc == "" {
		t.Fatal("KATA_ROOTFS_SRC is required")
	}
	kernel, image, cfg := os.Getenv("KATA_KERNEL"), os.Getenv("KATA_IMAGE"), os.Getenv("KATA_CONFIG")
	if kernel == "" || image == "" || cfg == "" {
		t.Fatal("KATA_KERNEL, KATA_IMAGE, and KATA_CONFIG are required for the owned-boot path")
	}
	chBin := envOrTest("KATA_CH", "/usr/local/bin/cloud-hypervisor")

	ns, name := "default", "e2e-blk"
	id := fmt.Sprintf("ateomchv-blk-%d", os.Getpid())
	container := "app"

	bundle := ateompath.OCIBundlePath(ns, name, id, container)
	rootfs := filepath.Join(bundle, "rootfs")
	if err := os.MkdirAll(rootfs, 0o755); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("cp", "-a", rootfsSrc+"/.", rootfs+"/").CombinedOutput(); err != nil {
		t.Fatalf("copying rootfs: %v: %s", err, out)
	}
	writeMinimalGvisorStyleSpec(t, bundle)

	podUID := "testpod-blk"
	_ = netns.DeleteNamed(ateompath.AteomNetNSName(podUID))
	interiorNetNS, err := createNetNSWithoutSwitching(ateompath.AteomNetNSName(podUID))
	if err != nil {
		t.Fatalf("creating interior netns: %v", err)
	}
	svc := NewService(podUID, chBin, "", true, interiorNetNS, actorlog.NewActorLogger(actorlog.NewSyncedWriter(os.Stdout), false))
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	t.Cleanup(func() {
		cctx, c := context.WithTimeout(context.Background(), 20*time.Second)
		svc.teardownActor(cctx, id, svc.running[id], nil)
		c()
		_ = os.RemoveAll(ateompath.ActorPath(ns, name, id))
		_ = os.RemoveAll(kata.VMDir(id))
		_ = interiorNetNS.Close()
		_ = netns.DeleteNamed(ateompath.AteomNetNSName(podUID))
	})

	if _, err := svc.RunWorkload(ctx, &ateompb.RunWorkloadRequest{
		ActorTemplateNamespace: ns, ActorTemplateName: name, ActorId: id,
		Spec: &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{Name: container}}},
		RuntimeAssetPaths: map[string]string{
			assetKernel: kernel,
			assetImage:  image,
			assetConfig: cfg,
			assetCH:     chBin,
		},
	}); err != nil {
		// Best-effort: dump the guest serial console (captured to VMDir/serial.log)
		// so a boot failure shows the kernel/agent output.
		if b, rerr := os.ReadFile(filepath.Join(kata.VMDir(id), "serial.log")); rerr == nil {
			t.Logf("[serial.log tail]\n%s", lastLines(string(b), 60))
		}
		t.Fatalf("RunWorkload (owned-boot): %v", err)
	}
	t.Log("RunWorkload OK (owned-boot: CH booted by ateom, actor rootfs on /dev/vdb)")

	// Liveness: the ateom-owned CH must be up and the VM Running.
	client := ch.NewClient(filepath.Join(kata.VMDir(id), "clh-api.sock"))
	if err := client.WaitReady(ctx, 10*time.Second); err != nil {
		t.Fatalf("owned CH not ready: %v", err)
	}
	// Confirm the actor's rootfs really came from /dev/vdb (a marker visible via
	// the guest debug console — the actor's own files live on the blk disk).
	dump := kata.DebugConsoleDump(ctx, kata.VsockSocketPath(id),
		"echo '== vdb =='; blkid /dev/vdb 2>&1; echo '== rootfs mount =='; grep vdb /proc/mounts 2>&1; echo '== ip =='; ip -4 addr show eth0 2>&1")
	t.Logf("[guest] %s", dump)
}

// TestServiceCheckpointRestoreBlkRootfs exercises memory-only snapshot + restore
// with in-RAM continuity: the owned-boot actor snapshots MEMORY-ONLY (no
// shared-dir.tar, no balloon) and restores with its guest RAM intact. It writes a
// sentinel into guest tmpfs (/run = RAM), checkpoints,
// ships the snapshot dir, restores on a fresh CH process, and reads the sentinel
// back — if RAM continuity holds it survives. Same gating/env as
// TestServiceRunBlkRootfs.
func TestServiceCheckpointRestoreBlkRootfs(t *testing.T) {
	if os.Getenv("KATA_INTEGRATION") != "1" {
		t.Skip("set KATA_INTEGRATION=1 to run (requires kata + /dev/kvm + root + e2fsprogs)")
	}
	rootfsSrc := os.Getenv("KATA_ROOTFS_SRC")
	kernel, image, cfg := os.Getenv("KATA_KERNEL"), os.Getenv("KATA_IMAGE"), os.Getenv("KATA_CONFIG")
	if rootfsSrc == "" || kernel == "" || image == "" || cfg == "" {
		t.Fatal("KATA_ROOTFS_SRC, KATA_KERNEL, KATA_IMAGE, KATA_CONFIG are required")
	}
	chBin := envOrTest("KATA_CH", "/usr/local/bin/cloud-hypervisor")

	ns, name := "default", "e2e-blkcr"
	id := fmt.Sprintf("ateomchv-blkcr-%d", os.Getpid())
	container := "app"

	bundle := ateompath.OCIBundlePath(ns, name, id, container)
	rootfs := filepath.Join(bundle, "rootfs")
	if err := os.MkdirAll(rootfs, 0o755); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("cp", "-a", rootfsSrc+"/.", rootfs+"/").CombinedOutput(); err != nil {
		t.Fatalf("copying rootfs: %v: %s", err, out)
	}
	writeMinimalGvisorStyleSpec(t, bundle)

	podUID := "testpod-blkcr"
	_ = netns.DeleteNamed(ateompath.AteomNetNSName(podUID))
	interiorNetNS, err := createNetNSWithoutSwitching(ateompath.AteomNetNSName(podUID))
	if err != nil {
		t.Fatalf("creating interior netns: %v", err)
	}
	svc := NewService(podUID, chBin, "", true, interiorNetNS, actorlog.NewActorLogger(actorlog.NewSyncedWriter(os.Stdout), false))
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cctx, c := context.WithTimeout(context.Background(), 20*time.Second)
		svc.teardownActor(cctx, id, svc.running[id], nil)
		c()
		_ = os.RemoveAll(ateompath.ActorPath(ns, name, id))
		_ = os.RemoveAll(kata.VMDir(id))
		_ = interiorNetNS.Close()
		_ = netns.DeleteNamed(ateompath.AteomNetNSName(podUID))
	})

	assets := map[string]string{assetKernel: kernel, assetImage: image, assetConfig: cfg, assetCH: chBin}
	if _, err := svc.RunWorkload(ctx, &ateompb.RunWorkloadRequest{
		ActorTemplateNamespace: ns, ActorTemplateName: name, ActorId: id,
		Spec:              &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{Name: container}}},
		RuntimeAssetPaths: assets,
	}); err != nil {
		t.Fatalf("RunWorkload: %v", err)
	}
	t.Log("RunWorkload OK")

	// Write an in-RAM (tmpfs /run) sentinel via the guest debug console.
	const sentinel = "BLKROOT_CONTINUITY_OK_4242"
	vsock := kata.VsockSocketPath(id)
	_ = kata.DebugConsoleDump(ctx, vsock, "echo "+sentinel+" > /run/blkroot-sentinel; sync; echo wrote")
	if got := kata.DebugConsoleDump(ctx, vsock, "cat /run/blkroot-sentinel"); !strings.Contains(got, sentinel) {
		t.Fatalf("sentinel not readable pre-checkpoint: %q", got)
	}
	t.Log("wrote in-RAM sentinel")

	// CheckpointWorkload — memory-only, no balloon/wipe.
	if _, err := svc.CheckpointWorkload(ctx, &ateompb.CheckpointWorkloadRequest{
		ActorTemplateNamespace: ns, ActorTemplateName: name, ActorId: id,
		Spec: &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{Name: container}}},
	}); err != nil {
		t.Fatalf("CheckpointWorkload: %v", err)
	}
	checkpointDir := ateompath.CheckpointStateDir(ns, name, id)
	for _, f := range []string{"config.json", "state.json", "memory-ranges", "base-id"} {
		if _, err := os.Stat(filepath.Join(checkpointDir, f)); err != nil {
			t.Fatalf("checkpoint missing %q: %v", f, err)
		}
	}
	if _, err := os.Stat(filepath.Join(checkpointDir, "shared-dir.tar")); err == nil {
		t.Error("snapshot has shared-dir.tar — owned-boot must be MEMORY-ONLY (no virtio-fs base)")
	}
	t.Log("CheckpointWorkload OK (memory-only: config/state/memory-ranges/base-id, no shared-dir.tar)")

	// Ship snapshot dir -> restore dir (simulating atelet object-storage round trip).
	restoreDir := ateompath.RestoreStateDir(ns, name, id)
	if err := os.MkdirAll(restoreDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("cp", "-a", checkpointDir+"/.", restoreDir+"/").CombinedOutput(); err != nil {
		t.Fatalf("shipping snapshot: %v: %s", err, out)
	}

	// RestoreWorkload — reopen /dev/vdb, no virtiofsd/reconstruct.
	if _, err := svc.RestoreWorkload(ctx, &ateompb.RestoreWorkloadRequest{
		ActorTemplateNamespace: ns, ActorTemplateName: name, ActorId: id,
		Spec:              &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{Name: container}}},
		RuntimeAssetPaths: assets,
	}); err != nil {
		t.Fatalf("RestoreWorkload: %v", err)
	}
	client := ch.NewClient(filepath.Join(kata.VMDir(id), "clh-api-restore.sock"))
	if err := client.WaitReady(ctx, 10*time.Second); err != nil {
		t.Fatalf("restored CH not ready: %v", err)
	}
	t.Log("RestoreWorkload OK")

	// In-RAM continuity: the sentinel written before checkpoint must survive.
	got := kata.DebugConsoleDump(ctx, vsock, "cat /run/blkroot-sentinel")
	if !strings.Contains(got, sentinel) {
		t.Fatalf("RAM continuity FAILED: sentinel gone after restore #1: %q", got)
	}
	t.Logf("cycle1 OK: memory-only snapshot + restore, in-RAM continuity (%q)", strings.TrimSpace(got))

	// --- SECOND cycle: checkpoint-AFTER-restore. This is the OnDemand diff-snapshot
	// case — CH writes only the faulted delta and CheckpointWorkload overlays it onto
	// the restore source to rebuild a COMPLETE snapshot. If the merge is wrong the
	// snapshot is incomplete and restore #2 boots a corrupt guest (sentinel gone /
	// unreachable). Write a SECOND sentinel first so we also prove pages dirtied in
	// THIS activation are captured by the merge. ---
	const sentinel2 = "BLKROOT_CYCLE2_OK_8888"
	_ = kata.DebugConsoleDump(ctx, vsock, "echo "+sentinel2+" > /run/blkroot-sentinel2; sync")
	if _, err := svc.CheckpointWorkload(ctx, &ateompb.CheckpointWorkloadRequest{
		ActorTemplateNamespace: ns, ActorTemplateName: name, ActorId: id,
		Spec: &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{Name: container}}},
	}); err != nil {
		t.Fatalf("CheckpointWorkload #2 (merge): %v", err)
	}
	// Ship the merged snapshot (overwrites restoreDir AFTER the merge read it).
	if out, err := exec.Command("cp", "-a", checkpointDir+"/.", restoreDir+"/").CombinedOutput(); err != nil {
		t.Fatalf("shipping snapshot #2: %v: %s", err, out)
	}
	if _, err := svc.RestoreWorkload(ctx, &ateompb.RestoreWorkloadRequest{
		ActorTemplateNamespace: ns, ActorTemplateName: name, ActorId: id,
		Spec:              &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{Name: container}}},
		RuntimeAssetPaths: assets,
	}); err != nil {
		t.Fatalf("RestoreWorkload #2: %v", err)
	}
	client2 := ch.NewClient(filepath.Join(kata.VMDir(id), "clh-api-restore.sock"))
	if err := client2.WaitReady(ctx, 10*time.Second); err != nil {
		t.Fatalf("restored CH #2 not ready: %v", err)
	}
	// BOTH sentinels must survive: sentinel (from cycle 1, an un-faulted source page
	// recovered by the overlay) AND sentinel2 (dirtied this cycle, in CH's delta).
	g1 := kata.DebugConsoleDump(ctx, vsock, "cat /run/blkroot-sentinel")
	g2 := kata.DebugConsoleDump(ctx, vsock, "cat /run/blkroot-sentinel2")
	if !strings.Contains(g1, sentinel) {
		t.Fatalf("merge INCOMPLETE: cycle-1 sentinel lost after restore #2 (un-faulted source page dropped): %q", g1)
	}
	if !strings.Contains(g2, sentinel2) {
		t.Fatalf("merge lost the cycle-2 delta: sentinel2 gone after restore #2: %q", g2)
	}
	t.Logf("OnDemand-merge OK: 2-cycle suspend/resume, both sentinels survived (%q | %q)",
		strings.TrimSpace(g1), strings.TrimSpace(g2))
}

// TestServiceResetToGoldenBlkRootfs exercises reset-to-golden. From the
// golden snapshot, each restore recreates /dev/vdb byte-identical to the golden
// disk template, so an actor's rootfs writes do NOT persist into the next
// activation, while in-RAM state from the golden snapshot DOES. Two restores from
// the same golden snapshot: restore#1 writes a disk sentinel (runtime); restore#2
// must NOT see it (disk reset), while the RAM sentinel survives both.
func TestServiceResetToGoldenBlkRootfs(t *testing.T) {
	if os.Getenv("KATA_INTEGRATION") != "1" {
		t.Skip("set KATA_INTEGRATION=1 to run (requires kata + /dev/kvm + root + e2fsprogs)")
	}
	rootfsSrc := os.Getenv("KATA_ROOTFS_SRC")
	kernel, image, cfg := os.Getenv("KATA_KERNEL"), os.Getenv("KATA_IMAGE"), os.Getenv("KATA_CONFIG")
	if rootfsSrc == "" || kernel == "" || image == "" || cfg == "" {
		t.Fatal("KATA_ROOTFS_SRC, KATA_KERNEL, KATA_IMAGE, KATA_CONFIG are required")
	}
	chBin := envOrTest("KATA_CH", "/usr/local/bin/cloud-hypervisor")

	ns, name := "default", "e2e-blkrtg"
	id := fmt.Sprintf("ateomchv-blkrtg-%d", os.Getpid())
	container := "app"

	bundle := ateompath.OCIBundlePath(ns, name, id, container)
	if err := os.MkdirAll(filepath.Join(bundle, "rootfs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("cp", "-a", rootfsSrc+"/.", filepath.Join(bundle, "rootfs")+"/").CombinedOutput(); err != nil {
		t.Fatalf("copying rootfs: %v: %s", err, out)
	}
	writeMinimalGvisorStyleSpec(t, bundle)

	podUID := "testpod-blkrtg"
	_ = netns.DeleteNamed(ateompath.AteomNetNSName(podUID))
	interiorNetNS, err := createNetNSWithoutSwitching(ateompath.AteomNetNSName(podUID))
	if err != nil {
		t.Fatalf("creating interior netns: %v", err)
	}
	svc := NewService(podUID, chBin, "", true, interiorNetNS, actorlog.NewActorLogger(actorlog.NewSyncedWriter(os.Stdout), false))
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cctx, c := context.WithTimeout(context.Background(), 20*time.Second)
		svc.teardownActor(cctx, id, svc.running[id], nil)
		c()
		_ = os.RemoveAll(ateompath.ActorPath(ns, name, id))
		_ = os.RemoveAll(kata.VMDir(id))
		_ = interiorNetNS.Close()
		_ = netns.DeleteNamed(ateompath.AteomNetNSName(podUID))
	})

	assets := map[string]string{assetKernel: kernel, assetImage: image, assetConfig: cfg, assetCH: chBin}
	runReq := &ateompb.RunWorkloadRequest{
		ActorTemplateNamespace: ns, ActorTemplateName: name, ActorId: id,
		Spec:              &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{Name: container}}},
		RuntimeAssetPaths: assets,
	}
	restoreReq := &ateompb.RestoreWorkloadRequest{
		ActorTemplateNamespace: ns, ActorTemplateName: name, ActorId: id,
		Spec:              &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{Name: container}}},
		RuntimeAssetPaths: assets,
	}
	vsock := kata.VsockSocketPath(id)
	const ramSentinel = "RAM_GOLDEN_OK_7777"
	rootfsDir := "/run/kata-containers/" + id + "/rootfs"
	const diskSentinel = "DISK_WRITE_SHOULD_RESET_9999"

	// --- Golden: run, plant an in-RAM sentinel, checkpoint (saves golden snapshot
	// + golden disk template), tear down. ---
	if _, err := svc.RunWorkload(ctx, runReq); err != nil {
		t.Fatalf("RunWorkload: %v", err)
	}
	_ = kata.DebugConsoleDump(ctx, vsock, "echo "+ramSentinel+" > /run/ram-sentinel; sync")
	if _, err := svc.CheckpointWorkload(ctx, &ateompb.CheckpointWorkloadRequest{
		ActorTemplateNamespace: ns, ActorTemplateName: name, ActorId: id,
		Spec: &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{Name: container}}},
	}); err != nil {
		t.Fatalf("CheckpointWorkload: %v", err)
	}
	// golden disk template must have been saved.
	if _, err := os.Stat(filepath.Join(ateompath.ActorPath(ns, name, id), "golden-rootfs.ext4")); err != nil {
		t.Fatalf("golden rootfs template not saved: %v", err)
	}
	checkpointDir := ateompath.CheckpointStateDir(ns, name, id)
	restoreDir := ateompath.RestoreStateDir(ns, name, id)
	if err := os.MkdirAll(restoreDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("cp", "-a", checkpointDir+"/.", restoreDir+"/").CombinedOutput(); err != nil {
		t.Fatalf("shipping snapshot: %v: %s", err, out)
	}
	t.Log("golden checkpoint OK (snapshot + golden disk template saved)")

	// --- Restore #1: disk reset from golden template; write a disk sentinel at
	// runtime, confirm it lands, then tear down (discard). ---
	if _, err := svc.RestoreWorkload(ctx, restoreReq); err != nil {
		t.Fatalf("RestoreWorkload #1: %v", err)
	}
	if got := kata.DebugConsoleDump(ctx, vsock, "cat /run/ram-sentinel"); !strings.Contains(got, ramSentinel) {
		t.Fatalf("restore#1 RAM continuity failed: %q", got)
	}
	_ = kata.DebugConsoleDump(ctx, vsock, "echo "+diskSentinel+" > "+rootfsDir+"/disk-sentinel; sync")
	if got := kata.DebugConsoleDump(ctx, vsock, "cat "+rootfsDir+"/disk-sentinel"); !strings.Contains(got, diskSentinel) {
		t.Fatalf("restore#1 disk sentinel did not land: %q", got)
	}
	t.Log("restore#1 OK: RAM sentinel present, disk sentinel written")
	tdCtx, tdCancel := context.WithTimeout(ctx, 20*time.Second)
	svc.teardownActor(tdCtx, id, svc.running[id], ch.NewClient(filepath.Join(kata.VMDir(id), "clh-api-restore.sock")))
	tdCancel()
	delete(svc.running, id)

	// --- Restore #2: disk reset AGAIN from golden template — the disk sentinel
	// from restore#1 must be GONE, while the golden RAM sentinel still survives. ---
	if _, err := svc.RestoreWorkload(ctx, restoreReq); err != nil {
		t.Fatalf("RestoreWorkload #2: %v", err)
	}
	if got := kata.DebugConsoleDump(ctx, vsock, "cat /run/ram-sentinel"); !strings.Contains(got, ramSentinel) {
		t.Fatalf("restore#2 RAM continuity failed: %q", got)
	}
	got := kata.DebugConsoleDump(ctx, vsock, "cat "+rootfsDir+"/disk-sentinel 2>&1; echo END")
	if strings.Contains(got, diskSentinel) {
		t.Fatalf("reset-to-golden FAILED: disk sentinel persisted after restore#2: %q", got)
	}
	t.Logf("reset-to-golden OK: discarded the rootfs write (disk sentinel gone) while RAM continuity held: %q", strings.TrimSpace(got))
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n") + "\n"
}

func envOrTest(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// writeMinimalGvisorStyleSpec writes a deliberately minimal OCI spec (no
// linux.resources / cgroupsPath) so the test exercises ensureKataCompatibleSpec.
func writeMinimalGvisorStyleSpec(t *testing.T, bundle string) {
	t.Helper()
	spec := map[string]any{
		"ociVersion": "1.0.2",
		"process": map[string]any{
			"user": map[string]any{"uid": 0, "gid": 0},
			"args": []string{"sleep", "3600"},
			"env":  []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
			"cwd":  "/",
			"capabilities": map[string]any{
				"bounding":  []string{"CAP_KILL", "CAP_AUDIT_WRITE", "CAP_NET_BIND_SERVICE"},
				"effective": []string{"CAP_KILL", "CAP_AUDIT_WRITE", "CAP_NET_BIND_SERVICE"},
				"permitted": []string{"CAP_KILL", "CAP_AUDIT_WRITE", "CAP_NET_BIND_SERVICE"},
			},
		},
		"root":     map[string]any{"path": "rootfs", "readonly": false},
		"hostname": "ateomchv",
		"mounts": []map[string]any{
			{"destination": "/proc", "type": "proc", "source": "proc"},
			{"destination": "/dev", "type": "tmpfs", "source": "tmpfs"},
			{"destination": "/sys", "type": "sysfs", "source": "sysfs", "options": []string{"nosuid", "noexec", "nodev", "ro"}},
		},
		"linux": map[string]any{
			"namespaces": []map[string]any{
				{"type": "pid"}, {"type": "network"}, {"type": "ipc"}, {"type": "uts"}, {"type": "mount"},
			},
		},
	}
	b, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundle, "config.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
}
