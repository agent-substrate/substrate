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

// Each container's rootfs is an overlay assembled ON THE HOST — the stock kata
// arrangement (containerd's overlay snapshotter does the same): lower = the OCI image
// bundle, upper/work = host directories (see cmd/ateom-microvm/rootfsupper.go), merged
// by the host kernel and served to the guest over the ONE kataShared virtio-fs share.
// The guest runs the container on that directory directly; it never mounts an overlay
// itself. Rootfs writes therefore cost host disk, not guest RAM, and persist across
// suspend/resume via the snapshot's rootfs-upper tar.
//
// This file holds the rootfs-staging helpers.

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/reaper"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/third_party/kata/agentpb"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

const (
	// FsTag is the virtio-fs tag kata uses for the shared filesystem. The CH fs
	// device Tag and the agent mount Source must both be this value.
	FsTag = "kataShared"
	// typeVirtioFS / virtioFSDriver are the agent fstype + driver for it.
	typeVirtioFS   = "virtiofs"
	virtioFSDriver = "virtio-fs"
	// guestSharedDir is where the agent mounts the kataShared tag in the guest;
	// per-container rootfs then lives at <guestSharedDir>/<cid>/rootfs, durable
	// volumes at <guestSharedDir>/durable/<volumeName>, and CSI volumes at
	// <guestSharedDir>/csi/<volumeName>.
	guestSharedDir = "/run/kata-containers/shared/containers/"
)

// GuestDurableVolumeDir is the in-guest path holding one durable volume's
// contents, i.e. the bind source for that volume's container mount points.
func GuestDurableVolumeDir(volumeName string) string {
	return guestSharedDir + "durable/" + volumeName
}

// GuestCSIVolumeDir is the in-guest path holding one CSI volume's
// contents, i.e. the bind source for that volume's container mount points.
func GuestCSIVolumeDir(volumeName string) string {
	return guestSharedDir + "csi/" + volumeName
}

// SharedDir is the host directory virtiofsd serves into the guest as the RO base.
// Its layout (<cid>/rootfs) is what find-paths re-opens by path on restore.
func SharedDir(id string) string {
	return filepath.Join("/run/kata-containers/shared/sandboxes", id, "shared")
}

// VirtiofsdSocketPath is the vhost-user-fs socket CH connects to for the fs device.
func VirtiofsdSocketPath(id string) string { return filepath.Join(VMDir(id), "virtiofsd.sock") }

// UpperWorkDirs returns the HOST overlay upperdir and workdir for one container
// under the actor's rootfs-upper base dir: SIBLING directories, <cid>/fs and
// <cid>/work. Both properties are load-bearing — the kernel requires upperdir
// and workdir on the same filesystem and rejects a nested workdir — and the
// layout is also the snapshot tar's entry layout, so a change here breaks both
// every overlay mount and every existing snapshot. Covered by regression tests.
func UpperWorkDirs(upperBase, containerID string) (upper, work string) {
	return filepath.Join(upperBase, containerID, "fs"), filepath.Join(upperBase, containerID, "work")
}

// GuestSharedRootfs is the in-guest path the kataShared mount exposes a container's
// merged rootfs at. A container with this as Root.Path makes the agent's setup_bundle
// bind it to /run/kata-containers/<cid>/rootfs and run the container there — the
// stock kata flow.
func GuestSharedRootfs(containerID string) string { return guestSharedDir + containerID + "/rootfs" }

// VirtiofsdOptions configures StartVirtiofsd.
type VirtiofsdOptions struct {
	Binary     string // virtiofsd executable; defaults to "virtiofsd"
	SocketPath string // vhost-user socket CH connects to (VirtiofsdSocketPath)
	SharedDir  string // directory to serve (SharedDir(id))
	Log        io.Writer
}

// virtiofsdArgs builds the virtiofsd command line for o.
func virtiofsdArgs(o VirtiofsdOptions) []string {
	return []string{
		"--socket-path=" + o.SocketPath,
		"--shared-dir=" + o.SharedDir,
		"--cache=auto",
		"--thread-pool-size=1",
		"--announce-submounts",
		"--migration-mode", "find-paths",
	}
}

// StartVirtiofsd launches virtiofsd in find-paths migration mode serving o.SharedDir
// on o.SocketPath, and waits for the socket to appear. The returned cmd outlives the
// caller's ctx (CH demand-pages from it under the running VM); the caller owns it.
func StartVirtiofsd(ctx context.Context, o VirtiofsdOptions) (*exec.Cmd, error) {
	bin := o.Binary
	if bin == "" {
		bin = "virtiofsd"
	}
	_ = os.Remove(o.SocketPath)
	cmd := exec.Command(bin, virtiofsdArgs(o)...)
	cmd.Stdout = o.Log
	cmd.Stderr = o.Log
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting virtiofsd: %w", err)
	}
	if err := waitForSocket(ctx, o.SocketPath, virtiofsdSocketTimeout); err != nil {
		_ = cmd.Process.Kill()
		return nil, err
	}
	return cmd, nil
}

const (
	// virtiofsdSocketTimeout bounds how long we wait for virtiofsd to bind.
	virtiofsdSocketTimeout = 10 * time.Second
	// socketPollInterval is how often we look for it. This sits on the restore
	// path, ahead of the guest coming back, and virtiofsd binds in single-digit
	// milliseconds — so the interval, not the work, decides what this costs. At
	// 50ms every restore paid a full tick; polling finely enough to notice makes
	// it a few milliseconds instead, at the price of a handful of extra stats.
	socketPollInterval = 1 * time.Millisecond
)

// waitForSocket blocks until path exists, ctx is done, or timeout elapses.
func waitForSocket(ctx context.Context, path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	// One ticker rather than a timer per iteration: polling this finely, the
	// allocations add up, and a ticker does not stretch the interval by however
	// long the stat took.
	ticker := time.NewTicker(socketPollInterval)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("virtiofsd socket %q did not appear within %s", path, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// StageMergedRootfs mounts overlay(lower = the OCI image bundle rootfs, upper/work =
// the actor's host rootfs-upper dirs for cid) at SharedDir(restoreID)/<cid>/rootfs —
// the merged tree the ONE virtiofsd serves and the guest runs the container on
// directly. The host kernel owns the overlay (the canonical ext4-upper case: no
// special mount options, whiteouts/opaque markers are ordinary trusted.overlay.*
// metadata in the upper), and the lower stays pristine (overlayfs never writes below).
//
// The merged path is identical on every node — find-paths migration re-opens the
// guest's open files by path — given a deterministic image unpack plus the upper
// re-materialized from the snapshot tar (see cmd/ateom-microvm/rootfsupper.go).
func StageMergedRootfs(ctx context.Context, bundleRootfs, upperBase, restoreID, cid string) error {
	if cid == "" {
		return fmt.Errorf("StageMergedRootfs: empty container id")
	}
	dst := filepath.Join(SharedDir(restoreID), cid, "rootfs")
	upper, work := UpperWorkDirs(upperBase, cid)
	// Drop any stale mount first (lazy if busy), then ensure clean mountpoints.
	if err := reaper.Run(exec.Command("umount", dst)); err != nil {
		_ = reaper.Run(exec.Command("umount", "-l", dst))
	}
	// The workdir is scratch: wipe it so a volatile mount is never refused by a
	// dirty marker left behind by the previous activation.
	if err := os.RemoveAll(work); err != nil {
		return fmt.Errorf("clearing overlay workdir %q: %w", work, err)
	}
	for _, d := range []string{dst, upper, work} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return fmt.Errorf("creating %q: %w", d, err)
		}
	}
	// metacopy=off,index=off: pinned rather than inherited from the host's
	// overlay module defaults. Both features record file-handle references to
	// LOWER inodes in the upper (a metacopy'd file is a data-less upper entry
	// whose trusted.overlay.origin handle is where the data lives), and the
	// snapshot tar preserves trusted.overlay.* verbatim — but restore rebuilds
	// the lower from the OCI bundle with fresh inodes, so a preserved handle
	// goes stale and the file turns silently unreadable after resume. With both
	// off, every copy-up is a full data copy and the upper is self-contained —
	// the portability the find-paths comment above promises. (redirect_dir is
	// path-based and travels fine, so it stays at the kernel default.)
	// volatile: skip the sync overlayfs would otherwise do on this upper,
	// including at umount. The upper is throwaway — it is tarred into the
	// snapshot and then deleted — so the durability volatile gives up is
	// durability we do not use. It refuses to mount over a workdir left dirty by
	// a previous volatile mount, hence the wipe above.
	opts := "lowerdir=" + bundleRootfs + ",upperdir=" + upper + ",workdir=" + work +
		",metacopy=off,index=off,volatile"
	cmd := exec.CommandContext(ctx, "mount", "-t", "overlay", "overlay", "-o", opts, dst)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := reaper.Run(cmd); err != nil {
		return fmt.Errorf("mounting merged rootfs overlay at %q: %w (%s)", dst, err, strings.TrimSpace(stderr.String()))
	}
	// Ensure the standard OCI mountpoints exist even for minimal images: the container
	// mounts /proc,/sys,/dev over them, and find-paths re-opens the tree by path on
	// restore, so the layout must match on every node. Created in the MERGED tree, so
	// they land in the upper (and ride the snapshot tar) rather than dirtying the image.
	for _, d := range []string{"proc", "sys", "dev"} {
		_ = os.MkdirAll(filepath.Join(dst, d), 0o755)
	}
	return nil
}

// UnmountMergedRootfs drops one container's merged overlay mount (teardown and
// failure paths; lazy fallback if busy). Best-effort like the rest of teardown —
// CleanupSandboxState's sweep catches stragglers on the next boot.
func UnmountMergedRootfs(restoreID, cid string) {
	dst := filepath.Join(SharedDir(restoreID), cid, "rootfs")
	if err := reaper.Run(exec.Command("umount", dst)); err != nil {
		_ = reaper.Run(exec.Command("umount", "-l", dst))
	}
}

// ReconstructSharedDirFromImage bind-mounts a container's OCI image rootfs at
// <cid>/rootfs under SharedDir(restoreID) so virtiofsd serves it as the read-only
// lower. LEGACY restores only: guests from retired guest-tmpfs-upper snapshots hold
// this plain image tree open (their overlay upper lives inside the restored guest
// memory), so the share must present the bare image, not a merged overlay. The bind
// copies nothing on the host. cid is stable across the actor's lineage.
func ReconstructSharedDirFromImage(ctx context.Context, bundleRootfs, restoreID, cid string) error {
	if cid == "" {
		return fmt.Errorf("ReconstructSharedDirFromImage: empty container id")
	}
	dst := filepath.Join(SharedDir(restoreID), cid, "rootfs")
	// Drop any stale bind first (lazy if busy), then ensure a clean mountpoint. Not
	// RemoveAll: that would chase a live bind into bundleRootfs.
	if err := reaper.Run(exec.Command("umount", dst)); err != nil {
		_ = reaper.Run(exec.Command("umount", "-l", dst))
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return fmt.Errorf("creating shared dir %q: %w", dst, err)
	}
	cmd := exec.CommandContext(ctx, "mount", "--bind", bundleRootfs, dst)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := reaper.Run(cmd); err != nil {
		return fmt.Errorf("bind-mounting image rootfs %q -> %q: %w (%s)", bundleRootfs, dst, err, strings.TrimSpace(stderr.String()))
	}
	// Ensure the standard OCI mountpoints exist even for minimal images: the container
	// mounts /proc,/sys,/dev over them, and find-paths re-opens the lower by path on
	// restore, so the layout must match on every node. (Bind still writable; ignore EEXIST.)
	for _, d := range []string{"proc", "sys", "dev"} {
		_ = os.MkdirAll(filepath.Join(dst, d), 0o755)
	}
	// Remount read-only: the lower is immutable, so all writes go to the overlay upper
	// and it stays byte-identical across reconstructions (required by find-paths migration).
	ro := exec.CommandContext(ctx, "mount", "-o", "remount,bind,ro", dst)
	var roErr strings.Builder
	ro.Stderr = &roErr
	if err := reaper.Run(ro); err != nil {
		return fmt.Errorf("remounting overlay lower read-only %q: %w (%s)", dst, err, strings.TrimSpace(roErr.String()))
	}
	return nil
}

type CreateSandboxOpts struct {
	SandboxID string
	Hostname  string
}

// CreateSandboxForActor creates the guest sandbox with the kataShared virtio-fs mount
// (the merged rootfs trees, durable volumes, and CSI volumes every container runs on).
// Mirrors kata startSandbox.
func (a *AgentClient) CreateSandboxForActor(ctx context.Context, opts CreateSandboxOpts) error {
	storages := []*agentpb.Storage{{
		Driver:     virtioFSDriver,
		Source:     FsTag,
		Fstype:     typeVirtioFS,
		MountPoint: guestSharedDir,
	}}
	return a.CreateSandbox(ctx, &agentpb.CreateSandboxRequest{
		Hostname:  opts.Hostname,
		SandboxId: opts.SandboxID,
		Storages:  storages,
	})
}

// StartRootfsContainer creates + starts one container on the shared merged rootfs —
// the stock kata flow: the agent's setup_bundle binds shared/<cid>/rootfs to
// /run/kata-containers/<cid>/rootfs and the container runs there. Writable: the
// host-side overlay upper receives the writes (the guest mounts no overlay itself).
func (a *AgentClient) StartRootfsContainer(ctx context.Context, cid string, spec *specs.Spec) error {
	pbSpec := SpecToAgentPB(spec)
	pbSpec.Root = &agentpb.Root{Path: GuestSharedRootfs(cid), Readonly: false}
	// Per-container cgroup: the shaped spec carries the actor-wide
	// /ateomchv/<actorName> (spec.go), which collides across an actor's
	// containers — use the per-id path.
	if pbSpec.Linux != nil {
		pbSpec.Linux.CgroupsPath = "/ateomchv/" + cid
	}
	if err := a.CreateContainer(ctx, &agentpb.CreateContainerRequest{
		ContainerId: cid,
		ExecId:      cid,
		OCI:         pbSpec,
	}); err != nil {
		return fmt.Errorf("creating rootfs container %q: %w", cid, err)
	}
	if err := a.StartContainer(ctx, cid); err != nil {
		return fmt.Errorf("starting rootfs container %q: %w", cid, err)
	}
	return nil
}
