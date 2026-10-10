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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/third_party/kata/agentpb"
	"github.com/agent-substrate/substrate/internal/ocispec"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sys/unix"
)

const (
	// FsTag is the virtio-fs tag kata uses for the shared filesystem. The CH fs
	// device Tag and the agent mount Source must both be this value.
	FsTag = "kataShared"
	// typeVirtioFS / virtioFSDriver are the agent fstype + driver for it.
	typeVirtioFS   = "virtiofs"
	virtioFSDriver = "virtio-fs"
	// guestSharedDir is where the agent mounts the kataShared tag in the guest;
	// per-container rootfs then lives at <guestSharedDir>/<cid>/rootfs, and the
	// volume shares at the subdirectories ocispec.ShapeMicroVM points binds at.
	guestSharedDir = ocispec.GuestSharedDir + "/"
)

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
// and workdir on the same filesystem and rejects a nested workdir. The
// snapshot only carries each upperdir's contents (see
// cmd/ateom-microvm/rootfsupper.go); ateom creates this layout itself.
// Covered by regression tests.
func UpperWorkDirs(upperBase, containerID string) (upper, work string) {
	return filepath.Join(upperBase, containerID, "fs"), filepath.Join(upperBase, containerID, "work")
}

// MkdirUpperWorkDirs creates UpperWorkDirs under upperBase, which must exist.
// Only root uses <cid> and the workdir, so they are 0700. The upperdir is
// 0755: overlayfs presents its mode as the container's /.
func MkdirUpperWorkDirs(upperBase, containerID string) (upper, work string, err error) {
	upper, work = UpperWorkDirs(upperBase, containerID)
	for _, d := range []struct {
		path string
		mode os.FileMode
	}{{filepath.Dir(upper), 0o700}, {upper, 0o755}, {work, 0o700}} {
		if err := os.Mkdir(d.path, d.mode); err != nil && !errors.Is(err, fs.ErrExist) {
			return "", "", fmt.Errorf("creating %q: %w", d.path, err)
		}
	}
	return upper, work, nil
}

// GuestSharedRootfs is the in-guest path the kataShared mount exposes a container's
// merged rootfs at. A container with this as Root.Path makes the agent's setup_bundle
// bind it to /run/kata-containers/<cid>/rootfs and run the container there — the
// stock kata flow.
func GuestSharedRootfs(containerID string) string { return guestSharedDir + containerID + "/rootfs" }

// GuestSharedVolumeDir is the in-guest path one image volume's contents appear
// at, beside the container's rootfs in the same kataShared tree.
func GuestSharedVolumeDir(containerID, volumeName string) string {
	return filepath.Join(guestSharedDir, containerID, ocispec.ShareVolumes, volumeName)
}

// SharedVolumeDir is the host path under virtiofsd's served tree that
// GuestSharedVolumeDir resolves to.
func SharedVolumeDir(id, containerID, volumeName string) string {
	return filepath.Join(SharedDir(id), containerID, ocispec.ShareVolumes, volumeName)
}

// VirtiofsdOptions configures StartVirtiofsd.
type VirtiofsdOptions struct {
	Binary     string // virtiofsd executable; defaults to "virtiofsd"
	SocketPath string // vhost-user socket CH connects to (VirtiofsdSocketPath)
	SharedDir  string // directory to serve (SharedDir(id))
	Log        io.Writer
	// SysProcAttr, when set, is applied to the virtiofsd process.
	SysProcAttr *syscall.SysProcAttr
}

// virtiofsdArgs builds the virtiofsd command line for o.
func virtiofsdArgs(o VirtiofsdOptions, hasMknod bool) []string {
	args := []string{
		"--socket-path=" + o.SocketPath,
		"--shared-dir=" + o.SharedDir,
		"--cache=auto",
		"--thread-pool-size=1",
		"--announce-submounts",
		"--migration-mode", "find-paths",
		// The default (abort) makes a snapshot unrestorable if the guest still
		// references an inode with no findable path, such as a live-rewritten
		// trust bundle. guest-error returns EIO on those inodes instead.
		"--migration-on-error", "guest-error",
	}
	if !hasMknod {
		// virtiofsd's default child capability set includes MKNOD. It cannot
		// retain a capability the worker's bounding set excludes.
		args = append(args, "--modcaps=-mknod")
	}
	return args
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
	hasMknod, err := unix.PrctlRetInt(unix.PR_CAPBSET_READ, unix.CAP_MKNOD, 0, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("reading worker MKNOD bounding capability: %w", err)
	}
	cmd := exec.Command(bin, virtiofsdArgs(o, hasMknod == 1)...)
	cmd.Stdout = o.Log
	cmd.Stderr = o.Log
	cmd.SysProcAttr = o.SysProcAttr
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

// Unmount drops a mount at dst, falling back to lazy unmount if busy.
func Unmount(dst string) {
	if err := unix.Unmount(dst, 0); err != nil {
		if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOENT) {
			return
		}
		_ = unix.Unmount(dst, unix.MNT_DETACH)
	}
}

// RemountReadOnly remounts a bind mount at dst as read-only, preserving existing
// per-mount flags (nosuid, nodev, noexec, etc.) like libmount's remount,bind,ro.
func RemountReadOnly(dst string) error {
	var st unix.Statfs_t
	if err := unix.Statfs(dst, &st); err != nil {
		return fmt.Errorf("statfs %q: %w", dst, err)
	}
	flags := unix.MS_BIND | unix.MS_REMOUNT | unix.MS_RDONLY |
		int(st.Flags&(unix.ST_NOSUID|unix.ST_NODEV|unix.ST_NOEXEC|unix.ST_NOATIME|unix.ST_NODIRATIME))
	if err := unix.Mount("", dst, "", uintptr(flags), ""); err != nil {
		return err
	}
	return nil
}

// StageImageVolume bind-mounts one composed image volume read-only at
// <cid>/volumes/<name> under SharedDir(id), so virtiofsd exposes it to the
// guest.
func StageImageVolume(ctx context.Context, src, id, cid, volumeName string) error {
	if cid == "" || volumeName == "" {
		return fmt.Errorf("StageImageVolume: empty container id or volume name")
	}
	if err := BindIntoShare(ctx, src, id, filepath.Join(cid, "volumes", volumeName)); err != nil {
		return fmt.Errorf("while binding image volume %q into the shared tree: %w", volumeName, err)
	}
	// Read-only is this volume type's contract (the image is someone else's,
	// mounted for its contents); the other subtree consumers stay writable.
	dst := SharedVolumeDir(id, cid, volumeName)
	if err := RemountReadOnly(dst); err != nil {
		return fmt.Errorf("remounting image volume %q read-only: %w", dst, err)
	}
	return nil
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
	_, work := UpperWorkDirs(upperBase, cid)
	// Drop any stale mount first (lazy if busy), then ensure clean mountpoints.
	Unmount(dst)
	// The workdir is scratch: wipe it so a volatile mount is never refused by a
	// dirty marker left behind by the previous activation.
	if err := os.RemoveAll(work); err != nil {
		return fmt.Errorf("clearing overlay workdir %q: %w", work, err)
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return fmt.Errorf("creating %q: %w", dst, err)
	}
	upper, work, err := MkdirUpperWorkDirs(upperBase, cid)
	if err != nil {
		return err
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
	// nosuid,nodev: the upper is restored from a snapshot, and nothing on the
	// host should honor a setuid bit or device node in it. The guest sees the
	// tree over virtio-fs under its own mount flags, so this changes nothing
	// for it.
	opts := "lowerdir=" + bundleRootfs + ",upperdir=" + upper + ",workdir=" + work +
		",metacopy=off,index=off,volatile"
	if err := unix.Mount("overlay", dst, "overlay", unix.MS_NOSUID|unix.MS_NODEV, opts); err != nil {
		return fmt.Errorf("mounting merged rootfs overlay at %q: %w", dst, err)
	}
	// Ensure the standard OCI mountpoints exist even for minimal images: the container
	// mounts /proc,/sys,/dev over them, and find-paths re-opens the tree by path on
	// restore, so the layout must match on every node. Created in the MERGED tree, so
	// they land in the upper (and ride the snapshot tar) rather than dirtying the image.
	if err := ensureOCIMountpoints(dst); err != nil {
		return fmt.Errorf("creating OCI mountpoints under %q: %w", dst, err)
	}
	return nil
}

// ensureOCIMountpoints creates the /proc, /sys and /dev mountpoints in a container
// rootfs. Neither layer of that rootfs is trusted — the lower is the actor's image
// and the upper is restored from the snapshot the guest wrote — so it goes through
// os.Root: a plain MkdirAll would follow a symlink planted at one of those names and
// create the directory wherever it points on the worker pod, as root. An entry that
// already exists (including a symlink that stays inside the rootfs) is left alone.
func ensureOCIMountpoints(rootfs string) error {
	root, err := os.OpenRoot(rootfs)
	if err != nil {
		return err
	}
	defer root.Close()
	for _, d := range []string{"proc", "sys", "dev"} {
		if err := root.Mkdir(d, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
			return err
		}
	}
	return nil
}

// UnmountMergedRootfs drops one container's merged overlay mount (teardown and
// failure paths; lazy fallback if busy). Best-effort like the rest of teardown —
// CleanupSandboxState's sweep catches stragglers on the next boot.
func UnmountMergedRootfs(restoreID, cid string) {
	dst := filepath.Join(SharedDir(restoreID), cid, "rootfs")
	Unmount(dst)
}

// BindIntoShare bind-mounts a host directory at SharedDir(id)/<name>, so the
// ONE virtiofsd serves it to the guest as a subtree of the kataShared mount
// (--announce-submounts presents it to the guest as its own filesystem).
//
// This is THE pattern for exposing another host directory to the guest — the
// durable-dir and CSI volumes ride it today: a separate share would otherwise
// pay for its own virtiofsd (a process, a vhost socket, an fs device in every
// snapshot config and a restore-time revival of all three) per actor, forever.
// A bind costs one mount, and teardown already covers it: CleanupSandboxState
// lazily detaches every mount under the sandbox dir first, and will not remove
// a dir whose mounts it could not all detach, so the source directory (which
// may belong to atelet, as the volume dirs do) is never deleted through a live
// bind. Callers must stage binds before StartVirtiofsd, both to keep
// the served tree complete from the first request and because find-paths
// migration re-opens a restored guest's open files by path at reconnect.
//
// rel is the mount's path relative to the share root: a top-level name sits
// beside the per-container <cid>/... entries, so it must not collide with a
// container id (the durable/csi subtrees); a path inside a container's own
// subtree (the image volumes' <cid>/volumes/<name>) cannot collide at all.
func BindIntoShare(ctx context.Context, src, id, rel string) error {
	if rel == "" {
		return fmt.Errorf("BindIntoShare: empty share-relative path")
	}
	dst := filepath.Join(SharedDir(id), rel)
	// Drop any stale bind first (lazy if busy), then ensure a clean mountpoint.
	Unmount(dst)
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return fmt.Errorf("creating share subdir %q: %w", dst, err)
	}
	// --rbind, not --bind: a source that is itself a mount (a composed image
	// volume) comes along either way, but only rbind carries mounts nested
	// beneath it; for the plain-directory sources it is the same operation.
	if err := unix.Mount(src, dst, "", unix.MS_BIND|unix.MS_REC, ""); err != nil {
		return fmt.Errorf("bind-mounting %q into the shared tree at %q: %w", src, dst, err)
	}
	return setNosuidNodev(dst)
}

// setNosuidNodev adds nosuid and nodev to the mount at dst and every mount
// beneath it, keeping their other flags (a read-only image volume stays
// read-only). Shared trees hold workload-written and snapshot-restored files
// that nothing on the host should honor a setuid bit or device node in; the
// guest sees them over virtio-fs under its own mount flags, so this changes
// nothing for it.
func setNosuidNodev(dst string) error {
	attr := &unix.MountAttr{Attr_set: unix.MOUNT_ATTR_NOSUID | unix.MOUNT_ATTR_NODEV}
	if err := unix.MountSetattr(unix.AT_FDCWD, dst, unix.AT_RECURSIVE, attr); err != nil {
		return fmt.Errorf("setting nosuid,nodev on %q: %w", dst, err)
	}
	return nil
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
	Unmount(dst)
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return fmt.Errorf("creating shared dir %q: %w", dst, err)
	}
	if err := unix.Mount(bundleRootfs, dst, "", unix.MS_BIND, ""); err != nil {
		return fmt.Errorf("bind-mounting image rootfs %q -> %q: %w", bundleRootfs, dst, err)
	}
	// Ensure the standard OCI mountpoints exist even for minimal images: the container
	// mounts /proc,/sys,/dev over them, and find-paths re-opens the lower by path on
	// restore, so the layout must match on every node. (Bind still writable.)
	if err := ensureOCIMountpoints(dst); err != nil {
		return fmt.Errorf("creating OCI mountpoints under %q: %w", dst, err)
	}
	// Remount read-only: the lower is immutable, so all writes go to the overlay upper
	// and it stays byte-identical across reconstructions (required by find-paths migration).
	if err := RemountReadOnly(dst); err != nil {
		return fmt.Errorf("remounting overlay lower read-only %q: %w", dst, err)
	}
	return nil
}

type CreateSandboxOpts struct {
	SandboxID string
	Hostname  string
}

// CreateSandboxForActor creates the guest sandbox with the kataShared virtio-fs mount
// (the merged rootfs trees, durable volumes, CSI volumes, and system-info
// volumes every container runs on). Mirrors kata startSandbox.
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
	// Per-container cgroup under the shared /ateomchv parent, so the guest
	// kernel accounts an actor's containers hierarchically (see agentstats).
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
