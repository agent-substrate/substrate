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
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"

	"go.ate.dev/substrate/internal/nodepath"
)

// CleanupOptions configures CleanupSandboxStateOpts.
type CleanupOptions struct {
	// CgroupKilled reports whether the caller has already killed and drained
	// the actor's cgroup leaf (cgroup.KillActorLeaf), making a /proc scan for
	// orphaned sandbox processes unnecessary.
	CgroupKilled bool
}

// CleanupSandboxState removes leftover host-side state for a sandbox id (the
// virtio-fs shared sandbox dir and the per-VM runtime dir), lazily unmounting
// anything still mounted underneath them first, and kills orphaned per-sandbox
// processes. ateworker owns the cloud-hypervisor boot directly (no kata shim, no
// containerd), so a failed Create does not fully self-clean; the deterministic
// sandbox id (= actor name) then collides on the next attempt: "listen unix
// .../virtiofsd.sock: bind: address already in use", "Could not bind mount
// .../shared/sandboxes/<id>/mounts", "directory not empty". Calling this
// before each run gives a clean slate.
//
// Removal is gated on the unmounting: the shared tree holds bind mounts whose
// SOURCES belong to someone else (the durable-dir and CSI volume dirs are
// atelet's — see BindIntoShare), and a RemoveAll that walks into a live bind
// deletes the actor's data through it. So a dir is removed only once every
// mount beneath it is known detached; otherwise it is left in place for the
// next sweep (the stagers tolerate a leftover dir — they unmount stale binds
// and reuse mountpoints).
func CleanupSandboxState(ctx context.Context, id string) {
	CleanupSandboxStateOpts(ctx, id, CleanupOptions{})
}

// CleanupSandboxStateOpts removes leftover host-side state for a sandbox id
// using the provided CleanupOptions. It stats the candidate sandbox
// directories first, discovers mounts beneath existing directories via
// statx(STATX_MNT_ID) before falling back to /proc/self/mountinfo, and skips
// the /proc orphan scan when the actor cgroup leaf was already killed or no
// sandbox directory existed on disk.
func CleanupSandboxStateOpts(ctx context.Context, id string, opts CleanupOptions) {
	dirs := []string{
		filepath.Join("/run/kata-containers/shared/sandboxes", id),
		filepath.Join(vcVMDir, id),
	}
	cleanupSandboxDirs(ctx, id, dirs, opts)
}

// cleanupSandboxDirs unmounts and removes existing candidate sandbox
// directories in dirs and kills orphaned processes for id when needed.
func cleanupSandboxDirs(ctx context.Context, id string, dirs []string, opts CleanupOptions) {
	activeDirs := make([]string, 0, len(dirs))
	for _, d := range dirs {
		if _, err := os.Lstat(d); err == nil || !errors.Is(err, os.ErrNotExist) {
			activeDirs = append(activeDirs, d)
		}
	}

	if len(activeDirs) > 0 {
		unmountAndRemoveStatx(ctx, activeDirs)
	}

	if opts.CgroupKilled || len(activeDirs) == 0 {
		return
	}
	killOrphanedSandboxProcesses(ctx, id, "/proc")
}

// unmountAndRemoveStatx discovers mounts at or beneath each directory in dirs
// via statx(STATX_MNT_ID), detaches them deepest-first, and removes each
// directory whose mounts all detached. If statx mount discovery fails for a
// directory, it falls back to unmountAndRemoveMountinfo for the remaining
// directories.
func unmountAndRemoveStatx(ctx context.Context, dirs []string) {
	for i, d := range dirs {
		points, err := sandboxMountsUnderStatx(d)
		if err != nil {
			unmountAndRemoveMountinfo(ctx, dirs[i:], "/proc/self/mountinfo")
			return
		}
		sort.Slice(points, func(a, b int) bool { return len(points[a]) > len(points[b]) })
		removable := true
		for _, mp := range points {
			if err := unix.Unmount(mp, unix.MNT_DETACH); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, os.ErrNotExist) {
				slog.WarnContext(ctx, "Failed to unmount leftover sandbox mount",
					slog.String("mount", mp), slog.Any("err", err))
				removable = false
			}
		}
		if !removable {
			slog.WarnContext(ctx, "Leaving sandbox dir in place: mounts under it were not all detached",
				slog.String("dir", d))
			continue
		}
		if err := os.RemoveAll(d); err != nil {
			slog.WarnContext(ctx, "Failed to remove leftover sandbox dir",
				slog.String("dir", d), slog.Any("err", err))
		}
	}
}

// statxAt queries STATX_TYPE and STATX_MNT_ID for path relative to dirfd
// without following symlinks or forcing a remote filesystem sync.
func statxAt(dirfd int, path string) (unix.Statx_t, error) {
	var sx unix.Statx_t
	err := unix.Statx(dirfd, path, unix.AT_SYMLINK_NOFOLLOW|unix.AT_STATX_DONT_SYNC, unix.STATX_TYPE|unix.STATX_MNT_ID, &sx)
	if err != nil {
		return sx, err
	}
	if sx.Mask&unix.STATX_MNT_ID == 0 {
		return sx, unix.ENOSYS
	}
	return sx, nil
}

// isMountRoot reports whether sx represents a mount root, either via
// STATX_ATTR_MOUNT_ROOT or by crossing a mount ID boundary from parentMntID.
func isMountRoot(sx unix.Statx_t, parentMntID uint64) bool {
	if sx.Attributes_mask&unix.STATX_ATTR_MOUNT_ROOT != 0 && sx.Attributes&unix.STATX_ATTR_MOUNT_ROOT != 0 {
		return true
	}
	return sx.Mnt_id != parentMntID
}

// isSandboxLeafMountDir reports whether name (or a child of volumes/) is a
// leaf mountpoint directory under SharedDir(id) that never contains nested
// host mounts when unmounted.
func isSandboxLeafMountDir(name string, inVolumesDir bool) bool {
	if inVolumesDir {
		return true
	}
	switch name {
	case "rootfs", "durable", "csi", "system-info":
		return true
	default:
		return false
	}
}

// sandboxMountsUnderStatx discovers mount points at or below dir by comparing
// STATX_MNT_ID (and STATX_ATTR_MOUNT_ROOT) across directory boundaries instead
// of reading and scanning /proc/self/mountinfo.
func sandboxMountsUnderStatx(dir string) ([]string, error) {
	dir = filepath.Clean(dir)
	dirSx, err := statxAt(unix.AT_FDCWD, dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, unix.ENOENT) {
			return nil, nil
		}
		return nil, err
	}

	var points []string
	if dirSx.Attributes_mask&unix.STATX_ATTR_MOUNT_ROOT != 0 {
		if dirSx.Attributes&unix.STATX_ATTR_MOUNT_ROOT != 0 {
			points = append(points, dir)
		}
	} else if parentDir := filepath.Dir(dir); parentDir != dir {
		parentSx, err := statxAt(unix.AT_FDCWD, parentDir)
		if err != nil {
			return nil, err
		}
		if dirSx.Mnt_id != parentSx.Mnt_id {
			points = append(points, dir)
		}
	}

	if dirSx.Mode&unix.S_IFMT == unix.S_IFDIR {
		if err := walkSandboxMountsStatx(dir, dirSx.Mnt_id, filepath.Base(dir) == "volumes", &points); err != nil {
			return nil, err
		}
	}
	return points, nil
}

// walkSandboxMountsStatx recursively inspects directory entries under curDir
// using dirfd-relative statx calls, collecting mount roots into points without
// descending into mounted filesystems or unmounted leaf volume directories.
func walkSandboxMountsStatx(curDir string, dirMntID uint64, inVolumesDir bool, points *[]string) error {
	f, err := os.Open(curDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, unix.ENOENT) {
			return nil
		}
		return err
	}
	names, readErr := f.Readdirnames(-1)
	if readErr != nil {
		_ = f.Close()
		if errors.Is(readErr, os.ErrNotExist) || errors.Is(readErr, unix.ENOENT) {
			return nil
		}
		return readErr
	}

	dirfd := int(f.Fd())
	type subdir struct {
		path  string
		mntID uint64
		inVol bool
	}
	var subdirs []subdir

	for _, name := range names {
		sx, err := statxAt(dirfd, name)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, unix.ENOENT) {
				continue
			}
			_ = f.Close()
			return err
		}
		if sx.Mode&unix.S_IFMT == unix.S_IFLNK {
			continue
		}
		if isMountRoot(sx, dirMntID) {
			*points = append(*points, filepath.Join(curDir, name))
			continue
		}
		isDir := sx.Mode&unix.S_IFMT == unix.S_IFDIR
		if !isDir || isSandboxLeafMountDir(name, inVolumesDir) {
			continue
		}
		subdirs = append(subdirs, subdir{
			path:  filepath.Join(curDir, name),
			mntID: sx.Mnt_id,
			inVol: name == "volumes",
		})
	}
	_ = f.Close()

	for _, sub := range subdirs {
		if err := walkSandboxMountsStatx(sub.path, sub.mntID, sub.inVol, points); err != nil {
			return err
		}
	}
	return nil
}

// unmountAndRemoveMountinfo detaches mounts beneath dirs by scanning
// mountinfoPath (/proc/self/mountinfo) and removes each directory whose mounts
// all detached.
func unmountAndRemoveMountinfo(ctx context.Context, dirs []string, mountinfoPath string) {
	removable := make(map[string]bool, len(dirs))
	if b, err := os.ReadFile(mountinfoPath); err != nil {
		// Without the mount table there is no telling what is still mounted
		// under the dirs, so none of them is provably safe to remove.
		slog.WarnContext(ctx, "Cannot read mountinfo; leaving sandbox dirs in place",
			slog.Any("err", err))
	} else {
		for _, d := range dirs {
			removable[d] = true
		}
		type mount struct{ mp, dir string }
		var mounts []mount
		for _, line := range strings.Split(string(b), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 5 {
				continue
			}
			mp := fields[4] // mount point
			for _, d := range dirs {
				if mp == d || strings.HasPrefix(mp, d+"/") {
					mounts = append(mounts, mount{mp: mp, dir: d})
					break
				}
			}
		}
		// Deepest paths first so child mounts unmount before their parents.
		sort.Slice(mounts, func(i, j int) bool { return len(mounts[i].mp) > len(mounts[j].mp) })
		for _, m := range mounts {
			if err := unix.Unmount(m.mp, unix.MNT_DETACH); err != nil {
				slog.WarnContext(ctx, "Failed to unmount leftover sandbox mount",
					slog.String("mount", m.mp), slog.Any("err", err))
				removable[m.dir] = false
			}
		}
	}
	for _, d := range dirs {
		if !removable[d] {
			slog.WarnContext(ctx, "Leaving sandbox dir in place: mounts under it were not all detached",
				slog.String("dir", d))
			continue
		}
		if err := os.RemoveAll(d); err != nil {
			slog.WarnContext(ctx, "Failed to remove leftover sandbox dir",
				slog.String("dir", d), slog.Any("err", err))
		}
	}
}

// killOrphanedSandboxProcesses scans procRoot (/proc) and sends SIGKILL to any
// orphaned cloud-hypervisor or virtiofsd process serving the sandbox id.
func killOrphanedSandboxProcesses(ctx context.Context, id, procRoot string) {
	// Kill orphaned per-sandbox processes (cloud-hypervisor / virtiofsd) left by
	// a prior killed attempt: a canceled Create leaves the CH it spawned running
	// (reparented to the init) holding guest RAM and stale sockets. Matched strictly by
	// the sandbox id (an actor UUID) appearing in the cmdline, so nothing
	// unrelated can match.
	entries, err := os.ReadDir(procRoot)
	if err != nil {
		return
	}
	for _, e := range entries {
		pid, perr := strconv.Atoi(e.Name())
		if perr != nil || pid == os.Getpid() {
			continue
		}
		cmdline, rerr := os.ReadFile(filepath.Join(procRoot, e.Name(), "cmdline"))
		if rerr != nil {
			continue
		}
		exe, _ := os.Readlink(filepath.Join(procRoot, e.Name(), "exe"))
		if isSandboxProcess(id, string(cmdline), exe) {
			if err := unix.Kill(pid, unix.SIGKILL); err != nil {
				slog.WarnContext(ctx, "Failed to kill orphaned sandbox process",
					slog.Int("pid", pid), slog.String("exe", exe), slog.Any("err", err))
			}
		}
	}
}

// isSandboxProcess reports whether a process is a VMM or virtiofsd serving the
// sandbox id. Both carry id in their socket paths. atelet stages their binaries
// under the actor's own directory, and the sandbox id is the actor UID, so an
// executable under an actor's directory settles it: this sandbox's if it is
// id's directory, another sandbox's otherwise, whatever the command line
// says. Only a binary from elsewhere is judged by its name.
func isSandboxProcess(id, cmdline, exe string) bool {
	if !strings.Contains(cmdline, id) {
		return false
	}
	if strings.HasPrefix(exe, nodepath.ActorsDir+"/") {
		return strings.HasPrefix(exe, nodepath.ActorSandboxAssetsDir(id)+"/")
	}
	argv0 := strings.SplitN(cmdline, "\x00", 2)[0]
	return strings.Contains(argv0, "cloud-hypervisor") || strings.Contains(argv0, "virtiofsd")
}
