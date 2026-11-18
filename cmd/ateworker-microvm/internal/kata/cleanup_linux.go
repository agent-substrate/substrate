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
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"

	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/ocispec"
)

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
//
// cgroupKilled reports that cgroup.KillActorLeaf succeeded. cloud-hypervisor
// and virtiofsd start in the actor's leaf, so the /proc scan for orphans is
// then skipped. The scan is also skipped when neither dir exists.
func CleanupSandboxState(ctx context.Context, id string, cgroupKilled bool) {
	dirs := []string{
		filepath.Join("/run/kata-containers/shared/sandboxes", id),
		filepath.Join(vcVMDir, id),
	}
	cleanupSandboxDirs(ctx, id, dirs, cgroupKilled)
}

// cleanupSandboxDirs unmounts and removes the existing dirs and, unless
// cgroupKilled or none of dirs exists, kills orphaned processes for id.
func cleanupSandboxDirs(ctx context.Context, id string, dirs []string, cgroupKilled bool) {
	var existing []string
	for _, d := range dirs {
		if _, err := os.Lstat(d); !errors.Is(err, os.ErrNotExist) {
			existing = append(existing, d)
		}
	}
	if len(existing) == 0 {
		return
	}
	unmountAndRemove(ctx, existing)
	if !cgroupKilled {
		killOrphanedSandboxProcesses(ctx, id, "/proc")
	}
}

// unmountAndRemove detaches the mounts at or beneath each of dirs,
// deepest first, and removes each dir whose mounts all detached.
func unmountAndRemove(ctx context.Context, dirs []string) {
	for _, d := range dirs {
		points, err := sandboxMountsUnder(d)
		if err != nil {
			// Without the mounts there is no telling what RemoveAll would
			// walk into, so the dir is not provably safe to remove.
			slog.WarnContext(ctx, "Cannot list mounts; leaving sandbox dir in place",
				slog.String("dir", d), slog.Any("err", err))
			continue
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
		if len(points) > 0 {
			// Detaching a mount uncovers any mount stacked or hidden under it.
			if left, err := sandboxMountsUnder(d); err != nil || len(left) > 0 {
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

// statxAt queries the file type and mount-root attribute of path relative to
// dirfd without following symlinks or forcing a remote filesystem sync.
//
// STATX_ATTR_MOUNT_ROOT needs Linux 5.8. This package already needs
// mount_setattr (5.12), and the image cache's overlay mount needs 6.7 or 6.6.23.
func statxAt(dirfd int, path string) (unix.Statx_t, error) {
	var sx unix.Statx_t
	err := unix.Statx(dirfd, path, unix.AT_SYMLINK_NOFOLLOW|unix.AT_STATX_DONT_SYNC, unix.STATX_TYPE, &sx)
	return sx, err
}

// sandboxLeafMountDirs match, relative to the sandbox dir, the mountpoints
// that BindIntoShare, stageSystemInfoVolumes and the rootfs binds create under
// SharedDir. Nothing else is mounted beneath them.
var sandboxLeafMountDirs = []string{
	filepath.Join("shared", ocispec.ShareDurable),
	filepath.Join("shared", ocispec.ShareCSI),
	filepath.Join("shared", ocispec.ShareSystemInfo),
	filepath.Join("shared", "*", "rootfs"),
	filepath.Join("shared", "*", ocispec.ShareVolumes, "*"),
}

// isSandboxLeafMountDir reports whether rel, relative to the sandbox dir,
// matches sandboxLeafMountDirs.
func isSandboxLeafMountDir(rel string) bool {
	for _, pattern := range sandboxLeafMountDirs {
		if ok, _ := filepath.Match(pattern, rel); ok {
			return true
		}
	}
	return false
}

// sandboxMountsUnder lists the mount points at or below dir using
// STATX_ATTR_MOUNT_ROOT, without reading /proc/self/mountinfo.
func sandboxMountsUnder(dir string) ([]string, error) {
	dir = filepath.Clean(dir)
	sx, err := statxAt(unix.AT_FDCWD, dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var points []string
	if sx.Attributes&unix.STATX_ATTR_MOUNT_ROOT != 0 {
		points = append(points, dir)
	}
	if sx.Mode&unix.S_IFMT == unix.S_IFDIR {
		if err := walkSandboxMounts(dir, "", &points); err != nil {
			return nil, err
		}
	}
	return points, nil
}

// walkSandboxMounts appends the mount points below root/rel to points. It does
// not follow symlinks, descend into mount points, or descend into unmounted
// leaf mountpoint dirs (isSandboxLeafMountDir). It closes each dir before
// recursing, so it holds one open file at a time.
func walkSandboxMounts(root, rel string, points *[]string) error {
	curDir := filepath.Join(root, rel)
	f, err := os.Open(curDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	names, err := f.Readdirnames(-1)
	if err != nil {
		_ = f.Close()
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	dirfd := int(f.Fd())
	var subdirs []string
	for _, name := range names {
		sx, err := statxAt(dirfd, name)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			_ = f.Close()
			return err
		}
		if sx.Mode&unix.S_IFMT == unix.S_IFLNK {
			continue
		}
		if sx.Attributes&unix.STATX_ATTR_MOUNT_ROOT != 0 {
			*points = append(*points, filepath.Join(curDir, name))
			continue
		}
		subRel := filepath.Join(rel, name)
		if sx.Mode&unix.S_IFMT != unix.S_IFDIR || isSandboxLeafMountDir(subRel) {
			continue
		}
		subdirs = append(subdirs, subRel)
	}
	_ = f.Close()

	for _, sub := range subdirs {
		if err := walkSandboxMounts(root, sub, points); err != nil {
			return err
		}
	}
	return nil
}

// killOrphanedSandboxProcesses scans procRoot (/proc) and kills the
// per-sandbox processes (cloud-hypervisor / virtiofsd) left by a prior killed
// attempt: a canceled Create leaves the CH it spawned running (reparented to
// the init) holding guest RAM and stale sockets. Matched strictly by the
// sandbox id (an actor UUID) appearing in the cmdline, so nothing unrelated
// can match.
func killOrphanedSandboxProcesses(ctx context.Context, id, procRoot string) {
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
