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

// The consumer half of the image cache: everything in this file runs in the
// privileged ateom pods (which own all mounts on the node), never in atelet.

package imagecache

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/sys/unix"
)

// SetupBundleRootfs composes the bundle's rootfs from cached layers per the
// bundle's overlay spec: it finalizes each layer (whiteout materialization,
// once per layer node-wide), mounts an overlay at <bundle>/rootfs with the
// cached layers as read-only lowerdirs and the bundle-local upper/ + work/
// as the actor's private writable side, and creates the spec's ExtraDirs
// through the mount (so they land in the upper).
//
// A bundle without an overlay spec is left untouched (its rootfs is a plain
// extracted directory). The mount lives in the calling process's mount
// namespace, which is exactly where the workload (runsc's gofer, virtiofsd)
// resolves it.
func SetupBundleRootfs(bundlePath string) error {
	spec, err := ReadSpec(bundlePath)
	if err != nil {
		return err
	}
	if spec == nil {
		return nil
	}

	for _, layerDir := range spec.Layers {
		if err := FinalizeLayer(layerDir); err != nil {
			return fmt.Errorf("while finalizing layer %q: %w", layerDir, err)
		}
	}

	rootfs := filepath.Join(bundlePath, "rootfs")
	upper := filepath.Join(bundlePath, "upper")
	work := filepath.Join(bundlePath, "work")
	for _, d := range []string{rootfs, upper, work} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return fmt.Errorf("while creating %q: %w", d, err)
		}
	}

	// Detach any stale mount left by a previous incarnation of this bundle
	// path (e.g. a run that failed between mount and teardown). EINVAL just
	// means nothing was mounted there.
	_ = unix.Unmount(rootfs, unix.MNT_DETACH)

	if len(spec.Layers) == 0 {
		// Degenerate zero-layer image: the empty rootfs dir plus ExtraDirs is
		// all there is.
		return createExtraDirs(rootfs, spec.ExtraDirs)
	}

	if err := mountOverlay(rootfs, overlayLowerDirs(spec.Layers), upper, work); err != nil {
		return fmt.Errorf("while mounting overlay rootfs at %q: %w", rootfs, err)
	}

	if err := createExtraDirs(rootfs, spec.ExtraDirs); err != nil {
		return err
	}

	// Repair merged directory metadata shadowed by implicitly-created parent
	// dirs (see implicitdirs.go); the chmod/chowns copy up into this bundle's
	// private upper, never into the shared pool.
	fixups, err := resolveImplicitDirFixups(spec.Layers)
	if err != nil {
		return fmt.Errorf("while resolving implicit dir metadata: %w", err)
	}
	if err := applyDirFixups(rootfs, fixups); err != nil {
		return fmt.Errorf("while repairing implicit dir metadata: %w", err)
	}
	return nil
}

// mountOverlay attaches an overlay of lowers (top-most first) with the given
// upper/work dirs at mountpoint, using the new mount API rather than
// mount(2): appending lowerdirs one fsconfig(2) call at a time sidesteps
// mount(2)'s single-page option-string cap, which digest-derived layer paths
// (~114 bytes each) would hit at roughly 34 layers.
//
// Minimum supported kernel: Linux 6.5, where overlayfs gained the
// incremental "lowerdir+" option. Every current GKE channel is at or above
// it (Stable runs COS 121 LTS on kernel 6.6; Regular and Rapid run COS
// 125/129 on 6.12).
func mountOverlay(mountpoint string, lowers []string, upper, work string) error {
	fsfd, err := unix.Fsopen("overlay", unix.FSOPEN_CLOEXEC)
	if err != nil {
		return fmt.Errorf("while opening overlay fs context: %w", err)
	}
	defer unix.Close(fsfd)

	set := func(key, val string) error {
		if err := unix.FsconfigSetString(fsfd, key, val); err != nil {
			return fmt.Errorf("while setting overlay %s=%q: %w%s", key, val, err, fsContextLog(fsfd))
		}
		return nil
	}
	for _, lower := range lowers {
		if err := set("lowerdir+", lower); err != nil {
			return err
		}
	}
	if err := set("upperdir", upper); err != nil {
		return err
	}
	if err := set("workdir", work); err != nil {
		return err
	}

	if err := unix.FsconfigCreate(fsfd); err != nil {
		return fmt.Errorf("while creating overlay superblock: %w%s", err, fsContextLog(fsfd))
	}
	mfd, err := unix.Fsmount(fsfd, unix.FSMOUNT_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("while creating overlay mount object: %w%s", err, fsContextLog(fsfd))
	}
	defer unix.Close(mfd)
	if err := unix.MoveMount(mfd, "", unix.AT_FDCWD, mountpoint, unix.MOVE_MOUNT_F_EMPTY_PATH); err != nil {
		return fmt.Errorf("while attaching overlay at %q: %w", mountpoint, err)
	}
	return nil
}

// fsContextLog drains the human-readable message log the kernel queues on an
// fs context fd (one "e/w/i "-prefixed message per read, ENODATA when empty)
// and renders it for appending to an error. mount(2) had no equivalent — a
// failed overlay mount was a bare errno; here the kernel says which option
// it rejected and why.
func fsContextLog(fsfd int) string {
	var msgs []string
	buf := make([]byte, 1024)
	for range 8 {
		n, err := unix.Read(fsfd, buf)
		if err != nil || n <= 0 {
			break
		}
		msgs = append(msgs, strings.TrimSpace(string(buf[:n])))
	}
	if len(msgs) == 0 {
		return ""
	}
	return " (kernel: " + strings.Join(msgs, "; ") + ")"
}

// FinalizeLayer materializes the whiteout state recorded at unpack time:
// 0:0 char devices for whiteouts and trusted.overlay.opaque=y on opaque
// dirs. This runs in ateom rather than atelet because mknod needs CAP_MKNOD
// and trusted.* xattrs need CAP_SYS_ADMIN, both of which atelet deliberately
// drops.
//
// Idempotent and safe under concurrent callers (multiple ateom pods share
// the node's pool): EEXIST from mknod is success, setxattr is naturally
// idempotent, and the marker is written last.
func FinalizeLayer(layerDir string) error {
	marker := filepath.Join(layerDir, layerFinalizedMarkerName)
	if _, err := os.Stat(marker); err == nil {
		return nil
	}

	wh, err := readWhiteouts(layerDir)
	if err != nil {
		return err
	}

	fsDir := filepath.Join(layerDir, layerFSDirName)
	root, err := os.OpenRoot(fsDir)
	if err != nil {
		return fmt.Errorf("while opening layer fs %q: %w", fsDir, err)
	}
	defer root.Close()

	for _, p := range wh.Whiteouts {
		rel, skip, err := validateTarName(p)
		if err != nil {
			return fmt.Errorf("invalid whiteout path: %w", err)
		}
		if skip {
			continue
		}
		if err := mknodWhiteout(root, rel); err != nil {
			return fmt.Errorf("while creating whiteout %q: %w", rel, err)
		}
	}

	for _, p := range wh.Opaques {
		rel, skip, err := validateTarName(p)
		if err != nil {
			return fmt.Errorf("invalid opaque dir path: %w", err)
		}
		if skip {
			continue
		}
		if err := setOpaque(root, rel); err != nil {
			return fmt.Errorf("while marking %q opaque: %w", rel, err)
		}
	}

	if err := os.WriteFile(marker, nil, 0o600); err != nil {
		return fmt.Errorf("while writing finalized marker: %w", err)
	}
	return nil
}

// mknodWhiteout creates the overlayfs whiteout (a 0:0 char device) at rel
// inside root, creating parent directories as needed (the whited-out path's
// parent may only exist in a lower layer).
func mknodWhiteout(root *os.Root, rel string) error {
	dir, base := filepath.Dir(rel), filepath.Base(rel)
	if dir != "." {
		if err := root.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	df, err := root.Open(dir)
	if err != nil {
		return err
	}
	defer df.Close()
	if err := unix.Mknodat(int(df.Fd()), base, unix.S_IFCHR, 0); err != nil && !errors.Is(err, os.ErrExist) {
		return &os.PathError{Op: "mknodat", Path: rel, Err: err}
	}
	return nil
}

// setOpaque marks the directory rel inside root as overlayfs-opaque.
func setOpaque(root *os.Root, rel string) error {
	if err := root.MkdirAll(rel, 0o755); err != nil {
		return err
	}
	df, err := root.Open(rel)
	if err != nil {
		return err
	}
	defer df.Close()
	if err := unix.Fsetxattr(int(df.Fd()), "trusted.overlay.opaque", []byte("y"), 0); err != nil {
		return &os.PathError{Op: "fsetxattr", Path: rel, Err: err}
	}
	return nil
}

// UnmountAllUnder lazily detaches every mount at or below dir in the calling
// process's mount namespace. It is the teardown counterpart of
// SetupBundleRootfs, keyed by directory rather than by container name so a
// single call cleans up all of an actor's bundle mounts. Missing mounts are
// not an error.
func UnmountAllUnder(dir string) error {
	points, err := mountPointsUnder(dir)
	if err != nil {
		return err
	}
	// Deepest first, so nested mounts unmount before their parents.
	sort.Slice(points, func(i, j int) bool { return len(points[i]) > len(points[j]) })
	var errs []error
	for _, p := range points {
		if err := unix.Unmount(p, unix.MNT_DETACH); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, fmt.Errorf("while unmounting %q: %w", p, err))
		}
	}
	return errors.Join(errs...)
}

// mountPointsUnder lists mount points at or below dir per
// /proc/self/mountinfo.
func mountPointsUnder(dir string) ([]string, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, fmt.Errorf("while opening mountinfo: %w", err)
	}
	defer f.Close()
	return mountPointsIn(f, dir)
}
