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

// The consumer half of the image cache: everything in this file runs in the
// worker pods, which set up the rootfs mounts, never in atelet.

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

	// upper (with an overlay) or rootfs (without) becomes the container's "/".
	// At 0700 a non-root container cannot search its own root. Chmod after the
	// unmount, explicitly: MkdirAll applies the umask and skips existing dirs.
	for _, d := range []string{rootfs, upper} {
		if err := os.Chmod(d, 0o755); err != nil {
			return fmt.Errorf("while setting mode of %q: %w", d, err)
		}
	}

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

	if err := setupImageVolumes(bundlePath, spec.ImageVolumes); err != nil {
		return fmt.Errorf("while setting up image volumes: %w", err)
	}
	return nil
}

// setupImageVolumes exposes each image volume's contents read-only at its
// bundle-local mount point, for the OCI spec to bind into the container.
func setupImageVolumes(bundlePath string, volumes []ImageVolumeOverlay) error {
	for _, vol := range volumes {
		for _, layerDir := range vol.Layers {
			if err := FinalizeLayer(layerDir); err != nil {
				return fmt.Errorf("while finalizing layer %q of image volume %q: %w", layerDir, vol.Name, err)
			}
		}

		mountpoint := ImageVolumeMountPath(bundlePath, vol.Name)
		if err := os.MkdirAll(mountpoint, 0o700); err != nil {
			return fmt.Errorf("while creating image volume mount point %q: %w", mountpoint, err)
		}
		_ = unix.Unmount(mountpoint, unix.MNT_DETACH)

		if err := mountImageVolume(mountpoint, vol.Layers); err != nil {
			return fmt.Errorf("while mounting image volume %q: %w", vol.Name, err)
		}
	}
	return nil
}

// mountImageVolume attaches layers read-only at mountpoint.
func mountImageVolume(mountpoint string, layers []string) error {
	if len(layers) == 1 {
		// The kernel rejects a single lowerdir without an upperdir, so bind
		// the layer's fs/ tree directly for the single-layer case.
		fsDir := filepath.Join(layers[0], layerFSDirName)
		if err := unix.Mount(fsDir, mountpoint, "", unix.MS_BIND|unix.MS_REC, ""); err != nil {
			return fmt.Errorf("while binding %q: %w", fsDir, err)
		}
		// A bind can't be created read-only, so set it after, on every mount the
		// rbind brought along. Also private, otherwise a mount made under fsDir
		// later would propagate in writable.
		attr := &unix.MountAttr{Attr_set: unix.MOUNT_ATTR_RDONLY, Propagation: unix.MS_PRIVATE}
		if err := unix.MountSetattr(unix.AT_FDCWD, mountpoint, unix.AT_RECURSIVE, attr); err != nil {
			return fmt.Errorf("while making %q read-only: %w", mountpoint, err)
		}
		return nil
	}
	return mountOverlay(mountpoint, overlayLowerDirs(layers), "", "")
}

// mountOverlay attaches an overlay of lowers (top-most first) with the given
// upper/work dirs at mountpoint, using the new mount API rather than
// mount(2): appending lowerdirs one fsconfig(2) call at a time sidesteps
// mount(2)'s single-page option-string cap, which digest-derived layer paths
// (~114 bytes each) would hit at roughly 34 layers.
//
// An empty upper mounts the overlay without a writable layer, which overlayfs
// makes read-only.
//
// Requires overlayfs's "lowerdir+" option: Linux 6.7+, or 6.6.23+ where it was
// backported.
// https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=24e16e385f2272b1a9df51337a5c32d28a29c7ad
// https://git.kernel.org/pub/scm/linux/kernel/git/stable/linux.git/commit/?id=cc0918b3582c98f12cfb30bf7496496d14bff3e9
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
	if upper != "" {
		if err := set("upperdir", upper); err != nil {
			return err
		}
		if err := set("workdir", work); err != nil {
			return err
		}
	}
	// volatile: skip overlayfs's syncs on this upper, including the one it does
	// at umount, which measured ~450ms per actor on GKE. The bundle upper holds
	// only copy-ups made during one activation and atelet wipes it between them
	// (RemoveAllWritable), so nothing here is expected to outlive a crash. Note
	// the merged rootfs overlay stacked on top of this one must be volatile too:
	// with only one of them volatile the sync simply moves to the other.
	if err := unix.FsconfigSetFlag(fsfd, "volatile"); err != nil {
		return fmt.Errorf("while setting overlay volatile: %w%s", err, fsContextLog(fsfd))
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
// dirs. This runs in worker rather than atelet because trusted.* xattrs need
// CAP_SYS_ADMIN, which atelet deliberately drops. Unlike ordinary device nodes,
// 0:0 whiteouts can be created without CAP_MKNOD since Linux 5.8.
//
// Idempotent and safe under concurrent callers (multiple worker pods share
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

// maxUnmountPasses bounds how many rounds of detaches UnmountAllUnder and
// UnmountBundlesUnder make. Each walk after the first finds mounts that an
// earlier detach uncovered, such as a lower mount stacked under the one statx
// reported.
const maxUnmountPasses = 8

// UnmountAllUnder lazily detaches every mount at or below dir in the calling
// process's mount namespace. Missing mounts are not an error.
//
// dir must be an absolute path. UnmountAllUnder walks dir again after each
// round of detaches. It returns an error if mounts remain after
// maxUnmountPasses rounds or if any entry could not be checked, so callers
// must not remove dir when it returns an error.
func UnmountAllUnder(dir string) error {
	return unmountAllUnder(dir, hostMountSys(), mountPointsUnder)
}

// UnmountBundlesUnder lazily detaches the mounts SetupBundleRootfs makes in
// each bundle under dir, an OCI bundle dir that holds one bundle per container
// (actorDirs.GetOciBundleDir()). It is the teardown counterpart of
// SetupBundleRootfs, keyed by directory rather than by container name so a
// single call cleans up all of an actor's bundle mounts. It checks only
// <dir>/<container>/rootfs and <dir>/<container>/volumes/<name>, so use
// UnmountAllUnder for dirs that may hold other mounts. It returns errors as
// UnmountAllUnder does.
func UnmountBundlesUnder(dir string) error {
	return unmountAllUnder(dir, hostMountSys(), bundleMountPoints)
}

// mountSys holds the syscalls unmountAllUnder makes, so tests can fake them.
type mountSys struct {
	// statx stats path relative to dirfd, as statxAt does.
	statx func(dirfd int, path string) (unix.Statx_t, error)
	// unmount detaches target, as unix.Unmount does.
	unmount func(target string, flags int) error
}

// hostMountSys returns the mountSys that makes the real syscalls.
func hostMountSys() mountSys {
	return mountSys{statx: statxAt, unmount: unix.Unmount}
}

// mountPoint is a path found by mountPointsUnder or bundleMountPoints.
type mountPoint struct {
	path string
	// statErr is set when statx failed on path. path may be a mount whose
	// root cannot be stat'ed, such as a FUSE mount whose server is gone.
	statErr error
}

// unmountAllUnder detaches the mounts under dir that find lists, deepest
// first, until find lists none.
func unmountAllUnder(dir string, sys mountSys, find func(dir string, sys mountSys) ([]mountPoint, error)) error {
	if !filepath.IsAbs(dir) {
		return fmt.Errorf("refusing to unmount under %q: not an absolute path", dir)
	}
	dir = filepath.Clean(dir)
	for pass := 0; ; pass++ {
		points, err := find(dir, sys)
		if len(points) == 0 {
			return err
		}
		if pass == maxUnmountPasses {
			errs := []error{err}
			paths := make([]string, len(points))
			for i, p := range points {
				paths[i] = p.path
				if p.statErr != nil {
					errs = append(errs, fmt.Errorf("while checking %q: %w", p.path, p.statErr))
				}
			}
			errs = append(errs, fmt.Errorf("mounts remain under %q after %d passes: %q", dir, maxUnmountPasses, paths))
			return errors.Join(errs...)
		}
		errs := []error{err}
		progress := false
		// Deepest first, so nested mounts unmount before their parents.
		sort.Slice(points, func(i, j int) bool { return len(points[i].path) > len(points[j].path) })
		for _, p := range points {
			uerr := sys.unmount(p.path, unix.MNT_DETACH|unix.UMOUNT_NOFOLLOW)
			notMounted := errors.Is(uerr, unix.EINVAL) || errors.Is(uerr, os.ErrNotExist)
			switch {
			case uerr == nil:
				progress = true
			case p.statErr != nil && notMounted:
				errs = append(errs, fmt.Errorf("while checking %q: %w", p.path, p.statErr))
			case p.statErr != nil:
				errs = append(errs, fmt.Errorf("while checking %q: %w", p.path, p.statErr),
					fmt.Errorf("while unmounting %q: %w", p.path, uerr))
			case notMounted:
				// Detached by someone else since the walk, or a mount that
				// cannot be detached. The next walk tells which.
				progress = true
			default:
				errs = append(errs, fmt.Errorf("while unmounting %q: %w", p.path, uerr))
			}
		}
		if !progress {
			return errors.Join(errs...)
		}
	}
}

// statxAt queries file type and mount-root attribute metadata for path relative
// to dirfd without following symlinks, triggering automounts or forcing a
// remote filesystem sync.
//
// STATX_ATTR_MOUNT_ROOT needs Linux 5.8. mountOverlay already needs 6.7 or
// 6.6.23.
func statxAt(dirfd int, path string) (unix.Statx_t, error) {
	var sx unix.Statx_t
	err := unix.Statx(dirfd, path, unix.AT_SYMLINK_NOFOLLOW|unix.AT_NO_AUTOMOUNT|unix.AT_STATX_DONT_SYNC, unix.STATX_TYPE, &sx)
	return sx, err
}

// bundleMountPoints lists which of <dir>/<container>/rootfs and
// <dir>/<container>/volumes/<name> are mount points. SetupBundleRootfs mounts
// nothing else. It skips container and volumes dirs that are symlinks. An
// entry that fails statx is listed with statErr set. Other errors do not stop
// the listing: bundleMountPoints returns the points it found together with
// the joined errors.
func bundleMountPoints(dir string, sys mountSys) ([]mountPoint, error) {
	containers, err := readDirNames(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	var errs []error
	if err != nil {
		errs = append(errs, err)
	}
	var points []mountPoint
	for _, c := range containers {
		bundle := filepath.Join(dir, c)
		if !isDir(sys, bundle, &errs) {
			continue
		}
		points = appendIfMount(points, sys, filepath.Join(bundle, "rootfs"))
		volumes := filepath.Join(bundle, "volumes")
		if !isDir(sys, volumes, &errs) {
			continue
		}
		names, err := readDirNames(volumes)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
		for _, name := range names {
			points = appendIfMount(points, sys, filepath.Join(volumes, name))
		}
	}
	return points, errors.Join(errs...)
}

// readDirNames returns the names in dir without following a symlink at dir.
func readDirNames(dir string) ([]string, error) {
	f, err := os.OpenFile(dir, os.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	names, err := f.Readdirnames(-1)
	if err != nil {
		return names, fmt.Errorf("while reading %q: %w", dir, err)
	}
	return names, nil
}

// isDir reports whether path is a dir and not a symlink. It appends any statx
// error other than ENOENT to errs.
func isDir(sys mountSys, path string, errs *[]error) bool {
	sx, err := sys.statx(unix.AT_FDCWD, path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			*errs = append(*errs, fmt.Errorf("while checking %q: %w", path, err))
		}
		return false
	}
	return sx.Mode&unix.S_IFMT == unix.S_IFDIR
}

// appendIfMount appends path to points if it is a mount root, or with statErr
// set if statx fails on it with anything but ENOENT or ENOTDIR.
func appendIfMount(points []mountPoint, sys mountSys, path string) []mountPoint {
	sx, err := sys.statx(unix.AT_FDCWD, path)
	switch {
	case errors.Is(err, os.ErrNotExist) || errors.Is(err, unix.ENOTDIR):
		return points
	case err != nil:
		return append(points, mountPoint{path: path, statErr: err})
	case sx.Attributes&unix.STATX_ATTR_MOUNT_ROOT != 0:
		return append(points, mountPoint{path: path})
	}
	return points
}

// mountPointsUnder lists the mount points at or below dir using
// STATX_ATTR_MOUNT_ROOT.
//
// An entry that fails statx is listed with statErr set. Errors reading a dir
// do not stop the walk: mountPointsUnder returns the points it found together
// with the joined errors.
func mountPointsUnder(dir string, sys mountSys) ([]mountPoint, error) {
	sx, err := sys.statx(unix.AT_FDCWD, dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return []mountPoint{{path: dir, statErr: err}}, nil
	}
	var points []mountPoint
	if sx.Attributes&unix.STATX_ATTR_MOUNT_ROOT != 0 {
		points = append(points, mountPoint{path: dir})
	}
	var errs []error
	if sx.Mode&unix.S_IFMT == unix.S_IFDIR {
		walkMounts(dir, "", sys, &points, &errs)
	}
	return points, errors.Join(errs...)
}

// walkMounts appends the mount points below root/rel to points and the errors
// for dirs it could not read to errs. It does not follow symlinks or descend
// into mount points. It closes each dir before recursing, so it holds one open
// file at a time.
func walkMounts(root, rel string, sys mountSys, points *[]mountPoint, errs *[]error) {
	curDir := filepath.Join(root, rel)
	f, err := os.OpenFile(curDir, os.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		*errs = append(*errs, err)
		return
	}
	names, err := f.Readdirnames(-1)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		*errs = append(*errs, fmt.Errorf("while reading %q: %w", curDir, err))
	}

	dirfd := int(f.Fd())
	var subdirs []string
	for _, name := range names {
		path := filepath.Join(curDir, name)
		sx, err := sys.statx(dirfd, name)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			*points = append(*points, mountPoint{path: path, statErr: err})
			continue
		}
		if sx.Mode&unix.S_IFMT == unix.S_IFLNK {
			continue
		}
		if sx.Attributes&unix.STATX_ATTR_MOUNT_ROOT != 0 {
			*points = append(*points, mountPoint{path: path})
			continue
		}
		if sx.Mode&unix.S_IFMT == unix.S_IFDIR {
			subdirs = append(subdirs, filepath.Join(rel, name))
		}
	}
	_ = f.Close()

	for _, sub := range subdirs {
		walkMounts(root, sub, sys, points, errs)
	}
}
