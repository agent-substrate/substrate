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
// privileged worker pods (which own all mounts on the node), never in atelet.

package imagecache

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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
func SetupBundleRootfs(bundlePath string) (retErr error) {
	spec, err := ReadSpec(bundlePath)
	if err != nil {
		return err
	}
	if spec == nil {
		return nil
	}
	if !spec.EROFS {
		for _, layerDir := range spec.Layers {
			if err := FinalizeLayer(layerDir); err != nil {
				return fmt.Errorf("while finalizing layer %q: %w", layerDir, err)
			}
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
	// path (e.g. a run that failed between mount and teardown).
	_ = UnmountAllUnder(bundlePath)
	defer func() {
		if retErr != nil {
			_ = UnmountAllUnder(bundlePath)
		}
	}()

	if len(spec.Layers) == 0 {
		// Degenerate zero-layer image: the empty rootfs dir plus ExtraDirs is
		// all there is.
		return createExtraDirs(rootfs, spec.ExtraDirs)
	}

	var lowers []string
	if spec.EROFS {
		if err := PrepareEROFSUpper(upper, spec, nil); err != nil {
			return fmt.Errorf("while repairing implicit dir metadata: %w", err)
		}
		lowers, err = mountEROFSLayers(filepath.Join(bundlePath, "layers"), spec.Layers)
		if err != nil {
			return err
		}
	} else {
		lowers = overlayLowerDirs(spec.Layers)
	}

	if err := mountOverlay(rootfs, lowers, upper, work); err != nil {
		return fmt.Errorf("while mounting overlay rootfs at %q: %w", rootfs, err)
	}

	if err := createExtraDirs(rootfs, spec.ExtraDirs); err != nil {
		return err
	}

	if !spec.EROFS {
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
		if !vol.EROFS {
			for _, layerDir := range vol.Layers {
				if err := FinalizeLayer(layerDir); err != nil {
					return fmt.Errorf("while finalizing layer %q of image volume %q: %w", layerDir, vol.Name, err)
				}
			}
		}

		mountpoint := ImageVolumeMountPath(bundlePath, vol.Name)
		if err := os.MkdirAll(mountpoint, 0o700); err != nil {
			return fmt.Errorf("while creating image volume mount point %q: %w", mountpoint, err)
		}
		_ = unix.Unmount(mountpoint, unix.MNT_DETACH)

		if err := mountImageVolume(bundlePath, mountpoint, vol); err != nil {
			return fmt.Errorf("while mounting image volume %q: %w", vol.Name, err)
		}
	}
	return nil
}

// mountImageVolume attaches vol's layers read-only at mountpoint.
func mountImageVolume(bundlePath, mountpoint string, vol ImageVolumeOverlay) error {
	if vol.EROFS {
		layers := lowerLayers(vol.Layers)
		if len(layers) == 1 {
			return mountEROFS(LayerEROFSPath(layers[0]), mountpoint)
		}
		lowers, err := mountEROFSLayers(filepath.Join(bundlePath, "volumes-layers", vol.Name), vol.Layers)
		if err != nil {
			return err
		}
		return mountOverlay(mountpoint, lowers, "", "")
	}
	if len(vol.Layers) == 1 {
		// The kernel rejects a single lowerdir without an upperdir, so bind
		// the layer's fs/ tree directly for the single-layer case.
		fsDir := filepath.Join(vol.Layers[0], layerFSDirName)
		if err := unix.Mount(fsDir, mountpoint, "", unix.MS_BIND|unix.MS_REC, ""); err != nil {
			return fmt.Errorf("while binding %q: %w", fsDir, err)
		}
		// MS_BIND and MS_RDONLY cannot be combined; a second call applies read-only.
		if err := unix.Mount("", mountpoint, "", unix.MS_REMOUNT|unix.MS_BIND|unix.MS_RDONLY, ""); err != nil {
			return fmt.Errorf("while remounting %q read-only: %w", mountpoint, err)
		}
		return nil
	}
	return mountOverlay(mountpoint, overlayLowerDirs(vol.Layers), "", "")
}

// mountEROFSLayers mounts each distinct layer's EROFS image read-only under
// mountRoot and returns the mount points in overlayfs top-most-first order.
func mountEROFSLayers(mountRoot string, layers []string) ([]string, error) {
	unique := lowerLayers(layers)
	lowers := make([]string, len(unique))
	for i, layerDir := range unique {
		mp := filepath.Join(mountRoot, strconv.Itoa(i))
		if err := os.MkdirAll(mp, 0o700); err != nil {
			return nil, fmt.Errorf("while creating EROFS mount point %q: %w", mp, err)
		}
		_ = unix.Unmount(mp, unix.MNT_DETACH)
		if err := mountEROFS(LayerEROFSPath(layerDir), mp); err != nil {
			return nil, fmt.Errorf("while mounting EROFS layer %q: %w", layerDir, err)
		}
		lowers[i] = mp
	}
	return lowers, nil
}

// HostMountsEROFS reports whether this kernel can mount EROFS: the filesystem
// is built in or loadable. A kernel without it (CONFIG_EROFS_FS unset, as on
// Container-Optimized OS) answers fsopen with ENODEV, and no fallback can
// mount the images on the host; they can still be mapped into a guest.
func HostMountsEROFS() bool {
	fsfd, err := unix.Fsopen("erofs", unix.FSOPEN_CLOEXEC)
	if err != nil {
		return false
	}
	unix.Close(fsfd)
	return true
}

// mountEROFS mounts imagePath read-only at mountpoint, using a file-backed
// EROFS mount when the kernel supports it (Linux 6.12+) and falling back to an
// autoclear read-only loop device on older kernels.
func mountEROFS(imagePath, mountpoint string) error {
	err := mountEROFSFromSource(imagePath, mountpoint)
	if err == nil || !errors.Is(err, unix.ENOTBLK) {
		return err
	}
	return mountEROFSWithLoop(imagePath, mountpoint)
}

func mountEROFSFromSource(source, mountpoint string) error {
	fsfd, err := unix.Fsopen("erofs", unix.FSOPEN_CLOEXEC)
	if err != nil {
		return fmt.Errorf("while opening erofs fs context: %w", err)
	}
	defer unix.Close(fsfd)

	if err := unix.FsconfigSetString(fsfd, "source", source); err != nil {
		return fmt.Errorf("while setting erofs source=%q: %w%s", source, err, fsContextLog(fsfd))
	}
	if err := unix.FsconfigSetFlag(fsfd, "ro"); err != nil {
		return fmt.Errorf("while setting erofs ro: %w%s", err, fsContextLog(fsfd))
	}
	if err := unix.FsconfigCreate(fsfd); err != nil {
		return fmt.Errorf("while creating erofs superblock: %w%s", err, fsContextLog(fsfd))
	}
	mfd, err := unix.Fsmount(fsfd, unix.FSMOUNT_CLOEXEC, unix.MOUNT_ATTR_RDONLY)
	if err != nil {
		return fmt.Errorf("while creating erofs mount object: %w%s", err, fsContextLog(fsfd))
	}
	defer unix.Close(mfd)
	if err := unix.MoveMount(mfd, "", unix.AT_FDCWD, mountpoint, unix.MOVE_MOUNT_F_EMPTY_PATH); err != nil {
		return fmt.Errorf("while attaching erofs at %q: %w", mountpoint, err)
	}
	return nil
}

func mountEROFSWithLoop(imagePath, mountpoint string) error {
	imgFile, err := os.Open(imagePath)
	if err != nil {
		return fmt.Errorf("while opening EROFS image %q: %w", imagePath, err)
	}
	defer imgFile.Close()

	ctl, err := os.OpenFile("/dev/loop-control", os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("while opening /dev/loop-control: %w", err)
	}
	defer ctl.Close()

	for range 16 {
		num, err := unix.IoctlRetInt(int(ctl.Fd()), unix.LOOP_CTL_GET_FREE)
		if err != nil {
			return fmt.Errorf("while allocating loop device: %w", err)
		}
		devPath := fmt.Sprintf("/dev/loop%d", num)
		loopFile, err := os.OpenFile(devPath, os.O_RDONLY, 0)
		if err != nil {
			return fmt.Errorf("while opening %s: %w", devPath, err)
		}
		cfg := unix.LoopConfig{
			Fd:   uint32(imgFile.Fd()),
			Size: erofsBlockSize,
			Info: unix.LoopInfo64{
				Flags: unix.LO_FLAGS_READ_ONLY | unix.LO_FLAGS_AUTOCLEAR,
			},
		}
		err = unix.IoctlLoopConfigure(int(loopFile.Fd()), &cfg)
		if errors.Is(err, unix.EBUSY) {
			loopFile.Close()
			continue
		}
		if err != nil {
			loopFile.Close()
			return fmt.Errorf("while configuring %s for %q: %w", devPath, imagePath, err)
		}
		mountErr := mountEROFSFromSource(devPath, mountpoint)
		loopFile.Close()
		return mountErr
	}
	return fmt.Errorf("while allocating loop device for %q: all candidates busy", imagePath)
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
// dirs. This runs in worker rather than atelet because mknod needs CAP_MKNOD
// and trusted.* xattrs need CAP_SYS_ADMIN, both of which atelet deliberately
// drops.
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
