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

package imagecache

import (
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path"
	"slices"
	"syscall"

	erofs "github.com/erofs/go-erofs"
	"golang.org/x/sys/unix"
)

// UpperDialect is the overlay xattr namespace of the mount that will use an
// upper. It names the opaque marker written into it.
type UpperDialect int

const (
	// UpperUserXattr is a userxattr overlay mount (the micro-VM guest's,
	// whose upper lives on virtio-fs): user.overlay.*.
	UpperUserXattr UpperDialect = iota
	// UpperTrustedXattr is a privileged host overlay mount: trusted.overlay.*.
	UpperTrustedXattr
)

func (d UpperDialect) opaqueXattr() string {
	if d == UpperTrustedXattr {
		return overlayOpaqueXattr
	}
	return overlayUserOpaqueXattr
}

// PrepareEROFSUpper creates in upper every directory in need, plus the
// implicit-parent repairs, with the merged layers' attributes (see
// erofsupper.go). Directories already in upper are left as they are.
//
// upper may come from a ROOTFS snapshot in which the actor deleted a planned
// directory. A repair-only path is then left as the actor had it. A needed
// path over a whiteout becomes a directory marked opaque in dialect, as
// overlayfs does for a mkdir over a whiteout; any other non-directory in the
// way of a needed path is an error.
func PrepareEROFSUpper(upper string, spec *OverlaySpec, need []string, dialect UpperDialect) error {
	layers, err := EROFSLowerLayers(spec)
	if err != nil {
		return err
	}
	lowers := make([]fs.FS, 0, len(layers))
	implicit := make([]map[string]bool, 0, len(layers))
	for _, layerDir := range layers {
		f, err := os.Open(LayerEROFSPath(layerDir))
		if err != nil {
			return fmt.Errorf("while opening EROFS image of layer %q: %w", layerDir, err)
		}
		defer f.Close()
		fsys, err := erofs.Open(f)
		if err != nil {
			return fmt.Errorf("while reading EROFS image of layer %q: %w", layerDir, err)
		}
		set, err := implicitDirSet(layerDir)
		if err != nil {
			return err
		}
		lowers = append(lowers, fsys)
		implicit = append(implicit, set)
	}
	dirs, err := planEROFSUpper(newLowerMerge(lowers, implicit), need)
	if err != nil {
		return err
	}
	return applyUpperDirs(upper, dirs, dialect)
}

// applyUpperDirs creates dirs (parents first) inside upper. os.Root confines
// the writes: the upper is the actor's, but nothing in it is trusted.
func applyUpperDirs(upper string, dirs []upperDir, dialect UpperDialect) error {
	root, err := os.OpenRoot(upper)
	if err != nil {
		return fmt.Errorf("while opening upper %q: %w", upper, err)
	}
	defer root.Close()
	// deleted holds paths at or under a non-directory the actor put in the
	// upper; nothing is created below them. opaqueUpper holds upper
	// directories that hide the lowers: repairs under them are skipped and
	// needed subdirectories are created as 0755 without lower attributes.
	deleted := map[string]bool{}
	opaqueUpper := map[string]bool{}
	for _, d := range dirs {
		if deleted[path.Dir(d.Path)] {
			deleted[d.Path] = true
			continue
		}
		if opaqueUpper[path.Dir(d.Path)] {
			opaqueUpper[d.Path] = true
			if !d.Needed {
				continue
			}
			d.HasOwner = false
			d.Mode = 0o755
		}
		opaque := false
		if fi, err := root.Lstat(d.Path); err == nil {
			switch {
			case fi.IsDir():
				if isDirOpaque(root, d.Path, dialect) {
					opaqueUpper[d.Path] = true
				}
				continue
			case !d.Needed:
				// The actor deleted or replaced it, so skip the repair.
				deleted[d.Path] = true
				continue
			case isWhiteout(fi):
				// As in overlayfs, a mkdir over a whiteout replaces it
				// with an opaque directory.
				if err := root.Remove(d.Path); err != nil {
					return fmt.Errorf("while replacing the whiteout at %q in the upper: %w", d.Path, err)
				}
				opaque = true
				opaqueUpper[d.Path] = true
			default:
				return fmt.Errorf("upper already holds a non-directory at %q", d.Path)
			}
		} else if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("while checking %q in the upper: %w", d.Path, err)
		}
		if err := root.Mkdir(d.Path, 0o700); err != nil {
			return fmt.Errorf("while creating %q in the upper: %w", d.Path, err)
		}
		if opaque {
			if err := setDirXattrs(root, d.Path, map[string]string{dialect.opaqueXattr(): "y"}); err != nil {
				return err
			}
		}
		if !d.HasOwner {
			if err := root.Chmod(d.Path, d.Mode); err != nil {
				return fmt.Errorf("while setting mode of %q in the upper: %w", d.Path, err)
			}
			continue
		}
		// Chown first: it clears setuid/setgid, which the chmod restores.
		if err := root.Lchown(d.Path, int(d.UID), int(d.GID)); err != nil {
			return fmt.Errorf("while setting owner of %q in the upper: %w", d.Path, err)
		}
		if err := setDirXattrs(root, d.Path, d.Xattrs); err != nil {
			return err
		}
		if err := root.Chmod(d.Path, d.Mode); err != nil {
			return fmt.Errorf("while setting mode of %q in the upper: %w", d.Path, err)
		}
		if err := root.Chtimes(d.Path, d.Mtime, d.Mtime); err != nil {
			return fmt.Errorf("while setting times of %q in the upper: %w", d.Path, err)
		}
	}
	return nil
}

// isWhiteout reports whether fi is an overlayfs whiteout: a 0:0 character
// device.
func isWhiteout(fi fs.FileInfo) bool {
	st, ok := fi.Sys().(*syscall.Stat_t)
	return ok && fi.Mode()&fs.ModeCharDevice != 0 && st.Rdev == 0
}

// isDirOpaque reports whether the directory rel inside root carries the
// opaque marker in dialect.
func isDirOpaque(root *os.Root, rel string, dialect UpperDialect) bool {
	f, err := root.Open(rel)
	if err != nil {
		return false
	}
	defer f.Close()
	var buf [1]byte
	n, err := unix.Fgetxattr(int(f.Fd()), dialect.opaqueXattr(), buf[:])
	return err == nil && n == 1 && buf[0] == 'y'
}

// setDirXattrs sets xattrs on the directory rel inside root, through an fd
// so no path is re-resolved.
func setDirXattrs(root *os.Root, rel string, xattrs map[string]string) error {
	if len(xattrs) == 0 {
		return nil
	}
	f, err := root.Open(rel)
	if err != nil {
		return fmt.Errorf("while opening %q in the upper: %w", rel, err)
	}
	defer f.Close()
	for _, k := range slices.Sorted(maps.Keys(xattrs)) {
		if err := unix.Fsetxattr(int(f.Fd()), k, []byte(xattrs[k]), 0); err != nil {
			return fmt.Errorf("while setting xattr %q on %q in the upper: %w", k, rel, err)
		}
	}
	return nil
}

// SetupBundleImageVolumes composes the bundle's image volumes when the guest
// mounts the container rootfs from EROFS layers.
func SetupBundleImageVolumes(bundlePath string) error {
	spec, err := ReadSpec(bundlePath)
	if err != nil {
		return err
	}
	if spec == nil {
		return nil
	}
	if err := setupImageVolumes(bundlePath, spec.ImageVolumes); err != nil {
		return fmt.Errorf("while setting up image volumes: %w", err)
	}
	return nil
}
