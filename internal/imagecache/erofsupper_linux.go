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
	"slices"

	erofs "github.com/erofs/go-erofs"
	"golang.org/x/sys/unix"
)

// PrepareEROFSUpper populates a fresh overlay upperdir for a guest-mounted
// EROFS rootfs (see erofsupper.go). It creates every directory in need and any
// implicit-parent repairs using the merged layers' attributes. Existing
// directories in upper are left untouched.
func PrepareEROFSUpper(upper string, spec *OverlaySpec, need []string) error {
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
	return applyUpperDirs(upper, dirs)
}

// applyUpperDirs creates dirs (parents first) inside upper. os.Root confines
// the writes: the upper is the actor's, but nothing in it is trusted.
func applyUpperDirs(upper string, dirs []upperDir) error {
	root, err := os.OpenRoot(upper)
	if err != nil {
		return fmt.Errorf("while opening upper %q: %w", upper, err)
	}
	defer root.Close()
	for _, d := range dirs {
		if fi, err := root.Lstat(d.Path); err == nil {
			if fi.IsDir() {
				continue
			}
			return fmt.Errorf("upper already holds a non-directory at %q", d.Path)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("while checking %q in the upper: %w", d.Path, err)
		}
		if err := root.Mkdir(d.Path, 0o700); err != nil {
			return fmt.Errorf("while creating %q in the upper: %w", d.Path, err)
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
