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

// Repair of implicit-parent-directory metadata in the composed rootfs.
//
// In overlayfs, the TOP-most layer containing a directory supplies the
// merged directory's attributes. When an upper layer's tar omitted a parent
// entry, the pool holds a fabricated root:root 0755 directory there
// (recorded as ImplicitDirs at unpack time), which would shadow the real
// metadata a lower layer declared — turning /tmp's 1777 into 0755, or
// /root's 0700 into 0755. containerd avoids this structurally by applying
// each layer through the mounted parent chain; this cache unpacks layers
// standalone (atelet cannot mount), so the repair happens at compose time
// instead: after the overlay is mounted, chmod/chown the affected
// directories to the attrs of the top-most NON-implicit provider in this
// image's chain. The writes copy up into the actor's private upper — the
// shared layer pool is never modified, so images with different chains
// sharing these layers are unaffected.
//
// Residual (documented) gaps: directory mtimes and xattrs are not repaired,
// and a directory implicit in every layer of the chain keeps the fabricated
// root:root 0755.

package imagecache

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"syscall"
)

// dirFixup is one merged-view repair: set Path (rootfs-relative) to Mode and,
// when owner info was available, UID/GID.
type dirFixup struct {
	Path     string
	Mode     os.FileMode // permission bits + setuid/setgid/sticky
	UID, GID int
	HasOwner bool
}

// resolveImplicitDirFixups computes the repairs needed for an image whose
// (bottom-first) layer chain is layers: for every directory whose merged
// attributes would come from a layer that only created it implicitly, find
// the top-most layer below it that declared the directory for real and take
// its attributes. Layers whose metadata predates ImplicitDirs recording
// contribute no fixups (nothing recorded ⇒ nothing repaired), so old cached
// layers stay compatible.
func resolveImplicitDirFixups(layers []string) ([]dirFixup, error) {
	implicitAt := make([]map[string]bool, len(layers))
	union := map[string]bool{}
	for i, layerDir := range layers {
		meta, err := readWhiteouts(layerDir)
		if err != nil {
			return nil, err
		}
		set := make(map[string]bool, len(meta.ImplicitDirs))
		for _, p := range meta.ImplicitDirs {
			rel, skip, err := validateTarName(p)
			if err != nil {
				return nil, fmt.Errorf("invalid implicit dir path in %q: %w", layerDir, err)
			}
			if skip {
				continue
			}
			set[rel] = true
			union[rel] = true
		}
		implicitAt[i] = set
	}
	if len(union) == 0 {
		return nil, nil
	}

	paths := make([]string, 0, len(union))
	for p := range union {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	statDir := func(layerIdx int, rel string) (os.FileInfo, bool) {
		fi, err := os.Lstat(filepath.Join(layers[layerIdx], layerFSDirName, rel))
		if err != nil || !fi.IsDir() {
			return nil, false
		}
		return fi, true
	}

	var fixups []dirFixup
	for _, p := range paths {
		// The merged view takes the dir's attrs from the top-most layer
		// containing it; only repair when that provider is implicit.
		top := -1
		for i := range slices.Backward(layers) {
			if _, ok := statDir(i, p); ok {
				top = i
				break
			}
		}
		if top < 0 || !implicitAt[top][p] {
			continue
		}
		for j := top - 1; j >= 0; j-- {
			if implicitAt[j][p] {
				continue
			}
			fi, ok := statDir(j, p)
			if !ok {
				continue
			}
			f := dirFixup{
				Path: p,
				Mode: fi.Mode().Perm() | fi.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky),
			}
			if st, ok := fi.Sys().(*syscall.Stat_t); ok {
				f.UID, f.GID, f.HasOwner = int(st.Uid), int(st.Gid), true
			}
			fixups = append(fixups, f)
			break
		}
	}
	return fixups, nil
}

// applyDirFixups applies the repairs through the (mounted) rootfs at
// rootfsPath; on an overlay each write copies the directory's metadata up
// into the bundle's private upper. Paths hidden in the merged view (removed
// by a whiteout, or shadowed by a non-directory) are skipped. os.Root
// confines the writes to the rootfs.
func applyDirFixups(rootfsPath string, fixups []dirFixup) error {
	if len(fixups) == 0 {
		return nil
	}
	root, err := os.OpenRoot(rootfsPath)
	if err != nil {
		return fmt.Errorf("while opening rootfs %q: %w", rootfsPath, err)
	}
	defer root.Close()

	for _, f := range fixups {
		fi, err := root.Lstat(f.Path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return fmt.Errorf("while checking %q: %w", f.Path, err)
		}
		if !fi.IsDir() {
			continue
		}
		// Chown first: chown can clear setuid/setgid bits, so the chmod
		// must come after it.
		if f.HasOwner {
			if err := root.Chown(f.Path, f.UID, f.GID); err != nil {
				return fmt.Errorf("while restoring owner of %q: %w", f.Path, err)
			}
		}
		if err := root.Chmod(f.Path, f.Mode); err != nil {
			return fmt.Errorf("while restoring mode of %q: %w", f.Path, err)
		}
	}
	return nil
}
