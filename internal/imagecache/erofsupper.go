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

// Host-side preparation of the writable upper for a guest-assembled EROFS rootfs.
//
// When the guest mounts the rootfs from EROFS layer images, no host mount
// shows the merged tree. PrepareEROFSUpper resolves paths across the images
// with overlayfs lookup rules and creates in the upper:
//   - directories the host writes into before boot (such as etc for resolv.conf)
//   - implicit-parent repairs (see implicitdirs.go), so a lower layer's mode
//     and owner win over a fabricated 0755 parent.

package imagecache

import (
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"path"
	"slices"
	"strings"
	"time"

	erofs "github.com/erofs/go-erofs"
)

// EROFSLowerLayers returns the spec's layer directories top-most first,
// with repeats collapsed (see lowerLayers). Each holds a layer.erofs image.
// It returns an error if spec.EROFS is false.
func EROFSLowerLayers(spec *OverlaySpec) ([]string, error) {
	if !spec.EROFS {
		return nil, errors.New("bundle has no EROFS layer images")
	}
	if len(spec.Layers) == 0 {
		return nil, errors.New("image has no layers")
	}
	return lowerLayers(spec.Layers), nil
}

// upperDir is one directory to create in the upper, with the attributes the
// merged view shows for it.
type upperDir struct {
	Path string // upper-relative, clean
	Mode fs.FileMode
	// Needed marks a directory the consumer asked for (an entry in need or
	// an ancestor of one). Otherwise the directory only repairs shadowed
	// lower attributes.
	Needed bool
	// HasOwner is false for a directory no layer declares. It keeps the
	// creator's owner and the current time.
	HasOwner bool
	UID, GID uint32
	Mtime    time.Time
	Xattrs   map[string]string
}

// mergedKind is what a path resolves to in the merged view of the lowers.
type mergedKind int

const (
	mergedAbsent mergedKind = iota
	mergedDir
	mergedOther // a file, symlink, or device shadows the path
)

// mergedEntry is the merged view of one path.
type mergedEntry struct {
	kind mergedKind
	// dirs are the lowers (indexes into lowerMerge.lowers, top first)
	// holding a directory at this path, down to the first whiteout or
	// non-directory. The first supplies the merged attributes.
	dirs []int
	// children are the lowers a child of this directory can come from:
	// dirs cut at the first opaque directory, inclusive.
	children []int
}

// lowerMerge resolves paths through EROFS layer images with overlayfs lookup
// semantics.
type lowerMerge struct {
	lowers []fs.FS // top-most first
	// implicit[i] holds the directories lowers[i] only created as parents,
	// whose attributes are fabricated (see whiteoutSet.ImplicitDirs).
	implicit []map[string]bool
	cache    map[string]*mergedEntry
}

func newLowerMerge(lowers []fs.FS, implicit []map[string]bool) *lowerMerge {
	return &lowerMerge{lowers: lowers, implicit: implicit, cache: map[string]*mergedEntry{}}
}

// resolve returns the merged view of the clean, relative path rel.
func (m *lowerMerge) resolve(rel string) (*mergedEntry, error) {
	if e, ok := m.cache[rel]; ok {
		return e, nil
	}
	e := &mergedEntry{}
	if rel == "." {
		// The root is a directory in every layer.
		for i := range m.lowers {
			e.dirs = append(e.dirs, i)
		}
		e.kind, e.children = mergedDir, e.dirs
		m.cache[rel] = e
		return e, nil
	}
	parent, err := m.resolve(path.Dir(rel))
	if err != nil {
		return nil, err
	}
	if parent.kind != mergedDir {
		m.cache[rel] = e
		return e, nil
	}
	opaque := false
	for _, i := range parent.children {
		fi, err := fs.Lstat(m.lowers[i], rel)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, fmt.Errorf("while resolving %q in layer %d: %w", rel, i, err)
		}
		st, ok := fi.Sys().(*erofs.Stat)
		if !ok {
			return nil, fmt.Errorf("while resolving %q in layer %d: unexpected %T", rel, i, fi.Sys())
		}
		if fi.Mode()&fs.ModeCharDevice != 0 && st.Rdev == 0 {
			break // a whiteout hides everything below
		}
		if !fi.IsDir() {
			if len(e.dirs) == 0 {
				e.kind = mergedOther
			}
			break
		}
		e.dirs = append(e.dirs, i)
		if !opaque {
			e.children = append(e.children, i)
			opaque = st.Xattrs[overlayOpaqueXattr] == "y" || st.Xattrs[overlayUserOpaqueXattr] == "y"
		}
	}
	if len(e.dirs) > 0 {
		e.kind = mergedDir
	}
	m.cache[rel] = e
	return e, nil
}

// provider returns the layer index supplying the attributes for merged
// directory e at rel, skipping higher layers where rel was created implicitly.
func (m *lowerMerge) provider(e *mergedEntry, rel string) (layer int, repaired bool) {
	top := e.dirs[0]
	if !m.implicit[top][rel] {
		return top, false
	}
	for _, i := range e.dirs[1:] {
		if !m.implicit[i][rel] {
			return i, true
		}
	}
	return top, false
}

// dirAttrs returns the attributes of the directory rel in lower i.
func (m *lowerMerge) dirAttrs(i int, rel string) (upperDir, error) {
	fi, err := fs.Lstat(m.lowers[i], rel)
	if err != nil {
		return upperDir{}, err
	}
	st, ok := fi.Sys().(*erofs.Stat)
	if !ok {
		return upperDir{}, fmt.Errorf("unexpected %T from layer %d", fi.Sys(), i)
	}
	d := upperDir{
		Path:     rel,
		Mode:     fi.Mode() & (fs.ModePerm | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky),
		HasOwner: true,
		UID:      st.UID,
		GID:      st.GID,
		Mtime:    time.Unix(int64(st.Mtime), int64(st.MtimeNs)),
	}
	for k, v := range st.Xattrs {
		if !upperDirXattr(k) {
			continue
		}
		if d.Xattrs == nil {
			d.Xattrs = map[string]string{}
		}
		d.Xattrs[k] = v
	}
	return d, nil
}

// upperDirXattr reports whether a directory xattr from the merged view is
// copied to the upper: user.* and security.capability, the namespaces image
// layers carry. SELinux, IMA and EVM labels are the host's to assign.
func upperDirXattr(attr string) bool {
	if isOverlayXattr(attr) {
		return false
	}
	return strings.HasPrefix(attr, "user.") || attr == "security.capability"
}

// planEROFSUpper returns the directories to create in a fresh upper (parents
// first): every path in need with its ancestors, plus any directory whose
// declared attributes in a lower layer are shadowed by an implicit parent in a
// higher layer.
func planEROFSUpper(m *lowerMerge, need []string) ([]upperDir, error) {
	required := map[string]bool{}
	require := func(rel string) {
		for p := rel; p != "."; p = path.Dir(p) {
			required[p] = true
		}
	}
	for _, d := range need {
		rel := strings.TrimPrefix(path.Clean("/"+d), "/")
		if rel == "" {
			continue
		}
		require(rel)
	}
	needed := maps.Clone(required)

	candidates := map[string]bool{}
	for _, set := range m.implicit {
		for p := range set {
			candidates[p] = true
		}
	}
	for _, p := range sortedKeys(candidates) {
		e, err := m.resolve(p)
		if err != nil {
			return nil, err
		}
		if e.kind != mergedDir {
			continue
		}
		if _, repaired := m.provider(e, p); repaired {
			require(p)
		}
	}

	var dirs []upperDir
	for _, p := range sortedKeys(required) {
		e, err := m.resolve(p)
		if err != nil {
			return nil, err
		}
		switch e.kind {
		case mergedAbsent:
			dirs = append(dirs, upperDir{Path: p, Mode: 0o755, Needed: needed[p]})
		case mergedOther:
			return nil, fmt.Errorf("%q is not a directory in the image", p)
		case mergedDir:
			layer, _ := m.provider(e, p)
			d, err := m.dirAttrs(layer, p)
			if err != nil {
				return nil, fmt.Errorf("while reading %q from layer %d: %w", p, layer, err)
			}
			d.Needed = needed[p]
			dirs = append(dirs, d)
		}
	}
	return dirs, nil
}

// sortedKeys returns the keys of set in lexical order, which puts every
// directory before its descendants.
func sortedKeys(set map[string]bool) []string {
	return slices.Sorted(maps.Keys(set))
}

// implicitDirSet returns the layer's implicit directories as a set of clean
// relative paths.
func implicitDirSet(layerDir string) (map[string]bool, error) {
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
	}
	return set, nil
}
