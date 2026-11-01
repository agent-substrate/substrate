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
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"go.ate.dev/substrate/internal/tarutil"
)

const (
	// whiteoutPrefix marks an OCI layer entry that deletes the same-named
	// path from lower layers.
	whiteoutPrefix = ".wh."
	// opaqueMarkerName marks its directory as opaque: lower-layer contents of
	// that directory are hidden entirely.
	opaqueMarkerName = ".wh..wh..opq"
)

// whiteoutSet records per-layer metadata captured at unpack time that the
// (privileged) consumer needs at compose time: whiteout state materialized
// by FinalizeLayer, and the directories this layer only created implicitly.
// Paths are clean and relative to the layer's fs/ root.
type whiteoutSet struct {
	Version int `json:"version"`
	// Whiteouts are paths that must become 0:0 char devices in the lowerdir.
	Whiteouts []string `json:"whiteouts,omitempty"`
	// Opaques are directories that must carry trusted.overlay.opaque=y.
	Opaques []string `json:"opaques,omitempty"`
	// ImplicitDirs are directories the layer tar never declared but that
	// exist in the tree because a child entry (or a whiteout materialized at
	// finalize) needed a parent. Their root:root 0755 attrs are fabricated;
	// in the composed overlay the top-most layer containing a directory
	// supplies its metadata, so an implicit dir here would shadow the real
	// attrs a lower layer declared (e.g. /tmp's 1777). SetupBundleRootfs
	// repairs the merged view from these records (see resolveImplicitDirFixups).
	ImplicitDirs []string `json:"implicitDirs,omitempty"`
}

func readWhiteouts(layerDir string) (*whiteoutSet, error) {
	b, err := os.ReadFile(filepath.Join(layerDir, layerWhiteoutsFileName))
	if errors.Is(err, os.ErrNotExist) {
		return &whiteoutSet{Version: 1}, nil
	} else if err != nil {
		return nil, fmt.Errorf("while reading layer whiteouts: %w", err)
	}
	var wh whiteoutSet
	if err := json.Unmarshal(b, &wh); err != nil {
		return nil, fmt.Errorf("while decoding layer whiteouts: %w", err)
	}
	return &wh, nil
}

func validateTarName(name string) (cleaned string, skip bool, err error) {
	if name == "" {
		return "", true, nil
	}
	cleaned = filepath.Clean(name)
	if cleaned == "." {
		return "", true, nil
	}
	cleaned = strings.TrimPrefix(cleaned, "/")
	if cleaned == "" || cleaned == "." {
		return "", true, nil
	}
	if !filepath.IsLocal(cleaned) {
		return "", false, fmt.Errorf("not a local path: %q", name)
	}
	return cleaned, false, nil
}

// unpackLayer extracts one uncompressed OCI layer tar into root. Whiteout
// entries (.wh.*) are not written to the tree; they are returned so the
// caller can persist them for later materialization by a privileged process.
//
// Every entry keeps the numeric owner its tar header records, as containerd
// and moby apply it, so image content given to a non-root user (a home
// directory, an application's data dir) belongs to that user inside the
// actor. Hardlinks apply their header's owner to the shared inode, as those
// runtimes do. Run as root, a failed chown or chmod fails the unpack, so a
// layer never lands in the pool with an owner or mode silently dropped. An
// unprivileged process (unit tests, local tooling) cannot chown, so its
// extracted tree belongs to the extracting user (see tarutil.Lchown); New
// warns when the store runs that way.
//
// Unlike a flattened-image extract, cross-layer "later entry wins" semantics
// are overlayfs's job now; the handling here only needs to cope with
// duplicate entries within a single layer (real ko images repeat directory
// entries).
func unpackLayer(ctx context.Context, tarData io.Reader, root *os.Root) (*whiteoutSet, error) {
	wh := &whiteoutSet{Version: 1}

	// Directories are created root-owned and owner-writable, so their children
	// can be written whatever mode or owner the image gives them, and get
	// their real mode, mtime and owner in the restore pass below. Headers are
	// kept by directory, not by name: each directory takes the last header
	// written to it under any name.
	dirHdrs := map[dirInode]*dirHdr{}

	// Ancestors an entry needed vs. directories the tar declared: the
	// difference is recorded as ImplicitDirs (attrs fabricated, see the
	// whiteoutSet field doc). Whiteout/opaque markers count too — their
	// parents are created by FinalizeLayer's MkdirAll with the same
	// fabricated attrs.
	declared := map[string]bool{}
	implicit := map[string]bool{}
	markAncestors := func(name string) {
		for p := filepath.Dir(name); p != "."; p = filepath.Dir(p) {
			if !declared[p] {
				implicit[p] = true
			}
		}
	}

	// replaceExisting removes whatever is at name ("later entry wins" within a
	// layer) and drops the deferred restore of every directory it removes,
	// under any name, so the restore pass applies none of their headers to a
	// directory created later.
	replaceExisting := func(name, before string) error {
		if fi, err := root.Lstat(name); err == nil {
			var removed map[dirInode]string
			if fi.IsDir() {
				if removed, err = dirPaths(root, name); err != nil {
					return fmt.Errorf("while replacing existing path at %q before %s: %w", name, before, err)
				}
			}
			if err := root.RemoveAll(name); err != nil {
				return fmt.Errorf("while replacing existing path at %q before %s: %w", name, before, err)
			}
			for inode := range removed {
				delete(dirHdrs, inode)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("while checking existing path at %q before %s: %w", name, before, err)
		}
		return nil
	}

	tarReader := tar.NewReader(tarData)
	for {
		hdr, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, fmt.Errorf("in tarReader.Next: %w", err)
		}

		name, skip, err := validateTarName(hdr.Name)
		if err != nil {
			return nil, fmt.Errorf("invalid tar entry: %w", err)
		}
		if skip {
			continue
		}

		if base := filepath.Base(name); base == opaqueMarkerName {
			if dir := filepath.Dir(name); dir != "." {
				wh.Opaques = append(wh.Opaques, dir)
			}
			markAncestors(name)
			continue
		} else if strings.HasPrefix(base, whiteoutPrefix+whiteoutPrefix) {
			// AUFS bookkeeping entries (.wh..wh.plnk, .wh..wh.aufs, ...);
			// nothing to represent in an overlayfs lowerdir.
			continue
		} else if deleted, ok := strings.CutPrefix(base, whiteoutPrefix); ok {
			wh.Whiteouts = append(wh.Whiteouts, filepath.Join(filepath.Dir(name), deleted))
			markAncestors(name)
			continue
		}

		// Skip device nodes like rootless containerd does: an image must never
		// grant device access, and the runtime provides /dev.
		if hdr.Typeflag == tar.TypeChar || hdr.Typeflag == tar.TypeBlock {
			if err := replaceExisting(name, "device node"); err != nil {
				return nil, err
			}
			slog.DebugContext(ctx, "Skipping device node in image layer", slog.String("name", name))
			continue
		}

		mode := hdr.FileInfo().Mode().Perm()

		// A layer tar routinely omits entries for parent directories that
		// exist in lower layers (e.g. just "etc/nsswitch.conf", with "etc/"
		// declared only in the base layer). Unpacking per layer, those
		// parents must be created here; overlayfs merges them with the
		// lower layers' directories at compose time.
		markAncestors(name)
		if parent := filepath.Dir(name); parent != "." {
			if err := root.MkdirAll(parent, 0o755); err != nil {
				return nil, fmt.Errorf("while creating parent directories for %q: %w", name, err)
			}
		}

		switch hdr.Typeflag {
		case tar.TypeReg: // Regular file
			// "Later entry wins" within the layer: if any entry exists at the target
			// path, remove it first. This ensures that:
			// 1. If it's a symlink, we don't write through it (security vulnerability / incorrectness).
			// 2. If it's a hardlink, we unlink it instead of truncating the shared inode.
			// 3. If it's a directory, we recursively remove it so we can write the file.
			if err := replaceExisting(name, "regular file"); err != nil {
				return nil, err
			}

			// Stream directly from tarReader to target file to avoid buffering in memory.
			outFile, err := root.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_TRUNC, mode)
			if err != nil {
				return nil, fmt.Errorf("while creating file %q: %w", name, err)
			}
			_, err = io.Copy(outFile, tarReader)
			closeErr := outFile.Close()
			if err != nil {
				return nil, fmt.Errorf("while writing contents of %q from tar stream: %w", name, err)
			}
			if closeErr != nil {
				return nil, fmt.Errorf("while closing file %q: %w", name, closeErr)
			}
			// OpenFile's mode passed through atelet's umask and carries no
			// setuid, setgid or sticky bit, and chown clears setuid and
			// setgid, so the header's mode goes after the owner.
			if err := tarutil.Lchown(root, name, hdr); err != nil {
				return nil, err
			}
			if err := setFileMode(root, name, tarutil.Mode(hdr)); err != nil {
				return nil, err
			}
			if err := restoreModTime(root, name, hdr.ModTime); err != nil {
				return nil, err
			}

		case tar.TypeDir:
			// Create owner-writable so children can be written even when the image
			// marks the dir read-only; the real mode is restored after extraction
			// (see dirHdrs / the restore pass below).
			err := root.Mkdir(name, mode|0o700)
			if errors.Is(err, os.ErrExist) {
				// OCI layers can repeat a directory entry (real ko images do); the
				// existing dir is already owner-writable, so let the later entry's
				// mode win at restore time.
			} else if err != nil {
				return nil, fmt.Errorf("while creating directory=%q, mode=%v: %w", name, mode, err)
			}
			// The header belongs to the directory now at name; Mkdir may
			// have found a non-directory there, which keeps no header.
			fi, err := root.Lstat(name)
			if err != nil {
				return nil, fmt.Errorf("while checking directory %q: %w", name, err)
			}
			if fi.IsDir() {
				inode, err := dirInodeOf(name, fi)
				if err != nil {
					return nil, err
				}
				dh := dirHdrs[inode]
				if dh == nil {
					dh = &dirHdr{}
					dirHdrs[inode] = dh
				}
				dh.hdr = hdr
				dh.names = append(dh.names, name)
			}
			declared[name] = true
			delete(implicit, name)

		case tar.TypeSymlink:
			// A layer may re-define the same path (e.g. declare /var/run as a dir
			// then re-declare it as a symlink). Standard tar-extract semantics are
			// "later entry wins": replace any existing entry.
			same := false
			if existing, err := root.Lstat(name); err == nil {
				// If it's already the same symlink, skip the unlink+symlink pair.
				if existing.Mode()&os.ModeSymlink != 0 {
					if cur, rerr := root.Readlink(name); rerr == nil && cur == hdr.Linkname {
						same = true
					}
				}
			}
			if !same {
				// Root.RemoveAll removes the symlink entry itself; it does NOT
				// traverse and remove the directory the symlink points to.
				// That's the desired semantic here — replace this path's
				// entry without touching whatever the prior symlink targeted.
				if err := replaceExisting(name, "symlink"); err != nil {
					return nil, err
				}
				if err := root.Symlink(hdr.Linkname, name); err != nil {
					return nil, fmt.Errorf("while creating symlink src=%q target=%q: %w", name, hdr.Linkname, err)
				}
			}
			// Even an unchanged symlink takes the later entry's owner.
			if err := tarutil.Lchown(root, name, hdr); err != nil {
				return nil, err
			}
			// The symlink's own mtime is not restored: Chtimes follows the link,
			// so it would stamp the target instead.

		case tar.TypeLink:
			linkname, linkSkip, err := validateTarName(hdr.Linkname)
			if err != nil {
				return nil, fmt.Errorf("invalid hardlink target for %q: %w", name, err)
			}
			if linkSkip {
				return nil, fmt.Errorf("invalid hardlink target for %q: empty", name)
			}
			// Same "later entry wins" handling as TypeSymlink: replace existing entry.
			if err := replaceExisting(name, "hardlink"); err != nil {
				return nil, err
			}
			// The chown below gives the shared inode this header's owner.
			if err := root.Link(linkname, name); err != nil {
				return nil, fmt.Errorf("while creating hardlink src=%q target=%q: %w", name, linkname, err)
			}
			// The link shares its target's inode, so the chown below clears the
			// setuid and setgid bits the target entry set; keep its mode to
			// put them back.
			target, err := root.Lstat(name)
			if err != nil {
				return nil, fmt.Errorf("while checking hardlink %q: %w", name, err)
			}
			if err := tarutil.Lchown(root, name, hdr); err != nil {
				return nil, err
			}
			if target.Mode().IsRegular() {
				if err := setFileMode(root, name, target.Mode()&tarutil.ModeBits); err != nil {
					return nil, err
				}
			}
			// No mtime to restore: the link shares the target's inode, which
			// already has the target entry's mtime.

		default:
			tfStr := string([]byte{hdr.Typeflag})
			slog.ErrorContext(ctx, "Unhandled tar entry typeflag", slog.String("typeflag", tfStr), slog.String("name", name), slog.Any("hdr", hdr))
			return nil, fmt.Errorf("unhandled tar entry typeflag %q at %q", tfStr, name)
		}
	}

	// Restore directory modes, mtimes and owners now that every child exists.
	// Deepest (longest name) first, so a parent's restored mode or owner can't
	// block restoring its children. Chown keeps a directory's setgid and
	// sticky bits, so it can go last.
	type dirRestore struct {
		name string
		hdr  *tar.Header
	}
	dirs := make([]dirRestore, 0, len(dirHdrs))
	unnamed := map[dirInode]*tar.Header{}
	for inode, dh := range dirHdrs {
		name := ""
		for i := len(dh.names) - 1; i >= 0 && name == ""; i-- {
			// Any error means the name does not reach the directory (it may
			// lead outside the layer or into a loop).
			fi, err := root.Lstat(dh.names[i])
			if err != nil || !fi.IsDir() {
				continue
			}
			if got, err := dirInodeOf(dh.names[i], fi); err == nil && got == inode {
				name = dh.names[i]
			}
		}
		if name == "" {
			unnamed[inode] = dh.hdr
			continue
		}
		dirs = append(dirs, dirRestore{name: name, hdr: dh.hdr})
	}
	if len(unnamed) > 0 {
		paths, err := dirPaths(root, ".")
		if err != nil {
			return nil, fmt.Errorf("while finding unpacked directories: %w", err)
		}
		for inode, hdr := range unnamed {
			if name, ok := paths[inode]; ok {
				dirs = append(dirs, dirRestore{name: name, hdr: hdr})
			}
		}
	}
	sort.Slice(dirs, func(i, j int) bool { return len(dirs[i].name) > len(dirs[j].name) })
	for _, d := range dirs {
		name, hdr := d.name, d.hdr
		mode := tarutil.Mode(hdr)
		if err := root.Chmod(name, mode); err != nil {
			return nil, fmt.Errorf("while restoring mode %v on directory %q: %w", mode, name, err)
		}
		if err := restoreModTime(root, name, hdr.ModTime); err != nil {
			return nil, err
		}
		if err := tarutil.Lchown(root, name, hdr); err != nil {
			return nil, err
		}
	}

	// Keep only implicit candidates that survive in the tree as directories
	// ("later entry wins" may have replaced one with a file or symlink), plus
	// the ones FinalizeLayer will create for whiteout/opaque materialization
	// (they may not exist yet). Sorted for deterministic metadata.
	finalizeDirs := map[string]bool{}
	for _, w := range wh.Whiteouts {
		for p := filepath.Dir(w); p != "."; p = filepath.Dir(p) {
			finalizeDirs[p] = true
		}
	}
	for _, o := range wh.Opaques {
		for p := o; p != "."; p = filepath.Dir(p) {
			finalizeDirs[p] = true
		}
	}
	for p := range implicit {
		if declared[p] {
			continue
		}
		if fi, err := root.Lstat(p); err == nil && fi.IsDir() {
			wh.ImplicitDirs = append(wh.ImplicitDirs, p)
		} else if finalizeDirs[p] {
			wh.ImplicitDirs = append(wh.ImplicitDirs, p)
		}
	}
	sort.Strings(wh.ImplicitDirs)

	return wh, nil
}

// dirHdr is the header written last to a directory and every name it was
// written under, in tar order.
type dirHdr struct {
	hdr   *tar.Header
	names []string
}

// dirInode identifies a directory by device and inode number.
type dirInode struct{ dev, ino uint64 }

// dirInodeOf returns the device and inode number of name, whose Lstat result
// is fi.
func dirInodeOf(name string, fi os.FileInfo) (dirInode, error) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return dirInode{}, fmt.Errorf("while checking directory %q: no inode in %T", name, fi.Sys())
	}
	return dirInode{dev: uint64(st.Dev), ino: st.Ino}, nil
}

// dirPaths returns the directories at and below dir, a directory in root,
// with the path of each, without following symlinks. It reads directories
// through root rather than root.FS(), which rejects names that are not valid
// UTF-8 and tar allows.
func dirPaths(root *os.Root, dir string) (map[dirInode]string, error) {
	paths := map[dirInode]string{}
	for pending := []string{dir}; len(pending) > 0; {
		name := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		fi, err := root.Lstat(name)
		if err != nil {
			return nil, err
		}
		inode, err := dirInodeOf(name, fi)
		if err != nil {
			return nil, err
		}
		paths[inode] = name
		f, err := root.Open(name)
		if err != nil {
			return nil, err
		}
		entries, err := f.ReadDir(-1)
		f.Close()
		if err != nil {
			return nil, fmt.Errorf("while reading directory %q: %w", name, err)
		}
		for _, e := range entries {
			if e.IsDir() {
				pending = append(pending, filepath.Join(name, e.Name()))
			}
		}
	}
	return paths, nil
}

// setFileMode sets name, a regular file whose owner was just applied, to
// mode. Without CAP_FSETID, chmod drops setgid for a group the caller is not
// in instead of failing, so a setgid mode is read back.
func setFileMode(root *os.Root, name string, mode os.FileMode) error {
	if err := root.Chmod(name, mode); err != nil {
		return fmt.Errorf("while setting mode %v on %q: %w", mode, name, err)
	}
	if mode&os.ModeSetgid == 0 {
		return nil
	}
	fi, err := root.Lstat(name)
	if err != nil {
		return fmt.Errorf("while checking mode of %q: %w", name, err)
	}
	if got := fi.Mode() & tarutil.ModeBits; got != mode {
		return fmt.Errorf("mode of %q is %v after setting %v: keeping setgid for a group the caller is not in takes CAP_FSETID", name, got, mode)
	}
	return nil
}

// restoreModTime sets the atime and mtime of an unpacked path to the mtime its
// tar entry records, so that every node unpacks a layer to the same tree (see
// the deterministic-unpack note in cmd/ateworker-microvm/restore.go).
func restoreModTime(root *os.Root, name string, modTime time.Time) error {
	if modTime.IsZero() {
		return nil
	}
	if err := root.Chtimes(name, modTime, modTime); err != nil {
		return fmt.Errorf("while restoring mtime on %q: %w", name, err)
	}
	return nil
}
