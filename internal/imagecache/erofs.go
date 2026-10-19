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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	erofs "github.com/erofs/go-erofs"
)

const (
	// layerEROFSFileName is the EROFS image of a layer, built when EROFS
	// layers are enabled.
	layerEROFSFileName = "layer.erofs"

	// erofsBlockSize must match the guest page size for DAX mounts.
	erofsBlockSize = 4096

	// erofsImageAlign is the 2 MiB size alignment Cloud Hypervisor requires
	// for virtio-pmem backing files.
	erofsImageAlign = 2 << 20

	// layerEROFSDigestFileName holds "sha256:<hex>" of layer.erofs so restore
	// can verify the local image matches the snapshot's.
	layerEROFSDigestFileName = layerEROFSFileName + ".sha256"

	// overlayOpaqueXattr marks a directory opaque to an overlayfs mounted
	// without userxattr; overlayUserOpaqueXattr is the same marker for a
	// mount with it. The kernel reads whichever prefix the mount option
	// selects on every layer, lowers included, so an image carries both.
	overlayOpaqueXattr     = "trusted.overlay.opaque"
	overlayUserOpaqueXattr = "user.overlay.opaque"

	// paxXattrPrefix prefixes PAX records carrying extended attributes.
	paxXattrPrefix = "SCHILY.xattr."

	// statTypeChrdev is S_IFCHR (a 0:0 char device is an overlayfs whiteout).
	statTypeChrdev = 0o020000
)

// LayerEROFSPath returns the EROFS image path inside a cached layer directory.
func LayerEROFSPath(layerDir string) string {
	return filepath.Join(layerDir, layerEROFSFileName)
}

// LayerEROFSDigest reads the recorded "sha256:<hex>" digest of the layer's
// EROFS image.
func LayerEROFSDigest(layerDir string) (string, error) {
	b, err := os.ReadFile(filepath.Join(layerDir, layerEROFSDigestFileName))
	if err != nil {
		return "", fmt.Errorf("while reading EROFS image digest: %w", err)
	}
	return strings.TrimSpace(string(b)), nil
}

// writeLayerEROFSDigest records the image's digest beside it.
func writeLayerEROFSDigest(layerDir, digest string) error {
	return os.WriteFile(filepath.Join(layerDir, layerEROFSDigestFileName), []byte(digest+"\n"), 0o600)
}

// buildLayerEROFS writes an uncompressed OCI layer tar as a reproducible,
// 2 MiB-aligned EROFS image into out and returns its size, sha256 digest, and
// per-layer implicit-directory metadata.
//
// It applies unpackLayer's entry rules (later entry wins within a layer,
// device nodes skipped, parents resolved through in-image symlinks), with
// these differences: whiteouts become 0:0 char devices and opaque markers
// become trusted.overlay.opaque=y and user.overlay.opaque=y on the directory
// (fs/ records both in whiteouts.json for a privileged pass), and ownership,
// mode bits, mtime and non-overlay xattrs are written as the tar states them
// (fs/ keeps what an unprivileged unpack can). A whiteout applies to lower
// layers only: an entry earlier in the same layer at its path survives, and
// a same-layer directory is marked opaque instead of removed.
func buildLayerEROFS(tarData io.Reader, out *os.File, tmpDir string) (size int64, digest string, wh *whiteoutSet, err error) {
	wh = &whiteoutSet{Version: 1}
	declared := map[string]bool{}
	implicit := map[string]bool{}
	markAncestors := func(name string) {
		for p := filepath.Dir(name); p != "."; p = filepath.Dir(p) {
			if !declared[p] {
				implicit[p] = true
			}
		}
	}

	w := erofs.Create(out, erofs.WithBlockSize(erofsBlockSize), erofs.WithTempDir(tmpDir), erofs.WithBuildTime(0, 0), erofs.WithNoInlineData())

	tarReader := tar.NewReader(tarData)
	for {
		hdr, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return 0, "", nil, fmt.Errorf("in tarReader.Next: %w", err)
		}

		name, skip, err := validateTarName(hdr.Name)
		if err != nil {
			return 0, "", nil, fmt.Errorf("invalid tar entry: %w", err)
		}
		if skip {
			continue
		}

		// Resolve the parent the way os.Root does for fs/: through symlinks
		// that stay inside the image. The final component is never followed,
		// so an entry replaces a symlink at its path rather than writing
		// through it.
		base := filepath.Base(name)
		parent, err := erofsResolve(w, filepath.Dir(name))
		if err != nil {
			return 0, "", nil, fmt.Errorf("while resolving parent of %q: %w", name, err)
		}
		name = filepath.Join(parent, base)

		if base == opaqueMarkerName {
			markAncestors(name)
			if parent != "." {
				if err := erofsEnsureDir(w, parent); err != nil {
					return 0, "", nil, fmt.Errorf("while marking %q opaque: %w", parent, err)
				}
				for _, attr := range []string{overlayOpaqueXattr, overlayUserOpaqueXattr} {
					if err := w.Setxattr(parent, attr, "y"); err != nil {
						return 0, "", nil, fmt.Errorf("while marking %q opaque: %w", parent, err)
					}
				}
			}
			continue
		} else if strings.HasPrefix(base, whiteoutPrefix+whiteoutPrefix) {
			continue
		} else if deleted, ok := strings.CutPrefix(base, whiteoutPrefix); ok {
			if err := validateWhiteoutName(deleted); err != nil {
				return 0, "", nil, fmt.Errorf("invalid tar entry %q: %w", hdr.Name, err)
			}
			target := filepath.Join(parent, deleted)
			markAncestors(target)
			if err := erofsEnsureDir(w, parent); err != nil {
				return 0, "", nil, fmt.Errorf("while creating whiteout %q: %w", target, err)
			}
			// OCI whiteouts hide lower layers only. This layer's own entry
			// at the path stays: a directory becomes opaque so the lower
			// one is hidden under it, anything else makes the whiteout moot.
			if fi, err := w.Lstat(target); err == nil {
				if fi.IsDir() {
					for _, attr := range []string{overlayOpaqueXattr, overlayUserOpaqueXattr} {
						if err := w.Setxattr(target, attr, "y"); err != nil {
							return 0, "", nil, fmt.Errorf("while marking %q opaque for whiteout: %w", target, err)
						}
					}
				}
				continue
			} else if !errors.Is(err, fs.ErrNotExist) {
				return 0, "", nil, fmt.Errorf("while checking whiteout target %q: %w", target, err)
			}
			if err := w.Mknod(target, statTypeChrdev, 0); err != nil {
				return 0, "", nil, fmt.Errorf("while creating whiteout %q: %w", target, err)
			}
			continue
		}

		markAncestors(name)
		if err := erofsEnsureDir(w, parent); err != nil {
			return 0, "", nil, fmt.Errorf("while creating parent directories for %q: %w", name, err)
		}

		mode := hdr.FileInfo().Mode()
		switch hdr.Typeflag {
		case tar.TypeReg:
			// A later tar entry at a path replaces whatever came before it,
			// subtree included; go-erofs's RemoveAll is a no-op on a missing path.
			if err := w.RemoveAll(name); err != nil {
				return 0, "", nil, fmt.Errorf("while replacing existing path at %q before regular file: %w", name, err)
			}
			f, err := w.Create(name)
			if err != nil {
				return 0, "", nil, fmt.Errorf("while creating file %q: %w", name, err)
			}
			_, err = f.ReadFrom(tarReader)
			closeErr := f.Close()
			if err != nil {
				return 0, "", nil, fmt.Errorf("while writing contents of %q from tar stream: %w", name, err)
			}
			if closeErr != nil {
				return 0, "", nil, fmt.Errorf("while closing file %q: %w", name, closeErr)
			}

		case tar.TypeDir:
			fi, err := w.Lstat(name)
			switch {
			case err == nil && fi.IsDir():
				// Repeated or implicitly created directory: the later
				// entry's attributes win below.
			case err == nil || errors.Is(err, fs.ErrNotExist):
				if err := w.RemoveAll(name); err != nil {
					return 0, "", nil, fmt.Errorf("while replacing existing path at %q before directory: %w", name, err)
				}
				if err := w.Mkdir(name, mode); err != nil {
					return 0, "", nil, fmt.Errorf("while creating directory=%q, mode=%v: %w", name, mode, err)
				}
			default:
				return 0, "", nil, fmt.Errorf("while checking existing path at %q before directory: %w", name, err)
			}
			declared[name] = true
			delete(implicit, name)

		case tar.TypeSymlink:
			if err := w.RemoveAll(name); err != nil {
				return 0, "", nil, fmt.Errorf("while replacing existing path at %q before symlink: %w", name, err)
			}
			if err := w.Symlink(hdr.Linkname, name); err != nil {
				return 0, "", nil, fmt.Errorf("while creating symlink src=%q target=%q: %w", name, hdr.Linkname, err)
			}

		case tar.TypeLink:
			linkname, linkSkip, err := validateTarName(hdr.Linkname)
			if err != nil {
				return 0, "", nil, fmt.Errorf("invalid hardlink target for %q: %w", name, err)
			}
			if linkSkip {
				return 0, "", nil, fmt.Errorf("invalid hardlink target for %q: empty", name)
			}
			linkParent, err := erofsResolve(w, filepath.Dir(linkname))
			if err != nil {
				return 0, "", nil, fmt.Errorf("while resolving hardlink target for %q: %w", name, err)
			}
			linkname = filepath.Join(linkParent, filepath.Base(linkname))
			if err := w.RemoveAll(name); err != nil {
				return 0, "", nil, fmt.Errorf("while replacing existing path at %q before hardlink: %w", name, err)
			}
			if err := w.Link(linkname, name); err != nil {
				return 0, "", nil, fmt.Errorf("while creating hardlink src=%q target=%q: %w", name, linkname, err)
			}
			// The link shares its target's inode and therefore its
			// attributes; the tar header of a hardlink carries none of its
			// own.
			continue

		case tar.TypeChar, tar.TypeBlock:
			// Skipped like unpackLayer does: an image must never grant
			// device access, and the runtime provides /dev. The entry still
			// replaces what came before it at the path.
			if err := w.RemoveAll(name); err != nil {
				return 0, "", nil, fmt.Errorf("while replacing existing path at %q before device node: %w", name, err)
			}
			continue

		default:
			return 0, "", nil, fmt.Errorf("unhandled tar entry typeflag %q", string([]byte{hdr.Typeflag}))
		}

		if err := erofsSetAttrs(w, name, hdr); err != nil {
			return 0, "", nil, err
		}
	}

	for p := range implicit {
		if declared[p] {
			continue
		}
		if fi, err := w.Lstat(p); err == nil && fi.IsDir() {
			wh.ImplicitDirs = append(wh.ImplicitDirs, p)
		}
	}
	sort.Strings(wh.ImplicitDirs)

	if err := w.Close(); err != nil {
		return 0, "", nil, fmt.Errorf("while writing EROFS image: %w", err)
	}
	fi, err := out.Stat()
	if err != nil {
		return 0, "", nil, fmt.Errorf("while sizing EROFS image: %w", err)
	}
	size = (fi.Size() + erofsImageAlign - 1) / erofsImageAlign * erofsImageAlign
	if size != fi.Size() {
		if err := out.Truncate(size); err != nil {
			return 0, "", nil, fmt.Errorf("while padding EROFS image: %w", err)
		}
	}
	if _, err := out.Seek(0, io.SeekStart); err != nil {
		return 0, "", nil, fmt.Errorf("while rewinding EROFS image: %w", err)
	}
	h := sha256.New()
	if _, err := io.Copy(h, out); err != nil {
		return 0, "", nil, fmt.Errorf("while hashing EROFS image: %w", err)
	}
	return size, "sha256:" + hex.EncodeToString(h.Sum(nil)), wh, nil
}

// erofsSetAttrs applies a tar header's ownership, mode, mtime, and xattrs to
// the entry at name. Symlink modes are fixed by the format, so Chmod is
// skipped for them.
func erofsSetAttrs(w *erofs.Writer, name string, hdr *tar.Header) error {
	if hdr.Typeflag != tar.TypeSymlink {
		if err := w.Chmod(name, hdr.FileInfo().Mode()); err != nil {
			return fmt.Errorf("while setting mode on %q: %w", name, err)
		}
	}
	if err := w.Chown(name, hdr.Uid, hdr.Gid); err != nil {
		return fmt.Errorf("while setting owner on %q: %w", name, err)
	}
	if err := w.Chtimes(name, hdr.ModTime, hdr.ModTime); err != nil {
		return fmt.Errorf("while setting mtime on %q: %w", name, err)
	}
	for k, v := range hdr.PAXRecords {
		attr, ok := strings.CutPrefix(k, paxXattrPrefix)
		if !ok || isOverlayXattr(attr) {
			continue
		}
		if err := w.Setxattr(name, attr, v); err != nil {
			return fmt.Errorf("while setting xattr %q on %q: %w", attr, name, err)
		}
	}
	return nil
}

// isOverlayXattr reports whether attr is in a namespace overlayfs interprets
// on its layers.
func isOverlayXattr(attr string) bool {
	return strings.HasPrefix(attr, "trusted.overlay.") || strings.HasPrefix(attr, "user.overlay.")
}

// erofsRelPath converts a clean image-root-relative path into the "." form
// the rest of the package uses for the root.
func erofsRelPath(p string) string {
	rel := strings.TrimPrefix(path.Clean(p), "/")
	if rel == "" {
		return "."
	}
	return filepath.FromSlash(rel)
}

// erofsResolve resolves directory path p through in-image symlinks, clamping
// targets that traverse above the image root.
func erofsResolve(w *erofs.Writer, p string) (string, error) {
	const maxSymlinkHops = 40
	comps := strings.Split(strings.Trim(filepath.ToSlash(p), "/"), "/")
	resolved := "/"
	hops := 0
	for i := 0; i < len(comps); i++ {
		c := comps[i]
		switch c {
		case "", ".":
			continue
		case "..":
			resolved = path.Dir(resolved)
			continue
		}
		next := path.Join(resolved, c)
		fi, err := w.Lstat(next)
		if errors.Is(err, fs.ErrNotExist) {
			// Nothing below an absent component can be a symlink.
			return erofsRelPath(path.Join(append([]string{next}, comps[i+1:]...)...)), nil
		} else if err != nil {
			return "", err
		}
		if fi.Mode()&fs.ModeSymlink == 0 {
			resolved = next
			continue
		}
		hops++
		if hops > maxSymlinkHops {
			return "", &fs.PathError{Op: "resolve", Path: p, Err: errors.New("too many levels of symbolic links")}
		}
		target, err := w.ReadLink(next)
		if err != nil {
			return "", err
		}
		if path.IsAbs(target) {
			resolved = "/"
		}
		comps = append(strings.Split(strings.Trim(target, "/"), "/"), comps[i+1:]...)
		i = -1
	}
	return erofsRelPath(resolved), nil
}

// erofsEnsureDir ensures dir exists as a directory, creating missing parents
// with mode 0755.
func erofsEnsureDir(w *erofs.Writer, dir string) error {
	fi, err := w.Lstat(dir)
	switch {
	case err == nil && fi.IsDir():
		return nil
	case err == nil:
		return &fs.PathError{Op: "mkdir", Path: dir, Err: erofs.ErrNotDirectory}
	case errors.Is(err, fs.ErrNotExist):
		return w.Mkdir(dir, 0o755)
	default:
		return err
	}
}
