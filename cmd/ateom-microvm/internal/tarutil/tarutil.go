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

// Package tarutil archives and restores a directory tree as a tar file,
// preserving the metadata a workload's data directory depends on: modes,
// ownership, modification times, symlinks, hardlinks, FIFOs, device nodes,
// and user.* / trusted.overlay.* extended attributes (as PAX SCHILY.xattr
// records).
//
// It exists for snapshotting durable-dir volumes and rootfs overlay uppers
// (see cmd/ateom-microvm): the contents are written by the sandboxed workload
// under arbitrary uids, shipped to object storage, and restored — possibly
// onto another node — where the workload must see them unchanged. The upper
// is why device nodes and overlay xattrs matter: the host kernel's overlayfs
// records a deleted file as a 0:0 character device (whiteout) and a replaced
// directory as a trusted.overlay.opaque xattr, and losing either silently
// resurrects deleted content after a resume.
//
// Extraction is confined to the destination with os.Root, so a crafted archive
// cannot write outside it via "..", an absolute path, or a symlink.
package tarutil

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/sys/unix"
)

// Create writes a tar archive of srcDir's contents to tarPath. Entry names are
// relative to srcDir, so extracting into another directory reproduces the tree.
// srcDir itself is not an entry.
//
// Regular files, directories, symlinks, FIFOs, and device nodes are archived
// with their mode, ownership, modification time, and user.* / trusted.overlay.*
// xattrs. A file with multiple links inside srcDir is archived once and
// referenced as a hardlink thereafter. Sockets are skipped (see writeTree).
func Create(ctx context.Context, tarPath, srcDir string) error {
	return CreateFiltered(ctx, tarPath, srcDir, nil)
}

// SkipFunc reports whether an archive entry should be omitted, given its
// slash-separated path relative to the archive root. Returning true for a
// directory omits its entire subtree.
type SkipFunc func(rel string) bool

// CreateFiltered is Create with entries omitted where skip returns true. A nil
// skip archives everything.
func CreateFiltered(ctx context.Context, tarPath, srcDir string, skip SkipFunc) error {
	f, err := os.Create(tarPath)
	if err != nil {
		return fmt.Errorf("creating tar %q: %w", tarPath, err)
	}
	defer f.Close()

	tw := tar.NewWriter(f)
	if err := writeTree(ctx, tw, srcDir, skip); err != nil {
		return err
	}
	if err := tw.Close(); err != nil {
		return fmt.Errorf("closing tar %q: %w", tarPath, err)
	}
	// Durable-dir tars are handed to atelet for upload as soon as we return, so
	// flush to disk rather than trusting the page cache to outlive us.
	if err := f.Sync(); err != nil {
		return fmt.Errorf("syncing tar %q: %w", tarPath, err)
	}
	return nil
}

// writeTree walks srcDir in lexical order (filepath.WalkDir) and writes one
// entry per path, omitting entries (and, for directories, subtrees) that skip
// selects. The deterministic order keeps archives of identical trees
// byte-comparable, which makes snapshot diffs meaningful.
func writeTree(ctx context.Context, tw *tar.Writer, srcDir string, skip SkipFunc) error {
	// Maps an already-archived multi-link inode to the name it was archived
	// under, so later links become tar hardlink entries instead of copies.
	linked := map[inodeKey]string{}

	return filepath.WalkDir(srcDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if skip != nil && skip(filepath.ToSlash(rel)) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		// Skipped before the header is built, because archive/tar cannot
		// represent a socket at all. A socket carries no data and is meaningless
		// without the process listening on it, which is gone by the time this
		// archive is read; workloads recreate theirs on start. GNU tar and
		// containerd's archiver both skip sockets for that reason. Failing
		// instead would be worse than useless here: agents leave sockets lying
		// around in their data directories (ssh-agent, gpg-agent, language
		// servers), and one of them would make every checkpoint fail, stranding
		// the actor on its worker with no way to suspend it.
		if info.Mode()&os.ModeSocket != 0 {
			slog.WarnContext(ctx, "Skipping socket while archiving directory",
				slog.String("path", path), slog.String("root", srcDir))
			return nil
		}

		link := ""
		if info.Mode()&os.ModeSymlink != 0 {
			if link, err = os.Readlink(path); err != nil {
				return fmt.Errorf("reading symlink %q: %w", path, err)
			}
		}
		hdr, err := tar.FileInfoHeader(info, link)
		if err != nil {
			return fmt.Errorf("building tar header for %q: %w", path, err)
		}
		hdr.Name = filepath.ToSlash(rel)
		if d.IsDir() {
			hdr.Name += "/"
		}
		setOwner(hdr, info)

		// Overlay-relevant xattrs ride as PAX records — the host kernel's
		// overlayfs stores opaque-directory markers as trusted.overlay.*
		// (and user.* is preserved for workload data) — and the round trip
		// must keep them or replaced directories un-replace on restore.
		// Symlinks are exempt: Linux refuses user.* xattrs on them, and
		// overlay metadata never targets them.
		if info.Mode()&os.ModeSymlink == 0 {
			xattrs, err := readOverlayXattrs(path)
			if err != nil {
				return fmt.Errorf("reading xattrs of %q: %w", path, err)
			}
			for attr, val := range xattrs {
				if hdr.PAXRecords == nil {
					hdr.PAXRecords = map[string]string{}
				}
				hdr.PAXRecords["SCHILY.xattr."+attr] = val
			}
		}

		switch {
		case info.Mode().IsRegular():
			// A second link to an already-archived inode: record the link and
			// skip the contents.
			if key, ok := inodeOf(info); ok && info.Sys() != nil && nlinkOf(info) > 1 {
				if target, seen := linked[key]; seen {
					hdr.Typeflag = tar.TypeLink
					hdr.Linkname = target
					hdr.Size = 0
					return tw.WriteHeader(hdr)
				}
				linked[key] = hdr.Name
			}
			if err := tw.WriteHeader(hdr); err != nil {
				return fmt.Errorf("writing tar header for %q: %w", path, err)
			}
			return copyFileInto(tw, path)

		case d.IsDir(), info.Mode()&os.ModeSymlink != 0, info.Mode()&os.ModeNamedPipe != 0,
			info.Mode()&os.ModeDevice != 0:
			// FileInfoHeader already populated the Typeflag (and, for devices,
			// the major/minor) with size 0. Devices are archived rather than
			// rejected because an overlay upper legitimately contains them:
			// every deleted lower-layer file is a 0:0 char-device whiteout.
			return tw.WriteHeader(hdr)

		default:
			return fmt.Errorf("unsupported file type %v at %q", info.Mode().Type(), path)
		}
	})
}

// copyFileInto streams path's contents into the archive.
func copyFileInto(tw *tar.Writer, path string) error {
	in, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening %q: %w", path, err)
	}
	defer in.Close()
	if _, err := io.Copy(tw, in); err != nil {
		return fmt.Errorf("archiving contents of %q: %w", path, err)
	}
	return nil
}

// Extract unpacks tarPath into dstDir, which must already exist. Modes,
// ownership, modification times, symlinks, and hardlinks are restored.
//
// All writes go through an os.Root rooted at dstDir, so entries naming a path
// outside it — via "..", an absolute path, or a symlink planted earlier in the
// same archive — are refused rather than followed. An entry that collides with
// an existing path replaces it ("later entry wins", standard tar semantics),
// except that an existing directory is kept when the entry is also a directory.
func Extract(tarPath, dstDir string) error {
	f, err := os.Open(tarPath)
	if err != nil {
		return fmt.Errorf("opening tar %q: %w", tarPath, err)
	}
	defer f.Close()

	root, err := os.OpenRoot(dstDir)
	if err != nil {
		return fmt.Errorf("opening destination %q: %w", dstDir, err)
	}
	defer root.Close()

	// Directories are created owner-writable so their children can be written
	// even when the archive marks them read-only; the recorded modes and times
	// are applied after every child exists (see restoreDirMeta).
	dirs := map[string]*tar.Header{}

	tr := tar.NewReader(f)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("reading tar %q: %w", tarPath, err)
		}
		name, skip, err := cleanTarName(hdr.Name)
		if err != nil {
			return fmt.Errorf("invalid entry in tar %q: %w", tarPath, err)
		}
		if skip {
			continue
		}
		if err := extractEntry(root, tr, hdr, name, dirs); err != nil {
			return err
		}
	}
	return restoreDirMeta(root, dirs)
}

// extractEntry materializes one archive entry under root.
func extractEntry(root *os.Root, tr *tar.Reader, hdr *tar.Header, name string, dirs map[string]*tar.Header) error {
	mode := hdr.FileInfo().Mode().Perm()

	switch hdr.Typeflag {
	case tar.TypeDir:
		if err := root.Mkdir(name, mode|0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("creating directory %q: %w", name, err)
		}
		dirs[name] = hdr
		return nil

	case tar.TypeReg:
		if err := replaceExisting(root, name); err != nil {
			return err
		}
		out, err := root.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
		if err != nil {
			return fmt.Errorf("creating file %q: %w", name, err)
		}
		_, copyErr := io.Copy(out, tr)
		closeErr := out.Close()
		if copyErr != nil {
			return fmt.Errorf("writing contents of %q: %w", name, copyErr)
		}
		if closeErr != nil {
			return fmt.Errorf("closing %q: %w", name, closeErr)
		}
		return restoreMeta(root, name, hdr)

	case tar.TypeSymlink:
		if err := replaceExisting(root, name); err != nil {
			return err
		}
		if err := root.Symlink(hdr.Linkname, name); err != nil {
			return fmt.Errorf("creating symlink %q -> %q: %w", name, hdr.Linkname, err)
		}
		// Only ownership applies to a symlink itself; its mode is meaningless
		// and its times would follow the target.
		return lchownEntry(root, name, hdr)

	case tar.TypeFifo:
		if err := replaceExisting(root, name); err != nil {
			return err
		}
		if err := createFifo(root, name, mode); err != nil {
			return err
		}
		return restoreMeta(root, name, hdr)

	case tar.TypeLink:
		target, skip, err := cleanTarName(hdr.Linkname)
		if err != nil || skip {
			return fmt.Errorf("invalid hardlink target %q for %q", hdr.Linkname, name)
		}
		if err := replaceExisting(root, name); err != nil {
			return err
		}
		if err := root.Link(target, name); err != nil {
			return fmt.Errorf("creating hardlink %q -> %q: %w", name, target, err)
		}
		return nil

	case tar.TypeChar, tar.TypeBlock:
		if err := replaceExisting(root, name); err != nil {
			return err
		}
		if err := createDevice(root, name, hdr, mode); err != nil {
			return err
		}
		return restoreMeta(root, name, hdr)

	default:
		return fmt.Errorf("unsupported tar entry type %q at %q", string([]byte{hdr.Typeflag}), name)
	}
}

// replaceExisting removes whatever currently occupies name so the new entry
// does not write through a symlink, truncate a shared inode, or collide with a
// directory.
func replaceExisting(root *os.Root, name string) error {
	if _, err := root.Lstat(name); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("checking existing path %q: %w", name, err)
	}
	if err := root.RemoveAll(name); err != nil {
		return fmt.Errorf("replacing existing path %q: %w", name, err)
	}
	return nil
}

// restoreDirMeta applies the archived modes, ownership, and times to extracted
// directories, deepest first: a directory's path is always longer than its
// parent's, so length-descending order restores children before the parent's
// mode can make them unreachable.
func restoreDirMeta(root *os.Root, dirs map[string]*tar.Header) error {
	names := make([]string, 0, len(dirs))
	for name := range dirs {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool { return len(names[i]) > len(names[j]) })
	for _, name := range names {
		if err := restoreMeta(root, name, dirs[name]); err != nil {
			return err
		}
	}
	return nil
}

// restoredMode is the mode an extracted entry ends up with: the permission bits
// plus setuid, setgid, and sticky, which FileMode.Perm() alone would drop.
//
// The archive records them, so dropping them would make the round trip silently
// lossy. The case that bites is a setgid data directory (0o2775): the workload
// relies on group inheritance for files its different uids create, and losing
// the bit only surfaces later, when the next file lands with the wrong group.
// These bits govern behavior inside the sandbox — the host never executes a
// workload's files — and a running workload can set them on its own data
// anyway, so carrying them across a suspend/resume grants it nothing new.
func restoredMode(hdr *tar.Header) os.FileMode {
	return hdr.FileInfo().Mode() & (fs.ModePerm | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky)
}

// restoreMeta applies ownership, mode, and modification time to an extracted
// path. Ownership is applied first because chowning a file clears its setuid
// and setgid bits, which the chmod below then puts back.
func restoreMeta(root *os.Root, name string, hdr *tar.Header) error {
	if err := lchownEntry(root, name, hdr); err != nil {
		return err
	}
	if err := root.Chmod(name, restoredMode(hdr)); err != nil {
		return fmt.Errorf("restoring mode on %q: %w", name, err)
	}
	if !hdr.ModTime.IsZero() {
		if err := root.Chtimes(name, hdr.ModTime, hdr.ModTime); err != nil {
			return fmt.Errorf("restoring times on %q: %w", name, err)
		}
	}
	return restoreOverlayXattrs(root, name, hdr)
}

// restoreOverlayXattrs re-applies the user.* and trusted.overlay.* xattrs
// recorded in the entry's PAX records (writeTree's SCHILY.xattr.* — the
// overlayfs deletion metadata plus workload-owned user attrs). Writing
// trusted.* requires CAP_SYS_ADMIN; extraction runs as root in ateom, and the
// tests gate on it.
//
// The target is addressed THROUGH its parent directory opened via root (the
// same containment pattern createFifo and createDevice use), never by joining
// a host path: a symlinked intermediate component in a crafted archive must
// not redirect the write outside the extraction dir. The final component is
// addressed path-wise under /proc/self/fd/<parent> rather than by opening the
// entry itself — opening would block on a FIFO and fail on a whiteout device —
// and Lsetxattr does not follow a final-component symlink (nor do symlink
// entries take this path: extractEntry never calls restoreMeta for them).
func restoreOverlayXattrs(root *os.Root, name string, hdr *tar.Header) error {
	var attrs map[string]string
	for k, v := range hdr.PAXRecords {
		if strings.HasPrefix(k, "SCHILY.xattr.user.") ||
			strings.HasPrefix(k, "SCHILY.xattr.trusted.overlay.") {
			if attrs == nil {
				attrs = map[string]string{}
			}
			attrs[strings.TrimPrefix(k, "SCHILY.xattr.")] = v
		}
	}
	if len(attrs) == 0 {
		return nil
	}
	dir, base := filepath.Split(name)
	if dir == "" {
		dir = "."
	}
	parent, err := root.Open(filepath.Clean(dir))
	if err != nil {
		return fmt.Errorf("opening parent directory of %q to restore xattrs: %w", name, err)
	}
	defer parent.Close()
	for attr, val := range attrs {
		target := fmt.Sprintf("/proc/self/fd/%d/%s", parent.Fd(), base)
		if err := unix.Lsetxattr(target, attr, []byte(val), 0); err != nil {
			return fmt.Errorf("restoring xattr %q on %q: %w", attr, name, err)
		}
	}
	return nil
}

// readOverlayXattrs returns path's user.* and trusted.overlay.* extended
// attributes. Filesystems without xattr support report none rather than
// failing: tarutil archives arbitrary workload trees, and only those two
// namespaces carry state the round trip must preserve (trusted.overlay.* is
// the host kernel's overlay deletion metadata; reading it requires root,
// which archiving in ateom always has).
func readOverlayXattrs(path string) (map[string]string, error) {
	sz, err := unix.Llistxattr(path, nil)
	if err != nil {
		if errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) {
			return nil, nil
		}
		return nil, err
	}
	if sz <= 0 {
		return nil, nil
	}
	buf := make([]byte, sz)
	if sz, err = unix.Llistxattr(path, buf); err != nil {
		return nil, err
	}

	var attrs map[string]string
	for _, attr := range strings.Split(string(buf[:sz]), "\x00") {
		if !strings.HasPrefix(attr, "user.") && !strings.HasPrefix(attr, "trusted.overlay.") {
			continue
		}
		vsz, err := unix.Lgetxattr(path, attr, nil)
		if err != nil {
			return nil, err
		}
		val := make([]byte, vsz)
		if vsz > 0 {
			if vsz, err = unix.Lgetxattr(path, attr, val); err != nil {
				return nil, err
			}
			val = val[:vsz]
		}
		if attrs == nil {
			attrs = map[string]string{}
		}
		attrs[attr] = string(val)
	}
	return attrs, nil
}

// cleanTarName validates an archive entry name and returns it relative and
// slash-free of surprises. skip is true for entries that name nothing ("" or
// "."), which some tar writers emit for the archive root.
func cleanTarName(name string) (cleaned string, skip bool, err error) {
	if name == "" {
		return "", true, nil
	}
	cleaned = filepath.Clean(filepath.FromSlash(name))
	cleaned = strings.TrimPrefix(cleaned, string(filepath.Separator))
	if cleaned == "" || cleaned == "." {
		return "", true, nil
	}
	if !filepath.IsLocal(cleaned) {
		return "", false, fmt.Errorf("not a local path: %q", name)
	}
	return cleaned, false, nil
}
