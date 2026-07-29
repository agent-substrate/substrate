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
// ownership, modification times, symlinks, hardlinks, and FIFOs.
//
// It exists for snapshotting durable-dir volumes (see cmd/ateom-microvm): the
// contents are written by the sandboxed workload under arbitrary uids, shipped
// to object storage, and restored — possibly onto another node — where the
// workload must see them unchanged.
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
)

// Create writes a tar archive of srcDir's contents to tarPath. Entry names are
// relative to srcDir, so extracting into another directory reproduces the tree.
// srcDir itself is not an entry.
//
// Regular files, directories, symlinks, and FIFOs are archived with their mode,
// ownership, and modification time. A file with multiple links inside srcDir is
// archived once and referenced as a hardlink thereafter. Sockets are skipped
// (see writeTree). A device node is an error rather than silent data loss.
func Create(ctx context.Context, tarPath, srcDir string) error {
	f, err := os.Create(tarPath)
	if err != nil {
		return fmt.Errorf("creating tar %q: %w", tarPath, err)
	}
	defer f.Close()

	tw := tar.NewWriter(f)
	if err := writeTree(ctx, tw, srcDir); err != nil {
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
// entry per path. The deterministic order keeps archives of identical trees
// byte-comparable, which makes snapshot diffs meaningful.
func writeTree(ctx context.Context, tw *tar.Writer, srcDir string) error {
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

		case d.IsDir(), info.Mode()&os.ModeSymlink != 0, info.Mode()&os.ModeNamedPipe != 0:
			// FileInfoHeader already gave a FIFO Typeflag TypeFifo and size 0.
			return tw.WriteHeader(hdr)

		default:
			// Device nodes reach here. They need privilege to create, so one in a
			// workload's data directory means something unexpected happened —
			// worth failing on rather than silently dropping or re-creating it
			// under a later restore.
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
	return nil
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
