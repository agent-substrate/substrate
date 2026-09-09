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

package tarutil

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// blobRecord is the PAX key naming the sibling file that holds a regular-file
// entry's contents. Only entries written by CreateSplit carry it, so a reader
// tells the two arrangements apart per entry rather than per archive.
const blobRecord = "ate.blob"

// CreateSplit archives srcDir like Create, except that each non-empty regular
// file's contents go to their own blob file in blobDir instead of into the
// archive. tarPath is left holding the tree's metadata alone — modes,
// ownership, times, symlinks, hardlinks, FIFOs, devices, and xattrs, exactly
// as Create records them — which makes it small no matter how large the tree
// is. Blobs are named blobPrefix followed by a four-digit sequence number in
// walk order; the returned slice lists them.
//
// The blobs are HARDLINKS to the source files, not copies. That is the whole
// point: the split exists to take the file contents off a checkpoint's paused
// critical path, and copying them would put the bytes straight back. The
// caller must therefore treat srcDir as frozen from the moment CreateSplit
// returns — a later write through an original path is a write to the blob.
// ateom relies on that: it captures a paused guest whose sandbox is torn down,
// and whose directories are reset, before anything can run again. A blobDir on
// a different filesystem than srcDir falls back to copying.
//
// ExtractSplit puts the two halves back together.
func CreateSplit(ctx context.Context, tarPath, srcDir, blobDir, blobPrefix string) ([]string, error) {
	sink := &blobSink{dir: blobDir, prefix: blobPrefix}
	if err := createTree(ctx, tarPath, srcDir, nil, sink); err != nil {
		return nil, err
	}
	return sink.names, nil
}

// ExtractSplit unpacks a CreateSplit pair into dstDir, which must already
// exist: tarPath supplies the tree and its metadata, blobDir the file
// contents. It accepts an archive Create wrote too, so a caller that dispatches
// on the archive's name does not also have to know how each entry was stored.
//
// Extraction ADOPTS each blob: the extracted file is a hardlink to it, not a
// copy, so blobDir is consumed rather than merely read and a later write to an
// extracted file is a write to the blob. This is the mirror of CreateSplit's
// hardlink and exists for the same reason — a copy here would spend a second
// full write of the tree on the restore path, which is what the split
// arrangement is trying to avoid.
//
// The caller must therefore own blobDir outright. A blob that is already linked
// from elsewhere is copied rather than adopted, so a caller that assembled the
// directory by linking gets correct contents instead of a shared inode; so does
// a blobDir on a different filesystem than dstDir.
func ExtractSplit(tarPath, blobDir, dstDir string) error {
	blobs, err := os.OpenRoot(blobDir)
	if err != nil {
		return fmt.Errorf("opening blob directory %q: %w", blobDir, err)
	}
	defer blobs.Close()
	return extractTree(tarPath, dstDir, blobs)
}

// blobSink hands out sequential blob names and links each regular file to one.
type blobSink struct {
	dir    string
	prefix string
	names  []string
}

// put stores path's contents as the next blob and returns its name.
func (s *blobSink) put(path string) (string, error) {
	name := fmt.Sprintf("%s%04d", s.prefix, len(s.names))
	dst := filepath.Join(s.dir, name)

	err := os.Link(path, dst)
	// EXDEV: a blob directory on another filesystem. EMLINK: the inode is
	// already at the filesystem's link ceiling. EPERM: some filesystems refuse
	// hardlinks outright. None of them are worth failing a checkpoint over
	// when copying the bytes still produces a correct blob.
	if errors.Is(err, syscall.EXDEV) || errors.Is(err, syscall.EMLINK) || errors.Is(err, syscall.EPERM) {
		err = copyBlob(path, dst)
	}
	if err != nil {
		return "", fmt.Errorf("storing contents of %q as %q: %w", path, name, err)
	}

	s.names = append(s.names, name)
	return name, nil
}

// copyBlob writes path's contents to dst, flushing them before returning: the
// blob is handed to atelet for upload as soon as the checkpoint completes.
func copyBlob(path, dst string) error {
	in, err := os.Open(path)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	if err := out.Sync(); err != nil {
		return err
	}
	return out.Close()
}

// checkBlobName rejects a blob name that is not a single path component: it is
// read back from an archive that traveled through object storage, so it is not
// trusted to stay inside the blob directory on its own.
func checkBlobName(name, entry string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsRune(name, filepath.Separator) {
		return fmt.Errorf("entry %q names an invalid blob %q", entry, name)
	}
	return nil
}

// openBlob returns a reader over the blob an entry's PAX record names.
func openBlob(blobs *os.Root, name, entry string) (io.ReadCloser, error) {
	if err := checkBlobName(name, entry); err != nil {
		return nil, err
	}
	f, err := blobs.Open(name)
	if err != nil {
		return nil, fmt.Errorf("opening blob %q for entry %q: %w", name, entry, err)
	}
	return f, nil
}

// linkat is a seam for exercising the copy fallback, which otherwise needs two
// filesystems to reach.
var linkat = unix.Linkat

// linkBlob hardlinks the blob named blobName to entry name under root,
// reporting whether it succeeded. A filesystem that cannot take the link is
// not a failure — the caller copies instead — so only a genuine error is
// returned.
//
// A blob that is already linked from somewhere else is copied instead. Adopting
// it would hand the extracted tree an inode a second owner still holds, and
// whatever that owner is keeping it for would then take the writes the tree
// receives. Refusing enforces the contract ExtractSplit states, rather than
// leaving it to whoever assembles the blob directory next.
//
// Both ends are addressed through their directory's file descriptor rather
// than by joining host paths, the same containment the rest of this package
// uses: a symlinked intermediate component in a crafted archive must not
// redirect the new link outside the extraction dir.
func linkBlob(blobs *os.Root, blobName string, root *os.Root, name string) (bool, error) {
	if err := checkBlobName(blobName, name); err != nil {
		return false, err
	}
	info, err := blobs.Stat(blobName)
	if err != nil {
		return false, fmt.Errorf("stating blob %q for entry %q: %w", blobName, name, err)
	}
	if st, ok := info.Sys().(*syscall.Stat_t); !ok || st.Nlink != 1 {
		return false, nil
	}
	src, err := blobs.Open(".")
	if err != nil {
		return false, fmt.Errorf("opening blob directory to link blob %q: %w", blobName, err)
	}
	defer src.Close()

	dir, base := filepath.Split(name)
	if dir == "" {
		dir = "."
	}
	parent, err := root.Open(filepath.Clean(dir))
	if err != nil {
		return false, fmt.Errorf("opening parent directory of %q to link blob %q: %w", name, blobName, err)
	}
	defer parent.Close()

	err = linkat(int(src.Fd()), blobName, int(parent.Fd()), base, 0)
	switch {
	case err == nil:
		return true, nil
	// EXDEV: the blob directory is on another filesystem. EMLINK: the inode is
	// already at the filesystem's link ceiling. EPERM/EOPNOTSUPP: some
	// filesystems refuse hardlinks outright.
	case errors.Is(err, unix.EXDEV), errors.Is(err, unix.EMLINK),
		errors.Is(err, unix.EPERM), errors.Is(err, unix.EOPNOTSUPP):
		return false, nil
	default:
		return false, fmt.Errorf("linking blob %q to entry %q: %w", blobName, name, err)
	}
}
