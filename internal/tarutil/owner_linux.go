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
	"archive/tar"
	"errors"
	"fmt"
	"os"
	"syscall"
)

// inodeKey identifies an inode within one filesystem, so hardlinks to the same
// file can be recognized while distinct files that share an inode number across
// devices are not conflated.
type inodeKey struct {
	dev uint64
	ino uint64
}

// inodeOf returns info's inode identity, or ok=false on a platform or file
// where it is unavailable (the caller then archives the contents rather than
// emitting a hardlink).
func inodeOf(info os.FileInfo) (inodeKey, bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return inodeKey{}, false
	}
	return inodeKey{dev: uint64(st.Dev), ino: uint64(st.Ino)}, true
}

// nlinkOf returns the file's link count, or 1 when unavailable.
func nlinkOf(info os.FileInfo) uint64 {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 1
	}
	return uint64(st.Nlink)
}

// setOwner records the on-disk uid/gid in the header. tar.FileInfoHeader leaves
// them zero, which would silently re-own every restored file to root.
func setOwner(hdr *tar.Header, info os.FileInfo) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return
	}
	hdr.Uid = int(st.Uid)
	hdr.Gid = int(st.Gid)
	// Names are deliberately left empty: the guest's passwd/group namespace is
	// not the host's, so only the numeric ids are meaningful.
	hdr.Uname = ""
	hdr.Gname = ""
}

// lchownEntry restores an entry's ownership without following symlinks.
//
// Changing a file's owner requires privilege. The production caller (ateom) is
// root in its worker pod, so ownership is restored faithfully and any EPERM
// there signals a real problem worth failing on. An unprivileged process cannot
// chown at all, so rather than making the package unusable outside a root
// context (tests, local tooling), EPERM is tolerated there and the extracted
// files simply belong to the extracting user.
func lchownEntry(root *os.Root, name string, hdr *tar.Header) error {
	err := root.Lchown(name, hdr.Uid, hdr.Gid)
	if err == nil {
		return nil
	}
	if errors.Is(err, os.ErrPermission) && os.Geteuid() != 0 {
		return nil
	}
	return fmt.Errorf("restoring ownership of %q to %d:%d: %w", name, hdr.Uid, hdr.Gid, err)
}
