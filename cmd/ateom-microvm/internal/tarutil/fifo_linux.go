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
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// createFifo creates a FIFO at name, a path relative to root, with mode's
// permission bits.
//
// os.Root has no Mkfifo, so this opens name's parent directory THROUGH root —
// which refuses to traverse a symlink out of the tree, the same guarantee the
// rest of extraction relies on — and creates the FIFO relative to that
// directory's descriptor. The final element is a single path component, so it
// cannot walk anywhere from there.
func createFifo(root *os.Root, name string, mode os.FileMode) error {
	dir, base := filepath.Split(name)
	if dir == "" {
		dir = "."
	}
	parent, err := root.Open(filepath.Clean(dir))
	if err != nil {
		return fmt.Errorf("opening parent directory of %q: %w", name, err)
	}
	defer parent.Close()

	if err := unix.Mkfifoat(int(parent.Fd()), base, uint32(mode.Perm())); err != nil {
		return fmt.Errorf("creating fifo %q: %w", name, err)
	}
	return nil
}

// createDevice creates a character or block device node at name, a path
// relative to root, using the same parent-directory containment as createFifo.
// Device nodes matter here because overlayfs records a deleted lower-layer
// file as a 0:0 character device ("whiteout") in the upper — dropping one at
// extraction would resurrect the deleted file on restore. mknod requires
// privilege; extraction runs as root in ateom, and the tests gate on it.
func createDevice(root *os.Root, name string, hdr *tar.Header, mode os.FileMode) error {
	dir, base := filepath.Split(name)
	if dir == "" {
		dir = "."
	}
	parent, err := root.Open(filepath.Clean(dir))
	if err != nil {
		return fmt.Errorf("opening parent directory of %q: %w", name, err)
	}
	defer parent.Close()

	var typ uint32 = unix.S_IFCHR
	if hdr.Typeflag == tar.TypeBlock {
		typ = unix.S_IFBLK
	}
	dev := unix.Mkdev(uint32(hdr.Devmajor), uint32(hdr.Devminor))
	if err := unix.Mknodat(int(parent.Fd()), base, typ|uint32(mode.Perm()), int(dev)); err != nil {
		return fmt.Errorf("creating device node %q: %w", name, err)
	}
	return nil
}
