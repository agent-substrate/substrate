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
