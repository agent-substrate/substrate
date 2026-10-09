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

package assetfetch

import (
	"os"

	"golang.org/x/sys/unix"
)

// exchange atomically swaps the entries a and b of dir, below root. It fails
// with an error matching os.ErrNotExist if either is missing, so it never
// creates b.
func exchange(root *os.Root, dir, a, b string) error {
	d, err := root.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	fd := int(d.Fd())
	if err := unix.Renameat2(fd, a, fd, b, unix.RENAME_EXCHANGE); err != nil {
		return &os.LinkError{Op: "renameat2", Old: a, New: b, Err: err}
	}
	return nil
}
