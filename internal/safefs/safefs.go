// Copyright 2026 The Agent Substrate Authors
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

// Package safefs reads files in directories that a less trusted process can
// write.
package safefs

import (
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// ReadRegularFile reads name under root if it is a regular file of at most
// maxBytes. A symlink, FIFO, socket, device or directory there is an error,
// as is a larger file.
func ReadRegularFile(root *os.Root, name string, maxBytes int64) ([]byte, error) {
	lfi, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !lfi.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file (%v)", name, lfi.Mode().Type())
	}
	// name may have been replaced since the Lstat. O_NONBLOCK keeps a FIFO
	// from blocking the open; the mode and SameFile check rejects anything
	// else, including a symlink target (which Root.OpenFile follows) and a
	// reused inode.
	f, err := root.OpenFile(name, os.O_RDONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() || !os.SameFile(lfi, fi) {
		return nil, fmt.Errorf("%s was replaced while it was opened", name)
	}
	b, err := io.ReadAll(io.LimitReader(f, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > maxBytes {
		return nil, fmt.Errorf("%s holds more than %d bytes", name, maxBytes)
	}
	return b, nil
}
