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
)

// Lchown applies hdr's numeric owner to name, a path relative to root,
// without following a final-component symlink. It is shared by every
// extractor that restores tar ownership: Extract here, and the image layer
// unpack in internal/imagecache. Unlike the rest of this package it builds on
// every platform, because the image cache's unit tests run off Linux.
//
// Giving a file to another uid requires CAP_CHOWN. The production callers run
// as root with it (ateom restoring snapshots, atelet unpacking image layers),
// so any error there means ownership was lost and is returned. An
// unprivileged process cannot chown at all, so rather than making extraction
// unusable outside a root context (unit tests, local tooling), EPERM is
// tolerated there and the extracted files belong to the extracting user.
func Lchown(root *os.Root, name string, hdr *tar.Header) error {
	err := root.Lchown(name, hdr.Uid, hdr.Gid)
	if err == nil {
		return nil
	}
	if errors.Is(err, os.ErrPermission) && os.Geteuid() != 0 {
		return nil
	}
	return fmt.Errorf("restoring ownership of %q to %d:%d: %w", name, hdr.Uid, hdr.Gid, err)
}
