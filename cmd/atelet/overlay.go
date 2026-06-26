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

package main

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// setupOverlayfs mounts an overlayfs at target using the given layers.
//   - lowerDir: read-only shared rootfs from the node-level cache
//   - upperDir: per-actor writable layer (created if absent)
//   - workDir:  overlayfs work directory (created if absent)
//
// The target directory must already exist (it is the bundle's rootfs/ dir).
func setupOverlayfs(target, lowerDir, upperDir, workDir string) error {
	for _, d := range []string{upperDir, workDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return fmt.Errorf("creating overlay dir %s: %w", d, err)
		}
	}

	opts := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", lowerDir, upperDir, workDir)
	if err := unix.Mount("overlay", target, "overlay", 0, opts); err != nil {
		return fmt.Errorf("mounting overlayfs at %s: %w", target, err)
	}
	return nil
}

// teardownOverlayfs unmounts the overlayfs at target.  The caller is
// responsible for removing the upper/work directories (typically done by
// resetActorDirs).
func teardownOverlayfs(target string) error {
	if err := unix.Unmount(target, unix.MNT_DETACH); err != nil {
		return fmt.Errorf("unmounting overlayfs at %s: %w", target, err)
	}
	return nil
}

// isOverlayfsAvailable checks whether the overlayfs kernel module is available
// by attempting a mount on a temporary directory.
func isOverlayfsAvailable() bool {
	tmpLower, err := os.MkdirTemp("", "overlay-check-lower-")
	if err != nil {
		return false
	}
	defer os.RemoveAll(tmpLower)

	tmpUpper, err := os.MkdirTemp("", "overlay-check-upper-")
	if err != nil {
		return false
	}
	defer os.RemoveAll(tmpUpper)

	tmpWork, err := os.MkdirTemp("", "overlay-check-work-")
	if err != nil {
		return false
	}
	defer os.RemoveAll(tmpWork)

	tmpTarget, err := os.MkdirTemp("", "overlay-check-target-")
	if err != nil {
		return false
	}
	defer os.RemoveAll(tmpTarget)

	opts := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", tmpLower, tmpUpper, tmpWork)
	if err := unix.Mount("overlay", tmpTarget, "overlay", 0, opts); err != nil {
		return false
	}
	_ = unix.Unmount(tmpTarget, 0)
	return true
}
