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

package ocispec

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"go.ate.dev/substrate/internal/volumepath"
	"golang.org/x/sys/unix"
)

// PrepareSubPaths pins each selected directory at a stable mount source. Both
// workers call this on start and restore, before their runtime opens the source.
// The caller owns targetDir and must unmount it during sandbox teardown.
func PrepareSubPaths(o Options, targetDir string) error {
	for i, v := range volumeBinds(o) {
		if v.mount.GetSubPath() == "" {
			continue
		}
		if err := stageSubPath(v.source, v.mount.GetSubPath(), filepath.Join(targetDir, fmt.Sprint(i)), v.readOnly); err != nil {
			return fmt.Errorf("mount %q sub_path %q: %w", v.mount.GetMountPath(), v.mount.GetSubPath(), err)
		}
	}
	return nil
}

// openSubPath never follows symlinks and only creates a missing final component.
// Keeping the directory open until the bind completes prevents path-swap races.
func openSubPath(root, sub string, readOnly bool) (*os.File, error) {
	if err := volumepath.ValidateProjected(sub); err != nil {
		return nil, err
	}
	rootFD, err := unix.Open(root, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	defer unix.Close(rootFD)
	how := &unix.OpenHow{Flags: unix.O_PATH | unix.O_DIRECTORY | unix.O_CLOEXEC, Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS}
	fd, err := unix.Openat2(rootFD, sub, how)
	if errors.Is(err, unix.ENOENT) && !readOnly {
		parent, parentErr := unix.Openat2(rootFD, path.Dir(sub), how)
		if parentErr != nil {
			return nil, parentErr
		}
		defer unix.Close(parent)
		// Workers run as root, as does the sandbox process. Match durable directories.
		if err := unix.Mkdirat(parent, path.Base(sub), 0o750); err != nil && !errors.Is(err, unix.EEXIST) {
			return nil, err
		}
		fd, err = unix.Openat2(rootFD, sub, how)
	}
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), sub), nil
}

func stageSubPath(root, sub, target string, readOnly bool) error {
	f, err := openSubPath(root, sub, readOnly)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := os.MkdirAll(target, 0o750); err != nil {
		return err
	}
	if err := unix.Unmount(target, unix.MNT_DETACH); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
		return err
	}
	if err := unix.Mount(fmt.Sprintf("/proc/self/fd/%d", f.Fd()), target, "", unix.MS_BIND, ""); err != nil {
		return err
	}
	if readOnly {
		// Preserve source restrictions while adding read-only to this bind alone.
		var st unix.Statfs_t
		if err := unix.Statfs(target, &st); err != nil {
			return err
		}
		flags := uintptr(unix.MS_BIND|unix.MS_REMOUNT|unix.MS_RDONLY) | uintptr(st.Flags)&(unix.MS_NOSUID|unix.MS_NODEV|unix.MS_NOEXEC|unix.MS_NOATIME|unix.MS_NODIRATIME)
		if st.Flags&unix.ST_RELATIME != 0 {
			flags |= unix.MS_RELATIME
		}
		if err := unix.Mount("", target, "", flags, ""); err != nil {
			return err
		}
	}
	return nil
}
