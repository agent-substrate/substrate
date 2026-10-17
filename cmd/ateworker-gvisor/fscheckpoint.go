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

package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
)

const fsCheckpointManifest = "fscheckpoint.pb"

// rootfsFSCheckpointPaths returns the runsc filesystem checkpoint target specs
// (rootfs/=<container_name>:/) for all workload containers' rootfs overlays,
// grouping them into the "rootfs/" subdirectory bundle.
func rootfsFSCheckpointPaths(containers []*ateworkerpb.Container) []string {
	paths := make([]string, 0, len(containers))
	for _, c := range containers {
		paths = append(paths, fmt.Sprintf("%s/=%s:/", resources.RootfsCheckpointSubdir, c.GetName()))
	}
	return paths
}

// durableFSCheckpointPaths returns the runsc filesystem checkpoint target
// specs (fs/=<container_name>:<mount_path>) for all durable-dir volume mounts
// across containers, grouping them into the "fs/" subdirectory bundle. Every
// mount is emitted even when a volume is shared across multiple mounts: runsc
// creates one shared tmpfs under whichever mount is set up first and
// deduplicates by vfs.Filesystem when saving.
func durableFSCheckpointPaths(containers []*ateworkerpb.Container) []string {
	var paths []string
	for _, c := range containers {
		for _, m := range c.GetDurableDirVolumeMounts() {
			paths = append(paths, fmt.Sprintf("%s/=%s:%s", resources.FSCheckpointSubdir, c.GetName(), m.GetMountPath()))
		}
	}
	return paths
}

// durableSnapshotFiles returns the subset of snapshotFiles that belong to the
// durable-dir filesystem checkpoint in the fs/ subdirectory.
func durableSnapshotFiles(snapshotFiles []string) []string {
	var out []string
	for _, f := range snapshotFiles {
		if filepath.Dir(f) == resources.FSCheckpointSubdir {
			out = append(out, f)
		}
	}
	return out
}

// fsRestoreArgs returns repeatable `--fs-restore-image-path` arguments for the
// filesystem checkpoint bundles in checkpointDir that apply at the requested
// fidelity:
//   - ROOTFS and MEMORY require `<checkpointDir>/rootfs/fscheckpoint.pb`.
//   - Every fidelity uses `<checkpointDir>/fs` when
//     `<checkpointDir>/fs/fscheckpoint.pb` exists.
func fsRestoreArgs(checkpointDir string, fidelity ateworkerpb.SnapshotFidelity) ([]string, error) {
	var subdirs []string
	if fidelity == ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS ||
		fidelity == ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY {
		subdirs = append(subdirs, resources.RootfsCheckpointSubdir)
	}
	subdirs = append(subdirs, resources.FSCheckpointSubdir)

	var args []string
	for _, sub := range subdirs {
		dir := filepath.Join(checkpointDir, sub)
		manifestPath := filepath.Join(dir, fsCheckpointManifest)
		if _, err := os.Stat(manifestPath); err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				return nil, fmt.Errorf("while checking filesystem checkpoint manifest %q: %w", manifestPath, err)
			}
			if sub == resources.RootfsCheckpointSubdir {
				return nil, fmt.Errorf("%v snapshot has no rootfs filesystem checkpoint %q: %w", fidelity, manifestPath, err)
			}
			continue
		}
		args = append(args, "--fs-restore-image-path", dir)
	}
	return args, nil
}

// fsRestoreContainers returns, in order, the containers that register an entry
// in runsc's fsRestore waitMap for `runsc wait -fsrestore <container>`: those
// whose filesystems fsRestoreArgs restores at the requested fidelity. ROOTFS
// and MEMORY restore every container's rootfs overlay, so every container has
// an entry. VOLUMES restores only durable-dir volumes, so only a container
// that owns the master mount of at least one volume (the first container to
// mount it) has one.
func fsRestoreContainers(fidelity ateworkerpb.SnapshotFidelity, containers []*ateworkerpb.Container) []string {
	if fidelity == ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS ||
		fidelity == ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY {
		out := make([]string, 0, len(containers))
		for _, c := range containers {
			out = append(out, c.GetName())
		}
		return out
	}
	seen := make(map[string]bool)
	var out []string
	for _, c := range containers {
		ownsMount := false
		for _, m := range c.GetDurableDirVolumeMounts() {
			if !seen[m.GetName()] {
				seen[m.GetName()] = true
				ownsMount = true
			}
		}
		if ownsMount {
			out = append(out, c.GetName())
		}
	}
	return out
}
