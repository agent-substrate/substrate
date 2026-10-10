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

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// fsCheckpointSuffixes are the suffixes of the files runsc writes per
// filesystem checkpoint bundle prefix.
var fsCheckpointSuffixes = []string{
	"_fscheckpoint.pb",
	"_multitar.img",
	"_pages_meta.img",
	"_pages.img",
}

// rootfsBundlePrefix returns the bundle prefix used for a container's root
// filesystem overlay checkpoint (<container_name>_rootfs).
func rootfsBundlePrefix(containerName string) string {
	return containerName + "_rootfs"
}

// rootfsBundlePrefixes returns the bundle prefixes for all containers' root
// filesystem overlay checkpoints.
func rootfsBundlePrefixes(containers []*ateompb.Container) []string {
	prefixes := make([]string, 0, len(containers))
	for _, c := range containers {
		prefixes = append(prefixes, rootfsBundlePrefix(c.GetName()))
	}
	return prefixes
}

// rootfsFSCheckpointPaths returns the runsc filesystem checkpoint bundle
// target specs (<container_name>_rootfs=<container_name>:/) for all
// containers' root filesystem overlays.
func rootfsFSCheckpointPaths(containers []*ateompb.Container) []string {
	paths := make([]string, 0, len(containers))
	for _, c := range containers {
		paths = append(paths, fmt.Sprintf("%s=%s:/", rootfsBundlePrefix(c.GetName()), c.GetName()))
	}
	return paths
}

// durableFSCheckpointPaths returns the runsc filesystem checkpoint bundle
// target specs (<volume_name>=<container_name>:<mount_path>) for all
// durable-dir volume mounts across containers.
func durableFSCheckpointPaths(containers []*ateompb.Container) []string {
	var paths []string
	for _, c := range containers {
		for _, m := range c.GetDurableDirVolumeMounts() {
			paths = append(paths, fmt.Sprintf("%s=%s:%s", m.GetVolumeName(), c.GetName(), m.GetMountPath()))
		}
	}
	return paths
}

// fsRestorePrefixes returns the filesystem checkpoint bundle prefixes to
// restore for the requested snapshot fidelity:
//   - VOLUMES: only durable-dir volume bundles
//   - ROOTFS, MEMORY: container rootfs bundles plus durable-dir volume bundles
func fsRestorePrefixes(fidelity ateompb.SnapshotFidelity, containers []*ateompb.Container, durableVolumes []string) []string {
	switch fidelity {
	case ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES:
		return durableVolumes
	case ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS, ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY:
		return append(rootfsBundlePrefixes(containers), durableVolumes...)
	default:
		return nil
	}
}

// durableSnapshotFiles returns the subset of snapshotFiles that belong to the
// given durable-dir volumes.
func durableSnapshotFiles(snapshotFiles, volumes []string) []string {
	want := make(map[string]struct{}, len(volumes)*len(fsCheckpointSuffixes))
	for _, vol := range volumes {
		for _, suffix := range fsCheckpointSuffixes {
			want[vol+suffix] = struct{}{}
		}
	}
	var out []string
	for _, f := range snapshotFiles {
		if _, ok := want[f]; ok {
			out = append(out, f)
		}
	}
	return out
}

// fsRestoreArgs returns the `--fs-restore-image-path <checkpointDir>/<prefix>`
// arguments for each filesystem checkpoint bundle prefix whose manifest exists
// in checkpointDir. A prefix with no manifest in the snapshot (e.g. a volume
// added to the template since) is skipped so it starts empty.
func fsRestoreArgs(checkpointDir string, prefixes []string) ([]string, error) {
	var args []string
	for _, prefix := range prefixes {
		manifestPath := filepath.Join(checkpointDir, prefix+"_fscheckpoint.pb")
		if _, err := os.Stat(manifestPath); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("while checking filesystem checkpoint manifest %q: %w", manifestPath, err)
		}
		args = append(args, "--fs-restore-image-path", filepath.Join(checkpointDir, prefix))
	}
	return args, nil
}
