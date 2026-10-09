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

const (
	// fsCheckpointSubdir is the subdirectory under the checkpoint directory where
	// runsc writes filesystem checkpoint files (fscheckpoint.pb, multitar.img,
	// pages_meta.img, pages.img).
	fsCheckpointSubdir   = "fs"
	fsCheckpointManifest = "fscheckpoint.pb"
)

// durableFSCheckpointPaths returns the runsc filesystem checkpoint target
// specs (<container_name>:<mount_path>) for all durable-dir volume mounts
// across containers.
func durableFSCheckpointPaths(containers []*ateompb.Container) []string {
	var paths []string
	for _, c := range containers {
		for _, m := range c.GetDurableDirVolumeMounts() {
			paths = append(paths, fmt.Sprintf("%s:%s", c.GetName(), m.GetMountPath()))
		}
	}
	return paths
}

// durableSnapshotFiles returns the subset of snapshotFiles that belong to the
// filesystem checkpoint in the fs/ subdirectory.
func durableSnapshotFiles(snapshotFiles []string) []string {
	var out []string
	for _, f := range snapshotFiles {
		if filepath.Dir(f) == fsCheckpointSubdir {
			out = append(out, f)
		}
	}
	return out
}

// fsRestoreArgs returns `--fs-restore-image-path <checkpointDir>/fs` if a
// filesystem checkpoint manifest exists in <checkpointDir>/fs, or nil if no
// filesystem checkpoint is present in the snapshot.
func fsRestoreArgs(checkpointDir string) ([]string, error) {
	fsDir := filepath.Join(checkpointDir, fsCheckpointSubdir)
	manifestPath := filepath.Join(fsDir, fsCheckpointManifest)
	if _, err := os.Stat(manifestPath); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("while checking filesystem checkpoint manifest %q: %w", manifestPath, err)
	}
	return []string{"--fs-restore-image-path", fsDir}, nil
}
