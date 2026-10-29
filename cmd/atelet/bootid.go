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
	"strings"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/cmd/atelet/internal/atomicfile"
	"go.ate.dev/substrate/internal/imagecache"
)

// localSnapshotBootCheck returns the snapshot tracker's check of whether an
// actor's local snapshot was written on the boot bootID: whether the
// snapshot's manifest records bootID. A manifest that is missing or does not
// parse records none.
func localSnapshotBootCheck(bootID string) func(actorUID, snapshotUUID string) bool {
	return func(actorUID, snapshotUUID string) bool {
		manifest, err := readSnapshotManifest(ateletpath.LocalSnapshotDir(actorUID, snapshotUUID))
		if err != nil {
			return false
		}
		rec, err := unmarshalSandboxRecord(manifest)
		return err == nil && rec.BootID == bootID
	}
}

// clearCachesAfterReboot empties each of cacheDirs, and removes every actor's
// OCI bundle dir and every worker pod's default-runtime dir, when markerPath
// records a boot other than bootID. Then it records bootID there. It reports
// whether it cleared anything.
//
// Cache writes aren't synced, so an unclean reboot can leave cache entries
// and bundle specs empty or truncated. The bundle and default-runtime dirs
// hold specs naming cached layers and hard links to cached assets, which
// atelet reuses instead of linking again. No sandbox survives a reboot, so
// nothing still uses any of them. Run recreates an actor's bundle dir, and the
// worker sandbox syncer recreates a worker's default-runtime dir.
//
// A missing marker only records bootID. atelet may be starting with this
// check for the first time while actors run on what it would clear.
func clearCachesAfterReboot(bootID, markerPath, actorsDir, workersDir string, cacheDirs ...string) (bool, error) {
	b, err := os.ReadFile(markerPath)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return false, writeCacheBootID(markerPath, bootID)
	case err != nil:
		return false, fmt.Errorf("while reading the cache boot ID: %w", err)
	case strings.TrimSpace(string(b)) == bootID:
		return false, nil
	}
	for _, dir := range cacheDirs {
		if err := removeChildren(dir, ""); err != nil {
			return false, err
		}
	}
	if err := removeChildren(actorsDir, "bundles"); err != nil {
		return false, err
	}
	if err := removeChildren(workersDir, defaultRuntimeDirsName); err != nil {
		return false, err
	}
	return true, writeCacheBootID(markerPath, bootID)
}

// removeChildren removes the named entry in each child directory of dir, or
// each child itself when name is empty. A missing dir has nothing to remove.
func removeChildren(dir, name string) error {
	if dir == "" {
		return nil
	}
	children, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("while listing %s: %w", dir, err)
	}
	for _, child := range children {
		if name != "" && !child.IsDir() {
			continue
		}
		path := filepath.Join(dir, child.Name(), name)
		// RemoveAllWritable, not os.RemoveAll: unpacked image layers keep
		// the image's directory modes, which can be read-only.
		if err := imagecache.RemoveAllWritable(path); err != nil {
			return fmt.Errorf("while removing %s: %w", path, err)
		}
	}
	return nil
}

func writeCacheBootID(path, bootID string) error {
	if err := atomicfile.WriteSync(path, []byte(bootID+"\n"), 0o600); err != nil {
		return fmt.Errorf("while recording the cache boot ID: %w", err)
	}
	return nil
}
