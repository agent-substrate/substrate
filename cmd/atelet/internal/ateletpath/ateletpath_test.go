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

package ateletpath

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestActorDirs(t *testing.T) {
	const actorUID = "actor-uid-1"
	actorDirs := ActorDirs(actorUID)
	if actorDirs.GetRootDir() != ActorPath(actorUID) {
		t.Fatalf("root_dir = %q, want %q", actorDirs.GetRootDir(), ActorPath(actorUID))
	}
	snapshotRoot := ActorSnapshotPath(actorUID)
	// Disjoint trees: neither root may sit inside (or equal) the other.
	for _, p := range [][2]string{{snapshotRoot, actorDirs.GetRootDir()}, {actorDirs.GetRootDir(), snapshotRoot}} {
		if rel, err := filepath.Rel(p[1], p[0]); err == nil && !strings.HasPrefix(rel, "..") {
			t.Fatalf("%q is inside %q; ActorSnapshotPath and ActorPath must be disjoint", p[0], p[1])
		}
	}

	underActorRoot := map[string]string{
		"oci_bundle_dir":                actorDirs.GetOciBundleDir(),
		"durable_dir_volume_mounts_dir": actorDirs.GetDurableDirVolumeMountsDir(),
		"system_info_volume_roots_dir":  actorDirs.GetSystemInfoVolumeRootsDir(),
		"external_volume_mounts_dir":    actorDirs.GetExternalVolumeMountsDir(),
		"sandbox_assets_dir":            actorDirs.GetSandboxAssetsDir(),
	}
	underSnapshotRoot := map[string]string{
		"checkpoint_dir":        actorDirs.GetCheckpointDir(),
		"restore_dir":           actorDirs.GetRestoreDir(),
		"local_checkpoints_dir": LocalCheckpointsDir(actorUID),
		"local_snapshot_dir":    LocalSnapshotDir(actorUID, "snap-1"),
	}

	seen := map[string]string{}
	checkSubdirs := func(parentName, parent string, dirs map[string]string) {
		for field, dir := range dirs {
			rel, err := filepath.Rel(parent, dir)
			if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
				t.Errorf("%s = %q is not a proper subdirectory of %s %q", field, dir, parentName, parent)
			}
			if other, dup := seen[dir]; dup {
				t.Errorf("%s and %s share %q", field, other, dir)
			}
			seen[dir] = field
		}
	}
	checkSubdirs("root_dir", actorDirs.GetRootDir(), underActorRoot)
	checkSubdirs("snapshot_root", snapshotRoot, underSnapshotRoot)
}
