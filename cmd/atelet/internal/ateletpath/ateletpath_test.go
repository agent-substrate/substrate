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
	under := map[string]string{
		"oci_bundle_dir":                actorDirs.GetOciBundleDir(),
		"checkpoint_dir":                actorDirs.GetCheckpointDir(),
		"restore_dir":                   actorDirs.GetRestoreDir(),
		"durable_dir_volume_mounts_dir": actorDirs.GetDurableDirVolumeMountsDir(),
		"system_info_volume_roots_dir":  actorDirs.GetSystemInfoVolumeRootsDir(),
		"volumes_dir":                   actorDirs.GetVolumesDir(),
	}
	seen := map[string]string{}
	for field, dir := range under {
		rel, err := filepath.Rel(actorDirs.GetRootDir(), dir)
		if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
			t.Errorf("%s = %q is not a proper subdirectory of root_dir %q", field, dir, actorDirs.GetRootDir())
		}
		if other, dup := seen[dir]; dup {
			t.Errorf("%s and %s share %q", field, other, dir)
		}
		seen[dir] = field
	}
}
