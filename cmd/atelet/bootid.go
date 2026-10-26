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
	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
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
