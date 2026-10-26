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
	"os"
	"path/filepath"
	"testing"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
)

// localSnapshotBootCheck matches a local snapshot only when its manifest
// records the given boot ID.
func TestLocalSnapshotBootCheck(t *testing.T) {
	const (
		actorUID     = "01234567-89ab-cdef-0123-456789abcdef"
		snapshotUUID = "pause-snap-1"
	)
	for _, tc := range []struct {
		name string
		// manifest is the manifest written beside the snapshot, or empty
		// for none.
		manifest string
		want     bool
	}{
		{name: "records the current boot", manifest: `{"bootId":"boot-2"}`, want: true},
		{name: "records an earlier boot", manifest: `{"bootId":"boot-1"}`},
		{name: "records no boot", manifest: `{}`},
		{name: "does not parse", manifest: `{"bootId":`},
		{name: "is missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useTempNodeDirs(t)
			if tc.manifest != "" {
				dir := ateletpath.LocalSnapshotDir(actorUID, snapshotUUID)
				if err := os.MkdirAll(dir, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, sandboxManifestName), []byte(tc.manifest), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if got := localSnapshotBootCheck("boot-2")(actorUID, snapshotUUID); got != tc.want {
				t.Errorf("localSnapshotBootCheck(%q) = %v, want %v", "boot-2", got, tc.want)
			}
		})
	}
}
