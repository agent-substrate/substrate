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
	"strings"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
)

// bootIDPath is where Linux exposes the boot ID: a random UUID generated once
// per boot, so it changes on every reboot, clean or not, and on nothing else.
// It is not namespaced, so atelet reads the node's.
const bootIDPath = "/proc/sys/kernel/random/boot_id"

// readBootID returns the boot ID read from path. It is never empty, so a local
// snapshot whose manifest records none never matches it.
func readBootID(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("while reading the boot ID: %w", err)
	}
	id := strings.TrimSpace(string(b))
	if id == "" {
		return "", fmt.Errorf("boot ID file %s is empty", path)
	}
	return id, nil
}

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
