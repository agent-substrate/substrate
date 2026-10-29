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
	"path/filepath"
	"syscall"
	"testing"

	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateletpb"
)

// TestLocalSnapshotGCWithSeparateSnapshotsFilesystem runs TestLocalSnapshotGC
// with SnapshotsDir on a different filesystem from ActorsDir.
func TestLocalSnapshotGCWithSeparateSnapshotsFilesystem(t *testing.T) {
	for _, fidelity := range []ateletpb.SnapshotFidelity{
		ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
		ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
		ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
	} {
		t.Run(fidelity.String(), func(t *testing.T) {
			runLocalSnapshotGC(t, fidelity, useSeparateSnapshotsFilesystem)
		})
	}
}

// useSeparateSnapshotsFilesystem is useTempNodeDirs with SnapshotsDir on another mount.
func useSeparateSnapshotsFilesystem(t *testing.T) {
	t.Helper()
	useTempNodeDirs(t)
	shm := otherMount(t)
	root := filepath.Dir(nodepath.ActorsDir)
	if err := canHardLink(root, shm); !errors.Is(err, syscall.EXDEV) {
		t.Fatalf("canHardLink(%s, %s) = %v, want EXDEV: SnapshotsDir must be on another mount than ActorsDir", root, shm, err)
	}
	nodepath.SnapshotsDir = filepath.Join(shm, "snapshots")
}
