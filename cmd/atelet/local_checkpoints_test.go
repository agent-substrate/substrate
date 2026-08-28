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
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeSnapshotDir(t *testing.T, dir, prefix string) {
	t.Helper()
	p := filepath.Join(dir, prefix)
	if err := os.MkdirAll(p, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p, "memory.img"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestPruneRemovesEverySnapshot(t *testing.T) {
	dir := t.TempDir()
	writeSnapshotDir(t, dir, "pause-1")
	writeSnapshotDir(t, dir, "pause-2")
	writeSnapshotDir(t, dir, "pause-3")

	if err := pruneLocalCheckpointDir(context.Background(), dir); err != nil {
		t.Fatalf("pruneLocalCheckpointDir() = %v, want nil", err)
	}

	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("dir still exists (err=%v), want removed entirely", err)
	}
}

func TestPruneMissingDirIsNoop(t *testing.T) {
	if err := pruneLocalCheckpointDir(context.Background(), filepath.Join(t.TempDir(), "absent")); err != nil {
		t.Fatalf("pruneLocalCheckpointDir() = %v, want nil", err)
	}
}

// An undeletable snapshot must be reported — Terminate turns that error into a
// failed RPC so the delete workflow retries — without stranding the snapshots
// that could have been removed.
func TestPruneReportsFailureAndStillRemovesTheRest(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions, so no snapshot can be made undeletable")
	}
	dir := t.TempDir()
	writeSnapshotDir(t, dir, "pause-1")
	writeSnapshotDir(t, dir, "pause-2")

	// A snapshot dir with no write bit: its files cannot be unlinked.
	stuck := filepath.Join(dir, "pause-stuck")
	writeSnapshotDir(t, dir, "pause-stuck")
	if err := os.Chmod(stuck, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(stuck, 0o700) })

	err := pruneLocalCheckpointDir(context.Background(), dir)
	if err == nil {
		t.Fatal("pruneLocalCheckpointDir() = nil, want an error naming the undeletable snapshot")
	}
	if !strings.Contains(err.Error(), "pause-stuck") {
		t.Errorf("pruneLocalCheckpointDir() = %v, want it to name pause-stuck", err)
	}
	for _, name := range []string{"pause-1", "pause-2"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Errorf("%s still exists (err=%v), want removed", name, err)
		}
	}
}
