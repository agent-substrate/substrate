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

	pruneLocalCheckpointDir(context.Background(), dir)

	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("dir still exists (err=%v), want removed entirely", err)
	}
}

func TestPruneMissingDirIsNoop(t *testing.T) {
	pruneLocalCheckpointDir(context.Background(), filepath.Join(t.TempDir(), "absent"))
}
