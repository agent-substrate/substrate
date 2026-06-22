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

package kata

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRootfsDiskGeometry(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a"), make([]byte, 1<<20), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "sub", "b"), make([]byte, 2<<20), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("a", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	// Entries the walk should count: the root dir, sub, a, sub/b, link = 5.
	const wantEntries = 5

	size1, inodes1, err := rootfsDiskGeometry(dir)
	if err != nil {
		t.Fatalf("rootfsDiskGeometry: %v", err)
	}

	// Determinism is required: the cold-boot build and the restore-time rebuild
	// must produce an identically-sized disk for the same tree.
	size2, inodes2, err := rootfsDiskGeometry(dir)
	if err != nil {
		t.Fatal(err)
	}
	if size1 != size2 || inodes1 != inodes2 {
		t.Errorf("non-deterministic geometry: (%d MiB, %d inodes) vs (%d MiB, %d inodes)", size1, inodes1, size2, inodes2)
	}

	// Size must cover the ~3 MiB of contents plus the scratch reserve.
	if floorMiB := rootfsDiskScratchBytes/(1<<20) + 3; size1 < floorMiB {
		t.Errorf("size %d MiB below expected floor %d MiB", size1, floorMiB)
	}

	// Inodes must cover every entry plus the reserve (so a file-heavy rootfs can
	// still create files), and never fewer than the entries present.
	if inodes1 < wantEntries {
		t.Errorf("inodes %d < %d entries", inodes1, wantEntries)
	}
	if inodes1 < 8192 {
		t.Errorf("inodes %d missing the fixed reserve", inodes1)
	}
}

func TestRootfsDiskGeometryMissingDir(t *testing.T) {
	if _, _, err := rootfsDiskGeometry(filepath.Join(t.TempDir(), "does-not-exist")); err == nil {
		t.Fatal("rootfsDiskGeometry on a missing dir: want error, got nil")
	}
}
