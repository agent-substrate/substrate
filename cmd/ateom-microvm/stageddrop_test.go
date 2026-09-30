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

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func writeStagedFile(t *testing.T, path string, size int) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("creating parent of %q: %v", path, err)
	}
	if err := os.WriteFile(path, make([]byte, size), 0o600); err != nil {
		t.Fatalf("writing %q: %v", path, err)
	}
}

func assertNotExist(t *testing.T, path, why string) {
	t.Helper()
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("%s: %q still exists (lstat err = %v)", why, path, err)
	}
}

// The staged name must be gone by the time startStagedDrop returns: that is what
// lets the resume path hand the bytes off instead of waiting for them.
func TestStagedDropRemovesStagedNameBeforeReturning(t *testing.T) {
	staged := filepath.Join(t.TempDir(), "memory-ranges")
	writeStagedFile(t, staged, 4096)

	drop := startStagedDrop(context.Background(), staged)
	if drop == nil {
		t.Fatal("startStagedDrop returned nil for a staged file that exists")
	}
	assertNotExist(t, staged, "staged name must be free before startStagedDrop returns")

	drop.wait()
	assertNotExist(t, staged, "staged name after the drop completes")
	assertNotExist(t, drop.path, "renamed path after the drop completes")
}

// The unlink must target the renamed path, never the original name. That is what
// makes a later restore safe: atelet re-stages memory-ranges under the original
// name, and a drop that still pointed there would delete the fresh file.
func TestStagedDropUnlinksTheRenamedPathNotTheStagedName(t *testing.T) {
	staged := filepath.Join(t.TempDir(), "memory-ranges")
	writeStagedFile(t, staged, 1<<20)

	drop := startStagedDrop(context.Background(), staged)
	if drop == nil {
		t.Fatal("startStagedDrop returned nil for a staged file that exists")
	}
	if drop.path == staged {
		t.Fatalf("drop unlinks the staged name %q, so it can race a re-stage", staged)
	}
	if want := staged + dropSuffix; drop.path != want {
		t.Fatalf("drop path = %q, want %q", drop.path, want)
	}
	if _, err := os.Lstat(drop.path); err != nil {
		t.Fatalf("the path the drop unlinks is not the renamed file: %v", err)
	}

	drop.wait()
}

// A re-staged file under the original name survives the drop, whatever order the
// two happen in.
func TestStagedDropDoesNotRaceAReStagedFile(t *testing.T) {
	dir := t.TempDir()
	staged := filepath.Join(dir, "memory-ranges")
	writeStagedFile(t, staged, 1<<20)

	drop := startStagedDrop(context.Background(), staged)
	// Re-stage the same name, whether or not the unlink has already run.
	writeStagedFile(t, staged, 512)

	drop.wait()
	info, err := os.Stat(staged)
	if err != nil {
		t.Fatalf("a re-staged %q was removed by the previous drop: %v", staged, err)
	}
	if info.Size() != 512 {
		t.Fatalf("re-staged file was clobbered: size = %d, want 512", info.Size())
	}
}

// An absent staged file is the expected outcome, not an error: a teardown that
// removed it first is what the drop wanted.
func TestStagedDropOnMissingPathIsNotAnError(t *testing.T) {
	staged := filepath.Join(t.TempDir(), "memory-ranges")

	if drop := startStagedDrop(context.Background(), staged); drop != nil {
		drop.wait()
		t.Fatalf("startStagedDrop returned a drop for a path that does not exist: %+v", drop)
	}
}

// Directories are dropped the same way, so the checkpoint tar's whole upper
// directory does not have to be unlinked on the caller's thread.
func TestStagedDropRemovesADirectory(t *testing.T) {
	upper := filepath.Join(t.TempDir(), "rootfs-upper")
	writeStagedFile(t, filepath.Join(upper, "cid", "fs", "file"), 1024)

	drop := startStagedDrop(context.Background(), upper)
	if drop == nil {
		t.Fatal("startStagedDrop returned nil for a staged directory that exists")
	}
	assertNotExist(t, upper, "staged directory must be gone before startStagedDrop returns")

	drop.wait()
	assertNotExist(t, upper, "staged directory after the drop completes")
}

// wait must be safe to call more than once: the teardown path and a test may both
// want to be sure the unlink is done.
func TestStagedDropWaitIsIdempotent(t *testing.T) {
	staged := filepath.Join(t.TempDir(), "memory-ranges")
	writeStagedFile(t, staged, 256)

	drop := startStagedDrop(context.Background(), staged)
	if drop == nil {
		t.Fatal("startStagedDrop returned nil for a staged file that exists")
	}
	drop.wait()
	drop.wait()
}
