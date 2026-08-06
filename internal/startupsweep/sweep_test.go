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

package startupsweep

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestSweepRemovesMatchingFiles checks that only files matching
// the registered pattern are deleted.
func TestSweepRemovesMatchingFiles(t *testing.T) {
	// Given
	dir := t.TempDir()
	orphan := filepath.Join(dir, ".tmp-orphan")
	keep := filepath.Join(dir, "keep")
	writeFile(t, orphan)
	writeFile(t, keep)

	// When
	sw := New()
	sw.Register("test files", dir, ".tmp-*", os.Remove)
	sw.Sweep(context.Background())

	// Then
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Errorf("orphan file was not removed")
	}
	if _, err := os.Stat(keep); err != nil {
		t.Errorf("non-matching file was removed: %v", err)
	}
}

// TestSweepRemovesMatchingDirs checks that matching directories
// are deleted correctly and that not matching dirs are kept.
func TestSweepRemovesMatchingDirs(t *testing.T) {
	// Given
	dir := t.TempDir()
	orphan := filepath.Join(dir, ".tmp-orphan-dir")
	keep := filepath.Join(dir, "keep-dir")
	makeDir(t, orphan)
	makeDir(t, keep)

	// When
	sw := New()
	sw.Register("test dirs", dir, ".tmp-*", os.RemoveAll)
	sw.Sweep(context.Background())

	// Then
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Errorf("orphan dir was not removed")
	}
	if _, err := os.Stat(keep); err != nil {
		t.Errorf("non-matching dir was removed: %v", err)
	}
}

// TestSweepNoMatchIsNoop checks that no-op when there are no matching
// files or directories to be swept.
func TestSweepNoMatchIsNoop(t *testing.T) {
	// Given
	dir := t.TempDir()
	keep := filepath.Join(dir, "keep")
	writeFile(t, keep)

	// When
	sw := New()
	sw.Register("test", dir, ".tmp-*", os.Remove)
	sw.Sweep(context.Background())

	// Then
	if _, err := os.Stat(keep); err != nil {
		t.Errorf("non-matching file was removed: %v", err)
	}
}

// TestAddDelegatesRegistration validates that registration via Add
// removes matching files.
func TestAddDelegatesRegistration(t *testing.T) {
	// Given
	dir := t.TempDir()
	orphan := filepath.Join(dir, ".tmp-via-add")
	writeFile(t, orphan)

	register := func(s *Sweeper) {
		s.Register("add test", dir, ".tmp-*", os.Remove)
	}

	// When
	sw := New()
	sw.Add(register)
	sw.Sweep(context.Background())

	// Then
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Errorf("orphan registered via Add was not removed")
	}
}

// TestRegisterOnNilSweeperIsNoop checks that registering on a nil
// Sweeper does not panic.
func TestRegisterOnNilSweeperIsNoop(t *testing.T) {
	// Given
	var sw *Sweeper

	// Then
	sw.Register("nil test", t.TempDir(), "*", os.Remove) // must not panic
}

// TestSweepMissingDirIsNoop checks that sweeping a non-existent
// directory does not panic.
func TestSweepMissingDirIsNoop(t *testing.T) {
	// Given
	sw := New()

	// When
	sw.Register("missing dir", "/nonexistent/dir/that/cannot/exist", "*.tmp", os.Remove)

	// Then
	sw.Sweep(context.Background()) // must not panic or error
}

func writeFile(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func makeDir(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
}
