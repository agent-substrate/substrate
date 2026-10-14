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

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
	"github.com/agent-substrate/substrate/internal/snapshotcache"
)

const (
	testRestoreStateDir = "restore-state"
	testRootfsUpperTar  = "rootfs-upper-app.tar"
	testDurableDirTar   = "durable-dir-data.tar"
)

func TestDropStagedMemoryImage(t *testing.T) {
	restoreDir := filepath.Join(t.TempDir(), testRestoreStateDir)
	if err := os.MkdirAll(restoreDir, 0o700); err != nil {
		t.Fatal(err)
	}
	stagedFiles := []string{memoryRangesFile, testRootfsUpperTar, testDurableDirTar}
	for _, name := range stagedFiles {
		if err := os.WriteFile(filepath.Join(restoreDir, name), []byte("staged data"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cfgPath := filepath.Join(restoreDir, "config.json")
	if err := os.WriteFile(cfgPath, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	dropStagedMemoryImage(context.Background(), restoreDir)

	for _, name := range stagedFiles {
		if _, err := os.Stat(filepath.Join(restoreDir, name)); !os.IsNotExist(err) {
			t.Fatalf("staged %s still exists after dropStagedMemoryImage: err=%v", name, err)
		}
	}
	if _, err := os.Stat(cfgPath); err != nil {
		t.Fatalf("config.json should be preserved: %v", err)
	}
}

func TestMaybeDropStagedMemoryImage_PreservedDirNotUnlinked(t *testing.T) {
	restoreDir := filepath.Join(t.TempDir(), testRestoreStateDir)
	if err := os.MkdirAll(restoreDir, 0o700); err != nil {
		t.Fatal(err)
	}
	memFile := filepath.Join(restoreDir, memoryRangesFile)
	want := "preserved snapshot memory"
	if err := os.WriteFile(memFile, []byte(want), 0o600); err != nil {
		t.Fatal(err)
	}

	maybeDropStagedMemoryImage(context.Background(), restoreDir, ch.MemRestoreEager, true)

	got, err := os.ReadFile(memFile)
	if err != nil {
		t.Fatalf("preserved memory-ranges missing: %v", err)
	}
	if string(got) != want {
		t.Fatalf("preserved memory-ranges = %q, want %q", string(got), want)
	}
}

func TestIsSnapshotDataFile(t *testing.T) {
	restoreDir := filepath.Join(t.TempDir(), testRestoreStateDir)
	if err := os.MkdirAll(restoreDir, 0o700); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{memoryRangesFile, testRootfsUpperTar, "config.json"} {
		if err := os.WriteFile(filepath.Join(restoreDir, name), []byte("data"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	// Eager mode (includeMemory=true): both memory-ranges and .tar are matched.
	targetsEager := snapshotcache.FindFiles(restoreDir, func(name string) bool {
		return isSnapshotDataFile(name, true)
	})
	if len(targetsEager) != 2 {
		t.Fatalf("FindFiles(includeMemory=true) returned %d targets, want 2", len(targetsEager))
	}

	// OnDemand mode (includeMemory=false): memory-ranges is excluded, only .tar is matched.
	targetsOnDemand := snapshotcache.FindFiles(restoreDir, func(name string) bool {
		return isSnapshotDataFile(name, false)
	})
	if len(targetsOnDemand) != 1 || filepath.Base(targetsOnDemand[0].Path) != testRootfsUpperTar {
		t.Fatalf("FindFiles(includeMemory=false) returned %+v, want only %s", targetsOnDemand, testRootfsUpperTar)
	}
}
