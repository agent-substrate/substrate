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

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

func TestDropStagedMemoryImage_SingleLink(t *testing.T) {
	restoreDir := filepath.Join(t.TempDir(), "restore-state")
	if err := os.MkdirAll(restoreDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"memory-ranges", rootfsUpperTarFile, durableTarFile} {
		if err := os.WriteFile(filepath.Join(restoreDir, name), []byte("staged data"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	dropStagedMemoryImage(context.Background(), &ateompb.ActorDirs{RestoreDir: restoreDir}, restoreDir)

	for _, name := range []string{"memory-ranges", rootfsUpperTarFile, durableTarFile} {
		if _, err := os.Stat(filepath.Join(restoreDir, name)); !os.IsNotExist(err) {
			t.Fatalf("staged %s still exists after dropStagedMemoryImage: err=%v", name, err)
		}
	}
}

func TestDropStagedMemoryImage_PreservedDirNotUnlinked(t *testing.T) {
	dir := t.TempDir()
	restoreDir := filepath.Join(dir, "restore-state")
	localSnapDir := filepath.Join(dir, "local-checkpoint", "snap-1")
	if err := os.MkdirAll(localSnapDir, 0o700); err != nil {
		t.Fatal(err)
	}
	localFile := filepath.Join(localSnapDir, "memory-ranges")
	want := "direct local checkpoint memory"
	if err := os.WriteFile(localFile, []byte(want), 0o600); err != nil {
		t.Fatal(err)
	}

	dropStagedMemoryImage(context.Background(), &ateompb.ActorDirs{RestoreDir: restoreDir}, localSnapDir)

	got, err := os.ReadFile(localFile)
	if err != nil {
		t.Fatalf("preserved local-checkpoint memory-ranges missing: %v", err)
	}
	if string(got) != want {
		t.Fatalf("preserved local-checkpoint memory-ranges = %q, want %q", string(got), want)
	}
}

func TestDropStagedMemoryImage_HardLinkedPreservesLocalCheckpoint(t *testing.T) {
	dir := t.TempDir()
	localDir := filepath.Join(dir, "local-checkpoint", "snap-1")
	restoreDir := filepath.Join(dir, "restore-state")
	if err := os.MkdirAll(localDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(restoreDir, 0o700); err != nil {
		t.Fatal(err)
	}

	localFile := filepath.Join(localDir, "memory-ranges")
	stagedFile := filepath.Join(restoreDir, "memory-ranges")
	want := "paused guest memory"
	if err := os.WriteFile(localFile, []byte(want), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(localFile, stagedFile); err != nil {
		t.Fatal(err)
	}

	dropStagedMemoryImage(context.Background(), &ateompb.ActorDirs{RestoreDir: restoreDir}, restoreDir)

	if _, err := os.Stat(stagedFile); !os.IsNotExist(err) {
		t.Fatalf("staged memory-ranges still exists: err=%v", err)
	}
	got, err := os.ReadFile(localFile)
	if err != nil {
		t.Fatalf("local-checkpoint memory-ranges missing: %v", err)
	}
	if string(got) != want {
		t.Fatalf("local-checkpoint memory-ranges = %q, want %q", string(got), want)
	}
}

func TestFindAndEvictSnapshotFiles(t *testing.T) {
	dir := t.TempDir()
	localCkptDir := filepath.Join(dir, "local-checkpoint")
	snapDir := filepath.Join(localCkptDir, "snap-1")
	restoreDir := filepath.Join(dir, "restore-state")
	if err := os.MkdirAll(snapDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(restoreDir, 0o700); err != nil {
		t.Fatal(err)
	}

	memFile := filepath.Join(snapDir, "memory-ranges")
	if err := os.WriteFile(memFile, []byte("memory"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(memFile, filepath.Join(restoreDir, "memory-ranges")); err != nil {
		t.Fatal(err)
	}
	tarFile := filepath.Join(restoreDir, "rootfs-upper.tar")
	if err := os.WriteFile(tarFile, []byte("tar"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Eager mode (includeMemory=true): both memory-ranges (deduplicated) and .tar are returned.
	targetsEager := findSnapshotFilesInDirs(true, restoreDir, localCkptDir)
	if len(targetsEager) != 2 {
		t.Fatalf("findSnapshotFilesInDirs(includeMemory=true) returned %d targets, want 2", len(targetsEager))
	}

	// OnDemand mode (includeMemory=false): memory-ranges is excluded, only .tar is returned.
	targetsOnDemand := findSnapshotFilesInDirs(false, restoreDir, localCkptDir)
	if len(targetsOnDemand) != 1 || filepath.Base(targetsOnDemand[0].path) != "rootfs-upper.tar" {
		t.Fatalf("findSnapshotFilesInDirs(includeMemory=false) returned %+v, want only rootfs-upper.tar", targetsOnDemand)
	}
	if !evictSnapshotFiles(targetsEager) {
		t.Errorf("evictSnapshotFiles() = false, want true")
	}

	// Replacing the file with a new inode stops eviction for the old target.
	replacement := filepath.Join(dir, "memory-ranges.new")
	if err := os.WriteFile(replacement, []byte("new memory"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, targetsEager[0].path); err != nil {
		t.Fatal(err)
	}
	if evictSnapshotFiles(targetsEager[:1]) {
		t.Errorf("evictSnapshotFiles() = true after inode replacement, want false")
	}
}
