// Copyright 2026 The Agent Substrate Authors
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

package actordirs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/cmd/atelet/internal/trash"
	"go.ate.dev/substrate/internal/nodepath"
)

func useTempNodeDirs(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	origActors := nodepath.ActorsDir
	origSnapshots := nodepath.SnapshotsDir
	nodepath.ActorsDir = filepath.Join(root, "actors")
	nodepath.SnapshotsDir = filepath.Join(root, "snapshots")
	t.Cleanup(func() {
		nodepath.ActorsDir = origActors
		nodepath.SnapshotsDir = origSnapshots
	})
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestManagerResetActorDirsRetiresToPerMountTrash(t *testing.T) {
	useTempNodeDirs(t)
	const actorUID = "actor-1"

	writeFile(t, filepath.Join(ateletpath.DurableDirVolumeMountsDir(actorUID), "ws", "file.txt"), "data")
	writeFile(t, filepath.Join(ateletpath.OCIBundleDir(actorUID), "ctr", "config.json"), "{}")
	writeFile(t, filepath.Join(ateletpath.CheckpointStateDir(actorUID), "checkpoint.img"), "mem")
	writeFile(t, filepath.Join(ateletpath.RestoreStateDir(actorUID), "restore.img"), "mem")
	writeFile(t, filepath.Join(ateletpath.SystemInfoVolumeRootsDir(actorUID), "ca", "cert.pem"), "pem")
	writeFile(t, ateletpath.ActorSnapshotStateFile(actorUID), "{}")

	// Reaper not started yet: retired entries sit in .trash/ so we can verify
	// per-mount placement before starting workers.
	reaper := trash.New(t.Context())
	mgr := New(reaper)

	if err := mgr.ResetActorDirs(t.Context(), actorUID); err != nil {
		t.Fatalf("ResetActorDirs: %v", err)
	}

	// Fresh empty directories are recreated immediately.
	for _, dir := range []string{
		ateletpath.DurableDirVolumeMountsDir(actorUID),
		ateletpath.OCIBundleDir(actorUID),
		ateletpath.CheckpointStateDir(actorUID),
		ateletpath.RestoreStateDir(actorUID),
		ateletpath.SystemInfoVolumeRootsDir(actorUID),
	} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("ReadDir(%s): %v", dir, err)
		}
		if len(entries) != 0 {
			t.Errorf("%s not empty after ResetActorDirs: %v", dir, entries)
		}
	}

	// snapshot-state.json is preserved.
	if _, err := os.Stat(ateletpath.ActorSnapshotStateFile(actorUID)); err != nil {
		t.Errorf("snapshot-state.json missing after ResetActorDirs: %v", err)
	}

	// ActorsDir/.trash received bundles, durable-dir, system-info (3);
	// SnapshotsDir/.trash received checkpoint-state, restore-state (2).
	actorTrashEntries, err := os.ReadDir(ateletpath.ActorsTrashDir())
	if err != nil {
		t.Fatalf("ReadDir(ActorsTrashDir): %v", err)
	}
	if len(actorTrashEntries) != 3 {
		t.Errorf("ActorsTrashDir has %d entries, want 3", len(actorTrashEntries))
	}
	snapTrashEntries, err := os.ReadDir(ateletpath.SnapshotsTrashDir())
	if err != nil {
		t.Fatalf("ReadDir(SnapshotsTrashDir): %v", err)
	}
	if len(snapTrashEntries) != 2 {
		t.Errorf("SnapshotsTrashDir has %d entries, want 2", len(snapTrashEntries))
	}

	// Starting the reaper drains both trash dirs.
	reaper.Start()
	t.Cleanup(func() {
		_ = reaper.Wait(context.Background())
	})
	if err := reaper.WaitForActor(t.Context(), actorUID); err != nil {
		t.Fatalf("WaitForActor: %v", err)
	}
	for _, trashDir := range []string{ateletpath.ActorsTrashDir(), ateletpath.SnapshotsTrashDir()} {
		entries, err := os.ReadDir(trashDir)
		if err != nil {
			t.Fatalf("ReadDir(%s): %v", trashDir, err)
		}
		if len(entries) != 0 {
			t.Errorf("%s still has %d entries after WaitForActor", trashDir, len(entries))
		}
	}
}

func TestManagerRemoveActorFilesExceptStateKeepsState(t *testing.T) {
	useTempNodeDirs(t)
	const actorUID = "actor-2"

	writeFile(t, filepath.Join(ateletpath.LocalSnapshotDir(actorUID, "snap-1"), "checkpoint.img"), "mem")
	writeFile(t, filepath.Join(ateletpath.DurableDirVolumeMountsDir(actorUID), "file.txt"), "data")
	writeFile(t, ateletpath.ActorSnapshotStateFile(actorUID), `{"latest":"snap-1"}`)

	reaper := trash.New(t.Context())
	reaper.Start()
	t.Cleanup(func() {
		_ = reaper.Wait(context.Background())
	})

	if err := New(reaper).RemoveActorFilesExceptState(t.Context(), actorUID); err != nil {
		t.Fatalf("RemoveActorFilesExceptState: %v", err)
	}
	if _, err := os.Stat(ateletpath.ActorSnapshotStateFile(actorUID)); err != nil {
		t.Fatalf("snapshot-state.json missing: %v", err)
	}
	if _, err := os.Stat(ateletpath.ActorSnapshotPath(actorUID)); !os.IsNotExist(err) {
		t.Fatalf("ActorSnapshotPath still exists: %v", err)
	}
	if err := reaper.WaitForActor(t.Context(), actorUID); err != nil {
		t.Fatalf("WaitForActor: %v", err)
	}
}
