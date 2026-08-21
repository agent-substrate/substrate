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

package checkpointmarker

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateompath"
)

// useTempActorsDir points the shared actor-state root at a temp directory for
// the duration of the test, and creates the actor's checkpoint dir (ateom
// makes it before checkpointing).
func useTempActorsDir(t *testing.T, actorUID string) {
	t.Helper()
	orig := ateompath.ActorsDir
	t.Cleanup(func() { ateompath.ActorsDir = orig })
	ateompath.ActorsDir = t.TempDir()

	if err := os.MkdirAll(ateompath.CheckpointStateDir(actorUID), 0o700); err != nil {
		t.Fatalf("creating checkpoint dir: %v", err)
	}
}

// testScope stands in for a CheckpointWorkloadRequest's stringified scope.
const testScope = "SNAPSHOT_SCOPE_FULL"

func TestWriteThenRead(t *testing.T) {
	const actorUID = "actor-1"
	useTempActorsDir(t, actorUID)

	want := []string{"checkpoint.img", "pages.img", "pages_meta.img"}
	if err := Write(actorUID, testScope, want); err != nil {
		t.Fatalf("Write: %v", err)
	}

	rec, ok, err := Read(actorUID, testScope)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !ok {
		t.Fatal("Read reported no marker after Write")
	}
	if !slices.Equal(rec.SnapshotFiles, want) {
		t.Errorf("SnapshotFiles = %v, want %v", rec.SnapshotFiles, want)
	}
}

func TestWriteLeavesNoTempFiles(t *testing.T) {
	const actorUID = "actor-1"
	useTempActorsDir(t, actorUID)

	if err := Write(actorUID, testScope, []string{"checkpoint.img"}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// The atomic write renames a temp file into place; anything left beside the
	// marker would be shipped as snapshot content by a caller listing the dir.
	entries, err := os.ReadDir(ateompath.CheckpointStateDir(actorUID))
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != ateompath.CheckpointDoneFileName {
		var got []string
		for _, e := range entries {
			got = append(got, e.Name())
		}
		t.Errorf("checkpoint dir contents = %v, want only %q", got, ateompath.CheckpointDoneFileName)
	}
}

func TestReadNoMarker(t *testing.T) {
	const actorUID = "actor-1"
	useTempActorsDir(t, actorUID)

	// The ordinary first-attempt case: no marker is not an error, or every
	// checkpoint would fail before it started.
	rec, ok, err := Read(actorUID, testScope)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if ok || rec != nil {
		t.Errorf("Read = (%v, %v), want (nil, false)", rec, ok)
	}
}

// An unusable marker is discarded and reported as "no completed checkpoint",
// so the caller re-runs the checkpoint. Returning an error instead would wedge
// the actor: nothing else deletes the marker, so every retry would fail here
// identically while the control plane re-drove a workflow that could never
// progress.
func TestReadDiscardsUnusableMarker(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{"corrupt", "{not json"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const actorUID = "actor-1"
			useTempActorsDir(t, actorUID)

			path := filepath.Join(ateompath.CheckpointStateDir(actorUID), ateompath.CheckpointDoneFileName)
			if err := os.WriteFile(path, []byte(tt.content), 0o600); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}

			rec, ok, err := Read(actorUID, testScope)
			if err != nil {
				t.Fatalf("Read: %v, want no error", err)
			}
			if ok || rec != nil {
				t.Errorf("Read = (%v, %v), want (nil, false)", rec, ok)
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Errorf("marker still on disk (err=%v), want removed", err)
			}
		})
	}
}

// A Data-scope checkpoint of an actor whose only volumes are CSI ones writes
// no snapshot files, and is still a checkpoint that completed and took the
// guest down. It is exactly the result that most needs replaying: without a
// marker the retry finds no guest either and reports the actor unrecoverable,
// when in truth its checkpoint had succeeded.
func TestWriteThenReadEmptyFileSet(t *testing.T) {
	const actorUID = "actor-1"
	useTempActorsDir(t, actorUID)

	if err := Write(actorUID, testScope, nil); err != nil {
		t.Fatalf("Write: %v", err)
	}

	rec, ok, err := Read(actorUID, testScope)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !ok {
		t.Fatal("Read reported no completed checkpoint, want the recorded one")
	}
	if len(rec.SnapshotFiles) != 0 {
		t.Errorf("SnapshotFiles = %v, want none", rec.SnapshotFiles)
	}

	path := filepath.Join(ateompath.CheckpointStateDir(actorUID), ateompath.CheckpointDoneFileName)
	if _, err := os.Stat(path); err != nil {
		t.Errorf("marker not on disk (err=%v), want it kept", err)
	}
}

// Write refuses an unscoped marker: Read can only match a marker against the
// scope asked for, so one without a scope could never be replayed.
func TestWriteRejectsEmptyScope(t *testing.T) {
	const actorUID = "actor-1"
	useTempActorsDir(t, actorUID)

	if err := Write(actorUID, "", []string{"checkpoint.img"}); err == nil {
		t.Fatal("Write succeeded, want an error")
	}
	path := filepath.Join(ateompath.CheckpointStateDir(actorUID), ateompath.CheckpointDoneFileName)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("marker written (err=%v), want none", err)
	}
}

// The marker records one particular checkpoint, not the fact that the actor
// has had one. Replaying a DATA marker against a FULL request would have
// atelet commit a manifest claiming a full snapshot whose file set is only the
// durable-dir tar, leaving nothing to resume the guest from.
func TestReadRejectsMarkerFromADifferentScope(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{"different scope", `{"snapshotFiles":["durable-dir.tar"],"scope":"SNAPSHOT_SCOPE_DATA"}`},
		// Written by an ateom from before the scope was recorded: unmatchable,
		// so not replayable either.
		{"no scope", `{"snapshotFiles":["checkpoint.img"]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const actorUID = "actor-1"
			useTempActorsDir(t, actorUID)

			path := filepath.Join(ateompath.CheckpointStateDir(actorUID), ateompath.CheckpointDoneFileName)
			if err := os.WriteFile(path, []byte(tt.content), 0o600); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}

			rec, ok, err := Read(actorUID, testScope)
			if err != nil {
				t.Fatalf("Read: %v, want no error", err)
			}
			if ok || rec != nil {
				t.Errorf("Read = (%v, %v), want (nil, false)", rec, ok)
			}
			// Unlike a damaged marker, a mismatched one is a valid record of
			// the checkpoint that wrote it, so it is not Read's to delete.
			// (Its caller will usually clear the checkpoint dir moments
			// later; that is the caller's decision, not this one's.)
			if _, err := os.Stat(path); err != nil {
				t.Errorf("marker removed (err=%v), want it left in place", err)
			}
		})
	}
}
