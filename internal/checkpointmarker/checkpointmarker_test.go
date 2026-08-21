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
	tests := []struct {
		name  string
		files []string
	}{
		{"with files", []string{"checkpoint.img", "pages.img", "pages_meta.img"}},
		{"empty file set", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const actorUID = "actor-1"
			useTempActorsDir(t, actorUID)

			if err := Write(actorUID, testScope, tt.files); err != nil {
				t.Fatalf("Write: %v", err)
			}

			rec, ok, err := Read(actorUID, testScope)
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
			if !ok {
				t.Fatal("Read reported no marker after Write")
			}
			if !slices.Equal(rec.SnapshotFiles, tt.files) {
				t.Errorf("SnapshotFiles = %v, want %v", rec.SnapshotFiles, tt.files)
			}
		})
	}
}

func TestWriteLeavesNoTempFiles(t *testing.T) {
	const actorUID = "actor-1"
	useTempActorsDir(t, actorUID)

	if err := Write(actorUID, testScope, []string{"checkpoint.img"}); err != nil {
		t.Fatalf("Write: %v", err)
	}

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

	rec, ok, err := Read(actorUID, testScope)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if ok || rec != nil {
		t.Errorf("Read = (%v, %v), want (nil, false)", rec, ok)
	}
}

// An unusable marker is discarded so the checkpoint can be re-attempted.
func TestReadDiscardsUnusableMarker(t *testing.T) {
	const actorUID = "actor-1"
	useTempActorsDir(t, actorUID)

	path := filepath.Join(ateompath.CheckpointStateDir(actorUID), ateompath.CheckpointDoneFileName)
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
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
}

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

// Read rejects markers with a different or missing scope.
func TestReadRejectsMarkerFromADifferentScope(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{"different scope", `{"snapshotFiles":["durable-dir.tar"],"scope":"SNAPSHOT_SCOPE_DATA"}`},
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
			if _, err := os.Stat(path); err != nil {
				t.Errorf("marker removed (err=%v), want it left in place", err)
			}
		})
	}
}
