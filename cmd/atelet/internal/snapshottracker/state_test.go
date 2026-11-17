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

package snapshottracker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"google.golang.org/protobuf/proto"
)

// writePreviousSnapshotState writes, where an earlier release kept it, a
// snapshot state file recording local snapshot uuid for the tracked actor, and
// returns its contents.
func writePreviousSnapshotState(t *testing.T, uuid string) []byte {
	t.Helper()
	ref := testRef(uuid, 1, time.Now())
	data, err := json.Marshal(actorSnapshotsRecord{LatestLocal: &ref})
	if err != nil {
		t.Fatal(err)
	}
	path := ateletpath.PreviousSnapshotStateFile(trackedActorUID)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return data
}

func assertNotExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("stat(%s) = %v, want not exist", path, err)
	}
}

// A state file an earlier release wrote is moved to where this release keeps
// it, so the actor keeps its snapshots after an upgrade. When both copies
// exist, as after a crash partway through an earlier move, the move is
// finished. The actor's directory stays even when the state file was all it
// held, as for an actor that had detached, since atelet treats an actor
// without one as terminated.
func TestMigrateSnapshotStateFiles(t *testing.T) {
	for _, tc := range []struct {
		name     string
		bothCopy bool
	}{
		{name: "moves the file"},
		{name: "finishes an interrupted move", bothCopy: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useTempNodeDirs(t)
			if err := os.MkdirAll(ateletpath.ActorPath(trackedActorUID), 0o700); err != nil {
				t.Fatal(err)
			}
			want := writePreviousSnapshotState(t, "s1")
			to := ateletpath.ActorSnapshotStateFile(trackedActorUID)
			if tc.bothCopy {
				if err := os.MkdirAll(filepath.Dir(to), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(to, want, 0o600); err != nil {
					t.Fatal(err)
				}
			}

			if err := MigrateSnapshotStateFiles(); err != nil {
				t.Fatalf("MigrateSnapshotStateFiles() = %v", err)
			}
			if got, err := os.ReadFile(to); err != nil || string(got) != string(want) {
				t.Errorf("%s = (%q, %v), want (%q, nil)", to, got, err, want)
			}
			assertNotExist(t, ateletpath.PreviousSnapshotStateFile(trackedActorUID))
			if _, err := os.Stat(ateletpath.ActorPath(trackedActorUID)); err != nil {
				t.Errorf("actor dir stat = %v, want kept", err)
			}
			if got := uuidOf(newTracker(t).State(trackedActorUID).GetLatestLocal()); got != "s1" {
				t.Errorf("latest_local = %q, want s1", got)
			}
		})
	}
}

// A save that finds the actor's directory gone also removes a state file left
// in its snapshot directory, as after a crash between the two disks during a
// cleanup, so the actor's state does not come back.
func TestSaveWithoutActorDirRemovesLeftoverStateFile(t *testing.T) {
	useTempNodeDirs(t)
	stateFile := ateletpath.ActorSnapshotStateFile(trackedActorUID)
	if err := os.MkdirAll(filepath.Dir(stateFile), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stateFile, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	newTracker(t).CommitLocal(trackedActorUID, testRef("s1", 1, time.Now()))
	assertNotExist(t, ateletpath.ActorSnapshotPath(trackedActorUID))
	assertNotExist(t, ateletpath.ActorPath(trackedActorUID))
	if got := newTracker(t).State(trackedActorUID); !proto.Equal(got, &ateletpb.ActorSnapshotState{}) {
		t.Errorf("State() = %v, want empty", got)
	}
}

// A move that fails keeps the original file, so the actor's state is not lost,
// and returns the error, so atelet does not start without it.
func TestMigrateSnapshotStateFilesKeepsFileWhenMoveFails(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions, so no write can be made to fail")
	}
	useTempNodeDirs(t)
	if err := os.MkdirAll(ateletpath.ActorPath(trackedActorUID), 0o700); err != nil {
		t.Fatal(err)
	}
	want := writePreviousSnapshotState(t, "s1")
	// A read-only destination directory makes the write fail.
	toDir := filepath.Dir(ateletpath.ActorSnapshotStateFile(trackedActorUID))
	if err := os.MkdirAll(toDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(toDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(toDir, 0o700) })

	if err := MigrateSnapshotStateFiles(); err == nil {
		t.Fatal("MigrateSnapshotStateFiles() = nil, want an error")
	}
	from := ateletpath.PreviousSnapshotStateFile(trackedActorUID)
	if got, err := os.ReadFile(from); err != nil || string(got) != string(want) {
		t.Errorf("%s = (%q, %v), want (%q, nil)", from, got, err, want)
	}
}

// assertSnapshotDirHoldsOnlyState fails unless the actor's snapshot directory
// holds only its snapshot state file.
func assertSnapshotDirHoldsOnlyState(t *testing.T, actorUID string) {
	t.Helper()
	entries, err := os.ReadDir(ateletpath.ActorSnapshotPath(actorUID))
	if err != nil {
		t.Fatalf("reading %s snapshot dir: %v", actorUID, err)
	}
	want := filepath.Base(ateletpath.ActorSnapshotStateFile(actorUID))
	if len(entries) != 1 || entries[0].Name() != want {
		var got []string
		for _, e := range entries {
			got = append(got, e.Name())
		}
		t.Errorf("%s snapshot dir entries = %v, want [%s]", actorUID, got, want)
	}
}

// A node with nothing to move needs no change.
func TestMigrateSnapshotStateFilesWithNothingToMove(t *testing.T) {
	useTempNodeDirs(t)
	if err := MigrateSnapshotStateFiles(); err != nil {
		t.Fatalf("MigrateSnapshotStateFiles() = %v, want nil on a node with no directories", err)
	}
	if err := os.MkdirAll(ateletpath.LocalCheckpointsDir(trackedActorUID), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(ateletpath.ActorPath(trackedActorUID), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := MigrateSnapshotStateFiles(); err != nil {
		t.Fatalf("MigrateSnapshotStateFiles() = %v, want nil with no state file to move", err)
	}
	assertNotExist(t, ateletpath.ActorSnapshotStateFile(trackedActorUID))
}
