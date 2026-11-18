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

// writeNextSnapshotState writes, where a future release keeps it, a snapshot
// state file recording local snapshot uuid for the tracked actor, and returns
// its contents.
func writeNextSnapshotState(t *testing.T, uuid string) []byte {
	t.Helper()
	ref := testRef(uuid, 1, time.Now())
	data, err := json.Marshal(actorSnapshotsRecord{LatestLocal: &ref})
	if err != nil {
		t.Fatal(err)
	}
	path := ateletpath.NextSnapshotStateFile(trackedActorUID)
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

// A state file a future release wrote is moved to where this release keeps it,
// so the actor keeps its snapshots after a rollback. When both copies exist, as
// after a crash partway through an earlier move, the move is finished. The
// snapshot directory goes if the move left it empty, as for an actor that had
// detached, and stays if it holds anything else.
func TestMigrateSnapshotStateFiles(t *testing.T) {
	for _, tc := range []struct {
		name     string
		bothCopy bool
		// otherFile is a file left in the snapshot directory, if not empty.
		otherFile string
	}{
		{name: "moves the file"},
		{name: "finishes an interrupted move", bothCopy: true},
		{name: "keeps a snapshot directory that holds other files", otherFile: "restore-state"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useTempNodeDirs(t)
			if err := os.MkdirAll(ateletpath.ActorPath(trackedActorUID), 0o700); err != nil {
				t.Fatal(err)
			}
			want := writeNextSnapshotState(t, "s1")
			if tc.otherFile != "" {
				if err := os.MkdirAll(filepath.Join(ateletpath.ActorSnapshotPath(trackedActorUID), tc.otherFile), 0o700); err != nil {
					t.Fatal(err)
				}
			}
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
			assertNotExist(t, ateletpath.NextSnapshotStateFile(trackedActorUID))
			if tc.otherFile == "" {
				assertNotExist(t, ateletpath.ActorSnapshotPath(trackedActorUID))
			} else if _, err := os.Stat(filepath.Join(ateletpath.ActorSnapshotPath(trackedActorUID), tc.otherFile)); err != nil {
				t.Errorf("snapshot directory entry %s: %v, want kept", tc.otherFile, err)
			}
			if got := uuidOf(newTracker(t).State(trackedActorUID).GetLatestLocal()); got != "s1" {
				t.Errorf("latest_local = %q, want s1", got)
			}
		})
	}
}

// A state file whose actor directory is gone, as after a terminate that a
// crash interrupted, is removed rather than moved, so the actor's state does
// not come back.
func TestMigrateSnapshotStateFilesRemovesFileOfGoneActor(t *testing.T) {
	useTempNodeDirs(t)
	writeNextSnapshotState(t, "s1")

	if err := MigrateSnapshotStateFiles(); err != nil {
		t.Fatalf("MigrateSnapshotStateFiles() = %v", err)
	}
	assertNotExist(t, ateletpath.NextSnapshotStateFile(trackedActorUID))
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
	want := writeNextSnapshotState(t, "s1")
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
	from := ateletpath.NextSnapshotStateFile(trackedActorUID)
	if got, err := os.ReadFile(from); err != nil || string(got) != string(want) {
		t.Errorf("%s = (%q, %v), want (%q, nil)", from, got, err, want)
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
