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

package snapshottracker

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	clocktesting "k8s.io/utils/clock/testing"
)

const trackedActorUID = "actor-uid-1"

// useTempNodeDirs roots atelet's on-node state in temp directories so a test
// can drive the real filesystem layout. Not parallel-safe: the paths are
// process-global.
func useTempNodeDirs(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	origActors, origStatic := nodepath.ActorsDir, nodepath.StaticFilesDir
	nodepath.ActorsDir = filepath.Join(root, "actors")
	nodepath.StaticFilesDir = filepath.Join(root, "static-files")
	t.Cleanup(func() {
		nodepath.ActorsDir, nodepath.StaticFilesDir = origActors, origStatic
	})
}

// newTracker returns a tracker on a fake clock whose context ends with the
// test. Its workers are not started.
func newTracker(t *testing.T) *Tracker {
	return New(t.Context(), clocktesting.NewFakeClock(time.Unix(1_700_000_000, 0)))
}

// testRef is a MEMORY snapshot of the tracked actor.
func testRef(uuid string, generation int32, commitTime time.Time) SnapshotRef {
	return SnapshotRef{
		Snapshot: &ateletpb.Snapshot{
			SnapshotUuid: uuid,
			Generation:   generation,
			Fidelity:     ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			Object:       &ateletpb.ObjectStorage{SnapshotUri: "gs://test-bucket/atespaces/ate-demo/actors/" + trackedActorUID + "/snapshots/" + uuid},
		},
		CommitTime: commitTime,
	}
}

func timeOf(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

func uuidOf(r *ateletpb.SnapshotReport) string {
	return r.GetSnapshot().GetSnapshotUuid()
}

// State reports the latest local snapshot a deferred checkpoint leaves, and
// the latest durable one an immediate checkpoint leaves without a local copy.
// Each update is written to the actor's snapshot state file and evicted from
// memory; Forget deletes the file and drops any in-memory state.
func TestTrackerState(t *testing.T) {
	useTempNodeDirs(t)
	if err := os.MkdirAll(ateletpath.ActorPath(trackedActorUID), 0o700); err != nil {
		t.Fatal(err)
	}
	tr := newTracker(t)
	t0 := time.Now()
	st := tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
	if got := st.GetLatestLocal(); uuidOf(got) != "s1" || !timeOf(got.GetLocalCommitTime()).Equal(t0) {
		t.Errorf("latest_local = %v, want s1 committed at %v", got, t0)
	}
	if st.GetLatestDurable() != nil {
		t.Errorf("latest_durable = %v, want unset", st.GetLatestDurable())
	}
	if len(tr.actors) != 0 {
		t.Errorf("in-memory actors after CommitLocal = %d, want 0 after writing to file", len(tr.actors))
	}

	s2 := testRef("s2", 2, t0.Add(time.Minute))
	s2.UploadTime = s2.CommitTime.Add(time.Second)
	st = tr.CommitDurable(trackedActorUID, s2)
	if st.GetLatestLocal() != nil {
		t.Errorf("latest_local = %v after an immediate checkpoint, want unset", st.GetLatestLocal())
	}
	if got := st.GetLatestDurable(); uuidOf(got) != "s2" || !timeOf(got.GetDurableCompleteTime()).Equal(s2.UploadTime) {
		t.Errorf("latest_durable = %v, want s2 uploaded at %v", got, s2.UploadTime)
	}
	if len(tr.actors) != 0 {
		t.Errorf("in-memory actors after CommitDurable = %d, want 0 after writing to file", len(tr.actors))
	}

	st = tr.CommitLocal(trackedActorUID, testRef("s3", 3, t0.Add(2*time.Minute)))
	if uuidOf(st.GetLatestLocal()) != "s3" || uuidOf(st.GetLatestDurable()) != "s2" {
		t.Errorf("state = %v, want s3 local and s2 still durable", st)
	}
	if got := tr.State(trackedActorUID); !proto.Equal(got, st) {
		t.Errorf("State() = %v, want %v, as CommitLocal returned", got, st)
	}
	if got := newTracker(t).State(trackedActorUID); !proto.Equal(got, st) {
		t.Errorf("State() on a fresh tracker = %v, want %v from the state file", got, st)
	}

	if err := tr.Forget(trackedActorUID); err != nil {
		t.Fatalf("forget: %v", err)
	}
	if _, err := os.Stat(ateletpath.ActorSnapshotStateFile(trackedActorUID)); !os.IsNotExist(err) {
		t.Errorf("state file after forget: stat = %v, want it deleted", err)
	}
	if got := tr.State(trackedActorUID); !proto.Equal(got, &ateletpb.ActorSnapshotState{}) {
		t.Errorf("state after forget = %v, want empty", got)
	}
}

// ForgetLocal drops the local snapshot and keeps the durable one. With no
// local snapshot to drop, it writes nothing.
func TestTrackerForgetLocal(t *testing.T) {
	useTempNodeDirs(t)
	if err := os.MkdirAll(ateletpath.ActorPath(trackedActorUID), 0o700); err != nil {
		t.Fatal(err)
	}
	tr := newTracker(t)
	tr.ForgetLocal(trackedActorUID)
	if _, err := os.Stat(ateletpath.ActorSnapshotStateFile(trackedActorUID)); !os.IsNotExist(err) {
		t.Errorf("state file after forgetting nothing: stat = %v, want none written", err)
	}

	t0 := time.Now()
	tr.CommitDurable(trackedActorUID, testRef("s1", 1, t0))
	tr.CommitLocal(trackedActorUID, testRef("s2", 2, t0.Add(time.Minute)))
	tr.ForgetLocal(trackedActorUID)
	if st := newTracker(t).State(trackedActorUID); st.GetLatestLocal() != nil || uuidOf(st.GetLatestDurable()) != "s1" {
		t.Errorf("state from the file = %v, want no local snapshot and s1 still durable", st)
	}
	if len(tr.actors) != 0 {
		t.Errorf("in-memory actors = %d, want 0 after writing to file", len(tr.actors))
	}
}

// CommitPromoted makes the promoted local snapshot the latest durable one,
// keeping the commit time recorded for its local copy, and drops the local
// one. A retried promote of the same snapshot keeps what was recorded.
func TestTrackerCommitPromoted(t *testing.T) {
	useTempNodeDirs(t)
	if err := os.MkdirAll(ateletpath.ActorPath(trackedActorUID), 0o700); err != nil {
		t.Fatal(err)
	}
	tr := newTracker(t)
	t0 := time.Now()
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))

	promoted := testRef("s1", 1, time.Time{})
	promoted.UploadTime = t0.Add(time.Minute)
	st := tr.CommitPromoted(trackedActorUID, promoted)
	if st.GetLatestLocal() != nil {
		t.Errorf("latest_local = %v after the promote, want unset", st.GetLatestLocal())
	}
	if got := st.GetLatestDurable(); uuidOf(got) != "s1" || !timeOf(got.GetLocalCommitTime()).Equal(t0) || !timeOf(got.GetDurableCompleteTime()).Equal(promoted.UploadTime) {
		t.Errorf("latest_durable = %v, want s1 committed at %v and uploaded at %v", got, t0, promoted.UploadTime)
	}
	if got := newTracker(t).State(trackedActorUID); !proto.Equal(got, st) {
		t.Errorf("State() on a fresh tracker = %v, want %v from the state file", got, st)
	}

	retry := testRef("s1", 1, time.Time{})
	retry.UploadTime = t0.Add(time.Hour)
	if got := tr.CommitPromoted(trackedActorUID, retry); !proto.Equal(got, st) {
		t.Errorf("state after a retried promote = %v, want %v", got, st)
	}

	// Promoting a snapshot other than the local one carries no commit time.
	tr.CommitLocal(trackedActorUID, testRef("s2", 2, t0.Add(2*time.Hour)))
	other := testRef("s3", 3, time.Time{})
	other.UploadTime = t0.Add(3 * time.Hour)
	st = tr.CommitPromoted(trackedActorUID, other)
	if got := st.GetLatestDurable(); uuidOf(got) != "s3" || got.GetLocalCommitTime() != nil || st.GetLatestLocal() != nil {
		t.Errorf("state = %v, want s3 durable without a commit time, and no local snapshot", st)
	}
}

// When a save fails, the entry stays in memory and is reported instead of the
// state file, until a later save succeeds.
func TestTrackerKeepsUnsavedStateInMemory(t *testing.T) {
	useTempNodeDirs(t)
	stateFile := ateletpath.ActorSnapshotStateFile(trackedActorUID)
	// A directory where the state file goes makes the write's rename fail.
	if err := os.MkdirAll(stateFile, 0o700); err != nil {
		t.Fatal(err)
	}
	tr := newTracker(t)
	t0 := time.Now()
	st := tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
	if len(tr.actors) != 1 {
		t.Fatalf("in-memory actors = %d after a failed save, want 1", len(tr.actors))
	}
	if got := tr.State(trackedActorUID); !proto.Equal(got, st) {
		t.Errorf("State() = %v, want %v from memory", got, st)
	}

	if err := os.Remove(stateFile); err != nil {
		t.Fatal(err)
	}
	st = tr.CommitLocal(trackedActorUID, testRef("s2", 2, t0.Add(time.Minute)))
	if len(tr.actors) != 0 {
		t.Errorf("in-memory actors = %d after a successful save, want 0", len(tr.actors))
	}
	if got := newTracker(t).State(trackedActorUID); !proto.Equal(got, st) {
		t.Errorf("State() on a fresh tracker = %v, want %v from the state file", got, st)
	}
}

// A save that finds the actor's directory gone, as after the actor was
// terminated on this node, writes nothing and drops the entry.
func TestTrackerDropsActorWithoutDirectory(t *testing.T) {
	useTempNodeDirs(t)
	tr := newTracker(t)
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, time.Now()))
	if len(tr.actors) != 0 {
		t.Errorf("in-memory actors = %d, want 0 for an actor whose directory is gone", len(tr.actors))
	}
	if _, err := os.Stat(ateletpath.ActorSnapshotStateFile(trackedActorUID)); !os.IsNotExist(err) {
		t.Errorf("state file: stat = %v, want none written", err)
	}
	if got := tr.State(trackedActorUID); !proto.Equal(got, &ateletpb.ActorSnapshotState{}) {
		t.Errorf("State() = %v, want empty", got)
	}
}

// Commits for one actor take turns, so a racing deferred checkpoint never
// loses an immediate checkpoint's durable snapshot. The control plane never
// sends one actor's checkpoints concurrently: the race only exercises the
// lock, so either commit may land last, even leaving a local snapshot older
// than the durable one.
func TestTrackerSerializesEachActor(t *testing.T) {
	useTempNodeDirs(t)
	if err := os.MkdirAll(ateletpath.ActorPath(trackedActorUID), 0o700); err != nil {
		t.Fatal(err)
	}
	tr := newTracker(t)
	t0 := time.Now()
	for i := range 20 {
		local := testRef(fmt.Sprintf("local-%d", i), int32(2*i+1), t0)
		durable := testRef(fmt.Sprintf("durable-%d", i), int32(2*i+2), t0)
		var wg sync.WaitGroup
		wg.Go(func() { tr.CommitLocal(trackedActorUID, local) })
		wg.Go(func() { tr.CommitDurable(trackedActorUID, durable) })
		wg.Wait()
		if got, want := uuidOf(tr.State(trackedActorUID).GetLatestDurable()), durable.Snapshot.GetSnapshotUuid(); got != want {
			t.Fatalf("round %d: latest_durable = %q, want %q", i, got, want)
		}
	}
	if len(tr.actors) != 0 {
		t.Errorf("in-memory actors = %d, want 0 once every commit is written", len(tr.actors))
	}
}

// A caller holding one actor's lock does not hold up another actor's commit.
func TestTrackerLocksEachActorSeparately(t *testing.T) {
	useTempNodeDirs(t)
	tr := newTracker(t)
	tr.lock("other-actor-uid")
	defer tr.unlock("other-actor-uid")

	done := make(chan struct{})
	go func() {
		defer close(done)
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, time.Now()))
	}()
	select {
	case <-done:
	case <-time.After(time.Minute):
		t.Fatal("CommitLocal waited for another actor's lock")
	}
}

// An unreadable state file on disk is skipped and treated as empty state.
func TestTrackerSkipsUnreadableSnapshotState(t *testing.T) {
	useTempNodeDirs(t)
	if err := os.MkdirAll(ateletpath.ActorPath(trackedActorUID), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ateletpath.ActorSnapshotStateFile(trackedActorUID), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	tr := newTracker(t)
	if got := tr.State(trackedActorUID); !proto.Equal(got, &ateletpb.ActorSnapshotState{}) {
		t.Errorf("state with a broken file = %v, want empty", got)
	}
}
