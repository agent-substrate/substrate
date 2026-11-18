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
	"context"
	"errors"
	"os"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/internal/proto/ateletpb"
)

// localRef is a pause snapshot of the tracked actor with no object storage
// location, as a deferred checkpoint commits today.
func localRef(uuid string, generation int32, commitTime time.Time) SnapshotRef {
	return SnapshotRef{
		Snapshot: &ateletpb.Snapshot{
			SnapshotUuid: uuid,
			Generation:   generation,
			Fidelity:     ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		},
		CommitTime: commitTime,
	}
}

// commitLocalSnapshot writes the local snapshot directory of name and commits
// it as generation gen, as a pause does.
func commitLocalSnapshot(t *testing.T, tr *Tracker, name string, gen int32) {
	t.Helper()
	commitSnapshotDir(t, tr, localRef(name, gen, tr.clock.Now()))
}

// commitSnapshotDir writes ref's local snapshot directory and commits it.
func commitSnapshotDir(t *testing.T, tr *Tracker, ref SnapshotRef) {
	t.Helper()
	if err := os.MkdirAll(ateletpath.LocalSnapshotDir(trackedActorUID, ref.uuid()), 0o700); err != nil {
		t.Fatal(err)
	}
	tr.CommitLocal(trackedActorUID, ref)
}

// commitLocalSnapshots commits names as consecutive pauses, generations 1, 2,
// and so on.
func commitLocalSnapshots(t *testing.T, tr *Tracker, names ...string) {
	t.Helper()
	for i, name := range names {
		commitLocalSnapshot(t, tr, name, int32(i+1))
	}
}

func localSnapshotExists(name string) bool {
	_, err := os.Stat(ateletpath.LocalSnapshotDir(trackedActorUID, name))
	return err == nil
}

// pruneDue reports whether the tracked actor has a snapshot to prune now.
func pruneDue(tr *Tracker) bool {
	tr.lock(trackedActorUID)
	defer tr.unlock(trackedActorUID)
	a := tr.findLocked(trackedActorUID)
	return a != nil && a.pruneDue()
}

// pruneList returns the snapshots the tracked actor's state file lists to
// prune.
func pruneList() []string {
	if a := readSnapshotState(trackedActorUID); a != nil {
		return a.Prune
	}
	return nil
}

// blockRemovals makes tr's prunes block on each snapshot until release is
// closed, as a disk far behind on writeback would; started receives once per
// blocked snapshot.
func blockRemovals(tr *Tracker) (started <-chan struct{}, release chan struct{}) {
	rel := make(chan struct{})
	st := make(chan struct{}, 8)
	tr.removeLocalSnapshot = func(ctx context.Context, actorUID, name string) error {
		select {
		case st <- struct{}{}:
		default:
		}
		<-rel
		return removeLocalCheckpoint(ctx, actorUID, name)
	}
	return st, rel
}

// failRemovals makes tr's prunes fail on each snapshot, as a disk with an I/O
// error would, and returns the count of snapshots they refused.
func failRemovals(tr *Tracker) *atomic.Int32 {
	var calls atomic.Int32
	tr.removeLocalSnapshot = func(context.Context, string, string) error {
		calls.Add(1)
		return errors.New("input/output error")
	}
	return &calls
}

// restoreRemovals makes tr's prunes delete snapshots again.
func restoreRemovals(tr *Tracker) { tr.removeLocalSnapshot = removeLocalCheckpoint }

// processNextPrune takes the next actor off the prune queue and runs its prune
// in the test's goroutine, as a prune worker would, so a test on an idle
// tracker drives prunes one at a time.
func processNextPrune(t *testing.T, tr *Tracker) {
	t.Helper()
	next := make(chan string, 1)
	go func() {
		if actorUID, shutdown := tr.pruning.queue.Get(); !shutdown {
			next <- actorUID
		}
	}()
	select {
	case actorUID := <-next:
		tr.prune(actorUID)
		tr.pruning.queue.Done(actorUID)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for a prune to be queued")
	}
}

// A second pause queues a prune of the first snapshot, which a worker runs
// in the background: the superseded snapshot goes, the latest stays, and the
// actor's entry is dropped once nothing is pending.
func TestPruneRunsInBackgroundAfterCommit(t *testing.T) {
	tr := newTestTracker(t, nil)
	commitLocalSnapshot(t, tr, "s1", 1)
	if pruneDue(tr) {
		t.Fatal("a first pause left a prune due, want none: nothing is superseded")
	}
	commitLocalSnapshot(t, tr, "s2", 2)
	eventually(t, "the first snapshot is pruned", func() bool { return !localSnapshotExists("s1") })
	if !localSnapshotExists("s2") {
		t.Error("the latest snapshot was pruned, want it kept")
	}
	eventually(t, "the prune is no longer due", func() bool { return !pruneDue(tr) && !inMemory(tr) })
}

// A second pause lists the first snapshot, and the prune a worker takes off
// the queue deletes it and nothing else.
func TestPruneDeletesTheListedSnapshot(t *testing.T) {
	tr := newIdleTestTracker(t, nil) // workers not started
	commitLocalSnapshots(t, tr, "s1", "s2")
	if got := pruneList(); !slices.Equal(got, []string{"s1"}) {
		t.Fatalf("state file lists %v to prune after the second pause, want [s1]", got)
	}
	processNextPrune(t, tr)
	if localSnapshotExists("s1") || !localSnapshotExists("s2") {
		t.Errorf("after the prune: s1 exists = %v, s2 exists = %v; want only s2", localSnapshotExists("s1"), localSnapshotExists("s2"))
	}
	if pruneDue(tr) || pruneList() != nil {
		t.Errorf("after the prune: due = %v, listed = %v; want neither", pruneDue(tr), pruneList())
	}
}

// A checkpoint that begins while the previous pause's prune is still queued
// runs that prune before it holds the actor, so superseded snapshots do not
// pile up however far behind the prune workers are.
func TestBeginCheckpointRunsAQueuedPruneFirst(t *testing.T) {
	tr := newIdleTestTracker(t, nil) // workers not started: the prune stays queued
	commitLocalSnapshots(t, tr, "s1", "s2")
	_, end, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, localRef("s3", 3, tr.clock.Now()).Snapshot, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
	if err != nil {
		t.Fatalf("BeginCheckpoint: %v", err)
	}
	defer end()
	if localSnapshotExists("s1") {
		t.Error("BeginCheckpoint returned with the previous pause's superseded snapshot still on disk")
	}
	if !localSnapshotExists("s2") {
		t.Error("BeginCheckpoint pruned the latest snapshot")
	}
	if pruneDue(tr) {
		t.Error("prune still due after BeginCheckpoint ran it")
	}
}

// A context that ends while waiting for a running prune returns its error,
// and the prune itself goes on to completion.
func TestBeginCheckpointHonorsContextWhileAPruneRuns(t *testing.T) {
	tr := newIdleTestTracker(t, nil)
	started, release := blockRemovals(tr)
	tr.Start()
	commitLocalSnapshots(t, tr, "s1", "s2")
	<-started

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if _, _, err := tr.BeginCheckpoint(ctx, trackedActorUID, localRef("s3", 3, tr.clock.Now()).Snapshot, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err == nil || ctx.Err() == nil {
		t.Fatalf("BeginCheckpoint = %v, want the context's error", err)
	}
	close(release)
	eventually(t, "the prune finishes", func() bool { return !localSnapshotExists("s1") && !pruneDue(tr) })
}

// A prune saved as due survives an atelet restart: a new tracker finds it in
// the state file and a worker runs it.
func TestPruneListSurvivesRestart(t *testing.T) {
	before := newIdleTestTracker(t, nil) // workers not started
	commitLocalSnapshots(t, before, "s1", "s2")
	if !pruneDue(before) || !localSnapshotExists("s1") {
		t.Fatal("setup: expected a due prune with s1 still on disk")
	}

	after := newStoreTracker(t, nil)
	after.Start()
	eventually(t, "the restarted tracker prunes s1", func() bool { return !localSnapshotExists("s1") })
	if !localSnapshotExists("s2") {
		t.Error("the restarted tracker pruned the latest snapshot")
	}
}

// A commit that arrives after a newer generation already committed (a stalled
// checkpoint) is superseded on arrival: its copy is the one a prune deletes.
func TestStaleCommitQueuesAPruneOfItsOwnCopy(t *testing.T) {
	tr := newIdleTestTracker(t, nil)
	commitLocalSnapshot(t, tr, "s2", 2)
	commitLocalSnapshot(t, tr, "s1", 1) // stale: generation 1 after 2
	processNextPrune(t, tr)
	if localSnapshotExists("s1") || !localSnapshotExists("s2") {
		t.Errorf("after the prune: s1 exists = %v, s2 exists = %v; want only s2", localSnapshotExists("s1"), localSnapshotExists("s2"))
	}
}

// A prune does not wait for an upload of the actor's older snapshot, which it
// keeps: an actor that pauses and resumes during a slow upload still has its
// snapshots in between deleted.
func TestPruneRunsWhileAnUploadIsInFlight(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	commitSnapshotDir(t, tr, testRef("s1", 1, tr.clock.Now()))
	stepTo(tr, tr.clock.Now().Add(testUploadDelay))
	if uuid := <-store.started; uuid != "s1" {
		t.Fatalf("upload started for %q, want s1", uuid)
	}
	defer func() { store.release <- nil }()

	// The second pause lists s1, which the running upload keeps, so no prune
	// is due yet; the third pause lists s2, which nothing keeps.
	commitLocalSnapshot(t, tr, "s2", 2)
	if pruneDue(tr) {
		t.Fatal("a prune is due while the upload keeps s1")
	}
	commitLocalSnapshot(t, tr, "s3", 3)
	eventually(t, "the third pause's prune ends during the upload", func() bool { return !pruneDue(tr) })
	if localSnapshotExists("s2") || !localSnapshotExists("s1") || !localSnapshotExists("s3") {
		t.Errorf("after the prune: s1 exists = %v, s2 exists = %v, s3 exists = %v; want s2 pruned and the others kept", localSnapshotExists("s1"), localSnapshotExists("s2"), localSnapshotExists("s3"))
	}
}

// A checkpoint whose inline prune fails makes one attempt and goes on to hold
// the actor. The snapshot stays listed, a worker does not retry it at once,
// and the next change to the actor, here the checkpoint's end, tries again.
func TestBeginCheckpointMakesOnePruneAttemptWhenItFails(t *testing.T) {
	tr := newIdleTestTracker(t, nil) // workers not started
	calls := failRemovals(tr)
	commitLocalSnapshots(t, tr, "s1", "s2")
	_, end, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, localRef("s3", 3, tr.clock.Now()).Snapshot, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
	if err != nil {
		t.Fatalf("BeginCheckpoint: %v", err)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("BeginCheckpoint tried the failing prune %d times, want 1", n)
	}
	if !localSnapshotExists("s1") || !pruneDue(tr) {
		t.Fatal("a failed prune left s1 deleted or no prune due")
	}
	end()

	// The second pause queued the actor before the failure. The worker's
	// attempt fails too, and the actor is not queued again by the failure.
	processNextPrune(t, tr)
	if n := calls.Load(); n != 2 {
		t.Errorf("the worker tried the failing prune %d times in total, want 2", n)
	}
	if got := tr.pruning.queue.Len(); got != 0 {
		t.Errorf("prune queue holds %d actors after a failed prune, want 0: a failure must not queue a retry at once", got)
	}

	// The next change to the actor queues it again, and the prune succeeds.
	restoreRemovals(tr)
	commitLocalSnapshot(t, tr, "s3", 3)
	processNextPrune(t, tr)
	if localSnapshotExists("s1") || localSnapshotExists("s2") || !localSnapshotExists("s3") {
		t.Errorf("after the retry: s1 exists = %v, s2 exists = %v, s3 exists = %v; want only s3", localSnapshotExists("s1"), localSnapshotExists("s2"), localSnapshotExists("s3"))
	}
	if pruneDue(tr) || inMemory(tr) {
		t.Errorf("after the retry: prune due = %v, in memory = %v; want neither", pruneDue(tr), inMemory(tr))
	}
}

// Forget cancels a prune in progress and waits for it to end, as it does for
// an upload or a detach, and the prune leaves no entry behind.
func TestForgetWaitsForARunningPrune(t *testing.T) {
	tr := newIdleTestTracker(t, nil)
	started, release := blockRemovals(tr)
	tr.Start()
	commitLocalSnapshots(t, tr, "s1", "s2")
	<-started // a worker is deleting s1, held open

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := tr.Forget(trackedActorUID); err != nil {
			t.Errorf("Forget: %v", err)
		}
	}()
	select {
	case <-done:
		t.Fatal("Forget returned while a prune of the actor was still running")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	waitClosed(t, done, "Forget once the prune ended")
	if inMemory(tr) {
		t.Error("the actor's entry is still in memory after Forget")
	}
	if readSnapshotState(trackedActorUID) != nil {
		t.Error("the actor's state file survived Forget")
	}
}

// A prune does not run while a Run, Restore, Checkpoint, or PromoteSnapshot
// holds the actor: the hold's end queues it.
func TestPruneDefersWhileTheActorIsHeld(t *testing.T) {
	tr := newIdleTestTracker(t, nil) // workers not started
	calls := failRemovals(tr)
	commitLocalSnapshots(t, tr, "s1", "s2")
	_, end, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, localRef("s3", 3, tr.clock.Now()).Snapshot, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
	if err != nil {
		t.Fatalf("BeginCheckpoint: %v", err)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("BeginCheckpoint tried the failing prune %d times, want 1", n)
	}
	restoreRemovals(tr)
	processNextPrune(t, tr) // queued by the second pause; the hold defers it
	if !localSnapshotExists("s1") {
		t.Fatal("a prune ran while a Checkpoint held the actor")
	}
	end()
	processNextPrune(t, tr)
	if localSnapshotExists("s1") {
		t.Error("the prune did not run once the Checkpoint ended")
	}
}

// A state file that lags memory, because a save failed, never makes a prune
// delete a snapshot it does not list: after a restart, the prune deletes only
// the snapshots the file names, and leaves a newer snapshot the file lacks.
func TestPruneAfterRestartDeletesOnlyListedSnapshots(t *testing.T) {
	before := newIdleTestTracker(t, nil) // workers not started
	commitLocalSnapshots(t, before, "s1", "s2")
	stateFile := ateletpath.ActorSnapshotStateFile(trackedActorUID)
	stale, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatal(err)
	}
	commitLocalSnapshot(t, before, "s3", 3)
	// As if the save of s3's commit had failed: the file still names s2 as
	// the latest, with s1 to prune, while s3 is on disk and reported.
	if err := os.WriteFile(stateFile, stale, 0o600); err != nil {
		t.Fatal(err)
	}

	after := newStoreTracker(t, nil)
	after.Start()
	eventually(t, "the restarted tracker prunes s1", func() bool { return !localSnapshotExists("s1") })
	if !localSnapshotExists("s2") || !localSnapshotExists("s3") {
		t.Errorf("after the prune: s2 exists = %v, s3 exists = %v; want both kept", localSnapshotExists("s2"), localSnapshotExists("s3"))
	}
}

// BeginStart tells a Restore whether it may read the requested snapshot's
// local copy in place: not while the snapshot is listed to prune, whether the
// prune is queued, running, or waiting for an upload of the snapshot to end.
// It waits for no prune. A Run, or a Restore of LatestLocal, may use the copy.
func TestBeginStartReportsWhetherTheLocalCopyIsUsable(t *testing.T) {
	t.Run("listed and queued", func(t *testing.T) {
		tr := newIdleTestTracker(t, nil) // workers not started: the prune stays queued
		commitLocalSnapshots(t, tr, "s1", "s2")
		for uuid, want := range map[string]bool{"s1": false, "s2": true, "": true} {
			useLocal, end, err := tr.BeginStart(t.Context(), trackedActorUID, uuid)
			if err != nil {
				t.Fatalf("BeginStart(%q): %v", uuid, err)
			}
			end()
			if useLocal != want {
				t.Errorf("BeginStart(%q) useLocal = %v, want %v", uuid, useLocal, want)
			}
		}
	})

	t.Run("being pruned", func(t *testing.T) {
		tr := newIdleTestTracker(t, nil)
		started, release := blockRemovals(tr)
		defer close(release)
		tr.Start()
		commitLocalSnapshots(t, tr, "s1", "s2")
		<-started // a worker is deleting s1, held open
		returnsPromptly(t, "BeginStart during a prune of s1", func() {
			useLocal, end, err := tr.BeginStart(t.Context(), trackedActorUID, "s1")
			if err != nil {
				t.Errorf("BeginStart(s1): %v", err)
				return
			}
			end()
			if useLocal {
				t.Error("BeginStart(s1) useLocal = true while a prune of s1 runs, want false")
			}
		})
	})

	t.Run("listed while its upload runs", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		tr := newTestTracker(t, store)
		commitSnapshotDir(t, tr, testRef("s1", 1, tr.clock.Now()))
		stepTo(tr, tr.clock.Now().Add(testUploadDelay))
		<-store.started // s1 is uploading, held open
		defer func() { store.release <- nil }()
		commitLocalSnapshot(t, tr, "s2", 2) // lists s1, which the upload keeps
		if pruneDue(tr) {
			t.Fatal("a prune is due while the upload keeps s1")
		}
		useLocal, end, err := tr.BeginStart(t.Context(), trackedActorUID, "s1")
		if err != nil {
			t.Fatalf("BeginStart(s1): %v", err)
		}
		end()
		if useLocal {
			t.Error("BeginStart(s1) useLocal = true while s1 is listed and uploading, want false")
		}
	})
}

// A retry of a Checkpoint whose snapshot is committed already writes nothing,
// so it runs no prune first: the pause that committed the snapshot is over.
func TestBeginCheckpointOfACommittedSnapshotRunsNoPrune(t *testing.T) {
	tr := newIdleTestTracker(t, nil) // workers not started: the prune stays queued
	calls := failRemovals(tr)
	commitLocalSnapshots(t, tr, "s1", "s2")
	committed, end, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, localRef("s2", 2, tr.clock.Now()).Snapshot, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
	if err != nil || !committed {
		t.Fatalf("BeginCheckpoint(s2 again) = (%v, %v), want (true, nil)", committed, err)
	}
	end()
	if n := calls.Load(); n != 0 {
		t.Errorf("the retry tried a prune %d times, want 0", n)
	}
	if !pruneDue(tr) {
		t.Error("the retry cleared the prune due, want it left for a worker")
	}
}
