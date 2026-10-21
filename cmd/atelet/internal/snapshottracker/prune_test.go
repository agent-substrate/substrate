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

	"google.golang.org/grpc/codes"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/internal/apierror"
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
	end, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, localRef("s3", 3, tr.clock.Now()).Snapshot, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
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

// A Restore of the snapshot a prune is deleting, as after a revert to an older
// generation, waits for the prune, so it does not read a directory being
// emptied. A Run, or a Restore of the latest snapshot, which no prune deletes,
// does not wait.
func TestBeginStartWaitsForAPruneOfItsSnapshotOnly(t *testing.T) {
	tr := newIdleTestTracker(t, nil)
	started, release := blockRemovals(tr)
	tr.Start()
	commitLocalSnapshots(t, tr, "s1", "s2")
	<-started // a worker is deleting s1, held open

	for _, uuid := range []string{"", "s2"} {
		end, err := tr.BeginStart(t.Context(), trackedActorUID, uuid)
		if err != nil {
			t.Fatalf("BeginStart(%q) during a prune of s1: %v", uuid, err)
		}
		end()
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		end, err := tr.BeginStart(t.Context(), trackedActorUID, "s1")
		if err != nil {
			t.Errorf("BeginStart(s1): %v", err)
			return
		}
		end()
	}()
	select {
	case <-done:
		t.Fatal("BeginStart(s1) returned while a prune of s1 was still running")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	waitClosed(t, done, "BeginStart(s1) once the prune ended")
	if localSnapshotExists("s1") {
		t.Error("s1 survived the prune")
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
	if _, err := tr.BeginCheckpoint(ctx, trackedActorUID, localRef("s3", 3, tr.clock.Now()).Snapshot, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err == nil || ctx.Err() == nil {
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
// the actor; the prune is retried in the background once its backoff has
// passed, no sooner even if a worker finds the actor queued from before the
// failure, and the actor's entry is dropped once it succeeds.
func TestBeginCheckpointMakesOnePruneAttemptWhenItFails(t *testing.T) {
	tr := newIdleTestTracker(t, nil) // workers not started
	calls := failRemovals(tr)
	commitLocalSnapshots(t, tr, "s1", "s2")
	end, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, localRef("s3", 3, tr.clock.Now()).Snapshot, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
	if err != nil {
		t.Fatalf("BeginCheckpoint: %v", err)
	}
	end()
	if n := calls.Load(); n != 1 {
		t.Errorf("BeginCheckpoint tried the failing prune %d times, want 1", n)
	}
	if !localSnapshotExists("s1") || !pruneDue(tr) {
		t.Fatal("a failed prune left s1 deleted or no prune due")
	}

	restoreRemovals(tr)
	// The second pause queued the actor before the failure set its backoff:
	// a worker that takes it now waits the backoff out rather than retry.
	stepTo(tr, tr.clock.Now().Add(pruneRetryDelay-time.Second))
	processNextPrune(t, tr)
	if !localSnapshotExists("s1") {
		t.Fatal("the prune was retried before its backoff passed")
	}
	stepTo(tr, tr.clock.Now().Add(time.Second))
	processNextPrune(t, tr)
	if localSnapshotExists("s1") {
		t.Error("the prune was not retried once its backoff passed")
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

// A Run or Restore that finds a prune and an upload both in progress waits
// for the prune only: it goes on as soon as the prune ends, however long the
// upload runs.
func TestBeginStartWaitsForThePruneNotTheUpload(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	commitSnapshotDir(t, tr, testRef("s1", 1, tr.clock.Now()))
	stepTo(tr, tr.clock.Now().Add(testUploadDelay))
	<-store.started // s1 is uploading, held open
	defer func() { store.release <- nil }()
	commitLocalSnapshot(t, tr, "s2", 2)
	eventually(t, "the second pause's prune ends", func() bool { return !pruneDue(tr) })
	started, release := blockRemovals(tr)
	commitLocalSnapshot(t, tr, "s3", 3)
	<-started // a worker is deleting s2, held open

	done := make(chan struct{})
	go func() {
		defer close(done)
		end, err := tr.BeginStart(t.Context(), trackedActorUID, "s2")
		if err != nil {
			t.Errorf("BeginStart: %v", err)
			return
		}
		end()
	}()
	select {
	case <-done:
		t.Fatal("BeginStart(s2) returned while a prune of s2 was still running")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	waitClosed(t, done, "BeginStart(s2) once the prune ended, with the upload still running")
}

// A prune does not run while a Run, Restore, Checkpoint, or PromoteSnapshot
// holds the actor, even once its backoff has passed: the hold's end queues it.
func TestPruneDefersWhileTheActorIsHeld(t *testing.T) {
	tr := newIdleTestTracker(t, nil) // workers not started
	calls := failRemovals(tr)
	commitLocalSnapshots(t, tr, "s1", "s2")
	end, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, localRef("s3", 3, tr.clock.Now()).Snapshot, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
	if err != nil {
		t.Fatalf("BeginCheckpoint: %v", err)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("BeginCheckpoint tried the failing prune %d times, want 1", n)
	}
	restoreRemovals(tr)
	stepTo(tr, tr.clock.Now().Add(pruneRetryDelay))
	processNextPrune(t, tr)
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

// A Checkpoint that leaves a copy of its snapshot behind without committing
// it, as one that fails partway through keeping the copy does, lists the copy
// for a prune when it ends. One that left no copy lists nothing, and a retry
// that commits the snapshot takes it off the list.
func TestCheckpointEndListsALeftoverCopy(t *testing.T) {
	tr := newIdleTestTracker(t, nil) // workers not started
	commitLocalSnapshot(t, tr, "s1", 1)
	s2 := localRef("s2", 2, tr.clock.Now())

	end, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, s2.Snapshot, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
	if err != nil {
		t.Fatalf("BeginCheckpoint: %v", err)
	}
	end() // failed before writing anything
	if got := pruneList(); got != nil {
		t.Fatalf("state file lists %v to prune after a checkpoint that left no copy, want none", got)
	}

	if end, err = tr.BeginCheckpoint(t.Context(), trackedActorUID, s2.Snapshot, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err != nil {
		t.Fatalf("BeginCheckpoint: %v", err)
	}
	if err := os.MkdirAll(ateletpath.LocalSnapshotDir(trackedActorUID, "s2"), 0o700); err != nil {
		t.Fatal(err)
	}
	end() // failed after writing the copy, before the commit
	if got := pruneList(); !slices.Equal(got, []string{"s2"}) {
		t.Fatalf("state file lists %v to prune, want [s2]", got)
	}

	// The next checkpoint prunes the leftover copy before it writes. While
	// that prune fails, the checkpoint fails with Unavailable rather than
	// write s2 under a listed copy.
	calls := failRemovals(tr)
	if _, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, s2.Snapshot, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); apierror.Code(err) != codes.Unavailable {
		t.Fatalf("BeginCheckpoint with s2's copy still listed = %v, want Unavailable", err)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("the failing prune was tried %d times, want 1", n)
	}
	restoreRemovals(tr)
	if end, err = tr.BeginCheckpoint(t.Context(), trackedActorUID, s2.Snapshot, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err != nil {
		t.Fatalf("BeginCheckpoint once the prune can run: %v", err)
	}
	end()
	if localSnapshotExists("s2") || pruneList() != nil {
		t.Errorf("after the checkpoint began: s2 exists = %v, listed = %v; want the copy pruned and nothing listed", localSnapshotExists("s2"), pruneList())
	}
}

// A Restore of a listed snapshot, as a resume after a revert to an older
// generation can be, has that snapshot pruned before it begins, so it reads
// the durable copy rather than a local copy a later prune deletes under the
// running workload. While the prune fails, the Restore fails with Unavailable.
func TestBeginStartPrunesAListedSnapshotFirst(t *testing.T) {
	tr := newIdleTestTracker(t, nil) // workers not started
	commitLocalSnapshots(t, tr, "s1", "s2")
	calls := failRemovals(tr)
	if _, err := tr.BeginStart(t.Context(), trackedActorUID, "s1"); apierror.Code(err) != codes.Unavailable {
		t.Fatalf("BeginStart(s1) while its prune fails = %v, want Unavailable", err)
	}
	if n := calls.Load(); n != 1 || !localSnapshotExists("s1") {
		t.Fatalf("the failing prune was tried %d times and s1 exists = %v; want 1 and true", n, localSnapshotExists("s1"))
	}
	restoreRemovals(tr)
	end, err := tr.BeginStart(t.Context(), trackedActorUID, "s1")
	if err != nil {
		t.Fatalf("BeginStart(s1): %v", err)
	}
	end()
	if localSnapshotExists("s1") || !localSnapshotExists("s2") {
		t.Errorf("after BeginStart(s1): s1 exists = %v, s2 exists = %v; want s1 pruned and s2 kept", localSnapshotExists("s1"), localSnapshotExists("s2"))
	}
}
