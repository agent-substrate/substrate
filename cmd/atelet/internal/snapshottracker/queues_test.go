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
	"os"
	"slices"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
)

// Uploads and detaches have queues and workers of their own, so a due detach
// runs while a slow upload holds the only upload worker, and an upload that
// comes due meanwhile waits for that worker.
func TestTrackerDetachDoesNotWaitBehindUploads(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newIdleTestTracker(t, store)
	setWorkers(tr, 1, 1)
	tr.Start()
	for _, id := range []string{"actor-1", "actor-2", "actor-3"} {
		if err := os.MkdirAll(ateletpath.ActorPath(id), 0o700); err != nil {
			t.Fatal(err)
		}
	}

	// actor-2's snapshot is durable already, as after an immediate
	// checkpoint, so its detach only prunes.
	t0 := tr.clock.Now()
	a2 := testRef("a2-s1", 1, t0)
	tr.CommitDurable("actor-2", a2)
	tr.CommitLocal("actor-2", a2)
	stepTo(tr, t0.Add(testDetachDelay-time.Minute))

	taken := tr.clock.Now().Add(-testUploadDelay)
	tr.CommitLocal("actor-1", testRef("a1-s1", 1, taken))
	if got := waitStarted(t, store); got != "a1-s1" {
		t.Fatalf("started = %q, want a1-s1", got)
	}
	tr.CommitLocal("actor-3", testRef("a3-s1", 1, taken))

	stepTo(tr, t0.Add(testDetachDelay))
	eventually(t, "actor-2 is detached while actor-1 uploads", func() bool {
		return tr.State("actor-2").GetLatestLocal() == nil
	})
	select {
	case got := <-store.started:
		t.Fatalf("upload of %s started while actor-1's holds the only upload worker", got)
	default:
	}

	store.release <- nil
	if got := waitStarted(t, store); got != "a3-s1" {
		t.Fatalf("started = %q, want a3-s1 once actor-1's upload ended", got)
	}
	store.release <- nil
}

// When both an upload and a detach are due for the same actor, whichever
// starts first holds a.op and causes the other queue's worker to skip the
// actor. When the upload runs first, endOp requeues the detach, which then
// prunes the now-durable snapshot without uploading again.
func TestTrackerSameActorUploadAndDetachDue(t *testing.T) {
	t.Run("upload first", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		tr := newIdleTestTracker(t, store)
		t0 := tr.clock.Now()
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
		stepTo(tr, t0.Add(testDetachDelay))

		uploadDone := make(chan struct{})
		go func() {
			defer close(uploadDone)
			processNext(t, tr, opUpload)
		}()
		if got := waitStarted(t, store); got != "s1" {
			t.Fatalf("started = %q, want s1", got)
		}

		// The detach worker finds a.op held by the upload and skips the actor.
		processNext(t, tr, opDetach)
		select {
		case got := <-store.started:
			t.Fatalf("store called for %q while upload holds a.op", got)
		default:
		}
		if got := tr.queues[opDetach].Len(); got != 0 {
			t.Errorf("detach queue len while upload runs = %d, want 0", got)
		}
		if got := uuidOf(tr.State(trackedActorUID).GetLatestLocal()); got != "s1" {
			t.Fatalf("latest_local while upload runs = %q, want s1", got)
		}

		store.release <- nil
		waitClosed(t, uploadDone, "upload worker")
		st := tr.State(trackedActorUID)
		if uuidOf(st.GetLatestLocal()) != "s1" || uuidOf(st.GetLatestDurable()) != "s1" {
			t.Fatalf("state after upload = %v, want s1 both local and durable", st)
		}
		if got := tr.queues[opDetach].Len(); got != 1 {
			t.Fatalf("detach queue len after upload endOp = %d, want 1", got)
		}

		// The requeued detach sees s1 durable and prunes without uploading again.
		processNext(t, tr, opDetach)
		if got, want := store.uploads(), []string{"s1"}; !slices.Equal(got, want) {
			t.Errorf("uploads = %v, want %v", got, want)
		}
		st = tr.State(trackedActorUID)
		if st.GetLatestLocal() != nil || uuidOf(st.GetLatestDurable()) != "s1" {
			t.Errorf("state after detach = %v, want nothing local and s1 durable", st)
		}
		if got := queuedNow(tr); got != 0 {
			t.Errorf("queued after detach = %d, want 0", got)
		}
	})

	t.Run("detach first", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		tr := newIdleTestTracker(t, store)
		t0 := tr.clock.Now()
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
		stepTo(tr, t0.Add(testDetachDelay))

		detachDone := make(chan struct{})
		go func() {
			defer close(detachDone)
			processNext(t, tr, opDetach)
		}()
		if got := waitStarted(t, store); got != "s1" {
			t.Fatalf("started = %q, want s1", got)
		}

		// The upload worker finds a.op held by the detach and skips the actor.
		processNext(t, tr, opUpload)
		select {
		case got := <-store.started:
			t.Fatalf("store called for %q while detach holds a.op", got)
		default:
		}
		if got := tr.queues[opUpload].Len(); got != 0 {
			t.Errorf("upload queue len while detach runs = %d, want 0", got)
		}

		store.release <- nil
		waitClosed(t, detachDone, "detach worker")
		if got, want := store.uploads(), []string{"s1"}; !slices.Equal(got, want) {
			t.Errorf("uploads = %v, want %v", got, want)
		}
		st := tr.State(trackedActorUID)
		if st.GetLatestLocal() != nil || uuidOf(st.GetLatestDurable()) != "s1" {
			t.Errorf("state after detach = %v, want nothing local and s1 durable", st)
		}
		if got := queuedNow(tr); got != 0 {
			t.Errorf("queued after detach = %d, want 0", got)
		}
	})
}

// A Checkpoint hold (a.hold != holdNone) keeps opDetach from pruning local
// snapshots underneath the checkpoint, while opUpload still runs in the
// background. When the hold ends, opDetach prunes the now-durable snapshot
// without uploading again.
func TestTrackerUploadRunsDuringCheckpointHoldWhileDetachWaits(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newIdleTestTracker(t, store)
	t0 := tr.clock.Now()
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))

	endCheckpoint, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, testRef("s2", 2, time.Time{}).Snapshot)
	if err != nil {
		t.Fatalf("BeginCheckpoint: %v", err)
	}
	stepTo(tr, t0.Add(testDetachDelay))

	processNext(t, tr, opDetach)
	if got := store.uploads(); len(got) != 0 {
		t.Fatalf("uploads while held = %v, want none from opDetach", got)
	}
	if got := uuidOf(tr.State(trackedActorUID).GetLatestLocal()); got != "s1" {
		t.Fatalf("latest_local while held = %q, want s1", got)
	}
	if got := tr.queues[opDetach].Len(); got != 0 {
		t.Errorf("detach queue len while held = %d, want 0 until hold or op ends", got)
	}

	processNext(t, tr, opUpload)
	if got, want := store.uploads(), []string{"s1"}; !slices.Equal(got, want) {
		t.Fatalf("uploads after opUpload = %v, want %v", got, want)
	}
	st := tr.State(trackedActorUID)
	if uuidOf(st.GetLatestLocal()) != "s1" || uuidOf(st.GetLatestDurable()) != "s1" {
		t.Fatalf("state after opUpload = %v, want s1 both local and durable", st)
	}

	// endOp from opUpload requeues opDetach, which still waits while held.
	processNext(t, tr, opDetach)
	if got := uuidOf(tr.State(trackedActorUID).GetLatestLocal()); got != "s1" {
		t.Fatalf("latest_local while still held = %q, want s1", got)
	}

	endCheckpoint()
	processNext(t, tr, opDetach)
	if got, want := store.uploads(), []string{"s1"}; !slices.Equal(got, want) {
		t.Errorf("uploads after hold ended = %v, want %v without a second upload", got, want)
	}
	st = tr.State(trackedActorUID)
	if st.GetLatestLocal() != nil || uuidOf(st.GetLatestDurable()) != "s1" {
		t.Errorf("state after detach = %v, want nothing local and s1 durable", st)
	}
}
