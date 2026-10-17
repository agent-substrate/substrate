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
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// startUpload advances the clock to when the tracked actor's upload is due,
// and waits for a worker to start it. It returns a channel closed once the
// upload has ended.
func startUpload(t *testing.T, tr *Tracker, store *fakeSnapshotStore) <-chan struct{} {
	t.Helper()
	due := timeOf(tr.State(trackedActorUID).GetUploadDueTime())
	if due.IsZero() {
		t.Fatal("no upload scheduled")
	}
	stepTo(tr, due)
	waitStarted(t, store)
	return opDone(t, tr, trackedActorUID)
}

// uploadNow runs the tracked actor's upload once it is due, and ends it on the
// blocking store with err.
func uploadNow(t *testing.T, tr *Tracker, store *fakeSnapshotStore, err error) {
	t.Helper()
	done := startUpload(t, tr, store)
	store.release <- err
	waitClosed(t, done, "the upload")
}

// Deferred checkpoints keep their snapshots local, and atelet uploads the
// latest one once the oldest snapshot not yet durable has waited the upload
// delay. The local copy stays after the upload.
func TestTrackerUploadsLatestLocalSnapshot(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	if err := os.MkdirAll(ateletpath.LocalSnapshotDir(trackedActorUID, "s2"), 0o700); err != nil {
		t.Fatal(err)
	}
	t0 := tr.clock.Now()

	st := tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
	if got := st.GetLatestLocal(); uuidOf(got) != "s1" || !timeOf(got.GetLocalCommitTime()).Equal(t0) {
		t.Errorf("latest_local = %v, want s1 committed at %v", got, t0)
	}
	if got, want := timeOf(st.GetUploadDueTime()), t0.Add(testUploadDelay); !got.Equal(want) {
		t.Errorf("upload_due_time = %v, want %v", got, want)
	}

	st = tr.CommitLocal(trackedActorUID, testRef("s2", 2, t0.Add(10*time.Minute)))
	if got, want := timeOf(st.GetUploadDueTime()), t0.Add(testUploadDelay); !got.Equal(want) {
		t.Errorf("upload_due_time = %v, want %v: when the first snapshot was due", got, want)
	}

	uploadNow(t, tr, store, nil)
	if got, want := store.uploads(), []string{"s2"}; !slices.Equal(got, want) {
		t.Errorf("uploads = %v, want %v", got, want)
	}
	if _, err := os.Stat(ateletpath.LocalSnapshotDir(trackedActorUID, "s2")); err != nil {
		t.Errorf("s2 local dir after upload: %v, want it kept", err)
	}
	st = tr.State(trackedActorUID)
	if got := st.GetLatestDurable(); uuidOf(got) != "s2" || got.GetDurableCompleteTime() == nil {
		t.Errorf("latest_durable = %v, want s2 with its upload time", got)
	}
	if got := uuidOf(st.GetLatestLocal()); got != "s2" {
		t.Errorf("latest_local = %q, want s2 kept after its upload", got)
	}
	if st.GetUploading() != nil || st.GetUploadDueTime() != nil {
		t.Errorf("state = %v, want no upload in progress or due", st)
	}
}

// A deferred checkpoint during an older snapshot's upload keeps that snapshot
// until the upload ends: LocalSnapshotsToKeep names it beside the new one. The
// upload's end deletes it and schedules the new snapshot's upload, which the
// older upload does not cover, an upload delay after the older upload
// succeeded.
func TestTrackerCheckpointDuringUpload(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	for _, name := range []string{"s1", "s2"} {
		if err := os.MkdirAll(ateletpath.LocalSnapshotDir(trackedActorUID, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	t0 := tr.clock.Now()
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
	done := startUpload(t, tr, store)

	st := tr.CommitLocal(trackedActorUID, testRef("s2", 2, tr.clock.Now()))
	if keep := tr.LocalSnapshotsToKeep(trackedActorUID); !slices.Equal(keep, []string{"s2", "s1"}) {
		t.Errorf("LocalSnapshotsToKeep = %v, want [s2 s1]: s1 is being uploaded", keep)
	}
	if got := uuidOf(st.GetUploading()); got != "s1" {
		t.Errorf("uploading = %q, want s1", got)
	}
	if got, want := timeOf(st.GetUploadDueTime()), t0.Add(testUploadDelay); !got.Equal(want) {
		t.Errorf("upload_due_time = %v, want %v: s1's, which the upload in progress delays", got, want)
	}

	stepTo(tr, tr.clock.Now().Add(10*time.Minute))
	store.release <- nil
	waitClosed(t, done, "the upload of s1")
	if _, err := os.Stat(ateletpath.LocalSnapshotDir(trackedActorUID, "s1")); !os.IsNotExist(err) {
		t.Errorf("s1 after its upload: stat = %v, want it deleted", err)
	}
	if _, err := os.Stat(ateletpath.LocalSnapshotDir(trackedActorUID, "s2")); err != nil {
		t.Errorf("s2 after the upload of s1: %v, want it kept", err)
	}
	st = tr.State(trackedActorUID)
	if got := uuidOf(st.GetLatestDurable()); got != "s1" {
		t.Errorf("latest_durable = %q, want s1", got)
	}
	if got, want := timeOf(st.GetUploadDueTime()), tr.clock.Now().Add(testUploadDelay); !got.Equal(want) {
		t.Errorf("upload_due_time = %v, want %v: an upload delay after the upload of s1 succeeded", got, want)
	}

	uploadNow(t, tr, store, nil)
	if got := uuidOf(tr.State(trackedActorUID).GetLatestDurable()); got != "s2" {
		t.Errorf("latest_durable = %q, want s2", got)
	}
}

// An immediate checkpoint makes the actor's newest snapshot durable, which
// drops the older local snapshot, then records its own local copy. An older
// snapshot's upload in progress runs on: LocalSnapshotsToKeep keeps that
// snapshot, and the upload deletes it when it ends without holding the actor
// back when it fails, though the new local copy has a detach scheduled.
func TestTrackerImmediateCheckpointDuringUpload(t *testing.T) {
	for _, tc := range []struct {
		name      string
		uploadErr error
	}{
		{name: "older upload succeeds"},
		{name: "older upload fails", uploadErr: errors.New("boom")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeSnapshotStore()
			store.setBlocking(true)
			tr := newTestTracker(t, store)
			if err := os.MkdirAll(ateletpath.LocalSnapshotDir(trackedActorUID, "s1"), 0o700); err != nil {
				t.Fatal(err)
			}
			t0 := tr.clock.Now()
			tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
			done := startUpload(t, tr, store)

			s2 := testRef("s2", 2, t0.Add(time.Minute))
			s2.UploadTime = s2.CommitTime.Add(time.Second)
			st := tr.CommitDurable(trackedActorUID, s2)
			if st.GetLatestLocal() != nil || st.GetUploadDueTime() != nil || uuidOf(st.GetUploading()) != "s1" {
				t.Errorf("state = %v, want nothing local or due, and s1 uploading", st)
			}
			if keep := tr.LocalSnapshotsToKeep(trackedActorUID); !slices.Equal(keep, []string{"s1"}) {
				t.Errorf("LocalSnapshotsToKeep = %v, want [s1]: s1 is being uploaded", keep)
			}
			if got := st.GetLatestDurable(); uuidOf(got) != "s2" || !timeOf(got.GetDurableCompleteTime()).Equal(s2.UploadTime) {
				t.Errorf("latest_durable = %v, want s2 uploaded at %v", got, s2.UploadTime)
			}
			tr.CommitLocal(trackedActorUID, testRef("s2", 2, s2.CommitTime))

			store.release <- tc.uploadErr
			waitClosed(t, done, "the upload of s1")
			if _, err := os.Stat(ateletpath.LocalSnapshotDir(trackedActorUID, "s1")); !os.IsNotExist(err) {
				t.Errorf("s1 after its upload: stat = %v, want it deleted", err)
			}
			if got := uuidOf(tr.State(trackedActorUID).GetLatestDurable()); got != "s2" {
				t.Errorf("latest_durable = %q after the upload of s1, want s2", got)
			}
			if inMemory(tr) {
				t.Error("entry stayed in memory after the older upload ended, want it dropped")
			}

			// The next deferred checkpoint is due a full delay after it was taken.
			s3Time := tr.clock.Now()
			st = tr.CommitLocal(trackedActorUID, testRef("s3", 3, s3Time))
			if got, want := timeOf(st.GetUploadDueTime()), s3Time.Add(testUploadDelay); !got.Equal(want) {
				t.Errorf("upload_due_time = %v, want %v", got, want)
			}
		})
	}
}

// An older upload finishing after a newer snapshot became durable and another
// local checkpoint was taken does not push the new local checkpoint's upload
// due time back.
func TestTrackerLateUploadDoesNotPushBackNextUpload(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	t0 := tr.clock.Now()
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
	done := startUpload(t, tr, store)

	s2 := testRef("s2", 2, t0.Add(time.Minute))
	s2.UploadTime = s2.CommitTime.Add(time.Second)
	tr.CommitDurable(trackedActorUID, s2)
	t3 := t0.Add(2 * time.Minute)
	tr.CommitLocal(trackedActorUID, testRef("s3", 3, t3))

	stepTo(tr, t3.Add(10*time.Minute))
	store.release <- nil
	waitClosed(t, done, "the upload of s1")

	st := tr.State(trackedActorUID)
	if got := uuidOf(st.GetLatestDurable()); got != "s2" {
		t.Errorf("latest_durable = %q, want s2", got)
	}
	if got, want := timeOf(st.GetUploadDueTime()), t3.Add(testUploadDelay); !got.Equal(want) {
		t.Errorf("upload_due_time = %v, want %v: an upload delay after s3 was taken", got, want)
	}
}

// A failed background upload is retried once the retry delay has passed, and
// the delay doubles with each further failure.
func TestTrackerRetriesFailedUpload(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

	for _, delay := range []time.Duration{testRetryDelay, 2 * testRetryDelay} {
		uploadNow(t, tr, store, errors.New("boom"))
		st := tr.State(trackedActorUID)
		if st.GetLatestDurable() != nil {
			t.Errorf("latest_durable = %v after a failed upload, want unset", st.GetLatestDurable())
		}
		if got, want := timeOf(st.GetUploadDueTime()), tr.clock.Now().Add(delay); !got.Equal(want) {
			t.Errorf("upload_due_time = %v, want %v: %v after the failure", got, want, delay)
		}
	}

	uploadNow(t, tr, store, nil)
	if got := uuidOf(tr.State(trackedActorUID).GetLatestDurable()); got != "s1" {
		t.Errorf("latest_durable = %q after the retry, want s1", got)
	}
}

// A failed upload leaves the actor undurable since its oldest snapshot not yet
// durable, rather than since the snapshot the upload read, and a checkpoint
// taken during the upload does not move that.
func TestTrackerFailedUploadKeepsUndurableSince(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	t0 := tr.clock.Now()
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
	tr.CommitLocal(trackedActorUID, testRef("s2", 2, t0.Add(50*time.Minute)))
	done := startUpload(t, tr, store)
	tr.CommitLocal(trackedActorUID, testRef("s3", 3, tr.clock.Now()))

	store.release <- errors.New("boom")
	waitClosed(t, done, "the upload of s2")
	if got, want := timeOf(tr.State(trackedActorUID).GetUploadDueTime()), tr.clock.Now().Add(testRetryDelay); !got.Equal(want) {
		t.Errorf("upload_due_time = %v, want %v: the retry delay after the failure, as s1 has waited over an upload delay", got, want)
	}
}

// Forget does not wait for a background upload in progress: it cancels the
// upload, which then leaves the forgotten actor alone. The upload records no
// state for the actor, and deletes none of its local snapshots, not even one
// a newer checkpoint superseded: Terminate prunes them. Its metric records
// error.type Canceled.
func TestTrackerForgetCancelsUpload(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newIdleTestTracker(t, store)
	inst, reader := newTestInstruments(t)
	tr.instruments = inst
	tr.Start()
	if err := os.MkdirAll(ateletpath.LocalSnapshotDir(trackedActorUID, "s1"), 0o700); err != nil {
		t.Fatal(err)
	}
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	done := startUpload(t, tr, store)
	tr.CommitLocal(trackedActorUID, testRef("s2", 2, tr.clock.Now()))

	returnsPromptly(t, "Forget during an upload", func() {
		if err := tr.Forget(trackedActorUID); err != nil {
			t.Errorf("Forget: %v", err)
		}
	})
	waitClosed(t, done, "the cancelled upload")
	if st := tr.State(trackedActorUID); !proto.Equal(st, &ateletpb.ActorSnapshotState{}) {
		t.Errorf("state after forget = %v, want empty", st)
	}
	if _, err := os.Stat(ateletpath.ActorSnapshotStateFile(trackedActorUID)); !os.IsNotExist(err) {
		t.Errorf("state file once the cancelled upload ended: stat = %v, want none", err)
	}
	if _, err := os.Stat(ateletpath.LocalSnapshotDir(trackedActorUID, "s1")); err != nil {
		t.Errorf("s1 once the cancelled upload ended: %v, want it left for Terminate to prune", err)
	}
	if got, want := recordedErrorTypes(t, reader), []string{codes.Canceled.String()}; !slices.Equal(got, want) {
		t.Errorf("recorded upload error types = %v, want %v", got, want)
	}
}

// The upload workers bound how many background uploads run at once, and an
// actor that checkpoints again while it waits for one uploads only its newest
// snapshot.
func TestTrackerWorkersBoundBackgroundWork(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newIdleTestTracker(t, store)
	// The idle detach worker leaves actor-3's upload to the upload workers.
	setWorkers(tr, 2, 1)
	tr.Start()

	// Snapshots taken an upload delay ago are due for upload straight away.
	taken := tr.clock.Now().Add(-testUploadDelay)
	for _, id := range []string{"actor-1", "actor-2", "actor-3"} {
		if err := os.MkdirAll(ateletpath.ActorPath(id), 0o700); err != nil {
			t.Fatal(err)
		}
		tr.CommitLocal(id, testRef(id+"-s1", 1, taken))
	}
	started := []string{waitStarted(t, store), waitStarted(t, store)}
	slices.Sort(started)
	if want := []string{"actor-1-s1", "actor-2-s1"}; !slices.Equal(started, want) {
		t.Fatalf("first started = %v, want %v", started, want)
	}
	select {
	case got := <-store.started:
		t.Fatalf("a third upload (%q) started while both upload workers are busy", got)
	default:
	}
	if st := tr.State("actor-3"); st.GetUploading() != nil || st.GetUploadDueTime() == nil {
		t.Errorf("actor-3 state while it waits = %v, want an upload due and none in progress", st)
	}

	// actor-3 checkpoints again while it waits for an upload worker.
	tr.CommitLocal("actor-3", testRef("actor-3-s2", 2, taken.Add(time.Minute)))

	done1 := opDone(t, tr, "actor-1")
	done2 := opDone(t, tr, "actor-2")
	// Ending one upload frees an upload worker, which takes actor-3.
	store.release <- nil
	if got := waitStarted(t, store); got != "actor-3-s2" {
		t.Fatalf("started = %q, want actor-3-s2", got)
	}
	done3 := opDone(t, tr, "actor-3")
	store.release <- nil
	store.release <- nil
	waitClosed(t, done1, "actor-1's upload")
	waitClosed(t, done2, "actor-2's upload")
	waitClosed(t, done3, "actor-3's upload")

	if got := uuidOf(tr.State("actor-3").GetLatestDurable()); got != "actor-3-s2" {
		t.Errorf("actor-3 latest_durable = %q, want actor-3-s2", got)
	}
	if got := len(store.uploads()); got != 3 {
		t.Errorf("uploads = %v, want 3: actor-3-s1 superseded before it was uploaded", store.uploads())
	}
}

// The queue holds an actor until the earliest time it was queued for. A worker
// that takes the actor before its upload is due, as after an immediate
// checkpoint made the snapshot it was queued for durable, does nothing and
// queues it again for when its upload is due.
func TestTrackerRequeuesUntilWorkIsDue(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newIdleTestTracker(t, store)
	t0 := tr.clock.Now()
	// Queued for the upload of s1, due an upload delay out. The immediate
	// checkpoint makes s2 durable, so the upload of s3 is due an upload delay
	// after s3 was taken, later.
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
	tr.CommitDurable(trackedActorUID, testRef("s2", 2, t0.Add(10*time.Minute)))
	tr.CommitLocal(trackedActorUID, testRef("s3", 3, t0.Add(20*time.Minute)))

	stepTo(tr, t0.Add(testUploadDelay))
	processNext(t, tr, opUpload)
	if got := store.uploads(); len(got) != 0 {
		t.Errorf("uploads = %v before the upload of s3 is due, want none", got)
	}

	stepTo(tr, t0.Add(20*time.Minute+testUploadDelay))
	processNext(t, tr, opUpload)
	if got, want := store.uploads(), []string{"s3"}; !slices.Equal(got, want) {
		t.Errorf("uploads = %v, want %v", got, want)
	}
}

// Start queues each actor with a snapshot state file, so a tracker started
// after an atelet restart uploads the newest snapshot its predecessor left
// due, using the oldest undurable snapshot's commit time.
func TestTrackerResumesUploadAfterRestart(t *testing.T) {
	useActorDir(t)
	store := newFakeSnapshotStore()
	before := newStoreTracker(t, store)
	t0 := before.clock.Now().Add(-testUploadDelay)
	before.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
	before.CommitLocal(trackedActorUID, testRef("s2", 2, t0.Add(50*time.Minute)))

	after := newStoreTracker(t, store)
	if got, want := timeOf(after.State(trackedActorUID).GetUploadDueTime()), t0.Add(testUploadDelay); !got.Equal(want) {
		t.Fatalf("upload_due_time after restart = %v, want %v from s1's commit time", got, want)
	}
	after.Start()
	if got := waitStarted(t, store); got != "s2" {
		t.Fatalf("started = %q after the restart, want s2", got)
	}
}

// A tracker without a store schedules no upload or detach, and reports no
// upload due.
func TestTrackerWithoutStoreSchedulesNoUploads(t *testing.T) {
	useActorDir(t)
	tr := newTracker(t)
	taken := tr.clock.Now().Add(-time.Hour)
	if st := tr.CommitLocal(trackedActorUID, testRef("s1", 1, taken)); st.GetUploadDueTime() != nil {
		t.Errorf("upload_due_time = %v without a store, want unset", st.GetUploadDueTime())
	}
	if n := queuedNow(tr); n != 0 {
		t.Errorf("queued actors = %d without a store, want none", n)
	}
	// The queues' lengths leave out actors queued for later, as a detach is.
	tr.lock(trackedActorUID)
	due, ok := tr.nextDueLocked(tr.findLocked(trackedActorUID))
	tr.unlock(trackedActorUID)
	if ok {
		t.Errorf("background work due at %v without a store, want none", due)
	}
}

// A local snapshot no upload can make durable, as a pause takes today, never
// comes due for a background upload, rather than failing it again and again.
// A checkpoint that replaces it schedules an upload again.
func TestTrackerSkipsUnuploadableSnapshots(t *testing.T) {
	for _, tc := range []struct {
		name   string
		commit func(tr *Tracker, taken time.Time)
	}{
		{
			name: "no object storage location",
			commit: func(tr *Tracker, taken time.Time) {
				local := testRef("s1", 1, taken)
				local.Snapshot.Object = nil
				tr.CommitLocal(trackedActorUID, local)
			},
		},
		{
			name: "durable as another generation",
			commit: func(tr *Tracker, taken time.Time) {
				tr.CommitDurable(trackedActorUID, testRef("s1", 1, taken))
				tr.CommitLocal(trackedActorUID, testRef("s1", 2, taken))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := newIdleTestTracker(t, newFakeSnapshotStore())
			// Taken an upload delay ago, so an upload would be due at once.
			taken := tr.clock.Now().Add(-testUploadDelay)
			tc.commit(tr, taken)
			if due := tr.State(trackedActorUID).GetUploadDueTime(); due != nil {
				t.Errorf("upload_due_time = %v, want unset", due)
			}
			if n := queuedNow(tr); n != 0 {
				t.Errorf("queued actors = %d, want none", n)
			}

			tr.CommitLocal(trackedActorUID, testRef("s2", 3, taken))
			if due := tr.State(trackedActorUID).GetUploadDueTime(); due == nil {
				t.Error("upload_due_time unset once a checkpoint replaced the snapshot, want set")
			}
		})
	}
}

// A Run or Restore unschedules detach, but leaves failed upload backoff intact
// while an upload is still due.
func TestTrackerStartKeepsUploadBackoff(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

	uploadNow(t, tr, store, errors.New("boom"))
	retryAt := tr.clock.Now().Add(testRetryDelay)

	end, err := tr.BeginStart(t.Context(), trackedActorUID)
	if err != nil {
		t.Fatalf("BeginStart: %v", err)
	}
	end()

	if got := timeOf(tr.State(trackedActorUID).GetUploadDueTime()); !got.Equal(retryAt) {
		t.Fatalf("upload_due_time after start = %v, want %v kept", got, retryAt)
	}
	uploadNow(t, tr, store, errors.New("boom"))
	if got, want := timeOf(tr.State(trackedActorUID).GetUploadDueTime()), retryAt.Add(2*testRetryDelay); !got.Equal(want) {
		t.Errorf("upload_due_time after second failure = %v, want %v (doubled)", got, want)
	}
}

// BeginStart and BeginCheckpoint wait only for a detach in progress, not for a
// background upload.
func TestTrackerStartAndCheckpointDoNotWaitForUpload(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	done := startUpload(t, tr, store)

	for _, tc := range []struct {
		name  string
		begin func(context.Context, string) (func(), error)
	}{
		{"BeginStart", tr.BeginStart},
		{"BeginCheckpoint", func(ctx context.Context, uid string) (func(), error) {
			return tr.BeginCheckpoint(ctx, uid, testRef("s2", 2, time.Time{}).Snapshot)
		}},
	} {
		returnsPromptly(t, tc.name+" during a background upload", func() {
			end, err := tc.begin(t.Context(), trackedActorUID)
			if err != nil {
				t.Errorf("%s: %v", tc.name, err)
				return
			}
			end()
		})
	}

	store.release <- nil
	waitClosed(t, done, "the upload")
}

// A background upload runs even while the actor is held by a Run, Restore, or
// Checkpoint, since an upload keeps the local snapshot intact.
func TestTrackerUploadRunsWhileActorHeld(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	t0 := tr.clock.Now()
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))

	end, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, testRef("s2", 2, t0).Snapshot)
	if err != nil {
		t.Fatalf("BeginCheckpoint: %v", err)
	}
	defer end()

	done := startUpload(t, tr, store)
	store.release <- nil
	waitClosed(t, done, "the upload while held")
	if got := uuidOf(tr.State(trackedActorUID).GetLatestDurable()); got != "s1" {
		t.Errorf("latest_durable = %q, want s1", got)
	}
}

// BeginPromote waits for any detach in progress and any background upload of
// the same snapshot, reports whether the snapshot is already durable, and
// fails with FailedPrecondition when the durable snapshot with that UUID has a
// different generation, fidelity, or URI.
func TestTrackerBeginPromote(t *testing.T) {
	t.Run("waits for detach", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		tr := newTestTracker(t, store)
		s1 := testRef("s1", 1, tr.clock.Now())
		tr.CommitLocal(trackedActorUID, s1)
		detached := startDetach(t, tr)
		waitStarted(t, store)

		type promoteRes struct {
			durable bool
			err     error
		}
		resCh := make(chan promoteRes, 1)
		go func() {
			durable, end, err := tr.BeginPromote(t.Context(), trackedActorUID, s1)
			if err == nil {
				end()
			}
			resCh <- promoteRes{durable, err}
		}()
		select {
		case res := <-resCh:
			t.Fatalf("BeginPromote returned (%v, %v) during a detach", res.durable, res.err)
		case <-time.After(50 * time.Millisecond):
		}

		store.release <- nil
		if err := <-detached; err != nil {
			t.Fatalf("detach: %v", err)
		}
		res := <-resCh
		if res.err != nil || !res.durable {
			t.Errorf("BeginPromote = (%v, %v), want (true, nil)", res.durable, res.err)
		}
	})

	t.Run("waits for upload of same snapshot", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		tr := newTestTracker(t, store)
		s1 := testRef("s1", 1, tr.clock.Now())
		tr.CommitLocal(trackedActorUID, s1)
		done := startUpload(t, tr, store)

		type promoteRes struct {
			durable bool
			err     error
		}
		resCh := make(chan promoteRes, 1)
		go func() {
			durable, end, err := tr.BeginPromote(t.Context(), trackedActorUID, s1)
			if err == nil {
				end()
			}
			resCh <- promoteRes{durable, err}
		}()
		select {
		case res := <-resCh:
			t.Fatalf("BeginPromote returned (%v, %v) during an upload of the same snapshot", res.durable, res.err)
		case <-time.After(50 * time.Millisecond):
		}

		store.release <- nil
		waitClosed(t, done, "the upload")
		res := <-resCh
		if res.err != nil || !res.durable {
			t.Errorf("BeginPromote = (%v, %v), want (true, nil)", res.durable, res.err)
		}
	})

	t.Run("rejects snapshot superseded during upload", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		tr := newTestTracker(t, store)
		s1 := testRef("s1", 1, tr.clock.Now())
		tr.CommitLocal(trackedActorUID, s1)
		done := startUpload(t, tr, store)

		errCh := make(chan error, 1)
		go func() {
			_, end, err := tr.BeginPromote(t.Context(), trackedActorUID, s1)
			if err == nil {
				end()
			}
			errCh <- err
		}()
		select {
		case err := <-errCh:
			t.Fatalf("BeginPromote returned (%v) during an upload of the same snapshot", err)
		case <-time.After(50 * time.Millisecond):
		}

		tr.CommitLocal(trackedActorUID, testRef("s2", 2, tr.clock.Now()))
		store.release <- nil
		waitClosed(t, done, "the upload")
		if err := <-errCh; apierror.Code(err) != codes.Aborted {
			t.Errorf("BeginPromote = %v, want Aborted", err)
		}
	})

	t.Run("does not wait for upload of older snapshot", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		tr := newTestTracker(t, store)
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
		done := startUpload(t, tr, store)
		s2 := testRef("s2", 2, tr.clock.Now())
		tr.CommitLocal(trackedActorUID, s2)

		returnsPromptly(t, "BeginPromote of a newer snapshot during an older upload", func() {
			durable, end, err := tr.BeginPromote(t.Context(), trackedActorUID, s2)
			if err != nil {
				t.Errorf("BeginPromote: %v", err)
				return
			}
			end()
			if durable {
				t.Errorf("BeginPromote durable = true, want false")
			}
		})

		store.release <- nil
		waitClosed(t, done, "the upload")
	})

	for _, tc := range []struct {
		name   string
		mutate func(s *ateletpb.Snapshot)
	}{
		{
			name:   "refuses generation mismatch",
			mutate: func(s *ateletpb.Snapshot) { s.Generation = 2 },
		},
		{
			name:   "refuses fidelity mismatch",
			mutate: func(s *ateletpb.Snapshot) { s.Fidelity = ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES },
		},
		{
			name:   "refuses URI mismatch",
			mutate: func(s *ateletpb.Snapshot) { s.Object.SnapshotUri += "-other" },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := newIdleTestTracker(t, newFakeSnapshotStore())
			tr.CommitDurable(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
			req := testRef("s1", 1, time.Time{})
			tc.mutate(req.Snapshot)
			if _, _, err := tr.BeginPromote(t.Context(), trackedActorUID, req); apierror.Code(err) != codes.FailedPrecondition {
				t.Errorf("BeginPromote = %v, want FailedPrecondition", err)
			}
		})
	}
}

// When a background upload of s1 fails after a pause replaced LatestLocal with
// s2 (which has no object storage location), s1 stays in Uploading on disk and
// is retried. Further pause checkpoints keep s1 on disk and preserve its
// backoff, and once s1's retry succeeds, s1's local copy is deleted.
func TestTrackerRetriesUploadingWhenLatestLocalHasNoObject(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	for _, name := range []string{"s1", "s2", "s3"} {
		if err := os.MkdirAll(ateletpath.LocalSnapshotDir(trackedActorUID, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	done := startUpload(t, tr, store)

	s2 := testRef("s2", 2, tr.clock.Now())
	s2.Snapshot.Object = nil
	tr.CommitLocal(trackedActorUID, s2)
	if keep := tr.LocalSnapshotsToKeep(trackedActorUID); !slices.Equal(keep, []string{"s2", "s1"}) {
		t.Errorf("LocalSnapshotsToKeep during upload = %v, want [s2 s1]", keep)
	}

	store.release <- errors.New("boom")
	waitClosed(t, done, "the failed upload of s1")
	retryAt := tr.clock.Now().Add(testRetryDelay)

	if _, err := os.Stat(ateletpath.LocalSnapshotDir(trackedActorUID, "s1")); err != nil {
		t.Fatalf("s1 after failed upload: %v, want it kept to retry", err)
	}
	st := tr.State(trackedActorUID)
	if uuidOf(st.GetLatestLocal()) != "s2" || uuidOf(st.GetUploading()) != "s1" || !timeOf(st.GetUploadDueTime()).Equal(retryAt) {
		t.Fatalf("state after failed upload = %v, want s2 local, s1 uploading, due at %v", st, retryAt)
	}

	// Another pause checkpoint during backoff still keeps s1 and its backoff.
	s3 := testRef("s3", 3, tr.clock.Now())
	s3.Snapshot.Object = nil
	st = tr.CommitLocal(trackedActorUID, s3)
	if keep := tr.LocalSnapshotsToKeep(trackedActorUID); !slices.Equal(keep, []string{"s3", "s1"}) {
		t.Errorf("LocalSnapshotsToKeep during backoff = %v, want [s3 s1]", keep)
	}
	if uuidOf(st.GetUploading()) != "s1" || !timeOf(st.GetUploadDueTime()).Equal(retryAt) {
		t.Errorf("state after s3 = %v, want s1 still uploading and due at %v", st, retryAt)
	}

	uploadNow(t, tr, store, nil)
	if got, want := store.uploads(), []string{"s1", "s1"}; !slices.Equal(got, want) {
		t.Errorf("uploads = %v, want %v", got, want)
	}
	if _, err := os.Stat(ateletpath.LocalSnapshotDir(trackedActorUID, "s1")); !os.IsNotExist(err) {
		t.Errorf("s1 after retry succeeded: stat = %v, want it deleted", err)
	}
	st = tr.State(trackedActorUID)
	if uuidOf(st.GetLatestDurable()) != "s1" || uuidOf(st.GetLatestLocal()) != "s3" || st.GetUploading() != nil || st.GetUploadDueTime() != nil {
		t.Errorf("state after retry = %v, want s1 durable, s3 local, and no upload left", st)
	}
}

// When s1 failed and sits in Uploading waiting for retry, a newer durable
// snapshot supersedes s1: a worker clears Uploading and deletes s1's local
// copy without waiting for the retry timer. A promote that makes s1 itself
// durable clears Uploading too, but keeps s1's copy, which is still the latest
// local snapshot.
func TestTrackerClearsIdleUploadingWhenSuperseded(t *testing.T) {
	for _, tc := range []struct {
		name   string
		commit func(tr *Tracker)
		keepS1 bool
	}{
		{
			name: "CommitDurable",
			commit: func(tr *Tracker) {
				tr.CommitDurable(trackedActorUID, testRef("s2", 2, tr.clock.Now()))
			},
		},
		{
			name: "CommitPromoted of s1",
			commit: func(tr *Tracker) {
				s1 := testRef("s1", 1, time.Time{})
				s1.UploadTime = tr.clock.Now()
				tr.CommitPromoted(trackedActorUID, s1)
			},
			keepS1: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeSnapshotStore()
			store.setErr(errors.New("boom"))
			tr := newIdleTestTracker(t, store)
			s1Dir := ateletpath.LocalSnapshotDir(trackedActorUID, "s1")
			if err := os.MkdirAll(s1Dir, 0o700); err != nil {
				t.Fatal(err)
			}
			t0 := tr.clock.Now()
			tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
			stepTo(tr, t0.Add(testUploadDelay))
			processNext(t, tr, opUpload) // s1's upload fails, and s1 waits in Uploading to retry.

			store.setErr(nil)
			tc.commit(tr)
			if got := tr.queues[opDetach].Len(); got != 0 {
				t.Errorf("detach queue len after %s = %d, want 0", tc.name, got)
			}
			processNext(t, tr, opUpload)
			// State hides an Uploading it no longer keeps, so read the file.
			if rec := readSnapshotState(trackedActorUID); rec == nil || rec.Uploading != nil {
				t.Errorf("state file after %s = %+v, want Uploading cleared", tc.name, rec)
			}
			_, err := os.Stat(s1Dir)
			switch {
			case tc.keepS1 && err != nil:
				t.Errorf("s1 after %s: stat = %v, want it kept as the latest local snapshot", tc.name, err)
			case !tc.keepS1 && !os.IsNotExist(err):
				t.Errorf("s1 after %s: stat = %v, want it deleted", tc.name, err)
			}
		})
	}
}

// When s1 failed and sits in Uploading waiting for retry, CommitLocal of a
// newer snapshot that can be uploaded drops s1 from the snapshot state file in
// the same save, and leaves s1's local copy for the Checkpoint's prune. An
// atelet restart before any worker runs then uploads s2, rather than resuming
// s1 from a copy the prune has deleted.
func TestTrackerCommitLocalDropsFailedUploading(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setErr(errors.New("boom"))
	tr := newIdleTestTracker(t, store)
	// Taken an upload delay ago, so an upload is due at once, both here and
	// after the restart.
	taken := tr.clock.Now().Add(-testUploadDelay)
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, taken))
	processNext(t, tr, opUpload) // s1's upload fails, and s1 waits in Uploading to retry.

	tr.CommitLocal(trackedActorUID, testRef("s2", 2, tr.clock.Now()))
	if rec := readSnapshotState(trackedActorUID); rec == nil || rec.Uploading != nil {
		t.Errorf("state file after CommitLocal(s2) = %+v, want Uploading cleared", rec)
	}
	if keep := tr.LocalSnapshotsToKeep(trackedActorUID); !slices.Equal(keep, []string{"s2"}) {
		t.Errorf("LocalSnapshotsToKeep = %v, want [s2]", keep)
	}

	store2 := newFakeSnapshotStore()
	after := newStoreTracker(t, store2)
	after.Start()
	if got := waitStarted(t, store2); got != "s2" {
		t.Errorf("first upload after the restart = %q, want s2", got)
	}
}

// A superseded local copy stays on disk while a Run, Restore, Checkpoint, or
// PromoteSnapshot of the actor is in progress, since a Restore may be reading
// it in place. It is left for the actor's next checkpoint to prune.
func TestTrackerKeepsSupersededCopyWhileHeld(t *testing.T) {
	t.Run("upload ends", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		tr := newTestTracker(t, store)
		s1Dir := ateletpath.LocalSnapshotDir(trackedActorUID, "s1")
		if err := os.MkdirAll(s1Dir, 0o700); err != nil {
			t.Fatal(err)
		}
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
		done := startUpload(t, tr, store)
		tr.CommitLocal(trackedActorUID, testRef("s2", 2, tr.clock.Now()))

		end, err := tr.BeginStart(t.Context(), trackedActorUID)
		if err != nil {
			t.Fatalf("BeginStart: %v", err)
		}
		store.release <- nil
		waitClosed(t, done, "the upload of s1")
		end()
		if _, err := os.Stat(s1Dir); err != nil {
			t.Errorf("s1 after its upload ended during a Restore: stat = %v, want it kept", err)
		}
		if keep := tr.LocalSnapshotsToKeep(trackedActorUID); !slices.Equal(keep, []string{"s2"}) {
			t.Errorf("LocalSnapshotsToKeep = %v, want [s2], so the next checkpoint prunes s1", keep)
		}
	})

	t.Run("worker clears a superseded Uploading", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setErr(errors.New("boom"))
		tr := newIdleTestTracker(t, store)
		s1Dir := ateletpath.LocalSnapshotDir(trackedActorUID, "s1")
		if err := os.MkdirAll(s1Dir, 0o700); err != nil {
			t.Fatal(err)
		}
		taken := tr.clock.Now().Add(-testUploadDelay)
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, taken))
		processNext(t, tr, opUpload) // s1's upload fails, and s1 waits in Uploading to retry.
		tr.CommitDurable(trackedActorUID, testRef("s2", 2, tr.clock.Now()))

		end, err := tr.BeginStart(t.Context(), trackedActorUID)
		if err != nil {
			t.Fatalf("BeginStart: %v", err)
		}
		processNext(t, tr, opUpload)
		end()
		if rec := readSnapshotState(trackedActorUID); rec == nil || rec.Uploading != nil {
			t.Errorf("state file = %+v, want Uploading cleared", rec)
		}
		if _, err := os.Stat(s1Dir); err != nil {
			t.Errorf("s1 after a worker cleared it during a Restore: stat = %v, want it kept", err)
		}
	})
}

// Uploading is saved to the snapshot state file, so an atelet restart mid-upload
// resumes s1 unless a newer snapshot was already durable before the restart.
func TestTrackerResumesUploadingAfterRestart(t *testing.T) {
	t.Run("continues uploading across pause checkpoint", func(t *testing.T) {
		useActorDir(t)
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		ctx, cancel := context.WithCancel(t.Context())
		before := New(ctx, store, newStoreTracker(t, nil).clock, WithUploadDelay(testUploadDelay), WithDetachDelay(testDetachDelay))
		before.Start()

		t0 := before.clock.Now().Add(-testUploadDelay)
		before.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
		if got := waitStarted(t, store); got != "s1" {
			t.Fatalf("before started = %q, want s1", got)
		}
		s2 := testRef("s2", 2, t0.Add(time.Minute))
		s2.Snapshot.Object = nil
		before.CommitLocal(trackedActorUID, s2)
		cancel()
		if err := before.Wait(t.Context()); err != nil {
			t.Fatalf("before.Wait: %v", err)
		}

		store2 := newFakeSnapshotStore()
		after := newStoreTracker(t, store2)
		if got := uuidOf(after.State(trackedActorUID).GetUploading()); got != "s1" {
			t.Fatalf("uploading after restart = %q, want s1 from state file", got)
		}
		after.Start()
		if got := waitStarted(t, store2); got != "s1" {
			t.Fatalf("after started = %q, want s1", got)
		}
	})

	t.Run("continues uploading across uploadable local checkpoint", func(t *testing.T) {
		useActorDir(t)
		for _, name := range []string{"s1", "s2"} {
			if err := os.MkdirAll(ateletpath.LocalSnapshotDir(trackedActorUID, name), 0o700); err != nil {
				t.Fatal(err)
			}
		}
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		ctx, cancel := context.WithCancel(t.Context())
		before := New(ctx, store, newStoreTracker(t, nil).clock, WithUploadDelay(testUploadDelay), WithDetachDelay(testDetachDelay))
		before.Start()

		t0 := before.clock.Now().Add(-testUploadDelay)
		before.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
		if got := waitStarted(t, store); got != "s1" {
			t.Fatalf("before started = %q, want s1", got)
		}
		before.CommitLocal(trackedActorUID, testRef("s2", 2, t0.Add(time.Minute)))
		cancel()
		if err := before.Wait(t.Context()); err != nil {
			t.Fatalf("before.Wait: %v", err)
		}

		store2 := newFakeSnapshotStore()
		store2.setBlocking(true)
		after := newStoreTracker(t, store2)
		if got := uuidOf(after.State(trackedActorUID).GetUploading()); got != "s1" {
			t.Fatalf("uploading after restart = %q, want s1", got)
		}
		after.Start()
		if got := waitStarted(t, store2); got != "s1" {
			t.Fatalf("after started = %q, want s1 continued before s2", got)
		}
		done := opDone(t, after, trackedActorUID)
		store2.release <- nil
		waitClosed(t, done, "the resumed upload of s1")

		if _, err := os.Stat(ateletpath.LocalSnapshotDir(trackedActorUID, "s1")); !os.IsNotExist(err) {
			t.Errorf("s1 after resumed upload: stat = %v, want it deleted", err)
		}
		st := after.State(trackedActorUID)
		if uuidOf(st.GetLatestDurable()) != "s1" || st.GetUploading() != nil {
			t.Errorf("state after s1 finished = %v, want s1 durable and uploading nil", st)
		}
		if got, want := timeOf(st.GetUploadDueTime()), after.clock.Now().Add(testUploadDelay); !got.Equal(want) {
			t.Errorf("s2 upload_due_time = %v, want %v", got, want)
		}
	})

	t.Run("drops uploading when newer snapshot was already durable", func(t *testing.T) {
		useActorDir(t)
		s1Dir := ateletpath.LocalSnapshotDir(trackedActorUID, "s1")
		if err := os.MkdirAll(s1Dir, 0o700); err != nil {
			t.Fatal(err)
		}
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		ctx, cancel := context.WithCancel(t.Context())
		before := New(ctx, store, newStoreTracker(t, nil).clock, WithUploadDelay(testUploadDelay), WithDetachDelay(testDetachDelay))
		before.Start()

		t0 := before.clock.Now().Add(-testUploadDelay)
		before.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
		if got := waitStarted(t, store); got != "s1" {
			t.Fatalf("before started = %q, want s1", got)
		}
		before.CommitDurable(trackedActorUID, testRef("s2", 2, t0.Add(time.Minute)))
		// Capture the state file as a hard crash mid-upload leaves it.
		stateFile := ateletpath.ActorSnapshotStateFile(trackedActorUID)
		crashedState, err := os.ReadFile(stateFile)
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		if err := before.Wait(t.Context()); err != nil {
			t.Fatalf("before.Wait: %v", err)
		}
		if err := os.WriteFile(stateFile, crashedState, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(s1Dir, 0o700); err != nil {
			t.Fatal(err)
		}

		store2 := newFakeSnapshotStore()
		after := newStoreTracker(t, store2)
		if got := after.State(trackedActorUID).GetUploading(); got != nil {
			t.Errorf("uploading after restart = %v, want nil because s2 is durable", got)
		}
		setWorkers(after, 0, 0)
		after.Start()
		if got := after.queues[opDetach].Len(); got != 0 {
			t.Errorf("detach queue len after restart = %d, want 0", got)
		}
		processNext(t, after, opUpload)
		if got := store2.uploads(); len(got) != 0 {
			t.Errorf("uploads after restart = %v, want none", got)
		}
		if _, err := os.Stat(s1Dir); !os.IsNotExist(err) {
			t.Errorf("s1 after startup cleanup: stat = %v, want it deleted", err)
		}
	})
}

// TestTrackerUploadActsForTheActor checks that a background upload names the
// actor the snapshot was committed for, including an upload of Uploading that
// an atelet restart resumes from the snapshot state file.
func TestTrackerUploadActsForTheActor(t *testing.T) {
	t.Run("latest local", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		tr := newTestTracker(t, store)
		s1 := testRef("s1", 1, tr.clock.Now())
		s1.Actor = testActor
		tr.CommitLocal(trackedActorUID, s1)

		uploadNow(t, tr, store, nil)
		if got := store.actorOf("s1"); got != testActor {
			t.Errorf("upload acted for %+v, want %+v", got, testActor)
		}
	})

	t.Run("resumed after a restart", func(t *testing.T) {
		useActorDir(t)
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		ctx, cancel := context.WithCancel(t.Context())
		before := New(ctx, store, newStoreTracker(t, nil).clock, WithUploadDelay(testUploadDelay), WithDetachDelay(testDetachDelay))
		before.Start()

		t0 := before.clock.Now().Add(-testUploadDelay)
		s1 := testRef("s1", 1, t0)
		s1.Actor = testActor
		before.CommitLocal(trackedActorUID, s1)
		if got := waitStarted(t, store); got != "s1" {
			t.Fatalf("before started = %q, want s1", got)
		}
		// A pause snapshot no upload can make durable leaves s1 in Uploading,
		// so the resumed upload reads its actor from there.
		s2 := testRef("s2", 2, t0.Add(time.Minute))
		s2.Snapshot.Object = nil
		before.CommitLocal(trackedActorUID, s2)
		cancel()
		if err := before.Wait(t.Context()); err != nil {
			t.Fatalf("before.Wait: %v", err)
		}

		store2 := newFakeSnapshotStore()
		after := newStoreTracker(t, store2)
		after.Start()
		if got := waitStarted(t, store2); got != "s1" {
			t.Fatalf("after started = %q, want s1", got)
		}
		if got := store2.actorOf("s1"); got != testActor {
			t.Errorf("resumed upload acted for %+v, want %+v", got, testActor)
		}
	})
}

// A background upload that comes due while PromoteSnapshot holds the actor
// waits until the promote ends, then skips if the promote made the snapshot
// durable or runs if the promote failed.
func TestTrackerUploadWaitsForPromote(t *testing.T) {
	t.Run("promote succeeds", func(t *testing.T) {
		store := newFakeSnapshotStore()
		tr := newIdleTestTracker(t, store)
		s1 := testRef("s1", 1, tr.clock.Now())
		tr.CommitLocal(trackedActorUID, s1)

		durable, endPromote, err := tr.BeginPromote(t.Context(), trackedActorUID, s1)
		if err != nil || durable {
			t.Fatalf("BeginPromote = (%v, %v), want (false, nil)", durable, err)
		}
		stepTo(tr, tr.clock.Now().Add(testUploadDelay))
		processNext(t, tr, opUpload)
		if got := store.uploads(); len(got) != 0 {
			t.Fatalf("uploads during promote = %v, want none", got)
		}

		s1.UploadTime = tr.clock.Now()
		tr.CommitPromoted(trackedActorUID, s1)
		endPromote()
		if n := tr.queues[opUpload].Len(); n != 0 {
			processNext(t, tr, opUpload)
		}
		if got := store.uploads(); len(got) != 0 {
			t.Errorf("uploads after promote succeeded = %v, want none", got)
		}
	})

	t.Run("promote fails", func(t *testing.T) {
		store := newFakeSnapshotStore()
		tr := newIdleTestTracker(t, store)
		s1 := testRef("s1", 1, tr.clock.Now())
		tr.CommitLocal(trackedActorUID, s1)

		_, endPromote, err := tr.BeginPromote(t.Context(), trackedActorUID, s1)
		if err != nil {
			t.Fatalf("BeginPromote: %v", err)
		}
		stepTo(tr, tr.clock.Now().Add(testUploadDelay))
		processNext(t, tr, opUpload)
		if got := store.uploads(); len(got) != 0 {
			t.Fatalf("uploads during promote = %v, want none", got)
		}
		if n := tr.queues[opUpload].Len(); n != 0 {
			t.Fatalf("queued uploads during promote = %d, want none: the promote's end queues it", n)
		}

		endPromote()
		processNext(t, tr, opUpload)
		if got, want := store.uploads(), []string{"s1"}; !slices.Equal(got, want) {
			t.Errorf("uploads after the failed promote ended = %v, want %v", got, want)
		}
	})
}

// An out-of-order CommitLocal with a lower generation than Uploading does not
// replace a failed Uploading target, reset its backoff, or delete Uploading's
// local copy when it finishes.
func TestTrackerOlderLocalDoesNotSupersedeUploading(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	s2Dir := ateletpath.LocalSnapshotDir(trackedActorUID, "s2")
	if err := os.MkdirAll(s2Dir, 0o700); err != nil {
		t.Fatal(err)
	}
	t0 := tr.clock.Now()
	tr.CommitLocal(trackedActorUID, testRef("s2", 2, t0))

	// Fail s2 once, then record an older generation s1 as LatestLocal.
	uploadNow(t, tr, store, errors.New("boom"))
	retryAt := tr.clock.Now().Add(testRetryDelay)
	st := tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0.Add(time.Minute)))
	if got := uuidOf(st.GetUploading()); got != "s2" {
		t.Fatalf("uploading after older CommitLocal = %q, want s2 kept", got)
	}
	if got := timeOf(st.GetUploadDueTime()); !got.Equal(retryAt) {
		t.Errorf("upload_due_time after older CommitLocal = %v, want s2's retry at %v", got, retryAt)
	}

	// Retry s2 and let it succeed. Because LatestLocal has a lower generation,
	// s2 is not treated as superseded and its local directory stays intact.
	uploadNow(t, tr, store, nil)
	if got, want := store.uploads(), []string{"s2", "s2"}; !slices.Equal(got, want) {
		t.Errorf("uploads = %v, want %v", got, want)
	}
	if _, err := os.Stat(s2Dir); err != nil {
		t.Errorf("s2 local dir after upload: %v, want it kept", err)
	}
}

// A background upload that runs past the upload timeout is canceled, kept in
// Uploading, and retried once the retry delay has passed.
func TestTrackerBackgroundUploadTimeout(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	t0 := tr.clock.Now()
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))

	done := startUpload(t, tr, store)
	stepTo(tr, tr.clock.Now().Add(testUploadTimeout))
	waitClosed(t, done, "the timed-out upload")

	st := tr.State(trackedActorUID)
	if st.GetLatestDurable() != nil {
		t.Errorf("latest_durable after timeout = %v, want unset", st.GetLatestDurable())
	}
	if got := uuidOf(st.GetUploading()); got != "s1" {
		t.Errorf("uploading after timeout = %q, want s1 kept to retry", got)
	}
	if got, want := timeOf(st.GetUploadDueTime()), tr.clock.Now().Add(testRetryDelay); !got.Equal(want) {
		t.Errorf("upload_due_time after timeout = %v, want %v", got, want)
	}

	uploadNow(t, tr, store, nil)
	if got := uuidOf(tr.State(trackedActorUID).GetLatestDurable()); got != "s1" {
		t.Errorf("latest_durable after retry = %q, want s1", got)
	}
}

// A background upload, and the upload a background detach runs first, may run
// uploadTimeout plus the time the snapshot's local copy takes at
// uploadEstThroughput, so a large snapshot is not cut off.
func TestTrackerUploadTimeoutScalesWithSnapshotSize(t *testing.T) {
	// writeLocalCopy writes a local copy of s1 of testUploadEstThroughput
	// bytes, and returns how long an upload of it may run. The filesystem
	// decides how much space the copy takes, so that is measured: it takes at
	// least a second at testUploadEstThroughput.
	writeLocalCopy := func(t *testing.T) time.Duration {
		t.Helper()
		writeDenseFile(t, filepath.Join(ateletpath.LocalSnapshotDir(trackedActorUID, "s1"), "pages.img"), testUploadEstThroughput)
		size, err := localSnapshotSize(trackedActorUID, "s1")
		if err != nil {
			t.Fatalf("localSnapshotSize: %v", err)
		}
		scaled := testUploadTimeout + uploadAllowance(size, testUploadEstThroughput)
		if scaled < testUploadTimeout+time.Second {
			t.Fatalf("a local copy of %d bytes may upload for %v, want at least %v", size, scaled, testUploadTimeout+time.Second)
		}
		return scaled
	}

	t.Run("upload", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		tr := newTestTracker(t, store)
		scaled := writeLocalCopy(t)
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

		done := startUpload(t, tr, store)
		start := tr.clock.Now()
		if got := store.lastDeadline().Sub(start); got != scaled {
			t.Errorf("upload may run %v, want %v", got, scaled)
		}
		stepTo(tr, start.Add(scaled))
		waitClosed(t, done, "the timed-out upload")
	})

	t.Run("detach", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		tr := newIdleTestTracker(t, store)
		setWorkers(tr, 0, DefaultDetachWorkers)
		tr.Start()
		scaled := writeLocalCopy(t)
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

		stepTo(tr, detachDueOf(tr, trackedActorUID))
		waitStarted(t, store)
		if got := store.lastDeadline().Sub(tr.clock.Now()); got != scaled {
			t.Errorf("the detach's upload may run %v, want %v", got, scaled)
		}
	})

	t.Run("no local copy", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		tr := newTestTracker(t, store)
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

		startUpload(t, tr, store)
		if got := store.lastDeadline().Sub(tr.clock.Now()); got != testUploadTimeout {
			t.Errorf("upload may run %v, want %v", got, testUploadTimeout)
		}
	})

	t.Run("zero throughput adds nothing", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		useActorDir(t)
		tr := newStoreTracker(t, store, WithUploadEstThroughput(0))
		tr.Start()
		writeLocalCopy(t)
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

		startUpload(t, tr, store)
		if got := store.lastDeadline().Sub(tr.clock.Now()); got != testUploadTimeout {
			t.Errorf("upload may run %v, want %v", got, testUploadTimeout)
		}
	})
}

// Each consecutive failure of an actor's background work doubles its next
// upload attempt's timeout, to at most 8 times the first's, so an upload
// slower than the estimated throughput still finishes. Once the actor's work
// succeeds, its next upload gets the first attempt's timeout again.
func TestTrackerUploadTimeoutGrowsWithFailures(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newIdleTestTracker(t, store)
	// Without detach workers, no detach runs between the attempts.
	setWorkers(tr, DefaultUploadWorkers, 0)
	tr.Start()
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

	for attempt, factor := range []time.Duration{1, 2, 4, 8, 8} {
		done := startUpload(t, tr, store)
		start := tr.clock.Now()
		want := factor * testUploadTimeout
		if got := store.lastDeadline().Sub(start); got != want {
			t.Fatalf("attempt %d may run %v, want %v", attempt+1, got, want)
		}
		stepTo(tr, start.Add(want))
		waitClosed(t, done, "the timed-out upload")
	}

	uploadNow(t, tr, store, nil)
	tr.CommitLocal(trackedActorUID, testRef("s2", 2, tr.clock.Now()))
	startUpload(t, tr, store)
	if got := store.lastDeadline().Sub(tr.clock.Now()); got != testUploadTimeout {
		t.Errorf("the upload after a success may run %v, want %v", got, testUploadTimeout)
	}
}

// A timeout too long to double saturates rather than wrapping negative, which
// would time the upload out at once.
func TestUploadTimeoutForSaturates(t *testing.T) {
	useActorDir(t)
	tr := newStoreTracker(t, newFakeSnapshotStore(), WithUploadTimeout(math.MaxInt64))
	for range maxUploadTimeoutDoublings {
		tr.retries.When(trackedActorUID)
	}
	if got := tr.uploadTimeoutFor(t.Context(), trackedActorUID, testRef("s1", 1, tr.clock.Now())); got < math.MaxInt64/2 {
		t.Errorf("uploadTimeoutFor = %v, want it to saturate near the longest time.Duration", got)
	}
}

// uploadAllowance rounds the time a snapshot takes to upload up to a whole
// second, and allows none for an empty snapshot or a throughput of zero or
// less.
func TestUploadAllowance(t *testing.T) {
	const mib = 1 << 20
	for _, tc := range []struct {
		size, bytesPerSecond int64
		want                 time.Duration
	}{
		{size: 0, bytesPerSecond: 100 * mib, want: 0},
		{size: 100 * mib, bytesPerSecond: 0, want: 0},
		{size: 100 * mib, bytesPerSecond: -1, want: 0},
		{size: 1, bytesPerSecond: 100 * mib, want: time.Second},
		{size: 100 * mib, bytesPerSecond: 100 * mib, want: time.Second},
		{size: 100*mib + 1, bytesPerSecond: 100 * mib, want: 2 * time.Second},
		// 102.4 seconds.
		{size: 10 << 30, bytesPerSecond: 100 * mib, want: 103 * time.Second},
		// Too large to multiply by time.Second before dividing.
		{size: 1 << 40, bytesPerSecond: mib, want: mib * time.Second},
	} {
		if got := uploadAllowance(tc.size, tc.bytesPerSecond); got != tc.want {
			t.Errorf("uploadAllowance(%d, %d) = %v, want %v", tc.size, tc.bytesPerSecond, got, tc.want)
		}
	}
}
