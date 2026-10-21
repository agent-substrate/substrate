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
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// eventually polls cond until it holds, and fails the test if it does not
// within 10 seconds.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// detachDueOf returns when atelet detaches the actor, or zero if it does not.
func detachDueOf(tr *Tracker, actorUID string) time.Time {
	tr.lock(actorUID)
	defer tr.unlock(actorUID)
	if a := tr.findLocked(actorUID); a != nil {
		due, _ := a.detachDue()
		return due
	}
	return time.Time{}
}

// detachNow advances the clock to when the tracked actor's detach is due, and
// waits for a worker to detach it.
func detachNow(t *testing.T, tr *Tracker) {
	t.Helper()
	due := detachDueOf(tr, trackedActorUID)
	if due.IsZero() {
		t.Fatal("no detach scheduled")
	}
	stepTo(tr, due)
	eventually(t, "the actor is detached", func() bool {
		tr.lock(trackedActorUID)
		defer tr.unlock(trackedActorUID)
		a := tr.findLocked(trackedActorUID)
		return a != nil && a.op == nil && a.LatestLocal == nil
	})
}

// startDetach runs a Detach of the tracked actor in the background, and
// returns a channel that receives its error.
func startDetach(t *testing.T, tr *Tracker) <-chan error {
	t.Helper()
	detached := make(chan error, 1)
	go func() {
		_, err := tr.Detach(t.Context(), trackedActorUID)
		detached <- err
	}()
	return detached
}

// A detach makes the latest local snapshot durable and drops it, once any
// upload in progress has ended, and is a no-op when atelet holds no local
// snapshot for the actor.
func TestTrackerDetach(t *testing.T) {
	t.Run("requested", func(t *testing.T) {
		store := newFakeSnapshotStore()
		tr := newTestTracker(t, store)
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

		st, err := tr.Detach(t.Context(), trackedActorUID)
		if err != nil {
			t.Fatalf("detach: %v", err)
		}
		if got, wantCalls := store.uploads(), []string{"s1"}; !slices.Equal(got, wantCalls) {
			t.Errorf("uploads = %v, want %v", got, wantCalls)
		}
		if st.GetLatestLocal() != nil {
			t.Errorf("state = %v, want no local snapshot", st)
		}
		durable := st.GetLatestDurable()
		if got := durable.GetSnapshot().GetFidelity(); got != ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY {
			t.Errorf("latest_durable fidelity = %v, want the MEMORY it holds", got)
		}
		if durable.GetDurableCompleteTime() == nil || durable.GetLocalCommitTime() == nil {
			t.Errorf("latest_durable = %v, want its commit and upload times", durable)
		}
		if !detachDueOf(tr, trackedActorUID).IsZero() {
			t.Error("a detach is scheduled after a detach")
		}
	})

	t.Run("no-op when no local snapshot", func(t *testing.T) {
		store := newFakeSnapshotStore()
		tr := newTestTracker(t, store)
		st, err := tr.Detach(t.Context(), trackedActorUID)
		if err != nil {
			t.Fatalf("detach of untracked actor: %v", err)
		}
		if !proto.Equal(st, &ateletpb.ActorSnapshotState{}) {
			t.Errorf("state = %v, want empty", st)
		}
		if got := store.uploads(); len(got) != 0 {
			t.Errorf("uploads = %v, want none", got)
		}
	})

	t.Run("by the timer", func(t *testing.T) {
		store := newFakeSnapshotStore()
		tr := newIdleTestTracker(t, store)
		setWorkers(tr, 0, DefaultDetachWorkers)
		tr.Start()
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
		// With no upload workers, the detach runs and makes s1 durable itself.
		detachNow(t, tr)
		if got, want := store.uploads(), []string{"s1"}; !slices.Equal(got, want) {
			t.Errorf("uploads = %v, want %v", got, want)
		}
		st := tr.State(trackedActorUID)
		if st.GetLatestLocal() != nil || uuidOf(st.GetLatestDurable()) != "s1" {
			t.Errorf("state = %v, want s1 durable and nothing local", st)
		}
	})

	t.Run("already durable, as after an immediate checkpoint", func(t *testing.T) {
		store := newFakeSnapshotStore()
		tr := newTestTracker(t, store)
		local := testRef("s1", 1, tr.clock.Now())
		durable := local
		durable.UploadTime = local.CommitTime.Add(time.Second)
		tr.CommitDurable(trackedActorUID, durable)
		tr.CommitLocal(trackedActorUID, local)
		detachNow(t, tr)
		if got := store.uploads(); len(got) != 0 {
			t.Errorf("uploads = %v, want none for an already durable snapshot", got)
		}
		st := tr.State(trackedActorUID)
		if st.GetLatestLocal() != nil || uuidOf(st.GetLatestDurable()) != "s1" {
			t.Errorf("state = %v, want s1 durable and nothing local", st)
		}
		if got := timeOf(st.GetLatestDurable().GetDurableCompleteTime()); !got.Equal(durable.UploadTime) {
			t.Errorf("durable_complete_time = %v, want %v from the checkpoint", got, durable.UploadTime)
		}
	})

	t.Run("already durable, as after a promote", func(t *testing.T) {
		store := newFakeSnapshotStore()
		tr := newTestTracker(t, store)
		local := testRef("s1", 1, tr.clock.Now())
		tr.CommitLocal(trackedActorUID, local)

		promoted := testRef("s1", 1, time.Time{})
		promoted.UploadTime = local.CommitTime.Add(time.Minute)
		tr.CommitPromoted(trackedActorUID, promoted)

		detachNow(t, tr)
		if got := store.uploads(); len(got) != 0 {
			t.Errorf("uploads = %v, want none for a promoted snapshot", got)
		}
		st := tr.State(trackedActorUID)
		if st.GetLatestLocal() != nil || uuidOf(st.GetLatestDurable()) != "s1" {
			t.Errorf("state = %v, want s1 durable and nothing local", st)
		}
		if got := timeOf(st.GetLatestDurable().GetDurableCompleteTime()); !got.Equal(promoted.UploadTime) {
			t.Errorf("durable_complete_time = %v, want %v from the promote", got, promoted.UploadTime)
		}
	})

	t.Run("by the timer after the upload", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		tr := newTestTracker(t, store)
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
		uploadNow(t, tr, store, nil)
		uploadTime := tr.State(trackedActorUID).GetLatestDurable().GetDurableCompleteTime()

		detachNow(t, tr)
		if got := len(store.uploads()); got != 1 {
			t.Errorf("%d store calls, want only the background upload's", got)
		}
		st := tr.State(trackedActorUID)
		if st.GetLatestLocal() != nil || !proto.Equal(st.GetLatestDurable().GetDurableCompleteTime(), uploadTime) {
			t.Errorf("state = %v, want nothing local and s1 durable since %v", st, uploadTime)
		}
	})

	t.Run("waits for the upload of its snapshot", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		tr := newTestTracker(t, store)
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
		startUpload(t, tr, store)
		detached := startDetach(t, tr)

		store.release <- nil
		if err := <-detached; err != nil {
			t.Fatalf("detach: %v", err)
		}
		if got := len(store.uploads()); got != 1 {
			t.Errorf("%d store calls, want only the in-progress upload's", got)
		}
		if got := tr.State(trackedActorUID).GetLatestDurable(); got.GetDurableCompleteTime() == nil {
			t.Errorf("latest_durable = %v, want the background upload's time", got)
		}
	})

	t.Run("waits for an older snapshot's upload", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		tr := newTestTracker(t, store)
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
		startUpload(t, tr, store)
		tr.CommitLocal(trackedActorUID, testRef("s2", 2, tr.clock.Now()))
		store.setBlocking(false)
		detached := startDetach(t, tr)
		select {
		case err := <-detached:
			t.Fatalf("detach returned (%v) during the upload of s1", err)
		case <-time.After(50 * time.Millisecond):
		}

		store.release <- nil
		if err := <-detached; err != nil {
			t.Fatalf("detach: %v", err)
		}
		if got, want := store.uploads(), []string{"s1", "s2"}; !slices.Equal(got, want) {
			t.Errorf("uploads = %v, want %v", got, want)
		}
		st := tr.State(trackedActorUID)
		if st.GetLatestLocal() != nil || uuidOf(st.GetLatestDurable()) != "s2" {
			t.Errorf("state = %v, want s2 durable and nothing local", st)
		}
	})

	// A refused detach keeps the local snapshot and uploads nothing.
	for _, tc := range []struct {
		name        string
		modifyLocal func(*SnapshotRef)
		setup       func(t *testing.T, tr *Tracker)
		wantCode    codes.Code
	}{
		{
			name: "refused after a start",
			setup: func(t *testing.T, tr *Tracker) {
				end, err := tr.BeginStart(t.Context(), trackedActorUID)
				if err != nil {
					t.Fatalf("beginStart: %v", err)
				}
				end()
			},
			wantCode: codes.Aborted,
		},
		{
			name: "refused while a start is in progress",
			setup: func(t *testing.T, tr *Tracker) {
				end, err := tr.BeginStart(t.Context(), trackedActorUID)
				if err != nil {
					t.Fatalf("beginStart: %v", err)
				}
				t.Cleanup(end)
			},
			wantCode: codes.Aborted,
		},
		{
			name: "refused when durable generation mismatches local",
			setup: func(t *testing.T, tr *Tracker) {
				tr.CommitDurable(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
				tr.CommitLocal(trackedActorUID, testRef("s1", 2, tr.clock.Now()))
			},
			wantCode: codes.FailedPrecondition,
		},
		{
			name: "refused when durable URI mismatches local",
			modifyLocal: func(local *SnapshotRef) {
				local.Snapshot.Object.SnapshotUri += "-other"
			},
			setup: func(t *testing.T, tr *Tracker) {
				tr.CommitDurable(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
			},
			wantCode: codes.FailedPrecondition,
		},
		{
			name: "refused without an object storage location",
			modifyLocal: func(local *SnapshotRef) {
				local.Snapshot.Object = nil
			},
			wantCode: codes.FailedPrecondition,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeSnapshotStore()
			tr := newTestTracker(t, store)
			local := testRef("s1", 1, tr.clock.Now())
			if tc.modifyLocal != nil {
				tc.modifyLocal(&local)
			}
			tr.CommitLocal(trackedActorUID, local)
			if tc.setup != nil {
				tc.setup(t, tr)
			}
			if _, err := tr.Detach(t.Context(), trackedActorUID); apierror.Code(err) != tc.wantCode {
				t.Errorf("detach = %v, want %v", err, tc.wantCode)
			}
			if got := store.uploads(); len(got) != 0 {
				t.Errorf("uploads = %v, want none", got)
			}
			if got := uuidOf(tr.State(trackedActorUID).GetLatestLocal()); got != "s1" {
				t.Errorf("latest_local = %q, want s1 kept", got)
			}
		})
	}
}

// A local snapshot no detach can make durable, as a pause takes today, never
// comes due for a background detach, rather than failing it again and again.
// A checkpoint that replaces it schedules a detach again.
func TestTrackerSkipsUndetachableSnapshots(t *testing.T) {
	for _, tc := range []struct {
		name   string
		commit func(tr *Tracker)
	}{
		{
			name: "no object storage location",
			commit: func(tr *Tracker) {
				local := testRef("s1", 1, tr.clock.Now())
				local.Snapshot.Object = nil
				tr.CommitLocal(trackedActorUID, local)
			},
		},
		{
			name: "durable elsewhere",
			commit: func(tr *Tracker) {
				tr.CommitDurable(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
				local := testRef("s1", 1, tr.clock.Now())
				local.Snapshot.Object.SnapshotUri += "-other"
				tr.CommitLocal(trackedActorUID, local)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeSnapshotStore()
			tr := newTestTracker(t, store)
			tc.commit(tr)
			if due := detachDueOf(tr, trackedActorUID); !due.IsZero() {
				t.Fatalf("detach due at %v, want none", due)
			}

			tr.CommitLocal(trackedActorUID, testRef("s2", 2, tr.clock.Now()))
			detachNow(t, tr)
			if got, want := store.uploads(), []string{"s2"}; !slices.Equal(got, want) {
				t.Errorf("uploads = %v, want %v", got, want)
			}
		})
	}
}

// BeginStart unschedules the actor's detach, and saves that, before the start
// begins, so neither a restart during the start nor a failed start brings the
// detach back: only the actor's next checkpoint schedules one again. While a
// Checkpoint is in progress, a worker that finds the detach due skips it
// without changing DetachAt or queuing the actor again. The end of the
// checkpoint queues the actor again, and the detach runs then.
func TestTrackerDetachTimer(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newIdleTestTracker(t, store)
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

	endStart, err := tr.BeginStart(t.Context(), trackedActorUID)
	if err != nil {
		t.Fatalf("beginStart: %v", err)
	}
	if due := detachDueOf(newTracker(t), trackedActorUID); !due.IsZero() {
		t.Fatalf("detachDue on a fresh tracker during the start = %v, want zero from the state file", due)
	}
	endStart()
	if due := detachDueOf(tr, trackedActorUID); !due.IsZero() {
		t.Fatalf("detachDue after the start = %v, want zero whether or not it succeeded", due)
	}

	tr.CommitLocal(trackedActorUID, testRef("s2", 2, tr.clock.Now()))
	due := detachDueOf(tr, trackedActorUID)
	_, endCheckpoint, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, testRef("s3", 3, time.Time{}).Snapshot, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
	if err != nil {
		t.Fatalf("beginCheckpoint: %v", err)
	}
	stepTo(tr, due)
	processNext(t, tr, opDetach)
	if got := detachDueOf(tr, trackedActorUID); !got.Equal(due) {
		t.Errorf("detachDue after a worker found the actor held = %v, want %v kept", got, due)
	}
	if n := tr.queues[opDetach].Len(); n != 0 {
		t.Errorf("actors queued for a detach while the actor is held = %d, want none until the hold ends", n)
	}
	if got := uuidOf(tr.State(trackedActorUID).GetLatestLocal()); got != "s2" {
		t.Fatalf("latest_local = %q while the actor is held, want s2 kept", got)
	}

	endCheckpoint()
	processNext(t, tr, opDetach)
	if got := tr.State(trackedActorUID).GetLatestLocal(); got != nil {
		t.Errorf("latest_local = %v once the checkpoint ended, want unset", got)
	}
	if got, want := store.uploads(), []string{"s2"}; !slices.Equal(got, want) {
		t.Errorf("uploads = %v, want %v", got, want)
	}
}

// A start that cannot save that it began fails before anything starts, and
// leaves the detach scheduled and the actor not started.
func TestTrackerStartFailsIfItCannotSave(t *testing.T) {
	tr := newIdleTestTracker(t, newFakeSnapshotStore())
	stateFile := ateletpath.ActorSnapshotStateFile(trackedActorUID)
	// A directory where the state file goes makes every save fail, so the
	// entry stays in memory.
	if err := os.MkdirAll(stateFile, 0o700); err != nil {
		t.Fatal(err)
	}
	s1 := testRef("s1", 1, tr.clock.Now())
	tr.CommitLocal(trackedActorUID, s1)
	due := detachDueOf(tr, trackedActorUID)
	if due.IsZero() {
		t.Fatal("no detach scheduled after the checkpoint")
	}

	if _, err := tr.BeginStart(t.Context(), trackedActorUID); apierror.Code(err) != codes.Unavailable {
		t.Fatalf("beginStart = %v, want Unavailable", err)
	}
	if got := detachDueOf(tr, trackedActorUID); !got.Equal(due) {
		t.Errorf("detachDue after the failed start = %v, want %v kept", got, due)
	}
	if !tr.NotStartedSince(trackedActorUID, s1.Snapshot) {
		t.Error("NotStartedSince after the failed start = false, want true")
	}
	if hold := tr.actors[trackedActorUID].hold; hold != holdNone {
		t.Errorf("hold after the failed start = %v, want none", hold)
	}

	// Once saves work again, the start goes ahead.
	if err := os.Remove(stateFile); err != nil {
		t.Fatal(err)
	}
	end, err := tr.BeginStart(t.Context(), trackedActorUID)
	if err != nil {
		t.Fatalf("beginStart once saves work: %v", err)
	}
	end()
	if due := detachDueOf(newTracker(t), trackedActorUID); !due.IsZero() {
		t.Errorf("detachDue from the state file = %v, want zero", due)
	}
}

// A restore or a checkpoint that arrives during a detach waits for it to end.
func TestTrackerWaitsForDetach(t *testing.T) {
	for _, tc := range []struct {
		name  string
		begin func(tr *Tracker, ctx context.Context, uid string) (func(), error)
	}{
		{
			name: "BeginStart",
			begin: func(tr *Tracker, ctx context.Context, uid string) (func(), error) {
				return tr.BeginStart(ctx, uid)
			},
		},
		{
			name: "BeginCheckpoint",
			begin: func(tr *Tracker, ctx context.Context, uid string) (func(), error) {
				_, end, err := tr.BeginCheckpoint(ctx, uid, testRef("s2", 2, time.Time{}).Snapshot, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
				return end, err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeSnapshotStore()
			store.setBlocking(true)
			tr := newTestTracker(t, store)
			tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
			detachResult := startDetach(t, tr)
			waitStarted(t, store)

			result := make(chan error, 1)
			go func() {
				end, err := tc.begin(tr, t.Context(), trackedActorUID)
				if err == nil {
					end()
				}
				result <- err
			}()
			select {
			case err := <-result:
				t.Fatalf("%s returned (%v) during a detach", tc.name, err)
			case <-time.After(50 * time.Millisecond):
			}

			store.release <- nil
			for name, ch := range map[string]<-chan error{"detach": detachResult, tc.name: result} {
				select {
				case err := <-ch:
					if err != nil {
						t.Errorf("%s: %v", name, err)
					}
				case <-time.After(10 * time.Second):
					t.Fatalf("%s did not return after the detach", name)
				}
			}
			if got := tr.State(trackedActorUID).GetLatestLocal(); got != nil {
				t.Errorf("latest_local = %v after the detach, want unset", got)
			}
		})
	}
}

// BeginCheckpoint and BeginPromote fail at once during a detach when the
// requested snapshot is already stale.
func TestTrackerDetachWaitChecksGeneration(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)
	tr.CommitLocal(trackedActorUID, testRef("s1", 2, tr.clock.Now()))
	detached := startDetach(t, tr)
	waitStarted(t, store)

	stale := testRef("s0", 1, time.Time{})
	returnsPromptly(t, "BeginCheckpoint of stale generation during detach", func() {
		if _, _, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, stale.Snapshot, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); apierror.Code(err) != codes.Aborted {
			t.Errorf("BeginCheckpoint = %v, want Aborted", err)
		}
	})
	returnsPromptly(t, "BeginPromote of stale generation during detach", func() {
		if _, _, err := tr.BeginPromote(t.Context(), trackedActorUID, stale); apierror.Code(err) != codes.Aborted {
			t.Errorf("BeginPromote = %v, want Aborted", err)
		}
	})

	store.release <- nil
	if err := <-detached; err != nil {
		t.Fatalf("detach: %v", err)
	}
}

// A failed detach keeps the local snapshot, and is retried no sooner than the
// retry delay after it, nor than it was due. The upload is held back the retry
// delay too.
func TestTrackerFailedDetach(t *testing.T) {
	t.Run("requested", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setErr(errors.New("boom"))
		tr := newIdleTestTracker(t, store)
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now().Add(-testUploadDelay)))
		due := detachDueOf(tr, trackedActorUID)
		if _, err := tr.Detach(t.Context(), trackedActorUID); err == nil {
			t.Fatal("detach succeeded, want the store's error")
		}
		retryAt := tr.clock.Now().Add(testRetryDelay)
		st := tr.State(trackedActorUID)
		if uuidOf(st.GetLatestLocal()) != "s1" || !timeOf(st.GetUploadDueTime()).Equal(retryAt) {
			t.Errorf("state = %v, want s1 kept and its upload due at %v", st, retryAt)
		}
		// testRetryDelay is shorter than testDetachDelay.
		if got := detachDueOf(tr, trackedActorUID); !got.Equal(due) {
			t.Errorf("detach due at %v after the failed one, want %v as scheduled", got, due)
		}
	})

	t.Run("background", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setErr(errors.New("boom"))
		tr := newIdleTestTracker(t, store)
		setWorkers(tr, 0, DefaultDetachWorkers)
		tr.Start()
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
		due := detachDueOf(tr, trackedActorUID)
		stepTo(tr, due)
		retryAt := due.Add(testRetryDelay)
		eventually(t, "the failed detach is retried later", func() bool {
			return detachDueOf(tr, trackedActorUID).Equal(retryAt)
		})
		if got := uuidOf(tr.State(trackedActorUID).GetLatestLocal()); got != "s1" {
			t.Errorf("latest_local = %q after the failed detach, want s1 kept", got)
		}

		store.setErr(nil)
		detachNow(t, tr)
		if got := tr.State(trackedActorUID).GetLatestLocal(); got != nil {
			t.Errorf("latest_local = %v after the retried detach, want unset", got)
		}
	})
}

// A detach whose prune fails keeps LatestLocal, now durable, and is retried:
// the retry finds the snapshot durable and only prunes.
func TestTrackerDetachRetriesFailedPrune(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newTestTracker(t, store)
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	// A file where the local checkpoint directory belongs fails the prune.
	if err := os.WriteFile(ateletpath.LocalCheckpointsDir(trackedActorUID), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Detach(t.Context(), trackedActorUID); err == nil {
		t.Fatal("detach succeeded, want the prune's error")
	}
	if st := tr.State(trackedActorUID); uuidOf(st.GetLatestLocal()) != "s1" || uuidOf(st.GetLatestDurable()) != "s1" {
		t.Errorf("state = %v, want s1 still local and now durable", st)
	}

	if err := os.Remove(ateletpath.LocalCheckpointsDir(trackedActorUID)); err != nil {
		t.Fatal(err)
	}
	detachNow(t, tr)
	if got, want := store.uploads(), []string{"s1"}; !slices.Equal(got, want) {
		t.Errorf("uploads = %v, want %v, as the retry only prunes", got, want)
	}
	if got := tr.State(trackedActorUID).GetLatestLocal(); got != nil {
		t.Errorf("latest_local = %v after the retried detach, want unset", got)
	}
}

// Detach fails on a tracker without a store, rather than uploading through it.
func TestTrackerDetachWithoutStore(t *testing.T) {
	useActorDir(t)
	tr := newTracker(t)
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	if _, err := tr.Detach(t.Context(), trackedActorUID); apierror.Code(err) != codes.Unimplemented {
		t.Errorf("detach = %v, want Unimplemented", err)
	}
	if got := uuidOf(tr.State(trackedActorUID).GetLatestLocal()); got != "s1" {
		t.Errorf("latest_local = %q, want s1 kept", got)
	}
}

// A Checkpoint holds off detaches even when atelet tracked nothing of the
// actor as it began, as on the actor's first checkpoint on this node.
func TestTrackerHoldsFirstCheckpoint(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newIdleTestTracker(t, store)
	s1 := testRef("s1", 1, tr.clock.Now())
	_, end, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, s1.Snapshot, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
	if err != nil {
		t.Fatalf("beginCheckpoint: %v", err)
	}
	tr.CommitLocal(trackedActorUID, s1)
	stepTo(tr, detachDueOf(tr, trackedActorUID))
	if run := tr.beginDueWork(trackedActorUID, opDetach); run != nil {
		t.Error("a due detach began during the checkpoint, want it held off")
		run()
	}
	end()
	run := tr.beginDueWork(trackedActorUID, opDetach)
	if run == nil {
		t.Fatal("no detach began after the checkpoint, want the due detach")
	}
	run()
}

// A start when no upload is due, or a checkpoint replacing the local snapshot,
// lifts the backoff of the actor's failed detaches, which have nothing left to
// retry, so the entry leaves memory.
func TestTrackerLiftsBackoffWithDetach(t *testing.T) {
	for name, drop := range map[string]func(t *testing.T, tr *Tracker){
		"start": func(t *testing.T, tr *Tracker) {
			end, err := tr.BeginStart(t.Context(), trackedActorUID)
			if err != nil {
				t.Fatalf("beginStart: %v", err)
			}
			end()
		},
		"checkpoint": func(t *testing.T, tr *Tracker) {
			tr.CommitLocal(trackedActorUID, testRef("s2", 2, tr.clock.Now()))
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := newFakeSnapshotStore()
			tr := newTestTracker(t, store)
			s1 := testRef("s1", 1, tr.clock.Now())
			tr.CommitDurable(trackedActorUID, s1)
			tr.CommitLocal(trackedActorUID, s1)
			// A file where the local checkpoint directory belongs fails the prune.
			if err := os.WriteFile(ateletpath.LocalCheckpointsDir(trackedActorUID), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := tr.Detach(t.Context(), trackedActorUID); err == nil {
				t.Fatal("detach succeeded, want the prune's error")
			}
			if !inMemory(tr) {
				t.Fatal("entry not in memory after a failed detach, want it kept for its backoff")
			}
			drop(t, tr)
			if inMemory(tr) {
				t.Error("entry in memory, want it dropped with no backoff left")
			}
		})
	}
}

// Forget cancels a detach in progress and waits for it to end, leaving the
// forgotten actor's local snapshots for Terminate to prune.
func TestTrackerForgetCancelsDetach(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	store.setIgnoreCancel(true)
	tr := newTestTracker(t, store)
	if err := os.MkdirAll(ateletpath.LocalSnapshotDir(trackedActorUID, "s1"), 0o700); err != nil {
		t.Fatal(err)
	}
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	detached := startDetach(t, tr)
	waitStarted(t, store)

	forgetDone := make(chan error, 1)
	go func() {
		forgetDone <- tr.Forget(trackedActorUID)
	}()
	if got := waitCanceled(t, store); got != "s1" {
		t.Fatalf("canceled detach upload = %q, want s1", got)
	}
	select {
	case err := <-forgetDone:
		t.Fatalf("Forget returned (%v) before the canceled detach ended", err)
	case <-time.After(20 * time.Millisecond):
	}
	store.release <- context.Canceled
	if err := <-detached; !errors.Is(err, context.Canceled) {
		t.Errorf("detach = %v, want %v", err, context.Canceled)
	}
	select {
	case err := <-forgetDone:
		if err != nil {
			t.Errorf("Forget: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for Forget to return")
	}
	if st := tr.State(trackedActorUID); !proto.Equal(st, &ateletpb.ActorSnapshotState{}) {
		t.Errorf("state after forget = %v, want empty", st)
	}
	if _, err := os.Stat(ateletpath.LocalSnapshotDir(trackedActorUID, "s1")); err != nil {
		t.Errorf("s1 after the cancelled detach: %v, want it left for Terminate to prune", err)
	}
}

// Detach runs in the calling goroutine, so it does not wait behind other
// actors' background work, and a worker that takes the actor afterwards finds
// nothing left to do.
func TestTrackerForegroundDetachBypassesQueue(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newIdleTestTracker(t, store)
	setWorkers(tr, 1, 0)
	tr.Start()
	for _, id := range []string{"actor-1", "actor-2", "actor-3"} {
		if err := os.MkdirAll(ateletpath.ActorPath(id), 0o700); err != nil {
			t.Fatal(err)
		}
	}

	taken := tr.clock.Now().Add(-testUploadDelay)
	tr.CommitLocal("actor-1", testRef("a1-s1", 1, taken))
	if got := waitStarted(t, store); got != "a1-s1" {
		t.Fatalf("started = %q, want a1-s1", got)
	}
	// actor-2's upload is due too, but the only worker is busy.
	tr.CommitLocal("actor-2", testRef("a2-s1", 1, taken))

	store.setBlocking(false)
	st, err := tr.Detach(t.Context(), "actor-2")
	if err != nil {
		t.Fatalf("detach of actor-2: %v", err)
	}
	if got := waitStarted(t, store); got != "a2-s1" {
		t.Fatalf("started = %q, want a2-s1", got)
	}
	if st.GetLatestLocal() != nil || uuidOf(st.GetLatestDurable()) != "a2-s1" {
		t.Errorf("actor-2 state = %v, want detached with a2-s1 durable", st)
	}

	done1 := opDone(t, tr, "actor-1")
	store.release <- nil
	waitClosed(t, done1, "actor-1's upload")
	// actor-3 is queued behind actor-2, so its upload starts only once the
	// worker has taken actor-2.
	tr.CommitLocal("actor-3", testRef("a3-s1", 1, taken))
	if got := waitStarted(t, store); got != "a3-s1" {
		t.Fatalf("started = %q, want a3-s1", got)
	}
	wantCalls := []string{"a1-s1", "a2-s1", "a3-s1"}
	if got := store.uploads(); !slices.Equal(got, wantCalls) {
		t.Errorf("uploads = %v, want %v", got, wantCalls)
	}
}

// The queue holds an actor until the earliest time it was queued for. A worker
// that takes the actor before its detach is due, as after a later checkpoint
// moved the detach out, leaves the actor's local snapshot and queues it again
// for when its detach is due. Both snapshots are durable, so no upload is due
// either.
func TestTrackerRequeuesUntilDetachIsDue(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newIdleTestTracker(t, store)
	t0 := tr.clock.Now()
	commitImmediate := func(ref SnapshotRef) {
		tr.CommitDurable(trackedActorUID, ref)
		tr.CommitLocal(trackedActorUID, ref)
	}
	commitImmediate(testRef("s1", 1, t0))
	stepTo(tr, t0.Add(time.Hour))
	commitImmediate(testRef("s2", 2, tr.clock.Now()))

	stepTo(tr, t0.Add(testDetachDelay))
	processNext(t, tr, opDetach)
	if got := uuidOf(tr.State(trackedActorUID).GetLatestLocal()); got != "s2" {
		t.Errorf("latest_local = %q before the detach is due, want s2", got)
	}
	if got := store.uploads(); len(got) != 0 {
		t.Errorf("uploads = %v, want none: both snapshots are durable", got)
	}

	stepTo(tr, t0.Add(time.Hour+testDetachDelay))
	processNext(t, tr, opDetach)
	if got := tr.State(trackedActorUID).GetLatestLocal(); got != nil {
		t.Errorf("latest_local = %v once the detach was due, want unset", got)
	}
}

// cancelingStore is a Store whose upload succeeds just after cancel ends the
// detach's context, as Forget or the Detach caller can.
type cancelingStore struct{ cancel context.CancelFunc }

func (s *cancelingStore) UploadLocalSnapshot(context.Context, string, SnapshotRef) (bool, error) {
	s.cancel()
	return true, nil
}

// A detach whose context ends as its upload succeeds records the upload, but
// deletes no local snapshot, and fails with the context's error.
func TestTrackerDetachDoesNotPruneOnceCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	tr := newIdleTestTracker(t, &cancelingStore{cancel: cancel})
	if err := os.MkdirAll(ateletpath.LocalSnapshotDir(trackedActorUID, "s1"), 0o700); err != nil {
		t.Fatal(err)
	}
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

	if _, err := tr.Detach(ctx, trackedActorUID); !errors.Is(err, context.Canceled) {
		t.Fatalf("detach = %v, want %v", err, context.Canceled)
	}
	if _, err := os.Stat(ateletpath.LocalSnapshotDir(trackedActorUID, "s1")); err != nil {
		t.Errorf("s1 after the cancelled detach: %v, want it kept", err)
	}
	if st := tr.State(trackedActorUID); uuidOf(st.GetLatestLocal()) != "s1" || uuidOf(st.GetLatestDurable()) != "s1" {
		t.Errorf("state = %v, want s1 still local, and durable", st)
	}
}

// TestTrackerDetachActsForTheActor checks that a detach's upload names the
// actor the snapshot was committed for, across an atelet restart that reloads
// the snapshot state file, and that a promote keeps it on the local copy.
func TestTrackerDetachActsForTheActor(t *testing.T) {
	t.Run("after a restart", func(t *testing.T) {
		store := newFakeSnapshotStore()
		first := newIdleTestTracker(t, store)
		ref := testRef("s1", 1, first.clock.Now())
		ref.Actor = testActor
		first.CommitLocal(trackedActorUID, ref)

		// A new tracker over the same node directories reloads the state file.
		tr := newStoreTracker(t, store)
		tr.Start()
		if _, err := tr.Detach(t.Context(), trackedActorUID); err != nil {
			t.Fatalf("detach: %v", err)
		}
		if got := store.actorOf("s1"); got != testActor {
			t.Errorf("upload acted for %+v, want %+v", got, testActor)
		}
	})

	t.Run("kept by a promote", func(t *testing.T) {
		store := newFakeSnapshotStore()
		tr := newTestTracker(t, store)
		ref := testRef("s1", 1, tr.clock.Now())
		ref.Actor = testActor
		tr.CommitLocal(trackedActorUID, ref)
		promoted := testRef("s1", 1, time.Time{})
		promoted.Actor = testActor
		tr.CommitPromoted(trackedActorUID, promoted)

		tr.lock(trackedActorUID)
		got := tr.findLocked(trackedActorUID).LatestLocal.Actor
		tr.unlock(trackedActorUID)
		if got != testActor {
			t.Errorf("local copy's actor after promote = %+v, want %+v", got, testActor)
		}
	})
}

// A background detach whose upload runs past the upload timeout fails, backs
// off, and keeps the local snapshot to retry; a foreground Detach RPC is
// bounded by its caller's context instead.
func TestTrackerDetachUploadTimeout(t *testing.T) {
	t.Run("background", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		tr := newIdleTestTracker(t, store)
		setWorkers(tr, 0, DefaultDetachWorkers)
		tr.Start()
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
		due := detachDueOf(tr, trackedActorUID)
		stepTo(tr, due)
		waitStarted(t, store)
		done := opDone(t, tr, trackedActorUID)

		stepTo(tr, tr.clock.Now().Add(testUploadTimeout))
		waitClosed(t, done, "the timed-out background detach")

		retryAt := tr.clock.Now().Add(testRetryDelay)
		if got := detachDueOf(tr, trackedActorUID); !got.Equal(retryAt) {
			t.Errorf("detach due after timeout = %v, want %v", got, retryAt)
		}
		if got := uuidOf(tr.State(trackedActorUID).GetLatestLocal()); got != "s1" {
			t.Errorf("latest_local after timeout = %q, want s1 kept", got)
		}

		store.setBlocking(false)
		detachNow(t, tr)
		if got := tr.State(trackedActorUID).GetLatestLocal(); got != nil {
			t.Errorf("latest_local after retry = %v, want unset", got)
		}
	})

	t.Run("foreground ignores background upload timeout", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		tr := newTestTracker(t, store)
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))

		detached := startDetach(t, tr)
		waitStarted(t, store)
		if d := store.lastDeadline(); !d.IsZero() {
			t.Errorf("foreground Detach's upload has deadline %v, want none beyond its caller's", d)
		}

		store.release <- nil
		if err := <-detached; err != nil {
			t.Fatalf("Detach: %v", err)
		}
	})
}
