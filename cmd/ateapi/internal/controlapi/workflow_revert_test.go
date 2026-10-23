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

package controlapi

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/objectstore/objectstoretest"
	"go.ate.dev/substrate/internal/objectstoreplugin/objectstoreplugintest"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// TestEnsureMarkedReverting_StateMatrix pins which states a revert is accepted
// from. It runs over every enum value, so a state added to the proto without a
// decision here fails rather than silently landing in the reject branch.
//
// SUSPENDED is the load-bearing rejection: it is revert's own destination, and
// also what a suspend that won the race leaves behind, so accepting it would
// report success for the opposite of what was asked. A SUSPENDED actor that
// holds a node-local snapshot is accepted; see
// TestEnsureMarkedReverting_AcceptsPaused.
func TestEnsureMarkedReverting_StateMatrix(t *testing.T) {
	allowed := map[ateapi.ActorState]bool{
		ateapi.ActorState_ACTOR_STATE_RUNNING: true,
		ateapi.ActorState_ACTOR_STATE_CRASHED: true,
		// Re-entry: a previous attempt already marked it, so the step skips
		// rather than re-committing.
		ateapi.ActorState_ACTOR_STATE_REVERTING: true,
	}

	for _, seedState := range allActorStates {
		ctx := context.Background()
		st, cleanup := storetest.SetupTestStore(t)
		w := newTestActorWorkflow(t, st, "ns", "tmpl1")

		actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
		seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", seedState)
		actor, err := st.GetActor(ctx, actorRef)
		if err != nil {
			t.Fatalf("state %v: get seeded actor: %v", seedState, err)
		}

		updated, err := w.ensureMarkedReverting(ctx, actorRef, actor)
		assertPrerequisiteResult(t, seedState, err, allowed[seedState])
		if err == nil {
			if got := updated.GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_REVERTING {
				t.Errorf("state %v: ensureMarkedReverting returned actor in %v, want REVERTING", seedState, got)
			}
		}
		cleanup()
	}
}

// TestEnsureMarkedReverting_AcceptsPaused verifies a SUSPENDED actor whose
// latest snapshot is node-local takes the revert edge: the revert discards that
// local copy and returns the actor to its durable snapshot.
func TestEnsureMarkedReverting_AcceptsPaused(t *testing.T) {
	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	w := newTestActorWorkflow(t, st, "ns", "tmpl1")

	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
	seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", ateapi.ActorState_ACTOR_STATE_SUSPENDED, withPausedShape(t, "node-1"))
	actor, err := st.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("get seeded actor: %v", err)
	}

	updated, err := w.ensureMarkedReverting(ctx, actorRef, actor)
	if err != nil {
		t.Fatalf("ensureMarkedReverting: %v", err)
	}
	if got := updated.GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_REVERTING {
		t.Errorf("state = %v, want REVERTING", got)
	}
}

// TestRevertActor_ReturnsActorToItsSnapshot covers the whole workflow from each
// accepted origin: the actor lands SUSPENDED holding the same external snapshot
// it started with, and every pointer to the discarded execution is gone.
//
// None of these actors has an assigned node, so the terminate path is not
// exercised here; it needs the atelet rig, and the unit workflow is built with
// a nil dialer on purpose so an unexpected dial fails loudly.
func TestRevertActor_ReturnsActorToItsSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name      string
		seedState ateapi.ActorState
		// paused seeds the local-snapshot shape: the latest snapshot is node-local
		// and the actor is pinned to its node. Only meaningful with
		// SUSPENDED, which the revert otherwise rejects.
		paused bool
	}{
		{name: "RUNNING", seedState: ateapi.ActorState_ACTOR_STATE_RUNNING},
		{name: "paused", seedState: ateapi.ActorState_ACTOR_STATE_SUSPENDED, paused: true},
		{name: "CRASHED", seedState: ateapi.ActorState_ACTOR_STATE_CRASHED},
		// A revert that died partway is retried, not rejected.
		{name: "REVERTING", seedState: ateapi.ActorState_ACTOR_STATE_REVERTING},
	} {
		seedState := tc.seedState
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			w := newTestActorWorkflow(t, st, "ns", "tmpl1")

			actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
			seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", seedState)
			actor, err := st.GetActor(ctx, actorRef)
			if err != nil {
				t.Fatalf("get seeded actor: %v", err)
			}

			// The snapshot revert must preserve, plus the node-local state a
			// suspend left behind, which it must not. The suspended actor's
			// latest snapshot is the completed local one; the other origins
			// also carry the half-written local snapshot an interrupted suspend
			// leaves.
			const keptURI = "gs://snapshots/atespaces/team-a/actors/keep/snapshots/snapshot"
			tmpl := &ateapi.ActorTemplate{SnapshotConfig: &ateapi.SnapshotConfig{StorageLocation: "gs://snapshots"}}
			mustUpdateActorStatus(t, ctx, st, actor, func(s *ateapi.ActorStatus) {
				local1 := newPausedSnapshot(t, 2, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local-1", "node-1")
				local1.DurableSnapshot.Object.SnapshotUri = mustActorSnapshotURI(t, tmpl, actor, "local-1").String()
				inProgress := newPausedSnapshot(t, 3, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local-in-progress", "")
				inProgress.DurableSnapshot.Object.SnapshotUri = mustActorSnapshotURI(t, tmpl, actor, "local-in-progress").String()
				s.LastAssignedGeneration = 3
				s.Snapshots = []*ateapi.Snapshot{
					newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "kept", keptURI, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
					local1,
					inProgress,
				}
				if tc.paused {
					s.Snapshots[2].Locality = "node-1"
				}
			})

			reverted, err := w.RevertActor(ctx, actorRef)
			if err != nil {
				t.Fatalf("RevertActor: %v", err)
			}

			gotStatus := reverted.GetStatus()
			if got := gotStatus.GetState(); got != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
				t.Errorf("state = %v, want SUSPENDED", got)
			}
			// LastAssignedGeneration is not decreased on revert so we can keep track
			// of stale snapshots that need to be cleaned up and future snapshots
			// never reuse generations 2 or 3 after revert discards them.
			if got := gotStatus.GetLastAssignedGeneration(); got != 3 {
				t.Errorf("last_assigned_generation = %d, want 3", got)
			}
			gotSnap := findLatestDurableSnapshot(gotStatus, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
			if got := gotSnap.GetGeneration(); got != 1 {
				t.Errorf("retained snapshot generation = %d, want 1", got)
			}
			if got := gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri(); got != keptURI {
				t.Errorf("external snapshot = %q, want it untouched at %q", got, keptURI)
			}
			// Resume prefers a local snapshot over the external one, so a
			// surviving LocalSnapshot would restore the execution this
			// revert just discarded.
			if got := findLatestLocalSnapshot(gotStatus); got != nil {
				t.Errorf("local snapshot info = %v, want nil", got)
			}
			if got := findLatestDurableSnapshot(gotStatus, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING); got != nil {
				t.Errorf("in-progress durable snapshot = %v, want nil", got)
			}
			if got := len(gotStatus.GetSnapshots()); got != 1 {
				t.Errorf("len(snapshots) = %d, want 1 (only the completed durable snapshot should remain)", got)
			}
			if got := gotStatus.GetAssignedNode(); got != "" {
				t.Errorf("assigned node = %q, want empty", got)
			}
			if got := gotStatus.GetWorkerAssignment(); got != nil {
				t.Errorf("worker assignment = %v, want nil", got)
			}

			// If the actor runs again and takes another snapshot, it must
			// allocate generation 4 rather than reusing 2 or 3.
			running := mustUpdateActorStatus(t, ctx, st, reverted, func(s *ateapi.ActorStatus) {
				s.State = ateapi.ActorState_ACTOR_STATE_RUNNING
				s.AssignedNode = "node-1"
				s.WorkerAssignment = wireTestAssignment()
			})
			seedWireWorker(t, ctx, st, assignmentOf(running))
			wire, atelet := newWireCaptureWorkflow(t, st)
			suspended, err := wire.SuspendActor(ctx, actorRef, suspendSync)
			if err != nil {
				t.Fatalf("SuspendActor after revert: %v", err)
			}
			if got := suspended.GetStatus().GetLastAssignedGeneration(); got != 4 {
				t.Errorf("last_assigned_generation after post-revert suspend = %d, want 4", got)
			}
			newSnap := snapshotAtLatestGeneration(suspended.GetStatus())
			if got := newSnap.GetGeneration(); got != 4 {
				t.Errorf("new snapshot generation after post-revert suspend = %d, want 4", got)
			}
			if got, want := atelet.checkpointRequest().GetSnapshot().GetSnapshotUuid(), newSnap.GetUuid(); got != want {
				t.Errorf("CheckpointRequest.Snapshot.SnapshotUuid = %q, want the generation 4 snapshot %q", got, want)
			}
		})
	}
}

func TestRevertActor_UnreachableNode(t *testing.T) {
	for _, tt := range []struct {
		name     string
		node     string
		err      error
		pending  bool
		assigned bool
		crashed  bool
		conflict bool
		wantCode codes.Code
	}{
		{name: "missing atelet", node: "node-3"},
		{name: "unavailable atelet", node: "node-1", err: apierror.Unavailable("offline")},
		{name: "atelet deadline", node: "node-1", err: context.DeadlineExceeded},
		{name: "pending beside older completed snapshot", node: "node-1", err: apierror.Unavailable("offline"), pending: true, wantCode: codes.Unavailable},
		{name: "worker still assigned", node: "node-1", err: apierror.Unavailable("offline"), assigned: true, wantCode: codes.Unavailable},
		{name: "crashed execution still needs termination", node: "node-1", err: apierror.Unavailable("offline"), crashed: true, wantCode: codes.Unavailable},
		{name: "other RPC failure", node: "node-1", err: apierror.Internal("injected failure"), wantCode: codes.Internal},
		{name: "concurrent update", node: "node-1", err: apierror.Unavailable("offline"), conflict: true, wantCode: codes.Aborted},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			t.Cleanup(cleanup)
			w := newTestActorWorkflow(t, st, "ns", "tmpl1")
			wire, atelet := newWireCaptureWorkflow(t, st)
			w.dialer = wire.dialer
			ref := resources.ActorRef{Atespace: "team-a", Name: "id1"}
			seedWorkflowActor(t, ctx, st, ref, "ns", "tmpl1", ateapi.ActorState_ACTOR_STATE_SUSPENDED)
			actor, err := st.GetActor(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			actor = mustUpdateActorStatus(t, ctx, st, actor, func(s *ateapi.ActorStatus) {
				s.AssignedNode = tt.node
				s.LastAssignedGeneration = 2
				snap := newPausedSnapshot(t, 2, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "uploaded", tt.node)
				snap.DurableSnapshot.Status = ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
				if tt.pending {
					snap.DurableSnapshot.Status = ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING
				}
				s.Snapshots = []*ateapi.Snapshot{
					newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, snap.GetFidelity(), "", "older", "gs://snapshots/atespaces/team-a/actors/older/snapshots/snapshot", ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
					snap,
				}
				if tt.assigned {
					s.State = ateapi.ActorState_ACTOR_STATE_RUNNING
					s.WorkerAssignment = wireTestAssignment()
				}
				if tt.crashed {
					s.State = ateapi.ActorState_ACTOR_STATE_CRASHED
					s.Crash = newActorCrash("ResumeActor", "interrupted execution")
				}
			})
			want := proto.CloneOf(actor.GetStatus().GetSnapshots()[1])
			atelet.terminateErr = tt.err
			if tt.conflict {
				atelet.onTerminate = func() {
					cur, err := st.GetActor(ctx, ref)
					if err != nil {
						t.Errorf("GetActor: %v", err)
						return
					}
					if _, err := st.UpdateActor(ctx, ref, store.PreconditionFrom(cur), func(a *ateapi.Actor) error {
						a.WorkerSelector = &ateapi.Selector{MatchLabels: map[string]string{"changed": "true"}}
						return nil
					}); err != nil {
						t.Errorf("UpdateActor: %v", err)
					}
				}
			}
			reverted, err := w.RevertActor(ctx, ref)
			if got := apierror.Code(err); got != tt.wantCode {
				t.Fatalf("RevertActor = %v, want %v", err, tt.wantCode)
			}
			if tt.wantCode != codes.OK {
				stored, err := st.GetActor(ctx, ref)
				if err != nil {
					t.Fatal(err)
				}
				got := stored.GetStatus()
				if got.GetState() != ateapi.ActorState_ACTOR_STATE_REVERTING || got.GetAssignedNode() != tt.node {
					t.Fatalf("status = %v, want REVERTING on %s", got, tt.node)
				}
				if !proto.Equal(got.GetSnapshots()[1], want) {
					t.Fatalf("snapshot changed after failed revert: %v", got.GetSnapshots())
				}
				if !tt.conflict {
					return
				}
				atelet.mu.Lock()
				atelet.onTerminate = nil
				atelet.mu.Unlock()
				reverted, err = w.RevertActor(ctx, ref)
				if err != nil {
					t.Fatalf("retry after conflict: %v", err)
				}
			}
			got := reverted.GetStatus()
			if got.GetState() != ateapi.ActorState_ACTOR_STATE_SUSPENDED || got.GetAssignedNode() != "" || got.GetWorkerAssignment() != nil {
				t.Fatalf("reverted status = %v, want unbound SUSPENDED", got)
			}
			if len(got.GetSnapshots()) != 1 {
				t.Fatalf("snapshots = %v, want only the latest durable snapshot", got.GetSnapshots())
			}
			want.Locality = ""
			if !proto.Equal(got.GetSnapshots()[0], want) {
				t.Errorf("retained snapshot = %v, want %v", got.GetSnapshots()[0], want)
			}
		})
	}
}

// TestRevertActor_PausedSnapshotAlreadyUploaded verifies a revert of a
// suspended actor whose local snapshot atelet uploaded returns it to that
// upload, its latest durable snapshot, and collects the durable snapshot the
// upload replaced. The actor has no assigned node, so there is nothing to
// terminate, as in TestRevertActor_ReturnsActorToItsSnapshot.
func TestRevertActor_PausedSnapshotAlreadyUploaded(t *testing.T) {
	memory := ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	completed := ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	w := newTestActorWorkflow(t, st, "ns", "tmpl1")
	objects := objectstoretest.New()
	w.objectStorePlugin = objectstoreplugintest.ControlClient(objects)

	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
	seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", ateapi.ActorState_ACTOR_STATE_SUSPENDED)
	actor, err := st.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("get seeded actor: %v", err)
	}
	// newTestActorWorkflow's template stores snapshots under gs://snapshots.
	tmpl := &ateapi.ActorTemplate{SnapshotConfig: &ateapi.SnapshotConfig{StorageLocation: "gs://snapshots"}}
	old := mustActorSnapshotURI(t, tmpl, actor, "old")
	uploaded := mustActorSnapshotURI(t, tmpl, actor, "pause-snap")
	objects.PutSnapshot(t, old, "manifest.json")
	objects.PutSnapshot(t, uploaded, "manifest.json")
	mustUpdateActorStatus(t, ctx, st, actor, func(s *ateapi.ActorStatus) {
		paused := newPausedSnapshot(t, 2, memory, "", "pause-snap", "node-1")
		paused.DurableSnapshot = &ateapi.SnapshotStorage{Status: completed, Object: &ateapi.ObjectSnapshot{SnapshotUri: uploaded.String()}}
		s.LastAssignedGeneration = 2
		s.Snapshots = []*ateapi.Snapshot{
			newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, memory, "", old.Name(), old.String(), completed),
			paused,
		}
	})

	reverted, err := w.RevertActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("RevertActor: %v", err)
	}
	want := []snapshotOutcome{{generation: 2, fidelity: memory, durable: completed}}
	if diff := cmp.Diff(want, describeSnapshots(reverted.GetStatus()), cmp.AllowUnexported(snapshotOutcome{})); diff != "" {
		t.Errorf("snapshots mismatch (-want +got):\n%s", diff)
	}
	if len(objects.Snapshot(t, old)) != 0 {
		t.Error("the durable snapshot the upload replaced was not released")
	}
	if len(objects.Snapshot(t, uploaded)) == 0 {
		t.Error("the upload the revert returned the actor to was released")
	}
}

// TestRevertActor_RejectsSuspended covers both ways an actor can already be
// SUSPENDED when a revert arrives: it never ran, or a previous revert already
// finished. Both are FailedPrecondition — revert is not a no-op success on its
// own destination, for the same reason a second DeleteActor is NotFound.
func TestRevertActor_RejectsSuspended(t *testing.T) {
	t.Run("actor was already suspended", func(t *testing.T) {
		ctx := context.Background()
		st, cleanup := storetest.SetupTestStore(t)
		defer cleanup()
		w := newTestActorWorkflow(t, st, "ns", "tmpl1")

		actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
		seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", ateapi.ActorState_ACTOR_STATE_SUSPENDED)

		if _, err := w.RevertActor(ctx, actorRef); apierror.Code(err) != codes.FailedPrecondition {
			t.Fatalf("RevertActor = %v, want FailedPrecondition", err)
		}
	})

	t.Run("second revert of an already-reverted actor", func(t *testing.T) {
		ctx := context.Background()
		st, cleanup := storetest.SetupTestStore(t)
		defer cleanup()
		w := newTestActorWorkflow(t, st, "ns", "tmpl1")

		actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
		seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", ateapi.ActorState_ACTOR_STATE_CRASHED)

		if _, err := w.RevertActor(ctx, actorRef); err != nil {
			t.Fatalf("first RevertActor: %v", err)
		}
		if _, err := w.RevertActor(ctx, actorRef); apierror.Code(err) != codes.FailedPrecondition {
			t.Fatalf("second RevertActor = %v, want FailedPrecondition", err)
		}
	})
}

// TestRevertActor_MissingTemplate covers the wreck revert most needs to
// recover: an actor whose template was deleted. Suspend refuses this actor
// outright, so revert must not, or the only way out would be deletion.
func TestRevertActor_MissingTemplate(t *testing.T) {
	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	w := newTestActorWorkflow(t, st, "ns", "tmpl1")

	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
	seedWorkflowActor(t, ctx, st, actorRef, "ns", "missing-tmpl", ateapi.ActorState_ACTOR_STATE_CRASHED)

	reverted, err := w.RevertActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("RevertActor: %v", err)
	}
	if got := reverted.GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("state = %v, want SUSPENDED", got)
	}
}

// TestRevertActor_NoSnapshotToRevertTo covers "revert to birth": an actor that
// crashed before it ever suspended holds no external snapshot, so there is
// nothing to return it to. That is accepted, not an error — the actor resumes
// from its template as a fresh one.
func TestRevertActor_NoSnapshotToRevertTo(t *testing.T) {
	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	w := newTestActorWorkflow(t, st, "ns", "tmpl1")

	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
	seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", ateapi.ActorState_ACTOR_STATE_CRASHED, func(a *ateapi.Actor) {
		a.Status.Crash = newActorCrash("resume", crashMessageWorkerGone)
	})

	reverted, err := w.RevertActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("RevertActor: %v", err)
	}
	if got := reverted.GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("state = %v, want SUSPENDED", got)
	}
	if got := reverted.GetStatus().GetCrash(); got != nil {
		t.Errorf("Crash = %v, want cleared by the revert", got)
	}
	gotSnap := findLatestDurableSnapshot(reverted.GetStatus(), ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	if got := gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri(); got != "" {
		t.Errorf("external snapshot = %q, want none", got)
	}
}

// TestEnsureInProgressSnapshotDiscarded covers what a revert deletes from
// object storage and, more importantly, what it must not. Delete drops the
// actor's whole prefix; revert cannot, because the snapshot it is returning the
// actor to lives under that same prefix.
func TestEnsureInProgressSnapshotDiscarded(t *testing.T) {
	const inFlightSnapshotName = "2026-01-01t00-00-00z-abandoned"
	const earlierPauseSnapshotName = "2025-12-31t00-00-00z-earlier-pause"

	tests := []struct {
		name string
		// tagOwnedSnapshot makes the actor's external snapshot a tag's rather
		// than one it took itself, as an actor created from a tag starts out.
		tagOwnedSnapshot bool
		// inFlight names a snapshot the interrupted suspend was writing.
		inFlight string
		// foreignInFlight records the in-progress snapshot under another
		// owner's prefix, which no suspend can produce.
		foreignInFlight bool
		// earlierPause records an older PENDING snapshot: an async suspend whose
		// upload atelet never reported before the actor resumed.
		earlierPause              bool
		wantInFlightDiscarded     bool
		wantEarlierPauseDiscarded bool
		wantErr                   bool
	}{
		{
			name:                  "discards the snapshot an interrupted suspend was writing",
			inFlight:              inFlightSnapshotName,
			wantInFlightDiscarded: true,
		},
		{
			name:     "leaves the actor's own external snapshot in place",
			inFlight: "",
		},
		{
			name:                  "leaves a snapshot borrowed from a tag in place",
			tagOwnedSnapshot:      true,
			inFlight:              inFlightSnapshotName,
			wantInFlightDiscarded: true,
		},
		{
			name:            "refuses an in-progress snapshot the actor does not own",
			inFlight:        inFlightSnapshotName,
			foreignInFlight: true,
			wantErr:         true,
		},
		{
			name:                      "discards an earlier pause's unrecorded upload along with the in-flight snapshot",
			inFlight:                  inFlightSnapshotName,
			earlierPause:              true,
			wantInFlightDiscarded:     true,
			wantEarlierPauseDiscarded: true,
		},
		{
			name:                      "discards an earlier pause's unrecorded upload with no suspend in flight",
			earlierPause:              true,
			wantEarlierPauseDiscarded: true,
		},
		{
			name:            "discards nothing when any in-progress snapshot is not the actor's",
			inFlight:        inFlightSnapshotName,
			foreignInFlight: true,
			earlierPause:    true,
			wantErr:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
			w, objects := newFinalizeWorkflow(persistence)

			actor := storetest.MustCreateActor(t, ctx, persistence, &ateapi.Actor{
				Metadata:      &ateapi.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
				ActorTemplate: &ateapi.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
				Status:        &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_REVERTING},
			})

			// The actor's prefix is keyed on the UID the store just assigned,
			// so its snapshots can only be placed now.
			current := mustActorSnapshotURI(t, template, actor, "current")
			currentType := ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR
			if tt.tagOwnedSnapshot {
				current = mustTagSnapshotURI(t, template, "team-a", "v1-snapshot")
				currentType = ateapi.SnapshotOwner_SNAPSHOT_OWNER_TAG
			}
			objects.PutSnapshot(t, current, "manifest.json")
			inFlight := mustActorSnapshotURI(t, template, actor, inFlightSnapshotName)
			if tt.foreignInFlight {
				inFlight = mustTagSnapshotURI(t, template, "team-a", "someone-elses-snapshot")
			}
			if tt.inFlight != "" {
				objects.PutSnapshot(t, inFlight, "manifest.json")
			}
			earlierPause := mustActorSnapshotURI(t, template, actor, earlierPauseSnapshotName)
			if tt.earlierPause {
				objects.PutSnapshot(t, earlierPause, "manifest.json")
			}
			actor = mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapi.ActorStatus) {
				s.LastAssignedGeneration = 1
				s.Snapshots = []*ateapi.Snapshot{
					newDurableSnapshot(1, currentType, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", current.Name(), current.String(), ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
				}
				if tt.earlierPause {
					s.LastAssignedGeneration++
					s.Snapshots = append(s.Snapshots, newDurableSnapshot(s.LastAssignedGeneration, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", earlierPause.Name(), earlierPause.String(), ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING))
				}
				if tt.inFlight != "" {
					s.LastAssignedGeneration++
					s.Snapshots = append(s.Snapshots, newDurableSnapshot(s.LastAssignedGeneration, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", inFlight.Name(), inFlight.String(), ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING))
				}
			})

			err := w.ensureInProgressSnapshotDiscarded(ctx, actor)
			if tt.wantErr {
				if err == nil {
					t.Fatal("ensureInProgressSnapshotDiscarded = nil, want an error for a snapshot the actor does not own")
				}
				if len(objects.Snapshot(t, inFlight)) == 0 {
					t.Errorf("snapshot %v was discarded, but it belongs to another owner", inFlight)
				}
				if tt.earlierPause && len(objects.Snapshot(t, earlierPause)) == 0 {
					t.Errorf("snapshot %v was discarded, but a corrupted record must discard nothing", earlierPause)
				}
				return
			}
			if err != nil {
				t.Fatalf("ensureInProgressSnapshotDiscarded: %v", err)
			}

			// The whole point: whatever else happens, the snapshot the actor
			// is being returned to survives.
			if len(objects.Snapshot(t, current)) == 0 {
				t.Errorf("external snapshot %v was collected, but revert must preserve it", current)
			}
			if tt.inFlight != "" {
				discarded := len(objects.Snapshot(t, inFlight)) == 0
				if discarded != tt.wantInFlightDiscarded {
					t.Errorf("in-flight snapshot discarded = %v, want %v", discarded, tt.wantInFlightDiscarded)
				}
			}
			if tt.earlierPause {
				discarded := len(objects.Snapshot(t, earlierPause)) == 0
				if discarded != tt.wantEarlierPauseDiscarded {
					t.Errorf("earlier pause snapshot discarded = %v, want %v", discarded, tt.wantEarlierPauseDiscarded)
				}
			}
		})
	}
}

// TestEnsureRevertedFinalized_NoObjectStore covers a workflow built without an
// object store: the discard step skips instead of failing, so the revert still
// finalizes.
func TestEnsureRevertedFinalized_NoObjectStore(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	w := &ActorWorkflow{store: persistence}

	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
		ActorTemplate: &ateapi.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
		Status: &ateapi.ActorStatus{
			State:                  ateapi.ActorState_ACTOR_STATE_REVERTING,
			AssignedNode:           "node-1",
			LastAssignedGeneration: 1,
			Snapshots: []*ateapi.Snapshot{
				newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "abandoned", someActorSnapshotURI(t, testStorageLocation, "team-a", "abandoned"), ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
			},
		},
	})

	if err := w.ensureInProgressSnapshotDiscarded(ctx, actor); err != nil {
		t.Fatalf("ensureInProgressSnapshotDiscarded: %v", err)
	}

	actorRef := resources.ActorRefFromActor(actor)
	finalized, err := w.ensureRevertedFinalized(ctx, actorRef)
	if err != nil {
		t.Fatalf("ensureRevertedFinalized: %v", err)
	}
	if got := finalized.GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("state = %v, want SUSPENDED", got)
	}
	if got := finalized.GetStatus().GetAssignedNode(); got != "" {
		t.Errorf("assigned node = %q, want empty", got)
	}
	if got := findLatestDurableSnapshot(finalized.GetStatus(), ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING); got != nil {
		t.Errorf("in-progress durable snapshot = %v, want nil", got)
	}
}
