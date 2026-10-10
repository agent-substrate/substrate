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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
)

// TestEnsureMarkedReverting_StateMatrix pins which states a revert is accepted
// from. It runs over every enum value, so a state added to the proto without a
// decision here fails rather than silently landing in the reject branch.
//
// SUSPENDED is the load-bearing rejection: it is revert's own destination, and
// also what a suspend that won the race leaves behind, so accepting it would
// report success for the opposite of what was asked.
func TestEnsureMarkedReverting_StateMatrix(t *testing.T) {
	allowed := map[ateapipb.ActorState]bool{
		ateapipb.ActorState_ACTOR_STATE_RUNNING: true,
		ateapipb.ActorState_ACTOR_STATE_PAUSED:  true,
		ateapipb.ActorState_ACTOR_STATE_CRASHED: true,
		// Re-entry: a previous attempt already marked it, so the step skips
		// rather than re-committing.
		ateapipb.ActorState_ACTOR_STATE_REVERTING: true,
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
			if got := updated.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_REVERTING {
				t.Errorf("state %v: ensureMarkedReverting returned actor in %v, want REVERTING", seedState, got)
			}
		}
		cleanup()
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
	for _, seedState := range []ateapipb.ActorState{
		ateapipb.ActorState_ACTOR_STATE_RUNNING,
		ateapipb.ActorState_ACTOR_STATE_PAUSED,
		ateapipb.ActorState_ACTOR_STATE_CRASHED,
		// A revert that died partway is retried, not rejected.
		ateapipb.ActorState_ACTOR_STATE_REVERTING,
	} {
		t.Run(seedState.String(), func(t *testing.T) {
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
			// pause left behind, which it must not.
			const keptURI = "gs://snapshots/team-a/actors/keep/snapshot"
			mustUpdateActorStatus(t, ctx, st, actor, func(s *ateapipb.ActorStatus) {
				s.LastAssignedGeneration = 3
				s.Snapshots = []*ateapipb.Snapshot{
					newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", keptURI, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
					newLocalSnapshot(2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
					newLocalSnapshot(3, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local-in-progress", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS),
				}
			})

			reverted, err := w.RevertActor(ctx, actorRef)
			if err != nil {
				t.Fatalf("RevertActor: %v", err)
			}

			gotStatus := reverted.GetStatus()
			if got := gotStatus.GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
				t.Errorf("state = %v, want SUSPENDED", got)
			}
			// LastAssignedGeneration is not decreased on revert so we can keep track
			// of stale snapshots that need to be cleaned up and future snapshots
			// never reuse generations 2 or 3 after revert discards them.
			if got := gotStatus.GetLastAssignedGeneration(); got != 3 {
				t.Errorf("last_assigned_generation = %d, want 3", got)
			}
			gotSnap, gotSt := findLatestSnapshotStorage(gotStatus, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
			if got := gotSnap.GetGeneration(); got != 1 {
				t.Errorf("retained snapshot generation = %d, want 1", got)
			}
			if got := gotSt.GetObject().GetSnapshotUri(); got != keptURI {
				t.Errorf("external snapshot = %q, want it untouched at %q", got, keptURI)
			}
			// Resume prefers a local snapshot over the external one, so a
			// surviving LocalSnapshot would restore the execution this
			// revert just discarded.
			if _, got := findLatestSnapshotStorage(gotStatus, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED); got != nil {
				t.Errorf("local snapshot info = %v, want nil", got)
			}
			if _, got := findLatestSnapshotStorage(gotStatus, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS); got != nil {
				t.Errorf("in-progress durable snapshot = %v, want nil", got)
			}
			if _, got := findLatestSnapshotStorage(gotStatus, ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS); got != nil {
				t.Errorf("in-progress local snapshot = %v, want nil", got)
			}
			if got := gotStatus.GetAssignedNode(); got != "" {
				t.Errorf("assigned node = %q, want empty", got)
			}
			if got := gotStatus.GetWorkerAssignment(); got != nil {
				t.Errorf("worker assignment = %v, want nil", got)
			}

			// If the actor runs again and takes another snapshot, it must
			// allocate generation 4 rather than reusing 2 or 3.
			running := mustUpdateActorStatus(t, ctx, st, reverted, func(s *ateapipb.ActorStatus) {
				s.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
			})
			tmpl := &ateapipb.ActorTemplate{
				SnapshotConfig: &ateapipb.SnapshotConfig{StorageLocation: "gs://snapshots/"},
			}
			suspending, err := w.ensureMarkedSuspending(ctx, actorRef, running, tmpl)
			if err != nil {
				t.Fatalf("ensureMarkedSuspending after revert: %v", err)
			}
			if got := suspending.GetStatus().GetLastAssignedGeneration(); got != 4 {
				t.Errorf("last_assigned_generation after post-revert suspend = %d, want 4", got)
			}
			if got := snapshotAtLatestGeneration(suspending.GetStatus()).GetGeneration(); got != 4 {
				t.Errorf("new snapshot generation after post-revert suspend = %d, want 4", got)
			}
		})
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
		seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

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
		seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_CRASHED)

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
	seedWorkflowActor(t, ctx, st, actorRef, "ns", "missing-tmpl", ateapipb.ActorState_ACTOR_STATE_CRASHED)

	reverted, err := w.RevertActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("RevertActor: %v", err)
	}
	if got := reverted.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
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
	seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_CRASHED, func(a *ateapipb.Actor) {
		a.Status.Crash = newActorCrash("resume", crashMessageWorkerGone)
	})

	reverted, err := w.RevertActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("RevertActor: %v", err)
	}
	if got := reverted.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("state = %v, want SUSPENDED", got)
	}
	if got := reverted.GetStatus().GetCrash(); got != nil {
		t.Errorf("Crash = %v, want cleared by the revert", got)
	}
	_, gotSt := findLatestSnapshotStorage(reverted.GetStatus(), ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	if got := gotSt.GetObject().GetSnapshotUri(); got != "" {
		t.Errorf("external snapshot = %q, want none", got)
	}
}

// TestEnsureInProgressSnapshotDiscarded covers what a revert deletes from
// object storage and, more importantly, what it must not. Delete drops the
// actor's whole prefix; revert cannot, because the snapshot it is returning the
// actor to lives under that same prefix.
func TestEnsureInProgressSnapshotDiscarded(t *testing.T) {
	const inFlightSnapshotName = "2026-01-01t00-00-00z-abandoned"

	tests := []struct {
		name string
		// tagOwnedSnapshot makes the actor's external snapshot a tag's rather
		// than one it took itself, as an actor created from a tag starts out.
		tagOwnedSnapshot bool
		// inFlight names a snapshot the interrupted suspend was writing.
		inFlight string
		// foreignInFlight records the in-progress snapshot under another
		// owner's prefix, which no suspend can produce.
		foreignInFlight       bool
		wantInFlightDiscarded bool
		wantErr               bool
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
			w, objects := newFinalizeWorkflow(persistence)

			actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
				Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_REVERTING},
			})

			// The actor's prefix is keyed on the UID the store just assigned,
			// so its snapshots can only be placed now.
			current := mustActorSnapshotURI(t, template, actor, "current")
			currentType := ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR
			if tt.tagOwnedSnapshot {
				current = mustTagSnapshotURI(t, template, "team-a", "v1-snapshot")
				currentType = ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG
			}
			objects.PutSnapshot(t, current, "manifest.json")
			inFlight := mustActorSnapshotURI(t, template, actor, inFlightSnapshotName)
			if tt.foreignInFlight {
				inFlight = mustTagSnapshotURI(t, template, "team-a", "someone-elses-snapshot")
			}
			if tt.inFlight != "" {
				objects.PutSnapshot(t, inFlight, "manifest.json")
			}
			actor = mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
				s.LastAssignedGeneration = 1
				s.Snapshots = []*ateapipb.Snapshot{
					newDurableSnapshot(1, currentType, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", current.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
				}
				if tt.inFlight != "" {
					s.LastAssignedGeneration = 2
					s.Snapshots = append(s.Snapshots, newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", inFlight.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS))
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

	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_REVERTING,
			AssignedNode:           "node-1",
			LastAssignedGeneration: 1,
			Snapshots: []*ateapipb.Snapshot{
				newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", someActorSnapshotURI(t, testStorageLocation, "team-a", "abandoned"), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS),
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
	if got := finalized.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("state = %v, want SUSPENDED", got)
	}
	if got := finalized.GetStatus().GetAssignedNode(); got != "" {
		t.Errorf("assigned node = %q, want empty", got)
	}
	if _, got := findLatestSnapshotStorage(finalized.GetStatus(), ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS); got != nil {
		t.Errorf("in-progress durable snapshot = %v, want nil", got)
	}
}
