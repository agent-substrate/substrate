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
	"errors"
	"strings"
	"testing"

	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/objectstore/objectstoretest"
	"go.ate.dev/substrate/internal/objectstoreplugin/objectstoreplugintest"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
)

func TestDeleteActorWorkflow_ExecutionPaths(t *testing.T) {
	tests := []struct {
		name        string
		seedState   ateapipb.ActorState
		missingTmpl bool
	}{
		{
			name:      "delete suspended actor succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		},
		{
			name:      "delete crashed actor succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_CRASHED,
		},
		{
			name:      "delete deleting actor succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_DELETING,
		},
		{
			name:      "delete running actor succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_RUNNING,
		},
		{
			name:        "delete suspended actor with missing template succeeds",
			seedState:   ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			missingTmpl: true,
		},
		{
			name:        "delete running actor with missing template succeeds",
			seedState:   ateapipb.ActorState_ACTOR_STATE_RUNNING,
			missingTmpl: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			w := newTestActorWorkflow(t, st, "ns", "tmpl1")

			actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
			tmplName := "tmpl1"
			if tc.missingTmpl {
				tmplName = "missing-tmpl"
			}
			seedWorkflowActor(t, ctx, st, actorRef, "ns", tmplName, tc.seedState)

			deleted, err := w.DeleteActor(ctx, actorRef, store.DeletePreconditions{})
			if err != nil {
				t.Fatalf("DeleteActor failed: %v", err)
			}
			if deleted == nil {
				t.Fatalf("expected non-nil deleted actor")
			}
			if _, err := st.GetActor(ctx, actorRef); err == nil {
				t.Errorf("expected actor to be deleted from store, but it still exists")
			}
		})
	}
}

// TestEnsureMarkedDeleting_AllStates checks that every actor state can be
// marked DELETING.
func TestEnsureMarkedDeleting_AllStates(t *testing.T) {
	for _, seedState := range allActorStates {
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

			updated, err := w.ensureMarkedDeleting(ctx, actorRef, actor)
			if err != nil {
				t.Fatalf("ensureMarkedDeleting failed: %v", err)
			}
			if got := updated.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_DELETING {
				t.Errorf("ensureMarkedDeleting returned actor in %v, want DELETING", got)
			}
		})
	}
}

// TestEnsureExternalSnapshotsReleased covers what a delete collects on its way
// out: the external snapshot the actor owns, and any snapshot an abandoned
// suspend left in flight, but never one the actor is only borrowing from a tag.
func TestEnsureExternalSnapshotsReleased(t *testing.T) {
	// inFlightSnapshotName names a snapshot written during a suspend operation
	// that never finalized.
	const inFlightSnapshotName = "2026-01-01t00-00-00z-abandoned"

	tests := []struct {
		name string
		// tagOwnedSnapshot makes the actor's external snapshot a tag's rather than one
		// it took itself, which is how an actor created from a tag starts out.
		tagOwnedSnapshot bool
		// inFlight, when set, names a snapshot an abandoned suspend left
		// behind. It must always be collected by the delete.
		inFlight            string
		wantCurrentReleased bool
	}{
		{
			name:                "releases the external snapshot the actor owns",
			wantCurrentReleased: true,
		},
		{
			// The actor can't delete a snapshot it only borrows from a tag:
			// that snapshot goes away with the tag.
			name:                "leaves an external snapshot borrowed from a tag in place",
			tagOwnedSnapshot:    true,
			wantCurrentReleased: false,
		},
		{
			name:                "collects the external snapshot an abandoned suspend left in flight",
			inFlight:            inFlightSnapshotName,
			wantCurrentReleased: true,
		},
		{
			name:                "collects an in-flight external snapshot even while borrowing",
			tagOwnedSnapshot:    true,
			inFlight:            inFlightSnapshotName,
			wantCurrentReleased: false,
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
				Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_DELETING},
			})

			// The actor's prefix is keyed on the UID the store just assigned, so
			// its snapshots can only be placed now.
			current := mustActorSnapshotURI(t, template, actor, "current")
			currentType := ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR
			if tt.tagOwnedSnapshot {
				current = mustTagSnapshotURI(t, template, "team-a", "v1-snapshot")
				currentType = ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG
			}
			objects.PutSnapshot(t, current, "manifest.json")
			inFlight := mustActorSnapshotURI(t, template, actor, inFlightSnapshotName)
			if tt.inFlight != "" {
				objects.PutSnapshot(t, inFlight, "manifest.json")
			}
			actor = mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
				s.LastAssignedGeneration = 2
				s.Snapshots = []*ateapipb.Snapshot{
					newDurableSnapshot(1, currentType, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", current.Name(), current.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
					newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", inFlight.Name(), inFlight.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
				}
			})

			if err := w.ensureExternalSnapshotsReleased(ctx, actor); err != nil {
				t.Fatalf("ensureExternalSnapshotsReleased: %v", err)
			}
			if released := len(objects.Snapshot(t, current)) == 0; released != tt.wantCurrentReleased {
				t.Errorf("current external snapshot released = %v, want %v", released, tt.wantCurrentReleased)
			}
			if tt.inFlight != "" && len(objects.Snapshot(t, inFlight)) != 0 {
				t.Errorf("in-flight external snapshot %v was not released", inFlight)
			}
		})
	}
}

// TestEnsureExternalSnapshotsReleased_CollectsStrandedSnapshots covers that
// any leaked resource in external storage is cleaned upon actor deletion because
// they're under the same storage prefix:
//   - objects that dont have an entry in the DB
//   - a suspend that died before it recorded anything in the DB
func TestEnsureExternalSnapshotsReleased_CollectsStrandedSnapshots(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	w, objects := newFinalizeWorkflow(persistence)

	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_DELETING},
	})
	otherActor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-2"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})

	currentSnapshot := mustActorSnapshotURI(t, template, actor, "current")
	strandedSnapshot := mustActorSnapshotURI(t, template, actor, "stranded")
	otherActorsSnapshot := mustActorSnapshotURI(t, template, otherActor, "current")
	for _, uri := range []resources.SnapshotURI{currentSnapshot, strandedSnapshot, otherActorsSnapshot} {
		objects.PutSnapshot(t, uri, "manifest.json")
	}
	actor = mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
		s.LastAssignedGeneration = 1
		s.Snapshots = []*ateapipb.Snapshot{
			newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", currentSnapshot.Name(), currentSnapshot.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
		}
	})

	if err := w.ensureExternalSnapshotsReleased(ctx, actor); err != nil {
		t.Fatalf("ensureExternalSnapshotsReleased: %v", err)
	}
	if remaining := objects.Snapshot(t, strandedSnapshot); len(remaining) != 0 {
		t.Errorf("stranded external snapshot %v was not released, %v remains", strandedSnapshot, remaining)
	}
	// Deletion should not release snapshots from other actors
	if len(objects.Snapshot(t, otherActorsSnapshot)) == 0 {
		t.Errorf("another actor's external snapshot %v was released", otherActorsSnapshot)
	}
}

// TestDeleteActor_CollectsSnapshotWithoutActorTemplate checks that snapshots
// are collected even if the actor template is gone.
func TestDeleteActor_CollectsInFlightSnapshotWithoutTemplate(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	objects := objectstoretest.New()
	w := NewActorWorkflow(persistence, nil, nil, nil, nil, "", nil, objectstoreplugintest.ControlClient(objects))

	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "gone-tmpl"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_CRASHED},
	})

	// The template that holds the storage location is gone (was never written to storage).
	// We should still be able to access/delete the current snapshot for this actor.
	inFlight := mustActorSnapshotURI(t, &ateapipb.ActorTemplate{
		SnapshotConfig: &ateapipb.SnapshotConfig{StorageLocation: testStorageLocation},
	}, actor, "abandoned")
	objects.PutSnapshot(t, inFlight, "manifest.json")
	mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
		s.LastAssignedGeneration = 1
		s.Snapshots = []*ateapipb.Snapshot{
			newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", inFlight.Name(), inFlight.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
		}
	})

	if _, err := w.DeleteActor(ctx, actorRef, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteActor: %v", err)
	}
	if left := objects.Prefix(t, inFlight.OwnerPrefix()); len(left) != 0 {
		t.Errorf("Deleting the actor left %v under its own prefix, want the in-flight snapshot collected", left)
	}
}

// TestEnsureExternalSnapshotsReleased_DeletePrefixFailure verifies that if
// objectstore.DeletePrefix fails with a transient error during actor deletion,
// ensureExternalSnapshotsReleased returns that error, preserves the un-deleted
// objects, and cleanly completes the deletion on a subsequent retry.
func TestEnsureExternalSnapshotsReleased_DeletePrefixFailure(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	w, objects := newFinalizeWorkflow(persistence)

	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_DELETING},
	})

	current := mustActorSnapshotURI(t, template, actor, "current")
	objects.PutSnapshot(t, current, "manifest.json", "memory.zst")
	actor = mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
		s.LastAssignedGeneration = 1
		s.Snapshots = []*ateapipb.Snapshot{
			newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", current.Name(), current.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
		}
	})

	errTransient := errors.New("simulated transient delete failure")
	objects.OnDelete = func(bucket, object string) error {
		return errTransient
	}

	err := w.ensureExternalSnapshotsReleased(ctx, actor)
	if err == nil || !strings.Contains(err.Error(), errTransient.Error()) {
		t.Fatalf("ensureExternalSnapshotsReleased error = %v, want it to report %v", err, errTransient)
	}

	// Objects should not have been deleted
	if len(objects.Snapshot(t, current)) == 0 {
		t.Fatal("objects were unexpectedly deleted despite OnDelete failure")
	}

	// Retry without failure: should successfully clean up the snapshot objects
	objects.OnDelete = nil
	if err := w.ensureExternalSnapshotsReleased(ctx, actor); err != nil {
		t.Fatalf("ensureExternalSnapshotsReleased on retry failed: %v", err)
	}
	if remaining := objects.Snapshot(t, current); len(remaining) != 0 {
		t.Errorf("external snapshot objects remain after retry: %v", remaining)
	}
}

// TestDeleteActor_CollectsSnapshotsAfterWorkerDelete verifies that
// deleting an actor whose suspend a worker delete crashed mid-finalize deletes
// every object that suspend wrote. When an actor crashes mid-suspend, only
// DeleteActor or RevertActor can delete the in-progress snapshot:
// whatever they cannot name is leaked for good.
func TestDeleteActor_CollectsSnapshotsAfterWorkerDelete(t *testing.T) {
	tests := []struct {
		name string
		// hasBeenSuspended means the actor was replacing a snapshot it took
		// itself, rather than suspending for the first time. Only the first
		// suspend needs the retained in-progress name to find what it wrote; a
		// replacement is already reachable through the snapshot it owns.
		hasBeenSuspended bool
	}{
		{
			name:             "replacing a snapshot the actor owned",
			hasBeenSuspended: true,
		},
		{
			name:             "first suspend, nothing to replace",
			hasBeenSuspended: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
			objects := objectstoretest.New()

			actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
			workerName := wireTestAssignment().GetWorker().GetName()
			actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
				Status: &ateapipb.ActorStatus{
					State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
					WorkerAssignment: wireTestAssignment(),
				},
			})
			seedWireWorker(t, ctx, persistence, assignmentOf(actor))

			if tt.hasBeenSuspended {
				previous := mustActorSnapshotURI(t, template, actor, "old")
				objects.PutSnapshot(t, previous, "manifest.json")
				mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
					s.LastAssignedGeneration = 1
					s.Snapshots = []*ateapipb.Snapshot{
						newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", previous.Name(), previous.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
					}
				})
			}

			actorWorkflow, atelet := newWireCaptureWorkflow(t, persistence)
			actorWorkflow.objectStorePlugin = objectstoreplugintest.ControlClient(objects)
			// atelet writes part of the snapshot, then the worker's pod goes
			// away mid-checkpoint, so the checkpoint fails and the suspend never
			// commits.
			var fresh resources.SnapshotURI
			atelet.onCheckpoint = func(context.Context) error {
				uri, err := resources.ParseSnapshotURI(atelet.checkpointRequest().GetSnapshot().GetObject().GetSnapshotUri())
				if err != nil {
					t.Errorf("ParseSnapshotURI: %v", err)
					return err
				}
				fresh = uri
				objects.PutSnapshot(t, fresh, "manifest.json")
				// The worker's pod goes away with the commit still outstanding, so
				// the suspend never gets to finish.
				if _, err := NewWorkerWorkflow(persistence).DeleteWorker(ctx, workerName, store.DeletePreconditions{}); err != nil {
					t.Errorf("DeleteWorker: %v", err)
					return err
				}
				return apierror.Unavailable("worker pod is gone")
			}
			if _, err := actorWorkflow.SuspendActor(ctx, actorRef, suspendSync); apierror.Code(err) != codes.Unavailable {
				t.Fatalf("SuspendActor err = %v, want Unavailable from the failed checkpoint", err)
			}

			// The actor is CRASHED; DeleteActor deletes the in-progress snapshot
			// (in_progress_snapshot_uri).
			stored, err := persistence.GetActor(ctx, actorRef)
			if err != nil {
				t.Fatalf("GetActor: %v", err)
			}
			if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_CRASHED {
				t.Fatalf("state = %v, want CRASHED", got)
			}
			if _, err := actorWorkflow.DeleteActor(ctx, actorRef, store.DeletePreconditions{}); err != nil {
				t.Fatalf("DeleteActor: %v", err)
			}
			if left := objects.Prefix(t, fresh.OwnerPrefix()); len(left) != 0 {
				t.Errorf("deleting the actor left %v under its own prefix, want everything it wrote collected", left)
			}
		})
	}
}
