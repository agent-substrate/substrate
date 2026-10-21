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
	"slices"
	"strings"
	"testing"

	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/objectstore/objectstoretest"
	"go.ate.dev/substrate/internal/objectstoreplugin/objectstoreplugintest"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/volume"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// A deleted Node object counts as the node being gone for good, so revert and
// delete do not wait for its atelet. A node that is still there may get its
// atelet back, and a failed lookup proves nothing, so both stay retryable.
func TestEnsureAteletTerminated_NodeGone(t *testing.T) {
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
	actor := &ateapipb.Actor{Status: &ateapipb.ActorStatus{AssignedNode: "node1"}}
	for _, tc := range []struct {
		name      string
		nodes     []string
		lookupErr error
		wantErr   bool
	}{
		{name: "node gone", nodes: []string{"node2"}},
		{name: "node still there", nodes: []string{"node1"}, wantErr: true},
		{name: "node lookup fails", lookupErr: errors.New("connection refused"), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newTestActorWorkflow(t, newTestPersistence(t), "team-a", "tmpl")
			w.dialer = NewAteletDialer(newTestAteletIndexer(t), installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace), "", "",
				WithNodeGetter(nodeGetter(tc.lookupErr, tc.nodes...)))
			err := w.ensureAteletTerminated(context.Background(), actorRef, actor, nil, "revert")
			if tc.wantErr != (err != nil) {
				t.Fatalf("ensureAteletTerminated = %v, want error %v", err, tc.wantErr)
			}
			if tc.wantErr && !errors.Is(err, ErrNoAteletOnNode) {
				t.Fatalf("ensureAteletTerminated = %v, want ErrNoAteletOnNode", err)
			}
			if tc.lookupErr != nil && !strings.Contains(err.Error(), tc.lookupErr.Error()) {
				t.Fatalf("ensureAteletTerminated = %v, want the node lookup error in it", err)
			}
		})
	}
}

// detachDeletePlugin is a volume plugin for a whole delete: it records
// detaches like mockDetachVolumePlugin and accepts the volume delete.
type detachDeletePlugin struct{ mockDetachVolumePlugin }

func (*detachDeletePlugin) DeleteVolume(context.Context, string) error { return nil }

// TestRevertAndDelete_AssignedNodeGone runs the whole revert and delete
// workflows for a paused actor with a PENDING upload and a crashed actor whose
// assigned node has no atelet (#2470). If the Node object is gone, both finish,
// and revert clears assigned_node. If it still exists, both fail with
// ErrNoAteletOnNode and leave the actor REVERTING or DELETING for a retry.
// A volume detach on the gone node still runs: OK and NotFound from the plugin
// count as detached, and any other error fails the step as before.
func TestRevertAndDelete_AssignedNodeGone(t *testing.T) {
	memory := ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	completed := ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
	for _, tc := range []struct {
		name      string
		state     ateapipb.ActorState
		paused    bool
		nodeKept  bool
		volume    bool
		detachErr error
		wantErr   error // ErrNoAteletOnNode for a kept node
		// wantDetachErr expects the plugin's detach error back.
		wantDetachErr bool
	}{
		{name: "paused, node gone", state: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, paused: true},
		{name: "crashed, node gone", state: ateapipb.ActorState_ACTOR_STATE_CRASHED},
		{name: "paused, node kept", state: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, paused: true, nodeKept: true, wantErr: ErrNoAteletOnNode},
		{name: "crashed, node kept", state: ateapipb.ActorState_ACTOR_STATE_CRASHED, nodeKept: true, wantErr: ErrNoAteletOnNode},
		{name: "node gone, volume detach OK", state: ateapipb.ActorState_ACTOR_STATE_CRASHED, volume: true},
		{name: "node gone, volume detach NotFound", state: ateapipb.ActorState_ACTOR_STATE_CRASHED, volume: true, detachErr: status.Error(codes.NotFound, "no such node")},
		{name: "node gone, volume detach fails", state: ateapipb.ActorState_ACTOR_STATE_CRASHED, volume: true, detachErr: status.Error(codes.Internal, "instance not found"), wantDetachErr: true},
	} {
		for _, verb := range []string{"revert", "delete"} {
			t.Run(tc.name+"/"+verb, func(t *testing.T) {
				ctx := context.Background()
				st, cleanup := storetest.SetupTestStore(t)
				defer cleanup()
				w := newTestActorWorkflow(t, st, "ns", "tmpl1")
				nodes := []string{"node-2"}
				if tc.nodeKept {
					nodes = append(nodes, "node-1")
				}
				w.dialer = NewAteletDialer(newTestAteletIndexer(t), installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace), "", "",
					WithNodeGetter(nodeGetter(nil, nodes...)))
				plugin := &detachDeletePlugin{mockDetachVolumePlugin{detachErrs: map[string]error{"storage-vol-1": tc.detachErr}}}
				w.pluginRegistry = &mockPluginRegistry{plugins: map[string]volume.VolumePluginControlPlane{"mock": plugin}}
				tmpl := &ateapipb.ActorTemplate{
					Metadata:       &ateapipb.ResourceMetadata{Atespace: "ns", Name: "tmpl-vol"},
					SnapshotConfig: &ateapipb.SnapshotConfig{StorageLocation: "gs://snapshots"},
					Volumes:        []*ateapipb.Volume{{Name: "vol1"}},
					Containers:     []*ateapipb.Container{{VolumeMounts: []*ateapipb.VolumeMount{{Name: "vol1"}}}},
				}
				if _, err := st.CreateActorTemplate(ctx, tmpl); err != nil {
					t.Fatalf("create ActorTemplate: %v", err)
				}

				actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
				seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl-vol", tc.state)
				actor, err := st.GetActor(ctx, actorRef)
				if err != nil {
					t.Fatalf("get seeded actor: %v", err)
				}
				mustUpdateActorStatus(t, ctx, st, actor, func(s *ateapipb.ActorStatus) {
					s.AssignedNode = "node-1"
					s.LastAssignedGeneration = 1
					s.Snapshots = []*ateapipb.Snapshot{newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, memory, "", "kept",
						mustActorSnapshotURI(t, tmpl, actor, "kept").String(), completed)}
					if tc.paused {
						local := newPausedSnapshot(t, 2, memory, "", "local", "node-1")
						local.DurableSnapshot.Object.SnapshotUri = mustActorSnapshotURI(t, tmpl, actor, "local").String()
						s.LastAssignedGeneration = 2
						s.Snapshots = append(s.Snapshots, local)
					}
					if tc.volume {
						s.ExternalVolumes = &ateapipb.ExternalVolumes{Volumes: []*ateapipb.ExternalVolumeStatus{
							{Name: "vol1", StorageVolumeId: "storage-vol-1", VolumeType: "mock"}}}
					}
				})

				wantState := ateapipb.ActorState_ACTOR_STATE_REVERTING
				if verb == "revert" {
					_, err = w.RevertActor(ctx, actorRef)
				} else {
					wantState = ateapipb.ActorState_ACTOR_STATE_DELETING
					_, err = w.DeleteActor(ctx, actorRef, store.DeletePreconditions{})
				}
				switch {
				case tc.wantDetachErr:
					if err == nil || !strings.Contains(err.Error(), "instance not found") {
						t.Fatalf("%s = %v, want the detach error", verb, err)
					}
				case tc.wantErr != nil:
					if !errors.Is(err, tc.wantErr) {
						t.Fatalf("%s = %v, want %v", verb, err, tc.wantErr)
					}
				case err != nil:
					t.Fatalf("%s: %v", verb, err)
				}
				if tc.volume {
					if want := []detachCall{{VolumeID: "storage-vol-1", Node: "node-1"}}; !slices.Equal(plugin.detachCalls, want) {
						t.Errorf("detach calls = %v, want %v", plugin.detachCalls, want)
					}
				}

				got, getErr := st.GetActor(ctx, actorRef)
				switch {
				case tc.wantErr != nil || tc.wantDetachErr:
					if getErr != nil {
						t.Fatalf("get actor after failed %s: %v", verb, getErr)
					}
					if s := got.GetStatus(); s.GetState() != wantState || s.GetAssignedNode() != "node-1" {
						t.Errorf("after failed %s: state %v, assigned node %q, want %v on node-1", verb, s.GetState(), s.GetAssignedNode(), wantState)
					}
				case verb == "revert":
					if getErr != nil {
						t.Fatalf("get actor after revert: %v", getErr)
					}
					if s := got.GetStatus(); s.GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED || s.GetAssignedNode() != "" {
						t.Errorf("after revert: state %v, assigned node %q, want SUSPENDED with no node", s.GetState(), s.GetAssignedNode())
					}
					if snaps := got.GetStatus().GetSnapshots(); len(snaps) != 1 || snaps[0].GetGeneration() != 1 || snaps[0].GetDurableSnapshot().GetStatus() != completed {
						t.Errorf("after revert: snapshots %v, want only the completed durable snapshot at generation 1", snaps)
					}
				default:
					if !errors.Is(getErr, store.ErrNotFound) {
						t.Errorf("get actor after delete = %v, want ErrNotFound", getErr)
					}
				}
			})
		}
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
