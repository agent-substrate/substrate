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
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDeleteActorWorkflow_ExecutionPaths(t *testing.T) {
	tests := []struct {
		name        string
		seedState   ateapipb.ActorState
		anyState    bool
		missingTmpl bool
		wantErr     bool
		wantCode    codes.Code
	}{
		{
			name:      "delete suspended actor succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			anyState:  false,
			wantErr:   false,
		},
		{
			name:      "delete crashed actor succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_CRASHED,
			anyState:  false,
			wantErr:   false,
		},
		{
			name:      "delete deleting actor succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_DELETING,
			anyState:  false,
			wantErr:   false,
		},
		{
			name:      "delete running actor rejected when not any_state",
			seedState: ateapipb.ActorState_ACTOR_STATE_RUNNING,
			anyState:  false,
			wantErr:   true,
			wantCode:  codes.FailedPrecondition,
		},
		{
			name:      "delete paused actor rejected when not any_state",
			seedState: ateapipb.ActorState_ACTOR_STATE_PAUSED,
			anyState:  false,
			wantErr:   true,
			wantCode:  codes.FailedPrecondition,
		},
		{
			name:      "any_state delete suspended actor succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			anyState:  true,
			wantErr:   false,
		},
		{
			name:      "any_state delete running actor succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_RUNNING,
			anyState:  true,
			wantErr:   false,
		},
		{
			name:      "any_state delete paused actor succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_PAUSED,
			anyState:  true,
			wantErr:   false,
		},
		{
			name:      "any_state delete crashed actor succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_CRASHED,
			anyState:  true,
			wantErr:   false,
		},
		{
			name:        "delete suspended actor with missing template succeeds",
			seedState:   ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			anyState:    false,
			missingTmpl: true,
			wantErr:     false,
		},
		{
			name:        "any_state delete suspended actor with missing template succeeds",
			seedState:   ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			anyState:    true,
			missingTmpl: true,
			wantErr:     false,
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

			deleted, err := w.DeleteActor(ctx, actorRef, tc.anyState)
			if tc.wantErr {
				if got := status.Code(err); got != tc.wantCode {
					t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, tc.wantCode, err)
				}
			} else {
				if err != nil {
					t.Fatalf("DeleteActor failed: %v", err)
				}
				if deleted == nil {
					t.Fatalf("expected non-nil deleted actor")
				}
				if _, err := st.GetActor(ctx, actorRef); err == nil {
					t.Errorf("expected actor to be deleted from store, but it still exists")
				}
			}
		})
	}
}

func TestEnsureMarkedDeleting_StateMatrix(t *testing.T) {
	tests := []struct {
		name     string
		anyState bool
		allowed  map[ateapipb.ActorState]bool
	}{
		{
			name:     "standard delete",
			anyState: false,
			allowed: map[ateapipb.ActorState]bool{
				ateapipb.ActorState_ACTOR_STATE_SUSPENDED: true,
				ateapipb.ActorState_ACTOR_STATE_CRASHED:   true,
				ateapipb.ActorState_ACTOR_STATE_DELETING:  true, // skipped
			},
		},
		{
			name:     "any_state delete",
			anyState: true,
			allowed: map[ateapipb.ActorState]bool{
				ateapipb.ActorState_ACTOR_STATE_UNSPECIFIED: true,
				ateapipb.ActorState_ACTOR_STATE_RUNNING:     true,
				ateapipb.ActorState_ACTOR_STATE_RESUMING:    true,
				ateapipb.ActorState_ACTOR_STATE_SUSPENDING:  true,
				ateapipb.ActorState_ACTOR_STATE_PAUSING:     true,
				ateapipb.ActorState_ACTOR_STATE_PAUSED:      true,
				ateapipb.ActorState_ACTOR_STATE_SUSPENDED:   true,
				ateapipb.ActorState_ACTOR_STATE_CRASHED:     true,
				ateapipb.ActorState_ACTOR_STATE_DELETING:    true, // skipped
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
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

				updated, err := w.ensureMarkedDeleting(ctx, actorRef, actor, tc.anyState)
				assertPrerequisiteResult(t, seedState, err, tc.allowed[seedState])
				if err == nil && seedState != ateapipb.ActorState_ACTOR_STATE_DELETING {
					if updated.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_DELETING {
						t.Errorf("state %v: ensureMarkedDeleting returned actor in %v, want DELETING", seedState, updated.GetStatus().GetState())
					}
				}
				cleanup()
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
			if tt.tagOwnedSnapshot {
				current = mustTagSnapshotURI(t, template, "team-a", "v1-snapshot")
			}
			objects.PutSnapshot(t, current, "manifest.json")
			inFlight := mustActorSnapshotURI(t, template, actor, inFlightSnapshotName)
			if tt.inFlight != "" {
				objects.PutSnapshot(t, inFlight, "manifest.json")
			}
			actor = mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
				s.ExternalSnapshot = &ateapipb.ExternalSnapshot{SnapshotUri: current.String()}
				s.InProgressSnapshotName = tt.inFlight
			})

			if err := w.ensureExternalSnapshotsReleased(ctx, actor, template); err != nil {
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
		s.ExternalSnapshot = &ateapipb.ExternalSnapshot{SnapshotUri: currentSnapshot.String()}
	})

	if err := w.ensureExternalSnapshotsReleased(ctx, actor, template); err != nil {
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
