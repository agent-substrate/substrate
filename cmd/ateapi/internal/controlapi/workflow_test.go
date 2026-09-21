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
	"maps"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/actorevent"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// TestActorStateChangeRecords drives each transition that commits a new state
// and pins the record it writes. The state a consumer reads has to be the state
// the store now holds, so each case asserts both.
func TestActorStateChangeRecords(t *testing.T) {
	const (
		tmplAtespace = "ns"
		tmplName     = "tmpl1"
	)

	tests := []struct {
		name      string
		seedState ateapipb.ActorState
		// transition runs the workflow step that commits the new state.
		transition func(t *testing.T, w *ActorWorkflow, ref resources.ActorRef, actor *ateapipb.Actor, tmpl *ateapipb.ActorTemplate)
		wantOp     string
		wantState  string
		wantStored ateapipb.ActorState
	}{
		{
			name:      "suspend marks the actor suspending",
			seedState: ateapipb.ActorState_ACTOR_STATE_RUNNING,
			transition: func(t *testing.T, w *ActorWorkflow, ref resources.ActorRef, actor *ateapipb.Actor, tmpl *ateapipb.ActorTemplate) {
				if _, err := w.ensureMarkedSuspending(context.Background(), ref, actor, tmpl); err != nil {
					t.Fatalf("ensureMarkedSuspending: %v", err)
				}
			},
			wantOp:     ateattr.OperationSuspend,
			wantState:  ateattr.ActorStateSuspending,
			wantStored: ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
		},
		{
			name:      "pause marks the actor pausing",
			seedState: ateapipb.ActorState_ACTOR_STATE_RUNNING,
			transition: func(t *testing.T, w *ActorWorkflow, ref resources.ActorRef, actor *ateapipb.Actor, _ *ateapipb.ActorTemplate) {
				if _, err := w.ensureMarkedPausing(context.Background(), ref, actor); err != nil {
					t.Fatalf("ensureMarkedPausing: %v", err)
				}
			},
			wantOp:     ateattr.OperationPause,
			wantState:  ateattr.ActorStatePausing,
			wantStored: ateapipb.ActorState_ACTOR_STATE_PAUSING,
		},
		{
			name:      "delete marks the actor deleting",
			seedState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			transition: func(t *testing.T, w *ActorWorkflow, ref resources.ActorRef, actor *ateapipb.Actor, _ *ateapipb.ActorTemplate) {
				if _, err := w.ensureMarkedDeleting(context.Background(), ref, actor, false); err != nil {
					t.Fatalf("ensureMarkedDeleting: %v", err)
				}
			},
			wantOp:     ateattr.OperationDelete,
			wantState:  ateattr.ActorStateDeleting,
			wantStored: ateapipb.ActorState_ACTOR_STATE_DELETING,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			records := logRecords(t, actorevent.StateChanged.Body)
			events := otlpEvents(t)

			persistence := newTestPersistence(t)
			storetest.MustCreateAtespace(t, ctx, persistence, tmplAtespace)
			if _, err := persistence.CreateActorTemplate(ctx, &ateapipb.ActorTemplate{
				Metadata: &ateapipb.ResourceMetadata{Atespace: tmplAtespace, Name: tmplName},
				SnapshotsConfig: &ateapipb.SnapshotsConfig{
					StorageLocation: testStorageLocation,
					OnPause:         ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
				},
			}); err != nil {
				t.Fatalf("create template: %v", err)
			}

			actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
			seedWorkflowActor(t, ctx, persistence, actorRef, tmplAtespace, tmplName, tt.seedState)
			actor, err := persistence.GetActor(ctx, actorRef)
			if err != nil {
				t.Fatalf("get actor: %v", err)
			}
			tmpl, err := persistence.GetActorTemplate(ctx, resources.ActorTemplateRef{Atespace: tmplAtespace, Name: tmplName})
			if err != nil {
				t.Fatalf("get template: %v", err)
			}

			w := &ActorWorkflow{store: persistence}
			tt.transition(t, w, actorRef, actor, tmpl)

			if len(*records) != 1 {
				t.Fatalf("got %d state records, want 1: %v", len(*records), *records)
			}
			got := (*records)[0].attrs
			want := map[string]string{
				string(ateattr.AtespaceKey):           actorRef.Atespace,
				string(ateattr.ActorNameKey):          actorRef.Name,
				string(ateattr.ActorUIDKey):           actor.GetMetadata().GetUid(),
				string(ateattr.TemplateAtespaceKey):   tmplAtespace,
				string(ateattr.TemplateNameKey):       tmplName,
				string(ateattr.ActorOperationNameKey): tt.wantOp,
				string(ateattr.ActorStateKey):         tt.wantState,
			}
			for k, wv := range want {
				if got[k] != wv {
					t.Errorf("%s = %q, want %q", k, got[k], wv)
				}
			}
			if len(got) != len(want) {
				t.Errorf("got %d attributes, want %d: %v", len(got), len(want), got)
			}

			// The OTLP copy is the same record under an event name. One call writes
			// both, so anything either copy holds alone is a bug in actorevent.Log.
			gotEvents := events()
			if len(gotEvents) != 1 {
				t.Fatalf("got %d state events, want 1: %v", len(gotEvents), gotEvents)
			}
			if gotEvents[0].name != actorevent.StateChanged.Name {
				t.Errorf("event name = %q, want %q", gotEvents[0].name, actorevent.StateChanged.Name)
			}
			if !maps.Equal(gotEvents[0].attrs, got) {
				t.Errorf("state event attributes = %v, want the stdout record's %v", gotEvents[0].attrs, got)
			}
			assertCopiesAgree(t, (*records)[0], gotEvents[0], actorevent.StateChanged)

			stored, err := persistence.GetActor(ctx, actorRef)
			if err != nil {
				t.Fatalf("reload actor: %v", err)
			}
			if stored.GetStatus().GetState() != tt.wantStored {
				t.Errorf("stored state = %v, want %v", stored.GetStatus().GetState(), tt.wantStored)
			}
		})
	}
}

// TestActorCreatedRecord covers creation. An actor created and never resumed
// makes no other transition, so without this it has no record at all, at any
// retention.
func TestActorCreatedRecord(t *testing.T) {
	ctx := context.Background()
	records := logRecords(t, actorevent.StateChanged.Body)

	persistence := newTestPersistence(t)
	storetest.MustCreateAtespace(t, ctx, persistence, "ns")
	storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "id1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns", Name: "tmpl1"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})
	stored, err := persistence.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
	if err != nil {
		t.Fatalf("get actor: %v", err)
	}

	logActorStateChanged(ctx, stored, ateattr.OperationCreate)

	if len(*records) != 1 {
		t.Fatalf("got %d state records, want 1: %v", len(*records), *records)
	}
	got := (*records)[0].attrs
	if got[string(ateattr.ActorOperationNameKey)] != ateattr.OperationCreate {
		t.Errorf("operation = %q, want %q", got[string(ateattr.ActorOperationNameKey)], ateattr.OperationCreate)
	}
	// A new actor is born suspended, and the record has to say so rather than
	// inventing a "created" state the store does not have.
	if got[string(ateattr.ActorStateKey)] != ateattr.ActorStateSuspended {
		t.Errorf("state = %q, want %q", got[string(ateattr.ActorStateKey)], ateattr.ActorStateSuspended)
	}
}

// TestActorDeletedRecord covers the terminal record. Without it "deleting" is
// the last thing a deleted actor ever reports, and a consumer cannot tell a
// finished delete from one that is stuck.
func TestActorDeletedRecord(t *testing.T) {
	ctx := context.Background()
	records := logRecords(t, actorevent.StateChanged.Body)

	persistence := newTestPersistence(t)
	storetest.MustCreateAtespace(t, ctx, persistence, "ns")
	if _, err := persistence.CreateActorTemplate(ctx, &ateapipb.ActorTemplate{
		Metadata:        &ateapipb.ResourceMetadata{Atespace: "ns", Name: "tmpl1"},
		SnapshotsConfig: &ateapipb.SnapshotsConfig{StorageLocation: testStorageLocation},
	}); err != nil {
		t.Fatalf("create template: %v", err)
	}

	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
	seedWorkflowActor(t, ctx, persistence, actorRef, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_DELETING)
	actor, err := persistence.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("get actor: %v", err)
	}

	w := &ActorWorkflow{store: persistence}
	if _, err := w.finalizeDeleted(ctx, actorRef); err != nil {
		t.Fatalf("finalizeDeleted: %v", err)
	}

	if len(*records) != 1 {
		t.Fatalf("got %d state records, want 1: %v", len(*records), *records)
	}
	got := (*records)[0].attrs
	if got[string(ateattr.ActorStateKey)] != ateattr.ActorStateDeleted {
		t.Errorf("state = %q, want %q", got[string(ateattr.ActorStateKey)], ateattr.ActorStateDeleted)
	}
	// The identity has to survive the row it described, or the terminal record
	// cannot be joined to the rest of the actor's history.
	if got[string(ateattr.ActorUIDKey)] != actor.GetMetadata().GetUid() {
		t.Errorf("uid = %q, want %q", got[string(ateattr.ActorUIDKey)], actor.GetMetadata().GetUid())
	}

	if _, err := persistence.GetActor(ctx, actorRef); err == nil {
		t.Error("actor still readable after finalizeDeleted")
	}
}

// TestActorStateChangeRecordSkippedOnConflict pins that a transition which did
// not commit writes nothing. A losing writer that still logged would put a state
// in the stream the store never held.
func TestActorStateChangeRecordSkippedOnConflict(t *testing.T) {
	ctx := context.Background()
	records := logRecords(t, actorevent.StateChanged.Body)

	persistence := newTestPersistence(t)
	storetest.MustCreateAtespace(t, ctx, persistence, "ns")
	if _, err := persistence.CreateActorTemplate(ctx, &ateapipb.ActorTemplate{
		Metadata:        &ateapipb.ResourceMetadata{Atespace: "ns", Name: "tmpl1"},
		SnapshotsConfig: &ateapipb.SnapshotsConfig{StorageLocation: testStorageLocation},
	}); err != nil {
		t.Fatalf("create template: %v", err)
	}

	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
	seedWorkflowActor(t, ctx, persistence, actorRef, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	stale, err := persistence.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("get actor: %v", err)
	}

	// Bump the stored version so the workflow's precondition is stale.
	if _, err := persistence.UpdateActor(ctx, actorRef, store.PreconditionFrom(stale), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.InProgressSnapshotUri = someActorSnapshotURI(t, testStorageLocation, "team-a", "someone-else")
		return nil
	}); err != nil {
		t.Fatalf("bump version: %v", err)
	}

	w := &ActorWorkflow{store: persistence}
	if _, err := w.ensureMarkedDeleting(ctx, actorRef, stale, false); err == nil {
		t.Fatal("ensureMarkedDeleting on a stale actor = nil, want a conflict")
	}
	if len(*records) != 0 {
		t.Errorf("got %d state records from a losing writer, want 0: %v", len(*records), *records)
	}
}
