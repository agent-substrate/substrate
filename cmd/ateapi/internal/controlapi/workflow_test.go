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

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/actorevent"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
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
		seedState ateapi.ActorState
		seedOpts  []func(*ateapi.Actor)
		// transition runs the workflow step that commits the new state.
		transition func(t *testing.T, w *ActorWorkflow, ref resources.ActorRef, actor *ateapi.Actor, tmpl *ateapi.ActorTemplate)
		wantOp     string
		wantState  string
		wantStored ateapi.ActorState
	}{
		{
			name:      "suspend marks the actor suspending",
			seedState: ateapi.ActorState_ACTOR_STATE_RUNNING,
			transition: func(t *testing.T, w *ActorWorkflow, ref resources.ActorRef, actor *ateapi.Actor, _ *ateapi.ActorTemplate) {
				markOnly(t, w, ref, actor, suspendSync)
			},
			wantOp:     ateattr.OperationSuspend,
			wantState:  ateattr.ActorStateSuspending,
			wantStored: ateapi.ActorState_ACTOR_STATE_SUSPENDING,
		},
		{
			name:      "pause marks the actor suspending",
			seedState: ateapi.ActorState_ACTOR_STATE_RUNNING,
			transition: func(t *testing.T, w *ActorWorkflow, ref resources.ActorRef, actor *ateapi.Actor, _ *ateapi.ActorTemplate) {
				markOnly(t, w, ref, actor, suspendAsync)
			},
			wantOp:     ateattr.OperationSuspend,
			wantState:  ateattr.ActorStateSuspending,
			wantStored: ateapi.ActorState_ACTOR_STATE_SUSPENDING,
		},
		{
			name:      "finalize pause marks the actor suspended",
			seedState: ateapi.ActorState_ACTOR_STATE_SUSPENDING,
			seedOpts: []func(*ateapi.Actor){
				withPauseInProgress(t),
				func(a *ateapi.Actor) {
					a.Status.AssignedNode = "node-1"
					a.Status.WorkerAssignment = wireTestAssignment()
				},
			},
			transition: func(t *testing.T, w *ActorWorkflow, ref resources.ActorRef, _ *ateapi.Actor, _ *ateapi.ActorTemplate) {
				if _, err := w.SuspendActor(context.Background(), ref, suspendAsync); err != nil {
					t.Fatalf("SuspendActor: %v", err)
				}
			},
			wantOp:     ateattr.OperationSuspend,
			wantState:  ateattr.ActorStateSuspended,
			wantStored: ateapi.ActorState_ACTOR_STATE_SUSPENDED,
		},
		{
			name:      "delete marks the actor deleting",
			seedState: ateapi.ActorState_ACTOR_STATE_SUSPENDED,
			transition: func(t *testing.T, w *ActorWorkflow, ref resources.ActorRef, _ *ateapi.Actor, _ *ateapi.ActorTemplate) {
				if _, err := w.ensureMarkedDeleting(context.Background(), ref, store.DeletePreconditions{}); err != nil {
					t.Fatalf("ensureMarkedDeleting: %v", err)
				}
				// A retry finds the actor already marked and writes no second record.
				if _, err := w.ensureMarkedDeleting(context.Background(), ref, store.DeletePreconditions{}); err != nil {
					t.Fatalf("retried ensureMarkedDeleting: %v", err)
				}
			},
			wantOp:     ateattr.OperationDelete,
			wantState:  ateattr.ActorStateDeleting,
			wantStored: ateapi.ActorState_ACTOR_STATE_DELETING,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			records := logRecords(t, actorevent.StateChanged.Body)
			events := otlpEvents(t)

			persistence := newTestPersistence(t)
			storetest.MustCreateAtespace(t, ctx, persistence, tmplAtespace)
			if _, err := persistence.CreateActorTemplate(ctx, &ateapi.ActorTemplate{
				Metadata: &ateapi.ResourceMetadata{Atespace: tmplAtespace, Name: tmplName},
				SnapshotConfig: &ateapi.SnapshotConfig{
					StorageLocation: testStorageLocation,
				},
			}); err != nil {
				t.Fatalf("create template: %v", err)
			}

			actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
			seedWorkflowActor(t, ctx, persistence, actorRef, tmplAtespace, tmplName, tt.seedState, tt.seedOpts...)
			actor, err := persistence.GetActor(ctx, actorRef)
			if err != nil {
				t.Fatalf("get actor: %v", err)
			}
			tmpl, err := persistence.GetActorTemplate(ctx, resources.ActorTemplateRef{Atespace: tmplAtespace, Name: tmplName})
			if err != nil {
				t.Fatalf("get template: %v", err)
			}

			w, _ := newWireCaptureWorkflow(t, persistence)
			tt.transition(t, w, actorRef, actor, tmpl)

			// The logs exporter is on, so the record goes to OTLP only.
			if len(*records) != 0 {
				t.Errorf("got %d stdout state records with the exporter on, want 0: %v", len(*records), *records)
			}
			gotEvents := events()
			if len(gotEvents) != 1 {
				t.Fatalf("got %d state events, want 1: %v", len(gotEvents), gotEvents)
			}
			assertEvent(t, gotEvents[0], actorevent.StateChanged)
			got := gotEvents[0].attrs
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

// markOnly runs a suspend (sync or async) on a RUNNING actor only as far as its
// mark step: the actor gets a worker to checkpoint on, but the atelet dial
// fails, so the SUSPENDING mark is the only transition the workflow commits.
func markOnly(t *testing.T, w *ActorWorkflow, ref resources.ActorRef, actor *ateapi.Actor, mode suspendMode) {
	t.Helper()
	ctx := context.Background()
	if _, err := w.store.UpdateActor(ctx, ref, store.PreconditionFrom(actor), func(toUpdate *ateapi.Actor) error {
		toUpdate.Status.WorkerAssignment = wireTestAssignment()
		return nil
	}); err != nil {
		t.Fatalf("UpdateActor: %v", err)
	}
	w.dialer = newDanglingDialer()
	_, err := w.SuspendActor(ctx, ref, mode)
	assertStoppedAtDial(t, err)
}

// TestActorCreatedRecord covers creation. An actor created and never resumed
// makes no other transition, so without this it has no record at all, at any
// retention.
func TestActorCreatedRecord(t *testing.T) {
	ctx := context.Background()
	records := logRecords(t, actorevent.StateChanged.Body)

	persistence := newTestPersistence(t)
	storetest.MustCreateAtespace(t, ctx, persistence, "ns")
	storetest.MustCreateActor(t, ctx, persistence, &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: "team-a", Name: "id1"},
		ActorTemplate: &ateapi.ObjectRef{Atespace: "ns", Name: "tmpl1"},
		Status:        &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED},
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
	if _, err := persistence.CreateActorTemplate(ctx, &ateapi.ActorTemplate{
		Metadata:       &ateapi.ResourceMetadata{Atespace: "ns", Name: "tmpl1"},
		SnapshotConfig: &ateapi.SnapshotConfig{StorageLocation: testStorageLocation},
	}); err != nil {
		t.Fatalf("create template: %v", err)
	}

	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
	seedWorkflowActor(t, ctx, persistence, actorRef, "ns", "tmpl1", ateapi.ActorState_ACTOR_STATE_DELETING)
	actor, err := persistence.MarkActorForDeletion(ctx, actorRef, store.DeletePreconditions{}, nil)
	if err != nil {
		t.Fatalf("mark actor: %v", err)
	}

	w := &ActorWorkflow{store: persistence}
	if _, err := w.finalizeDeleted(ctx, actor); err != nil {
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
	if _, err := persistence.CreateActorTemplate(ctx, &ateapi.ActorTemplate{
		Metadata:       &ateapi.ResourceMetadata{Atespace: "ns", Name: "tmpl1"},
		SnapshotConfig: &ateapi.SnapshotConfig{StorageLocation: testStorageLocation},
	}); err != nil {
		t.Fatalf("create template: %v", err)
	}

	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
	seedWorkflowActor(t, ctx, persistence, actorRef, "ns", "tmpl1", ateapi.ActorState_ACTOR_STATE_RUNNING)
	stale, err := persistence.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("get actor: %v", err)
	}

	// Bump the stored version so the workflow's precondition is stale.
	if _, err := persistence.UpdateActor(ctx, actorRef, store.PreconditionFrom(stale), func(toUpdate *ateapi.Actor) error {
		toUpdate.Status.LastAssignedGeneration = 1
		toUpdate.Status.Snapshots = []*ateapi.Snapshot{
			newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "someone-else", someActorSnapshotURI(t, testStorageLocation, "team-a", "someone-else"), ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
		}
		return nil
	}); err != nil {
		t.Fatalf("bump version: %v", err)
	}

	w := &ActorWorkflow{store: persistence}
	if _, err := w.ensureMarkedDeleting(ctx, actorRef, store.DeletePreconditions{Version: stale.GetMetadata().GetVersion()}); apierror.Code(err) != codes.Aborted {
		t.Fatalf("ensureMarkedDeleting with a stale guard = %v, want ABORTED", err)
	}
	if len(*records) != 0 {
		t.Errorf("got %d state records from a losing writer, want 0: %v", len(*records), *records)
	}
}
