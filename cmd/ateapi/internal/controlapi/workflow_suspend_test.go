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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/installdefaults"
	"github.com/agent-substrate/substrate/internal/objectstore/objectstoretest"
	"github.com/agent-substrate/substrate/internal/objectstoreplugin/objectstoreplugintest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"k8s.io/client-go/tools/cache"
)

func TestEnsureMarkedSuspending_SnapshotURI(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})
	tmpl := &ateapipb.ActorTemplate{
		SnapshotConfig: &ateapipb.SnapshotConfig{StorageLocation: "gs://bucket/root/"},
	}
	w := &ActorWorkflow{store: persistence}
	marked, err := w.ensureMarkedSuspending(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"}, actor, tmpl)
	if err != nil {
		t.Fatalf("ensureMarkedSuspending: %v", err)
	}

	inProgressSnap := findLatestDurableSnapshot(marked.GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
	uri := mustParseSnapshotURI(t, inProgressSnap.GetDurableSnapshot().GetObject().GetSnapshotUri())
	if !resources.IsValidResourceName(uri.Name()) {
		t.Errorf("in-progress snapshot name = %q, want a valid resource name", uri.Name())
	}
	want := "gs://bucket/root/atespaces/team-a/actors/" + marked.GetMetadata().GetUid() + "/snapshots/" + uri.Name()
	if uri.String() != want {
		t.Errorf("snapshot URI = %q, want %q", uri, want)
	}
}

// TestEnsureMarkedSuspending_ReentryKeepsPersistedSnapshotLocation verifies a
// re-entered workflow does not mint a second snapshot location: the location
// persisted by the first attempt stays authoritative.
func TestEnsureMarkedSuspending_ReentryKeepsPersistedSnapshotLocation(t *testing.T) {
	const firstAttempt = "gs://bucket/root/atespaces/team-a/actors/actor-uid/snapshots/first-attempt"

	ctx := context.Background()
	persistence := newTestPersistence(t)
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			LastAssignedGeneration: 1,
			Snapshots: []*ateapipb.Snapshot{
				newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "first-attempt", firstAttempt, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
			},
		},
	})
	w := &ActorWorkflow{store: persistence}
	marked, err := w.ensureMarkedSuspending(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"}, actor, &ateapipb.ActorTemplate{})
	if err != nil {
		t.Fatalf("ensureMarkedSuspending: %v", err)
	}
	if gotSnap := findLatestDurableSnapshot(marked.GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING); gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri() != firstAttempt {
		t.Errorf("in-progress durable snapshot URI = %q, want the first attempt's location", gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri())
	}
}

// TestSuspendActorWorkflow_RejectedAndIdempotentPaths covers the two
// short-circuit paths of the suspend workflow: rejection of the suspend edge
// for a non-RUNNING actor and the idempotent fast-forward for a SUSPENDED one.
func TestSuspendActorWorkflow_RejectedAndIdempotentPaths(t *testing.T) {
	tests := []struct {
		name      string
		seedState ateapipb.ActorState
		// wantErr true means SuspendActor must fail with FailedPrecondition.
		wantErr bool
		// wantState is the stored state after the call.
		wantState ateapipb.ActorState
	}{
		{
			// Suspending a SUSPENDED actor succeeds idempotently via
			// IsComplete fast-forward without calling atelet.
			name:      "newly created suspended succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			wantState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			w := newTestActorWorkflow(t, st, "ns", "tmpl1")

			seedWorkflowActor(t, ctx, st, resources.ActorRef{Atespace: "team-a", Name: "id1"}, "ns", "tmpl1", tc.seedState)

			actor, err := w.SuspendActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
			if tc.wantErr {
				if got := apierror.Code(err); got != codes.FailedPrecondition {
					t.Fatalf("apierror.Code(err) = %v, want %v (err: %v)", got, codes.FailedPrecondition, err)
				}
			} else {
				if err != nil {
					t.Fatalf("SuspendActor failed: %v", err)
				}
				if actor.GetStatus().GetState() != tc.wantState {
					t.Errorf("returned state = %v, want %v", actor.GetStatus().GetState(), tc.wantState)
				}
			}

			got, err := st.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
			if err != nil {
				t.Fatalf("GetActor failed: %v", err)
			}
			if got.GetStatus().GetState() != tc.wantState {
				t.Errorf("stored state = %v, want %v", got.GetStatus().GetState(), tc.wantState)
			}
		})
	}
}

// TestEnsureMarkedSuspending_StateMatrix verifies the suspend edge's state
// gating against every actor state: RUNNING takes the edge (checkpoint the
// workload), PAUSED takes it too (upload the node-local pause snapshot),
// SUSPENDING skips (a previous attempt already marked the actor), everything
// else is rejected with FailedPrecondition. SUSPENDED is rejected here
// because the orchestrator early-returns before this step for a fully
// suspended actor.
func TestEnsureMarkedSuspending_StateMatrix(t *testing.T) {
	allowed := map[ateapipb.ActorState]bool{
		ateapipb.ActorState_ACTOR_STATE_RUNNING:    true,
		ateapipb.ActorState_ACTOR_STATE_PAUSED:     true,
		ateapipb.ActorState_ACTOR_STATE_SUSPENDING: true, // skipped, not re-marked
	}

	for _, seedState := range allActorStates {
		ctx := context.Background()
		persistence := newTestPersistence(t)
		w := &ActorWorkflow{store: persistence}

		actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
		actorStatus := &ateapipb.ActorStatus{State: seedState}
		if seedState == ateapipb.ActorState_ACTOR_STATE_PAUSED {
			actorStatus.LastAssignedGeneration = 1
			actorStatus.Snapshots = []*ateapipb.Snapshot{
				newLocalSnapshot(1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "snap-1", "node-1"),
			}
		}
		actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
			Status:   actorStatus,
		})

		tmpl := &ateapipb.ActorTemplate{
			SnapshotConfig: &ateapipb.SnapshotConfig{StorageLocation: "gs://snapshots"},
		}
		marked, err := w.ensureMarkedSuspending(ctx, actorRef, actor, tmpl)
		assertPrerequisiteResult(t, seedState, err, allowed[seedState])
		if err == nil && marked.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDING {
			t.Errorf("state %v: ensureMarkedSuspending returned actor in %v, want SUSPENDING", seedState, marked.GetStatus().GetState())
		}
		if seedState == ateapipb.ActorState_ACTOR_STATE_PAUSED && err == nil {
			wantURI, err := resources.NewActorSnapshotURI("gs://snapshots", actorRef.Atespace, actor.GetMetadata().GetUid(), "snap-1")
			if err != nil {
				t.Fatalf("NewActorSnapshotURI: %v", err)
			}
			gotURI := findLatestDurableSnapshot(marked.GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING).GetDurableSnapshot().GetObject().GetSnapshotUri()
			if gotURI != wantURI.String() {
				t.Errorf("paused-origin in-progress SnapshotUri = %q, want %q (derived from local snapshot uuid)", gotURI, wantURI.String())
			}
		}
	}
}

// TestSuspendActor_CrashesWhenSuspendingActorMissingWorkerPod verifies that a
// SUSPENDING actor with no worker pod recorded is moved to CRASHED by
// CallAteletSuspendStep's prerequisite check and the suspend fails.
func TestSuspendActor_CrashesWhenSuspendingActorMissingWorkerPod(t *testing.T) {
	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	w := newTestActorWorkflow(t, st, "ns", "tmpl1")

	seedWorkflowActor(t, ctx, st, resources.ActorRef{Atespace: "team-a", Name: "id1"}, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_SUSPENDING)

	if _, err := w.SuspendActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}); err == nil {
		t.Fatal("SuspendActor succeeded, want error for SUSPENDING actor with no worker pod")
	}

	got, err := st.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("stored state = %v, want %v", got.GetStatus().GetState(), ateapipb.ActorState_ACTOR_STATE_CRASHED)
	}
	if msg, want := got.GetStatus().GetCrash().GetMessage(), "suspend failed: "+crashMessageWorkerAssignmentMissing; msg != want {
		t.Errorf("crash message = %q, want %q", msg, want)
	}
}

// newTestPersistence returns an isolated PostgreSQL-backed store.
func newTestPersistence(t *testing.T) store.Interface {
	persistence, _ := storetest.SetupTestStore(t)
	storetest.MustCreateAtespace(t, context.Background(), persistence, "team-a")
	return persistence
}

// newDanglingDialer returns a dialer whose informer cache has no pods, so
// DialForAteletOnNode always returns ErrNoAteletOnNode.
func newDanglingDialer() *AteletDialer {
	empty := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		byNode: func(obj any) ([]string, error) { return nil, nil },
	})
	return NewAteletDialer(empty, installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace), "", "")
}

func TestEnsureAteletSuspended_DialFailureLeavesActorRetryable(t *testing.T) {
	neverWritten := someActorSnapshotURI(t, testStorageLocation, "team-a", "never-written")

	tests := []struct {
		name         string
		prevSnapshot string
	}{
		{
			name:         "keeps previous external snapshot",
			prevSnapshot: someActorSnapshotURI(t, testStorageLocation, "team-a", "prev"),
		},
		{
			name:         "stays empty without previous external snapshot",
			prevSnapshot: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			var snapshots []*ateapipb.Snapshot
			if tt.prevSnapshot != "" {
				snapshots = append(snapshots, newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "prev-snapshot", tt.prevSnapshot, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED))
			}
			snapshots = append(snapshots, newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "never-written", neverWritten, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING))

			actor := &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
				Status: &ateapipb.ActorStatus{
					State: ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
					WorkerAssignment: &ateapipb.WorkerAssignment{
						WorkerNamespace: "worker-ns",
						WorkerPool:      "pool",
						WorkerPod:       "pod-gone",
						NodeName:        "node-gone",
					},
					LastAssignedGeneration: 2,
					Snapshots:              snapshots,
				},
			}
			created := storetest.MustCreateActor(t, ctx, persistence, actor)

			w := &ActorWorkflow{store: persistence, dialer: newDanglingDialer()}
			if _, err := w.ensureAteletSuspended(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"}, created, &ateapipb.ActorTemplate{}); err == nil {
				t.Fatal("ensureAteletSuspended: want error when atelet is unreachable, got nil")
			}

			// A dial failure is transient from this workflow's point of view: it
			// must not crash the actor or touch its snapshot state. A worker that
			// is genuinely gone is handled by the DeleteWorker workflow instead.
			stored, err := persistence.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"})
			if err != nil {
				t.Fatalf("GetActor: %v", err)
			}
			if stored.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDING {
				t.Errorf("state = %v, want unchanged SUSPENDING", stored.GetStatus().GetState())
			}
			if gotSnap := findLatestDurableSnapshot(stored.GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING); gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri() != neverWritten {
				t.Errorf("in-progress durable snapshot URI = %q, want preserved for debugging", gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri())
			}
			gotSnap := findLatestDurableSnapshot(stored.GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
			if got := gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri(); got != tt.prevSnapshot {
				t.Errorf("SnapshotUri = %q, want %q", got, tt.prevSnapshot)
			}
		})
	}
}

// TestEnsureSuspendedFinalized_NoAssignment verifies finalization runs even when
// the actor has no worker assignment: the external snapshot must be recorded on
// the actor and the actor moved to SUSPENDED rather than silently left
// SUSPENDING. This is the shape a paused-origin suspend (#791) produces — a
// PAUSED actor has no worker — and the regression test for finalization
// previously living inside the worker-freeing branch.
func TestEnsureSuspendedFinalized_NoAssignment(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)

	snapshotURI := someActorSnapshotURI(t, testStorageLocation, "team-a", "2026-01-01t00-00-00z-abc")
	snap := newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-uid-1", "actor-1-pause-snapshot", snapshotURI, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
	snap.Locality = "node1"
	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			AssignedNode:           "node1",
			LastAssignedGeneration: 1,
			Snapshots:              []*ateapipb.Snapshot{snap},
		},
	}
	storetest.MustCreateActor(t, ctx, persistence, actor)

	w := &ActorWorkflow{store: persistence}
	stored, err := w.ensureSuspendedFinalized(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"})
	if err != nil {
		t.Fatalf("ensureSuspendedFinalized: %v", err)
	}

	if stored.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("state = %v, want SUSPENDED", stored.GetStatus().GetState())
	}
	gotSnap := findLatestDurableSnapshot(stored.GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	if got := gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri(); got != snapshotURI {
		t.Errorf("SnapshotUri = %q, want %q", got, snapshotURI)
	}
	// The snapshot carries the template it was captured under, so a later
	// repoint can tell that its guest state no longer matches.
	if got := gotSnap.GetActorTemplateUid(); got != "tmpl-uid-1" {
		t.Errorf("Snapshot.ActorTemplateUid = %q, want %q", got, "tmpl-uid-1")
	}
	if got := findLatestDurableSnapshot(stored.GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING); got != nil {
		t.Errorf("in-progress durable snapshot = %v, want cleared", got)
	}
	if local := findLatestLocalSnapshot(stored.GetStatus()); local != nil {
		t.Errorf("LocalSnapshot = %v, want cleared", local)
	}
	if got := stored.GetStatus().GetAssignedNode(); got != "" {
		t.Errorf("AssignedNode = %q, want cleared", got)
	}
}

// TestEnsureSuspendedFinalized_ReleasesReplacedSnapshot verifies which external
// snapshot a suspend collects. The actor's previous one goes away, because
// nothing else can name it — unless the actor was only borrowing it from a tag,
// in which case the tag owns those objects and the suspend must leave them be.
func TestEnsureSuspendedFinalized_ReleasesReplacedSnapshot(t *testing.T) {
	// The name of the snapshot the suspend has just written, in place of the
	// one the actor was running from.
	const snapshotName = "2026-01-01t00-00-00z-new"

	tests := []struct {
		name string
		// tagOwnedSnapshot makes the snapshot the actor is running from a tag's rather
		// than one it took itself, which is how an actor created from a tag
		// starts out.
		tagOwnedSnapshot bool
		wantReleased     bool
	}{
		{
			name:         "releases the external snapshot the actor owned",
			wantReleased: true,
		},
		{
			name:             "leaves an external snapshot borrowed from a tag in place",
			tagOwnedSnapshot: true,
			wantReleased:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
			w, objects := newFinalizeWorkflow(persistence)

			actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
			actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
				Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDING},
			})

			previous := mustActorSnapshotURI(t, template, actor, "old")
			prevType := ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR
			if tt.tagOwnedSnapshot {
				previous = mustTagSnapshotURI(t, template, "team-a", "v1-snapshot")
				prevType = ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG
			}
			fresh := mustActorSnapshotURI(t, template, actor, snapshotName)
			objects.PutSnapshot(t, previous, "manifest.json")
			objects.PutSnapshot(t, fresh, "manifest.json")

			mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
				s.LastAssignedGeneration = 2
				s.Snapshots = []*ateapipb.Snapshot{
					newDurableSnapshot(1, prevType, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", previous.Name(), previous.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
					newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", fresh.Name(), fresh.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
				}
			})

			stored, err := w.ensureSuspendedFinalized(ctx, actorRef)
			if err != nil {
				t.Fatalf("ensureSuspendedFinalized: %v", err)
			}
			// Whichever way the previous snapshot went, the actor now owns the
			// one it just wrote and is no longer borrowing.
			gotSnap := findLatestDurableSnapshot(stored.GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
			if got := gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri(); got != fresh.String() {
				t.Errorf("external snapshot = %q, want the one this suspend wrote, %q", got, fresh)
			}
			if released := len(objects.Snapshot(t, previous)) == 0; released != tt.wantReleased {
				t.Errorf("previous external snapshot released = %v, want %v", released, tt.wantReleased)
			}
			if len(objects.Snapshot(t, fresh)) == 0 {
				t.Error("the external snapshot this suspend wrote was collected")
			}
		})
	}
}

// errObjectStore stands in for object storage being unreachable.
var errObjectStore = errors.New("object storage is unavailable")

// isObjectStoreErr reports whether err came from errObjectStore. The object
// store sits behind the snapshot plugin's gRPC boundary, which carries the
// message but not the error value, so errors.Is cannot match it.
func isObjectStoreErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), errObjectStore.Error())
}

// TestEnsureSuspendedFinalized_CommitsDespiteObjectStoreFailure verifies that
// failing to collect the snapshot a suspend replaced does not fail the
// suspend. The worker is already released by then, so aborting would leave the
// actor SUSPENDING with no way to resume; it commits SUSPENDED on the new
// snapshot instead and leaves the replaced one in storage.
func TestEnsureSuspendedFinalized_CommitsDespiteObjectStoreFailure(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	w, objects := newFinalizeWorkflow(persistence)

	const snapshotName = "2026-01-01t00-00-00z-new"
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDING},
	})

	previous := mustActorSnapshotURI(t, template, actor, "old")
	fresh := mustActorSnapshotURI(t, template, actor, snapshotName)
	objects.PutSnapshot(t, previous, "manifest.json")
	objects.PutSnapshot(t, fresh, "manifest.json")

	mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
		s.LastAssignedGeneration = 2
		s.Snapshots = []*ateapipb.Snapshot{
			newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", previous.Name(), previous.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
			newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", fresh.Name(), fresh.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
		}
	})

	objects.OnDelete = func(string, string) error { return errObjectStore }
	stored, err := w.ensureSuspendedFinalized(ctx, actorRef)
	if err != nil {
		t.Fatalf("ensureSuspendedFinalized: %v", err)
	}
	if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("state = %v, want SUSPENDED", got)
	}
	gotSnap := findLatestDurableSnapshot(stored.GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	if got := gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri(); got != fresh.String() {
		t.Errorf("actor snapshot uri = %q, want the one this suspend wrote, %q", got, fresh)
	}
	if got := findLatestDurableSnapshot(stored.GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING); got != nil {
		t.Errorf("in-progress durable snapshot = %v, want cleared", got)
	}
	if len(objects.Snapshot(t, previous)) == 0 {
		t.Error("replaced external snapshot was collected despite every delete failing")
	}
	if len(objects.Snapshot(t, fresh)) == 0 {
		t.Error("the external snapshot this suspend wrote was collected")
	}
}

// TestEnsureSuspendedFinalized_KeepsReplacedSnapshotOnConflict verifies that a
// commit lost to a concurrent update releases nothing: the actor record still
// names its old snapshot, so collecting it would leave the actor pointing at
// deleted objects.
func TestEnsureSuspendedFinalized_KeepsReplacedSnapshotOnConflict(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	objects := objectstoretest.New()

	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDING},
	})

	previous := mustActorSnapshotURI(t, template, actor, "old")
	fresh := mustActorSnapshotURI(t, template, actor, "2026-01-01t00-00-00z-new")
	objects.PutSnapshot(t, previous, "manifest.json")
	objects.PutSnapshot(t, fresh, "manifest.json")
	mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
		s.LastAssignedGeneration = 2
		s.Snapshots = []*ateapipb.Snapshot{
			newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", previous.Name(), previous.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
			newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", fresh.Name(), fresh.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
		}
	})

	w := &ActorWorkflow{store: &conflictingUpdateStore{Interface: persistence}, snapshotPlugin: objectstoreplugintest.ControlClient(objects)}
	if _, err := w.ensureSuspendedFinalized(ctx, actorRef); apierror.Code(err) != codes.Aborted {
		t.Fatalf("ensureSuspendedFinalized = %v, want code Aborted", err)
	}
	if len(objects.Snapshot(t, previous)) == 0 {
		t.Error("replaced external snapshot was collected though the commit lost")
	}
}

// conflictingUpdateStore fails every actor update with a version conflict, as
// if another writer always got there first.
type conflictingUpdateStore struct {
	store.Interface
}

func (conflictingUpdateStore) UpdateActor(context.Context, resources.ActorRef, store.Precondition, func(*ateapipb.Actor) error) (*ateapipb.Actor, error) {
	return nil, store.ErrVersionConflict
}

func TestEnsureSuspendedFinalized_ReleasesOnlyOwnWorker(t *testing.T) {
	tests := []struct {
		name               string
		assignmentAtespace string
		mismatchedUID      bool
		wantReleased       bool
	}{
		{
			name:               "frees worker assigned to this actor",
			assignmentAtespace: "team-a",
			wantReleased:       true,
		},
		{
			name:               "keeps worker assigned to same-named actor in another atespace",
			assignmentAtespace: "team-b",
			wantReleased:       false,
		},
		{
			name:               "keeps worker assigned to previous incarnation of same actor",
			assignmentAtespace: "team-a",
			mismatchedUID:      true,
			wantReleased:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			actor := &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "shared"},
				Status: &ateapipb.ActorStatus{
					State: ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
					WorkerAssignment: &ateapipb.WorkerAssignment{
						Worker:          &ateapipb.ObjectRef{Name: testWorkerUID("pod-1")},
						WorkerNamespace: "worker-ns",
						WorkerPool:      "pool",
						WorkerPod:       "pod-1",
						WorkerPodUid:    testWorkerUID("pod-1"),
					},
					LastAssignedGeneration: 1,
					Snapshots: []*ateapipb.Snapshot{
						newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "snapshot-1", someActorSnapshotURI(t, testStorageLocation, "team-a", "snapshot-1"), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
					},
				},
			}
			created := storetest.MustCreateActor(t, ctx, persistence, actor)

			uid := created.GetMetadata().GetUid()
			if tt.assignmentAtespace != "team-a" || tt.mismatchedUID {
				uid = "other-actor-uid-b"
			}
			worker := &ateapipb.Worker{
				Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID("pod-1")},
				WorkerNamespace: "worker-ns",
				WorkerPool:      "pool",
				WorkerPod:       "pod-1",
				WorkerPodUid:    testWorkerUID("pod-1"),
				Status:          &ateapipb.WorkerStatus{},
			}
			if _, err := persistence.CreateWorker(ctx, worker); err != nil {
				t.Fatalf("CreateWorker: %v", err)
			}
			seedAssignment(t, persistence, testWorkerUID("pod-1"), &ateapipb.ActorAssignment{
				Actor:    &ateapipb.ObjectRef{Atespace: tt.assignmentAtespace, Name: "shared"},
				ActorUid: uid,
			})

			w := &ActorWorkflow{store: persistence}
			if _, err := w.ensureSuspendedFinalized(ctx, resources.ActorRef{Atespace: "team-a", Name: "shared"}); err != nil {
				t.Fatalf("ensureSuspendedFinalized: %v", err)
			}

			stored := firstAssignment(t, persistence, testWorkerUID("pod-1"))
			if released := stored == nil; released != tt.wantReleased {
				t.Errorf("worker released = %t, want %t (assignment: %v)", released, tt.wantReleased, stored)
			}
		})
	}
}

// TestPreferredFidelity verifies golden actors always commit Full — new
// actors resume the golden snapshot Full, so the template's preferredFidelity must not
// thin it down to a data-only capture.
func TestPreferredFidelity(t *testing.T) {
	tmpl := func(fidelity ateapipb.SnapshotFidelity) *ateapipb.ActorTemplate {
		return &ateapipb.ActorTemplate{
			SnapshotConfig: &ateapipb.SnapshotConfig{PreferredFidelity: fidelity},
		}
	}
	fullScope := ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	dataScope := ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
	tests := []struct {
		name     string
		atespace string
		fidelity ateapipb.SnapshotFidelity
		want     ateapipb.SnapshotFidelity
	}{
		{"golden actor ignores Data fidelity", resources.GoldenActorAtespace, dataScope, fullScope},
		{"golden actor keeps Full fidelity", resources.GoldenActorAtespace, fullScope, fullScope},
		{"regular actor uses Data fidelity", "team-a", dataScope, dataScope},
		{"regular actor uses Full fidelity", "team-a", fullScope, fullScope},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := preferredFidelity(tc.atespace, tmpl(tc.fidelity)); got != tc.want {
				t.Errorf("preferredFidelity(%q, fidelity=%s) = %s, want %s", tc.atespace, tc.fidelity, got, tc.want)
			}
		})
	}
}

// TestIsPausedOriginSuspend pins the paused-origin discriminator:
// LocalSnapshot alone must not select the paused path, because resume
// leaves it stale on RUNNING actors; only PAUSED state, or SUSPENDING with
// no worker assignment, means the suspend uploads a local snapshot.
func TestIsPausedOriginSuspend(t *testing.T) {
	assignment := &ateapipb.WorkerAssignment{WorkerNamespace: "ns", WorkerPool: "pool", WorkerPod: "pod-1"}
	localSnaps := []*ateapipb.Snapshot{
		newLocalSnapshot(1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "snap", "node1"),
	}
	tests := []struct {
		name  string
		actor *ateapipb.Actor
		want  bool
	}{
		{"paused actor", &ateapipb.Actor{Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_PAUSED, AssignedNode: "node1", Snapshots: localSnaps}}, true},
		{"suspending retry of a paused-origin suspend", &ateapipb.Actor{Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDING, AssignedNode: "node1", Snapshots: localSnaps}}, true},
		{"running actor with stale local snapshot info", &ateapipb.Actor{Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING, Snapshots: localSnaps, WorkerAssignment: assignment}}, false},
		{"suspending retry of a running-origin suspend with stale local snapshot info", &ateapipb.Actor{Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDING, Snapshots: localSnaps, WorkerAssignment: assignment}}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isPausedOriginSuspend(tc.actor); got != tc.want {
				t.Errorf("isPausedOriginSuspend = %t, want %t", got, tc.want)
			}
		})
	}
}

// TestEnsurePausedSnapshotUploaded_Preconditions covers the paused branch's
// failure handling: a lost node record crashes the actor (the snapshot can
// never be found), while an unreachable atelet stays retryable (the bytes
// are likely still on the node's disk).
func TestEnsurePausedSnapshotUploaded_Preconditions(t *testing.T) {
	t.Run("no node recorded crashes", func(t *testing.T) {
		ctx := context.Background()
		persistence := newTestPersistence(t)
		w := &ActorWorkflow{store: persistence, dialer: newDanglingDialer()}

		created := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
			Status: &ateapipb.ActorStatus{
				State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
				LastAssignedGeneration: 1,
				Snapshots: []*ateapipb.Snapshot{
					newLocalSnapshot(1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "snap", "node1"),
				},
			},
		})

		if _, err := w.ensurePausedSnapshotUploaded(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"}, created, &ateapipb.ActorTemplate{}); err == nil {
			t.Fatal("ensurePausedSnapshotUploaded = nil, want error for missing node record")
		}

		stored, err := persistence.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"})
		if err != nil {
			t.Fatalf("GetActor: %v", err)
		}
		if stored.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
			t.Errorf("state = %v, want CRASHED", stored.GetStatus().GetState())
		}
	})

	t.Run("no atelet on node stays retryable", func(t *testing.T) {
		ctx := context.Background()
		persistence := newTestPersistence(t)
		w := &ActorWorkflow{store: persistence, dialer: newDanglingDialer()}

		snap := newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "snap", someActorSnapshotURI(t, testStorageLocation, "team-a", "snap-dest"), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
		snap.Locality = "node1"
		created := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
			Status: &ateapipb.ActorStatus{
				State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
				AssignedNode:           "node1",
				LastAssignedGeneration: 1,
				Snapshots:              []*ateapipb.Snapshot{snap},
			},
		})

		tmpl := &ateapipb.ActorTemplate{SnapshotConfig: &ateapipb.SnapshotConfig{StorageLocation: "gs://snapshots"}}
		_, err := w.ensurePausedSnapshotUploaded(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"}, created, tmpl)
		if !errors.Is(err, ErrNoAteletOnNode) {
			t.Fatalf("ensurePausedSnapshotUploaded = %v, want ErrNoAteletOnNode", err)
		}

		stored, err := persistence.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"})
		if err != nil {
			t.Fatalf("GetActor: %v", err)
		}
		if stored.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDING {
			t.Errorf("state = %v, want SUSPENDING (retryable, not crashed)", stored.GetStatus().GetState())
		}
	})
}

// TestSuspendActor_PausedWithoutLocalSnapshotCrashes verifies a PAUSED actor
// whose LocalSnapshot is missing (corrupted store state: nothing records
// where the pause snapshot lives) is crashed by the suspend workflow rather
// than left flapping between PAUSED and SUSPENDING.
func TestSuspendActor_PausedWithoutLocalSnapshotCrashes(t *testing.T) {
	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()

	// The template needs a snapshot location: MarkSuspending validates the
	// destination URI before the workflow reaches the crash under test.
	// newTestActorWorkflow's stored template carries one.
	w := newTestActorWorkflow(t, st, "ns", "tmpl1")

	seedWorkflowActor(t, ctx, st, resources.ActorRef{Atespace: "team-a", Name: "id1"}, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_PAUSED)

	if _, err := w.SuspendActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}); err == nil {
		t.Fatal("SuspendActor succeeded, want error for PAUSED actor with no local snapshot record")
	}

	got, err := st.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("stored state = %v, want %v", got.GetStatus().GetState(), ateapipb.ActorState_ACTOR_STATE_CRASHED)
	}
	if msg, want := got.GetStatus().GetCrash().GetMessage(), "suspend failed: "+crashMessageLocalSnapshotNodeUnknown; msg != want {
		t.Errorf("crash message = %q, want %q", msg, want)
	}
}

// TestEnsureSuspendedFinalized_MissingDurableSnapshotClearsAssignment verifies
// that if the latest generation snapshot entry is missing when
// ensureSuspendedFinalized runs, the actor transitions to CRASHED and clears
// WorkerAssignment after releasing the worker rather than getting stuck in
// SUSPENDING.
func TestEnsureSuspendedFinalized_MissingDurableSnapshotClearsAssignment(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	ctx := context.Background()
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	records := crashRecords(t)

	workerName := testWorkerUID("worker-pod-1")
	created := storetest.MustCreateActor(t, ctx, st, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		Status: &ateapipb.ActorStatus{
			State:        ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			AssignedNode: "node1",
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker:          &ateapipb.ObjectRef{Name: workerName},
				WorkerNamespace: "default",
				WorkerPool:      "pool1",
				WorkerPod:       "worker-pod-1",
				WorkerPodUid:    workerName,
			},
			LastAssignedGeneration: 1,
		},
	})
	if _, err := st.CreateWorker(ctx, &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: workerName},
		WorkerNamespace: "default",
		WorkerPool:      "pool1",
		WorkerPod:       "worker-pod-1",
		WorkerPodUid:    workerName,
		NodeName:        "node1",
		Status:          &ateapipb.WorkerStatus{},
	}); err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}
	seedAssignment(t, st, workerName, &ateapipb.ActorAssignment{
		Actor:    &ateapipb.ObjectRef{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorUid: created.GetMetadata().GetUid(),
	})

	w := &ActorWorkflow{store: st}
	got, err := w.ensureSuspendedFinalized(ctx, actorRef)
	if err != nil {
		t.Fatalf("ensureSuspendedFinalized: %v", err)
	}
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("state = %v, want CRASHED", got.GetStatus().GetState())
	}
	if msg, want := got.GetStatus().GetCrash().GetMessage(), "suspend failed: "+crashMessageDurableSnapshotMissing; msg != want {
		t.Errorf("crash message = %q, want %q", msg, want)
	}
	if got.GetStatus().GetWorkerAssignment() != nil {
		t.Errorf("WorkerAssignment = %v, want nil", got.GetStatus().GetWorkerAssignment())
	}
	if len(*records) != 1 {
		t.Fatalf("got %d crash records, want 1", len(*records))
	}
}
