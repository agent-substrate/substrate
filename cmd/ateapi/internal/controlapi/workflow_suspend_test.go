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
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/tools/cache"
)

func TestEnsureMarkedSuspending_SnapshotName(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})
	tmpl := &ateapipb.ActorTemplate{
		SnapshotsConfig: &ateapipb.SnapshotsConfig{StorageLocation: "gs://bucket/root/"},
	}
	w := &ActorWorkflow{store: persistence}
	marked, err := w.ensureMarkedSuspending(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"}, actor, tmpl)
	if err != nil {
		t.Fatalf("ensureMarkedSuspending: %v", err)
	}

	// The field holds the snapshot's name, not its URI: FinalizeSuspended
	// names the ActorSnapshot after it, so it has to be usable as a resource
	// name verbatim.
	snapshotName := marked.GetStatus().GetInProgressSnapshotName()
	if !resources.IsValidResourceName(snapshotName) {
		t.Fatalf("in-progress snapshot = %q, want a valid resource name", snapshotName)
	}
	// The URI the later steps rebuild from that name nests under the actor's
	// own prefix, so the objects name their owner and nothing else can collect
	// them.
	uri, err := inProgressSnapshotURI(tmpl, marked, snapshotName)
	if err != nil {
		t.Fatalf("inProgressSnapshotURI(%q): %v", snapshotName, err)
	}
	want := "gs://bucket/root/atespaces/team-a/actors/" + marked.GetMetadata().GetUid() + "/snapshots/" + snapshotName
	if uri.String() != want {
		t.Errorf("snapshot URI = %q, want %q", uri, want)
	}
}

// TestEnsureMarkedSuspending_ReentryKeepsPersistedSnapshotLocation verifies a
// re-entered workflow does not mint a second snapshot location: the location
// persisted by the first attempt stays authoritative.
func TestEnsureMarkedSuspending_ReentryKeepsPersistedSnapshotLocation(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			InProgressSnapshotName: "first-attempt",
		},
	})
	w := &ActorWorkflow{store: persistence}
	marked, err := w.ensureMarkedSuspending(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"}, actor, &ateapipb.ActorTemplate{})
	if err != nil {
		t.Fatalf("ensureMarkedSuspending: %v", err)
	}
	if got := marked.GetStatus().GetInProgressSnapshotName(); got != "first-attempt" {
		t.Errorf("InProgressSnapshotName = %q, want the first attempt's location", got)
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
				if got := status.Code(err); got != codes.FailedPrecondition {
					t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, codes.FailedPrecondition, err)
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
		actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
			Status:   &ateapipb.ActorStatus{State: seedState},
		})

		tmpl := &ateapipb.ActorTemplate{
			SnapshotsConfig: &ateapipb.SnapshotsConfig{StorageLocation: "gs://snapshots"},
		}
		marked, err := w.ensureMarkedSuspending(ctx, actorRef, actor, tmpl)
		assertPrerequisiteResult(t, seedState, err, allowed[seedState])
		if err == nil && marked.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDING {
			t.Errorf("state %v: ensureMarkedSuspending returned actor in %v, want SUSPENDING", seedState, marked.GetStatus().GetState())
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
}

// newTestPersistence returns an isolated PostgreSQL-backed store.
func newTestPersistence(t *testing.T) store.Interface {
	persistence, _ := storetest.SetupTestStore(t)
	storetest.MustCreateAtespace(t, context.Background(), persistence, "team-a")
	return persistence
}

// newDanglingDialer returns a dialer whose informer cache has no pods, so
// DialForWorker returns ErrWorkerPodNotFound and DialForAteletOnNode returns
// ErrNoAteletOnNode.
func newDanglingDialer() *AteletDialer {
	empty := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		byNamespaceAndName: func(obj any) ([]string, error) { return nil, nil },
		byNode:             func(obj any) ([]string, error) { return nil, nil },
	})
	return NewAteletDialer(empty, empty, "", "")
}

func TestEnsureAteletSuspended_DanglingWorkerDoesNotRecordPhantomSnapshot(t *testing.T) {
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
			actor := &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
				Status: &ateapipb.ActorStatus{
					State: ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
					WorkerAssignment: &ateapipb.WorkerAssignment{
						WorkerNamespace: "worker-ns",
						WorkerPool:      "pool",
						WorkerPod:       "pod-gone",
					},
					InProgressSnapshotName: "never-written",
					ExternalSnapshot:       &ateapipb.ExternalSnapshot{SnapshotUri: tt.prevSnapshot},
				},
			}
			created := storetest.MustCreateActor(t, ctx, persistence, actor)

			w := &ActorWorkflow{store: persistence, dialer: newDanglingDialer()}
			if _, err := w.ensureAteletSuspended(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"}, created, &ateapipb.ActorTemplate{}); err == nil {
				t.Fatal("ensureAteletSuspended: want error for dangling worker, got nil")
			}

			stored, err := persistence.GetActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"})
			if err != nil {
				t.Fatalf("GetActor: %v", err)
			}
			if stored.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
				t.Errorf("state = %v, want CRASHED", stored.GetStatus().GetState())
			}
			if got := stored.GetStatus().GetInProgressSnapshotName(); got != "never-written" {
				t.Errorf("InProgressSnapshotName = %q, want preserved for debugging", got)
			}
			if got := stored.GetStatus().GetExternalSnapshot().GetSnapshotUri(); got != tt.prevSnapshot {
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

	const snapshotName = "2026-01-01t00-00-00z-abc"
	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			InProgressSnapshotName: snapshotName,
			LocalSnapshotInfo: &ateapipb.LocalSnapshotInfo{
				SnapshotName:              "actor-1-pause-snapshot",
				NodeVmsWithLocalSnapshots: []string{"node1"},
			},
		},
	}
	created := storetest.MustCreateActor(t, ctx, persistence, actor)

	w := &ActorWorkflow{store: persistence}
	tmpl := &ateapipb.ActorTemplate{SnapshotsConfig: &ateapipb.SnapshotsConfig{StorageLocation: "gs://snapshots"}}
	stored, err := w.ensureSuspendedFinalized(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"}, tmpl)
	if err != nil {
		t.Fatalf("ensureSuspendedFinalized: %v", err)
	}

	if stored.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("state = %v, want SUSPENDED", stored.GetStatus().GetState())
	}
	wantURI, err := inProgressSnapshotURI(tmpl, created, snapshotName)
	if err != nil {
		t.Fatalf("inProgressSnapshotURI: %v", err)
	}
	if got := stored.GetStatus().GetExternalSnapshot().GetSnapshotUri(); got != wantURI.String() {
		t.Errorf("SnapshotUri = %q, want %q", got, wantURI.String())
	}
	if got := stored.GetStatus().GetInProgressSnapshotName(); got != "" {
		t.Errorf("InProgressSnapshotName = %q, want cleared", got)
	}
	if stored.GetStatus().GetLocalSnapshotInfo() != nil {
		t.Errorf("LocalSnapshotInfo = %v, want cleared", stored.GetStatus().GetLocalSnapshotInfo())
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
			if tt.tagOwnedSnapshot {
				previous = mustTagSnapshotURI(t, template, "team-a", "v1-snapshot")
			}
			fresh := mustActorSnapshotURI(t, template, actor, snapshotName)
			objects.PutSnapshot(t, previous, "manifest.json")
			objects.PutSnapshot(t, fresh, "manifest.json")

			mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
				s.InProgressSnapshotName = snapshotName
				s.ExternalSnapshot = &ateapipb.ExternalSnapshot{SnapshotUri: previous.String()}
			})

			stored, err := w.ensureSuspendedFinalized(ctx, actorRef, template)
			if err != nil {
				t.Fatalf("ensureSuspendedFinalized: %v", err)
			}
			// Whichever way the previous snapshot went, the actor now owns the
			// one it just wrote and is no longer borrowing.
			if got := stored.GetStatus().GetExternalSnapshot().GetSnapshotUri(); got != fresh.String() {
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

// TestEnsureSuspendedFinalized_RetriesAfterObjectStoreFailure verifies a suspend
// that dies collecting the snapshot it replaced leaves the actor exactly where a
// retry picks it up — SUSPENDING, still naming the snapshot it was replacing —
// and that the retry then finishes the suspend.
func TestEnsureSuspendedFinalized_RetriesAfterObjectStoreFailure(t *testing.T) {
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
		s.InProgressSnapshotName = snapshotName
		s.ExternalSnapshot = &ateapipb.ExternalSnapshot{SnapshotUri: previous.String()}
	})

	objects.OnDelete = func(string, string) error { return errObjectStore }
	if _, err := w.ensureSuspendedFinalized(ctx, actorRef, template); !errors.Is(err, errObjectStore) {
		t.Fatalf("ensureSuspendedFinalized = %v, want an error wrapping %v", err, errObjectStore)
	}
	stuck, err := persistence.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if got := stuck.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDING {
		t.Errorf("state after the failure = %v, want SUSPENDING (retryable)", got)
	}
	if got := stuck.GetStatus().GetExternalSnapshot().GetSnapshotUri(); got != previous.String() {
		t.Errorf("actor snapshot uri after the failure = %q, want the replaced %q", got, previous)
	}

	objects.OnDelete = nil
	stored, err := w.ensureSuspendedFinalized(ctx, actorRef, template)
	if err != nil {
		t.Fatalf("retried ensureSuspendedFinalized: %v", err)
	}
	if got := stored.GetStatus().GetExternalSnapshot().GetSnapshotUri(); got != fresh.String() {
		t.Errorf("actor snapshot uri after the retry = %q, want %q", got, fresh)
	}
	if got := objects.Snapshot(t, previous); len(got) != 0 {
		t.Errorf("replaced external snapshot still holds %v, want it collected by the retry", got)
	}
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
					InProgressSnapshotName: "snapshot-1",
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
			tmpl := &ateapipb.ActorTemplate{SnapshotsConfig: &ateapipb.SnapshotsConfig{StorageLocation: "gs://bucket/root"}}
			if _, err := w.ensureSuspendedFinalized(ctx, resources.ActorRef{Atespace: "team-a", Name: "shared"}, tmpl); err != nil {
				t.Fatalf("ensureSuspendedFinalized: %v", err)
			}

			stored := firstAssignment(t, persistence, testWorkerUID("pod-1"))
			if released := stored == nil; released != tt.wantReleased {
				t.Errorf("worker released = %t, want %t (assignment: %v)", released, tt.wantReleased, stored)
			}
		})
	}
}

// TestCommitSnapshotScope verifies golden actors always commit Full — the
// golden snapshot is the base an OnGolden data resume combines into, so the
// template's onCommit must not thin it down to a data-only capture.
func TestCommitSnapshotScope(t *testing.T) {
	tmpl := func(onCommit ateapipb.SnapshotContentScope) *ateapipb.ActorTemplate {
		return &ateapipb.ActorTemplate{
			SnapshotsConfig: &ateapipb.SnapshotsConfig{OnCommit: onCommit},
		}
	}
	fullScope := ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL
	dataScope := ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA
	tests := []struct {
		name     string
		atespace string
		onCommit ateapipb.SnapshotContentScope
		want     ateapipb.SnapshotContentScope
	}{
		{"golden actor ignores Data onCommit", resources.GoldenActorAtespace, dataScope, fullScope},
		{"golden actor keeps Full onCommit", resources.GoldenActorAtespace, fullScope, fullScope},
		{"regular actor uses Data onCommit", "team-a", dataScope, dataScope},
		{"regular actor uses Full onCommit", "team-a", fullScope, fullScope},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := commitSnapshotScope(tc.atespace, tmpl(tc.onCommit)); got != tc.want {
				t.Errorf("commitSnapshotScope(%q, onCommit=%s) = %s, want %s", tc.atespace, tc.onCommit, got, tc.want)
			}
		})
	}
}

// TestIsPausedOriginSuspend pins the paused-origin discriminator:
// LocalSnapshotInfo alone must not select the paused path, because resume
// leaves it stale on RUNNING actors; only PAUSED state, or SUSPENDING with
// no worker assignment, means the suspend uploads a local snapshot.
func TestIsPausedOriginSuspend(t *testing.T) {
	assignment := &ateapipb.WorkerAssignment{WorkerNamespace: "ns", WorkerPool: "pool", WorkerPod: "pod-1"}
	localInfo := &ateapipb.LocalSnapshotInfo{SnapshotName: "snap", NodeVmsWithLocalSnapshots: []string{"node1"}}
	tests := []struct {
		name  string
		actor *ateapipb.Actor
		want  bool
	}{
		{"paused actor", &ateapipb.Actor{Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_PAUSED, LocalSnapshotInfo: localInfo}}, true},
		{"suspending retry of a paused-origin suspend", &ateapipb.Actor{Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDING, LocalSnapshotInfo: localInfo}}, true},
		{"running actor with stale local snapshot info", &ateapipb.Actor{Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING, LocalSnapshotInfo: localInfo, WorkerAssignment: assignment}}, false},
		{"suspending retry of a running-origin suspend with stale local snapshot info", &ateapipb.Actor{Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDING, LocalSnapshotInfo: localInfo, WorkerAssignment: assignment}}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isPausedOriginSuspend(tc.actor); got != tc.want {
				t.Errorf("isPausedOriginSuspend = %t, want %t", got, tc.want)
			}
		})
	}
}

// TestEnsureMarkedSuspending_PausedScopeRejection verifies a paused-origin
// suspend is rejected before the actor leaves PAUSED when the pause captured
// Data but the template commits Full: an upload cannot fabricate memory.
func TestEnsureMarkedSuspending_PausedScopeRejection(t *testing.T) {
	tmpl := func(onPause, onCommit ateapipb.SnapshotContentScope) *ateapipb.ActorTemplate {
		return &ateapipb.ActorTemplate{
			SnapshotsConfig: &ateapipb.SnapshotsConfig{OnPause: onPause, OnCommit: onCommit, StorageLocation: "gs://snapshots"},
		}
	}
	fullScope := ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL
	dataScope := ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA
	tests := []struct {
		name     string
		captured ateapipb.SnapshotContentScope
		tmpl     *ateapipb.ActorTemplate
		wantErr  bool
	}{
		{"data capture cannot commit full", dataScope, tmpl(dataScope, fullScope), true},
		{"data capture commits data", dataScope, tmpl(dataScope, dataScope), false},
		{"full capture commits full", fullScope, tmpl(fullScope, fullScope), false},
		{"full capture commits data via conversion", fullScope, tmpl(fullScope, dataScope), false},
		// Actors paused before content_scope existed fall back to the
		// template's onPause.
		{"unset capture falls back to onPause", ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED, tmpl(dataScope, fullScope), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			w := &ActorWorkflow{store: persistence}

			actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
			actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
				Status: &ateapipb.ActorStatus{
					State:             ateapipb.ActorState_ACTOR_STATE_PAUSED,
					LocalSnapshotInfo: &ateapipb.LocalSnapshotInfo{SnapshotName: "snap", NodeVmsWithLocalSnapshots: []string{"node1"}, ContentScope: tc.captured},
				},
			})

			_, err := w.ensureMarkedSuspending(ctx, actorRef, actor, tc.tmpl)
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("ensureMarkedSuspending = %v, wantErr %t", err, tc.wantErr)
			}
			if tc.wantErr {
				if got := status.Code(err); got != codes.FailedPrecondition {
					t.Errorf("status.Code = %v, want FailedPrecondition", got)
				}
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
				State:             ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
				LocalSnapshotInfo: &ateapipb.LocalSnapshotInfo{SnapshotName: "snap"},
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

		created := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
			Status: &ateapipb.ActorStatus{
				State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
				InProgressSnapshotName: "snap-dest",
				LocalSnapshotInfo:      &ateapipb.LocalSnapshotInfo{SnapshotName: "snap", NodeVmsWithLocalSnapshots: []string{"node1"}},
			},
		})

		tmpl := &ateapipb.ActorTemplate{SnapshotsConfig: &ateapipb.SnapshotsConfig{StorageLocation: "gs://snapshots"}}
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
// whose LocalSnapshotInfo is missing (corrupted store state: nothing records
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
}
