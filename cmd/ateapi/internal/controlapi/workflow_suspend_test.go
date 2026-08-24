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
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/ateredis"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

func TestEnsureMarkedSuspending_SnapshotName(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	if _, err := persistence.CreateAtespace(ctx, &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "team-a"}}); err != nil {
		t.Fatalf("CreateAtespace: %v", err)
	}
	actor, err := persistence.CreateActor(ctx, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})
	if err != nil {
		t.Fatalf("CreateActor: %v", err)
	}
	tmpl := &atev1alpha1.ActorTemplate{Spec: atev1alpha1.ActorTemplateSpec{
		SnapshotsConfig: atev1alpha1.SnapshotsConfig{Location: "gs://bucket/root/"},
	}}
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
	// atespace so each tenant gets a distinct storage prefix.
	uri, err := resources.NewSnapshotURI(tmpl.Spec.SnapshotsConfig.Location, "team-a", snapshotName)
	if err != nil {
		t.Fatalf("NewSnapshotURI(%q): %v", snapshotName, err)
	}
	if want := "gs://bucket/root/snapshots/team-a/" + snapshotName; uri.String() != want {
		t.Errorf("snapshot URI = %q, want %q", uri, want)
	}
}

// TestEnsureMarkedSuspending_ReentryKeepsPersistedSnapshotLocation verifies a
// re-entered workflow does not mint a second snapshot location: the location
// persisted by the first attempt stays authoritative.
func TestEnsureMarkedSuspending_ReentryKeepsPersistedSnapshotLocation(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	if _, err := persistence.CreateAtespace(ctx, &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "team-a"}}); err != nil {
		t.Fatalf("CreateAtespace: %v", err)
	}
	actor, err := persistence.CreateActor(ctx, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
		Status: &ateapipb.ActorStatus{
			State:                                ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			InProgressSnapshotName:               "first-attempt",
			InProgressSnapshotSourceActorVersion: 7,
		},
	})
	if err != nil {
		t.Fatalf("CreateActor: %v", err)
	}
	w := &ActorWorkflow{store: persistence}
	marked, err := w.ensureMarkedSuspending(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"}, actor, &atev1alpha1.ActorTemplate{})
	if err != nil {
		t.Fatalf("ensureMarkedSuspending: %v", err)
	}
	if got := marked.GetStatus().GetInProgressSnapshotName(); got != "first-attempt" {
		t.Errorf("InProgressSnapshotName = %q, want the first attempt's location", got)
	}
	if got := marked.GetStatus().GetInProgressSnapshotSourceActorVersion(); got != 7 {
		t.Errorf("InProgressSnapshotSourceActorVersion = %d, want 7", got)
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
		if _, err := persistence.CreateAtespace(ctx, &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: actorRef.Atespace}}); err != nil {
			t.Fatalf("CreateAtespace: %v", err)
		}
		actor, err := persistence.CreateActor(ctx, &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
			Status:   &ateapipb.ActorStatus{State: seedState},
		})
		if err != nil {
			t.Fatalf("state %v: CreateActor: %v", seedState, err)
		}

		tmpl := &atev1alpha1.ActorTemplate{Spec: atev1alpha1.ActorTemplateSpec{
			SnapshotsConfig: atev1alpha1.SnapshotsConfig{Location: "gs://snapshots"},
		}}
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

// newTestPersistence returns a store backed by a throwaway miniredis.
func newTestPersistence(t *testing.T) store.Interface {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{mr.Addr()}})
	t.Cleanup(func() { rdb.Close() }) //nolint:errcheck // test cleanup
	return ateredis.NewPersistence(rdb)
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
		prevSnapshot *ateapipb.ObjectRef
	}{
		{
			name:         "keeps previous snapshot",
			prevSnapshot: &ateapipb.ObjectRef{Atespace: "team-a", Name: "prev"},
		},
		{
			name:         "stays nil without previous snapshot",
			prevSnapshot: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)

			if _, err := persistence.CreateAtespace(ctx, &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "team-a"}}); err != nil {
				t.Fatalf("CreateAtespace: %v", err)
			}

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
					LatestSnapshot:         tt.prevSnapshot,
				},
			}
			created, err := persistence.CreateActor(ctx, actor)
			if err != nil {
				t.Fatalf("CreateActor: %v", err)
			}

			w := &ActorWorkflow{store: persistence, dialer: newDanglingDialer()}
			if _, err := w.ensureAteletSuspended(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"}, created, &atev1alpha1.ActorTemplate{}); err == nil {
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
			if tt.prevSnapshot == nil {
				if stored.GetStatus().GetLatestSnapshot() != nil {
					t.Errorf("LatestSnapshot = %v, want nil", stored.GetStatus().GetLatestSnapshot())
				}
			} else if got, want := stored.GetStatus().GetLatestSnapshot().GetName(), tt.prevSnapshot.GetName(); got != want {
				t.Errorf("LatestSnapshot name = %q, want %q", got, want)
			}
		})
	}
}

// TestEnsureSuspendedFinalized_NoAssignment verifies finalization runs even when
// the actor has no worker assignment: the ActorSnapshot must be recorded and
// the actor moved to SUSPENDED rather than silently left SUSPENDING. This is
// the shape a paused-origin suspend (#791) produces — a PAUSED actor has no
// worker — and the regression test for finalization previously living inside
// the worker-freeing branch.
func TestEnsureSuspendedFinalized_NoAssignment(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)

	if _, err := persistence.CreateAtespace(ctx, &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "team-a"}}); err != nil {
		t.Fatalf("CreateAtespace: %v", err)
	}

	const snapshotName = "2026-01-01t00-00-00z-abc"
	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
		Status: &ateapipb.ActorStatus{
			State:                                ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			InProgressSnapshotName:               snapshotName,
			InProgressSnapshotSourceActorVersion: 1,
			LocalSnapshotInfo: &ateapipb.LocalSnapshotInfo{
				SnapshotName:              "actor-1-pause-snapshot",
				NodeVmsWithLocalSnapshots: []string{"node1"},
			},
		},
	}
	created, err := persistence.CreateActor(ctx, actor)
	if err != nil {
		t.Fatalf("CreateActor: %v", err)
	}

	w := &ActorWorkflow{store: persistence}
	tmpl := &atev1alpha1.ActorTemplate{Spec: atev1alpha1.ActorTemplateSpec{SnapshotsConfig: atev1alpha1.SnapshotsConfig{Location: "gs://snapshots"}}}
	stored, err := w.ensureSuspendedFinalized(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"}, tmpl)
	if err != nil {
		t.Fatalf("ensureSuspendedFinalized: %v", err)
	}

	if stored.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("state = %v, want SUSPENDED", stored.GetStatus().GetState())
	}
	if got := stored.GetStatus().GetLatestSnapshot().GetName(); got != snapshotName {
		t.Errorf("LatestSnapshot = %q, want %q", got, snapshotName)
	}
	if got := stored.GetStatus().GetInProgressSnapshotName(); got != "" {
		t.Errorf("InProgressSnapshotName = %q, want cleared", got)
	}
	if stored.GetStatus().GetLocalSnapshotInfo() != nil {
		t.Errorf("LocalSnapshotInfo = %v, want cleared", stored.GetStatus().GetLocalSnapshotInfo())
	}
	snapshot, err := persistence.GetActorSnapshot(ctx, "team-a", snapshotName)
	if err != nil {
		t.Fatalf("GetActorSnapshot: %v", err)
	}
	wantURI, err := resources.NewSnapshotURI("gs://snapshots", "team-a", snapshotName)
	if err != nil {
		t.Fatalf("NewSnapshotURI: %v", err)
	}
	if got := snapshot.GetStatus().GetSnapshotUri(); got != wantURI.String() {
		t.Errorf("snapshot URI = %q, want %q", got, wantURI.String())
	}
	if got := snapshot.GetStatus().GetSourceActorUid(); got != created.GetMetadata().GetUid() {
		t.Errorf("snapshot SourceActorUid = %q, want %q", got, created.GetMetadata().GetUid())
	}
	if got := snapshot.GetStatus().GetSourceActorVersion(); got != 1 {
		t.Errorf("snapshot SourceActorVersion = %d, want 1", got)
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

			if _, err := persistence.CreateAtespace(ctx, &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "team-a"}}); err != nil {
				t.Fatalf("CreateAtespace: %v", err)
			}

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
			created, err := persistence.CreateActor(ctx, actor)
			if err != nil {
				t.Fatalf("CreateActor: %v", err)
			}

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
				Status: &ateapipb.WorkerStatus{
					Assignment: &ateapipb.ActorAssignment{
						Actor:    &ateapipb.ObjectRef{Atespace: tt.assignmentAtespace, Name: "shared"},
						ActorUid: uid,
					},
				},
			}
			if err := persistence.CreateWorker(ctx, worker); err != nil {
				t.Fatalf("CreateWorker: %v", err)
			}

			w := &ActorWorkflow{store: persistence}
			tmpl := &atev1alpha1.ActorTemplate{Spec: atev1alpha1.ActorTemplateSpec{SnapshotsConfig: atev1alpha1.SnapshotsConfig{Location: "gs://bucket/root"}}}
			if _, err := w.ensureSuspendedFinalized(ctx, resources.ActorRef{Atespace: "team-a", Name: "shared"}, tmpl); err != nil {
				t.Fatalf("ensureSuspendedFinalized: %v", err)
			}

			stored, err := persistence.GetWorker(ctx, testWorkerUID("pod-1"))
			if err != nil {
				t.Fatalf("GetWorker: %v", err)
			}
			if released := stored.GetStatus().GetAssignment() == nil; released != tt.wantReleased {
				t.Errorf("worker released = %t, want %t (assignment: %v)", released, tt.wantReleased, stored.GetStatus().GetAssignment())
			}
		})
	}
}

// TestEnsureSuspendedFinalized_SnapshotSourceActorVersion pins that the
// ActorSnapshot records the source actor version persisted when suspension
// was marked — the version the checkpoint captured — rather than the actor's
// version at finalize time, including on a re-entered workflow.
func TestEnsureSuspendedFinalized_SnapshotSourceActorVersion(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)

	if _, err := persistence.CreateAtespace(ctx, &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "team-a"}}); err != nil {
		t.Fatalf("CreateAtespace: %v", err)
	}

	const snapshotName = "2026-01-01t00-00-00z-abc"
	_, err := persistence.CreateActor(ctx, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker:          &ateapipb.ObjectRef{Name: testWorkerUID("pod-gone")},
				WorkerNamespace: "worker-ns",
				WorkerPool:      "pool",
				WorkerPod:       "pod-gone",
				WorkerPodUid:    testWorkerUID("pod-gone"),
			},
			InProgressSnapshotName:               snapshotName,
			InProgressSnapshotSourceActorVersion: 42,
		},
	})
	if err != nil {
		t.Fatalf("CreateActor: %v", err)
	}

	w := &ActorWorkflow{store: persistence}
	tmpl := &atev1alpha1.ActorTemplate{Spec: atev1alpha1.ActorTemplateSpec{SnapshotsConfig: atev1alpha1.SnapshotsConfig{Location: "gs://snapshots"}}}
	final, err := w.ensureSuspendedFinalized(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"}, tmpl)
	if err != nil {
		t.Fatalf("ensureSuspendedFinalized: %v", err)
	}
	if final.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("state = %v, want SUSPENDED", final.GetStatus().GetState())
	}
	if final.GetStatus().GetInProgressSnapshotName() != "" || final.GetStatus().GetInProgressSnapshotSourceActorVersion() != 0 {
		t.Errorf("in-progress snapshot fields not cleared: %q / %d", final.GetStatus().GetInProgressSnapshotName(), final.GetStatus().GetInProgressSnapshotSourceActorVersion())
	}

	snap, err := persistence.GetActorSnapshot(ctx, "team-a", final.GetStatus().GetLatestSnapshot().GetName())
	if err != nil {
		t.Fatalf("GetActorSnapshot: %v", err)
	}
	if got := snap.GetStatus().GetSourceActorVersion(); got != 42 {
		t.Errorf("SourceActorVersion = %d, want 42", got)
	}
}

// TestCommitSnapshotScope verifies golden actors always commit Full — the
// golden snapshot is the base an OnGolden data resume combines into, so the
// template's onCommit must not thin it down to a data-only capture.
func TestCommitSnapshotScope(t *testing.T) {
	tmpl := func(onCommit atev1alpha1.SnapshotScope) *atev1alpha1.ActorTemplate {
		return &atev1alpha1.ActorTemplate{Spec: atev1alpha1.ActorTemplateSpec{
			SnapshotsConfig: atev1alpha1.SnapshotsConfig{OnCommit: onCommit},
		}}
	}
	tests := []struct {
		name     string
		atespace string
		onCommit atev1alpha1.SnapshotScope
		want     atev1alpha1.SnapshotScope
	}{
		{"golden actor ignores Data onCommit", resources.GoldenActorAtespace, atev1alpha1.SnapshotScopeData, atev1alpha1.SnapshotScopeFull},
		{"golden actor keeps Full onCommit", resources.GoldenActorAtespace, atev1alpha1.SnapshotScopeFull, atev1alpha1.SnapshotScopeFull},
		{"regular actor uses Data onCommit", "team-a", atev1alpha1.SnapshotScopeData, atev1alpha1.SnapshotScopeData},
		{"regular actor uses Full onCommit", "team-a", atev1alpha1.SnapshotScopeFull, atev1alpha1.SnapshotScopeFull},
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
	tmpl := func(onPause, onCommit atev1alpha1.SnapshotScope) *atev1alpha1.ActorTemplate {
		return &atev1alpha1.ActorTemplate{Spec: atev1alpha1.ActorTemplateSpec{
			SnapshotsConfig: atev1alpha1.SnapshotsConfig{OnPause: onPause, OnCommit: onCommit, Location: "gs://snapshots"},
		}}
	}
	tests := []struct {
		name     string
		captured ateapipb.SnapshotContentScope
		tmpl     *atev1alpha1.ActorTemplate
		wantErr  bool
	}{
		{"data capture cannot commit full", ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA, tmpl(atev1alpha1.SnapshotScopeData, atev1alpha1.SnapshotScopeFull), true},
		{"data capture commits data", ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA, tmpl(atev1alpha1.SnapshotScopeData, atev1alpha1.SnapshotScopeData), false},
		{"full capture commits full", ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL, tmpl(atev1alpha1.SnapshotScopeFull, atev1alpha1.SnapshotScopeFull), false},
		{"full capture commits data via conversion", ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL, tmpl(atev1alpha1.SnapshotScopeFull, atev1alpha1.SnapshotScopeData), false},
		// Actors paused before content_scope existed fall back to the
		// template's onPause.
		{"unset capture falls back to onPause", ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED, tmpl(atev1alpha1.SnapshotScopeData, atev1alpha1.SnapshotScopeFull), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			w := &ActorWorkflow{store: persistence}

			actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
			if _, err := persistence.CreateAtespace(ctx, &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: actorRef.Atespace}}); err != nil {
				t.Fatalf("CreateAtespace: %v", err)
			}
			actor, err := persistence.CreateActor(ctx, &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
				Status: &ateapipb.ActorStatus{
					State:             ateapipb.ActorState_ACTOR_STATE_PAUSED,
					LocalSnapshotInfo: &ateapipb.LocalSnapshotInfo{SnapshotName: "snap", NodeVmsWithLocalSnapshots: []string{"node1"}, ContentScope: tc.captured},
				},
			})
			if err != nil {
				t.Fatalf("CreateActor: %v", err)
			}

			_, err = w.ensureMarkedSuspending(ctx, actorRef, actor, tc.tmpl)
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

		if _, err := persistence.CreateAtespace(ctx, &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "team-a"}}); err != nil {
			t.Fatalf("CreateAtespace: %v", err)
		}

		created, err := persistence.CreateActor(ctx, &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
			Status: &ateapipb.ActorStatus{
				State:             ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
				LocalSnapshotInfo: &ateapipb.LocalSnapshotInfo{SnapshotName: "snap"},
			},
		})
		if err != nil {
			t.Fatalf("CreateActor: %v", err)
		}

		if _, err := w.ensurePausedSnapshotUploaded(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"}, created, &atev1alpha1.ActorTemplate{}); err == nil {
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

		if _, err := persistence.CreateAtespace(ctx, &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "team-a"}}); err != nil {
			t.Fatalf("CreateAtespace: %v", err)
		}

		created, err := persistence.CreateActor(ctx, &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
			Status: &ateapipb.ActorStatus{
				State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
				InProgressSnapshotName: "snap-dest",
				LocalSnapshotInfo:      &ateapipb.LocalSnapshotInfo{SnapshotName: "snap", NodeVmsWithLocalSnapshots: []string{"node1"}},
			},
		})
		if err != nil {
			t.Fatalf("CreateActor: %v", err)
		}

		tmpl := &atev1alpha1.ActorTemplate{Spec: atev1alpha1.ActorTemplateSpec{SnapshotsConfig: atev1alpha1.SnapshotsConfig{Location: "gs://snapshots"}}}
		_, err = w.ensurePausedSnapshotUploaded(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"}, created, tmpl)
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
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	if err := indexer.Add(&atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "tmpl1"},
		Spec:       atev1alpha1.ActorTemplateSpec{SnapshotsConfig: atev1alpha1.SnapshotsConfig{Location: "gs://snapshots"}},
	}); err != nil {
		t.Fatalf("add template to indexer: %v", err)
	}
	w := NewActorWorkflow(st, nil, nil, listersv1alpha1.NewActorTemplateLister(indexer), nil, nil, nil, nil, "", nil)

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
