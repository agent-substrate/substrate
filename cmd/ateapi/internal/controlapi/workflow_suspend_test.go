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
	"fmt"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/installdefaults"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"k8s.io/client-go/tools/cache"
)

func TestSuspendActor_MarkedSuspending_SnapshotURI(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: "gs://bucket/root/"})
	wantRuntime := &ateapipb.SandboxRuntime{
		SandboxClass: "gvisor",
		Version: &ateapipb.VersionedSandboxCompat{
			SchemaVersion: "v1",
			Attributes: []*ateapipb.AttributeEntry{
				{Key: "architecture", Value: "amd64"},
			},
		},
	}
	assignment := wireTestAssignment()
	assignment.SandboxRuntime = wantRuntime
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: suspendTestTemplateRef(),
		Status: &ateapipb.ActorStatus{
			State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: assignment,
		},
	})
	w := newMarkOnlyWorkflow(persistence)
	_, err := w.SuspendActor(ctx, actorRef)
	assertStoppedAtDial(t, err)
	marked := mustReadActor(t, ctx, persistence, actorRef)

	inProgressSnap := findLatestDurableSnapshot(marked.GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
	uri := mustParseSnapshotURI(t, inProgressSnap.GetDurableSnapshot().GetObject().GetSnapshotUri())
	if !resources.IsValidResourceName(uri.Name()) {
		t.Errorf("in-progress snapshot name = %q, want a valid resource name", uri.Name())
	}
	if got := inProgressSnap.GetUuid(); got != uri.Name() {
		t.Errorf("Snapshot.Uuid = %q, want the snapshot name in its URI, %q", got, uri.Name())
	}
	want := "gs://bucket/root/atespaces/team-a/actors/" + marked.GetMetadata().GetUid() + "/snapshots/" + uri.Name()
	if uri.String() != want {
		t.Errorf("snapshot URI = %q, want %q", uri, want)
	}
	if diff := cmp.Diff(wantRuntime, inProgressSnap.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Errorf("Snapshot.SandboxRuntime mismatch (-want +got):\n%s", diff)
	}
}

// TestSuspendActor_MarkedSuspending_ReentryKeepsPersistedSnapshotLocation verifies a
// re-entered workflow does not mint a second snapshot location: the location
// persisted by the first attempt stays authoritative, both on the wire and in
// the snapshot the suspend commits.
func TestSuspendActor_MarkedSuspending_ReentryKeepsPersistedSnapshotLocation(t *testing.T) {
	const firstAttempt = "gs://bucket/root/atespaces/team-a/actors/actor-uid/snapshots/first-attempt"

	ctx := context.Background()
	persistence := newTestPersistence(t)
	seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: testStorageLocation})
	w, atelet := newWireCaptureWorkflow(t, persistence)
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: suspendTestTemplateRef(),
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			AssignedNode:           "node-1",
			WorkerAssignment:       wireTestAssignment(),
			LastAssignedGeneration: 1,
			Snapshots: []*ateapipb.Snapshot{
				newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "first-attempt", firstAttempt, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
			},
		},
	})
	suspended, err := w.SuspendActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}
	if got := atelet.checkpointRequest().GetSnapshot().GetObject().GetSnapshotUri(); got != firstAttempt {
		t.Errorf("CheckpointRequest.Snapshot.Object.SnapshotUri = %q, want the first attempt's location", got)
	}
	if got := suspended.GetStatus().GetLastAssignedGeneration(); got != 1 {
		t.Errorf("last_assigned_generation = %d, want 1 (no new snapshot minted on re-entry)", got)
	}
	gotSnap := findLatestDurableSnapshot(suspended.GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	if got := gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri(); got != firstAttempt {
		t.Errorf("committed durable snapshot URI = %q, want the first attempt's location", got)
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

// TestSuspendActor_MarkedSuspending_StateMatrix verifies the suspend edge's state gating
// against every actor state: RUNNING takes the edge (checkpoint the
// workload), SUSPENDING skips the mark (a previous attempt already marked the
// actor) and goes on to atelet, everything else is rejected with
// FailedPrecondition and left untouched. SUSPENDED without a node-local
// snapshot fast-forwards (see TestSuspendActorWorkflow_RejectedAndIdempotentPaths);
// SUSPENDED with one is the paused shape, covered by
// TestSuspendActor_MarkedSuspending_PausedPromotesLocalSnapshot.
func TestSuspendActor_MarkedSuspending_StateMatrix(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: "gs://snapshots"})
	w := newMarkOnlyWorkflow(persistence)

	for _, seedState := range allActorStates {
		actorRef := resources.ActorRef{Atespace: "team-a", Name: fmt.Sprintf("id%d", seedState)}
		seedWorkflowActor(t, ctx, persistence, actorRef, "ns", "tmpl1", seedState, func(a *ateapipb.Actor) {
			a.Status.WorkerAssignment = wireTestAssignment()
		})

		_, err := w.SuspendActor(ctx, actorRef)
		stored := mustReadActor(t, ctx, persistence, actorRef)
		switch seedState {
		case ateapipb.ActorState_ACTOR_STATE_RUNNING, ateapipb.ActorState_ACTOR_STATE_SUSPENDING:
			assertStoppedAtDial(t, err)
			if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDING {
				t.Errorf("state %v: stored state = %v, want SUSPENDING", seedState, got)
			}
		case ateapipb.ActorState_ACTOR_STATE_SUSPENDED:
			if err != nil {
				t.Errorf("state %v: SuspendActor = %v, want the fast-forward", seedState, err)
			}
			if got := stored.GetStatus().GetState(); got != seedState {
				t.Errorf("state %v: stored state = %v, want it untouched", seedState, got)
			}
		default:
			if got := apierror.Code(err); got != codes.FailedPrecondition {
				t.Errorf("state %v: code = %v, want FailedPrecondition (err: %v)", seedState, got, err)
			}
			if got := stored.GetStatus().GetState(); got != seedState {
				t.Errorf("state %v: stored state = %v, want it untouched", seedState, got)
			}
		}
	}
}

// TestSuspendActor_MarkedSuspending_RejectsPauseInProgress verifies a suspend does not adopt a
// SUSPENDING actor whose latest generation is a pending node-local snapshot:
// that actor is mid-pause, and the pause owns it until it is paused.
func TestSuspendActor_MarkedSuspending_RejectsPauseInProgress(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: "gs://snapshots"})
	w := newMarkOnlyWorkflow(persistence)
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}

	seedWorkflowActor(t, ctx, persistence, actorRef, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_SUSPENDING, withPauseInProgress(), func(a *ateapipb.Actor) {
		a.Status.WorkerAssignment = wireTestAssignment()
	})

	_, err := w.SuspendActor(ctx, actorRef)
	if got := apierror.Code(err); got != codes.FailedPrecondition {
		t.Errorf("SuspendActor on a pause in progress: code = %v, want FailedPrecondition (err: %v)", got, err)
	}
	stored := mustReadActor(t, ctx, persistence, actorRef)
	if got := stored.GetStatus().GetLastAssignedGeneration(); got != 1 {
		t.Errorf("last_assigned_generation = %d, want 1 (the pause's snapshot untouched)", got)
	}
	if !latestSnapshotIsPendingLocal(stored.GetStatus()) {
		t.Errorf("snapshots = %v, want the pause's pending local snapshot untouched", stored.GetStatus().GetSnapshots())
	}
}

// TestSuspendActor_MarkedSuspending_PausedPromotesLocalSnapshot verifies the suspend edge from
// a paused actor (SUSPENDED whose latest snapshot is node-local): it takes the
// edge and promotes that snapshot in place to a pending durable one, keeping
// its generation, uuid, and sandbox runtime stamp, with the upload destination
// derived from the uuid.
func TestSuspendActor_MarkedSuspending_PausedPromotesLocalSnapshot(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: "gs://snapshots"})
	w := newMarkOnlyWorkflow(persistence)

	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
	localSnap := newLocalSnapshot(1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "snap-1", "node-1")
	localSnap.SandboxRuntime = testSandboxRuntime()
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: suspendTestTemplateRef(),
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			AssignedNode:           "node-1",
			LastAssignedGeneration: 1,
			Snapshots:              []*ateapipb.Snapshot{localSnap},
		},
	})

	// The upload's dial fails, so the actor stays as the mark left it.
	_, err := w.SuspendActor(ctx, actorRef)
	assertStoppedAtDial(t, err)
	marked := mustReadActor(t, ctx, persistence, actorRef)
	if got := marked.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDING {
		t.Errorf("state = %v, want SUSPENDING", got)
	}
	wantURI, err := resources.NewActorSnapshotURI("gs://snapshots", actorRef.Atespace, actor.GetMetadata().GetUid(), "snap-1")
	if err != nil {
		t.Fatalf("NewActorSnapshotURI: %v", err)
	}
	pending := findLatestDurableSnapshot(marked.GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
	gotURI := pending.GetDurableSnapshot().GetObject().GetSnapshotUri()
	if gotURI != wantURI.String() {
		t.Errorf("paused-origin in-progress SnapshotUri = %q, want %q (derived from local snapshot uuid)", gotURI, wantURI.String())
	}
	// The upload promotes the pause snapshot in place rather than minting a
	// new generation.
	if pending.GetGeneration() != 1 || pending.GetUuid() != "snap-1" {
		t.Errorf("paused-origin pending snapshot = generation %d, uuid %q; want the pause snapshot (generation 1, uuid %q)", pending.GetGeneration(), pending.GetUuid(), "snap-1")
	}
	if pending.GetLocality() != "node-1" {
		t.Errorf("pending snapshot locality = %q, want node-1 kept until the upload finishes", pending.GetLocality())
	}
	// The durable copy is the pause checkpoint uploaded, so it keeps the
	// runtime stamp the pause recorded.
	if diff := cmp.Diff(testSandboxRuntime(), pending.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Errorf("paused-origin pending Snapshot.SandboxRuntime mismatch (-want +got):\n%s", diff)
	}
	if got := marked.GetStatus().GetLastAssignedGeneration(); got != 1 {
		t.Errorf("paused-origin LastAssignedGeneration = %d, want 1", got)
	}
}

// TestSuspendActor_CrashesWhenSuspendingActorMissingWorkerPod verifies that a
// SUSPENDING actor with no worker pod recorded is moved to CRASHED by the
// corrupted-assignment check before atelet is called, and the suspend fails
// with FailedPrecondition.
func TestSuspendActor_CrashesWhenSuspendingActorMissingWorkerPod(t *testing.T) {
	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	w := newTestActorWorkflow(t, st, "ns", "tmpl1")

	seedWorkflowActor(t, ctx, st, resources.ActorRef{Atespace: "team-a", Name: "id1"}, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_SUSPENDING)

	_, err := w.SuspendActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
	if got := apierror.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("apierror.Code(err) = %v, want %v (err: %v)", got, codes.FailedPrecondition, err)
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

func TestSuspendActor_AteletSuspended_DialFailureLeavesActorRetryable(t *testing.T) {
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
			seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: testStorageLocation})
			var snapshots []*ateapipb.Snapshot
			if tt.prevSnapshot != "" {
				snapshots = append(snapshots, newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "prev-snapshot", tt.prevSnapshot, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED))
			}
			snapshots = append(snapshots, newDurableSnapshot(2, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "never-written", neverWritten, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING))

			actor := &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
				ActorTemplate: suspendTestTemplateRef(),
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
			storetest.MustCreateActor(t, ctx, persistence, actor)

			w := newMarkOnlyWorkflow(persistence)
			_, err := w.SuspendActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"})
			assertStoppedAtDial(t, err)

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

// TestSuspendActor_AteletSuspended_WireRequest verifies a running-origin suspend checkpoints
// to the actor's in-progress durable snapshot with an immediate upload, since
// the suspend's finalize then marks that snapshot COMPLETED.
func TestSuspendActor_AteletSuspended_WireRequest(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{PreferredFidelity: ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY})
	w, atelet := newWireCaptureWorkflow(t, persistence)
	inst, reader := newTestInstruments(t)
	w.instruments = inst

	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	snapshotName := "2026-01-01t00-00-00z-abc"
	snapshotURI := someActorSnapshotURI(t, testStorageLocation, "team-a", snapshotName)
	storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: suspendTestTemplateRef(),
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			AssignedNode:           "node-1",
			WorkerAssignment:       wireTestAssignment(),
			LastAssignedGeneration: 1,
			Snapshots: []*ateapipb.Snapshot{
				newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-uid-1", snapshotName, snapshotURI, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
			},
		},
	})

	if _, err := w.SuspendActor(ctx, actorRef); err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}
	req := atelet.checkpointRequest()
	if req == nil {
		t.Fatal("expected Checkpoint RPC to be called")
	}
	if got := req.GetSnapshot().GetSnapshotUuid(); got != snapshotName {
		t.Errorf("CheckpointRequest.Snapshot.SnapshotUuid = %q, want %q", got, snapshotName)
	}
	if got := req.GetSnapshot().GetGeneration(); got != 1 {
		t.Errorf("CheckpointRequest.Snapshot.Generation = %d, want 1, the in-progress snapshot's", got)
	}
	if got := req.GetSnapshot().GetObject().GetSnapshotUri(); got != snapshotURI {
		t.Errorf("CheckpointRequest.Snapshot.Object.SnapshotUri = %q, want %q", got, snapshotURI)
	}
	wantConfig := &ateletpb.UploadConfig{Mode: ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE}
	if got := req.GetUploadConfig(); !proto.Equal(got, wantConfig) {
		t.Errorf("CheckpointRequest.UploadConfig = %v, want %v", got, wantConfig)
	}
	// The fidelity sent to atelet labels the suspend's lifecycle-op metric.
	assertLifecycleFidelity(t, reader, ateattr.SnapshotFidelityMemory)
}

// TestSuspendActor_Finalized_NoAssignment verifies finalization runs even
// when the actor has no worker assignment: the external snapshot must be
// recorded on the actor and the actor moved to SUSPENDED rather than silently
// left SUSPENDING. This is the shape a paused-origin suspend (#791) produces —
// a paused actor has no worker — and the regression test for finalization
// previously living inside the worker-freeing branch.
func TestSuspendActor_Finalized_NoAssignment(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: testStorageLocation})
	w, atelet := newWireCaptureWorkflow(t, persistence)
	inst, reader := newTestInstruments(t)
	w.instruments = inst
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}

	snapshotURI := someActorSnapshotURI(t, testStorageLocation, "team-a", "2026-01-01t00-00-00z-abc")
	snap := newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "tmpl-uid-1", "actor-1-pause-snapshot", snapshotURI, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
	snap.Locality = "node-1"
	storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: suspendTestTemplateRef(),
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			AssignedNode:           "node-1",
			LastAssignedGeneration: 1,
			Snapshots:              []*ateapipb.Snapshot{snap},
		},
	})

	stored, err := w.SuspendActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}

	// With no worker there is no workload to checkpoint: the suspend uploads
	// the node-local snapshot instead.
	if checkpoints, promotes := atelet.calls(); checkpoints != 0 || promotes != 1 {
		t.Errorf("atelet calls = %d Checkpoint, %d PromoteSnapshot; want 0 and 1", checkpoints, promotes)
	}
	promote := atelet.promoteRequest()
	if got := promote.GetSnapshot().GetSnapshotUuid(); got != "actor-1-pause-snapshot" {
		t.Errorf("PromoteSnapshotRequest.Snapshot.SnapshotUuid = %q, want %q", got, "actor-1-pause-snapshot")
	}
	if got := promote.GetSnapshot().GetObject().GetSnapshotUri(); got != snapshotURI {
		t.Errorf("PromoteSnapshotRequest.Snapshot.Object.SnapshotUri = %q, want %q", got, snapshotURI)
	}
	// The template's preferred fidelity, which the store defaults to MEMORY,
	// labels the suspend's lifecycle-op metric.
	assertLifecycleFidelity(t, reader, ateattr.SnapshotFidelityMemory)

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

// TestSuspendActor_Finalized_ReleasesReplacedSnapshot verifies which external
// snapshot a suspend collects. The actor's previous one goes away, because
// nothing else can name it — unless the actor was only borrowing it from a tag,
// in which case the tag owns those objects and the suspend must leave them be.
func TestSuspendActor_Finalized_ReleasesReplacedSnapshot(t *testing.T) {
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
			w, _, objects := newWireFinalizeWorkflow(t, persistence)

			actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
			actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
				Status: &ateapipb.ActorStatus{
					State:            ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
					AssignedNode:     "node-1",
					WorkerAssignment: wireTestAssignment(),
				},
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

			stored, err := w.SuspendActor(ctx, actorRef)
			if err != nil {
				t.Fatalf("SuspendActor: %v", err)
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
// store sits behind the object-store plugin's gRPC boundary, which carries the
// message but not the error value, so errors.Is cannot match it.
func isObjectStoreErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), errObjectStore.Error())
}

// TestSuspendActor_Finalized_CommitsDespiteObjectStoreFailure verifies that
// failing to collect the snapshot a suspend replaced does not fail the
// suspend. The worker is already released by then, so aborting would leave the
// actor SUSPENDING with no way to resume; it commits SUSPENDED on the new
// snapshot instead and leaves the replaced one in storage.
func TestSuspendActor_Finalized_CommitsDespiteObjectStoreFailure(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	w, _, objects := newWireFinalizeWorkflow(t, persistence)

	const snapshotName = "2026-01-01t00-00-00z-new"
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
		Status: &ateapipb.ActorStatus{
			State:            ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			AssignedNode:     "node-1",
			WorkerAssignment: wireTestAssignment(),
		},
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
	stored, err := w.SuspendActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("SuspendActor: %v", err)
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

// TestSuspendActor_Finalized_KeepsReplacedSnapshotOnConflict verifies that a
// commit lost to a concurrent update releases nothing: the actor record still
// names its old snapshot, so collecting it would leave the actor pointing at
// deleted objects.
func TestSuspendActor_Finalized_KeepsReplacedSnapshotOnConflict(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	// The actor is seeded SUSPENDING, so the mark step skips and the first
	// actor update the suspend makes is its finalize commit.
	w, atelet, objects := newWireFinalizeWorkflow(t, &conflictingUpdateStore{Interface: persistence})

	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
		Status: &ateapipb.ActorStatus{
			State:            ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			AssignedNode:     "node-1",
			WorkerAssignment: wireTestAssignment(),
		},
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

	if _, err := w.SuspendActor(ctx, actorRef); apierror.Code(err) != codes.Aborted {
		t.Fatalf("SuspendActor = %v, want code Aborted", err)
	}
	// Every actor update conflicts, so pin that the Aborted came from finalize
	// and not from an earlier step that never reached atelet.
	if checkpoints, _ := atelet.calls(); checkpoints != 1 {
		t.Errorf("Checkpoint calls = %d, want 1 before finalize", checkpoints)
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

// TestSuspendActor_Finalized_KeepsCrashFromWorkerDelete verifies that a crash the
// DeleteWorker workflow commits while the suspend's finalize is freeing the
// worker is kept: the finalize update is pinned to the actor version it
// checked, so it fails Aborted instead of committing SUSPENDED over the crash.
func TestSuspendActor_Finalized_KeepsCrashFromWorkerDelete(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: testStorageLocation})
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}

	created := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: suspendTestTemplateRef(),
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			AssignedNode:           "node-1",
			WorkerAssignment:       wireTestAssignment(),
			LastAssignedGeneration: 1,
			Snapshots: []*ateapipb.Snapshot{
				newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "snapshot-1", someActorSnapshotURI(t, testStorageLocation, "team-a", "snapshot-1"), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
			},
		},
	})
	seedWireWorker(t, ctx, persistence, assignmentOf(created))

	w, _ := newWireCaptureWorkflow(t, workerDeletedDuringReleaseStore{Interface: persistence})
	_, err := w.SuspendActor(ctx, actorRef)
	assertConcurrentCrashKept(t, ctx, persistence, actorRef, err, "suspend failed: "+crashMessageWorkerPodGone)
}

func TestSuspendActor_Finalized_ReleasesOnlyOwnWorker(t *testing.T) {
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
			seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: testStorageLocation})
			w, _ := newWireCaptureWorkflow(t, persistence)
			actor := &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "shared"},
				ActorTemplate: suspendTestTemplateRef(),
				Status: &ateapipb.ActorStatus{
					State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
					AssignedNode:           "node-1",
					WorkerAssignment:       wireTestAssignment(),
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
			seedWireWorker(t, ctx, persistence, &ateapipb.ActorAssignment{
				Actor:    &ateapipb.ObjectRef{Atespace: tt.assignmentAtespace, Name: "shared"},
				ActorUid: uid,
			})

			if _, err := w.SuspendActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "shared"}); err != nil {
				t.Fatalf("SuspendActor: %v", err)
			}

			stored := firstAssignment(t, persistence, wireTestAssignment().GetWorker().GetName())
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

// TestIsPausedOriginSuspend pins the paused-origin discriminator: a local
// snapshot alone must not select the paused path, because a RUNNING actor
// resumed from pause still carries one; only the paused shape (SUSPENDED with
// a node-local latest snapshot), or SUSPENDING with no worker assignment,
// means the suspend uploads a local snapshot.
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
		{"paused actor", &ateapipb.Actor{Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, AssignedNode: "node1", LastAssignedGeneration: 1, Snapshots: localSnaps}}, true},
		{"suspended actor without a local snapshot", &ateapipb.Actor{Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED}}, false},
		{"suspending retry of a paused-origin suspend", &ateapipb.Actor{Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDING, AssignedNode: "node1", Snapshots: localSnaps}}, true},
		{"running actor resumed from pause", &ateapipb.Actor{Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING, Snapshots: localSnaps, WorkerAssignment: assignment}}, false},
		{"suspending retry of a running-origin suspend after a resume from pause", &ateapipb.Actor{Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDING, Snapshots: localSnaps, WorkerAssignment: assignment}}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isPausedOriginSuspend(tc.actor); got != tc.want {
				t.Errorf("isPausedOriginSuspend = %t, want %t", got, tc.want)
			}
		})
	}
}

// TestSuspendActor_PausedSnapshotUploaded_Preconditions covers the paused
// branch's failure handling: a lost node record crashes the actor (the
// snapshot can never be found), while an unreachable atelet stays retryable
// (the bytes are likely still on the node's disk). Both actors are SUSPENDING
// with no worker, so the mark step skips and the suspend takes the paused
// branch.
func TestSuspendActor_PausedSnapshotUploaded_Preconditions(t *testing.T) {
	t.Run("no node recorded crashes", func(t *testing.T) {
		ctx := context.Background()
		persistence := newTestPersistence(t)
		seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: "gs://snapshots"})
		w := newMarkOnlyWorkflow(persistence)
		actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}

		storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
			ActorTemplate: suspendTestTemplateRef(),
			Status: &ateapipb.ActorStatus{
				State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
				LastAssignedGeneration: 1,
				Snapshots: []*ateapipb.Snapshot{
					newLocalSnapshot(1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "snap", "node1"),
				},
			},
		})

		_, err := w.SuspendActor(ctx, actorRef)
		if got := apierror.Code(err); got != codes.FailedPrecondition {
			t.Fatalf("apierror.Code(err) = %v, want %v for a missing node record (err: %v)", got, codes.FailedPrecondition, err)
		}

		stored := mustReadActor(t, ctx, persistence, actorRef)
		if stored.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
			t.Errorf("state = %v, want CRASHED", stored.GetStatus().GetState())
		}
		if msg, want := stored.GetStatus().GetCrash().GetMessage(), "suspend failed: "+crashMessageLocalSnapshotNodeUnknown; msg != want {
			t.Errorf("crash message = %q, want %q", msg, want)
		}
	})

	t.Run("no atelet on node stays retryable", func(t *testing.T) {
		ctx := context.Background()
		persistence := newTestPersistence(t)
		seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: "gs://snapshots"})
		w := newMarkOnlyWorkflow(persistence)
		actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}

		snap := newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "snap", someActorSnapshotURI(t, testStorageLocation, "team-a", "snap-dest"), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
		snap.Locality = "node1"
		storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
			ActorTemplate: suspendTestTemplateRef(),
			Status: &ateapipb.ActorStatus{
				State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
				AssignedNode:           "node1",
				LastAssignedGeneration: 1,
				Snapshots:              []*ateapipb.Snapshot{snap},
			},
		})

		_, err := w.SuspendActor(ctx, actorRef)
		if !errors.Is(err, ErrNoAteletOnNode) {
			t.Fatalf("SuspendActor = %v, want ErrNoAteletOnNode", err)
		}

		stored := mustReadActor(t, ctx, persistence, actorRef)
		if stored.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDING {
			t.Errorf("state = %v, want SUSPENDING (retryable, not crashed)", stored.GetStatus().GetState())
		}
	})
}

// TestSuspendActor_PausedSnapshotUploaded_WireRequest verifies the
// PromoteSnapshot request names the paused actor's local snapshot, with its
// generation, and the destination the suspend recorded for it.
func TestSuspendActor_PausedSnapshotUploaded_WireRequest(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: testStorageLocation, PreferredFidelity: ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY})
	w, atelet := newWireCaptureWorkflow(t, persistence)

	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	destURI := someActorSnapshotURI(t, testStorageLocation, "team-a", "snap-dest")
	snap := newDurableSnapshot(3, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "pause-snap", destURI, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
	snap.Locality = "node-1"
	// A suspend from paused in progress: SUSPENDING with no worker, so the
	// suspend skips its mark step and promotes the local snapshot.
	storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: suspendTestTemplateRef(),
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			AssignedNode:           "node-1",
			LastAssignedGeneration: 3,
			Snapshots:              []*ateapipb.Snapshot{snap},
		},
	})

	if _, err := w.SuspendActor(ctx, actorRef); err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}
	req := atelet.promoteRequest()
	if req == nil {
		t.Fatal("expected PromoteSnapshot RPC to be called")
	}
	want := &ateletpb.Snapshot{
		SnapshotUuid: "pause-snap",
		Generation:   3,
		Fidelity:     ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		Object:       &ateletpb.ObjectStorage{SnapshotUri: destURI},
	}
	if got := req.GetSnapshot(); !proto.Equal(got, want) {
		t.Errorf("PromoteSnapshotRequest.Snapshot = %v, want %v", got, want)
	}
}

// TestSuspendActor_Finalized_MissingDurableSnapshotClearsAssignment verifies that if the
// latest generation snapshot entry is missing when the suspend finalizes, the
// actor transitions to CRASHED and clears WorkerAssignment after releasing the
// worker rather than getting stuck in SUSPENDING, and the suspend fails.
func TestSuspendActor_Finalized_MissingDurableSnapshotClearsAssignment(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	seedSuspendTemplate(t, ctx, st, &ateapipb.SnapshotConfig{StorageLocation: testStorageLocation})
	w, atelet := newWireCaptureWorkflow(t, st)
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	records := crashRecords(t)

	created := storetest.MustCreateActor(t, ctx, st, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: suspendTestTemplateRef(),
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			AssignedNode:           "node-1",
			WorkerAssignment:       wireTestAssignment(),
			LastAssignedGeneration: 1,
			Snapshots: []*ateapipb.Snapshot{
				newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "snapshot-1", someActorSnapshotURI(t, testStorageLocation, "team-a", "snapshot-1"), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
			},
		},
	})
	seedWireWorker(t, ctx, st, assignmentOf(created))
	updateDuringCheckpoint(t, atelet, st, actorRef, func(s *ateapipb.ActorStatus) {
		s.Snapshots = nil
	})

	if _, err := w.SuspendActor(ctx, actorRef); apierror.Code(err) != codes.FailedPrecondition {
		t.Fatalf("SuspendActor error = %v, want FailedPrecondition for the actor it crashed", err)
	}
	got, err := st.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
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

// suspendModes runs a table test against both suspend workflows: sync, whose
// snapshot is durable before it returns, and async, whose snapshot stays
// node-local.
var suspendModes = []struct {
	name string
	run  func(*ActorWorkflow, context.Context, resources.ActorRef) (*ateapipb.Actor, error)
}{
	{"sync", (*ActorWorkflow).SuspendActor},
	{"async", (*ActorWorkflow).PauseActor},
}

// suspendOutcome is what one suspend workflow does to a seeded actor. Rows
// omit zero-valued fields, which the matrix still pins.
type suspendOutcome struct {
	code         codes.Code
	atelet       ateletCall
	state        ateapipb.ActorState
	assignedNode string
	// workerHeld is whether the worker wireTestAssignment names still holds
	// the actor's assignment.
	workerHeld bool
	generation int32
	snapshots  []snapshotOutcome
}

// ateletCall is the suspend RPC atelet received; the zero value is none.
type ateletCall struct {
	checkpoints, promotes int
	// fidelity and object describe the request sent; uploadMode is set only
	// on a Checkpoint.
	fidelity   ateletpb.SnapshotFidelity
	uploadMode ateletpb.UploadMode
	object     bool
}

// snapshotOutcome is the part of a stored snapshot the matrix pins.
type snapshotOutcome struct {
	generation int32
	fidelity   ateapipb.SnapshotFidelity
	locality   string
	// durable is the durable copy's status, UNSPECIFIED when there is none.
	durable ateapipb.SnapshotStorageStatus
}

// TestSuspendAndPause_CharacteristicMatrix pins, for each actor shape either
// workflow can be called on, what SuspendActor (sync) and PauseActor (async)
// do: the code returned, the atelet RPC sent, the record left behind, and
// whether the actor's worker was released.
func TestSuspendAndPause_CharacteristicMatrix(t *testing.T) {
	const node = "node-1"
	volumes := ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
	durable := func(gen int32, status ateapipb.SnapshotStorageStatus) *ateapipb.Snapshot {
		name := fmt.Sprintf("snap-%d", gen)
		return newDurableSnapshot(gen, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, volumes, "", name, someActorSnapshotURI(t, testStorageLocation, "team-a", name), status)
	}
	local := func(gen int32, locality string) *ateapipb.Snapshot {
		return newLocalSnapshot(gen, volumes, "", fmt.Sprintf("snap-%d", gen), locality)
	}
	completed := ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
	pending := ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING
	noDurable := ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_UNSPECIFIED
	promoting := durable(1, pending)
	promoting.Locality = node

	ateletVolumes := ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
	immediate := ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE
	checkpointSync := ateletCall{checkpoints: 1, fidelity: ateletVolumes, uploadMode: immediate, object: true}
	checkpointAsync := ateletCall{checkpoints: 1, fidelity: ateletVolumes, uploadMode: ateletpb.UploadMode_UPLOAD_MODE_DEFERRED}
	promote := ateletCall{promotes: 1, fidelity: ateletVolumes, object: true}
	// rejected is a FailedPrecondition outcome: no atelet call, and the
	// record left as want describes it.
	rejected := func(want suspendOutcome) suspendOutcome {
		want.code = codes.FailedPrecondition
		return want
	}
	suspended := ateapipb.ActorState_ACTOR_STATE_SUSPENDED

	tests := []struct {
		name string
		// atespace defaults to team-a.
		atespace    string
		status      *ateapipb.ActorStatus
		sync, async suspendOutcome
	}{
		{
			name:   "RUNNING",
			status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING, AssignedNode: node, WorkerAssignment: wireTestAssignment()},
			sync:   suspendOutcome{code: codes.OK, atelet: checkpointSync, state: suspended, generation: 1, snapshots: []snapshotOutcome{{1, volumes, "", completed}}},
			async:  suspendOutcome{code: codes.OK, atelet: checkpointAsync, state: suspended, assignedNode: node, generation: 1, snapshots: []snapshotOutcome{{1, volumes, node, noDurable}}},
		},
		{
			name: "RUNNING resumed from pause",
			status: &ateapipb.ActorStatus{
				State: ateapipb.ActorState_ACTOR_STATE_RUNNING, AssignedNode: node, WorkerAssignment: wireTestAssignment(),
				LastAssignedGeneration: 1, Snapshots: []*ateapipb.Snapshot{local(1, node)},
			},
			sync:  suspendOutcome{code: codes.OK, atelet: checkpointSync, state: suspended, generation: 2, snapshots: []snapshotOutcome{{2, volumes, "", completed}}},
			async: suspendOutcome{code: codes.OK, atelet: checkpointAsync, state: suspended, assignedNode: node, generation: 2, snapshots: []snapshotOutcome{{2, volumes, node, noDurable}}},
		},
		{
			name: "RUNNING resumed from a durable snapshot",
			status: &ateapipb.ActorStatus{
				State: ateapipb.ActorState_ACTOR_STATE_RUNNING, AssignedNode: node, WorkerAssignment: wireTestAssignment(),
				LastAssignedGeneration: 1, Snapshots: []*ateapipb.Snapshot{durable(1, completed)},
			},
			sync:  suspendOutcome{code: codes.OK, atelet: checkpointSync, state: suspended, generation: 2, snapshots: []snapshotOutcome{{2, volumes, "", completed}}},
			async: suspendOutcome{code: codes.OK, atelet: checkpointAsync, state: suspended, assignedNode: node, generation: 2, snapshots: []snapshotOutcome{{1, volumes, "", completed}, {2, volumes, node, noDurable}}},
		},
		{
			name:   "SUSPENDED with a durable snapshot",
			status: &ateapipb.ActorStatus{State: suspended, LastAssignedGeneration: 1, Snapshots: []*ateapipb.Snapshot{durable(1, completed)}},
			sync:   suspendOutcome{code: codes.OK, state: suspended, generation: 1, snapshots: []snapshotOutcome{{1, volumes, "", completed}}},
			async:  rejected(suspendOutcome{state: suspended, generation: 1, snapshots: []snapshotOutcome{{1, volumes, "", completed}}}),
		},
		{
			name:   "paused",
			status: &ateapipb.ActorStatus{State: suspended, AssignedNode: node, LastAssignedGeneration: 1, Snapshots: []*ateapipb.Snapshot{local(1, node)}},
			sync:   suspendOutcome{code: codes.OK, atelet: promote, state: suspended, generation: 1, snapshots: []snapshotOutcome{{1, volumes, "", completed}}},
			async:  suspendOutcome{code: codes.OK, state: suspended, assignedNode: node, generation: 1, snapshots: []snapshotOutcome{{1, volumes, node, noDurable}}},
		},
		{
			name: "suspend from RUNNING in progress",
			status: &ateapipb.ActorStatus{
				State: ateapipb.ActorState_ACTOR_STATE_SUSPENDING, AssignedNode: node, WorkerAssignment: wireTestAssignment(),
				LastAssignedGeneration: 1, Snapshots: []*ateapipb.Snapshot{durable(1, pending)},
			},
			sync:  suspendOutcome{code: codes.OK, atelet: checkpointSync, state: suspended, generation: 1, snapshots: []snapshotOutcome{{1, volumes, "", completed}}},
			async: rejected(suspendOutcome{state: ateapipb.ActorState_ACTOR_STATE_SUSPENDING, assignedNode: node, workerHeld: true, generation: 1, snapshots: []snapshotOutcome{{1, volumes, "", pending}}}),
		},
		{
			name: "suspend from paused in progress",
			status: &ateapipb.ActorStatus{
				State: ateapipb.ActorState_ACTOR_STATE_SUSPENDING, AssignedNode: node,
				LastAssignedGeneration: 1, Snapshots: []*ateapipb.Snapshot{promoting},
			},
			sync:  suspendOutcome{code: codes.OK, atelet: promote, state: suspended, generation: 1, snapshots: []snapshotOutcome{{1, volumes, "", completed}}},
			async: rejected(suspendOutcome{state: ateapipb.ActorState_ACTOR_STATE_SUSPENDING, assignedNode: node, generation: 1, snapshots: []snapshotOutcome{{1, volumes, node, pending}}}),
		},
		{
			name: "pause in progress",
			status: &ateapipb.ActorStatus{
				State: ateapipb.ActorState_ACTOR_STATE_SUSPENDING, AssignedNode: node, WorkerAssignment: wireTestAssignment(),
				LastAssignedGeneration: 1, Snapshots: []*ateapipb.Snapshot{local(1, "")},
			},
			sync:  rejected(suspendOutcome{state: ateapipb.ActorState_ACTOR_STATE_SUSPENDING, assignedNode: node, workerHeld: true, generation: 1, snapshots: []snapshotOutcome{{1, volumes, "", noDurable}}}),
			async: suspendOutcome{code: codes.OK, atelet: checkpointAsync, state: suspended, assignedNode: node, generation: 1, snapshots: []snapshotOutcome{{1, volumes, node, noDurable}}},
		},
		{
			name:   "RESUMING",
			status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RESUMING},
			sync:   rejected(suspendOutcome{state: ateapipb.ActorState_ACTOR_STATE_RESUMING}),
			async:  rejected(suspendOutcome{state: ateapipb.ActorState_ACTOR_STATE_RESUMING}),
		},
		{
			name:   "CRASHED",
			status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_CRASHED},
			sync:   rejected(suspendOutcome{state: ateapipb.ActorState_ACTOR_STATE_CRASHED}),
			async:  rejected(suspendOutcome{state: ateapipb.ActorState_ACTOR_STATE_CRASHED}),
		},
		{
			name:   "DELETING",
			status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_DELETING},
			sync:   rejected(suspendOutcome{state: ateapipb.ActorState_ACTOR_STATE_DELETING}),
			async:  rejected(suspendOutcome{state: ateapipb.ActorState_ACTOR_STATE_DELETING}),
		},
		{
			// A golden actor commits MEMORY whatever the template prefers, and
			// cannot be paused at all.
			name:     "golden RUNNING",
			atespace: resources.GoldenActorAtespace,
			status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING, AssignedNode: node, WorkerAssignment: wireTestAssignment()},
			sync:     suspendOutcome{code: codes.OK, atelet: ateletCall{checkpoints: 1, fidelity: ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, uploadMode: immediate, object: true}, state: suspended, generation: 1, snapshots: []snapshotOutcome{{1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", completed}}},
			async:    rejected(suspendOutcome{state: ateapipb.ActorState_ACTOR_STATE_RUNNING, assignedNode: node, workerHeld: true}),
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range suspendModes {
				t.Run(mode.name, func(t *testing.T) {
					want := tc.sync
					if mode.name == "async" {
						want = tc.async
					}
					atespace := tc.atespace
					if atespace == "" {
						atespace = "team-a"
					}
					// Each run gets its own store, so it can seed the one worker
					// wireTestAssignment names.
					ctx := context.Background()
					persistence := newTestPersistence(t)
					seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: testStorageLocation, PreferredFidelity: volumes})
					actorRef := resources.ActorRef{Atespace: atespace, Name: fmt.Sprintf("row%d-%s", i, mode.name)}
					created := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
						Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
						ActorTemplate: suspendTestTemplateRef(),
						Status:        proto.CloneOf(tc.status),
					})
					var assignment *ateapipb.ActorAssignment
					if tc.status.GetWorkerAssignment() != nil {
						assignment = assignmentOf(created)
					}
					seedWireWorker(t, ctx, persistence, assignment)
					w, atelet := newWireCaptureWorkflow(t, persistence)

					_, err := mode.run(w, ctx, actorRef)
					stored := mustReadActor(t, ctx, persistence, actorRef)
					got := suspendOutcome{
						code:         apierror.Code(err),
						atelet:       describeAteletCall(atelet),
						state:        stored.GetStatus().GetState(),
						assignedNode: stored.GetStatus().GetAssignedNode(),
						workerHeld:   firstAssignment(t, persistence, wireTestAssignment().GetWorker().GetName()) != nil,
						generation:   stored.GetStatus().GetLastAssignedGeneration(),
						snapshots:    describeSnapshots(stored.GetStatus()),
					}
					if diff := cmp.Diff(want, got, cmp.AllowUnexported(suspendOutcome{}, ateletCall{}, snapshotOutcome{})); diff != "" {
						t.Errorf("outcome mismatch (-want +got):\n%s (err: %v)", diff, err)
					}
					if got.state == ateapipb.ActorState_ACTOR_STATE_SUSPENDED && stored.GetStatus().GetWorkerAssignment() != nil {
						t.Errorf("WorkerAssignment = %v, want cleared on SUSPENDED", stored.GetStatus().GetWorkerAssignment())
					}
					assertRequestNamesLatestSnapshot(t, atelet, stored)
				})
			}
		})
	}
}

// TestSuspendAndPause_FinalizeWithoutAssignment pins where the two finalize
// steps differ when the DeleteWorker workflow crashes the actor, clearing its
// worker assignment, between the atelet call and finalize. Both sides pin
// bugs tracked in #2676: the suspend commits SUSPENDED over the crash, and
// the pause keeps the crash but returns OK for an actor left CRASHED. The fix
// changes the sync wantState to CRASHED and both codes to FailedPrecondition,
// replacing the err != nil fatal below.
func TestSuspendAndPause_FinalizeWithoutAssignment(t *testing.T) {
	wantState := map[string]ateapipb.ActorState{
		"sync":  ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		"async": ateapipb.ActorState_ACTOR_STATE_CRASHED,
	}
	for _, mode := range suspendModes {
		t.Run(mode.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: testStorageLocation})
			w, atelet := newWireCaptureWorkflow(t, persistence)
			actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
			storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
				ActorTemplate: suspendTestTemplateRef(),
				Status: &ateapipb.ActorStatus{
					State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
					AssignedNode:     "node-1",
					WorkerAssignment: wireTestAssignment(),
				},
			})
			updateDuringCheckpoint(t, atelet, persistence, actorRef, func(s *ateapipb.ActorStatus) {
				s.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
				s.WorkerAssignment = nil
			})

			returned, err := mode.run(w, ctx, actorRef)
			// #2676: both modes should return FailedPrecondition here.
			if err != nil {
				t.Fatalf("%s: %v", mode.name, err)
			}
			if got := returned.GetStatus().GetState(); got != wantState[mode.name] {
				t.Errorf("returned state = %v, want %v", got, wantState[mode.name])
			}
			if got := mustReadActor(t, ctx, persistence, actorRef).GetStatus().GetState(); got != wantState[mode.name] {
				t.Errorf("stored state = %v, want %v", got, wantState[mode.name])
			}
		})
	}
}

// describeAteletCall summarizes the suspend RPCs atelet received: how many
// of each, and the fidelity, upload mode, and object destination requested.
func describeAteletCall(atelet *capturingAtelet) ateletCall {
	var call ateletCall
	call.checkpoints, call.promotes = atelet.calls()
	var snap *ateletpb.Snapshot
	if req := atelet.checkpointRequest(); req != nil {
		snap = req.GetSnapshot()
		call.uploadMode = req.GetUploadConfig().GetMode()
	} else if req := atelet.promoteRequest(); req != nil {
		snap = req.GetSnapshot()
	}
	call.fidelity = snap.GetFidelity()
	call.object = snap.GetObject() != nil
	return call
}

// describeSnapshots summarizes each stored snapshot as its generation,
// fidelity, locality, and durable copy's status.
func describeSnapshots(status *ateapipb.ActorStatus) []snapshotOutcome {
	var out []snapshotOutcome
	for _, snap := range status.GetSnapshots() {
		out = append(out, snapshotOutcome{
			generation: snap.GetGeneration(),
			fidelity:   snap.GetFidelity(),
			locality:   snap.GetLocality(),
			durable:    snap.GetDurableSnapshot().GetStatus(),
		})
	}
	return out
}

// assertRequestNamesLatestSnapshot verifies the suspend RPC atelet received,
// if any, names the snapshot at the stored actor's latest generation, and that
// an object destination is that snapshot's durable copy.
func assertRequestNamesLatestSnapshot(t *testing.T, atelet *capturingAtelet, stored *ateapipb.Actor) {
	t.Helper()
	var snap *ateletpb.Snapshot
	if req := atelet.checkpointRequest(); req != nil {
		snap = req.GetSnapshot()
	} else if req := atelet.promoteRequest(); req != nil {
		snap = req.GetSnapshot()
	} else {
		return
	}
	latest := snapshotAtLatestGeneration(stored.GetStatus())
	if got, want := snap.GetSnapshotUuid(), latest.GetUuid(); got != want {
		t.Errorf("request Snapshot.SnapshotUuid = %q, want the latest generation's %q", got, want)
	}
	if obj := snap.GetObject(); obj != nil {
		if got, want := obj.GetSnapshotUri(), latest.GetDurableSnapshot().GetObject().GetSnapshotUri(); got != want {
			t.Errorf("request Snapshot.Object.SnapshotUri = %q, want the latest generation's durable copy %q", got, want)
		}
	}
}
