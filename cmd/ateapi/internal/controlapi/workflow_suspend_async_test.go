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

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
)

// TestSuspendActorAsync_Finalized_WorkerGone reproduces the scenario where the
// worker pod disappears from the DB during async suspend finalization, so the
// node it ran on is unknown.
//
// Current behavior: AssignedNode is left empty, and the actor is crashed
// instead of left SUSPENDED with a local snapshot, since a local snapshot with
// an unknown node can never be safely resumed. The suspend fails with
// FailedPrecondition rather than reporting the suspend as done.
func TestSuspendActorAsync_Finalized_WorkerGone(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	seedSuspendTemplate(t, ctx, st, nil)
	w, atelet := newWireCaptureWorkflow(t, st)
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	records := crashRecords(t)

	storetest.MustCreateActor(t, ctx, st, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: suspendTestTemplateRef(),
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			WorkerAssignment:       wireTestAssignment(),
			LastAssignedGeneration: 1,
			Snapshots: []*ateapipb.Snapshot{
				newInProgressSnapshot(t, 1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "local-snap-1"),
			},
		},
	})
	// Intentionally NOT creating the worker in store, simulates worker already gone.

	if _, err := w.SuspendActor(ctx, actorRef, suspendAsync); apierror.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Async SuspendActor error = %v, want FailedPrecondition for the actor it crashed", err)
	}
	if checkpoints, _ := atelet.calls(); checkpoints != 1 {
		t.Errorf("Checkpoint calls = %d, want 1 before finalize", checkpoints)
	}

	got := mustReadActor(t, ctx, st, actorRef)
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("state = %v, want CRASHED (node name unknown, cannot resume safely)", got.GetStatus().GetState())
	}
	if msg, want := got.GetStatus().GetCrash().GetMessage(), "suspend failed: "+crashMessageLocalSnapshotNodeUnknown; msg != want {
		t.Errorf("crash message = %q, want %q", msg, want)
	}
	if got.GetStatus().GetAssignedNode() != "" {
		t.Errorf("AssignedNode = %q, want empty", got.GetStatus().GetAssignedNode())
	}
	if gotSnap := snapshotAtLatestGeneration(got.GetStatus()); gotSnap.GetUuid() != "local-snap-1" {
		t.Errorf("in-progress local snapshot uuid = %q, want %q preserved on crash", gotSnap.GetUuid(), "local-snap-1")
	}
	if got.GetStatus().GetWorkerAssignment() != nil {
		t.Error("stored actor still has a worker assignment, want it cleared")
	}

	// This site records the crash counter, so it must write the record too, or
	// an async-suspend-finalize crash is the one kind nothing can attribute to an actor.
	if len(*records) != 1 {
		t.Fatalf("got %d crash records, want 1", len(*records))
	}
	if got := (*records)[0].attrs[string(ateattr.ActorUIDKey)]; got == "" {
		t.Error("crash record carries no ate.actor.uid")
	}
	if got := (*records)[0].attrs[string(ateattr.ActorOperationNameKey)]; got != ateattr.OperationSuspend {
		t.Errorf("crash record %s = %q, want %q", ateattr.ActorOperationNameKey, got, ateattr.OperationSuspend)
	}
}

// TestSuspendActorAsync_Finalized_AlreadyCrashed verifies that if the actor was
// crashed out-of-band while it checkpointed, and has no AssignedNode, the
// async suspend's finalize preserves the existing Crash status and snapshots,
// emits no duplicate crash log record, and still fails.
func TestSuspendActorAsync_Finalized_AlreadyCrashed(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	seedSuspendTemplate(t, ctx, st, nil)
	w, atelet := newWireCaptureWorkflow(t, st)
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	records := crashRecords(t)

	storetest.MustCreateActor(t, ctx, st, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: suspendTestTemplateRef(),
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			WorkerAssignment:       wireTestAssignment(),
			LastAssignedGeneration: 1,
			Snapshots: []*ateapipb.Snapshot{
				newInProgressSnapshot(t, 1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "local-snap-1"),
			},
		},
	})
	originalCrash := newActorCrash(ateattr.OperationSuspend, "original crash reason")
	updateDuringCheckpoint(t, atelet, st, actorRef, func(s *ateapipb.ActorStatus) {
		s.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
		s.Crash = originalCrash
		s.WorkerAssignment = nil
	})

	if _, err := w.SuspendActor(ctx, actorRef, suspendAsync); apierror.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Async SuspendActor error = %v, want FailedPrecondition for a CRASHED actor", err)
	}
	got := mustReadActor(t, ctx, st, actorRef)
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("state = %v, want CRASHED", got.GetStatus().GetState())
	}
	if msg, want := got.GetStatus().GetCrash().GetMessage(), originalCrash.GetMessage(); msg != want {
		t.Errorf("crash message = %q, want original %q preserved", msg, want)
	}
	if gotSnap := snapshotAtLatestGeneration(got.GetStatus()); gotSnap.GetUuid() != "local-snap-1" {
		t.Errorf("in-progress local snapshot uuid = %q, want %q preserved", gotSnap.GetUuid(), "local-snap-1")
	}
	if got.GetStatus().GetWorkerAssignment() != nil {
		t.Errorf("WorkerAssignment = %v, want nil", got.GetStatus().GetWorkerAssignment())
	}
	if len(*records) != 0 {
		t.Errorf("got %d crash records, want 0 for an actor that was already crashed", len(*records))
	}
}

// TestSuspendActorAsync_Finalized_KeepsCrashFromWorkerDelete verifies that a crash the
// DeleteWorker workflow commits while the async suspend's finalize is freeing the
// worker is kept: the finalize update is pinned to the actor version it
// checked, so it fails Aborted instead of committing SUSPENDED over the crash.
func TestSuspendActorAsync_Finalized_KeepsCrashFromWorkerDelete(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	seedSuspendTemplate(t, ctx, st, nil)
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}

	created := storetest.MustCreateActor(t, ctx, st, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: suspendTestTemplateRef(),
		Status: &ateapipb.ActorStatus{
			State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
			AssignedNode:     "node-1",
			WorkerAssignment: wireTestAssignment(),
		},
	})
	seedWireWorker(t, ctx, st, assignmentOf(created))

	w, _ := newWireCaptureWorkflow(t, workerDeletedDuringReleaseStore{Interface: st})
	_, err := w.SuspendActor(ctx, actorRef, suspendAsync)
	assertConcurrentCrashKept(t, ctx, st, actorRef, err, "suspend failed: "+crashMessageWorkerPodGone)
}

// TestSuspendActorAsync_Finalized_RecordsFidelity verifies async suspend records the template's
// preferred_fidelity on the local Snapshot alongside Locality, and keeps its
// durable copy PENDING for atelet to upload in the background.
func TestSuspendActorAsync_Finalized_RecordsFidelity(t *testing.T) {
	tests := []struct {
		name      string
		preferred ateapipb.SnapshotFidelity
		want      ateapipb.SnapshotFidelity
	}{
		{"VOLUMES", ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES},
		{"ROOTFS", ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS},
		{"MEMORY", ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY},
		// The store defaults an unset preferred_fidelity to MEMORY.
		{"UNSPECIFIED defaults to MEMORY", ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := newTestPersistence(t)
			seedSuspendTemplate(t, ctx, st, &ateapipb.SnapshotConfig{PreferredFidelity: tc.preferred})
			w, _ := newWireCaptureWorkflow(t, st)
			actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}

			wantRuntime := &ateapipb.ActorSandboxRuntime{
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
			created := storetest.MustCreateActor(t, ctx, st, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
				ActorTemplate: suspendTestTemplateRef(),
				Status: &ateapipb.ActorStatus{
					State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
					AssignedNode:     "node-1",
					WorkerAssignment: assignment,
				},
			})
			seedWireWorker(t, ctx, st, assignmentOf(created))

			got, err := w.SuspendActor(ctx, actorRef, suspendAsync)
			if err != nil {
				t.Fatalf("SuspendActor: %v", err)
			}

			if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
				t.Fatalf("state = %v, want SUSPENDED", got.GetStatus().GetState())
			}
			if !latestSnapshotIsLocal(got.GetStatus()) {
				t.Errorf("snapshots = %v, want the latest one local", got.GetStatus().GetSnapshots())
			}
			localSnap := findLatestLocalSnapshot(got.GetStatus())
			if localSnap.GetFidelity() != tc.want {
				t.Errorf("local Snapshot.Fidelity = %v, want %v", localSnap.GetFidelity(), tc.want)
			}
			if got := localSnap.GetDurableSnapshot().GetStatus(); got != ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING {
				t.Errorf("local DurableSnapshot.Status = %v, want PENDING", got)
			}
			if got := localSnap.GetDurableSnapshot().GetObject().GetSnapshotUri(); got == "" {
				t.Error("local DurableSnapshot records no upload URI")
			}
			if localSnap.GetLocality() != "node-1" {
				t.Errorf("local Snapshot.Locality = %q, want %q", localSnap.GetLocality(), "node-1")
			}
			if diff := cmp.Diff(wantRuntime, localSnap.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
				t.Errorf("Snapshot.SandboxRuntime mismatch (-want +got):\n%s", diff)
			}
			if got.GetStatus().GetAssignedNode() != "node-1" {
				t.Errorf("AssignedNode = %q, want %q", got.GetStatus().GetAssignedNode(), "node-1")
			}
			if got.GetStatus().GetWorkerAssignment() != nil {
				t.Errorf("WorkerAssignment = %v, want nil", got.GetStatus().GetWorkerAssignment())
			}
			if a := firstAssignment(t, st, wireTestAssignment().GetWorker().GetName()); a != nil {
				t.Errorf("worker assignment = %v, want released", a)
			}
		})
	}
}

// TestSuspendActorAsync_Finalized_MissingSnapshotClearsAssignment verifies that if the
// latest generation snapshot entry, or its PENDING durable copy, is missing
// when the async suspend finalizes, the actor transitions to CRASHED and clears
// WorkerAssignment after releasing the worker rather than getting stuck
// SUSPENDING, and the suspend fails.
func TestSuspendActorAsync_Finalized_MissingSnapshotClearsAssignment(t *testing.T) {
	tests := []struct {
		name string
		// clobber rewrites the record while atelet checkpoints.
		clobber          func(s *ateapipb.ActorStatus)
		wantCrashMessage string
	}{
		{
			name:             "no snapshot at the latest generation",
			clobber:          func(s *ateapipb.ActorStatus) { s.Snapshots = nil },
			wantCrashMessage: "suspend failed: " + crashMessageLocalSnapshotMissing,
		},
		{
			name: "no durable copy of the latest snapshot",
			clobber: func(s *ateapipb.ActorStatus) {
				for _, snap := range s.Snapshots {
					snap.DurableSnapshot = nil
				}
			},
			wantCrashMessage: "suspend failed: " + crashMessageDurableSnapshotMissing,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st := newTestPersistence(t)
			seedSuspendTemplate(t, ctx, st, nil)
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
						newInProgressSnapshot(t, 1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "local-snap-1"),
					},
				},
			})
			seedWireWorker(t, ctx, st, assignmentOf(created))
			updateDuringCheckpoint(t, atelet, st, actorRef, tt.clobber)

			if _, err := w.SuspendActor(ctx, actorRef, suspendAsync); apierror.Code(err) != codes.FailedPrecondition {
				t.Fatalf("Async SuspendActor error = %v, want FailedPrecondition for the actor it crashed", err)
			}
			got := mustReadActor(t, ctx, st, actorRef)
			if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
				t.Errorf("state = %v, want CRASHED", got.GetStatus().GetState())
			}
			if msg := got.GetStatus().GetCrash().GetMessage(); msg != tt.wantCrashMessage {
				t.Errorf("crash message = %q, want %q", msg, tt.wantCrashMessage)
			}
			if got.GetStatus().GetWorkerAssignment() != nil {
				t.Errorf("WorkerAssignment = %v, want nil", got.GetStatus().GetWorkerAssignment())
			}
			if len(*records) != 1 {
				t.Fatalf("got %d crash records, want 1", len(*records))
			}
			if got := (*records)[0].attrs[string(ateattr.ActorOperationNameKey)]; got != ateattr.OperationSuspend {
				t.Errorf("crash record %s = %q, want %q", ateattr.ActorOperationNameKey, got, ateattr.OperationSuspend)
			}
		})
	}
}

func TestSuspendActorAsync_AteletPaused_DialFailureLeavesActorRetryable(t *testing.T) {
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
			seedSuspendTemplate(t, ctx, persistence, nil)

			var snapshots []*ateapipb.Snapshot
			if tt.prevSnapshot != "" {
				snapshots = append(snapshots, newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "prev-snapshot", tt.prevSnapshot, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED))
			}
			snapshots = append(snapshots, newInProgressSnapshot(t, 2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "actor-1-never-written"))

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
			_, err := w.SuspendActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"}, suspendAsync)
			assertStoppedAtDial(t, err)

			stored := mustReadActor(t, ctx, persistence, resources.ActorRef{Atespace: "team-a", Name: "actor-1"})
			if stored.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDING {
				t.Errorf("state = %v, want unchanged SUSPENDING", stored.GetStatus().GetState())
			}
			if gotSnap := snapshotAtLatestGeneration(stored.GetStatus()); gotSnap.GetUuid() != "actor-1-never-written" {
				t.Errorf("in-progress local snapshot uuid = %q, want preserved for debugging", gotSnap.GetUuid())
			}
			gotSnap := findLatestDurableSnapshot(stored.GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
			if got := gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri(); got != tt.prevSnapshot {
				t.Errorf("SnapshotUri = %q, want %q", got, tt.prevSnapshot)
			}
		})
	}
}

// TestSuspendActorAsync_CrashesWhenPausingActorMissingWorkerPod verifies that a
// suspending actor with no worker pod recorded is moved to CRASHED by the
// corrupted-assignment check before atelet is called, and the async suspend
// fails with FailedPrecondition.
func TestSuspendActorAsync_CrashesWhenPausingActorMissingWorkerPod(t *testing.T) {
	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	w := newTestActorWorkflow(t, st, "ns", "tmpl1")

	seedWorkflowActor(t, ctx, st, resources.ActorRef{Atespace: "team-a", Name: "id1"}, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_SUSPENDING, withPauseInProgress(t))

	_, err := w.SuspendActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, suspendAsync)
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

// TestSuspendActorAsync_AteletPaused_ForwardsPreferredFidelity verifies that async
// suspend checkpoints forward the template's PreferredFidelity to atelet, and
// label the suspend's lifecycle-op metric with it.
func TestSuspendActorAsync_AteletPaused_ForwardsPreferredFidelity(t *testing.T) {
	tests := []struct {
		preferred ateapipb.SnapshotFidelity
		want      ateletpb.SnapshotFidelity
	}{
		{ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY},
		{ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES},
		// The store defaults an unset preferred_fidelity to MEMORY.
		{ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY},
	}
	for _, tc := range tests {
		t.Run(tc.preferred.String(), func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{PreferredFidelity: tc.preferred})
			w, atelet := newWireCaptureWorkflow(t, persistence)
			inst, reader := newTestInstruments(t)
			w.instruments = inst

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
						newInProgressSnapshot(t, 1, tc.preferred, "local-snap-1"),
					},
				},
			})

			if _, err := w.SuspendActor(ctx, actorRef, suspendAsync); err != nil {
				t.Fatalf("SuspendActor: %v", err)
			}
			req := atelet.checkpointRequest()
			if req == nil {
				t.Fatal("expected Checkpoint RPC to be called")
			}
			if got := req.GetSnapshot().GetFidelity(); got != tc.want {
				t.Errorf("CheckpointRequest.Snapshot.Fidelity = %v, want %v", got, tc.want)
			}
			if got := req.GetSnapshot().GetSnapshotUuid(); got != "local-snap-1" {
				t.Errorf("CheckpointRequest.Snapshot.SnapshotUuid = %q, want %q", got, "local-snap-1")
			}
			if got := req.GetSnapshot().GetGeneration(); got != 1 {
				t.Errorf("CheckpointRequest.Snapshot.Generation = %d, want 1, the in-progress snapshot's", got)
			}
			// The mark recorded the upload URI; the checkpoint goes there.
			wantURI := someActorSnapshotURI(t, testStorageLocation, "team-a", "local-snap-1")
			if got := req.GetSnapshot().GetObject().GetSnapshotUri(); got != wantURI {
				t.Errorf("CheckpointRequest.Snapshot.Object.SnapshotUri = %q, want %q", got, wantURI)
			}
			if got := req.GetUploadConfig().GetMode(); got != ateletpb.UploadMode_UPLOAD_MODE_DEFERRED {
				t.Errorf("CheckpointRequest.UploadConfig.Mode = %v, want UPLOAD_MODE_DEFERRED", got)
			}
			// The fidelity sent to atelet labels the suspend's lifecycle-op metric.
			assertLifecycleFidelity(t, reader, ateattr.SnapshotFidelityValue(tc.want))
		})
	}
}
