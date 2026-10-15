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
	"fmt"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
)

// TestPauseActor_Finalized_WorkerGone reproduces the scenario where the
// worker pod disappears from the DB during pause finalization, so the node it
// ran on is unknown.
//
// Current behavior: AssignedNode is left empty, and the actor is crashed
// instead of left paused, since a local snapshot with an unknown node can
// never be safely resumed. The pause fails with FailedPrecondition rather than
// reporting the pause as done.
func TestPauseActor_Finalized_WorkerGone(t *testing.T) {
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
				newLocalSnapshot(1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local-snap-1", ""),
			},
		},
	})
	// Intentionally NOT creating the worker in store, simulates worker already gone.

	if _, err := w.PauseActor(ctx, actorRef); apierror.Code(err) != codes.FailedPrecondition {
		t.Fatalf("PauseActor error = %v, want FailedPrecondition for the actor it crashed", err)
	}
	if checkpoints, _ := atelet.calls(); checkpoints != 1 {
		t.Errorf("Checkpoint calls = %d, want 1 before finalize", checkpoints)
	}

	got := mustReadActor(t, ctx, st, actorRef)
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("state = %v, want CRASHED (node name unknown, cannot resume safely)", got.GetStatus().GetState())
	}
	if msg, want := got.GetStatus().GetCrash().GetMessage(), "pause failed: "+crashMessageLocalSnapshotNodeUnknown; msg != want {
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
	// a pause-finalize crash is the one kind nothing can attribute to an actor.
	if len(*records) != 1 {
		t.Fatalf("got %d crash records, want 1", len(*records))
	}
	if got := (*records)[0].attrs[string(ateattr.ActorUIDKey)]; got == "" {
		t.Error("crash record carries no ate.actor.uid")
	}
}

// TestPauseActor_Finalized_AlreadyCrashed verifies that if the actor was
// crashed out-of-band while it checkpointed, and has no AssignedNode, the
// pause's finalize preserves the existing Crash status and snapshots, emits no
// duplicate crash log record, and still fails.
func TestPauseActor_Finalized_AlreadyCrashed(t *testing.T) {
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
				newLocalSnapshot(1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local-snap-1", ""),
			},
		},
	})
	originalCrash := newActorCrash(ateattr.OperationPause, "original crash reason")
	updateDuringCheckpoint(t, atelet, st, actorRef, func(s *ateapipb.ActorStatus) {
		s.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
		s.Crash = originalCrash
	})

	if _, err := w.PauseActor(ctx, actorRef); apierror.Code(err) != codes.FailedPrecondition {
		t.Fatalf("PauseActor error = %v, want FailedPrecondition for a CRASHED actor", err)
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

// TestPauseActor_Finalized_KeepsCrashFromWorkerDelete verifies that a crash the
// DeleteWorker workflow commits while the pause's finalize is freeing the
// worker is kept: the finalize update is pinned to the actor version it
// checked, so it fails Aborted instead of committing SUSPENDED over the crash.
func TestPauseActor_Finalized_KeepsCrashFromWorkerDelete(t *testing.T) {
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
	_, err := w.PauseActor(ctx, actorRef)
	assertConcurrentCrashKept(t, ctx, st, actorRef, err, "pause failed: "+crashMessageWorkerPodGone)
}

// TestPauseActor_Finalized_RecordsFidelity verifies pause records the template's
// preferred_fidelity on the local Snapshot alongside Locality and leaves
// DurableSnapshot unset until the actor is suspended.
func TestPauseActor_Finalized_RecordsFidelity(t *testing.T) {
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

			got, err := w.PauseActor(ctx, actorRef)
			if err != nil {
				t.Fatalf("PauseActor: %v", err)
			}

			if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
				t.Fatalf("state = %v, want SUSPENDED", got.GetStatus().GetState())
			}
			if !latestSnapshotIsLocalOnly(got.GetStatus()) {
				t.Errorf("snapshots = %v, want the latest one local only", got.GetStatus().GetSnapshots())
			}
			localSnap := findLatestLocalSnapshot(got.GetStatus())
			if localSnap.GetFidelity() != tc.want {
				t.Errorf("local Snapshot.Fidelity = %v, want %v", localSnap.GetFidelity(), tc.want)
			}
			if localSnap.GetDurableSnapshot() != nil {
				t.Errorf("local DurableSnapshot = %v, want nil", localSnap.GetDurableSnapshot())
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

// TestPauseActor_Finalized_MissingLocalSnapshotClearsAssignment verifies that if the
// latest generation snapshot entry is missing when the pause finalizes, the
// actor transitions to CRASHED and clears WorkerAssignment after releasing
// the worker rather than getting stuck SUSPENDING, and the pause fails.
func TestPauseActor_Finalized_MissingLocalSnapshotClearsAssignment(t *testing.T) {
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
				newLocalSnapshot(1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local-snap-1", ""),
			},
		},
	})
	seedWireWorker(t, ctx, st, assignmentOf(created))
	updateDuringCheckpoint(t, atelet, st, actorRef, func(s *ateapipb.ActorStatus) {
		s.Snapshots = nil
	})

	if _, err := w.PauseActor(ctx, actorRef); apierror.Code(err) != codes.FailedPrecondition {
		t.Fatalf("PauseActor error = %v, want FailedPrecondition for the actor it crashed", err)
	}
	got := mustReadActor(t, ctx, st, actorRef)
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("state = %v, want CRASHED", got.GetStatus().GetState())
	}
	if msg, want := got.GetStatus().GetCrash().GetMessage(), "pause failed: "+crashMessageLocalSnapshotMissing; msg != want {
		t.Errorf("crash message = %q, want %q", msg, want)
	}
	if got.GetStatus().GetWorkerAssignment() != nil {
		t.Errorf("WorkerAssignment = %v, want nil", got.GetStatus().GetWorkerAssignment())
	}
	if len(*records) != 1 {
		t.Fatalf("got %d crash records, want 1", len(*records))
	}
}

// TestPauseActorWorkflow_RejectedAndIdempotentPaths covers the two
// short-circuit paths of the pause workflow: rejection of the pause edge for
// a non-RUNNING actor and the idempotent fast-forward for a paused one. Both
// are SUSPENDED: a node-local latest snapshot is what tells them apart.
func TestPauseActorWorkflow_RejectedAndIdempotentPaths(t *testing.T) {
	tests := []struct {
		name      string
		seedState ateapipb.ActorState
		seedOpts  []func(*ateapipb.Actor)
		// wantErr true means PauseActor must fail with FailedPrecondition.
		wantErr bool
		// wantState is the stored state after the call.
		wantState ateapipb.ActorState
	}{
		{
			// Pausing a SUSPENDED actor is rejected by the pause's state gate
			// and the actor's state is left untouched.
			name:      "not running rejected",
			seedState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			wantErr:   true,
			wantState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		},
		{
			// Pausing a paused actor succeeds idempotently via IsComplete
			// fast-forward without calling atelet.
			name:      "already paused succeeds",
			seedState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			seedOpts:  []func(*ateapipb.Actor){withPausedShape("node-1")},
			wantState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			w := newTestActorWorkflow(t, st, "ns", "tmpl1")

			seedWorkflowActor(t, ctx, st, resources.ActorRef{Atespace: "team-a", Name: "id1"}, "ns", "tmpl1", tc.seedState, tc.seedOpts...)

			actor, err := w.PauseActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
			if tc.wantErr {
				if got := apierror.Code(err); got != codes.FailedPrecondition {
					t.Fatalf("apierror.Code(err) = %v, want %v (err: %v)", got, codes.FailedPrecondition, err)
				}
			} else {
				if err != nil {
					t.Fatalf("PauseActor failed: %v", err)
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

// TestPauseActor_MarkedPausing_StateMatrix verifies the pause edge's state gating against
// every actor state: RUNNING takes the edge, everything else is rejected with
// FailedPrecondition and left untouched. SUSPENDED is rejected whatever its
// snapshots short of the paused shape, which fast-forwards (see
// TestPauseActorWorkflow_RejectedAndIdempotentPaths). SUSPENDING without a
// pending node-local snapshot is a suspend in progress, which owns the actor;
// a pause in progress is covered by TestPauseActor_MarkedPausing_ReentrySkipsPauseInProgress.
func TestPauseActor_MarkedPausing_StateMatrix(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	seedSuspendTemplate(t, ctx, st, nil)
	w := newMarkOnlyWorkflow(st)

	for _, seedState := range allActorStates {
		actorRef := resources.ActorRef{Atespace: "team-a", Name: fmt.Sprintf("id%d", seedState)}
		seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", seedState, func(a *ateapipb.Actor) {
			a.Status.WorkerAssignment = wireTestAssignment()
		})

		_, err := w.PauseActor(ctx, actorRef)
		stored := mustReadActor(t, ctx, st, actorRef)
		if seedState == ateapipb.ActorState_ACTOR_STATE_RUNNING {
			assertStoppedAtDial(t, err)
			if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDING {
				t.Errorf("state %v: stored state = %v, want SUSPENDING", seedState, got)
			}
			if !latestSnapshotIsPendingLocal(stored.GetStatus()) {
				t.Errorf("state %v: snapshots = %v, want a pending local snapshot at the latest generation", seedState, stored.GetStatus().GetSnapshots())
			}
			continue
		}
		if got := apierror.Code(err); got != codes.FailedPrecondition {
			t.Errorf("state %v: code = %v, want FailedPrecondition (err: %v)", seedState, got, err)
		}
		if got := stored.GetStatus().GetState(); got != seedState {
			t.Errorf("state %v: stored state = %v, want it untouched", seedState, got)
		}
	}
}

// TestPauseActor_MarkedPausing_ReentrySkipsPauseInProgress verifies a re-entered pause
// recognizes its own mark, a SUSPENDING actor whose latest generation is a
// pending node-local snapshot, and checkpoints under that snapshot without
// minting another, while a running-origin suspend in progress (pending
// durable snapshot) is rejected before atelet is called.
func TestPauseActor_MarkedPausing_ReentrySkipsPauseInProgress(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	seedSuspendTemplate(t, ctx, st, nil)
	w, atelet := newWireCaptureWorkflow(t, st)
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}

	seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_SUSPENDING, withPauseInProgress(), func(a *ateapipb.Actor) {
		a.Status.AssignedNode = "node-1"
		a.Status.WorkerAssignment = wireTestAssignment()
	})
	paused, err := w.PauseActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("PauseActor on a pause in progress: %v", err)
	}
	if got := atelet.checkpointRequest().GetSnapshot().GetSnapshotUuid(); got != "pause-snap" {
		t.Errorf("CheckpointRequest.Snapshot.SnapshotUuid = %q, want the persisted %q", got, "pause-snap")
	}
	if got := paused.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("state = %v, want SUSPENDED", got)
	}
	if got := paused.GetStatus().GetLastAssignedGeneration(); got != 1 {
		t.Errorf("last_assigned_generation = %d, want 1 (no new snapshot minted on re-entry)", got)
	}
	if got := len(paused.GetStatus().GetSnapshots()); got != 1 {
		t.Errorf("len(snapshots) = %d, want 1", got)
	}

	suspendingRef := resources.ActorRef{Atespace: actorRef.Atespace, Name: "id2"}
	seedWorkflowActor(t, ctx, st, suspendingRef, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_SUSPENDING, func(a *ateapipb.Actor) {
		a.Status.AssignedNode = "node-1"
		a.Status.WorkerAssignment = wireTestAssignment()
		a.Status.LastAssignedGeneration = 1
		a.Status.Snapshots = []*ateapipb.Snapshot{
			newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "snap", "gs://snapshots/s", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
		}
	})
	_, err = w.PauseActor(ctx, suspendingRef)
	if got := apierror.Code(err); got != codes.FailedPrecondition {
		t.Errorf("PauseActor on a suspend in progress: code = %v, want FailedPrecondition (err: %v)", got, err)
	}
	if checkpoints, _ := atelet.calls(); checkpoints != 1 {
		t.Errorf("Checkpoint calls = %d, want 1 (none for the suspend in progress)", checkpoints)
	}
}

func TestPauseActor_AteletPaused_DialFailureLeavesActorRetryable(t *testing.T) {
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
			snapshots = append(snapshots, newLocalSnapshot(2, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "actor-1-never-written", ""))

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
			_, err := w.PauseActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "actor-1"})
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

// TestPauseActor_CrashesWhenPausingActorMissingWorkerPod verifies that a
// pausing actor with no worker pod recorded is moved to CRASHED by the
// corrupted-assignment check before atelet is called, and the pause fails
// with FailedPrecondition.
func TestPauseActor_CrashesWhenPausingActorMissingWorkerPod(t *testing.T) {
	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	w := newTestActorWorkflow(t, st, "ns", "tmpl1")

	seedWorkflowActor(t, ctx, st, resources.ActorRef{Atespace: "team-a", Name: "id1"}, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_SUSPENDING, withPauseInProgress())

	_, err := w.PauseActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"})
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
	if msg, want := got.GetStatus().GetCrash().GetMessage(), "pause failed: "+crashMessageWorkerAssignmentMissing; msg != want {
		t.Errorf("crash message = %q, want %q", msg, want)
	}
}

// TestPauseActor_MarkedPausing_GoldenAtespaceRejected verifies golden actors cannot be
// paused: by design they can only be suspended (committed).
func TestPauseActor_MarkedPausing_GoldenAtespaceRejected(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	seedSuspendTemplate(t, ctx, st, nil)
	w := newMarkOnlyWorkflow(st)

	actorRef := resources.ActorRef{Atespace: resources.GoldenActorAtespace, Name: "golden-1"}
	seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_RUNNING, func(a *ateapipb.Actor) {
		a.Status.AssignedNode = "node-1"
		a.Status.WorkerAssignment = wireTestAssignment()
	})
	_, err := w.PauseActor(ctx, actorRef)
	if got := apierror.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("apierror.Code = %v (err %v), want FailedPrecondition", got, err)
	}
	if got := mustReadActor(t, ctx, st, actorRef).GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("stored state = %v, want RUNNING untouched", got)
	}
}

// TestPauseActor_AteletPaused_ForwardsPreferredFidelity verifies that local pause
// checkpoints forward the template's PreferredFidelity to atelet, and label the
// pause's lifecycle-op metric with it.
func TestPauseActor_AteletPaused_ForwardsPreferredFidelity(t *testing.T) {
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
						newLocalSnapshot(1, tc.preferred, "", "local-snap-1", ""),
					},
				},
			})

			if _, err := w.PauseActor(ctx, actorRef); err != nil {
				t.Fatalf("PauseActor: %v", err)
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
			if got := req.GetSnapshot().GetObject(); got != nil {
				t.Errorf("CheckpointRequest.Snapshot.Object = %v, want nil for a node-local checkpoint", got)
			}
			if got := req.GetUploadConfig().GetMode(); got != ateletpb.UploadMode_UPLOAD_MODE_DEFERRED {
				t.Errorf("CheckpointRequest.UploadConfig.Mode = %v, want UPLOAD_MODE_DEFERRED", got)
			}
			// The fidelity sent to atelet labels the pause's lifecycle-op metric.
			assertLifecycleFidelity(t, reader, ateattr.SnapshotFidelityValue(tc.want))
		})
	}
}
