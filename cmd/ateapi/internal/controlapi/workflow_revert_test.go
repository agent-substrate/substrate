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

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/objectstore/objectstoretest"
	"go.ate.dev/substrate/internal/objectstoreplugin/objectstoreplugintest"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// TestEnsureMarkedReverting_StateMatrix pins which states a revert is accepted
// from. It runs over every enum value, so a state added to the proto without a
// decision here fails rather than silently landing in the reject branch.
//
// SUSPENDED is the load-bearing rejection: it is revert's own destination, and
// also what a suspend that won the race leaves behind, so accepting it would
// report success for the opposite of what was asked.
func TestEnsureMarkedReverting_StateMatrix(t *testing.T) {
	allowed := map[ateapi.ActorState]bool{
		ateapi.ActorState_ACTOR_STATE_RUNNING: true,
		ateapi.ActorState_ACTOR_STATE_CRASHED: true,
		// Re-entry: a previous attempt already marked it, so the step skips
		// rather than re-committing.
		ateapi.ActorState_ACTOR_STATE_REVERTING: true,
	}

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

		updated, err := w.ensureMarkedReverting(ctx, actorRef, actor)
		assertPrerequisiteResult(t, seedState, err, allowed[seedState])
		if err == nil {
			if got := updated.GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_REVERTING {
				t.Errorf("state %v: ensureMarkedReverting returned actor in %v, want REVERTING", seedState, got)
			}
		}
		cleanup()
	}
}

// Locality is a cache detail, not an additional lifecycle state.
func TestEnsureMarkedReverting_RejectsPaused(t *testing.T) {
	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	w := newTestActorWorkflow(t, st, "ns", "tmpl1")

	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
	seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", ateapi.ActorState_ACTOR_STATE_SUSPENDED, withPausedShape(t, "node-1"))
	actor, err := st.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("get seeded actor: %v", err)
	}

	if _, err := w.ensureMarkedReverting(ctx, actorRef, actor); apierror.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ensureMarkedReverting = %v, want FailedPrecondition", err)
	}
}

// TestRevertActor_ReturnsActorToItsSnapshot covers the whole workflow from each
// accepted origin. A successful local checkpoint survives an interrupted newer
// checkpoint, whether or not an older durable fallback exists.
func TestRevertActor_ReturnsActorToItsSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name            string
		seedState       ateapi.ActorState
		noDurable       bool
		unrecordedLocal bool
	}{
		{name: "RUNNING", seedState: ateapi.ActorState_ACTOR_STATE_RUNNING},
		{name: "RUNNING without durable fallback", seedState: ateapi.ActorState_ACTOR_STATE_RUNNING, noDurable: true},
		{name: "CRASHED", seedState: ateapi.ActorState_ACTOR_STATE_CRASHED},
		{name: "CRASHED after committing checkpoint", seedState: ateapi.ActorState_ACTOR_STATE_CRASHED, unrecordedLocal: true},
		{name: "CRASHED after first checkpoint", seedState: ateapi.ActorState_ACTOR_STATE_CRASHED, unrecordedLocal: true, noDurable: true},
		// A revert that died partway is retried, not rejected.
		{name: "REVERTING", seedState: ateapi.ActorState_ACTOR_STATE_REVERTING},
	} {
		seedState := tc.seedState
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			w := newTestActorWorkflow(t, st, "ns", "tmpl1")
			wire, atelet := newWireCaptureWorkflow(t, st)
			w.dialer = wire.dialer
			objects := objectstoretest.New()
			w.objectStorePlugin = objectstoreplugintest.ControlClient(objects)

			actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
			seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", seedState)
			actor, err := st.GetActor(ctx, actorRef)
			if err != nil {
				t.Fatalf("get seeded actor: %v", err)
			}

			// Locality is only committed after a successful checkpoint. The newer
			// PENDING snapshot without locality is an interrupted suspend.
			const keptURI = "gs://snapshots/atespaces/team-a/actors/keep/snapshots/snapshot"
			tmpl := &ateapi.ActorTemplate{SnapshotConfig: &ateapi.SnapshotConfig{StorageLocation: "gs://snapshots"}}
			actor = mustUpdateActorStatus(t, ctx, st, actor, func(s *ateapi.ActorStatus) {
				local1 := newPausedSnapshot(t, 2, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local-1", "node-1")
				local1.DurableSnapshot.Object.SnapshotUri = mustActorSnapshotURI(t, tmpl, actor, "local-1").String()
				inProgress := newPausedSnapshot(t, 3, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local-in-progress", "")
				inProgress.DurableSnapshot.Object.SnapshotUri = mustActorSnapshotURI(t, tmpl, actor, "local-in-progress").String()
				objects.PutSnapshot(t, mustActorSnapshotURI(t, tmpl, actor, "local-1"), "manifest.json")
				objects.PutSnapshot(t, mustActorSnapshotURI(t, tmpl, actor, "local-in-progress"), "manifest.json")
				s.AssignedNode = "node-1"
				s.WorkerAssignment = wireTestAssignment()
				s.LastAssignedGeneration = 3
				s.Snapshots = []*ateapi.Snapshot{
					newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "kept", keptURI, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
					local1,
					inProgress,
				}
				if tc.noDurable {
					s.Snapshots = s.Snapshots[1:]
				}
				if tc.unrecordedLocal {
					local1.Locality = ""
					if !tc.noDurable {
						s.Snapshots[0].Locality = "node-1"
					}
				}
			})
			seedWireWorker(t, ctx, st, assignmentOf(actor))
			local := findSnapshotByGeneration(actor.GetStatus().GetSnapshots(), 2)
			atelet.terminateState = &ateletpb.ActorSnapshotState{LatestLocal: &ateletpb.SnapshotReport{Snapshot: &ateletpb.Snapshot{
				SnapshotUuid: local.GetUuid(), Generation: local.GetGeneration(), Fidelity: fidelityToAtelet(local.GetFidelity()),
				Object: &ateletpb.ObjectStorage{SnapshotUri: local.GetDurableSnapshot().GetObject().GetSnapshotUri()},
			}}}

			reverted, err := w.RevertActor(ctx, actorRef)
			if err != nil {
				t.Fatalf("RevertActor: %v", err)
			}

			gotStatus := reverted.GetStatus()
			if got := gotStatus.GetState(); got != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
				t.Errorf("state = %v, want SUSPENDED", got)
			}
			// LastAssignedGeneration is not decreased on revert so we can keep track
			// of stale snapshots that need to be cleaned up and future snapshots
			// never reuse generations 2 or 3 after revert discards them.
			if got := gotStatus.GetLastAssignedGeneration(); got != 3 {
				t.Errorf("last_assigned_generation = %d, want 3", got)
			}
			gotSnap := findLatestDurableSnapshot(gotStatus, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
			if got := gotSnap.GetGeneration(); !tc.noDurable && got != 1 {
				t.Errorf("retained snapshot generation = %d, want 1", got)
			}
			if got := gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri(); !tc.noDurable && got != keptURI {
				t.Errorf("external snapshot = %q, want it untouched at %q", got, keptURI)
			}
			if got := findLatestLocalSnapshot(gotStatus); got.GetGeneration() != 2 || got.GetLocality() != "node-1" {
				t.Errorf("local snapshot = %v, want generation 2 on node-1", got)
			}
			if got := findLatestDurableSnapshot(gotStatus, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING); got.GetGeneration() != 2 {
				t.Errorf("pending snapshot = %v, want the committed local generation 2", got)
			}
			wantSnapshots := 2
			if tc.noDurable {
				wantSnapshots = 1
			}
			if got := len(gotStatus.GetSnapshots()); got != wantSnapshots {
				t.Errorf("len(snapshots) = %d, want %d", got, wantSnapshots)
			}
			if got := gotStatus.GetAssignedNode(); got != "node-1" {
				t.Errorf("assigned node = %q, want node-1", got)
			}
			if len(objects.Snapshot(t, mustActorSnapshotURI(t, tmpl, actor, "local-1"))) == 0 {
				t.Error("committed local snapshot's pending objects were deleted")
			}
			if len(objects.Snapshot(t, mustActorSnapshotURI(t, tmpl, actor, "local-in-progress"))) != 0 {
				t.Error("incomplete checkpoint's objects survived revert")
			}
			if !atelet.terminateRequest().GetPreserveSnapshots() {
				t.Error("TerminateRequest did not preserve snapshots")
			}
			if got := gotStatus.GetWorkerAssignment(); got != nil {
				t.Errorf("worker assignment = %v, want nil", got)
			}

			// If the actor runs again and takes another snapshot, it must
			// allocate generation 4 rather than reusing 2 or 3.
			running := mustUpdateActorStatus(t, ctx, st, reverted, func(s *ateapi.ActorStatus) {
				s.State = ateapi.ActorState_ACTOR_STATE_RUNNING
				s.AssignedNode = "node-1"
				s.WorkerAssignment = wireTestAssignment()
			})
			seedAssignment(t, st, wireTestAssignment().GetWorker().GetName(), assignmentOf(running))
			suspended, err := wire.SuspendActor(ctx, actorRef, suspendSync)
			if err != nil {
				t.Fatalf("SuspendActor after revert: %v", err)
			}
			if got := suspended.GetStatus().GetLastAssignedGeneration(); got != 4 {
				t.Errorf("last_assigned_generation after post-revert suspend = %d, want 4", got)
			}
			newSnap := snapshotAtLatestGeneration(suspended.GetStatus())
			if got := newSnap.GetGeneration(); got != 4 {
				t.Errorf("new snapshot generation after post-revert suspend = %d, want 4", got)
			}
			if got, want := atelet.checkpointRequest().GetSnapshot().GetSnapshotUuid(), newSnap.GetUuid(); got != want {
				t.Errorf("CheckpointRequest.Snapshot.SnapshotUuid = %q, want the generation 4 snapshot %q", got, want)
			}
		})
	}
}

func TestApplyRevertedSnapshotState(t *testing.T) {
	for _, tc := range []struct {
		name             string
		change           func(*ateletpb.ActorSnapshotState)
		wantErr          bool
		completed        bool
		registeredUpload bool
	}{
		{name: "matching local"},
		{name: "older upload still recorded", registeredUpload: true, change: func(s *ateletpb.ActorSnapshotState) { s.Uploading = ateletReported(t, 1) }},
		{name: "empty tracker with durable checkpoint", completed: true, change: func(s *ateletpb.ActorSnapshotState) { s.LatestLocal = nil }},
		{name: "older upload no longer recorded", change: func(s *ateletpb.ActorSnapshotState) { s.Uploading = ateletReported(t, 1) }},
		{name: "empty tracker", change: func(s *ateletpb.ActorSnapshotState) { s.LatestLocal = nil }, wantErr: true},
		{name: "stale tracker", change: func(s *ateletpb.ActorSnapshotState) { s.LatestLocal = ateletReported(t, 1) }, wantErr: true},
		{name: "different UUID", change: func(s *ateletpb.ActorSnapshotState) { s.LatestLocal.Snapshot.SnapshotUuid = "another" }, wantErr: true},
		{name: "different URI", change: func(s *ateletpb.ActorSnapshotState) {
			s.LatestLocal.Snapshot.Object.SnapshotUri = "gs://another/snapshot"
		}, wantErr: true},
		{name: "different fidelity", change: func(s *ateletpb.ActorSnapshotState) {
			s.LatestLocal.Snapshot.Fidelity = ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
		}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status := &ateapi.ActorStatus{Snapshots: []*ateapi.Snapshot{
				withDurable(t, 2, reportNode, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
			}}
			if tc.completed {
				status.Snapshots[0].DurableSnapshot.Status = ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
			}
			if tc.registeredUpload {
				status.Snapshots = append(status.Snapshots, withDurable(t, 1, reportNode, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING))
			}
			state := &ateletpb.ActorSnapshotState{LatestLocal: ateletReported(t, 2)}
			if tc.change != nil {
				tc.change(state)
			}
			err := applyRevertedSnapshotState(t.Context(), status, reportNode, state)
			if tc.wantErr {
				if apierror.Code(err) != codes.FailedPrecondition {
					t.Fatalf("applyRevertedSnapshotState = %v, want FailedPrecondition", err)
				}
			} else if err != nil {
				t.Fatal(err)
			} else if got := findLatestLocalSnapshot(status); !tc.completed && got.GetGeneration() != 2 {
				t.Errorf("latest local snapshot = %v, want generation 2", got)
			}
		})
	}
}

func TestRevertActor_AteletAlreadyDetached(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(fmt.Sprintf("retry=%v", retry), func(t *testing.T) {
			ctx := t.Context()
			st, cleanup := storetest.SetupTestStore(t)
			t.Cleanup(cleanup)
			w := newTestActorWorkflow(t, st, "ns", "tmpl1")
			wire, atelet := newWireCaptureWorkflow(t, st)
			w.dialer = wire.dialer
			ref := resources.ActorRef{Atespace: "team-a", Name: "id1"}
			state := ateapi.ActorState_ACTOR_STATE_CRASHED
			if retry {
				state = ateapi.ActorState_ACTOR_STATE_REVERTING
			}
			seedWorkflowActor(t, ctx, st, ref, "ns", "tmpl1", state, withPausedShape(t, "node-1"))
			actor, err := st.GetActor(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			local := findLatestLocalSnapshot(actor.GetStatus())
			atelet.terminateState = &ateletpb.ActorSnapshotState{LatestDurable: &ateletpb.SnapshotReport{Snapshot: &ateletpb.Snapshot{
				SnapshotUuid: local.GetUuid(), Generation: local.GetGeneration(), Fidelity: fidelityToAtelet(local.GetFidelity()),
				Object: &ateletpb.ObjectStorage{SnapshotUri: local.GetDurableSnapshot().GetObject().GetSnapshotUri()},
			}}}
			reverted, err := w.RevertActor(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			got := reverted.GetStatus()
			if got.GetAssignedNode() != "" || findLatestLocalSnapshot(got) != nil {
				t.Errorf("stale locality survived completed detach: %v", got)
			}
			if snap := findLatestDurableSnapshot(got, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED); snap.GetUuid() != local.GetUuid() {
				t.Errorf("completed upload lost: got %v, want %v", snap, local)
			}
		})
	}
}

func TestRevertActor_SnapshotOnAnotherNode(t *testing.T) {
	for _, tt := range []struct {
		name         string
		kept         bool
		assignedGone bool
		lookupErr    error
		lookupNode   string
		conflict     bool
		wantCode     codes.Code
	}{
		{name: "deleted source node"},
		{name: "source node still exists", kept: true},
		{name: "assigned node deleted, source node still exists", kept: true, assignedGone: true},
		{name: "node lookup fails", lookupErr: apierror.Unavailable("node lookup failed"), wantCode: codes.Unavailable},
		{name: "later node lookup fails", lookupErr: apierror.Unavailable("node lookup failed"), lookupNode: "node-4", wantCode: codes.Unavailable},
		{name: "concurrent update", conflict: true, wantCode: codes.Aborted},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			st := newTestPersistence(t)
			w, atelet := newWireCaptureWorkflow(t, st)
			ref := resources.ActorRef{Atespace: "team-a", Name: "id1"}
			assignedNode, snapshotNode := "node-1", "node-3"
			if tt.assignedGone {
				assignedNode, snapshotNode = snapshotNode, assignedNode
			}
			seedWorkflowActor(t, ctx, st, ref, "ns", "tmpl1", ateapi.ActorState_ACTOR_STATE_REVERTING, func(a *ateapi.Actor) {
				a.Status.AssignedNode = assignedNode
				a.Status.LastAssignedGeneration = 2
				a.Status.Snapshots = []*ateapi.Snapshot{
					newPausedSnapshot(t, 1, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "older-pending", snapshotNode),
					newPausedSnapshot(t, 2, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "pending", snapshotNode),
				}
				if tt.lookupNode != "" {
					a.Status.Snapshots[1].Locality = tt.lookupNode
				}
			})
			actor, err := st.GetActor(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			want := proto.CloneOf(actor.GetStatus()).GetSnapshots()
			var nodes []string
			if tt.kept {
				nodes = append(nodes, snapshotNode)
			}
			lookup := nodeGetter(nil, nodes...)
			lookups := map[string]int{}
			WithNodeGetter(func(ctx context.Context, node string) error {
				lookups[node]++
				if tt.lookupErr != nil && (tt.lookupNode == "" || tt.lookupNode == node) {
					return tt.lookupErr
				}
				if tt.conflict {
					if _, err := st.UpdateActor(ctx, ref, store.PreconditionFrom(actor), func(a *ateapi.Actor) error {
						a.WorkerSelector = &ateapi.Selector{MatchLabels: map[string]string{"changed": "true"}}
						return nil
					}); err != nil {
						t.Fatal(err)
					}
				}
				return lookup(ctx, node)
			})(w.dialer)

			err = w.ensureWorkerDiscarded(ctx, ref, actor, nil)
			if apierror.Code(err) != tt.wantCode {
				t.Fatalf("ensureWorkerDiscarded = %v, want %v", err, tt.wantCode)
			}
			stored, err := st.GetActor(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			if !tt.kept && tt.wantCode == codes.OK {
				for _, snapshot := range want {
					snapshot.Locality = ""
				}
			}
			got := stored.GetStatus().GetSnapshots()
			if len(got) != len(want) {
				t.Fatalf("snapshots = %v, want retained pending snapshots %v", got, want)
			}
			for i := range want {
				if !proto.Equal(got[i], want[i]) {
					t.Errorf("snapshot = %v, want retained pending snapshot %v", got[i], want[i])
				}
			}
			if lookups[snapshotNode] != 1 {
				t.Errorf("snapshot node lookups = %d, want 1", lookups[snapshotNode])
			}
			if tt.conflict && stored.GetWorkerSelector().GetMatchLabels()["changed"] != "true" {
				t.Error("concurrent update was overwritten")
			}
			if tt.wantCode != codes.OK && atelet.terminateRequest() != nil {
				t.Error("terminated the actor after failing to check the snapshot node")
			}
		})
	}
}

func TestRevertActor_UnreachableNode(t *testing.T) {
	for _, tt := range []struct {
		name     string
		node     string
		err      error
		pending  bool
		assigned bool
		crashed  bool
		conflict bool
		wantCode codes.Code
	}{
		{name: "missing atelet", node: "node-3"},
		{name: "unavailable atelet", node: "node-1", err: apierror.Unavailable("offline")},
		{name: "atelet deadline", node: "node-1", err: context.DeadlineExceeded},
		{name: "pending beside older completed snapshot", node: "node-1", err: apierror.Unavailable("offline"), pending: true, wantCode: codes.Unavailable},
		{name: "worker still assigned", node: "node-1", err: apierror.Unavailable("offline"), assigned: true, wantCode: codes.Unavailable},
		{name: "crashed execution still needs termination", node: "node-1", err: apierror.Unavailable("offline"), crashed: true, wantCode: codes.Unavailable},
		{name: "other RPC failure", node: "node-1", err: apierror.Internal("injected failure"), wantCode: codes.Internal},
		{name: "concurrent update", node: "node-1", err: apierror.Unavailable("offline"), conflict: true, wantCode: codes.Aborted},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			t.Cleanup(cleanup)
			w := newTestActorWorkflow(t, st, "ns", "tmpl1")
			wire, atelet := newWireCaptureWorkflow(t, st)
			w.dialer = wire.dialer
			ref := resources.ActorRef{Atespace: "team-a", Name: "id1"}
			seedWorkflowActor(t, ctx, st, ref, "ns", "tmpl1", ateapi.ActorState_ACTOR_STATE_CRASHED)
			actor, err := st.GetActor(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			actor = mustUpdateActorStatus(t, ctx, st, actor, func(s *ateapi.ActorStatus) {
				s.AssignedNode = tt.node
				s.LastAssignedGeneration = 2
				snap := newPausedSnapshot(t, 2, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "uploaded", tt.node)
				snap.DurableSnapshot.Status = ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
				if tt.pending {
					snap.DurableSnapshot.Status = ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING
				}
				s.Snapshots = []*ateapi.Snapshot{
					newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, snap.GetFidelity(), "", "older", "gs://snapshots/atespaces/team-a/actors/older/snapshots/snapshot", ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
					snap,
				}
				if tt.assigned {
					s.State = ateapi.ActorState_ACTOR_STATE_RUNNING
					s.WorkerAssignment = wireTestAssignment()
				}
				if tt.crashed {
					s.State = ateapi.ActorState_ACTOR_STATE_CRASHED
					s.Crash = newActorCrash("ResumeActor", "interrupted execution")
				}
			})
			want := proto.CloneOf(actor.GetStatus().GetSnapshots()[1])
			atelet.terminateErr = tt.err
			if tt.conflict {
				atelet.onTerminate = func() {
					cur, err := st.GetActor(ctx, ref)
					if err != nil {
						t.Errorf("GetActor: %v", err)
						return
					}
					if _, err := st.UpdateActor(ctx, ref, store.PreconditionFrom(cur), func(a *ateapi.Actor) error {
						a.WorkerSelector = &ateapi.Selector{MatchLabels: map[string]string{"changed": "true"}}
						return nil
					}); err != nil {
						t.Errorf("UpdateActor: %v", err)
					}
				}
			}
			reverted, err := w.RevertActor(ctx, ref)
			if got := apierror.Code(err); got != tt.wantCode {
				t.Fatalf("RevertActor = %v, want %v", err, tt.wantCode)
			}
			if tt.wantCode != codes.OK {
				stored, err := st.GetActor(ctx, ref)
				if err != nil {
					t.Fatal(err)
				}
				got := stored.GetStatus()
				if got.GetState() != ateapi.ActorState_ACTOR_STATE_REVERTING || got.GetAssignedNode() != tt.node {
					t.Fatalf("status = %v, want REVERTING on %s", got, tt.node)
				}
				if !proto.Equal(got.GetSnapshots()[1], want) {
					t.Fatalf("snapshot changed after failed revert: %v", got.GetSnapshots())
				}
				if !tt.conflict {
					return
				}
				atelet.mu.Lock()
				atelet.onTerminate = nil
				atelet.mu.Unlock()
				reverted, err = w.RevertActor(ctx, ref)
				if err != nil {
					t.Fatalf("retry after conflict: %v", err)
				}
			}
			got := reverted.GetStatus()
			if got.GetState() != ateapi.ActorState_ACTOR_STATE_SUSPENDED || got.GetAssignedNode() != "" || got.GetWorkerAssignment() != nil {
				t.Fatalf("reverted status = %v, want unbound SUSPENDED", got)
			}
			if len(got.GetSnapshots()) != 1 {
				t.Fatalf("snapshots = %v, want only the latest durable snapshot", got.GetSnapshots())
			}
			want.Locality = ""
			if !proto.Equal(got.GetSnapshots()[0], want) {
				t.Errorf("retained snapshot = %v, want %v", got.GetSnapshots()[0], want)
			}
		})
	}
}

// A completed upload does not turn its local copy into disposable execution state.
func TestRevertActor_LocalSnapshotAlreadyUploaded(t *testing.T) {
	memory := ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	completed := ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	w := newTestActorWorkflow(t, st, "ns", "tmpl1")
	wire, atelet := newWireCaptureWorkflow(t, st)
	w.dialer = wire.dialer
	objects := objectstoretest.New()
	w.objectStorePlugin = objectstoreplugintest.ControlClient(objects)

	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
	seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", ateapi.ActorState_ACTOR_STATE_CRASHED)
	actor, err := st.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("get seeded actor: %v", err)
	}
	// newTestActorWorkflow's template stores snapshots under gs://snapshots.
	tmpl := &ateapi.ActorTemplate{SnapshotConfig: &ateapi.SnapshotConfig{StorageLocation: "gs://snapshots"}}
	old := mustActorSnapshotURI(t, tmpl, actor, "old")
	uploaded := mustActorSnapshotURI(t, tmpl, actor, "pause-snap")
	objects.PutSnapshot(t, old, "manifest.json")
	objects.PutSnapshot(t, uploaded, "manifest.json")
	atelet.terminateState = &ateletpb.ActorSnapshotState{LatestLocal: &ateletpb.SnapshotReport{Snapshot: &ateletpb.Snapshot{
		SnapshotUuid: "pause-snap", Generation: 2, Fidelity: fidelityToAtelet(memory),
		Object: &ateletpb.ObjectStorage{SnapshotUri: uploaded.String()},
	}}}
	mustUpdateActorStatus(t, ctx, st, actor, func(s *ateapi.ActorStatus) {
		paused := newPausedSnapshot(t, 2, memory, "", "pause-snap", "node-1")
		paused.DurableSnapshot = &ateapi.SnapshotStorage{Status: completed, Object: &ateapi.ObjectSnapshot{SnapshotUri: uploaded.String()}}
		s.AssignedNode = "node-1"
		s.LastAssignedGeneration = 2
		s.Snapshots = []*ateapi.Snapshot{
			newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, memory, "", old.Name(), old.String(), completed),
			paused,
		}
	})

	reverted, err := w.RevertActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("RevertActor: %v", err)
	}
	want := []snapshotOutcome{{generation: 2, fidelity: memory, durable: completed, locality: "node-1"}}
	if diff := cmp.Diff(want, describeSnapshots(reverted.GetStatus()), cmp.AllowUnexported(snapshotOutcome{})); diff != "" {
		t.Errorf("snapshots mismatch (-want +got):\n%s", diff)
	}
	if len(objects.Snapshot(t, old)) != 0 {
		t.Error("the durable snapshot the upload replaced was not released")
	}
	if len(objects.Snapshot(t, uploaded)) == 0 {
		t.Error("the upload the revert returned the actor to was released")
	}
	if req := atelet.terminateRequest(); !req.GetPreserveSnapshots() || req.GetWorkerPodUid() != "" {
		t.Errorf("TerminateRequest = %v, want snapshot preservation without a worker", req)
	}
}

// TestRevertActor_RejectsSuspended covers both ways an actor can already be
// SUSPENDED when a revert arrives: it never ran, or a previous revert already
// finished. Both are FailedPrecondition — revert is not a no-op success on its
// own destination, for the same reason a second DeleteActor is NotFound.
func TestRevertActor_RejectsSuspended(t *testing.T) {
	for _, tc := range []struct {
		name     string
		locality string
		status   ateapi.SnapshotStorageStatus
	}{
		{name: "local pending", locality: "node-1", status: ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING},
		{name: "local uploaded", locality: "node-1", status: ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED},
		{name: "detached uploaded", status: ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			st, cleanup := storetest.SetupTestStore(t)
			t.Cleanup(cleanup)
			w := newTestActorWorkflow(t, st, "ns", "tmpl1")
			wire, _ := newWireCaptureWorkflow(t, st)
			w.dialer = wire.dialer
			ref := resources.ActorRef{Atespace: "team-a", Name: "id1"}
			seedWorkflowActor(t, ctx, st, ref, "ns", "tmpl1", ateapi.ActorState_ACTOR_STATE_SUSPENDED, withPausedShape(t, "node-1"))
			actor, err := st.GetActor(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			actor = mustUpdateActorStatus(t, ctx, st, actor, func(s *ateapi.ActorStatus) {
				s.AssignedNode = tc.locality
				s.Snapshots[0].Locality = tc.locality
				s.Snapshots[0].DurableSnapshot.Status = tc.status
			})
			if _, err := w.RevertActor(ctx, ref); apierror.Code(err) != codes.FailedPrecondition {
				t.Fatalf("RevertActor = %v, want FailedPrecondition", err)
			}
			stored, err := st.GetActor(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(stored, actor) {
				t.Errorf("rejected revert changed actor: got %v, want %v", stored, actor)
			}
		})
	}
	t.Run("actor was already suspended", func(t *testing.T) {
		ctx := context.Background()
		st, cleanup := storetest.SetupTestStore(t)
		defer cleanup()
		w := newTestActorWorkflow(t, st, "ns", "tmpl1")

		actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
		seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", ateapi.ActorState_ACTOR_STATE_SUSPENDED)

		if _, err := w.RevertActor(ctx, actorRef); apierror.Code(err) != codes.FailedPrecondition {
			t.Fatalf("RevertActor = %v, want FailedPrecondition", err)
		}
	})

	t.Run("second revert of an already-reverted actor", func(t *testing.T) {
		ctx := context.Background()
		st, cleanup := storetest.SetupTestStore(t)
		defer cleanup()
		w := newTestActorWorkflow(t, st, "ns", "tmpl1")

		actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
		seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", ateapi.ActorState_ACTOR_STATE_CRASHED)

		if _, err := w.RevertActor(ctx, actorRef); err != nil {
			t.Fatalf("first RevertActor: %v", err)
		}
		if _, err := w.RevertActor(ctx, actorRef); apierror.Code(err) != codes.FailedPrecondition {
			t.Fatalf("second RevertActor = %v, want FailedPrecondition", err)
		}
	})
}

// TestRevertActor_MissingTemplate covers the wreck revert most needs to
// recover: an actor whose template was deleted. Suspend refuses this actor
// outright, so revert must not, or the only way out would be deletion.
func TestRevertActor_MissingTemplate(t *testing.T) {
	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	w := newTestActorWorkflow(t, st, "ns", "tmpl1")

	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
	seedWorkflowActor(t, ctx, st, actorRef, "ns", "missing-tmpl", ateapi.ActorState_ACTOR_STATE_CRASHED)

	reverted, err := w.RevertActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("RevertActor: %v", err)
	}
	if got := reverted.GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("state = %v, want SUSPENDED", got)
	}
}

// TestRevertActor_NoSnapshotToRevertTo covers "revert to birth": an actor that
// crashed before it ever suspended holds no external snapshot, so there is
// nothing to return it to. That is accepted, not an error — the actor resumes
// from its template as a fresh one.
func TestRevertActor_NoSnapshotToRevertTo(t *testing.T) {
	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	w := newTestActorWorkflow(t, st, "ns", "tmpl1")

	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
	seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", ateapi.ActorState_ACTOR_STATE_CRASHED, func(a *ateapi.Actor) {
		a.Status.Crash = newActorCrash("resume", crashMessageWorkerGone)
	})

	reverted, err := w.RevertActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("RevertActor: %v", err)
	}
	if got := reverted.GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("state = %v, want SUSPENDED", got)
	}
	if got := reverted.GetStatus().GetCrash(); got != nil {
		t.Errorf("Crash = %v, want cleared by the revert", got)
	}
	gotSnap := findLatestDurableSnapshot(reverted.GetStatus(), ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	if got := gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri(); got != "" {
		t.Errorf("external snapshot = %q, want none", got)
	}
}

// TestEnsureInProgressSnapshotDiscarded covers what a revert deletes from
// object storage and, more importantly, what it must not. Delete drops the
// actor's whole prefix; revert cannot, because the snapshot it is returning the
// actor to lives under that same prefix.
func TestEnsureInProgressSnapshotDiscarded(t *testing.T) {
	const inFlightSnapshotName = "2026-01-01t00-00-00z-abandoned"
	const earlierPauseSnapshotName = "2025-12-31t00-00-00z-earlier-pause"

	tests := []struct {
		name string
		// tagOwnedSnapshot makes the actor's external snapshot a tag's rather
		// than one it took itself, as an actor created from a tag starts out.
		tagOwnedSnapshot bool
		// inFlight names a snapshot the interrupted suspend was writing.
		inFlight string
		// foreignInFlight records the in-progress snapshot under another
		// owner's prefix, which no suspend can produce.
		foreignInFlight bool
		// earlierPause records an older PENDING snapshot: an async suspend whose
		// upload atelet never reported before the actor resumed.
		earlierPause              bool
		localEarlierPause         bool
		wantInFlightDiscarded     bool
		wantEarlierPauseDiscarded bool
		wantErr                   bool
	}{
		{
			name:                  "discards the snapshot an interrupted suspend was writing",
			inFlight:              inFlightSnapshotName,
			wantInFlightDiscarded: true,
		},
		{
			name:     "leaves the actor's own external snapshot in place",
			inFlight: "",
		},
		{
			name:                  "leaves a snapshot borrowed from a tag in place",
			tagOwnedSnapshot:      true,
			inFlight:              inFlightSnapshotName,
			wantInFlightDiscarded: true,
		},
		{
			name:            "refuses an in-progress snapshot the actor does not own",
			inFlight:        inFlightSnapshotName,
			foreignInFlight: true,
			wantErr:         true,
		},
		{
			name:                  "retains a committed local snapshot while discarding an incomplete checkpoint",
			inFlight:              inFlightSnapshotName,
			earlierPause:          true,
			localEarlierPause:     true,
			wantInFlightDiscarded: true,
		},
		{
			name:                      "discards an earlier pause's unrecorded upload along with the in-flight snapshot",
			inFlight:                  inFlightSnapshotName,
			earlierPause:              true,
			wantInFlightDiscarded:     true,
			wantEarlierPauseDiscarded: true,
		},
		{
			name:                      "discards an earlier pause's unrecorded upload with no suspend in flight",
			earlierPause:              true,
			wantEarlierPauseDiscarded: true,
		},
		{
			name:            "discards nothing when any in-progress snapshot is not the actor's",
			inFlight:        inFlightSnapshotName,
			foreignInFlight: true,
			earlierPause:    true,
			wantErr:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
			w, objects := newFinalizeWorkflow(persistence)

			actor := storetest.MustCreateActor(t, ctx, persistence, &ateapi.Actor{
				Metadata:      &ateapi.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
				ActorTemplate: &ateapi.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
				Status:        &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_REVERTING},
			})

			// The actor's prefix is keyed on the UID the store just assigned,
			// so its snapshots can only be placed now.
			current := mustActorSnapshotURI(t, template, actor, "current")
			currentType := ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR
			if tt.tagOwnedSnapshot {
				current = mustTagSnapshotURI(t, template, "team-a", "v1-snapshot")
				currentType = ateapi.SnapshotOwner_SNAPSHOT_OWNER_TAG
			}
			objects.PutSnapshot(t, current, "manifest.json")
			inFlight := mustActorSnapshotURI(t, template, actor, inFlightSnapshotName)
			if tt.foreignInFlight {
				inFlight = mustTagSnapshotURI(t, template, "team-a", "someone-elses-snapshot")
			}
			if tt.inFlight != "" {
				objects.PutSnapshot(t, inFlight, "manifest.json")
			}
			earlierPause := mustActorSnapshotURI(t, template, actor, earlierPauseSnapshotName)
			if tt.earlierPause {
				objects.PutSnapshot(t, earlierPause, "manifest.json")
			}
			actor = mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapi.ActorStatus) {
				s.LastAssignedGeneration = 1
				s.Snapshots = []*ateapi.Snapshot{
					newDurableSnapshot(1, currentType, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", current.Name(), current.String(), ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
				}
				if tt.earlierPause {
					s.LastAssignedGeneration++
					s.Snapshots = append(s.Snapshots, newDurableSnapshot(s.LastAssignedGeneration, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", earlierPause.Name(), earlierPause.String(), ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING))
					if tt.localEarlierPause {
						s.Snapshots[len(s.Snapshots)-1].Locality = "node-1"
						s.AssignedNode = "node-1"
					}
				}
				if tt.inFlight != "" {
					s.LastAssignedGeneration++
					s.Snapshots = append(s.Snapshots, newDurableSnapshot(s.LastAssignedGeneration, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", inFlight.Name(), inFlight.String(), ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING))
				}
			})

			err := w.ensureInProgressSnapshotDiscarded(ctx, actor)
			if tt.wantErr {
				if err == nil {
					t.Fatal("ensureInProgressSnapshotDiscarded = nil, want an error for a snapshot the actor does not own")
				}
				if len(objects.Snapshot(t, inFlight)) == 0 {
					t.Errorf("snapshot %v was discarded, but it belongs to another owner", inFlight)
				}
				if tt.earlierPause && len(objects.Snapshot(t, earlierPause)) == 0 {
					t.Errorf("snapshot %v was discarded, but a corrupted record must discard nothing", earlierPause)
				}
				return
			}
			if err != nil {
				t.Fatalf("ensureInProgressSnapshotDiscarded: %v", err)
			}

			// The whole point: whatever else happens, the snapshot the actor
			// is being returned to survives.
			if len(objects.Snapshot(t, current)) == 0 {
				t.Errorf("external snapshot %v was collected, but revert must preserve it", current)
			}
			if tt.inFlight != "" {
				discarded := len(objects.Snapshot(t, inFlight)) == 0
				if discarded != tt.wantInFlightDiscarded {
					t.Errorf("in-flight snapshot discarded = %v, want %v", discarded, tt.wantInFlightDiscarded)
				}
			}
			if tt.earlierPause {
				discarded := len(objects.Snapshot(t, earlierPause)) == 0
				if discarded != tt.wantEarlierPauseDiscarded {
					t.Errorf("earlier pause snapshot discarded = %v, want %v", discarded, tt.wantEarlierPauseDiscarded)
				}
			}
		})
	}
}

// TestEnsureRevertedFinalized_NoObjectStore covers a workflow built without an
// object store: the discard step skips instead of failing, so the revert still
// finalizes.
func TestEnsureRevertedFinalized_NoObjectStore(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	w := &ActorWorkflow{store: persistence}

	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
		ActorTemplate: &ateapi.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"},
		Status: &ateapi.ActorStatus{
			State:                  ateapi.ActorState_ACTOR_STATE_REVERTING,
			AssignedNode:           "node-1",
			LastAssignedGeneration: 1,
			Snapshots: []*ateapi.Snapshot{
				newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "abandoned", someActorSnapshotURI(t, testStorageLocation, "team-a", "abandoned"), ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING),
			},
		},
	})

	if err := w.ensureInProgressSnapshotDiscarded(ctx, actor); err != nil {
		t.Fatalf("ensureInProgressSnapshotDiscarded: %v", err)
	}

	actorRef := resources.ActorRefFromActor(actor)
	finalized, err := w.ensureRevertedFinalized(ctx, actorRef)
	if err != nil {
		t.Fatalf("ensureRevertedFinalized: %v", err)
	}
	if got := finalized.GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("state = %v, want SUSPENDED", got)
	}
	if got := finalized.GetStatus().GetAssignedNode(); got != "" {
		t.Errorf("assigned node = %q, want empty", got)
	}
	if got := findLatestDurableSnapshot(finalized.GetStatus(), ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING); got != nil {
		t.Errorf("in-progress durable snapshot = %v, want nil", got)
	}
}
