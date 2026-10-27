// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"log/slog"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
)

const (
	reportAtespace = "team-a"
	reportActorUID = "5f2c1a90-7b34-4e6d-8a11-0c3e9d5b7f42"
	reportNode     = "node-a"
	reportOther    = "node-b"
)

func snapName(gen int32) string { return fmt.Sprintf("snap-%d", gen) }

// ownedURI returns the upload URI of actorUID's snapshot of generation gen.
func ownedURI(t *testing.T, actorUID string, gen int32) string {
	t.Helper()
	uri, err := resources.NewActorSnapshotURI("gs://snapshots", reportAtespace, actorUID, snapName(gen))
	if err != nil {
		t.Fatal(err)
	}
	return uri.String()
}

// withDurable returns a Snapshot held on locality ("" for none) with a durable
// copy in the given storage state.
func withDurable(t *testing.T, gen int32, locality string, storage ateapi.SnapshotStorageState) *ateapi.Snapshot {
	t.Helper()
	snap := &ateapi.Snapshot{Uuid: snapName(gen), Generation: gen, Locality: locality, Fidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY}
	snap.DurableSnapshot = &ateapi.SnapshotStorage{
		State:  storage,
		Object: &ateapi.ObjectSnapshot{SnapshotUri: ownedURI(t, reportActorUID, gen)},
	}
	return snap
}

func reported(t *testing.T, gen int32) *ateapi.SnapshotReport {
	t.Helper()
	return reportedAt(t, gen, ownedURI(t, reportActorUID, gen))
}

func reportedAt(t *testing.T, gen int32, uri string) *ateapi.SnapshotReport {
	t.Helper()
	return &ateapi.SnapshotReport{
		Uuid:       snapName(gen),
		Generation: gen,
		Fidelity:   ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		Object:     &ateapi.ObjectSnapshot{SnapshotUri: uri},
	}
}

func TestApplyActorSnapshotState(t *testing.T) {
	const (
		pending   = ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING
		completed = ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED
		suspended = ateapi.ActorState_ACTOR_STATE_SUSPENDED
		running   = ateapi.ActorState_ACTOR_STATE_RUNNING
	)
	tests := []struct {
		name     string
		status   *ateapi.ActorStatus
		state    *ateapi.ActorSnapshotState
		want     *ateapi.ActorStatus
		wantCode codes.Code
	}{{
		name: "upload marks the pending snapshot COMPLETED and keeps it local",
		status: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, reportNode, pending),
		}},
		state: &ateapi.ActorSnapshotState{LatestLocal: reported(t, 2), LatestDurable: reported(t, 2)},
		want: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, reportNode, completed),
		}},
	}, {
		name: "upload records the fidelity the manifest reports",
		status: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, reportNode, pending),
		}},
		state: &ateapi.ActorSnapshotState{LatestLocal: reported(t, 2), LatestDurable: func() *ateapi.SnapshotReport {
			r := reported(t, 2)
			r.Fidelity = ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
			return r
		}()},
		want: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapi.Snapshot{
			func() *ateapi.Snapshot {
				snap := withDurable(t, 2, reportNode, completed)
				snap.Fidelity = ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
				return snap
			}(),
		}},
	}, {
		name: "an unset fidelity is refused",
		status: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, reportNode, pending),
		}},
		state: &ateapi.ActorSnapshotState{LatestLocal: reported(t, 2), LatestDurable: func() *ateapi.SnapshotReport {
			r := reported(t, 2)
			r.Fidelity = ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED
			return r
		}()},
		wantCode: codes.InvalidArgument,
	}, {
		name: "a snapshot already COMPLETED changes nothing",
		status: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, reportNode, completed),
		}},
		state: &ateapi.ActorSnapshotState{LatestLocal: reported(t, 2), LatestDurable: reported(t, 2)},
		want: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, reportNode, completed),
		}},
	}, {
		name: "a generation below the latest COMPLETED one changes nothing",
		status: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 1, reportNode, pending),
			withDurable(t, 2, reportNode, completed),
		}},
		state: &ateapi.ActorSnapshotState{LatestLocal: reported(t, 2), LatestDurable: func() *ateapi.SnapshotReport {
			r := reported(t, 1)
			r.Fidelity = ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
			return r
		}()},
		want: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 1, reportNode, pending),
			withDurable(t, 2, reportNode, completed),
		}},
	}, {
		name: "a generation status no longer has changes nothing",
		status: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, LastAssignedGeneration: 3, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 3, reportNode, pending),
		}},
		state: &ateapi.ActorSnapshotState{LatestLocal: reported(t, 3), LatestDurable: reported(t, 2)},
		want: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, LastAssignedGeneration: 3, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 3, reportNode, pending),
		}},
	}, {
		name: "a generation newer than any assigned is refused",
		status: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, reportNode, pending),
		}},
		state:    &ateapi.ActorSnapshotState{LatestLocal: reported(t, 2), LatestDurable: reported(t, 5)},
		wantCode: codes.FailedPrecondition,
	}, {
		name: "a uuid other than the recorded one is refused",
		status: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, reportNode, pending),
		}},
		state: &ateapi.ActorSnapshotState{LatestLocal: reported(t, 2), LatestDurable: func() *ateapi.SnapshotReport {
			r := reported(t, 2)
			r.Uuid = "other"
			return r
		}()},
		wantCode: codes.FailedPrecondition,
	}, {
		name: "an older snapshot's upload is recorded and the latest stays pending",
		status: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 1, reportNode, pending),
			withDurable(t, 2, reportNode, pending),
		}},
		state: &ateapi.ActorSnapshotState{LatestLocal: reported(t, 2), LatestDurable: reported(t, 1)},
		want: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 1, reportNode, completed),
			withDurable(t, 2, reportNode, pending),
		}},
	}, {
		name: "a snapshot that records no upload is refused",
		status: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{
			{Uuid: snapName(2), Generation: 2, Locality: reportNode, Fidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY},
		}},
		state:    &ateapi.ActorSnapshotState{LatestLocal: reported(t, 2), LatestDurable: reported(t, 2)},
		wantCode: codes.FailedPrecondition,
	}, {
		name: "an upload URI other than the recorded one is refused",
		status: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, reportNode, pending),
		}},
		state:    &ateapi.ActorSnapshotState{LatestLocal: reported(t, 2), LatestDurable: reportedAt(t, 2, ownedURI(t, reportActorUID, 9))},
		wantCode: codes.InvalidArgument,
	}, {
		name: "latest_local and uploading are not checked",
		status: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, reportNode, pending),
		}},
		state: &ateapi.ActorSnapshotState{LatestLocal: reported(t, 7), Uploading: reported(t, 8)},
		want: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, reportNode, pending),
		}},
	}, {
		name: "an upload in flight with no local snapshot is not a detach",
		status: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, reportNode, pending),
		}},
		state: &ateapi.ActorSnapshotState{Uploading: reported(t, 2)},
		want: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, reportNode, pending),
		}},
	}, {
		name: "detach releases a suspended actor from the node",
		status: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 1, "", completed),
			withDurable(t, 2, reportNode, completed),
		}},
		state: &ateapi.ActorSnapshotState{LatestDurable: reported(t, 2)},
		want: &ateapi.ActorStatus{State: suspended, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 1, "", completed),
			withDurable(t, 2, "", completed),
		}},
	}, {
		name: "a detach can complete the upload it reports",
		status: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, reportNode, pending),
		}},
		state: &ateapi.ActorSnapshotState{LatestDurable: reported(t, 2)},
		want: &ateapi.ActorStatus{State: suspended, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, "", completed),
		}},
	}, {
		name: "a late detach leaves a newer snapshot on the node and keeps the actor placed",
		status: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, reportNode, completed),
			withDurable(t, 3, reportNode, completed),
		}},
		state: &ateapi.ActorSnapshotState{LatestDurable: reported(t, 2)},
		want: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, "", completed),
			withDurable(t, 3, reportNode, completed),
		}},
	}, {
		name: "a late detach leaves a newer snapshot whose upload is pending on the node",
		status: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 1, "", completed),
			withDurable(t, 2, reportNode, pending),
		}},
		state: &ateapi.ActorSnapshotState{LatestDurable: reported(t, 1)},
		want: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 1, "", completed),
			withDurable(t, 2, reportNode, pending),
		}},
	}, {
		name: "a detach of a snapshot that isn't durable is refused",
		status: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 1, reportNode, pending),
		}},
		state:    &ateapi.ActorSnapshotState{LatestDurable: reported(t, 2)},
		wantCode: codes.FailedPrecondition,
	}, {
		name: "a detach from a node that holds no snapshot only releases the actor",
		status: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, "", completed),
		}},
		state: &ateapi.ActorSnapshotState{LatestDurable: reported(t, 2)},
		want: &ateapi.ActorStatus{State: suspended, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, "", completed),
		}},
	}, {
		name: "a running actor's detach keeps it placed on the node",
		status: &ateapi.ActorStatus{State: running, AssignedNode: reportNode, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, reportNode, completed),
		}},
		state: &ateapi.ActorSnapshotState{LatestDurable: reported(t, 2)},
		want: &ateapi.ActorStatus{State: running, AssignedNode: reportNode, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, "", completed),
		}},
	}, {
		name: "a detach from one node leaves another node alone",
		status: &ateapi.ActorStatus{State: suspended, AssignedNode: reportOther, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, reportNode, completed),
			withDurable(t, 3, reportOther, pending),
		}},
		state: &ateapi.ActorSnapshotState{LatestDurable: reported(t, 2)},
		want: &ateapi.ActorStatus{State: suspended, AssignedNode: reportOther, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, "", completed),
			withDurable(t, 3, reportOther, pending),
		}},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := applyActorSnapshotState(context.Background(), tt.status, reportNode, tt.state)
			if tt.wantCode != codes.OK {
				if got := apierror.Code(err); got != tt.wantCode {
					t.Fatalf("applyActorSnapshotState() error = %v, want code %v", err, tt.wantCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("applyActorSnapshotState() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, tt.status, protocmp.Transform()); diff != "" {
				t.Errorf("status mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// A reported durable snapshot older than the latest COMPLETED one, at a
// generation status no longer has, or of a CRASHED actor's crashed suspend, is
// logged at Info, once, with the generation it named. A repeat of the latest,
// and one that is applied, aren't logged.
func TestApplyLatestDurableLogsStaleReports(t *testing.T) {
	const (
		pending   = ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING
		completed = ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED
	)
	tests := []struct {
		name      string
		status    *ateapi.ActorStatus
		gen       int32
		wantMsg   string
		wantAttrs map[string]string
	}{{
		name: "below the latest COMPLETED generation",
		status: &ateapi.ActorStatus{LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 1, reportNode, pending),
			withDurable(t, 2, reportNode, completed),
		}},
		gen:       1,
		wantMsg:   msgDurableOlder,
		wantAttrs: map[string]string{"generation": "1", "uuid": snapName(1), "latest_completed_generation": "2"},
	}, {
		name: "a generation status no longer has",
		status: &ateapi.ActorStatus{LastAssignedGeneration: 3, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 3, reportNode, pending),
		}},
		gen:       2,
		wantMsg:   msgDurableNotRecorded,
		wantAttrs: map[string]string{"generation": "2", "uuid": snapName(2)},
	}, {
		name: "a CRASHED actor's crashed suspend",
		status: &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_CRASHED, LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 1, reportNode, completed),
			withDurable(t, 2, "", pending),
		}},
		gen:       2,
		wantMsg:   msgDurableCrashedSuspend,
		wantAttrs: map[string]string{"generation": "2", "uuid": snapName(2)},
	}, {
		name: "the latest COMPLETED generation itself",
		status: &ateapi.ActorStatus{LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, reportNode, completed),
		}},
		gen: 2,
	}, {
		name: "applied",
		status: &ateapi.ActorStatus{LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, reportNode, pending),
		}},
		gen: 2,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			type logged struct {
				msg   string
				attrs map[string]string
			}
			var got []logged
			prev := slog.Default()
			slog.SetDefault(slog.New(slogHandlerFunc(func(r slog.Record) {
				if r.Message != msgDurableOlder && r.Message != msgDurableNotRecorded && r.Message != msgDurableCrashedSuspend {
					return
				}
				l := logged{msg: r.Message, attrs: map[string]string{}}
				r.Attrs(func(a slog.Attr) bool {
					l.attrs[a.Key] = a.Value.String()
					return true
				})
				got = append(got, l)
			})))
			t.Cleanup(func() { slog.SetDefault(prev) })

			if err := applyLatestDurable(context.Background(), tt.status, reported(t, tt.gen)); err != nil {
				t.Fatalf("applyLatestDurable() error = %v", err)
			}
			var want []logged
			if tt.wantMsg != "" {
				want = []logged{{msg: tt.wantMsg, attrs: tt.wantAttrs}}
			}
			if diff := cmp.Diff(want, got, cmp.AllowUnexported(logged{})); diff != "" {
				t.Errorf("logged records mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// A report is applied to the stored actor under its lease. A refused report,
// and a report that changes nothing, write nothing.
func TestApplyReportedSnapshotState(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	w := NewActorWorkflow(persistence, nil, nil, nil, nil, "", nil, nil, nil)
	tmpl, err := persistence.CreateActorTemplate(ctx, &ateapi.ActorTemplate{
		Metadata:       &ateapi.ResourceMetadata{Atespace: reportAtespace, Name: "tmpl"},
		SnapshotConfig: &ateapi.SnapshotConfig{StorageLocation: testStorageLocation},
	})
	if err != nil {
		t.Fatalf("CreateActorTemplate() failed: %v", err)
	}

	actorRef := resources.ActorRef{Atespace: reportAtespace, Name: "actor-1"}
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: &ateapi.ObjectRef{Atespace: reportAtespace, Name: "tmpl"},
		Status:        &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED},
	})
	uid := actor.GetMetadata().GetUid()
	pending := mustActorSnapshotURI(t, tmpl, actor, "snap-2")
	actor = mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapi.ActorStatus) {
		s.AssignedNode = reportNode
		s.LastAssignedGeneration = 2
		snap := newDurableSnapshot(2, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", pending.Name(), pending.String(), ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING)
		snap.Locality = reportNode
		s.Snapshots = []*ateapi.Snapshot{snap}
	})
	uploaded := reportedAt(t, 2, pending.String())
	state := &ateapi.ActorSnapshotState{LatestLocal: uploaded, LatestDurable: uploaded}

	assertUnwritten := func(t *testing.T) {
		t.Helper()
		got, err := persistence.GetActor(ctx, actorRef)
		if err != nil {
			t.Fatal(err)
		}
		if got.GetMetadata().GetVersion() != actor.GetMetadata().GetVersion() {
			t.Errorf("a refused report wrote the actor: version %d, want %d", got.GetMetadata().GetVersion(), actor.GetMetadata().GetVersion())
		}
	}

	t.Run("refused while another operation holds the actor", func(t *testing.T) {
		lease, err := persistence.AcquireLease(ctx, actorLeaseKey(actorRef))
		if err != nil {
			t.Fatalf("AcquireLease() failed: %v", err)
		}
		defer lease.Close()
		if err := w.ApplyReportedSnapshotState(ctx, actorRef, uid, reportNode, state); apierror.Code(err) != codes.Aborted {
			t.Fatalf("ApplyReportedSnapshotState() = %v, want Aborted", err)
		}
		assertUnwritten(t)
	})
	for _, tc := range []struct {
		name, uid, node string
		ref             resources.ActorRef
	}{
		{"refused for another node", uid, reportOther, actorRef},
		{"refused for a stale incarnation", "1a7e4c83-6d20-4f95-b3c8-9e0a2f6d4b17", reportNode, actorRef},
		{"refused for a missing actor", uid, reportNode, resources.ActorRef{Atespace: reportAtespace, Name: "no-such-actor"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := w.ApplyReportedSnapshotState(ctx, tc.ref, tc.uid, tc.node, state); apierror.Code(err) != codes.NotFound {
				t.Fatalf("ApplyReportedSnapshotState() = %v, want NotFound", err)
			}
			assertUnwritten(t)
		})
	}
	// A transitioning actor gets Aborted.
	for _, st := range []ateapi.ActorState{
		ateapi.ActorState_ACTOR_STATE_SUSPENDING,
		ateapi.ActorState_ACTOR_STATE_RESUMING,
		ateapi.ActorState_ACTOR_STATE_DELETING,
		ateapi.ActorState_ACTOR_STATE_REVERTING,
	} {
		t.Run("refused for a "+st.String()+" actor", func(t *testing.T) {
			actor = mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapi.ActorStatus) { s.State = st })
			defer func() {
				actor = mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapi.ActorStatus) {
					s.State = ateapi.ActorState_ACTOR_STATE_SUSPENDED
				})
			}()
			if err := w.ApplyReportedSnapshotState(ctx, actorRef, uid, reportNode, state); apierror.Code(err) != codes.Aborted {
				t.Fatalf("ApplyReportedSnapshotState() = %v, want Aborted", err)
			}
			assertUnwritten(t)
		})
	}
	t.Run("refused for an upload to the wrong URI", func(t *testing.T) {
		wrong := &ateapi.ActorSnapshotState{LatestLocal: uploaded, LatestDurable: reportedAt(t, 2, ownedURI(t, uid, 9))}
		if err := w.ApplyReportedSnapshotState(ctx, actorRef, uid, reportNode, wrong); apierror.Code(err) != codes.InvalidArgument {
			t.Fatalf("ApplyReportedSnapshotState() = %v, want InvalidArgument", err)
		}
		assertUnwritten(t)
	})

	for _, st := range []ateapi.ActorState{ateapi.ActorState_ACTOR_STATE_SUSPENDED, ateapi.ActorState_ACTOR_STATE_CRASHED} {
		t.Run("applied to a "+st.String()+" actor", func(t *testing.T) {
			actor = mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapi.ActorStatus) {
				s.State = st
				s.Snapshots[0].DurableSnapshot.State = ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING
			})
			if err := w.ApplyReportedSnapshotState(ctx, actorRef, uid, reportNode, state); err != nil {
				t.Fatalf("ApplyReportedSnapshotState() failed: %v", err)
			}
			got, err := persistence.GetActor(ctx, actorRef)
			if err != nil {
				t.Fatal(err)
			}
			if st := got.GetStatus().GetSnapshots()[0].GetDurableSnapshot().GetState(); st != ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED {
				t.Errorf("durable snapshot status = %v, want COMPLETED", st)
			}

			// A report that changes nothing writes nothing.
			actor = got
			if err := w.ApplyReportedSnapshotState(ctx, actorRef, uid, reportNode, state); err != nil {
				t.Fatalf("repeated ApplyReportedSnapshotState() failed: %v", err)
			}
			assertUnwritten(t)
		})
	}

	// A suspend that crashed before it finalized leaves its snapshot on no node.
	older := mustActorSnapshotURI(t, tmpl, actor, "snap-1")
	snapshot := func(gen int32, uri resources.SnapshotURI, locality string, storage ateapi.SnapshotStorageState) *ateapi.Snapshot {
		snap := newDurableSnapshot(gen, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", uri.Name(), uri.String(), storage)
		snap.Locality = locality
		return snap
	}
	for _, tc := range []struct {
		name      string
		snapshots []*ateapi.Snapshot
		state     *ateapi.ActorSnapshotState
		want      []*ateapi.Snapshot
	}{{
		name: "a crashed suspend's upload stays pending",
		snapshots: []*ateapi.Snapshot{
			snapshot(1, older, reportNode, ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED),
			snapshot(2, pending, "", ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING),
		},
		state: state,
		want: []*ateapi.Snapshot{
			snapshot(1, older, reportNode, ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED),
			snapshot(2, pending, "", ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING),
		},
	}, {
		name: "a crashed suspend's detach releases the older snapshot",
		snapshots: []*ateapi.Snapshot{
			snapshot(1, older, reportNode, ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED),
			snapshot(2, pending, "", ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING),
		},
		state: &ateapi.ActorSnapshotState{LatestDurable: uploaded},
		want: []*ateapi.Snapshot{
			snapshot(1, older, "", ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED),
			snapshot(2, pending, "", ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING),
		},
	}, {
		name: "a crashed suspend's detach releases an older snapshot that isn't durable",
		snapshots: []*ateapi.Snapshot{
			snapshot(1, older, reportNode, ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING),
			snapshot(2, pending, "", ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING),
		},
		state: &ateapi.ActorSnapshotState{LatestDurable: uploaded},
		want: []*ateapi.Snapshot{
			snapshot(1, older, "", ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING),
			snapshot(2, pending, "", ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING),
		},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			actor = mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapi.ActorStatus) {
				s.State = ateapi.ActorState_ACTOR_STATE_CRASHED
				s.Snapshots = tc.snapshots
			})
			if err := w.ApplyReportedSnapshotState(ctx, actorRef, uid, reportNode, tc.state); err != nil {
				t.Fatalf("ApplyReportedSnapshotState() failed: %v", err)
			}
			got, err := persistence.GetActor(ctx, actorRef)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.want, got.GetStatus().GetSnapshots(), protocmp.Transform()); diff != "" {
				t.Errorf("snapshots mismatch (-want +got):\n%s", diff)
			}
			actor = got
		})
	}
}

func TestActorOnNode(t *testing.T) {
	actor := func(assigned string, localities ...string) *ateapi.Actor {
		status := &ateapi.ActorStatus{AssignedNode: assigned}
		for i, l := range localities {
			status.Snapshots = append(status.Snapshots, withDurable(t, int32(i+1), l, ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING))
		}
		return &ateapi.Actor{Status: status}
	}
	for _, tc := range []struct {
		name  string
		actor *ateapi.Actor
		want  bool
	}{
		{"placed on the node", actor(reportNode), true},
		{"holds only a snapshot on the node", actor(reportOther, "", reportNode), false},
		{"neither", actor(reportOther, reportOther), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := actorOnNode(tc.actor, reportNode); got != tc.want {
				t.Errorf("actorOnNode() = %v, want %v", got, tc.want)
			}
		})
	}
}

// ateletReported is reported(t, gen) as atelet returns it.
func ateletReported(t *testing.T, gen int32) *ateletpb.SnapshotReport {
	t.Helper()
	return &ateletpb.SnapshotReport{Snapshot: &ateletpb.Snapshot{
		SnapshotUuid: snapName(gen),
		Generation:   gen,
		Fidelity:     ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		Object:       &ateletpb.ObjectStorage{SnapshotUri: ownedURI(t, reportActorUID, gen)},
	}}
}

func TestApplyAteletResponseState(t *testing.T) {
	const (
		pending    = ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING
		completed  = ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED
		suspended  = ateapi.ActorState_ACTOR_STATE_SUSPENDED
		suspending = ateapi.ActorState_ACTOR_STATE_SUSPENDING

		otherActorUID = "1a7e4c83-6d20-4f95-b3c8-9e0a2f6d4b17"
	)
	// previousPause is a suspending actor's status, holding the snapshot of the
	// async suspend before, whose upload is pending.
	previousPause := func() *ateapi.ActorStatus {
		return &ateapi.ActorStatus{State: suspending, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 1, reportNode, pending),
			withDurable(t, 2, "", pending),
		}}
	}
	elsewhere, err := resources.NewActorSnapshotURI("gs://elsewhere", reportAtespace, reportActorUID, snapName(1))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name     string
		status   *ateapi.ActorStatus
		state    *ateletpb.ActorSnapshotState
		want     *ateapi.ActorStatus
		wantCode codes.Code
	}{{
		name: "an immediate checkpoint marks its snapshot COMPLETED",
		status: &ateapi.ActorStatus{State: suspending, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, "", pending),
		}},
		state: &ateletpb.ActorSnapshotState{LatestDurable: ateletReported(t, 2)},
		want: &ateapi.ActorStatus{State: suspending, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, "", completed),
		}},
	}, {
		name: "an immediate checkpoint records the fidelity it reports",
		status: &ateapi.ActorStatus{State: suspending, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, "", pending),
		}},
		state: &ateletpb.ActorSnapshotState{LatestDurable: func() *ateletpb.SnapshotReport {
			r := ateletReported(t, 2)
			r.Snapshot.Fidelity = ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
			return r
		}()},
		want: &ateapi.ActorStatus{State: suspending, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{
			func() *ateapi.Snapshot {
				snap := withDurable(t, 2, "", completed)
				snap.Fidelity = ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
				return snap
			}(),
		}},
	}, {
		name: "a deferred checkpoint changes nothing",
		status: &ateapi.ActorStatus{State: suspending, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, "", pending),
		}},
		state: &ateletpb.ActorSnapshotState{LatestLocal: ateletReported(t, 2)},
		want: &ateapi.ActorStatus{State: suspending, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, "", pending),
		}},
	}, {
		name:   "a deferred checkpoint records the upload of the previous pause's snapshot",
		status: previousPause(),
		state:  &ateletpb.ActorSnapshotState{LatestLocal: ateletReported(t, 2), LatestDurable: ateletReported(t, 1)},
		want: &ateapi.ActorStatus{State: suspending, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 1, reportNode, completed),
			withDurable(t, 2, "", pending),
		}},
	}, {
		name:   "the previous pause's snapshot uploaded under another location is refused",
		status: previousPause(),
		state: &ateletpb.ActorSnapshotState{LatestLocal: ateletReported(t, 2), LatestDurable: func() *ateletpb.SnapshotReport {
			r := ateletReported(t, 1)
			r.Snapshot.Object.SnapshotUri = elsewhere.String()
			return r
		}()},
		wantCode: codes.InvalidArgument,
	}, {
		name:   "the previous pause's snapshot uploaded under another actor's prefix is refused",
		status: previousPause(),
		state: &ateletpb.ActorSnapshotState{LatestLocal: ateletReported(t, 2), LatestDurable: func() *ateletpb.SnapshotReport {
			r := ateletReported(t, 1)
			r.Snapshot.Object.SnapshotUri = ownedURI(t, otherActorUID, 1)
			return r
		}()},
		wantCode: codes.InvalidArgument,
	}, {
		name: "no local snapshot is not a detach",
		status: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, reportNode, completed),
		}},
		state: &ateletpb.ActorSnapshotState{LatestDurable: ateletReported(t, 2)},
		want: &ateapi.ActorStatus{State: suspended, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, reportNode, completed),
		}},
	}, {
		name: "an upload URI other than the recorded one is refused",
		status: &ateapi.ActorStatus{State: suspending, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, "", pending),
		}},
		state: &ateletpb.ActorSnapshotState{LatestDurable: func() *ateletpb.SnapshotReport {
			r := ateletReported(t, 2)
			r.Snapshot.Object.SnapshotUri = ownedURI(t, reportActorUID, 9)
			return r
		}()},
		wantCode: codes.InvalidArgument,
	}, {
		name: "an unset state changes nothing",
		status: &ateapi.ActorStatus{State: suspending, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, "", pending),
		}},
		want: &ateapi.ActorStatus{State: suspending, AssignedNode: reportNode, LastAssignedGeneration: 2, Snapshots: []*ateapi.Snapshot{
			withDurable(t, 2, "", pending),
		}},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := applyAteletResponseState(context.Background(), tt.status, tt.state)
			if tt.wantCode != codes.OK {
				if got := apierror.Code(err); got != tt.wantCode {
					t.Fatalf("applyAteletResponseState() error = %v, want code %v", err, tt.wantCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("applyAteletResponseState() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, tt.status, protocmp.Transform()); diff != "" {
				t.Errorf("status mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
