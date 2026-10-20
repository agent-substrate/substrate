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

	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestResumeActor_NodeGone(t *testing.T) {
	for _, completed := range []bool{true, false} {
		name := "latest pending, older durable"
		if completed {
			name = "latest durable"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			st := newTestPersistence(t)
			actor := seedGoneNodeActor(t, ctx, st, completed)
			ref := resources.ActorRef{Atespace: "team-a", Name: "id1"}
			worker := seedRuntimeWorker(t, ctx, st, "pod-2", "node-2", testSandboxRuntime())
			w, atelet := newDetachTestWorkflow(t, ctx, st)
			addAteletOnNode(t, w, "node-2")
			w.dialer.getNode = func(context.Context, string) error {
				return apierrors.NewNotFound(schema.GroupResource{Resource: "nodes"}, "gone-node")
			}

			got, resumed, err := w.ResumeActor(ctx, ref)
			wantCode := codes.OK
			wantState := ateapipb.ActorState_ACTOR_STATE_RUNNING
			wantNode := "node-2"
			if !completed {
				wantCode = codes.FailedPrecondition
				wantState = ateapipb.ActorState_ACTOR_STATE_CRASHED
				wantNode = ""
			}
			if apierror.Code(err) != wantCode {
				t.Errorf("ResumeActor error = %v, want %v", err, wantCode)
			}
			if resumed != completed {
				t.Errorf("resumed = %v, want %v", resumed, completed)
			}
			after, getErr := st.GetActor(ctx, ref)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if after.GetStatus().GetState() != wantState || after.GetStatus().GetAssignedNode() != wantNode {
				t.Errorf("state/node = %v/%q, want %v/%q", after.GetStatus().GetState(), after.GetStatus().GetAssignedNode(), wantState, wantNode)
			}
			expected := proto.CloneOf(actor.GetStatus())
			for _, snap := range expected.Snapshots {
				snap.Locality = ""
			}
			actual := after.GetStatus()
			if actual.GetLastAssignedGeneration() != 10 || len(actual.GetSnapshots()) != len(expected.Snapshots) {
				t.Errorf("snapshot history changed: %v", actual)
			} else {
				for i, snap := range expected.Snapshots {
					if !proto.Equal(actual.Snapshots[i], snap) {
						t.Errorf("snapshot %d = %v, want %v", i, actual.Snapshots[i], snap)
					}
				}
			}
			restore, run := atelet.requests()
			if completed {
				if got == nil || restore == nil || run != nil {
					t.Fatalf("result/RPCs = %v/%v/%v, want actor and Restore only", got, restore, run)
				}
				if restore.GetSnapshot().GetGeneration() != 10 || restore.GetSnapshot().GetSnapshotUuid() != "snap-10" ||
					restore.GetWorkerPodUid() != worker.GetWorkerPodUid() || restore.GetSnapshot().GetObject().GetSnapshotUri() != expected.Snapshots[1].GetDurableSnapshot().GetObject().GetSnapshotUri() {
					t.Errorf("wrong restore source or worker: %v", restore)
				}
			} else {
				if restore != nil || run != nil {
					t.Errorf("unrecoverable actor sent Restore/Run: %v/%v", restore, run)
				}
				if _, resumed, retryErr := w.ResumeActor(ctx, ref); apierror.Code(retryErr) != codes.FailedPrecondition || resumed {
					t.Errorf("resume after crash = %v/%v, want FailedPrecondition", resumed, retryErr)
				}
				retried, retryErr := st.GetActor(ctx, ref)
				if retryErr != nil || !proto.Equal(retried, after) {
					t.Errorf("retry changed crashed actor: %v", retryErr)
				}
				message := actual.GetCrash().GetMessage()
				if !strings.Contains(message, "gone-node") || !strings.Contains(message, "10") {
					t.Errorf("crash does not identify lost node/generation: %q", message)
				}
			}
			hosts, hostsErr := workerHostsActor(ctx, st, worker.GetMetadata().GetName(), actor.GetMetadata().GetUid())
			if hostsErr != nil || hosts != completed {
				t.Errorf("worker hosts actor = %v/%v, want %v", hosts, hostsErr, completed)
			}
			if atelet.detachRequest() != nil {
				t.Error("called DetachActor on another node")
			}
		})
	}
}

func seedGoneNodeActor(t *testing.T, ctx context.Context, st store.Interface, completed bool) *ateapipb.Actor {
	t.Helper()
	if _, err := st.CreateActorTemplate(ctx, &ateapipb.ActorTemplate{
		Metadata:       &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "tmpl1"},
		SnapshotConfig: &ateapipb.SnapshotConfig{StorageLocation: testStorageLocation},
	}); err != nil {
		t.Fatal(err)
	}
	old := newPausedSnapshot(t, 9, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "snap-9", "")
	old.DurableSnapshot.Status = ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
	latest := newPausedSnapshot(t, 10, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "snap-10", "gone-node")
	if completed {
		latest.DurableSnapshot.Status = ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
	}
	ref := resources.ActorRef{Atespace: "team-a", Name: "id1"}
	seedWorkflowActor(t, ctx, st, ref, "team-a", "tmpl1", ateapipb.ActorState_ACTOR_STATE_SUSPENDED, func(a *ateapipb.Actor) {
		a.Status.AssignedNode = "gone-node"
		a.Status.LastAssignedGeneration = 10
		a.Status.Snapshots = []*ateapipb.Snapshot{old, latest}
	})
	actor, err := st.GetActor(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	return actor
}

func TestRecordGoneNode(t *testing.T) {
	tests := []struct {
		name   string
		change func(*ateapipb.Actor)
		uid    string
		code   codes.Code
		crash  bool
	}{
		{name: "latest durable"},
		{name: "latest pending", change: func(a *ateapipb.Actor) {
			a.Status.Snapshots[1].DurableSnapshot.Status = ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING
		}, code: codes.FailedPrecondition, crash: true},
		{name: "no completed snapshot", change: func(a *ateapipb.Actor) {
			a.Status.Snapshots = a.Status.Snapshots[1:]
			a.Status.Snapshots[0].DurableSnapshot.Status = ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING
		}, code: codes.FailedPrecondition, crash: true},
		{name: "missing latest record", change: func(a *ateapipb.Actor) { a.Status.Snapshots = a.Status.Snapshots[:1] }, code: codes.FailedPrecondition, crash: true},
		{name: "different incarnation", uid: "old-incarnation", code: codes.Aborted},
		{name: "different node", change: func(a *ateapipb.Actor) { a.Status.AssignedNode = "other-node" }, code: codes.Aborted},
		{name: "running", change: func(a *ateapipb.Actor) { a.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING }, code: codes.Aborted},
		{name: "worker assigned", change: func(a *ateapipb.Actor) {
			a.Status.WorkerAssignment = &ateapipb.WorkerAssignment{Worker: &ateapipb.ObjectRef{Name: "pod-2"}}
		}, code: codes.Aborted},
		{name: "mixed localities", change: func(a *ateapipb.Actor) { a.Status.Snapshots[0].Locality = "other-node" }, code: codes.FailedPrecondition},
		{name: "future generation", change: func(a *ateapipb.Actor) { a.Status.Snapshots[1].Generation = 11 }, code: codes.FailedPrecondition},
		{name: "crash recorded", change: func(a *ateapipb.Actor) { a.Status.Crash = &ateapipb.ActorCrash{} }, code: codes.Aborted},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st := newTestPersistence(t)
			actor := seedGoneNodeActor(t, ctx, st, true)
			ref := resources.ActorRef{Atespace: "team-a", Name: "id1"}
			if tt.change != nil {
				var err error
				actor, err = st.UpdateActor(ctx, ref, store.PreconditionFrom(actor), func(a *ateapipb.Actor) error { tt.change(a); return nil })
				if err != nil {
					t.Fatal(err)
				}
			}
			w, _ := newDetachTestWorkflow(t, ctx, st)
			previousCounter := actorCrashesCounter
			t.Cleanup(func() { actorCrashesCounter = previousCounter })
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
			if err := RegisterActorCrashes(provider.Meter("test")); err != nil {
				t.Fatal(err)
			}
			records := crashRecords(t)
			uid := actor.GetMetadata().GetUid()
			if tt.uid != "" {
				uid = tt.uid
			}
			got, err := w.recordGoneNode(ctx, ref, uid, "gone-node")
			if apierror.Code(err) != tt.code {
				t.Fatalf("recordGoneNode = %v, want %v", err, tt.code)
			}
			after, err := st.GetActor(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			if tt.crash {
				sum := mustMetric(t, reader, actorCrashesMetric).Data.(metricdata.Sum[int64])
				var count int64
				for _, dp := range sum.DataPoints {
					count += dp.Value
				}
				if count != 1 || len(*records) != 1 {
					t.Errorf("crash counter/records = %d/%d, want 1/1", count, len(*records))
				}
			} else {
				assertNoCrashMetricDatapoint(t, reader)
				if len(*records) != 0 {
					t.Error("logged a crash without committing one")
				}
			}
			if tt.code != codes.OK && !tt.crash {
				if !proto.Equal(after, actor) {
					t.Error("rejected recovery changed the actor")
				}
				return
			}
			want := proto.CloneOf(actor.GetStatus())
			want.AssignedNode = ""
			for _, snap := range want.Snapshots {
				if snap.Locality == "gone-node" {
					snap.Locality = ""
				}
			}
			if tt.crash {
				want.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
				if after.GetStatus().GetCrash().GetCrashTime() == nil {
					t.Error("missing crash record")
				}
				want.Crash = after.GetStatus().GetCrash()
			} else if !proto.Equal(got, after) {
				t.Error("recovery did not return the stored actor")
			}
			if !proto.Equal(after.GetStatus(), want) {
				t.Errorf("status = %v, want %v", after.GetStatus(), want)
			}
		})
	}
}

func TestDetachFromFullNode_NodeGone(t *testing.T) {
	for _, kind := range []string{"gone", "exists", "lookup error", "no getter", "atelet returns", "no capacity", "upload completes during lookup"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			st := newTestPersistence(t)
			actor := seedGoneNodeActor(t, ctx, st, false)
			if kind != "no capacity" {
				seedRuntimeWorker(t, ctx, st, "pod-2", "node-2", testSandboxRuntime())
			}
			w, atelet := newDetachTestWorkflow(t, ctx, st)
			lookups := 0
			if kind != "no getter" {
				w.dialer.getNode = func(context.Context, string) error {
					lookups++
					switch kind {
					case "exists":
						return nil
					case "lookup error":
						return errors.New("node lookup failed")
					case "atelet returns":
						addAteletOnNode(t, w, "gone-node")
					case "upload completes during lookup":
						_, err := st.UpdateActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "id1"}, store.PreconditionFrom(actor), func(a *ateapipb.Actor) error {
							a.Status.Snapshots[1].DurableSnapshot.Status = ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
							return nil
						})
						if err != nil {
							t.Fatal(err)
						}
					}
					return apierrors.NewNotFound(schema.GroupResource{Resource: "nodes"}, "gone-node")
				}
			}
			ref := resources.ActorRef{Atespace: "team-a", Name: "id1"}
			_, err := w.detachFromFullNode(ctx, ref, actor, &ateapipb.ActorTemplate{}, apierror.ResourceExhausted("%w", errNoWorkerRoom))
			wantCode := codes.Unavailable
			if kind == "gone" {
				wantCode = codes.FailedPrecondition
			}
			if kind == "upload completes during lookup" {
				wantCode = codes.OK
			}
			if kind == "no capacity" {
				wantCode = codes.ResourceExhausted
				if lookups != 0 {
					t.Error("looked up lost node without capacity elsewhere")
				}
			}
			if apierror.Code(err) != wantCode {
				t.Fatalf("detach = %v, want %v", err, wantCode)
			}
			after, getErr := st.GetActor(ctx, ref)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if kind != "gone" && kind != "upload completes during lookup" && !proto.Equal(after, actor) {
				t.Error("unconfirmed node loss changed actor")
			}
			if kind == "upload completes during lookup" {
				if after.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED || after.GetStatus().GetAssignedNode() != "" || after.GetStatus().GetCrash() != nil {
					t.Errorf("completed upload was not detached: %v", after.GetStatus())
				}
			}
			if kind == "lookup error" && !strings.Contains(err.Error(), "node lookup failed") {
				t.Error("lost node lookup error context")
			}
			if atelet.detachRequest() != nil {
				t.Error("unexpected DetachActor RPC")
			}
		})
	}
}

func TestRecordGoneNode_Conflict(t *testing.T) {
	for _, kind := range []string{"upload completes", "state changes", "incarnation changes", "write fails", "canceled", "lease lost before write"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			st := newTestPersistence(t)
			actor := seedGoneNodeActor(t, ctx, st, false)
			ref := resources.ActorRef{Atespace: "team-a", Name: "id1"}
			previousCounter := actorCrashesCounter
			t.Cleanup(func() { actorCrashesCounter = previousCounter })
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
			if err := RegisterActorCrashes(provider.Meter("test")); err != nil {
				t.Fatal(err)
			}
			records := crashRecords(t)
			w, _ := newDetachTestWorkflow(t, ctx, st)
			expected := actor
			wantCode := codes.Aborted
			switch kind {
			case "write fails":
				w.store = failingUpdateActorStore{Interface: st, err: errors.New("write failed")}
				wantCode = codes.Internal
			case "canceled":
				cancel()
				wantCode = codes.Canceled
			default:
				w.store = &conflictInjectingStore{Interface: st, inject: func() {
					if kind == "lease lost before write" {
						cancel()
						return
					}
					if kind == "incarnation changes" {
						if _, err := st.DeleteActor(ctx, ref, store.DeletePreconditions{UID: actor.GetMetadata().GetUid()}); err != nil {
							t.Fatal(err)
						}
						replacement := proto.CloneOf(actor)
						replacement.Metadata = &ateapipb.ResourceMetadata{Atespace: ref.Atespace, Name: ref.Name}
						var err error
						expected, err = st.CreateActor(ctx, replacement)
						if err != nil {
							t.Fatal(err)
						}
						if expected.GetMetadata().GetUid() == actor.GetMetadata().GetUid() {
							t.Fatal("replacement kept original UID")
						}
						return
					}
					var err error
					expected, err = st.UpdateActor(ctx, ref, store.PreconditionFrom(actor), func(a *ateapipb.Actor) error {
						switch kind {
						case "upload completes":
							a.Status.Snapshots[1].DurableSnapshot.Status = ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
						case "state changes":
							a.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
						}
						return nil
					})
					if err != nil {
						t.Fatal(err)
					}
				}}
			}
			_, err := w.recordGoneNode(ctx, ref, actor.GetMetadata().GetUid(), "gone-node")
			if kind == "canceled" || kind == "lease lost before write" {
				if !errors.Is(err, context.Canceled) {
					t.Errorf("error = %v, want canceled", err)
				}
			} else if apierror.Code(err) != wantCode {
				t.Errorf("error = %v, want %v", err, wantCode)
			}
			after, err := st.GetActor(context.Background(), ref)
			if err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(after, expected) {
				t.Errorf("failed write changed actor: %v, want %v", after, expected)
			}
			assertNoCrashMetricDatapoint(t, reader)
			if len(*records) != 0 {
				t.Error("logged a crash after a failed write")
			}
			if kind == "upload completes" {
				w.store = st
				recovered, err := w.recordGoneNode(ctx, ref, actor.GetMetadata().GetUid(), "gone-node")
				if err != nil || recovered.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED || recovered.GetStatus().GetAssignedNode() != "" {
					t.Errorf("retry = %v/%v, want durable-only suspended actor", recovered, err)
				}
				assertNoCrashMetricDatapoint(t, reader)
			}
		})
	}
}
