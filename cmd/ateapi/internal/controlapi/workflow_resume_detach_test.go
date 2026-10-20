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
	"time"

	"go.ate.dev/substrate/cmd/ateapi/internal/scheduling"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/cmd/ateapi/internal/workercache"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// newDetachTestWorkflow schedules over the workers already in st and dials the
// capturing atelet on node-1.
func newDetachTestWorkflow(t *testing.T, ctx context.Context, st store.Interface) (*ActorWorkflow, *capturingAtelet) {
	t.Helper()
	w, atelet := newWireCaptureWorkflow(t, st)
	cacheCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	wc := workercache.New(st, time.Minute)
	if err := wc.Start(cacheCtx); err != nil {
		t.Fatalf("workercache.Start: %v", err)
	}
	w.workerCache = wc
	w.scheduler = scheduling.New(wc)
	return w, atelet
}

// fillWorker binds another actor to worker, using up its one place.
func fillWorker(t *testing.T, st store.Interface, worker *ateapipb.Worker) {
	t.Helper()
	seedAssignment(t, st, worker.GetMetadata().GetName(), &ateapipb.ActorAssignment{
		Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: "other"},
		ActorUid: "other-uid",
	})
}

// detachedState is atelet's state after detaching snap.
func detachedState(snap *ateapipb.Snapshot) *ateletpb.ActorSnapshotState {
	return &ateletpb.ActorSnapshotState{LatestDurable: &ateletpb.SnapshotReport{Snapshot: &ateletpb.Snapshot{
		SnapshotUuid: snap.GetUuid(),
		Generation:   snap.GetGeneration(),
		Fidelity:     fidelityToAtelet(snap.GetFidelity()),
		Object:       &ateletpb.ObjectStorage{SnapshotUri: snap.GetDurableSnapshot().GetObject().GetSnapshotUri()},
	}}}
}

func TestShouldDetachFromNode(t *testing.T) {
	noRoom := apierror.ResourceExhausted("%w", errNoWorkerRoom)
	pinned := func(state ateapipb.ActorState, node string) *ateapipb.Actor {
		return &ateapipb.Actor{Status: &ateapipb.ActorStatus{State: state, AssignedNode: node}}
	}
	tests := []struct {
		name  string
		actor *ateapipb.Actor
		err   error
		want  bool
	}{
		{"pinned suspended actor with no room", pinned(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, "node-1"), noRoom, true},
		{"no room wrapped by a step", pinned(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, "node-1"), errors.Join(noRoom), true},
		{"assigned", pinned(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, "node-1"), nil, false},
		{"another error", pinned(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, "node-1"), apierror.Aborted("conflict"), false},
		{"not pinned", pinned(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ""), noRoom, false},
		{"resuming", pinned(ateapipb.ActorState_ACTOR_STATE_RESUMING, "node-1"), noRoom, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldDetachFromNode(tt.actor, tt.err); got != tt.want {
				t.Errorf("shouldDetachFromNode() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestDetachFromFullNode runs the step for an actor pinned to a full node.
func TestDetachFromFullNode(t *testing.T) {
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
	suspended := ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	const uuid = "pause-snap-1"
	noRoom := apierror.ResourceExhausted("%w", errNoWorkerRoom)
	amd64RT := testRuntime("amd64", "")
	arm64RT := testRuntime("arm64", "")

	tests := []struct {
		name string
		// node is the actor's full node. Only node-1 has an atelet.
		node string
		// otherNode adds a free worker on node-2 with otherRuntime, or the
		// test runtime when unset.
		otherNode    bool
		otherRuntime *ateapipb.SandboxRuntime
		// snapRuntime stamps the MEMORY snapshot.
		snapRuntime *ateapipb.SandboxRuntime
		// completed records the snapshot's upload COMPLETED.
		completed bool
		cancel    bool
		noIPs     bool
		// before updates the actor after it was read and before the step
		// runs, and during while atelet detaches it.
		before, during func(*ateapipb.Actor)
		// detachState builds DetachActor's reply from the snapshot.
		detachState  func(*ateapipb.Snapshot) *ateletpb.ActorSnapshotState
		detachErr    error
		wantDetached bool
		wantCode     codes.Code
		wantCall     bool
	}{{
		name:         "another node has room: detaches",
		node:         "node-1",
		otherNode:    true,
		detachState:  detachedState,
		wantDetached: true,
		wantCall:     true,
	}, {
		name:         "another node can restore the snapshot's runtime: detaches",
		node:         "node-1",
		otherNode:    true,
		otherRuntime: amd64RT,
		snapRuntime:  amd64RT,
		detachState:  detachedState,
		wantDetached: true,
		wantCall:     true,
	}, {
		name:         "actor updated after it was read: detaches",
		node:         "node-1",
		otherNode:    true,
		before:       func(*ateapipb.Actor) {},
		detachState:  detachedState,
		wantDetached: true,
		wantCall:     true,
	}, {
		name:      "selector changed after the actor was read: no room under the new one",
		node:      "node-1",
		otherNode: true,
		before: func(a *ateapipb.Actor) {
			a.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}
		},
		wantCode: codes.ResourceExhausted,
	}, {
		name:        "actor updated while atelet detaches: Aborted, record unchanged",
		node:        "node-1",
		otherNode:   true,
		during:      func(*ateapipb.Actor) {},
		detachState: detachedState,
		wantCode:    codes.Aborted,
		wantCall:    true,
	}, {
		name:         "atelet holds no entry, upload COMPLETED: detaches",
		node:         "node-1",
		otherNode:    true,
		completed:    true,
		detachState:  func(*ateapipb.Snapshot) *ateletpb.ActorSnapshotState { return &ateletpb.ActorSnapshotState{} },
		wantDetached: true,
		wantCall:     true,
	}, {
		name:        "atelet holds no entry, upload PENDING: FailedPrecondition, record unchanged",
		node:        "node-1",
		otherNode:   true,
		detachState: func(*ateapipb.Snapshot) *ateletpb.ActorSnapshotState { return &ateletpb.ActorSnapshotState{} },
		wantCode:    codes.FailedPrecondition,
		wantCall:    true,
	}, {
		name:     "no node has room: the assignment's error, without detaching",
		node:     "node-1",
		wantCode: codes.ResourceExhausted,
	}, {
		name:         "room only on another runtime: detaches",
		node:         "node-1",
		otherNode:    true,
		otherRuntime: arm64RT,
		snapRuntime:  amd64RT,
		detachState:  detachedState,
		wantDetached: true,
		wantCall:     true,
	}, {
		name:         "no atelet on the node, upload completed: detaches",
		node:         "node-3",
		otherNode:    true,
		completed:    true,
		wantDetached: true,
	}, {
		name:         "atelet unreachable, upload completed: detaches",
		node:         "node-1",
		otherNode:    true,
		completed:    true,
		detachErr:    apierror.Unavailable("connection refused"),
		wantDetached: true,
		wantCall:     true,
	}, {
		name:         "atelet deadline exceeded, upload completed: detaches",
		node:         "node-1",
		otherNode:    true,
		completed:    true,
		detachErr:    context.DeadlineExceeded,
		wantDetached: true,
		wantCall:     true,
	}, {
		name:      "actor updated while unreachable: Aborted, record unchanged",
		node:      "node-1",
		otherNode: true,
		completed: true,
		during:    func(*ateapipb.Actor) {},
		detachErr: apierror.Unavailable("connection refused"),
		wantCode:  codes.Aborted,
		wantCall:  true,
	}, {
		name:      "workflow canceled during detach: actor stays on its node",
		node:      "node-1",
		otherNode: true,
		completed: true,
		cancel:    true,
		detachErr: apierror.Unavailable("connection refused"),
		wantCode:  codes.Canceled,
	}, {
		name:      "atelet missing IP: actor stays on its node",
		node:      "node-1",
		otherNode: true,
		completed: true,
		noIPs:     true,
		wantCode:  codes.Unavailable,
	}, {
		name:      "no atelet on the node: Unavailable, actor stays on its node",
		node:      "node-3",
		otherNode: true,
		wantCode:  codes.Unavailable,
	}, {
		name:      "atelet refuses: Aborted, actor stays on its node",
		node:      "node-1",
		otherNode: true,
		detachErr: apierror.Aborted("actor is in use on this node"),
		wantCode:  codes.Aborted,
		wantCall:  true,
	}, {
		name:      "atelet unreachable: Unavailable, actor stays on its node",
		node:      "node-1",
		otherNode: true,
		detachErr: apierror.Unavailable("connection refused"),
		wantCode:  codes.Unavailable,
		wantCall:  true,
	}, {
		name:      "atelet keeps a local copy: Internal, actor stays on its node",
		node:      "node-1",
		otherNode: true,
		detachState: func(snap *ateapipb.Snapshot) *ateletpb.ActorSnapshotState {
			st := detachedState(snap)
			st.LatestLocal = st.GetLatestDurable()
			return st
		},
		wantCode: codes.Internal,
		wantCall: true,
	}, {
		name:      "returned state conflicts with the record: Internal, actor stays on its node",
		node:      "node-1",
		otherNode: true,
		detachState: func(snap *ateapipb.Snapshot) *ateletpb.ActorSnapshotState {
			st := detachedState(snap)
			st.LatestDurable.Snapshot.Object.SnapshotUri = someActorSnapshotURI(t, testStorageLocation, "team-a", "other")
			return st
		},
		wantCode: codes.Internal,
		wantCall: true,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			st := newTestPersistence(t)
			fillWorker(t, st, seedRuntimeWorker(t, ctx, st, "pod-"+tt.node, tt.node, testSandboxRuntime()))
			if tt.otherNode {
				otherRuntime := tt.otherRuntime
				if otherRuntime == nil {
					otherRuntime = testSandboxRuntime()
				}
				seedRuntimeWorker(t, ctx, st, "pod-2", "node-2", otherRuntime)
			}
			snap := newPausedSnapshot(t, 1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", uuid, tt.node)
			snap.SandboxRuntime = tt.snapRuntime
			if tt.completed {
				snap.DurableSnapshot.Status = ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
			}
			actor := seedActorWithSnapshot(t, ctx, st, suspended, tt.node, snap)
			w, atelet := newDetachTestWorkflow(t, ctx, st)
			if tt.noIPs {
				w.dialer = NewAteletDialer(newTestAteletIndexer(t, &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Namespace: installdefaults.SystemNamespace, Name: "atelet-1", UID: "atelet-uid"},
					Spec:       corev1.PodSpec{NodeName: tt.node},
				}), installdefaults.SystemNamespace, "", "")
			}
			if tt.detachState != nil {
				atelet.detachState = tt.detachState(snap)
			}
			atelet.detachErr = tt.detachErr
			// update writes mutate to the stored actor, moving its version on.
			update := func(mutate func(*ateapipb.Actor)) {
				t.Helper()
				cur, err := st.GetActor(ctx, actorRef)
				if err != nil {
					t.Fatalf("GetActor: %v", err)
				}
				if _, err := st.UpdateActor(ctx, actorRef, store.PreconditionFrom(cur), func(toUpdate *ateapipb.Actor) error {
					mutate(toUpdate)
					return nil
				}); err != nil {
					t.Fatalf("UpdateActor: %v", err)
				}
			}
			if tt.before != nil {
				update(tt.before)
			}
			if tt.during != nil {
				atelet.onDetach = func() { update(tt.during) }
			}
			if tt.cancel {
				atelet.onDetach = cancel
			}
			// The record unchanged by a failed step is the one as last written.
			want, err := st.GetActor(ctx, actorRef)
			if err != nil {
				t.Fatalf("GetActor: %v", err)
			}

			stored, err := w.detachFromFullNode(ctx, actorRef, actor, &ateapipb.ActorTemplate{}, noRoom)
			if got := apierror.Code(err); got != tt.wantCode {
				t.Fatalf("detachFromFullNode() = %v, want code %v", err, tt.wantCode)
			}
			req := atelet.detachRequest()
			if !tt.cancel && (req != nil) != tt.wantCall {
				t.Fatalf("DetachActor request = %v, want a call: %v", req, tt.wantCall)
			}
			if req != nil {
				want := &ateletpb.DetachActorRequest{Atespace: "team-a", ActorName: "id1", ActorUid: actor.GetMetadata().GetUid()}
				if !proto.Equal(req, want) {
					t.Errorf("DetachActor request = %v, want %v", req, want)
				}
			}

			after, err := st.GetActor(context.Background(), actorRef)
			if err != nil {
				t.Fatalf("GetActor: %v", err)
			}
			if !tt.wantDetached {
				if !proto.Equal(after.GetStatus(), want.GetStatus()) {
					t.Errorf("status = %v, want unchanged %v", after.GetStatus(), want.GetStatus())
				}
				return
			}
			if !proto.Equal(stored, after) {
				t.Errorf("returned actor = %v, want the stored %v", stored, after)
			}
			if got := after.GetStatus().GetAssignedNode(); got != "" {
				t.Errorf("AssignedNode = %q, want none", got)
			}
			got := after.GetStatus().GetSnapshots()
			if len(got) != 1 || got[0].GetLocality() != "" ||
				got[0].GetDurableSnapshot().GetStatus() != ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED {
				t.Errorf("snapshots = %v, want %s durable only and COMPLETED", got, uuid)
			}
		})
	}
}

// TestResumeActor_DetachesFromFullNode resumes an actor pinned to node-1 with
// room on node-2: it moves to node-2 only when node-1 is full.
func TestResumeActor_DetachesFromFullNode(t *testing.T) {
	tests := []struct {
		name     string
		node1    bool
		wantNode string
	}{
		{name: "node-1 full: detaches and resumes on node-2", wantNode: "node-2"},
		{name: "node-1 has room: resumes there from the local copy", node1: true, wantNode: "node-1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st := newTestPersistence(t)
			actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}

			storetest.MustCreateAtespace(t, ctx, st, "ns")
			if _, err := st.CreateActorTemplate(ctx, &ateapipb.ActorTemplate{
				Metadata:       &ateapipb.ResourceMetadata{Atespace: "ns", Name: "tmpl1"},
				SnapshotConfig: &ateapipb.SnapshotConfig{StorageLocation: testStorageLocation},
			}); err != nil {
				t.Fatalf("create template: %v", err)
			}
			workers := map[string]*ateapipb.Worker{
				"node-1": seedRuntimeWorker(t, ctx, st, "pod-1", "node-1", testSandboxRuntime()),
				"node-2": seedRuntimeWorker(t, ctx, st, "pod-2", "node-2", testSandboxRuntime()),
			}
			if !tt.node1 {
				fillWorker(t, st, workers["node-1"])
			}
			snap := newPausedSnapshot(t, 1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "pause-snap-1", "node-1")
			seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_SUSPENDED, func(a *ateapipb.Actor) {
				a.Status.AssignedNode = "node-1"
				a.Status.LastAssignedGeneration = 1
				a.Status.Snapshots = []*ateapipb.Snapshot{proto.CloneOf(snap)}
			})
			w, atelet := newDetachTestWorkflow(t, ctx, st)
			addAteletOnNode(t, w, "node-2")
			atelet.detachState = detachedState(snap)

			got, resumed, err := w.ResumeActor(ctx, actorRef)
			if err != nil {
				t.Fatalf("ResumeActor: %v", err)
			}
			if !resumed || got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
				t.Fatalf("ResumeActor = (state %v, resumed %v), want RUNNING and resumed", got.GetStatus().GetState(), resumed)
			}
			detached := atelet.detachRequest() != nil
			if wantDetach := tt.wantNode != "node-1"; detached != wantDetach {
				t.Errorf("DetachActor called = %v, want %v", detached, wantDetach)
			}
			if node := got.GetStatus().GetAssignedNode(); node != tt.wantNode {
				t.Errorf("AssignedNode = %q, want %s", node, tt.wantNode)
			}
			restore, run := atelet.requests()
			if restore == nil || run != nil {
				t.Fatalf("atelet requests = (restore=%v, run=%v), want exactly one Restore", restore, run)
			}
			if got, want := restore.GetWorkerPodUid(), workers[tt.wantNode].GetWorkerPodUid(); got != want {
				t.Errorf("Restore worker = %q, want %s's %q", got, tt.wantNode, want)
			}
			want := &ateletpb.Snapshot{
				SnapshotUuid: snap.GetUuid(),
				Generation:   snap.GetGeneration(),
				Fidelity:     ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
				// The actor's own snapshot, so private, whether atelet
				// downloads it or restores the local copy.
				Object: &ateletpb.ObjectStorage{
					SnapshotUri: snap.GetDurableSnapshot().GetObject().GetSnapshotUri(),
					Sharing:     ateletpb.SnapshotSharing_SNAPSHOT_SHARING_PRIVATE,
				},
			}
			if !proto.Equal(restore.GetSnapshot(), want) {
				t.Errorf("Restore snapshot = %v, want %v", restore.GetSnapshot(), want)
			}
		})
	}
}

// addAteletOnNode routes node's atelet to the same capturing fake as node-1's.
func addAteletOnNode(t *testing.T, w *ActorWorkflow, node string) {
	t.Helper()
	cached, ok := w.dialer.ateletConns.Get("atelet-uid")
	if !ok {
		t.Fatal("no cached conn for the fake atelet")
	}
	uid := types.UID("atelet-uid-" + node)
	if err := w.dialer.ateletIndexer.Add(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: installdefaults.SystemNamespace, Name: "atelet-" + node, UID: uid},
		Spec:       corev1.PodSpec{NodeName: node},
		Status:     corev1.PodStatus{PodIPs: []corev1.PodIP{{IP: "10.0.0.2"}}},
	}); err != nil {
		t.Fatalf("adding atelet pod on %s: %v", node, err)
	}
	w.dialer.ateletConns.Add(string(uid), &ateletConn{ip: "10.0.0.2", conn: cached.(*ateletConn).conn})
}

// TestDetachFromFullNode_ReentryRecordsAfterConflict loses the record write
// to a concurrent update, then runs the step again, as the next resume does:
// the second attempt detaches again and records the detach.
func TestDetachFromFullNode_ReentryRecordsAfterConflict(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
	fillWorker(t, st, seedRuntimeWorker(t, ctx, st, "pod-node-1", "node-1", testSandboxRuntime()))
	seedRuntimeWorker(t, ctx, st, "pod-2", "node-2", testSandboxRuntime())
	snap := newPausedSnapshot(t, 1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "pause-snap-1", "node-1")
	actor := seedActorWithSnapshot(t, ctx, st, ateapipb.ActorState_ACTOR_STATE_SUSPENDED, "node-1", snap)
	w, atelet := newDetachTestWorkflow(t, ctx, st)
	atelet.detachState = detachedState(snap)
	noRoom := apierror.ResourceExhausted("%w", errNoWorkerRoom)

	// A concurrent update while atelet detaches.
	atelet.onDetach = func() {
		cur, err := st.GetActor(ctx, actorRef)
		if err != nil {
			t.Errorf("GetActor: %v", err)
			return
		}
		if _, err := st.UpdateActor(ctx, actorRef, store.PreconditionFrom(cur), func(*ateapipb.Actor) error { return nil }); err != nil {
			t.Errorf("UpdateActor: %v", err)
		}
	}
	if _, err := w.detachFromFullNode(ctx, actorRef, actor, &ateapipb.ActorTemplate{}, noRoom); apierror.Code(err) != codes.Aborted {
		t.Fatalf("first detachFromFullNode() = %v, want Aborted", err)
	}

	atelet.onDetach = nil
	stored, err := w.detachFromFullNode(ctx, actorRef, actor, &ateapipb.ActorTemplate{}, noRoom)
	if err != nil {
		t.Fatalf("second detachFromFullNode() = %v, want success", err)
	}
	if got := stored.GetStatus().GetAssignedNode(); got != "" {
		t.Errorf("AssignedNode = %q, want none", got)
	}
	if got := stored.GetStatus().GetSnapshots(); len(got) != 1 || got[0].GetLocality() != "" ||
		got[0].GetDurableSnapshot().GetStatus() != ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED {
		t.Errorf("snapshots = %v, want pause-snap-1 durable only and COMPLETED", got)
	}
}
