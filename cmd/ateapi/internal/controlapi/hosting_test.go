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
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
)

// TestHostingNodeAfterWorkerRelease drives each of ReleaseWindowWorkflows into
// the window between releasing the actor's worker and rewriting its status, by
// failing the second write. The status still names the worker there, and keeps
// doing so until the workflow is retried. The worker's node must not count as
// hosting the actor in that window, nor once the worker hosts another actor.
func TestHostingNodeAfterWorkerRelease(t *testing.T) {
	// The worker is the one wireTestAssignment names, so an async or sync suspend
	// reaches newWireCaptureWorkflow's fake atelet on its node.
	wa := wireTestAssignment()
	node := wa.GetNodeName()
	// wantFinalNode is HostingNode once the retry commits, by workflow name.
	wantFinalNode := map[string]string{
		// Async suspend: the node holding the local snapshot.
		"pause": node,
	}
	for _, tc := range ReleaseWindowWorkflows() {
		t.Run(tc.Name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			seedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: testStorageLocation})
			actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
			workerName := wa.GetWorker().GetName()
			if _, err := persistence.CreateWorker(ctx, &ateapipb.Worker{
				Metadata:        &ateapipb.ResourceMetadata{Name: workerName},
				WorkerNamespace: wa.GetWorkerNamespace(),
				WorkerPool:      wa.GetWorkerPool(),
				WorkerPod:       wa.GetWorkerPod(),
				WorkerPodUid:    wa.GetWorkerPodUid(),
				NodeName:        node,
				Status: &ateapipb.WorkerStatus{
					State:          ateapipb.WorkerState_WORKER_STATE_ACTIVE,
					Capacity:       &ateapipb.WorkerResources{Actors: 1},
					SandboxClasses: []*ateapipb.WorkerSandboxClass{{SandboxClass: "gvisor"}},
				},
			}); err != nil {
				t.Fatalf("CreateWorker: %v", err)
			}
			status := &ateapipb.ActorStatus{
				State:                  tc.State,
				AssignedNode:           node,
				WorkerAssignment:       wireTestAssignment(),
				LastAssignedGeneration: 1,
			}
			if tc.Snapshot != nil {
				status.Snapshots = []*ateapipb.Snapshot{tc.Snapshot(t)}
			}
			created := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
				ActorTemplate: suspendTestTemplateRef(),
				Status:        status,
			})
			uid := created.GetMetadata().GetUid()
			seedAssignment(t, persistence, workerName, &ateapipb.ActorAssignment{
				Actor:    &ateapipb.ObjectRef{Atespace: actorRef.Atespace, Name: actorRef.Name},
				ActorUid: uid,
			})

			hostingNode := func(stage string) (string, *ateapipb.Actor) {
				t.Helper()
				actor, err := persistence.GetActor(ctx, actorRef)
				if err != nil {
					t.Fatalf("GetActor: %v", err)
				}
				got, err := HostingNode(ctx, persistence, actor)
				if err != nil {
					t.Fatalf("%s: HostingNode: %v", stage, err)
				}
				t.Logf("%s: actor_state=%s status_worker_set=%t hosting_node=%q",
					stage, actor.GetStatus().GetState(), actor.GetStatus().GetWorkerAssignment() != nil, got)
				return got, actor
			}

			got, actor := hostingNode("hosted")
			if got != node {
				t.Fatalf("HostingNode while the worker holds the actor = %q, want %q", got, node)
			}

			// The release commits; recording the new state fails.
			if err := tc.Finalize(t, ctx, conflictingUpdateStore{Interface: persistence}, actorRef, actor); err == nil {
				t.Fatal("finalize succeeded through a store that fails every actor update")
			}
			if _, err := persistence.GetWorkerAssignment(ctx, workerName, uid); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("worker assignment after release: err %v, want ErrNotFound", err)
			}
			got, actor = hostingNode("released, status not finalized")
			if actor.GetStatus().GetState() != tc.State || actor.GetStatus().GetWorkerAssignment() == nil {
				t.Fatalf("actor after the failed update: state %v, worker assignment %v; want %v naming the worker", actor.GetStatus().GetState(), actor.GetStatus().GetWorkerAssignment(), tc.State)
			}
			if got != "" {
				t.Errorf("HostingNode after the worker was released = %q, want \"\"", got)
			}

			// The worker takes another actor.
			seedAssignment(t, persistence, workerName, &ateapipb.ActorAssignment{
				Actor:    &ateapipb.ObjectRef{Atespace: actorRef.Atespace, Name: "actor-2"},
				ActorUid: "5c1d2e3f-4a5b-4c6d-8e7f-9a0b1c2d3e4f",
			})
			if got, _ := hostingNode("worker rebooked for another actor"); got != "" {
				t.Errorf("HostingNode after the worker was rebooked = %q, want \"\"", got)
			}

			// A retry records the new state.
			if err := tc.Finalize(t, ctx, persistence, actorRef, actor); apierror.Code(err) != tc.WantRetry {
				t.Fatalf("finalize retry: code %v, want %v (err: %v)", apierror.Code(err), tc.WantRetry, err)
			}
			got, actor = hostingNode("finalized")
			if actor.GetStatus().GetState() != tc.WantFinalState {
				t.Errorf("actor state once finalized = %v, want %v", actor.GetStatus().GetState(), tc.WantFinalState)
			}
			if want := wantFinalNode[tc.Name]; got != want {
				t.Errorf("HostingNode once finalized = %q, want %q", got, want)
			}
		})
	}
}

// TestHostingNodeAfterResumeOntoNewWorker verifies that a resume moves the
// hosting node to the worker it binds: the bind commits before the status
// names the worker, so the worker assignment check never refuses the new
// worker's node, and the worker the actor was claimed by before is refused.
func TestHostingNodeAfterResumeOntoNewWorker(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}

	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})
	// stale-pod on node1 holds a claim from a failed attempt but is no longer
	// eligible; free-pod on node2 is eligible and free.
	stale := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID("stale-pod")},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool-a",
		WorkerPod:       "stale-pod",
		WorkerPodUid:    testWorkerUID("stale-pod"),
		NodeName:        "node1",
		Status: &ateapipb.WorkerStatus{
			State:          ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity:       &ateapipb.WorkerResources{Actors: 1},
			SandboxClasses: []*ateapipb.WorkerSandboxClass{{SandboxClass: "microvm", DefaultRuntime: &ateapipb.WorkerSandboxRuntime{Version: testSandboxRuntime().GetVersion()}}},
		},
	}
	free := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID("free-pod")},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool-b",
		WorkerPod:       "free-pod",
		WorkerPodUid:    testWorkerUID("free-pod"),
		NodeName:        "node2",
		Labels:          map[string]string{"pool": "b"},
		Status: &ateapipb.WorkerStatus{
			State:          ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Capacity:       &ateapipb.WorkerResources{Actors: 1},
			SandboxClasses: workerSandboxClasses(testSandboxRuntime()),
		},
	}
	for _, w := range []*ateapipb.Worker{stale, free} {
		if _, err := persistence.CreateWorker(ctx, w); err != nil {
			t.Fatalf("CreateWorker(%s): %v", w.GetWorkerPod(), err)
		}
	}
	seedAssignment(t, persistence, testWorkerUID("stale-pod"), &ateapipb.ActorAssignment{
		Actor:    &ateapipb.ObjectRef{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorUid: actor.GetMetadata().GetUid(),
	})

	cacheCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wc := workercache.New(persistence, time.Minute)
	if err := wc.Start(cacheCtx); err != nil {
		t.Fatalf("workercache.Start: %v", err)
	}
	w := &ActorWorkflow{store: persistence, workerCache: wc, scheduler: scheduling.New(wc)}
	tmpl := &ateapipb.ActorTemplate{
		WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"pool": "b"}},
	}
	resuming, _, err := w.assignWorkerAttempt(ctx, actorRef, actor, tmpl)
	if err != nil {
		t.Fatalf("assignWorkerAttempt: %v", err)
	}
	if got := resuming.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_RESUMING {
		t.Fatalf("actor state after assignment = %v, want RESUMING", got)
	}

	got, err := HostingNode(ctx, persistence, resuming)
	if err != nil {
		t.Fatalf("HostingNode: %v", err)
	}
	if got != "node2" {
		t.Errorf("HostingNode after resuming onto free-pod = %q, want node2", got)
	}

	// A status naming the released stale worker is refused.
	onStale := proto.CloneOf(resuming)
	onStale.Status.WorkerAssignment = &ateapipb.WorkerAssignment{Worker: &ateapipb.ObjectRef{Name: testWorkerUID("stale-pod")}}
	if got, err := HostingNode(ctx, persistence, onStale); err != nil || got != "" {
		t.Errorf("HostingNode naming the released stale worker = %q, %v; want \"\", nil", got, err)
	}
}

// TestHostingNodeWithoutWorker pins which no-worker shapes count assigned_node
// as hosting the actor. None of them reads the store.
func TestHostingNodeWithoutWorker(t *testing.T) {
	durable := func(st ateapipb.SnapshotStorageStatus) *ateapipb.SnapshotStorage {
		return &ateapipb.SnapshotStorage{Status: st, Object: &ateapipb.ObjectSnapshot{SnapshotUri: "gs://snaps/root/atespaces/a/actors/uid/snap-1"}}
	}
	status := func(state ateapipb.ActorState, locality string, d *ateapipb.SnapshotStorage) *ateapipb.ActorStatus {
		return &ateapipb.ActorStatus{
			State:                  state,
			AssignedNode:           "node-1",
			LastAssignedGeneration: 1,
			Snapshots:              []*ateapipb.Snapshot{{Uuid: "snap-1", Generation: 1, Owner: ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, Locality: locality, DurableSnapshot: d}},
		}
	}
	olderOnNode := status(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, "node-1", durable(ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED))
	olderOnNode.LastAssignedGeneration = 2
	olderOnNode.Snapshots = append(olderOnNode.Snapshots, &ateapipb.Snapshot{Uuid: "snap-2", Generation: 2, Owner: ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, DurableSnapshot: durable(ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)})
	noAssignedNode := status(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, "node-1", nil)
	noAssignedNode.AssignedNode = ""
	for _, tc := range []struct {
		name   string
		status *ateapipb.ActorStatus
		want   string
	}{
		{"paused", status(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, "node-1", nil), "node-1"},
		{"paused, upload pending", status(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, "node-1", durable(ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)), "node-1"},
		{"paused, uploaded, local copy kept", status(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, "node-1", durable(ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)), "node-1"},
		{"older snapshot on the node, latest not", olderOnNode, "node-1"},
		{"local copy but no assigned node", noAssignedNode, ""},
		{"suspending without a worker", status(ateapipb.ActorState_ACTOR_STATE_SUSPENDING, "node-1", durable(ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)), ""},
		{"crashed with a local copy", status(ateapipb.ActorState_ACTOR_STATE_CRASHED, "node-1", nil), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actor := &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "a", Name: "n", Uid: "uid"}, Status: tc.status}
			got, err := HostingNode(context.Background(), nil, actor)
			if err != nil || got != tc.want {
				t.Fatalf("HostingNode = %q, %v, want %q", got, err, tc.want)
			}
		})
	}
}
