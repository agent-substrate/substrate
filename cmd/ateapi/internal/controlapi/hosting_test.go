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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/scheduling"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// TestHostingNodeAfterWorkerRelease drives each workflow that releases an
// actor's worker before it rewrites the actor's status into the window
// between the two, by failing the second write. The status still names the
// worker there, and keeps doing so until the workflow is retried. The worker's
// node must not count as hosting the actor in that window, nor once the worker
// hosts another actor.
func TestHostingNodeAfterWorkerRelease(t *testing.T) {
	// The worker is the one wireTestAssignment names, so a pause or suspend
	// reaches newWireCaptureWorkflow's fake atelet on its node.
	wa := wireTestAssignment()
	node := wa.GetNodeName()
	gvisorTemplate := &ateapipb.ActorTemplate{SandboxConfig: &ateapipb.SandboxConfig{SandboxClass: "gvisor"}}
	tests := []struct {
		name  string
		state ateapipb.ActorState
		// snapshot, when set, is the actor's latest snapshot.
		snapshot func(t *testing.T) *ateapipb.Snapshot
		// workerClass is the worker's sandbox class. A template asks for
		// gVisor, so "microvm" makes the worker ineligible.
		workerClass string
		// finalize runs the workflow or step that releases the worker and
		// then records the actor's new state against st.
		finalize func(t *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef, actor *ateapipb.Actor) error
		// wantRetry is the code of the retried finalize against the real
		// store.
		wantRetry codes.Code
		// wantFinalState and wantFinalNode are the actor's state and
		// HostingNode once the retry commits.
		wantFinalState ateapipb.ActorState
		wantFinalNode  string
	}{
		{
			name:  "pause",
			state: ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			snapshot: func(*testing.T) *ateapipb.Snapshot {
				// A pause in progress: node-local and not yet captured.
				return newLocalSnapshot(1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "local-snap-1", "")
			},
			workerClass: "gvisor",
			// The actor is seeded SUSPENDING, so the pause skips its mark step
			// and its first actor update is the finalize commit.
			finalize: func(t *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef, _ *ateapipb.Actor) error {
				w, _ := newWireCaptureWorkflow(t, st)
				_, err := w.PauseActor(ctx, actorRef)
				return err
			},
			wantRetry:      codes.OK,
			wantFinalState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			// Paused: the node holding the local snapshot.
			wantFinalNode: node,
		},
		{
			name:  "suspend",
			state: ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			snapshot: func(t *testing.T) *ateapipb.Snapshot {
				return newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "snapshot-1", someActorSnapshotURI(t, testStorageLocation, "team-a", "snapshot-1"), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING)
			},
			workerClass: "gvisor",
			// As for the pause, the finalize commit is the first actor update.
			finalize: func(t *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef, _ *ateapipb.Actor) error {
				w, _ := newWireCaptureWorkflow(t, st)
				_, err := w.SuspendActor(ctx, actorRef)
				return err
			},
			wantRetry:      codes.OK,
			wantFinalState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		},
		{
			name:        "crash",
			state:       ateapipb.ActorState_ACTOR_STATE_RUNNING,
			workerClass: "gvisor",
			finalize: func(_ *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef, _ *ateapipb.Actor) error {
				return crashActor(ctx, st, actorRef, ateattr.OperationResume, "test crash")
			},
			wantRetry:      codes.OK,
			wantFinalState: ateapipb.ActorState_ACTOR_STATE_CRASHED,
		},
		{
			// A resume that finds its worker ineligible releases it, then
			// crashes the actor. The retry finds the worker no longer hosts
			// the actor and crashes it.
			name:        "resume onto an ineligible worker",
			state:       ateapipb.ActorState_ACTOR_STATE_RESUMING,
			workerClass: "microvm",
			finalize: func(_ *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef, actor *ateapipb.Actor) error {
				_, err := (&ActorWorkflow{store: st, scheduler: scheduling.New(nil)}).validateAssignedWorker(ctx, actorRef, actor, gvisorTemplate)
				return err
			},
			wantRetry:      codes.Aborted,
			wantFinalState: ateapipb.ActorState_ACTOR_STATE_CRASHED,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
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
				SandboxClass:    tc.workerClass,
				Status:          &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE, Capacity: &ateapipb.WorkerResources{Actors: 1}},
			}); err != nil {
				t.Fatalf("CreateWorker: %v", err)
			}
			status := &ateapipb.ActorStatus{
				State:                  tc.state,
				AssignedNode:           node,
				WorkerAssignment:       wireTestAssignment(),
				LastAssignedGeneration: 1,
			}
			if tc.snapshot != nil {
				status.Snapshots = []*ateapipb.Snapshot{tc.snapshot(t)}
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
			if err := tc.finalize(t, ctx, conflictingUpdateStore{Interface: persistence}, actorRef, actor); err == nil {
				t.Fatal("finalize succeeded through a store that fails every actor update")
			}
			if _, err := persistence.GetWorkerAssignment(ctx, workerName, uid); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("worker assignment after release: err %v, want ErrNotFound", err)
			}
			got, actor = hostingNode("released, status not finalized")
			if actor.GetStatus().GetState() != tc.state || actor.GetStatus().GetWorkerAssignment() == nil {
				t.Fatalf("actor after the failed update: state %v, worker assignment %v; want %v naming the worker", actor.GetStatus().GetState(), actor.GetStatus().GetWorkerAssignment(), tc.state)
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
			if err := tc.finalize(t, ctx, persistence, actorRef, actor); apierror.Code(err) != tc.wantRetry {
				t.Fatalf("finalize retry: code %v, want %v (err: %v)", apierror.Code(err), tc.wantRetry, err)
			}
			got, actor = hostingNode("finalized")
			if actor.GetStatus().GetState() != tc.wantFinalState {
				t.Errorf("actor state once finalized = %v, want %v", actor.GetStatus().GetState(), tc.wantFinalState)
			}
			if got != tc.wantFinalNode {
				t.Errorf("HostingNode once finalized = %q, want %q", got, tc.wantFinalNode)
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
		SandboxClass:    "microvm",
		Status:          &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE, Capacity: &ateapipb.WorkerResources{Actors: 1}},
	}
	free := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: testWorkerUID("free-pod")},
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool-b",
		WorkerPod:       "free-pod",
		WorkerPodUid:    testWorkerUID("free-pod"),
		NodeName:        "node2",
		SandboxClass:    "gvisor",
		Status:          &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE, Capacity: &ateapipb.WorkerResources{Actors: 1}},
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
	tmpl := &ateapipb.ActorTemplate{SandboxConfig: &ateapipb.SandboxConfig{SandboxClass: "gvisor"}}
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
